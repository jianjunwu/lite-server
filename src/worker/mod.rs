pub mod protocol;
pub mod endpoint_manager;

use crate::config::{ModelConfig, OrchestrationConfig};
use crate::error::AppError;
use crate::inference_queue::InferenceQueue;
use crate::registry::{ModelRegistry, types::*};
use crate::worker::protocol::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::timeout;
use tracing::{error, info, warn};

/// Global pending response map: uid -> oneshot sender
pub type PendingMap = Arc<RwLock<HashMap<String, oneshot::Sender<InferenceResponse>>>>;

pub struct WorkerManager {
    registry: Arc<ModelRegistry>,
    repo_path: PathBuf,
    pending: PendingMap,
    // Track spawned child processes
    workers: Arc<RwLock<HashMap<String, Vec<WorkerProcess>>>>,
    inference_queue: Arc<InferenceQueue>,
}

struct WorkerProcess {
    worker_id: u32,
    model_name: String,
    version: String,
    child: Child,
    uds_path: PathBuf,
    response_tx: mpsc::UnboundedSender<InferenceResponse>,
}

impl WorkerManager {
    pub fn new(
        registry: Arc<ModelRegistry>,
        repo_path: PathBuf,
        inference_queue: Arc<InferenceQueue>,
    ) -> Self {
        Self {
            registry,
            repo_path,
            pending: Arc::new(RwLock::new(HashMap::new())),
            workers: Arc::new(RwLock::new(HashMap::new())),
            inference_queue,
        }
    }

    pub fn pending(&self) -> PendingMap {
        self.pending.clone()
    }

    pub async fn load_model(
        &self,
        model_name: &str,
        version: &str,
        config: &ModelConfig,
    ) -> Result<(), AppError> {
        info!("Loading model {} version {}", model_name, version);

        let model_dir = self.repo_path.join(model_name).join(version);
        if !model_dir.exists() {
            return Err(AppError::ModelNotFound(format!(
                "{} version {} not found at {}",
                model_name,
                version,
                model_dir.display()
            )));
        }

        let model_py = model_dir.join("model.py");
        let config_yaml = model_dir.join("config.yaml");

        // Check for ensemble
        let mut is_ensemble = false;
        if config_yaml.exists() {
            if let Ok(content) = tokio::fs::read_to_string(&config_yaml).await {
                if content.contains("ensemble:") {
                    is_ensemble = true;
                }
            }
        }

        if !model_py.exists() && !is_ensemble {
            return Err(AppError::ModelNotFound(format!(
                "Neither model.py nor ensemble config found in {}",
                model_dir.display()
            )));
        }

        // Register in registry
        let model_config = if config_yaml.exists() {
            crate::config::load_model_config(&config_yaml).unwrap_or_else(|_| config.clone())
        } else {
            config.clone()
        };

        let model_type = if is_ensemble {
            ModelType::Ensemble
        } else {
            ModelType::LitAPI
        };

        self.registry
            .register(model_name, version, model_config.clone(), model_type, model_dir.clone())
            .await?;

        if is_ensemble {
            // Ensemble: no workers, just mark ready
            self.registry
                .set_status(model_name, version, VersionStatus::Ready)
                .await?;
            crate::metrics::prometheus::record_model_load(model_name, version, true);
            info!("Ensemble {} version {} loaded", model_name, version);
            return Ok(());
        }

        // Launch workers
        let accelerator = model_config.accelerator.as_deref().unwrap_or("cpu");
        let devices = match &model_config.devices {
            Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(1) as usize,
            Some(serde_json::Value::String(s)) if s == "auto" => 1,
            _ => 1,
        };
        let workers_per_device = model_config.workers_per_device.unwrap_or(1);

        if model_config.continuous_batching && workers_per_device != 1 {
            warn!("continuous_batching enabled; forcing workers_per_device=1");
        }

        let total_workers = if model_config.continuous_batching {
            devices
        } else {
            devices * workers_per_device
        };

        let mut worker_infos = Vec::new();
        let mut worker_processes = Vec::new();

        for worker_id in 0..total_workers {
            let device = format!("{}:{}", accelerator, worker_id % devices);
            let uds_path = std::env::temp_dir()
                .join("lite-server")
                .join(format!("{}_{}_{}.sock", model_name, version, worker_id));

            // Ensure parent dir exists
            if let Some(parent) = uds_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            // Remove stale socket
            let _ = tokio::fs::remove_file(&uds_path).await;

            // Find python module path
            let python_path = Self::find_python_module_path().unwrap_or_default();
            let python_module_dir = if python_path.is_empty() {
                String::new()
            } else {
                format!("{}", python_path)
            };

            let mut cmd = Command::new("python");
            if !python_module_dir.is_empty() {
                let current_pythonpath = std::env::var("PYTHONPATH").unwrap_or_default();
                let new_pythonpath = if current_pythonpath.is_empty() {
                    python_module_dir
                } else {
                    format!("{}:{}", current_pythonpath, python_module_dir)
                };
                cmd.env("PYTHONPATH", new_pythonpath);
            }

            let mut child = cmd
                .arg("-m")
                .arg("lite_server.worker.inference")
                .arg("--model-name")
                .arg(model_name)
                .arg("--version")
                .arg(version)
                .arg("--model-py")
                .arg(&model_py)
                .arg("--config")
                .arg(&config_yaml)
                .arg("--device")
                .arg(&device)
                .arg("--worker-id")
                .arg(worker_id.to_string())
                .arg("--uds-path")
                .arg(&uds_path)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| AppError::Python(format!("failed to spawn worker: {}", e)))?;

            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            // Wait for "ready" signal
            let mut reader = BufReader::new(stdout).lines();
            let ready_line = timeout(Duration::from_secs(60), reader.next_line())
                .await
                .map_err(|_| AppError::InferenceTimeout("worker startup timeout".to_string()))?
                .map_err(|e| AppError::Io(e))?
                .ok_or_else(|| AppError::WorkerCrashed("worker exited before ready".to_string()))?;

            let startup: WorkerStartup = serde_json::from_str(&ready_line)
                .map_err(|e| AppError::Internal(format!("worker startup JSON parse error: {}", e)))?;

            if startup.status != "ready" {
                return Err(AppError::WorkerCrashed(format!(
                    "worker {} startup failed: {:?}",
                    worker_id, startup.message
                )));
            }

            info!("Worker {} for {} v{} ready (pid={:?})", worker_id, model_name, version, child.id());

            // Start stderr logger
            let model_name_clone = model_name.to_string();
            let version_clone = version.to_string();
            let worker_id_clone = worker_id;
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    eprintln!("[worker {} {} v{}] {}", worker_id_clone, model_name_clone, version_clone, line);
                }
            });

            let info = WorkerInfo {
                worker_id: worker_id as u32,
                device,
                uds_path: uds_path.clone(),
                pid: child.id(),
                status: WorkerStatus::Ready,
            };
            worker_infos.push(info);

            // Start response consumer for this worker
            let (response_tx, mut response_rx) = mpsc::unbounded_channel::<InferenceResponse>();
            let pending = self.pending.clone();
            tokio::spawn(async move {
                while let Some(response) = response_rx.recv().await {
                    let uid = response.uid.clone();
                    let sender = {
                        let guard = pending.read().await;
                        // oneshot::Sender is not Clone, so we need to remove it
                        guard.get(&uid).is_some()
                    };
                    if sender {
                        let mut guard = pending.write().await;
                        if let Some(sender) = guard.remove(&uid) {
                            let _ = sender.send(response);
                        }
                    }
                }
            });

            worker_processes.push(WorkerProcess {
                worker_id: worker_id as u32,
                model_name: model_name.to_string(),
                version: version.to_string(),
                child,
                uds_path,
                response_tx,
            });
        }

        self.registry
            .set_workers(model_name, version, worker_infos.clone())
            .await?;
        self.registry
            .set_status(model_name, version, VersionStatus::Ready)
            .await?;

        // Register inference queue for batching
        self.inference_queue
            .register_model(model_name, version, &model_config, worker_infos);

        {
            let mut workers = self.workers.write().await;
            let key = format!("{}_{}", model_name, version);
            workers.insert(key, worker_processes);
        }

        crate::metrics::prometheus::record_model_load(model_name, version, true);
        crate::metrics::prometheus::set_active_workers(model_name, version, total_workers as f64);

        info!("Model {} version {} loaded with {} workers", model_name, version, total_workers);
        Ok(())
    }

    pub async fn unload_model(
        &self,
        model_name: &str,
        version: Option<&str>,
    ) -> Result<bool, AppError> {
        if let Some(v) = version {
            self.unload_version(model_name, v).await?;
            Ok(true)
        } else {
            let versions = self.registry.list_versions(model_name).await;
            let version_strings: Vec<String> = versions.into_iter().map(|v| v.version).collect();
            let mut unloaded = false;
            for v in version_strings {
                self.unload_version(model_name, &v).await?;
                unloaded = true;
            }
            Ok(unloaded)
        }
    }

    async fn unload_version(
        &self,
        model_name: &str,
        version: &str,
    ) -> Result<(), AppError> {
        info!("Unloading {} version {}", model_name, version);

        // Unregister inference queue first to stop accepting new requests
        self.inference_queue.unregister_model(model_name, version);

        self.registry
            .set_status(model_name, version, VersionStatus::Unloading)
            .await?;

        let key = format!("{}_{}", model_name, version);
        {
            let mut workers = self.workers.write().await;
            if let Some(mut procs) = workers.remove(&key) {
                for mut proc in procs.drain(..) {
                    // Remove persistent connection from pool
                    crate::transport::uds::remove_connection(&proc.uds_path).await;
                    let _ = proc.child.kill().await;
                    let _ = proc.child.wait().await;
                    // Clean up UDS socket
                    let _ = tokio::fs::remove_file(&proc.uds_path).await;
                }
            }
        }

        self.registry.remove(model_name, version).await?;

        crate::metrics::prometheus::record_model_unload(model_name, version);
        crate::metrics::prometheus::set_active_workers(model_name, version, 0.0);

        info!("Model {} version {} unloaded", model_name, version);
        Ok(())
    }

    pub async fn reload_model(
        &self,
        model_name: &str,
        version: Option<&str>,
    ) -> Result<bool, AppError> {
        let v = match version {
            Some(v) => v.to_string(),
            None => match self.registry.get_active_version(model_name).await {
                Some(v) => v,
                None => return Ok(false),
            },
        };

        let config = match self.registry.get(model_name, Some(&v)).await {
            Some(mv) => mv.config,
            None => return Ok(false),
        };

        info!("Reloading {} version {}", model_name, v);
        self.unload_version(model_name, &v).await?;

        // Small delay to ensure cleanup
        tokio::time::sleep(Duration::from_millis(500)).await;

        self.load_model(model_name, &v, &config).await?;
        self.registry.activate_version(model_name, &v).await?;

        info!("Model {} version {} reloaded", model_name, v);
        Ok(true)
    }

    pub async fn shutdown(&self) {
        let workers = self.workers.read().await;
        let keys: Vec<String> = workers.keys().cloned().collect();
        drop(workers);

        for key in keys {
            let parts: Vec<&str> = key.rsplitn(2, '_').collect();
            if parts.len() == 2 {
                let version = parts[0];
                let model_name = parts[1];
                let _ = self.unload_version(model_name, version).await;
            }
        }
    }
}

/// Pick a worker using random strategy
pub fn pick_worker_random(num_workers: usize) -> usize {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_range(0..num_workers)
}

impl WorkerManager {
    fn find_python_module_path() -> Option<String> {
        // 1. Check compile-time manifest dir (development / cargo run)
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let dev_path = PathBuf::from(manifest_dir).join("python");
        if dev_path.exists() {
            return Some(dev_path.to_string_lossy().to_string());
        }

        // 2. Check relative to executable
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let exe_python = exe_dir.join("python");
                if exe_python.exists() {
                    return Some(exe_python.to_string_lossy().to_string());
                }
                // For target/debug/lite-server, check ../../python
                if let Ok(project_python) = exe_dir.join("../../python").canonicalize() {
                    if project_python.exists() {
                        return Some(project_python.to_string_lossy().to_string());
                    }
                }
            }
        }

        // 3. Check current working directory
        if let Ok(cwd) = std::env::current_dir() {
            let cwd_python = cwd.join("python");
            if cwd_python.exists() {
                return Some(cwd_python.to_string_lossy().to_string());
            }
        }

        None
    }
}
