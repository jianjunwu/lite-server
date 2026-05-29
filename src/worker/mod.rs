pub mod protocol;
pub mod endpoint_manager;

use crate::config::{ModelConfig, OrchestrationConfig};
use crate::error::AppError;
use crate::inference_queue::InferenceQueue;
use crate::registry::{ModelRegistry, types::*};
use crate::transport::zmq::WorkerZmqClient;
use crate::worker::protocol::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
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
    // ZMQ clients for active workers
    zmq_clients: Arc<RwLock<HashMap<String, Vec<Arc<WorkerZmqClient>>>>>,
}

struct WorkerProcess {
    worker_id: u32,
    model_name: String,
    version: String,
    child: Child,
    endpoint: String,
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
            zmq_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn pending(&self) -> PendingMap {
        self.pending.clone()
    }

    /// Get ZMQ clients for a model version (used for streaming).
    pub async fn get_zmq_clients(
        &self,
        model_name: &str,
        version: &str,
    ) -> Option<Vec<Arc<WorkerZmqClient>>> {
        let key = format!("{}_{}", model_name, version);
        let guard = self.zmq_clients.read().await;
        guard.get(&key).cloned()
    }

    pub async fn load_model(
        &self,
        model_name: &str,
        version: &str,
        config: &ModelConfig,
    ) -> Result<(), AppError> {
        info!("Loading model {} version {}", model_name, version);

        let model_dir = crate::validation::resolve_model_dir(
            &self.repo_path, model_name, version,
        )?;
        if !model_dir.exists() {
            return Err(AppError::ModelNotFound(format!(
                "{} version {} not found",
                model_name, version
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
                "Neither model.py nor ensemble config found for {} version {}",
                model_name, version
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
        let mut zmq_clients_for_model = Vec::new();

        for worker_id in 0..total_workers {
            let device = format!("{}:{}", accelerator, worker_id % devices);
            let endpoint = format!(
                "ipc:///tmp/lite-server/{}_{}_{}.sock",
                model_name, version, worker_id
            );

            // Remove stale socket
            let socket_str = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
            let socket_path = std::path::Path::new(socket_str);
            let _ = tokio::fs::remove_file(socket_path).await;
            if let Some(parent) = socket_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }

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
                .arg("--endpoint")
                .arg(&endpoint);

            if model_config.continuous_batching {
                child = child.arg("--continuous-batching");
            }

            let mut child = child
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| AppError::Python(format!("failed to spawn worker: {}", e)))?;

            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            // Wait for "ready" signal
            let mut reader = BufReader::new(stdout);
            let mut ready_line = String::new();
            let n = timeout(Duration::from_secs(60), reader.read_line(&mut ready_line))
                .await
                .map_err(|_| AppError::InferenceTimeout("worker startup timeout".to_string()))?
                .map_err(|e| AppError::Io(e))?;
            if n == 0 {
                return Err(AppError::WorkerCrashed("worker exited before ready".to_string()));
            }
            let stdout = reader.into_inner();

            let startup: WorkerStartup = serde_json::from_str(ready_line.trim())
                .map_err(|e| AppError::Internal(format!("worker startup JSON parse error: {}", e)))?;

            if startup.status != "ready" {
                return Err(AppError::WorkerCrashed(format!(
                    "worker {} startup failed: {:?}",
                    worker_id, startup.message
                )));
            }

            info!("Worker {} for {} v{} ready (pid={:?})", worker_id, model_name, version, child.id());

            // Drain stdout so the worker does not get SIGPIPE/BrokenPipeError
            // if anything writes to stdout after the ready signal.
            tokio::spawn(async move {
                let mut discard = [0u8; 1024];
                let mut stdout = stdout;
                loop {
                    match stdout.read(&mut discard).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            });

            // Start stderr logger
            let model_name_clone = model_name.to_string();
            let version_clone = version.to_string();
            let worker_id_clone = worker_id;
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut buf = Vec::with_capacity(1024);
                loop {
                    buf.clear();
                    match reader.read_until(b'\n', &mut buf).await {
                        Ok(0) => {
                            tracing::debug!(worker_id = worker_id_clone, "Worker stderr EOF");
                            break;
                        }
                        Ok(_) => {
                            // Strip trailing newline / carriage-return
                            while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                                buf.pop();
                            }
                            let line = String::from_utf8_lossy(&buf);
                            let trimmed = line.trim();
                            if trimmed.starts_with("[ERROR]") {
                                tracing::error!(worker_id = worker_id_clone, model = %model_name_clone, version = %version_clone, "{}", trimmed.strip_prefix("[ERROR]").unwrap_or(trimmed).trim());
                            } else if trimmed.starts_with("[WARN]") {
                                tracing::warn!(worker_id = worker_id_clone, model = %model_name_clone, version = %version_clone, "{}", trimmed.strip_prefix("[WARN]").unwrap_or(trimmed).trim());
                            } else {
                                tracing::info!(worker_id = worker_id_clone, model = %model_name_clone, version = %version_clone, "{}", trimmed);
                            }
                        }
                        Err(e) => {
                            tracing::error!(worker_id = worker_id_clone, "Worker stderr read error: {}", e);
                            break;
                        }
                    }
                }
            });

            // Create ZMQ client (binds the socket, worker connects)
            let zmq_client = Arc::new(WorkerZmqClient::new(endpoint.clone()));
            zmq_clients_for_model.push(zmq_client.clone());

            let info = WorkerInfo {
                worker_id: worker_id as u32,
                device,
                endpoint: endpoint.clone(),
                pid: child.id(),
                status: WorkerStatus::Ready,
            };
            worker_infos.push(info);

            worker_processes.push(WorkerProcess {
                worker_id: worker_id as u32,
                model_name: model_name.to_string(),
                version: version.to_string(),
                child,
                endpoint,
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
            .register_model(model_name, version, &model_config, worker_infos, zmq_clients_for_model.clone());

        {
            let mut workers = self.workers.write().await;
            let mut clients = self.zmq_clients.write().await;
            let key = format!("{}_{}", model_name, version);
            workers.insert(key.clone(), worker_processes);
            clients.insert(key, zmq_clients_for_model);
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
            let mut clients = self.zmq_clients.write().await;
            if let Some(mut procs) = workers.remove(&key) {
                for mut proc in procs.drain(..) {
                    // Clean up ZMQ socket file
                    let socket_str = proc.endpoint.strip_prefix("ipc://").unwrap_or(&proc.endpoint);
                    let socket_path = std::path::Path::new(socket_str);
                    let _ = tokio::fs::remove_file(socket_path).await;
                    let _ = proc.child.kill().await;
                    let _ = proc.child.wait().await;
                }
            }
            clients.remove(&key);
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

/// Pick a random worker index.
pub fn pick_worker_random(num_workers: usize) -> usize {
    use rand::Rng;
    rand::thread_rng().gen_range(0..num_workers.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_worker_random_single() {
        // With 1 worker, always returns 0
        for _ in 0..100 {
            assert_eq!(pick_worker_random(1), 0);
        }
    }

    #[test]
    fn test_pick_worker_random_zero_treated_as_one() {
        // 0 workers should still return 0 (max(1) fallback)
        assert_eq!(pick_worker_random(0), 0);
    }

    #[test]
    fn test_pick_worker_random_distribution() {
        // With multiple workers, all should be picked at least once
        let n = 4;
        let mut seen = vec![false; n];
        for _ in 0..1000 {
            let idx = pick_worker_random(n);
            assert!(idx < n, "idx {} >= num_workers {}", idx, n);
            seen[idx] = true;
        }
        assert!(seen.iter().all(|&s| s), "not all workers were picked");
    }
}
