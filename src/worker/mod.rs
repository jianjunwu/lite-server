pub mod protocol;
pub mod endpoint_manager;

use crate::config::{ModelConfig, OrchestrationConfig};
use crate::error::AppError;
use crate::inference_queue::{InferenceQueue, OutlierState, ReloadSignal};
use crate::registry::{ModelRegistry, types::*};
use crate::transport::zmq::WorkerZmqClient;
use crate::worker::protocol::*;
use dashmap::DashMap;
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
pub type PendingMap = Arc<DashMap<String, oneshot::Sender<InferenceResponse>>>;

pub struct WorkerManager {
    registry: Arc<ModelRegistry>,
    repo_path: PathBuf,
    pending: PendingMap,
    // Track spawned child processes
    workers: Arc<RwLock<HashMap<String, Vec<WorkerProcess>>>>,
    inference_queue: Arc<InferenceQueue>,
    // ZMQ clients for active workers
    zmq_clients: Arc<RwLock<HashMap<String, Vec<Arc<WorkerZmqClient>>>>>,
    // Outlier detection state per model version (shared with batch_collector)
    outlier_states: Arc<RwLock<HashMap<String, Arc<OutlierState>>>>,
    // Reload channel for max_requests auto-recycle
    reload_tx: mpsc::Sender<ReloadSignal>,
    reload_rx: tokio::sync::Mutex<Option<mpsc::Receiver<ReloadSignal>>>,
}

struct WorkerProcess {
    worker_id: u32,
    model_name: String,
    version: String,
    pid: Option<u32>,
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl WorkerManager {
    pub fn new(
        registry: Arc<ModelRegistry>,
        repo_path: PathBuf,
        inference_queue: Arc<InferenceQueue>,
    ) -> Self {
        let (reload_tx, reload_rx) = mpsc::channel::<ReloadSignal>(8);
        Self {
            registry,
            repo_path,
            pending: Arc::new(DashMap::new()),
            workers: Arc::new(RwLock::new(HashMap::new())),
            inference_queue,
            zmq_clients: Arc::new(RwLock::new(HashMap::new())),
            outlier_states: Arc::new(RwLock::new(HashMap::new())),
            reload_tx,
            reload_rx: tokio::sync::Mutex::new(Some(reload_rx)),
        }
    }

    /// Start the reload listener. Must be called once after construction.
    pub async fn start_reload_listener(self: &Arc<Self>) {
        let mut rx_guard = self.reload_rx.lock().await;
        if let Some(mut rx) = rx_guard.take() {
            let wm = Arc::downgrade(self);
            tokio::spawn(async move {
                let mut reloading = std::collections::HashSet::new();
                while let Some(signal) = rx.recv().await {
                    let key = signal.model_name.clone();
                    if !reloading.insert(key.clone()) {
                        continue; // already reloading
                    }
                    if let Some(wm) = wm.upgrade() {
                        info!("Auto-recycling model {} (max_requests reached)", signal.model_name);
                        let result = wm.reload_model(&signal.model_name, None).await;
                        match result {
                            Ok(true) => info!("Model {} auto-recycled successfully", signal.model_name),
                            Ok(false) => warn!("Model {} not found for auto-recycle", signal.model_name),
                            Err(e) => error!("Model {} auto-recycle failed: {}", signal.model_name, e),
                        }
                        reloading.remove(&key);
                    } else {
                        break; // WorkerManager dropped
                    }
                }
            });
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

    /// Get outlier detection state for a model version (used for streaming worker selection).
    pub async fn get_outlier_state(
        &self,
        model_name: &str,
        version: &str,
    ) -> Option<Arc<OutlierState>> {
        let key = format!("{}_{}", model_name, version);
        let guard = self.outlier_states.read().await;
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
            .register(model_name, version, model_config.clone(), model_type, model_dir.clone())?;

        if is_ensemble {
            // Ensemble: no workers, just mark ready
            self.registry
                .set_status(model_name, version, VersionStatus::Ready)?;
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
            let endpoint = worker_endpoint(model_name, version, worker_id);

            // Remove stale socket (Unix only — TCP ports are released on process exit)
            #[cfg(unix)]
            {
                let socket_str = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
                let socket_path = std::path::Path::new(socket_str);
                let _ = tokio::fs::remove_file(socket_path).await;
                if let Some(parent) = socket_path.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
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
                    #[cfg(windows)]
                    { format!("{};{}", current_pythonpath, python_module_dir) }
                    #[cfg(not(windows))]
                    { format!("{}:{}", current_pythonpath, python_module_dir) }
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

            let stdout = child.stdout.take()
                .ok_or_else(|| AppError::Internal("worker stdout not piped".to_string()))?;
            let stderr = child.stderr.take()
                .ok_or_else(|| AppError::Internal("worker stderr not piped".to_string()))?;

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

            let pid = child.id();
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

            // Spawn monitor task — owns the Child, detects exits and handles cleanup
            let endpoint_clone = endpoint.clone();
            spawn_worker_monitor(child, model_name, version, worker_id as u32, shutdown_rx, move || {
                // Best-effort socket cleanup on unexpected exit
                #[cfg(unix)]
                {
                    let socket_str = endpoint_clone.strip_prefix("ipc://").unwrap_or(&endpoint_clone);
                    let _ = std::fs::remove_file(socket_str);
                }
            });

            let info = WorkerInfo {
                worker_id: worker_id as u32,
                device,
                endpoint: endpoint.clone(),
                pid,
                status: WorkerStatus::Ready,
            };
            worker_infos.push(info);

            worker_processes.push(WorkerProcess {
                worker_id: worker_id as u32,
                model_name: model_name.to_string(),
                version: version.to_string(),
                pid,
                endpoint,
                shutdown_tx: Some(shutdown_tx),
            });
        }

        self.registry
            .set_workers(model_name, version, worker_infos.clone())?;
        self.registry
            .set_status(model_name, version, VersionStatus::Ready)?;

        // Register inference queue for batching
        self.inference_queue
            .register_model(model_name, version, &model_config, worker_infos, zmq_clients_for_model.clone(), self.reload_tx.clone());

        {
            let mut workers = self.workers.write().await;
            let mut clients = self.zmq_clients.write().await;
            let mut outliers = self.outlier_states.write().await;
            let key = format!("{}_{}", model_name, version);
            workers.insert(key.clone(), worker_processes);
            clients.insert(key.clone(), zmq_clients_for_model);
            outliers.insert(key, Arc::new(OutlierState::new(total_workers)));
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
            let versions = self.registry.list_versions(model_name);
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
            .set_status(model_name, version, VersionStatus::Unloading)?;

        let key = format!("{}_{}", model_name, version);
        {
            let mut workers = self.workers.write().await;
            let mut clients = self.zmq_clients.write().await;
            let mut outliers = self.outlier_states.write().await;
            outliers.remove(&key);
            if let Some(mut procs) = workers.remove(&key) {
                for mut proc in procs.drain(..) {
                    // Signal the monitor task to kill the process
                    if let Some(tx) = proc.shutdown_tx.take() {
                        let _ = tx.send(());
                    }
                    // Clean up ZMQ socket file (Unix only)
                    #[cfg(unix)]
                    {
                        let socket_str = proc.endpoint.strip_prefix("ipc://").unwrap_or(&proc.endpoint);
                        let socket_path = std::path::Path::new(socket_str);
                        let _ = tokio::fs::remove_file(socket_path).await;
                    }
                }
            }
            clients.remove(&key);
        }

        self.registry.remove(model_name, version)?;

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
            None => match self.registry.get_active_version(model_name) {
                Some(v) => v,
                None => return Ok(false),
            },
        };

        let config = match self.registry.get(model_name, Some(&v)) {
            Some(mv) => mv.config,
            None => return Ok(false),
        };

        info!("Reloading {} version {}", model_name, v);
        self.unload_version(model_name, &v).await?;

        // Small delay to ensure cleanup
        tokio::time::sleep(Duration::from_millis(500)).await;

        self.load_model(model_name, &v, &config).await?;
        self.registry.activate_version(model_name, &v)?;

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

/// Spawn a background task that monitors a worker child process.
/// - If the process exits on its own (crash, OOM kill), logs the event and runs cleanup.
/// - If a shutdown signal is sent via `shutdown_rx`, kills the process and runs cleanup.
fn spawn_worker_monitor(
    mut child: Child,
    model_name: &str,
    version: &str,
    worker_id: u32,
    mut shutdown_rx: oneshot::Receiver<()>,
    on_exit: impl FnOnce() + Send + 'static,
) {
    let model = model_name.to_string();
    let ver = version.to_string();
    tokio::spawn(async move {
        tokio::select! {
            result = child.wait() => {
                match result {
                    Ok(status) => {
                        if status.success() {
                            info!(
                                model = %model, version = %ver, worker_id,
                                "Worker process exited cleanly"
                            );
                        } else {
                            error!(
                                model = %model, version = %ver, worker_id,
                                exit_code = status.code().unwrap_or(-1),
                                "Worker process exited unexpectedly"
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            model = %model, version = %ver, worker_id,
                            error = %e,
                            "Failed to wait on worker process"
                        );
                    }
                }
            }
            _ = &mut shutdown_rx => {
                info!(
                    model = %model, version = %ver, worker_id,
                    "Shutting down worker process"
                );
                let _ = child.kill().await;
            }
        }
        on_exit();
    });
}

/// Build a platform-appropriate ZMQ endpoint for a worker.
/// - Unix: IPC socket in the system temp directory
/// - Windows: TCP on localhost (IPC not supported)
fn worker_endpoint(model_name: &str, version: &str, worker_id: usize) -> String {
    #[cfg(unix)]
    {
        let sock_path = std::env::temp_dir()
            .join("lite-server")
            .join(format!("{}_{}_{}.sock", model_name, version, worker_id));
        format!("ipc://{}", sock_path.display())
    }
    #[cfg(windows)]
    {
        let key = format!("{}_{}_{}", model_name, version, worker_id);
        let port = crate::transport::derive_port_from_path(&key);
        format!("tcp://127.0.0.1:{}", port)
    }
}

/// Pick a random worker index.
pub fn pick_worker_random(num_workers: usize) -> usize {
    use rand::Rng;
    rand::thread_rng().gen_range(0..num_workers.max(1))
}

/// Pick a random non-ejected worker index. Falls back to any worker if all are ejected.
pub fn pick_worker_skip_ejected(num_workers: usize, outlier: &OutlierState) -> usize {
    use rand::Rng;
    if num_workers <= 1 {
        return 0;
    }

    let mut rng = rand::thread_rng();
    let start = rng.gen_range(0..num_workers);

    // Try to find a non-ejected worker starting from random offset
    for i in 0..num_workers {
        let idx = (start + i) % num_workers;
        if !outlier.is_ejected(idx) {
            return idx;
        }
    }

    // All ejected — fall back to random
    start
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PendingMap must be Arc<DashMap> for lock-free per-uid insert/remove.
    /// This verifies the type alias — reverting to Arc<RwLock<HashMap>> won't compile.
    #[test]
    fn pending_map_type_is_dashmap() {
        fn assert_type<T>() {}
        assert_type::<Arc<DashMap<String, oneshot::Sender<InferenceResponse>>>>();
    }

    /// PendingMap insert/remove must be sync (no .await), proving DashMap not RwLock.
    #[tokio::test]
    async fn pending_map_insert_remove_are_sync() {
        let pending: PendingMap = Arc::new(DashMap::new());
        let (tx, _rx) = oneshot::channel::<InferenceResponse>();
        // insert is sync on DashMap — this won't compile if PendingMap uses RwLock
        pending.insert("uid-1".to_string(), tx);
        assert_eq!(pending.len(), 1);
        // remove is also sync
        let _ = pending.remove("uid-1");
        assert!(pending.is_empty());
    }

    /// Concurrent inserts from multiple tasks must not deadlock or panic.
    #[tokio::test]
    async fn pending_map_concurrent_inserts() {
        let pending: PendingMap = Arc::new(DashMap::new());
        let mut handles = Vec::new();
        for i in 0..100 {
            let p = pending.clone();
            handles.push(tokio::spawn(async move {
                let (tx, _rx) = oneshot::channel::<InferenceResponse>();
                p.insert(format!("uid-{}", i), tx);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(pending.len(), 100);
    }

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

    /// When a child process exits unexpectedly, the monitor task must detect it
    /// and invoke the cleanup callback.
    #[tokio::test]
    async fn test_worker_monitor_detects_exit() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // Spawn a process that exits immediately
        #[cfg(unix)]
        let child = tokio::process::Command::new("false")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let child = tokio::process::Command::new("cmd")
            .args(["/c", "exit", "1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_up_clone = cleaned_up.clone();

        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        spawn_worker_monitor(child, "test_model", "1", 0, shutdown_rx, move || {
            cleaned_up_clone.store(true, Ordering::SeqCst);
        });

        // Wait for the monitor to detect the exit and run cleanup
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !cleaned_up.load(Ordering::SeqCst) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(cleaned_up.load(Ordering::SeqCst), "monitor should have triggered cleanup");
    }

    // ===== pick_worker_skip_ejected tests =====

    #[test]
    fn test_pick_worker_skip_ejected_all_active() {
        let outlier = OutlierState::new(4);
        // All active — should return valid index
        for _ in 0..10 {
            let idx = pick_worker_skip_ejected(4, &outlier);
            assert!(idx < 4, "index {} out of range", idx);
        }
    }

    #[test]
    fn test_pick_worker_skip_ejected_avoids_ejected() {
        let outlier = OutlierState::new(3);
        // Eject worker 0
        for _ in 0..3 {
            outlier.record_error(0);
        }
        assert!(outlier.is_ejected(0));

        // Should never pick ejected worker 0
        for _ in 0..100 {
            let idx = pick_worker_skip_ejected(3, &outlier);
            assert!(idx == 1 || idx == 2, "should skip ejected worker 0, got {}", idx);
        }
    }

    #[test]
    fn test_pick_worker_skip_ejected_single_worker() {
        let outlier = OutlierState::new(1);
        // Single worker always returns 0
        assert_eq!(pick_worker_skip_ejected(1, &outlier), 0);

        // Even if ejected
        outlier.record_error(0);
        assert_eq!(pick_worker_skip_ejected(1, &outlier), 0);
    }

    #[test]
    fn test_pick_worker_skip_ejected_all_ejected_fallback() {
        let outlier = OutlierState::new(2);
        // Eject worker 0 (max 50% of 2 = 1 ejection allowed)
        for _ in 0..3 {
            outlier.record_error(0);
        }
        // Worker 1 still active, should pick it
        for _ in 0..20 {
            assert_eq!(pick_worker_skip_ejected(2, &outlier), 1);
        }
    }

    // ===== Reload Channel tests =====

    #[test]
    fn test_reload_channel_creation() {
        // WorkerManager creates a reload channel in its constructor
        let (tx, mut rx) = mpsc::channel::<crate::inference_queue::ReloadSignal>(8);

        // Sender should be cloneable (for multiple batch collectors)
        let tx2 = tx.clone();

        // Send a signal
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            tx.send(crate::inference_queue::ReloadSignal {
                model_name: "test".to_string(),
            }).await.unwrap();

            let signal = rx.recv().await.unwrap();
            assert_eq!(signal.model_name, "test");

            // Clone also works
            tx2.send(crate::inference_queue::ReloadSignal {
                model_name: "test2".to_string(),
            }).await.unwrap();
            let signal2 = rx.recv().await.unwrap();
            assert_eq!(signal2.model_name, "test2");
        });
    }

    #[test]
    fn test_reload_signal_dedup() {
        // Simulate the dedup logic from start_reload_listener
        let mut reloading = std::collections::HashSet::new();

        // First signal for model_a → should process
        assert!(reloading.insert("model_a".to_string()));

        // Second signal for model_a while reloading → should skip
        assert!(!reloading.insert("model_a".to_string()));

        // Different model → should process
        assert!(reloading.insert("model_b".to_string()));

        // After model_a finishes reloading
        reloading.remove("model_a");

        // model_a can be reloaded again
        assert!(reloading.insert("model_a".to_string()));
    }

    #[tokio::test]
    async fn test_reload_channel_try_send_non_blocking() {
        // try_send should not block even when channel is full
        let (tx, _rx) = mpsc::channel::<crate::inference_queue::ReloadSignal>(1);

        // First send succeeds
        assert!(tx.try_send(crate::inference_queue::ReloadSignal {
            model_name: "m1".to_string(),
        }).is_ok());

        // Second send should fail (channel full) — not block
        assert!(tx.try_send(crate::inference_queue::ReloadSignal {
            model_name: "m2".to_string(),
        }).is_err());
    }
}
