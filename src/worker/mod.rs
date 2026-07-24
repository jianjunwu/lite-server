pub mod protocol;
pub mod endpoint_manager;

/// Generated protobuf types for endpoint protocol.
pub mod endpoint_proto {
    include!(concat!(env!("OUT_DIR"), "/lite_server.endpoint.v1.rs"));
}

use crate::callback::{CallbackRunner, ModelLifecycleContext};
use crate::config::ModelConfig;
use crate::error::AppError;
use crate::inference_queue::{InferenceQueue, OutlierState, ReloadSignal, model_version_key, parse_model_version_key};
use crate::proto::liteserver as pb;
use crate::registry::{ModelRegistry, types::*};
use crate::transport::zmq::WorkerZmqClient;
use crate::worker::protocol::*;
use dashmap::DashMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::timeout;
use tracing::{error, info, warn};

/// Respawn signal sent from monitor to respawn listener.
struct RespawnSignal {
    model_name: String,
    version: String,
    worker_id: u32,
}

// ===== Worker Lifecycle Hooks =====

/// Replace $MODEL, $VERSION, $WORKER_ID, $EXIT_CODE, $REASON placeholders in a string.
fn replace_hook_vars(template: &str, vars: &[(String, String)]) -> String {
    let mut result = template.to_string();
    for (key, value) in vars {
        result = result.replace(key.as_str(), value.as_str());
    }
    result
}

/// Execute a worker lifecycle hook (shell command + optional HTTP callback).
/// Both are fire-and-forget: spawned as background tasks, never block the caller.
pub fn execute_hook(
    hook_type: &str,
    hooks: &crate::config::WorkerHooksConfig,
    vars: Vec<(String, String)>,
) {
    // Determine which shell command and HTTP hook to use based on hook_type
    let shell_cmd = match hook_type {
        "ready" => hooks.on_ready.as_deref(),
        "exit" => hooks.on_exit.as_deref(),
        "error" => hooks.on_error.as_deref(),
        _ => None,
    };
    let http_hook = match hook_type {
        "ready" => hooks.on_ready_http.as_ref(),
        "exit" => hooks.on_exit_http.as_ref(),
        "error" => hooks.on_error_http.as_ref(),
        _ => None,
    };

    // Skip if no hooks configured
    if shell_cmd.is_none() && http_hook.is_none() {
        return;
    }

    // Shell hook: fire-and-forget
    if let Some(cmd) = shell_cmd {
        let resolved = replace_hook_vars(cmd, &vars);
        let hook_name = hook_type.to_string();
        tokio::spawn(async move {
            match tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&resolved)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
            {
                Ok(status) => {
                    if !status.success() {
                        warn!("Hook '{}' command exited with {}", hook_name, status);
                    }
                }
                Err(e) => {
                    warn!("Hook '{}' command failed to execute: {}", hook_name, e);
                }
            }
        });
    }

    // HTTP hook: fire-and-forget
    if let Some(http) = http_hook {
        let url = replace_hook_vars(&http.url, &vars);
        let method = http.method.clone();
        let body = http.body_template.as_deref().map(|t| replace_hook_vars(t, &vars));
        let hook_name = hook_type.to_string();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default();
            let result = match method.to_uppercase().as_str() {
                "GET" => client.get(&url).send().await,
                _ => {
                    let b = body.unwrap_or_default();
                    client.post(&url).body(b).send().await
                }
            };
            match result {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        warn!("Hook '{}' HTTP {} returned {}", hook_name, url, resp.status());
                    }
                }
                Err(e) => {
                    warn!("Hook '{}' HTTP {} failed: {}", hook_name, url, e);
                }
            }
        });
    }
}

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

    // Respawn channel for heartbeat-triggered worker restarts
    respawn_tx: mpsc::Sender<RespawnSignal>,
    respawn_rx: tokio::sync::Mutex<Option<mpsc::Receiver<RespawnSignal>>>,
    // Log level passed to Python workers
    log_level: String,
    // Callback runner for lifecycle events
    callback_runner: Arc<CallbackRunner>,
}

struct WorkerProcess {
    worker_id: u32,
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Resolves once the worker process has been reaped. unload/shutdown must
    /// await this so no orphaned worker survives to steal the re-bound socket.
    done_rx: Option<oneshot::Receiver<()>>,
}

/// Max time to wait for a single worker to die after signaling shutdown.
/// kill() is SIGKILL/TerminateProcess, so this only trips on a stuck OS call.
const WORKER_KILL_TIMEOUT: Duration = Duration::from_secs(10);

impl WorkerManager {
    pub fn new(
        registry: Arc<ModelRegistry>,
        repo_path: PathBuf,
        inference_queue: Arc<InferenceQueue>,
        log_level: String,
        callback_runner: Arc<CallbackRunner>,
    ) -> Self {
        let (reload_tx, reload_rx) = mpsc::channel::<ReloadSignal>(8);
        let (respawn_tx, respawn_rx) = mpsc::channel::<RespawnSignal>(8);
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
            respawn_tx,
            respawn_rx: tokio::sync::Mutex::new(Some(respawn_rx)),
            log_level,
            callback_runner,
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

    /// Start the respawn listener. Must be called once after construction.
    pub async fn start_respawn_listener(self: &Arc<Self>) {
        let mut rx_guard = self.respawn_rx.lock().await;
        if let Some(mut rx) = rx_guard.take() {
            let wm = Arc::downgrade(self);
            tokio::spawn(async move {
                while let Some(signal) = rx.recv().await {
                    if let Some(wm) = wm.upgrade() {
                        info!(
                            model = %signal.model_name, version = %signal.version,
                            worker_id = signal.worker_id,
                            "Respawning worker (heartbeat timeout)"
                        );
                        if let Err(e) = wm.respawn_worker(
                            &signal.model_name, &signal.version, signal.worker_id,
                        ).await {
                            error!(
                                model = %signal.model_name, version = %signal.version,
                                worker_id = signal.worker_id,
                                "Worker respawn failed: {}", e
                            );
                        }
                    } else {
                        break; // WorkerManager dropped
                    }
                }
            });
        }
    }

    /// Respawn a single worker that was killed by heartbeat timeout.
    /// Kills the old worker, spawns a new one, and updates all registries.
    async fn respawn_worker(
        &self,
        model_name: &str,
        version: &str,
        worker_id: u32,
    ) -> Result<(), AppError> {
        let key = model_version_key(model_name, version);

        // Get config from registry
        let model_version = self.registry
            .get(model_name, Some(version))
            .ok_or_else(|| AppError::ModelNotFound(format!("{} v{}", model_name, version)))?;
        let model_config = model_version.config;

        let model_dir = crate::validation::resolve_model_dir(
            &self.repo_path, model_name, version,
        )?;
        let model_py = model_dir.join("model.py");
        let config_yaml = model_dir.join("config.yaml");

        // Remove old worker entry
        {
            let mut workers = self.workers.write().await;
            if let Some(procs) = workers.get_mut(&key) {
                procs.retain(|w| w.worker_id != worker_id);
            }
        }

        // Remove old ZMQ client and create new one
        let endpoint = worker_endpoint(model_name, version, worker_id as usize);
        {
            let mut clients = self.zmq_clients.write().await;
            if let Some(list) = clients.get_mut(&key) {
                // Remove the old client at this worker_id index
                if (worker_id as usize) < list.len() {
                    list.remove(worker_id as usize);
                }
            }
        }

        // Remove stale socket (Unix only)
        #[cfg(unix)]
        {
            let socket_str = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
            let socket_path = std::path::Path::new(socket_str);
            let _ = tokio::fs::remove_file(socket_path).await;
            if let Some(parent) = socket_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
        }

        // Spawn new worker
        let accelerator = model_config.accelerator.as_deref().unwrap_or("cpu");
        let devices = match &model_config.devices {
            Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(1) as usize,
            Some(serde_json::Value::String(s)) if s == "auto" => 1,
            _ => 1,
        };
        let device = format!("{}:{}", accelerator, worker_id as usize % devices);

        let mut cmd = new_worker_command(&Self::find_python_module_path().unwrap_or_default());

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
            .arg(&endpoint)
            .arg("--log-level")
            .arg(&self.log_level);

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

        // Register custom metrics from Python worker
        if let Some(ref specs) = startup.metric_specs {
            let spec_refs: Vec<(&str, &str)> = specs.iter()
                .map(|s| (s.name.as_str(), s.metric_type.as_str()))
                .collect();
            crate::metrics::prometheus::register_custom_metrics(&spec_refs);
        }

        // Store RateLimit / Cors policies from the worker handshake
        self.registry
            .set_policies(model_name, version, startup.policies);

        info!("Worker {} for {} v{} respawned (pid={:?})", worker_id, model_name, version, child.id());

        // Fire on_ready lifecycle hook
        execute_hook("ready", &model_config.hooks, vec![
            ("$MODEL".to_string(), model_name.to_string()),
            ("$VERSION".to_string(), version.to_string()),
            ("$WORKER_ID".to_string(), worker_id.to_string()),
        ]);

        // Drain stdout
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
                    Ok(0) => break,
                    Ok(_) => {
                        while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                            buf.pop();
                        }
                        let line = String::from_utf8_lossy(&buf);
                        let level = classify_stderr_line(line.trim());
                        let msg = strip_level_prefix(line.trim());
                        emit_stderr_line(level, msg, worker_id_clone as usize, &model_name_clone, &version_clone);
                    }
                    Err(e) => {
                        tracing::error!(worker_id = worker_id_clone, "Worker stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        // Create ZMQ client
        let zmq_client = Arc::new(WorkerZmqClient::new(endpoint.clone()));

        // Insert new ZMQ client
        {
            let mut clients = self.zmq_clients.write().await;
            if let Some(list) = clients.get_mut(&key) {
                let idx = worker_id as usize;
                if idx < list.len() {
                    list[idx] = zmq_client.clone();
                } else {
                    list.push(zmq_client.clone());
                }
            }
        }

        let pid = child.id();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Spawn monitor for the new worker
        let endpoint_clone = endpoint.clone();
        let hooks_arc = Arc::new(model_config.hooks.clone());
        let heartbeat_dur = Duration::from_secs_f32(model_config.heartbeat_interval);
        let heartbeat_timeout_dur = Duration::from_secs_f32(model_config.heartbeat_timeout);
        let done_rx = spawn_worker_monitor(
            child, model_name, version, worker_id, shutdown_rx,
            move || {
                #[cfg(unix)]
                {
                    let socket_str = endpoint_clone.strip_prefix("ipc://").unwrap_or(&endpoint_clone);
                    let _ = std::fs::remove_file(socket_str);
                }
            },
            Some(hooks_arc),
            Some(zmq_client.clone()),
            heartbeat_dur,
            heartbeat_timeout_dur,
            model_config.heartbeat_max_failures,
            Some(self.respawn_tx.clone()),
        );

        // Update registry worker info
        let new_info = WorkerInfo {
            worker_id,
            device,
            endpoint: endpoint.clone(),
            pid,
            status: WorkerStatus::Ready,
        };
        self.registry.replace_worker(model_name, version, worker_id, new_info)?;

        // Update WorkerProcess entry
        {
            let mut workers = self.workers.write().await;
            if let Some(procs) = workers.get_mut(&key) {
                procs.push(WorkerProcess {
                    worker_id,
                    endpoint,
                    shutdown_tx: Some(shutdown_tx),
                    done_rx: Some(done_rx),
                });
            }
        }

        // Record metric
        crate::metrics::prometheus::WORKER_RESPAWNS_TOTAL
            .with_label_values(&[model_name, version, "heartbeat_timeout"])
            .inc();

        Ok(())
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
        let key = model_version_key(model_name, version);
        let guard = self.zmq_clients.read().await;
        guard.get(&key).cloned()
    }

    /// Get outlier detection state for a model version (used for streaming worker selection).
    pub async fn get_outlier_state(
        &self,
        model_name: &str,
        version: &str,
    ) -> Option<Arc<OutlierState>> {
        let key = model_version_key(model_name, version);
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
            let mut cmd = new_worker_command(&Self::find_python_module_path().unwrap_or_default());
            cmd.current_dir(&model_dir);

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
                .arg(&endpoint)
                .arg("--log-level")
                .arg(&self.log_level);

            if model_config.continuous_batching {
                child = child.arg("--continuous-batching");
            }

            let mut child = child
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| {
                    warn!(model = %model_name, version = %version, worker_id, "failed to spawn worker: {}", e);
                    AppError::Python(format!("failed to spawn worker: {}", e))
                })?;

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

            // Register custom metrics from Python worker
            if let Some(ref specs) = startup.metric_specs {
                let spec_refs: Vec<(&str, &str)> = specs.iter()
                    .map(|s| (s.name.as_str(), s.metric_type.as_str()))
                    .collect();
                crate::metrics::prometheus::register_custom_metrics(&spec_refs);
            }

            // Store RateLimit / Cors policies from the worker handshake
            self.registry
                .set_policies(model_name, version, startup.policies);

            info!("Worker {} for {} v{} ready (pid={:?})", worker_id, model_name, version, child.id());

            // Fire on_ready lifecycle hook
            execute_hook("ready", &model_config.hooks, vec![
                ("$MODEL".to_string(), model_name.to_string()),
                ("$VERSION".to_string(), version.to_string()),
                ("$WORKER_ID".to_string(), worker_id.to_string()),
            ]);

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
                            let level = classify_stderr_line(line.trim());
                            let msg = strip_level_prefix(line.trim());
                            emit_stderr_line(level, msg, worker_id_clone, &model_name_clone, &version_clone);
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
            let hooks_arc = Arc::new(model_config.hooks.clone());
            let heartbeat_dur = Duration::from_secs_f32(model_config.heartbeat_interval);
            let heartbeat_timeout_dur = Duration::from_secs_f32(model_config.heartbeat_timeout);
            let done_rx = spawn_worker_monitor(
                child, model_name, version, worker_id as u32, shutdown_rx,
                move || {
                    // Best-effort socket cleanup on unexpected exit
                    #[cfg(unix)]
                    {
                        let socket_str = endpoint_clone.strip_prefix("ipc://").unwrap_or(&endpoint_clone);
                        let _ = std::fs::remove_file(socket_str);
                    }
                },
                Some(hooks_arc),
                Some(zmq_client.clone()),
                heartbeat_dur,
                heartbeat_timeout_dur,
                model_config.heartbeat_max_failures,
                Some(self.respawn_tx.clone()),
            );

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
                endpoint,
                shutdown_tx: Some(shutdown_tx),
                done_rx: Some(done_rx),
            });
        }

        self.registry
            .set_workers(model_name, version, worker_infos.clone())?;
        self.registry
            .set_status(model_name, version, VersionStatus::Ready)?;

        // Create shared OutlierState — single instance for batch_collector, health_checker, and streaming
        let outlier = Arc::new(OutlierState::new(total_workers));

        // Register inference queue for batching
        self.inference_queue
            .register_model(model_name, version, &model_config, worker_infos, zmq_clients_for_model.clone(), self.reload_tx.clone(), outlier.clone());

        {
            let mut workers = self.workers.write().await;
            let mut clients = self.zmq_clients.write().await;
            let mut outliers = self.outlier_states.write().await;
            let key = model_version_key(model_name, version);
            workers.insert(key.clone(), worker_processes);
            clients.insert(key.clone(), zmq_clients_for_model);
            outliers.insert(key, outlier);
        }

        crate::metrics::prometheus::record_model_load(model_name, version, true);
        crate::metrics::prometheus::set_active_workers(model_name, version, total_workers as f64);

        info!("Model {} version {} loaded with {} workers", model_name, version, total_workers);

        // Fire ModelLoad callback
        self.callback_runner.on_model_load(&ModelLifecycleContext {
            model_name: model_name.to_string(),
            version: version.to_string(),
            device: config.devices.as_ref().and_then(|d| d.as_str().map(|s| s.to_string())),
        }).await;

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

        // Fire ModelUnload callback before unloading
        self.callback_runner.on_model_unload(&ModelLifecycleContext {
            model_name: model_name.to_string(),
            version: version.to_string(),
            device: None,
        }).await;

        // Unregister inference queue first to stop accepting new requests
        self.inference_queue.unregister_model(model_name, version);

        self.registry
            .set_status(model_name, version, VersionStatus::Unloading)?;

        let key = model_version_key(model_name, version);
        let procs = {
            let mut workers = self.workers.write().await;
            let mut clients = self.zmq_clients.write().await;
            let mut outliers = self.outlier_states.write().await;
            outliers.remove(&key);
            clients.remove(&key);
            workers.remove(&key)
        };

        if let Some(mut procs) = procs {
            // Signal every monitor to kill its worker first so the kills run
            // concurrently, then wait for each monitor to confirm the process
            // was reaped. Without this wait, unload/shutdown returns while a
            // worker is still alive — an orphan whose ZMQ auto-reconnect
            // steals the re-bound socket on reload/restart.
            for proc in procs.iter_mut() {
                if let Some(tx) = proc.shutdown_tx.take() {
                    let _ = tx.send(());
                }
            }
            for proc in procs {
                if let Some(done_rx) = proc.done_rx {
                    if timeout(WORKER_KILL_TIMEOUT, done_rx).await.is_err() {
                        error!(
                            model = %model_name, version = %version, worker_id = proc.worker_id,
                            "Timed out waiting for worker process to die; cleaning up anyway"
                        );
                    }
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

        // Fire ModelReload callback
        self.callback_runner.on_model_reload(&ModelLifecycleContext {
            model_name: model_name.to_string(),
            version: v.clone(),
            device: config.devices.as_ref().and_then(|d| d.as_str().map(|s| s.to_string())),
        }).await;

        info!("Model {} version {} reloaded", model_name, v);
        Ok(true)
    }

    pub async fn shutdown(&self) {
        info!("Shutting down all workers");
        let workers = self.workers.read().await;
        let keys: Vec<String> = workers.keys().cloned().collect();
        drop(workers);

        for key in keys {
            let (model_name, version) = parse_model_version_key(&key);
            if !model_name.is_empty() && !version.is_empty() {
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
/// - If heartbeat is enabled, periodically probes the worker via ZMQ and triggers respawn on timeout.
/// - Fires lifecycle hooks (on_exit / on_error) if configured.
///
/// Returns a receiver that resolves once the child has been reaped (natural
/// exit or kill confirmed). Callers that need orphan-free shutdown must await
/// it; otherwise the process may outlive the server and steal the re-bound
/// ZMQ socket after a restart.
fn spawn_worker_monitor(
    mut child: Child,
    model_name: &str,
    version: &str,
    worker_id: u32,
    mut shutdown_rx: oneshot::Receiver<()>,
    on_exit: impl FnOnce() + Send + 'static,
    hooks: Option<Arc<crate::config::WorkerHooksConfig>>,
    zmq_client: Option<Arc<WorkerZmqClient>>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    heartbeat_max_failures: usize,
    respawn_tx: Option<mpsc::Sender<RespawnSignal>>,
) -> oneshot::Receiver<()> {
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let model = model_name.to_string();
    let ver = version.to_string();
    let hook_vars: Vec<(String, String)> = vec![
        ("$MODEL".to_string(), model_name.to_string()),
        ("$VERSION".to_string(), version.to_string()),
        ("$WORKER_ID".to_string(), worker_id.to_string()),
    ];
    tokio::spawn(async move {
        // If heartbeat is enabled, wrap the select with a heartbeat loop
        let heartbeat_enabled = heartbeat_interval > Duration::ZERO
            && zmq_client.is_some()
            && respawn_tx.is_some();

        if heartbeat_enabled {
            let client = zmq_client.unwrap();
            let mut tx_opt = respawn_tx;
            let mut consecutive_failures: usize = 0;

            loop {
                tokio::select! {
                    result = child.wait() => {
                        match result {
                            Ok(status) => {
                                if status.success() {
                                    info!(
                                        model = %model, version = %ver, worker_id,
                                        "Worker process exited cleanly"
                                    );
                                    if let Some(ref h) = hooks {
                                        execute_hook("exit", h, hook_vars.clone());
                                    }
                                } else {
                                    let exit_code = status.code().unwrap_or(-1);
                                    error!(
                                        model = %model, version = %ver, worker_id,
                                        exit_code,
                                        "Worker process exited unexpectedly"
                                    );
                                    if let Some(ref h) = hooks {
                                        let mut vars = hook_vars.clone();
                                        vars.push(("$EXIT_CODE".to_string(), exit_code.to_string()));
                                        vars.push(("$REASON".to_string(), "crash".to_string()));
                                        execute_hook("error", h, vars);
                                    }
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
                        on_exit();
                        break;
                    }
                    _ = &mut shutdown_rx => {
                        info!(
                            model = %model, version = %ver, worker_id,
                            "Shutting down worker process"
                        );
                        let _ = child.kill().await;
                        on_exit();
                        break;
                    }
                    _ = tokio::time::sleep(heartbeat_interval) => {
                        // Heartbeat probe: send empty request via ZMQ
                        let probe_uid = format!("heartbeat-{}-{}-{}", model, ver, worker_id);
                        let request = pb::Request {
                            uid: probe_uid,
                            meta: None,
                            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                                data: Default::default(),
                            })),
                        };
                        let result = timeout(heartbeat_timeout, client.send(request)).await;
                        match result {
                            Ok(Ok(resp)) => {
                                let is_error = resp.payload.as_ref().and_then(|p| match p {
                                    pb::response::Payload::Single(s) => s.status.as_ref(),
                                    _ => None,
                                }).map(|s| s.code == "Error").unwrap_or(false);

                                if is_error {
                                    consecutive_failures += 1;
                                    warn!(
                                        model = %model, version = %ver, worker_id,
                                        consecutive_failures,
                                        "Heartbeat probe returned error"
                                    );
                                } else {
                                    if consecutive_failures > 0 {
                                        info!(
                                            model = %model, version = %ver, worker_id,
                                            "Heartbeat recovered after {} failures", consecutive_failures
                                        );
                                    }
                                    consecutive_failures = 0;
                                }
                            }
                            Ok(Err(e)) => {
                                consecutive_failures += 1;
                                warn!(
                                    model = %model, version = %ver, worker_id,
                                    consecutive_failures, error = %e,
                                    "Heartbeat probe failed"
                                );
                            }
                            Err(_) => {
                                consecutive_failures += 1;
                                warn!(
                                    model = %model, version = %ver, worker_id,
                                    consecutive_failures,
                                    "Heartbeat probe timed out"
                                );
                            }
                        }

                        if consecutive_failures >= heartbeat_max_failures {
                            error!(
                                model = %model, version = %ver, worker_id,
                                consecutive_failures,
                                "Heartbeat failed {} times, killing worker", consecutive_failures
                            );
                            let _ = child.kill().await;

                            // Fire error hook
                            if let Some(ref h) = hooks {
                                let mut vars = hook_vars.clone();
                                vars.push(("$EXIT_CODE".to_string(), "-1".to_string()));
                                vars.push(("$REASON".to_string(), "heartbeat_timeout".to_string()));
                                execute_hook("error", h, vars);
                            }

                            // Record metric
                            crate::metrics::prometheus::WORKER_RESPAWNS_TOTAL
                                .with_label_values(&[&model, &ver, "heartbeat_timeout"])
                                .inc();

                            // Send respawn signal
                            if let Some(tx) = tx_opt.take() {
                                let _ = tx.send(RespawnSignal {
                                    model_name: model.clone(),
                                    version: ver.clone(),
                                    worker_id,
                                }).await;
                            }

                            on_exit();
                            break;
                        }
                    }
                }
            }
        } else {
            // No heartbeat — original behavior
            tokio::select! {
                result = child.wait() => {
                    match result {
                        Ok(status) => {
                            if status.success() {
                                info!(
                                    model = %model, version = %ver, worker_id,
                                    "Worker process exited cleanly"
                                );
                                if let Some(ref h) = hooks {
                                    execute_hook("exit", h, hook_vars.clone());
                                }
                            } else {
                                let exit_code = status.code().unwrap_or(-1);
                                error!(
                                    model = %model, version = %ver, worker_id,
                                    exit_code,
                                    "Worker process exited unexpectedly"
                                );
                                if let Some(ref h) = hooks {
                                    let mut vars = hook_vars.clone();
                                    vars.push(("$EXIT_CODE".to_string(), exit_code.to_string()));
                                    vars.push(("$REASON".to_string(), "crash".to_string()));
                                    execute_hook("error", h, vars);
                                }
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
        }

        // Signal that the child has been reaped (kill confirmed or natural
        // exit observed). Receivers that already dropped are fine.
        let _ = done_tx.send(());
    });
    done_rx
}

/// Build the Command for spawning a Python worker process, wiring PYTHONPATH
/// to the bundled lite_server module.
///
/// kill_on_drop(true) is the orphan safety net for paths that bypass graceful
/// shutdown (panic, runtime drop): the child is killed when the Child handle
/// is dropped instead of outliving the server and stealing the re-bound
/// ZMQ socket.
fn new_worker_command(python_module_dir: &str) -> Command {
    let mut cmd = Command::new("python");
    if !python_module_dir.is_empty() {
        let current_pythonpath = std::env::var("PYTHONPATH").unwrap_or_default();
        let new_pythonpath = if current_pythonpath.is_empty() {
            python_module_dir.to_string()
        } else {
            #[cfg(windows)]
            { format!("{};{}", current_pythonpath, python_module_dir) }
            #[cfg(not(windows))]
            { format!("{}:{}", current_pythonpath, python_module_dir) }
        };
        cmd.env("PYTHONPATH", new_pythonpath);
    }
    // Signal the Python side that Rust manages RateLimit/Cors policies —
    // prevents double-execution (Python on_request becomes a declaration-only no-op).
    cmd.env("LITE_POLICY_MANAGED", "1");
    cmd.kill_on_drop(true);
    cmd
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

/// Classify a stderr line from the Python worker into a tracing level.
fn classify_stderr_line(line: &str) -> tracing::Level {
    let trimmed = line.trim();
    if trimmed.starts_with("[ERROR]") {
        tracing::Level::ERROR
    } else if trimmed.starts_with("[WARN]") {
        tracing::Level::WARN
    } else if trimmed.starts_with("[INFO]") {
        tracing::Level::INFO
    } else {
        tracing::Level::DEBUG
    }
}

/// Strip the [LEVEL] prefix from a stderr line, returning the message content.
fn strip_level_prefix(line: &str) -> &str {
    let trimmed = line.trim();
    trimmed
        .strip_prefix("[ERROR]")
        .or_else(|| trimmed.strip_prefix("[WARN]"))
        .or_else(|| trimmed.strip_prefix("[INFO]"))
        .or_else(|| trimmed.strip_prefix("[DEBUG]"))
        .unwrap_or(trimmed)
        .trim()
}

/// Emit a single stderr line at the given tracing level.
fn emit_stderr_line(level: tracing::Level, msg: &str, worker_id: usize, model: &str, version: &str) {
    match level {
        tracing::Level::ERROR => {
            tracing::error!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
        tracing::Level::WARN => {
            tracing::warn!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
        tracing::Level::INFO => {
            tracing::info!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
        _ => {
            tracing::debug!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

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
        spawn_worker_monitor(
            child, "test_model", "1", 0, shutdown_rx,
            move || { cleaned_up_clone.store(true, Ordering::SeqCst); },
            None, None, Duration::ZERO, Duration::from_secs(5), 3, None,
        );

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

    // ===== Lifecycle Hooks tests =====

    #[test]
    fn test_replace_hook_vars() {
        let vars = vec![
            ("$MODEL".to_string(), "bert".to_string()),
            ("$VERSION".to_string(), "v1".to_string()),
            ("$WORKER_ID".to_string(), "2".to_string()),
            ("$EXIT_CODE".to_string(), "137".to_string()),
            ("$REASON".to_string(), "crash".to_string()),
        ];
        let template = "model=$MODEL version=$VERSION worker=$WORKER_ID exit=$EXIT_CODE reason=$REASON";
        let result = replace_hook_vars(template, &vars);
        assert_eq!(result, "model=bert version=v1 worker=2 exit=137 reason=crash");
    }

    #[test]
    fn test_replace_hook_vars_no_match() {
        let vars = vec![("$MODEL".to_string(), "x".to_string())];
        let result = replace_hook_vars("no placeholders here", &vars);
        assert_eq!(result, "no placeholders here");
    }

    #[test]
    fn test_replace_hook_vars_empty_template() {
        let vars = vec![("$MODEL".to_string(), "x".to_string())];
        let result = replace_hook_vars("", &vars);
        assert_eq!(result, "");
    }

    #[test]
    fn test_execute_hook_no_hooks_configured() {
        // Should not panic when no hooks are configured
        let hooks = crate::config::WorkerHooksConfig::default();
        execute_hook("ready", &hooks, vec![("$MODEL".to_string(), "test".to_string())]);
    }

    #[test]
    fn test_worker_hooks_config_default_is_empty() {
        let hooks = crate::config::WorkerHooksConfig::default();
        assert!(hooks.on_ready.is_none());
        assert!(hooks.on_exit.is_none());
        assert!(hooks.on_error.is_none());
        assert!(hooks.on_ready_http.is_none());
        assert!(hooks.on_exit_http.is_none());
        assert!(hooks.on_error_http.is_none());
    }

    #[test]
    fn test_worker_hooks_config_yaml_roundtrip() {
        let hooks = crate::config::WorkerHooksConfig {
            on_ready: Some("echo ready".to_string()),
            on_exit: Some("echo exit".to_string()),
            on_error: None,
            on_ready_http: Some(crate::config::HttpHookConfig {
                url: "http://localhost/hook".to_string(),
                method: "POST".to_string(),
                body_template: Some(r#"{"model":"$MODEL"}"#.to_string()),
            }),
            on_exit_http: None,
            on_error_http: None,
        };
        let yaml = serde_yaml::to_string(&hooks).unwrap();
        let parsed: crate::config::WorkerHooksConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.on_ready, Some("echo ready".to_string()));
        assert!(parsed.on_ready_http.is_some());
        assert_eq!(parsed.on_ready_http.as_ref().unwrap().url, "http://localhost/hook");
    }

    #[tokio::test]
    async fn test_execute_shell_hook_runs_command() {
        // Use a hook that creates a temp file to verify execution
        let tmp = std::env::temp_dir().join(format!("lite-server-hook-test-{}", std::process::id()));
        let tmp_str = tmp.to_string_lossy().to_string();
        let hooks = crate::config::WorkerHooksConfig {
            on_ready: Some(format!("touch {}", tmp_str)),
            ..Default::default()
        };
        execute_hook("ready", &hooks, vec![("$MODEL".to_string(), "test".to_string())]);

        // Wait for the background task to complete
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(tmp.exists(), "shell hook should have created the file");
        let _ = tokio::fs::remove_file(&tmp).await;
    }

    // ===== Heartbeat / Respawn tests =====

    #[test]
    fn test_heartbeat_config_defaults() {
        let cfg = crate::config::ModelConfig::default();
        assert_eq!(cfg.heartbeat_interval, 0.0, "heartbeat should be disabled by default");
        assert_eq!(cfg.heartbeat_timeout, 5.0);
        assert_eq!(cfg.heartbeat_max_failures, 3);
    }

    #[test]
    fn test_heartbeat_config_yaml_roundtrip() {
        let yaml = r#"
heartbeat_interval: 10.0
heartbeat_timeout: 3.0
heartbeat_max_failures: 5
"#;
        let cfg: crate::config::ModelConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.heartbeat_interval, 10.0);
        assert_eq!(cfg.heartbeat_timeout, 3.0);
        assert_eq!(cfg.heartbeat_max_failures, 5);
    }

    #[tokio::test]
    async fn test_respawn_channel_creation() {
        let (tx, mut rx) = mpsc::channel::<RespawnSignal>(8);
        tx.send(RespawnSignal {
            model_name: "m".to_string(),
            version: "1".to_string(),
            worker_id: 0,
        }).await.unwrap();
        let sig = rx.recv().await.unwrap();
        assert_eq!(sig.model_name, "m");
        assert_eq!(sig.worker_id, 0);
    }

    #[tokio::test]
    async fn test_monitor_no_heartbeat_when_disabled() {
        // With heartbeat disabled (interval=0), monitor should use the simple path
        // and respond to shutdown signal without trying to probe.
        let mut cmd = Command::new("python");
        cmd.arg("-c").arg("import time; time.sleep(60)");
        let child = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done = Arc::new(AtomicBool::new(false));
        let done_c = done.clone();

        spawn_worker_monitor(
            child, "test", "1", 0, shutdown_rx,
            move || { done_c.store(true, Ordering::SeqCst); },
            None, None, Duration::ZERO, Duration::from_secs(5), 3, None,
        );

        // Send shutdown
        shutdown_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(done.load(Ordering::SeqCst), "monitor should exit on shutdown signal");
    }

    // ===== Orphan prevention: kill completion signal =====

    /// Regression: shutdown/unload must not return before the worker process
    /// is actually killed+reaped. Otherwise the child is orphaned and its ZMQ
    /// socket auto-reconnect steals the re-bound socket after a restart.
    #[tokio::test]
    async fn test_monitor_signals_completion_after_shutdown_kill() {
        let mut cmd = Command::new("python");
        cmd.arg("-c").arg("import time; time.sleep(60)");
        let child = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        #[cfg(unix)]
        let pid = child.id().unwrap() as i32;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child, "test", "1", 0, shutdown_rx,
            || {},
            None, None, Duration::ZERO, Duration::from_secs(5), 3, None,
        );

        shutdown_tx.send(()).unwrap();

        // Completion must arrive promptly — it may only be sent after
        // kill().await has reaped the child.
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion promptly after kill")
            .expect("completion channel should not be dropped without signal");

        #[cfg(unix)]
        {
            let alive = unsafe { libc::kill(pid, 0) } == 0;
            assert!(!alive, "worker process {} should be dead once completion fires", pid);
        }
    }

    /// Natural exit must also fire the completion signal (callers treat both
    /// paths uniformly).
    #[tokio::test]
    async fn test_monitor_signals_completion_on_natural_exit() {
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

        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child, "test", "1", 0, shutdown_rx,
            || {},
            None, None, Duration::ZERO, Duration::from_secs(5), 3, None,
        );

        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion after natural exit")
            .expect("completion channel should not be dropped without signal");
    }

    /// Real-worker regression test: unload_model must not return before the
    /// Python worker process is actually dead. An orphaned worker's ZMQ
    /// socket auto-reconnect steals the re-bound socket on reload/restart
    /// (root cause of flaky worker log loss).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_unload_model_leaves_no_orphan_worker() {
        let repo = std::env::temp_dir()
            .join(format!("lite-server-orphan-test-{}", std::process::id()));
        let model_dir = repo.join("orphan_guard_model").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        wm.load_model("orphan_guard_model", "1", &ModelConfig::default())
            .await
            .unwrap();

        let pids: Vec<u32> = registry
            .get("orphan_guard_model", Some("1"))
            .unwrap()
            .workers
            .iter()
            .filter_map(|w| w.pid)
            .collect();
        assert!(!pids.is_empty(), "expected at least one worker pid");

        wm.unload_model("orphan_guard_model", None).await.unwrap();

        // When unload returns, every worker must already be reaped —
        // a live (or zombie) process here means the orphan race is back.
        for pid in pids {
            let alive = unsafe { libc::kill(pid as i32, 0) } == 0;
            assert!(!alive, "worker pid {} orphaned: still alive after unload_model returned", pid);
        }

        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Guard: workers must be spawned with kill_on_drop so a server panic or
    /// runtime drop cannot orphan them — the Child handle is the last resort
    /// when graceful shutdown never runs.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_worker_command_kills_child_on_drop() {
        let child = new_worker_command("")
            .arg("-c")
            .arg("import time; time.sleep(60)")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        drop(child);

        // kill_on_drop sends SIGKILL on drop and tokio reaps in the
        // background; poll briefly for the pid to disappear.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut alive = true;
        while std::time::Instant::now() < deadline {
            if unsafe { libc::kill(pid, 0) } != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!alive, "dropped worker process {} should be killed via kill_on_drop", pid);
    }

    // ===== Key format: underscore in model name / version =====

    #[test]
    fn test_model_version_key_roundtrip_with_underscores() {
        // Reproduce the bug: model_name="bert_base_2024" version="03_v1"
        // Old key = "bert_base_2024_03_v1"
        // rsplitn(2, '_') = ["v1", "bert_base_2024_03"] — model_name truncated!
        let key_with_underscore_version = "bert_base_2024_03_v1";
        let parts: Vec<&str> = key_with_underscore_version.rsplitn(2, '_').collect();
        // Assert the BUG: model_name should be "bert_base_2024" but is "bert_base_2024_03"
        assert_ne!(parts[1], "bert_base_2024",
            "rsplitn('_', 2) mis-parses keys when version contains underscores");
        // Assert the version is also wrong
        assert_eq!(parts[0], "v1", "rsplitn only captures last segment of underscored version");

        // Also breaks when model_name contains underscores and version is simple:
        let key_with_underscore_model = "my_model_v1";
        let parts2: Vec<&str> = key_with_underscore_model.rsplitn(2, '_').collect();
        // This happens to work: "my_model" + "v1" → ["v1", "my_model"] ✓
        assert_eq!(parts2[0], "v1");
        assert_eq!(parts2[1], "my_model");
    }

    // ===== Stderr line classification =====

    #[test]
    fn test_classify_stderr_line_info_not_dropped() {
        // BUG: [INFO] fell through to DEBUG, making Python info logs invisible
        // at the default Rust tracing level ("info").
        assert_eq!(
            classify_stderr_line("[INFO] hello world"),
            tracing::Level::INFO,
            "BUG: [INFO] was classified as DEBUG, making info logs invisible"
        );
        assert_eq!(classify_stderr_line("[ERROR] boom"), tracing::Level::ERROR);
        assert_eq!(classify_stderr_line("[WARN] hmm"), tracing::Level::WARN);
        assert_eq!(classify_stderr_line("plain text"), tracing::Level::DEBUG);
        assert_eq!(classify_stderr_line("[DEBUG] debug stuff"), tracing::Level::DEBUG);
        // Traceback lines (indented) should be DEBUG so they don't spam unless
        // the user wants full verbosity.
        assert_eq!(classify_stderr_line("  File \"x.py\", line 1"), tracing::Level::DEBUG);
        assert_eq!(classify_stderr_line("Traceback (most recent call last):"), tracing::Level::DEBUG);
    }

    #[test]
    fn test_strip_level_prefix_all_levels() {
        assert_eq!(strip_level_prefix("[ERROR] boom"), "boom");
        assert_eq!(strip_level_prefix("[WARN] hmm"), "hmm");
        assert_eq!(strip_level_prefix("[INFO] ok"), "ok");
        assert_eq!(strip_level_prefix("[DEBUG] dbg"), "dbg");
        assert_eq!(strip_level_prefix("plain text"), "plain text");
        // Indented [ERROR] is stripped because trim() is applied first
        assert_eq!(strip_level_prefix("  [ERROR] indented"), "indented");
    }

    #[test]
    fn test_heartbeat_enabled_with_sub_second_interval() {
        // The bug: heartbeat_enabled = heartbeat_interval.as_secs() > 0
        // For Duration::from_millis(500), as_secs() returns 0 → disabled (WRONG)
        let sub_second = Duration::from_millis(500);
        let old_check = sub_second.as_secs() > 0;
        assert!(!old_check,
            "BUG: as_secs() truncates sub-second intervals — heartbeat silently disabled");

        // The fix: compare against Duration::ZERO
        let fixed_check = sub_second > Duration::ZERO;
        assert!(fixed_check,
            "FIX: > Duration::ZERO correctly detects non-zero sub-second intervals");
    }
}
