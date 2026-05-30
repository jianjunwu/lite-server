use crate::config::{Config, OrchestrationConfig, ModelStrategyConfig};
use crate::error::AppError;
use crate::http;
use crate::inference_queue::InferenceQueue;
use crate::metrics::prometheus;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use crate::worker::endpoint_manager::EndpointManager;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};
use tracing::{info, error, warn};

pub struct LiteServer {
    config: Config,
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    inference_queue: Arc<InferenceQueue>,
}

impl LiteServer {
    pub fn new(config: Config) -> Self {
        let repo_path = PathBuf::from(&config.model_repository.path);
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path,
            inference_queue.clone(),
        ));

        Self {
            config,
            registry,
            worker_manager,
            inference_queue,
        }
    }

    pub async fn run(&self) -> Result<(), AppError> {
        // Register prometheus metrics
        if let Err(e) = prometheus::register_metrics() {
            error!("Failed to register metrics: {}", e);
        }

        // Start reload listener for max_requests auto-recycle
        self.worker_manager.start_reload_listener().await;

        // Load initial models
        self.load_initial_models().await?;

        // Start endpoint manager
        let repo_path = PathBuf::from(&self.config.model_repository.path)
            .canonicalize()
            .unwrap_or_else(|_| PathBuf::from(&self.config.model_repository.path));
        let endpoint_manager = Arc::new(EndpointManager::new(repo_path.clone(), self.registry.clone()));
        let endpoint_routes = match endpoint_manager.start().await {
            Ok(()) => endpoint_manager.routes().await,
            Err(e) => {
                warn!("Failed to start endpoint manager: {}", e);
                Vec::new()
            }
        };

        // Start HTTP server
        let http_fut = http::start_http_server(
            self.config.clone(),
            self.registry.clone(),
            self.worker_manager.clone(),
            self.inference_queue.clone(),
            Some(endpoint_manager.clone()),
            endpoint_routes,
        );

        // Start metrics server if enabled
        let metrics_fut = if self.config.metrics.enabled {
            Some(start_metrics_server(
                &self.config.server.host,
                self.config.server.metrics_port,
            ))
        } else {
            None
        };

        // Start gRPC server if enabled
        let grpc_fut = if self.config.grpc.enabled {
            Some(crate::grpc::start_grpc_server(
                self.config.server.host.clone(),
                self.config.server.grpc_port,
                self.registry.clone(),
                self.worker_manager.clone(),
                self.config.features.streaming_metrics,
            ))
        } else {
            None
        };

        // Start timeline sampler
        let registry_for_timeline = self.registry.clone();
        let timeline_handle = tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let models = registry_for_timeline.list_loaded();
                for (name, version, _) in models {
                    crate::metrics::aggregator::TIMELINE.sample(&name, &version).await;
                }
            }
        });

        // Start hot reload watcher
        let (watch_tx, mut watch_rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let watcher_handle = tokio::spawn(start_file_watcher(
            repo_path.clone(),
            self.worker_manager.clone(),
            watch_tx,
        ));

        // Process reload events
        let reload_worker = self.worker_manager.clone();
        let reload_registry = self.registry.clone();
        let reload_handle = tokio::spawn(async move {
            let mut last_reload: std::collections::HashMap<(String, String), Instant> = std::collections::HashMap::new();
            while let Some(paths) = watch_rx.recv().await {
                if let Err(e) = process_watch_events(paths, repo_path.clone(), reload_worker.clone(), reload_registry.clone(), &mut last_reload).await {
                    warn!("Hot reload processing error: {}", e);
                }
            }
        });

        // Wait for shutdown signal
        tokio::select! {
            result = http_fut => {
                if let Err(e) = &result {
                    error!("HTTP server error: {}", e);
                }
                watcher_handle.abort();
                timeline_handle.abort();
                reload_handle.abort();
                let _ = endpoint_manager.shutdown().await;
                result
            }
            result = async {
                if let Some(fut) = metrics_fut {
                    fut.await
                } else {
                    futures::future::pending().await
                }
            } => {
                if let Err(e) = &result {
                    error!("Metrics server error: {}", e);
                }
                watcher_handle.abort();
                timeline_handle.abort();
                reload_handle.abort();
                let _ = endpoint_manager.shutdown().await;
                result
            }
            result = async {
                if let Some(fut) = grpc_fut {
                    fut.await
                } else {
                    futures::future::pending().await
                }
            } => {
                if let Err(e) = &result {
                    error!("gRPC server error: {}", e);
                }
                watcher_handle.abort();
                timeline_handle.abort();
                reload_handle.abort();
                let _ = endpoint_manager.shutdown().await;
                result
            }
            _ = shutdown_signal() => {
                info!("Shutdown signal received");
                watcher_handle.abort();
                timeline_handle.abort();
                reload_handle.abort();
                let _ = endpoint_manager.shutdown().await;
                self.worker_manager.shutdown().await;
                Ok(())
            }
        }
    }

    async fn load_initial_models(&self) -> Result<(), AppError> {
        let repo_path = PathBuf::from(&self.config.model_repository.path);
        let orch = if let Some(orch_path) = find_orchestration(&repo_path) {
            crate::config::load_orchestration(&orch_path).unwrap_or_default()
        } else {
            OrchestrationConfig::default()
        };

        // Apply strategies to registry
        for strategy in &orch.models {
            self.registry.set_strategy(&strategy.name, strategy)?;
        }

        let available = scan_repo_models(&repo_path).await;
        let by_model = group_by_model(available);

        let names_to_load: Vec<String> = match orch.control_mode.as_str() {
            "all" => by_model.keys().cloned().collect(),
            "explicit" | "poll" => orch.load_models.clone(),
            _ => orch.load_models.clone(),
        };

        let strategy_map: HashMap<String, &ModelStrategyConfig> = orch
            .models
            .iter()
            .map(|s| (s.name.clone(), s))
            .collect();

        for name in names_to_load {
            let strategy = strategy_map.get(&name);
            let load_policy = strategy
                .map(|s| s.load_policy.as_str())
                .unwrap_or("explicit");
            let versions_to_load = strategy.map(|s| &s.versions_to_load);
            let default_version = strategy.and_then(|s| s.default_version.clone());

            let models = by_model.get(&name).cloned().unwrap_or_default();
            let mut versions_loaded = Vec::new();
            let max_version = models.iter().map(|m| &m.version).max().cloned();

            for m in &models {
                let version = m.version.clone();
                let should_load = match load_policy {
                    "all" => true,
                    "latest" => Some(&version) == max_version.as_ref(),
                    _ => versions_to_load
                        .map(|v| v.contains(&version))
                        .unwrap_or(true),
                };

                if should_load {
                    if let Err(e) = crate::validation::validate_identifier(&name) {
                        error!("Skipping model with invalid name '{}': {}", name, e);
                        continue;
                    }
                    if let Err(e) = crate::validation::validate_identifier(&version) {
                        error!("Skipping {} version with invalid version '{}': {}", name, version, e);
                        continue;
                    }
                    let config_path = repo_path.join(&name).join(&version).join("config.yaml");
                    let config = crate::config::load_model_config(&config_path).unwrap_or_default();

                    if let Err(e) = self.worker_manager.load_model(&name, &version, &config).await {
                        error!("Failed to load {} version {}: {}", name, version, e);
                    } else {
                        versions_loaded.push(version);
                    }
                }
            }

            // Activate default version
            if let Some(ref dv) = default_version {
                if versions_loaded.contains(dv) {
                    match self.registry.activate_version(&name, dv) {
                        Ok(true) => info!("Activated default version {} for {}", dv, name),
                        Ok(false) => warn!("Failed to activate default version {} for {} (not ready)", dv, name),
                        Err(e) => error!("Error activating default version {} for {}: {}", dv, name, e),
                    }
                }
            } else if !versions_loaded.is_empty() && self.registry.get_active_version(&name).is_none() {
                match self.registry.activate_version(&name, &versions_loaded[0]) {
                    Ok(true) => info!("Activated version {} for {}", versions_loaded[0], name),
                    Ok(false) => warn!("Failed to activate version {} for {} (not ready)", versions_loaded[0], name),
                    Err(e) => error!("Error activating version {} for {}: {}", versions_loaded[0], name, e),
                }
            } else {
                info!("Skipping activation for {}: versions_loaded={:?}, active={:?}", name, versions_loaded, self.registry.get_active_version(&name));
            }
        }

        Ok(())
    }
}

async fn start_metrics_server(host: &str, port: u16) -> Result<(), AppError> {
    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| AppError::Config(format!("invalid metrics address: {}", e)))?;

    let app = axum::Router::new().route("/metrics", axum::routing::get(|| async {
        let body = prometheus::gather_metrics();
        axum::response::Response::builder()
            .header("content-type", "text/plain; charset=utf-8")
            .body(body)
            .expect("metrics response: builder should not fail with string body")
    }));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| AppError::Io(e))?;

    info!("Starting metrics server on {}", addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| AppError::Internal(format!("metrics server error: {}", e)))?;

    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("signal received, starting graceful shutdown");
}

#[cfg(windows)]
async fn shutdown_signal() {
    signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    info!("signal received, starting graceful shutdown");
}

fn find_orchestration(repo_path: &Path) -> Option<PathBuf> {
    let path = repo_path.join("orchestration.yaml");
    if path.exists() {
        Some(path)
    } else {
        None
    }
}

#[derive(Clone)]
struct RepoModel {
    name: String,
    version: String,
    path: String,
    has_config: bool,
    model_type: String,
}

async fn scan_repo_models(repo_path: &Path) -> Vec<RepoModel> {
    let mut models = Vec::new();
    let mut entries = match tokio::fs::read_dir(repo_path).await {
        Ok(e) => e,
        Err(_) => return models,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let model_dir = entry.path();
        if !model_dir.is_dir() {
            continue;
        }
        let model_name = model_dir.file_name().unwrap().to_string_lossy().to_string();

        let mut versions = Vec::new();
        if let Ok(mut version_entries) = tokio::fs::read_dir(&model_dir).await {
            while let Ok(Some(ventry)) = version_entries.next_entry().await {
                let version_dir = ventry.path();
                if !version_dir.is_dir() {
                    continue;
                }
                let version = version_dir.file_name().unwrap().to_string_lossy().to_string();
                let model_py = version_dir.join("model.py");
                let config_yaml = version_dir.join("config.yaml");

                let mut is_ensemble = false;
                if config_yaml.exists() {
                    if let Ok(content) = tokio::fs::read_to_string(&config_yaml).await {
                        is_ensemble = content.contains("ensemble:");
                    }
                }

                if model_py.exists() || is_ensemble {
                    versions.push(RepoModel {
                        name: model_name.clone(),
                        version,
                        path: version_dir.to_string_lossy().to_string(),
                        has_config: config_yaml.exists(),
                        model_type: if is_ensemble { "ensemble".to_string() } else { "litapi".to_string() },
                    });
                }
            }
        }

        models.extend(versions);
    }

    models
}

fn group_by_model(models: Vec<RepoModel>) -> HashMap<String, Vec<RepoModel>> {
    let mut map: HashMap<String, Vec<RepoModel>> = HashMap::new();
    for m in models {
        map.entry(m.name.clone()).or_default().push(m);
    }
    map
}

async fn process_watch_events(
    paths: Vec<PathBuf>,
    repo_path: PathBuf,
    worker_manager: Arc<WorkerManager>,
    registry: Arc<ModelRegistry>,
    last_reload: &mut std::collections::HashMap<(String, String), Instant>,
) -> Result<(), AppError> {
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut models_to_reload: HashSet<(String, String)> = HashSet::new();
    let mut models_to_check_new: Vec<PathBuf> = Vec::new();
    let mut models_to_check_removed: Vec<(String, String)> = Vec::new();

    for path in paths {
        // Skip orchestration.yaml - handled separately if needed
        if path.file_name().map(|n| n == "orchestration.yaml").unwrap_or(false) {
            continue;
        }

        // Skip endpoint files and Python cache
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("_endpoint.py") || name == "__pycache__" || name.ends_with(".pyc") || name.ends_with(".pyo") {
                continue;
            }
        }
        if path.components().any(|c| c.as_os_str() == "__pycache__") {
            continue;
        }

        // Try to extract model name and version from path
        let strip_result = path.strip_prefix(&repo_path);
        if let Ok(relative) = strip_result {
            let components: Vec<std::path::Component> = relative.components().collect();
            if components.len() >= 2 {
                // Expected: model_name/version/file
                if let (Some(model_name), Some(version)) = (
                    components.get(0).and_then(|c| match c {
                        std::path::Component::Normal(s) => s.to_str(),
                        _ => None,
                    }),
                    components.get(1).and_then(|c| match c {
                        std::path::Component::Normal(s) => s.to_str(),
                        _ => None,
                    }),
                ) {
                    if path.is_dir() {
                        // Directory change: could be new model or version
                        if path.join("model.py").exists() || path.join("config.yaml").exists() {
                            models_to_check_new.push(path);
                        }
                    } else if path.exists() {
                        // File change: trigger reload of the containing model
                        if crate::validation::validate_identifier(model_name).is_ok()
                            && crate::validation::validate_identifier(version).is_ok()
                        {
                            models_to_reload.insert((model_name.to_string(), version.to_string()));
                        }
                    } else {
                        // File was deleted
                        if crate::validation::validate_identifier(model_name).is_ok()
                            && crate::validation::validate_identifier(version).is_ok()
                        {
                            models_to_reload.insert((model_name.to_string(), version.to_string()));
                            models_to_check_removed.push((model_name.to_string(), version.to_string()));
                        }
                    }
                }
            }
        }
    }

    // Reload changed models (with 3s cooldown per model/version)
    let cooldown = Duration::from_secs(3);
    for (name, version) in models_to_reload {
        let key = (name.clone(), version.clone());
        if let Some(last) = last_reload.get(&key) {
            if Instant::now().duration_since(*last) < cooldown {
                info!("Hot reload: skipping {} version {} (cooldown)", name, version);
                continue;
            }
        }
        last_reload.insert(key, Instant::now());

        if registry.get(&name, Some(&version)).is_some() {
            info!("Hot reload: reloading {} version {}", name, version);
            if let Err(e) = worker_manager.reload_model(&name, Some(&version)).await {
                warn!("Hot reload failed for {} version {}: {}", name, version, e);
            }
        } else {
            // Model not loaded yet, try to load it
            let config_path = repo_path.join(&name).join(&version).join("config.yaml");
            if config_path.exists() {
                let config = crate::config::load_model_config(&config_path).unwrap_or_default();
                info!("Hot reload: auto-loading new model {} version {}", name, version);
                if let Err(e) = worker_manager.load_model(&name, &version, &config).await {
                    warn!("Hot load failed for {} version {}: {}", name, version, e);
                }
            }
        }
    }

    // Check for removed models
    for (name, version) in models_to_check_removed {
        let version_dir = repo_path.join(&name).join(&version);
        if !version_dir.exists() {
            info!("Hot reload: auto-unloading removed model {} version {}", name, version);
            let _ = worker_manager.unload_model(&name, Some(&version)).await;
        }
    }

    Ok(())
}

// ===== Hot Reload File Watcher =====

async fn start_file_watcher(
    repo_path: PathBuf,
    worker_manager: Arc<WorkerManager>,
    tx: mpsc::Sender<Vec<PathBuf>>,
) {
    use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};

    let (notify_tx, mut notify_rx) = mpsc::channel::<notify::Result<Event>>(1024);

    let mut watcher: RecommendedWatcher = match Watcher::new(
        move |res: notify::Result<Event>| {
            let _ = notify_tx.blocking_send(res);
        },
        Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Failed to create file watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(&repo_path, RecursiveMode::Recursive) {
        error!("Failed to watch repo path: {}", e);
        return;
    }

    info!("File watcher started on {}", repo_path.display());

    // Debounce: collect events over 1.5s windows
    let mut debounce_deadline: Option<Instant> = None;
    let mut pending_paths: Vec<PathBuf> = Vec::new();
    let mut tick = interval(Duration::from_millis(200));

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        for path in event.paths {
                            // Skip UDS sockets, temp files, hidden files, Python cache
                            if path.extension().map(|e| e == "sock").unwrap_or(false) {
                                continue;
                            }
                            if path.file_name()
                                .and_then(|n| n.to_str())
                                .map(|n| n.starts_with('.'))
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            if path.file_name()
                                .and_then(|n| n.to_str())
                                .map(|n| n == "__pycache__" || n.ends_with(".pyc") || n.ends_with(".pyo"))
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            pending_paths.push(path);
                        }
                        debounce_deadline = Some(Instant::now() + Duration::from_millis(2500));
                    }
                    Err(e) => {
                        warn!("Watch error: {}", e);
                    }
                }
            }
            _ = tick.tick() => {
                if let Some(deadline) = debounce_deadline {
                    if Instant::now() >= deadline && !pending_paths.is_empty() {
                        // Deduplicate
                        pending_paths.sort();
                        pending_paths.dedup();
                        let paths: Vec<PathBuf> = pending_paths.drain(..).collect();
                        let _ = tx.send(paths).await;
                        debounce_deadline = None;
                    }
                }
            }
            else => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;

    #[tokio::test]
    async fn test_file_watcher_aborts_cleanly() {
        let tmp_dir = std::env::temp_dir().join(format!("lite-server-fw-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&tmp_dir).await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry,
            tmp_dir.clone(),
            inference_queue,
        ));

        let (tx, _rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx));

        // Give watcher time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        handle.abort();

        // Wait for abort to take effect
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(handle.is_finished());

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_file_watcher_detects_changes() {
        let tmp_dir_raw = std::env::temp_dir().join(format!("lite-server-fw-detect-{}", std::process::id()));
        tokio::fs::create_dir_all(&tmp_dir_raw).await.unwrap();
        let tmp_dir = tmp_dir_raw.canonicalize().unwrap();
        let sub_dir = tmp_dir.join("model").join("1");
        tokio::fs::create_dir_all(&sub_dir).await.unwrap();
        let model_py = sub_dir.join("model.py");
        tokio::fs::write(&model_py, "original").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry,
            tmp_dir.clone(),
            inference_queue,
        ));

        let (tx, mut rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx));

        // Give watcher time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Modify the file
        tokio::fs::write(&model_py, "modified").await.unwrap();

        // Wait for event with timeout
        let paths = tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            rx.recv()
        ).await.expect("Timeout waiting for watcher event").expect("Channel closed");

        // Check if strip_prefix works
        for p in &paths {
            let _stripped = p.strip_prefix(&tmp_dir);
        }

        handle.abort();
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }
}
