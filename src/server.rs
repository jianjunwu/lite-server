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
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, info, error, warn};

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
            config.logging.level.clone(),
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

        // Start respawn listener for heartbeat-triggered worker restarts
        self.worker_manager.start_respawn_listener().await;

        // Load initial models
        self.load_initial_models().await?;

        // P2: Check if any loaded model has hot_reload enabled
        let has_hot_reload = Arc::new(AtomicBool::new(self.registry_has_hot_reload_models()));
        if has_hot_reload.load(Ordering::Relaxed) {
            info!("Hot reload: enabled models detected, starting file watcher");
        } else {
            info!("Hot reload: no enabled models, file watcher will start on demand");
        }

        // Start endpoint manager
        let repo_path = PathBuf::from(&self.config.model_repository.path)
            .canonicalize()
            .unwrap_or_else(|_| PathBuf::from(&self.config.model_repository.path));
        let ep_dir = self.config.endpoints_dir.as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| repo_path.clone());
        let ep_dir = ep_dir.canonicalize().unwrap_or(ep_dir);
        let endpoint_manager = Arc::new(EndpointManager::new(ep_dir, self.registry.clone()));
        let endpoint_routes = match endpoint_manager.start().await {
            Ok(()) => endpoint_manager.routes().await,
            Err(e) => {
                warn!("Failed to start endpoint manager: {}", e);
                Vec::new()
            }
        };

        // Start HTTP server as spawned task with graceful shutdown channel
        let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
        let mut http_handle = tokio::spawn(http::start_http_server(
            self.config.clone(),
            self.registry.clone(),
            self.worker_manager.clone(),
            self.inference_queue.clone(),
            Some(endpoint_manager.clone()),
            endpoint_routes,
            http_shutdown_rx,
        ));

        // When HTTP uses a Unix socket, gRPC/metrics still need a TCP host.
        let tcp_host = if crate::config::unix_socket_path(&self.config.server.host).is_some() {
            "127.0.0.1".to_string()
        } else {
            self.config.server.host.clone()
        };

        // Start metrics server if enabled
        let mut metrics_handle = if self.config.metrics.enabled {
            Some(tokio::spawn(start_metrics_server(
                tcp_host.clone(),
                self.config.server.metrics_port,
            )))
        } else {
            None
        };

        // Start gRPC server if enabled
        let mut grpc_handle = if self.config.grpc.enabled {
            Some(tokio::spawn(crate::grpc::start_grpc_server(
                tcp_host,
                self.config.server.grpc_port,
                self.registry.clone(),
                self.worker_manager.clone(),
                self.config.features.streaming_metrics,
            )))
        } else {
            None
        };

        // Start timeline sampler (uses list_loaded_keys to avoid cloning ModelVersion)
        let registry_for_timeline = self.registry.clone();
        let timeline_handle = tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let models = registry_for_timeline.list_loaded_keys();
                for (name, version) in models {
                    crate::metrics::aggregator::TIMELINE.sample(&name, &version).await;
                }
            }
        });

        // P2: Start hot reload watcher with on-demand flag
        let (watch_tx, mut watch_rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let has_hot_reload_for_watcher = has_hot_reload.clone();
        let watcher_handle = tokio::spawn(start_file_watcher(
            repo_path.clone(),
            self.worker_manager.clone(),
            watch_tx,
            has_hot_reload_for_watcher,
        ));

        // Process reload events
        let reload_worker = self.worker_manager.clone();
        let reload_registry = self.registry.clone();
        let model_defaults = self.config.model_defaults.clone();
        let has_hot_reload_for_reload = has_hot_reload.clone();
        let reload_handle = tokio::spawn(async move {
            let mut last_reload: std::collections::HashMap<(String, String), Instant> = std::collections::HashMap::new();
            while let Some(paths) = watch_rx.recv().await {
                if let Err(e) = process_watch_events(
                    paths,
                    repo_path.clone(),
                    reload_worker.clone(),
                    reload_registry.clone(),
                    &mut last_reload,
                    &model_defaults,
                    &has_hot_reload_for_reload,
                ).await {
                    warn!("Hot reload processing error: {}", e);
                }
            }
        });

        // Wait for any server to exit or shutdown signal
        let shutdown_reason = tokio::select! {
            result = &mut http_handle => {
                match result {
                    Ok(Ok(())) => "http_server_finished".to_string(),
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(AppError::Internal(format!("HTTP task panicked: {}", e))),
                }
            }
            result = async {
                match metrics_handle.as_mut() {
                    Some(h) => h.await,
                    None => futures::future::pending().await,
                }
            } => {
                match result {
                    Ok(Ok(())) => "metrics_server_finished".to_string(),
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(AppError::Internal(format!("Metrics task panicked: {}", e))),
                }
            }
            result = async {
                match grpc_handle.as_mut() {
                    Some(h) => h.await,
                    None => futures::future::pending().await,
                }
            } => {
                match result {
                    Ok(Ok(())) => "grpc_server_finished".to_string(),
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(AppError::Internal(format!("gRPC task panicked: {}", e))),
                }
            }
            _ = shutdown_signal() => "shutdown_signal".to_string(),
        };

        info!("{} received, starting graceful shutdown", shutdown_reason);

        // Abort background tasks
        watcher_handle.abort();
        timeline_handle.abort();
        reload_handle.abort();

        // Notify HTTP server to start graceful shutdown
        let _ = http_shutdown_tx.send(());

        // Wait for HTTP server with graceful timeout
        let graceful_timeout = Duration::from_secs_f32(self.config.server.graceful_timeout);
        match tokio::time::timeout(graceful_timeout, http_handle).await {
            Ok(Ok(Ok(()))) => info!("HTTP server shut down gracefully"),
            Ok(Ok(Err(e))) => error!("HTTP server error during shutdown: {}", e),
            Ok(Err(e)) => error!("HTTP task panicked during shutdown: {}", e),
            Err(_) => warn!(
                "HTTP server graceful shutdown timed out after {}s",
                self.config.server.graceful_timeout
            ),
        }

        // Abort metrics and gRPC if still running
        if let Some(h) = metrics_handle {
            h.abort();
        }
        if let Some(h) = grpc_handle {
            h.abort();
        }

        let _ = endpoint_manager.shutdown().await;
        self.worker_manager.shutdown().await;

        Ok(())
    }

    /// Check if any loaded model has hot_reload enabled.
    fn registry_has_hot_reload_models(&self) -> bool {
        self.registry.list_loaded().iter().any(|(_, _, mv)| mv.config.hot_reload)
    }

    async fn load_initial_models(&self) -> Result<(), AppError> {
        let repo_path = PathBuf::from(&self.config.model_repository.path);
        let orch = resolve_orchestration(&repo_path, &self.config.orchestration);

        // Apply strategies to registry
        for strategy in &orch.models {
            self.registry.set_strategy(&strategy.name, strategy)?;
        }

        // Auto-unpack any .lma artifacts placed in the model repo
        auto_unpack_lma_files(&repo_path).await;

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
                    let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
                    self.config.apply_model_defaults(&mut config);

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

async fn start_metrics_server(host: String, port: u16) -> Result<(), AppError> {
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

/// Resolve orchestration config: prefer standalone file for backward compat,
/// fall back to server.yaml orchestration section.
fn resolve_orchestration(
    repo_path: &Path,
    config_orch: &OrchestrationConfig,
) -> OrchestrationConfig {
    if let Some(orch_path) = find_orchestration(repo_path) {
        crate::config::load_orchestration(&orch_path).unwrap_or_default()
    } else {
        config_orch.clone()
    }
}

#[derive(Clone)]
struct RepoModel {
    name: String,
    version: String,
}

/// Auto-unpack .lma artifact files found in the model repository root.
/// Shells out to `python -m lite_server unpack` to extract each .lma into
/// the standard repo/model_name/version/ directory layout.
async fn auto_unpack_lma_files(repo_path: &Path) {
    let mut entries = match tokio::fs::read_dir(repo_path).await {
        Ok(e) => e,
        Err(_) => return,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().map(|e| e == "lma").unwrap_or(false) {
            let output = tokio::process::Command::new("python")
                .args([
                    "-m",
                    "lite_server",
                    "unpack",
                    path.to_str().unwrap_or(""),
                    "--to",
                    repo_path.to_str().unwrap_or(""),
                ])
                .output()
                .await;

            match output {
                Ok(out) if out.status.success() => {
                    info!(
                        "Auto-unpacked .lma artifact: {}",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    );
                }
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    warn!(
                        "Failed to unpack .lma artifact {}: {}",
                        path.display(),
                        stderr.trim()
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to run unpack for .lma artifact {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }
    }
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

/// Check if a filename matches any of the glob-like patterns.
/// Supports simple `*` wildcard (e.g., `*.py`, `model_*.yaml`).
fn matches_patterns(filename: &str, patterns: &[String]) -> bool {
    if patterns.is_empty() {
        return true; // no patterns = match all
    }
    patterns.iter().any(|p| {
        if let Some(suffix) = p.strip_prefix("*.") {
            // Simple suffix match: "*.py" matches "foo.py"
            filename.ends_with(&format!(".{}", suffix))
        } else if let Some(prefix) = p.strip_suffix(".*") {
            // Prefix match: "model_.*" matches "model_abc"
            filename.starts_with(prefix)
        } else {
            // Exact match
            filename == p
        }
    })
}

async fn process_watch_events(
    paths: Vec<PathBuf>,
    repo_path: PathBuf,
    worker_manager: Arc<WorkerManager>,
    registry: Arc<ModelRegistry>,
    last_reload: &mut std::collections::HashMap<(String, String), Instant>,
    model_defaults: &crate::config::ModelDefaults,
    has_hot_reload: &AtomicBool,
) -> Result<(), AppError> {
    use std::collections::HashSet;

    // Map from (model, version) to the set of changed files for that model
    let mut models_to_reload: HashSet<(String, String)> = HashSet::new();
    let mut trigger_files: std::collections::HashMap<(String, String), Vec<PathBuf>> =
        std::collections::HashMap::new();
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
                            let key = (model_name.to_string(), version.to_string());
                            models_to_reload.insert(key.clone());
                            trigger_files.entry(key).or_default().push(path.clone());
                        }
                    } else {
                        // File was deleted
                        if crate::validation::validate_identifier(model_name).is_ok()
                            && crate::validation::validate_identifier(version).is_ok()
                        {
                            let key = (model_name.to_string(), version.to_string());
                            models_to_reload.insert(key.clone());
                            trigger_files.entry(key).or_default().push(path.clone());
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

        // P0: Check if hot_reload is enabled for this model version
        if let Some(mv) = registry.get(&name, Some(&version)) {
            if !mv.config.hot_reload {
                debug!("Hot reload: skipping {} version {} (hot_reload=false)", name, version);
                continue;
            }

            // P1: Check hot_reload_patterns - if configured, only reload when matching files change
            if !mv.config.hot_reload_patterns.is_empty() {
                let empty: Vec<PathBuf> = Vec::new();
                let files = trigger_files.get(&key).unwrap_or(&empty);
                let any_match = files.iter().any(|f| {
                    let fname = f.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    matches_patterns(fname, &mv.config.hot_reload_patterns)
                });
                if !any_match {
                    debug!("Hot reload: skipping {} version {} (no pattern match)", name, version);
                    continue;
                }
            }

            last_reload.insert(key, Instant::now());
            info!("Hot reload: reloading {} version {}", name, version);
            if let Err(e) = worker_manager.reload_model(&name, Some(&version)).await {
                warn!("Hot reload failed for {} version {}: {}", name, version, e);
            }
        } else {
            // Model not loaded yet, try to load it
            // For new models, check config.yaml for hot_reload setting
            let config_path = repo_path.join(&name).join(&version).join("config.yaml");
            if config_path.exists() {
                let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
                if let Some(v) = model_defaults.max_queue_size { config.max_queue_size = v; }
                if let Some(v) = model_defaults.max_requests { config.max_requests = v; }
                if let Some(v) = model_defaults.request_timeout { config.request_timeout = v; }
                if let Some(v) = model_defaults.health_check_interval { config.health_check_interval = v; }

                // P0: Only auto-load if hot_reload is enabled
                if !config.hot_reload {
                    debug!("Hot reload: skipping new model {} version {} (hot_reload=false)", name, version);
                    continue;
                }

                last_reload.insert(key, Instant::now());
                info!("Hot reload: auto-loading new model {} version {}", name, version);
                if let Err(e) = worker_manager.load_model(&name, &version, &config).await {
                    warn!("Hot load failed for {} version {}: {}", name, version, e);
                } else {
                    // P2: Update flag when new hot_reload model is loaded
                    has_hot_reload.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    // Check for removed models
    // After unloading, re-check if any hot_reload models remain
    let had_unloads = !models_to_check_removed.is_empty();
    for (name, version) in models_to_check_removed {
        let version_dir = repo_path.join(&name).join(&version);
        if !version_dir.exists() {
            info!("Hot reload: auto-unloading removed model {} version {}", name, version);
            let _ = worker_manager.unload_model(&name, Some(&version)).await;
        }
    }

    // P2: Re-check hot_reload flag after potential unloads
    if had_unloads {
        let any_hot_reload = registry.list_loaded().iter().any(|(_, _, mv)| mv.config.hot_reload);
        has_hot_reload.store(any_hot_reload, Ordering::Relaxed);
        if !any_hot_reload {
            info!("Hot reload: no more enabled models, watcher will skip events");
        }
    }

    Ok(())
}

// ===== Hot Reload File Watcher =====

/// Start a file watcher on the model repository directory.
/// The `has_hot_reload` flag controls whether events are actually sent for processing.
/// When no models have hot_reload enabled, events are collected but not forwarded.
async fn start_file_watcher(
    repo_path: PathBuf,
    _worker_manager: Arc<WorkerManager>,
    tx: mpsc::Sender<Vec<PathBuf>>,
    has_hot_reload: Arc<AtomicBool>,
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
                        // P2: Skip collecting events if no hot_reload models
                        if !has_hot_reload.load(Ordering::Relaxed) {
                            continue;
                        }

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
            "warn".to_string(),
        ));

        let (tx, _rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(true));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload));

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
            "warn".to_string(),
        ));

        let (tx, mut rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(true));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload));

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

    #[test]
    fn test_matches_patterns() {
        // Wildcard suffix
        assert!(matches_patterns("model.py", &["*.py".to_string()]));
        assert!(!matches_patterns("model.yaml", &["*.py".to_string()]));

        // Exact match
        assert!(matches_patterns("config.yaml", &["config.yaml".to_string()]));
        assert!(!matches_patterns("other.yaml", &["config.yaml".to_string()]));

        // Empty patterns = match all
        assert!(matches_patterns("anything.txt", &[]));

        // Multiple patterns
        let patterns = vec!["*.py".to_string(), "*.yaml".to_string()];
        assert!(matches_patterns("model.py", &patterns));
        assert!(matches_patterns("config.yaml", &patterns));
        assert!(!matches_patterns("data.json", &patterns));
    }

    // --- resolve_orchestration ---

    #[test]
    fn test_resolve_orchestration_from_file() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-orch-file-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let orch_path = tmp.join("orchestration.yaml");
        std::fs::write(
            &orch_path,
            "control_mode: poll\nload_models:\n  - from_file\n",
        )
        .unwrap();

        let config_orch = OrchestrationConfig::default();
        let resolved = resolve_orchestration(&tmp, &config_orch);
        assert_eq!(resolved.control_mode, "poll");
        assert_eq!(resolved.load_models, vec!["from_file"]);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_resolve_orchestration_fallback_to_config() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-orch-config-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        // No orchestration.yaml written

        let config_orch = OrchestrationConfig {
            control_mode: "explicit".to_string(),
            load_models: vec!["from_config".to_string()],
            ..Default::default()
        };
        let resolved = resolve_orchestration(&tmp, &config_orch);
        assert_eq!(resolved.control_mode, "explicit");
        assert_eq!(resolved.load_models, vec!["from_config"]);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_resolve_orchestration_file_takes_precedence() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-orch-prec-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let orch_path = tmp.join("orchestration.yaml");
        std::fs::write(
            &orch_path,
            "control_mode: all\nload_models:\n  - from_file\n",
        )
        .unwrap();

        let config_orch = OrchestrationConfig {
            control_mode: "explicit".to_string(),
            load_models: vec!["from_config".to_string()],
            ..Default::default()
        };
        let resolved = resolve_orchestration(&tmp, &config_orch);
        // File takes precedence for backward compat
        assert_eq!(resolved.control_mode, "all");
        assert_eq!(resolved.load_models, vec!["from_file"]);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
