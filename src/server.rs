use crate::callback::{CallbackRunner, ServerContext};
use crate::config::{Config, OrchestrationConfig, ModelStrategyConfig};
use crate::error::AppError;
use crate::http;
use crate::inference_queue::InferenceQueue;
use crate::metrics::prometheus;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, info, error, warn, Instrument};

/// Tracks in-flight requests during graceful shutdown.
#[derive(Clone)]
pub struct ShutdownState {
    pub pending_count: Arc<AtomicUsize>,
    pub start_time: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl ShutdownState {
    pub fn new() -> Self {
        Self {
            pending_count: Arc::new(AtomicUsize::new(0)),
            start_time: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn mark_start(&self) {
        *self.start_time.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }

    pub fn elapsed(&self) -> Option<Duration> {
        self.start_time
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map(|t| t.elapsed())
    }

    pub fn inc_pending(&self) {
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        prometheus::SHUTDOWN_PENDING_REQUESTS.inc();
    }

    pub fn dec_pending(&self) {
        self.pending_count.fetch_sub(1, Ordering::Relaxed);
        prometheus::SHUTDOWN_PENDING_REQUESTS.dec();
    }

    pub fn pending(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }
}

impl Default for ShutdownState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LiteServer {
    config: Config,
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    inference_queue: Arc<InferenceQueue>,
    callback_runner: Arc<CallbackRunner>,
}

/// Lenient semver parse for version directory names (§4.2): tolerates a
/// leading `v` and missing minor/patch components (`v2` → 2.0.0, `1.2` →
/// 1.2.0). Returns `None` for non-numeric schemes like `nightly`.
fn parse_lenient_semver(v: &str) -> Option<semver::Version> {
    let v = v.strip_prefix('v').unwrap_or(v);
    if let Ok(parsed) = semver::Version::parse(v) {
        return Some(parsed);
    }
    match v.split('.').count() {
        1 => semver::Version::parse(&format!("{}.0.0", v)).ok(),
        2 => semver::Version::parse(&format!("{}.0", v)).ok(),
        _ => None,
    }
}

/// Pick the "latest" version for `load_policy = "latest"` (§4.2). Semver
/// comparison when *every* candidate parses (fixing the lexicographic
/// `"v10" < "v2"` bug); any unparseable candidate falls the whole set back
/// to lexicographic order — predictable for mixed naming schemes.
fn pick_latest_version(versions: &[String]) -> Option<String> {
    let parsed: Option<Vec<semver::Version>> = versions
        .iter()
        .map(|v| parse_lenient_semver(v))
        .collect();
    match parsed {
        Some(sems) => versions
            .iter()
            .zip(sems.iter())
            .max_by_key(|(_, sem)| *sem)
            .map(|(orig, _)| orig.clone()),
        None => versions.iter().max().cloned(),
    }
}

impl LiteServer {
    pub fn new(config: Config) -> Self {
        let repo_path = PathBuf::from(&config.model_repository.path);
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path,
            inference_queue.clone(),
            config.logging.level.clone(),
            callback_runner.clone(),
        ).with_server_http(Self::loopback_http_base(&config))
         .with_unload_grace(Duration::from_secs_f32(config.server.timeout)));

        Self {
            config,
            registry,
            worker_manager,
            inference_queue,
            callback_runner,
        }
    }

    /// Loopback HTTP base URL handed to workers as --server-http so @route
    /// handlers can reach this server via ctx.server (phase 2b). Wildcard
    /// binds collapse to 127.0.0.1; unix-socket HTTP has no TCP loopback and
    /// yields None (ctx.server stays None in that deployment).
    fn loopback_http_base(config: &Config) -> Option<String> {
        if config.server.host.starts_with("unix:") {
            return None;
        }
        let host = match config.server.host.as_str() {
            "" | "0.0.0.0" | "::" => "127.0.0.1",
            h => h,
        };
        Some(format!("http://{}:{}", host, config.server.http_port))
    }

    pub async fn run(&self) -> Result<(), AppError> {
        // Register prometheus metrics
        if let Err(e) = prometheus::register_metrics() {
            error!("Failed to register metrics: {}", e);
        }

        // Start reload listener for max_requests auto-recycle
        self.worker_manager.start_reload_listener().await;

        // Start respawn listener for health-check-kill-triggered worker restarts
        self.worker_manager.start_respawn_listener().await;

        // Load initial models; the returned seen_lma set is handed to the
        // reconcile task so .lma artifacts unpacked at startup are not
        // re-unpacked (i.e. overwritten) on the first reconcile tick.
        let seen_lma = self.load_initial_models().await?;

        // Fire ServerStart callbacks
        self.callback_runner.on_server_start(&ServerContext {
            http_port: self.config.server.http_port,
            grpc_port: self.config.server.grpc_port,
            metrics_port: self.config.server.metrics_port,
        }).await;

        // P2: Check if any loaded model has hot_reload enabled
        let has_hot_reload = Arc::new(AtomicBool::new(self.registry_has_hot_reload_models()));
        if has_hot_reload.load(Ordering::Relaxed) {
            info!("Hot reload: enabled models detected, starting file watcher");
        } else {
            info!("Hot reload: no enabled models, file watcher will start on demand");
        }

        let repo_path = PathBuf::from(&self.config.model_repository.path)
            .canonicalize()
            .unwrap_or_else(|_| PathBuf::from(&self.config.model_repository.path));

        // Create shared shutdown state for pending request tracking
        let shutdown_state = Arc::new(ShutdownState::new());

        // Start HTTP server as spawned task with graceful shutdown channel
        let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
        let has_hot_reload_for_http = has_hot_reload.clone();
        let mut http_handle = tokio::spawn(http::start_http_server(
            self.config.clone(),
            self.registry.clone(),
            self.worker_manager.clone(),
            self.inference_queue.clone(),
            http_shutdown_rx,
            shutdown_state.clone(),
            self.callback_runner.clone(),
            has_hot_reload_for_http,
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
                self.callback_runner.clone(),
                Duration::from_secs_f64(self.config.server.timeout as f64),
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

        // P2: Start hot reload watcher. In auto mode events are always
        // forwarded — directory-level events feed the reconcile task even
        // when no loaded model has hot_reload enabled.
        let auto_mode = self.config.orchestration.control_mode == "auto";
        let (watch_tx, mut watch_rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let has_hot_reload_for_watcher = has_hot_reload.clone();
        let watcher_handle = tokio::spawn(start_file_watcher(
            repo_path.clone(),
            self.worker_manager.clone(),
            watch_tx,
            has_hot_reload_for_watcher,
            auto_mode,
        ));

        // Start the reconcile task for control_mode = "auto": directory
        // events trigger a reconcile (near-real-time), the poll interval is
        // the resync backstop (watch events can be lost on network FS).
        let (reconcile_tx, poller_handle) = if auto_mode {
            let (tx, rx) = mpsc::channel::<()>(8);
            (
                Some(tx),
                Some(tokio::spawn(start_reconcile_task(
                    repo_path.clone(),
                    self.config.orchestration.clone(),
                    self.worker_manager.clone(),
                    self.registry.clone(),
                    self.config.model_defaults.clone(),
                    rx,
                    seen_lma,
                ))),
            )
        } else {
            (None, None)
        };

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
                    &reconcile_tx,
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

        // Fire ServerEnd callbacks
        self.callback_runner.on_server_end(&ServerContext {
            http_port: self.config.server.http_port,
            grpc_port: self.config.server.grpc_port,
            metrics_port: self.config.server.metrics_port,
        }).await;

        // Mark shutdown start for pending request tracking
        shutdown_state.mark_start();

        // Abort background tasks
        watcher_handle.abort();
        timeline_handle.abort();
        reload_handle.abort();
        if let Some(h) = poller_handle {
            h.abort();
        }

        // Notify HTTP server to start graceful shutdown
        let _ = http_shutdown_tx.send(());

        // Spawn periodic shutdown status logger
        let state_for_monitor = shutdown_state.clone();
        let monitor_graceful = self.config.server.graceful_timeout;
        let monitor_handle = tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                let pending = state_for_monitor.pending();
                if pending == 0 {
                    let elapsed = state_for_monitor
                        .elapsed()
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    info!(
                        "Graceful shutdown: all requests drained after {}s",
                        elapsed
                    );
                    break;
                }
                let elapsed = state_for_monitor
                    .elapsed()
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                info!(
                    "Graceful shutdown: {} in-flight after {}s (timeout {}s)",
                    pending, elapsed, monitor_graceful
                );
            }
        });

        // Wait for HTTP server with graceful timeout
        let graceful_timeout = Duration::from_secs_f32(self.config.server.graceful_timeout);
        match tokio::time::timeout(graceful_timeout, http_handle).await {
            Ok(Ok(Ok(()))) => info!("HTTP server shut down gracefully"),
            Ok(Ok(Err(e))) => error!("HTTP server error during shutdown: {}", e),
            Ok(Err(e)) => error!("HTTP task panicked during shutdown: {}", e),
            Err(_) => {
                let remaining = shutdown_state.pending();
                warn!(
                    "HTTP server graceful shutdown timed out after {}s ({} requests still pending)",
                    self.config.server.graceful_timeout,
                    remaining
                );
            }
        }

        monitor_handle.abort();

        // Abort metrics and gRPC if still running
        if let Some(h) = metrics_handle {
            h.abort();
        }
        if let Some(h) = grpc_handle {
            h.abort();
        }

        self.worker_manager.shutdown().await;

        Ok(())
    }

    /// Check if any loaded model has hot_reload enabled.
    fn registry_has_hot_reload_models(&self) -> bool {
        self.registry.list_loaded().iter().any(|(_, _, mv)| mv.config.hot_reload)
    }

    async fn load_initial_models(&self) -> Result<HashSet<(PathBuf, std::time::SystemTime)>, AppError> {
        let repo_path = PathBuf::from(&self.config.model_repository.path);
        let orch = &self.config.orchestration;

        // Apply strategies to registry
        for strategy in &orch.models {
            self.registry.set_strategy(&strategy.name, strategy)?;
        }

        let mut seen_lma = HashSet::new();
        reconcile_models(
            &repo_path,
            orch,
            &self.worker_manager,
            &self.registry,
            &self.config.model_defaults,
            &mut seen_lma,
        )
        .await;

        Ok(seen_lma)
    }
}

/// Reconcile registry state with the model repository: load versions that
/// should be present per the orchestration config, unload managed versions
/// that should no longer be present (declarative semantics — the config is
/// the source of truth for models in scope). Runs once at startup (initial
/// load) and on every poll tick when control_mode = "auto".
///
/// `seen_lma` tracks (path, mtime) of .lma artifacts already unpacked so the
/// poller doesn't re-unpack — and thereby overwrite — the same file every
/// tick. A replaced artifact has a new mtime and is unpacked again.
async fn reconcile_models(
    repo_path: &Path,
    orch: &OrchestrationConfig,
    worker_manager: &WorkerManager,
    registry: &ModelRegistry,
    model_defaults: &crate::config::ModelTunables,
    seen_lma: &mut HashSet<(PathBuf, std::time::SystemTime)>,
) {
    auto_unpack_lma_files(repo_path, seen_lma).await;

    let available = scan_repo_models(repo_path).await;
    let by_model = group_by_model(available);

    let names: Vec<String> = match orch.control_mode.as_str() {
        "all" => by_model.keys().cloned().collect(),
        _ => orch.load_models.clone(),
    };

    let strategy_map: HashMap<String, &ModelStrategyConfig> = orch
        .models
        .iter()
        .map(|s| (s.name.clone(), s))
        .collect();

    for name in &names {
        let strategy = strategy_map.get(name).copied();
        let models = by_model.get(name).cloned().unwrap_or_default();
        let target = compute_target_versions(&models, strategy);

        let loaded: HashSet<String> = registry
            .list_versions(name)
            .into_iter()
            .map(|v| v.version)
            .collect();
        let target_set: HashSet<String> = target.iter().cloned().collect();

        // Unload managed versions that should no longer be present.
        for version in loaded.difference(&target_set) {
            info!("Unloading {} version {} (no longer in target set)", name, version);
            if let Err(e) = worker_manager.unload_model(name, Some(version)).await {
                warn!("Failed to unload {} version {}: {}", name, version, e);
            }
        }

        // Load missing versions.
        let mut versions_loaded = Vec::new();
        for version in &target {
            if loaded.contains(version) {
                continue;
            }
            if let Err(e) = crate::validation::validate_identifier(name) {
                error!("Skipping model with invalid name '{}': {}", name, e);
                break;
            }
            if let Err(e) = crate::validation::validate_identifier(version) {
                error!("Skipping {} version with invalid version '{}': {}", name, version, e);
                continue;
            }
            // Respect max_loaded_versions: loading beyond the cap would
            // trigger LRU eviction, which the next reconcile would see as
            // "missing" and reload — an evict/reload thrash loop.
            if let Some(max) = strategy.and_then(|s| s.max_loaded_versions) {
                let current = registry.list_versions(name).len();
                if current >= max {
                    warn!(
                        "Skipping load of {} version {}: max_loaded_versions={} already reached",
                        name, version, max
                    );
                    continue;
                }
            }
            let config_path = repo_path.join(name).join(version).join("config.yaml");
            let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
            model_defaults.apply_to(&mut config);

            if let Err(e) = worker_manager.load_model(name, version, &config).await {
                error!("Failed to load {} version {}: {}", name, version, e);
            } else {
                versions_loaded.push(version.clone());
            }
        }

        // Activate default version
        let default_version = strategy.and_then(|s| s.default_version.clone());
        if let Some(ref dv) = default_version {
            if versions_loaded.contains(dv) {
                match registry.activate_version(name, dv) {
                    Ok(true) => info!("Activated default version {} for {}", dv, name),
                    Ok(false) => warn!("Failed to activate default version {} for {} (not ready)", dv, name),
                    Err(e) => error!("Error activating default version {} for {}: {}", dv, name, e),
                }
            }
        } else if !versions_loaded.is_empty() && registry.get_active_version(name).is_none() {
            match registry.activate_version(name, &versions_loaded[0]) {
                Ok(true) => info!("Activated version {} for {}", versions_loaded[0], name),
                Ok(false) => warn!("Failed to activate version {} for {} (not ready)", versions_loaded[0], name),
                Err(e) => error!("Error activating version {} for {}: {}", versions_loaded[0], name, e),
            }
        }
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
        .map_err(AppError::Io)?;

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

#[derive(Clone)]
struct RepoModel {
    name: String,
    version: String,
}

/// Versions of a model that should be loaded per the strategy's load_policy:
/// "all" → every version found on disk; "latest" → the highest semver;
/// otherwise ("explicit") → versions_to_load ∩ disk versions. No strategy
/// (None) loads every disk version — the legacy default for load_models
/// entries without a strategy.
fn compute_target_versions(
    models: &[RepoModel],
    strategy: Option<&ModelStrategyConfig>,
) -> Vec<String> {
    let all: Vec<String> = models.iter().map(|m| m.version.clone()).collect();
    let load_policy = strategy
        .map(|s| s.load_policy.as_str())
        .unwrap_or("explicit");
    match load_policy {
        "all" => all,
        "latest" => pick_latest_version(&all).into_iter().collect(),
        _ => match strategy {
            Some(s) => models
                .iter()
                .filter(|m| s.versions_to_load.contains(&m.version))
                .map(|m| m.version.clone())
                .collect(),
            None => all,
        },
    }
}

/// Auto-unpack .lma artifact files found in the model repository root.
/// Shells out to `python -m lite_server unpack` to extract each .lma into
/// the standard repo/model_name/version/ directory layout.
///
/// `seen` tracks (path, mtime) of artifacts already unpacked: the unpacker
/// does not delete the .lma after extraction, so without this the model
/// poller would re-unpack — and overwrite the extracted directory — every
/// tick. Each artifact is attempted once per mtime; a failed unpack is not
/// retried unless the file is replaced (new mtime).
async fn auto_unpack_lma_files(
    repo_path: &Path,
    seen: &mut HashSet<(PathBuf, std::time::SystemTime)>,
) {
    let mut entries = match tokio::fs::read_dir(repo_path).await {
        Ok(e) => e,
        Err(_) => return,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if !path.extension().map(|e| e == "lma").unwrap_or(false) {
            continue;
        }
        let mtime = match tokio::fs::metadata(&path).await.and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !seen.insert((path.clone(), mtime)) {
            continue;
        }

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
                        is_ensemble = crate::config::config_content_is_ensemble(&content);
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

/// Process a debounced batch of file events.
///
/// Classification is registry-based, not directory-shape-based:
/// - events on a **live** version (loaded AND its dir still exists) are
///   file-level: the hot-reload path (gate + patterns + cooldown) restarts
///   the worker;
/// - everything else (unknown version, or the version dir is gone) is a
///   lifecycle event. In auto mode (`reconcile_trigger` = Some) these only
///   trigger a reconcile — the reconciler is the single authority on version
///   load/unload, applying load_policy and max_loaded_versions. In manual
///   mode the legacy behavior remains: auto-load hot_reload-enabled new
///   versions, and directly unload versions whose dir disappeared (that
///   invariant holds in every mode — the files are gone, the worker cannot
///   serve).
async fn process_watch_events(
    paths: Vec<PathBuf>,
    repo_path: PathBuf,
    worker_manager: Arc<WorkerManager>,
    registry: Arc<ModelRegistry>,
    last_reload: &mut std::collections::HashMap<(String, String), Instant>,
    model_defaults: &crate::config::ModelTunables,
    has_hot_reload: &AtomicBool,
    reconcile_trigger: &Option<mpsc::Sender<()>>,
) -> Result<(), AppError> {
    use std::collections::HashSet;

    // Map from (model, version) to the set of changed files for that model
    let mut models_to_reload: HashSet<(String, String)> = HashSet::new();
    let mut trigger_files: std::collections::HashMap<(String, String), Vec<PathBuf>> =
        std::collections::HashMap::new();
    let mut lifecycle_candidates: HashSet<(String, String)> = HashSet::new();

    for path in paths {
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
                    components.first().and_then(|c| match c {
                        std::path::Component::Normal(s) => s.to_str(),
                        _ => None,
                    }),
                    components.get(1).and_then(|c| match c {
                        std::path::Component::Normal(s) => s.to_str(),
                        _ => None,
                    }),
                ) {
                    if crate::validation::validate_identifier(model_name).is_err()
                        || crate::validation::validate_identifier(version).is_err()
                    {
                        continue;
                    }
                    let key = (model_name.to_string(), version.to_string());
                    let version_dir = repo_path.join(model_name).join(version);
                    let is_live = registry.get(model_name, Some(version)).is_some()
                        && version_dir.exists();
                    if path.is_dir() {
                        // Dir events on a live version (e.g. mtime changes
                        // when files are written inside) carry no signal.
                        if !is_live {
                            lifecycle_candidates.insert(key);
                        }
                    } else if is_live {
                        models_to_reload.insert(key.clone());
                        trigger_files.entry(key).or_default().push(path.clone());
                    } else {
                        lifecycle_candidates.insert(key);
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

        // P0: Check if hot_reload is enabled for this model version.
        // Classification guarantees the version is loaded; a concurrent
        // unload between classification and here is impossible on this task.
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

            last_reload.insert(key.clone(), Instant::now());
            // P3: give live workers a chance to refresh in-process via their
            // on_file_changed hook; fall back to a full worker restart
            // unless every worker of the version reports handled.
            let changed: Vec<String> = trigger_files
                .get(&key)
                .map(|fs| fs.iter().map(|f| f.to_string_lossy().into_owned()).collect())
                .unwrap_or_default();
            if worker_manager.notify_file_changed(&name, &version, &changed).await {
                info!(
                    "Hot reload: {} version {} refreshed in-process via on_file_changed (no restart)",
                    name, version
                );
            } else {
                info!("Hot reload: reloading {} version {}", name, version);
                if let Err(e) = worker_manager.reload_model(&name, Some(&version)).await {
                    warn!("Hot reload failed for {} version {}: {}", name, version, e);
                }
            }
        }
    }

    if lifecycle_candidates.is_empty() {
        return Ok(());
    }

    match reconcile_trigger {
        // Auto mode: the reconciler is the single authority on version
        // lifecycle. try_send coalesces naturally — a full channel means a
        // trigger is already pending.
        Some(tx) => {
            debug!(
                count = lifecycle_candidates.len(),
                "Lifecycle events detected, triggering reconcile"
            );
            let _ = tx.try_send(());
        }
        // Manual mode: legacy behavior (P4 will deprecate the auto-load half).
        None => {
            let mut had_unloads = false;
            for (name, version) in lifecycle_candidates {
                let key = (name.clone(), version.clone());
                let version_dir = repo_path.join(&name).join(&version);
                if version_dir.exists() {
                    // New version dir: auto-load only if its config opts in.
                    if let Some(last) = last_reload.get(&key) {
                        if Instant::now().duration_since(*last) < cooldown {
                            continue;
                        }
                    }
                    let config_path = version_dir.join("config.yaml");
                    if config_path.exists() {
                        let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
                        model_defaults.apply_to(&mut config);

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
                } else {
                    // Dir gone → unload. This invariant holds in every mode:
                    // the files no longer exist, the worker cannot serve.
                    had_unloads = true;
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
        }
    }

    Ok(())
}

// ===== Hot Reload File Watcher =====

/// Run the reconcile loop for control_mode = "auto": directory events
/// trigger a reconcile (near-real-time), the poll interval is the resync
/// backstop (watch events can be lost, e.g. on network filesystems).
/// `seen_lma` comes from the startup load so artifacts unpacked there are
/// not re-unpacked on the first tick.
async fn start_reconcile_task(
    repo_path: PathBuf,
    orch: OrchestrationConfig,
    worker_manager: Arc<WorkerManager>,
    registry: Arc<ModelRegistry>,
    model_defaults: crate::config::ModelTunables,
    trigger_rx: mpsc::Receiver<()>,
    seen_lma: HashSet<(PathBuf, std::time::SystemTime)>,
) {
    let poll_secs = orch.poll_interval.max(1);
    info!(
        "Reconcile task started (control_mode=auto, resync interval={}s)",
        poll_secs
    );
    let seen_lma = Arc::new(tokio::sync::Mutex::new(seen_lma));
    reconcile_loop(Duration::from_secs(poll_secs), trigger_rx, move || {
        let repo_path = repo_path.clone();
        let orch = orch.clone();
        let worker_manager = worker_manager.clone();
        let registry = registry.clone();
        let model_defaults = model_defaults.clone();
        let seen_lma = seen_lma.clone();
        async move {
            let mut seen_lma = seen_lma.lock().await;
            reconcile_models(
                &repo_path,
                &orch,
                &worker_manager,
                &registry,
                &model_defaults,
                &mut seen_lma,
            )
            .instrument(tracing::info_span!("reconcile"))
            .await;
        }
    })
    .await;
}

/// Generic reconcile driver: run `reconcile` on every resync tick, and
/// near-real-time on trigger events (coalesced over a 2s window so a burst
/// of filesystem events — e.g. unpacking a version dir — produces one run).
async fn reconcile_loop<F, Fut>(
    poll_interval: Duration,
    trigger_rx: mpsc::Receiver<()>,
    mut reconcile: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    const COALESCE: Duration = Duration::from_secs(2);
    let mut tick = interval(poll_interval);
    // The first interval tick fires immediately; skip it — the startup
    // reconcile just ran, a second one would be a redundant repo scan.
    tick.tick().await;
    let mut trigger_rx = Some(trigger_rx);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                reconcile().await;
            }
            msg = async {
                match trigger_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => futures::future::pending().await,
                }
            } => {
                match msg {
                    Some(()) => {
                        tokio::time::sleep(COALESCE).await;
                        if let Some(rx) = trigger_rx.as_mut() {
                            while rx.try_recv().is_ok() {}
                        }
                        reconcile().await;
                    }
                    // Sender dropped (reload task gone): tick-only resync.
                    None => trigger_rx = None,
                }
            }
        }
    }
}

/// Start a file watcher on the model repository directory.
/// The `has_hot_reload` flag controls whether events are actually sent for processing.
/// When no models have hot_reload enabled, events are collected but not forwarded —
/// unless `always_forward` (control_mode = "auto"), where directory-level
/// lifecycle events feed the reconcile task regardless of hot_reload.
async fn start_file_watcher(
    repo_path: PathBuf,
    _worker_manager: Arc<WorkerManager>,
    tx: mpsc::Sender<Vec<PathBuf>>,
    has_hot_reload: Arc<AtomicBool>,
    always_forward: bool,
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
                        // (auto mode always forwards — lifecycle events feed
                        // the reconcile task).
                        if !always_forward && !has_hot_reload.load(Ordering::Relaxed) {
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
                        let paths: Vec<PathBuf> = std::mem::take(&mut pending_paths);
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

    #[test]
    fn test_pick_latest_version_semver_v_prefix() {
        // The lexicographic bug: "v10" < "v2" as strings.
        let versions = vec!["v2".to_string(), "v10".to_string(), "v1".to_string()];
        assert_eq!(pick_latest_version(&versions), Some("v10".to_string()));
    }

    #[test]
    fn test_pick_latest_version_semver_dotted() {
        let versions = vec!["1.2.0".to_string(), "1.10.0".to_string(), "1.2".to_string()];
        assert_eq!(pick_latest_version(&versions), Some("1.10.0".to_string()));
    }

    #[test]
    fn test_pick_latest_version_fallback_lexicographic_when_mixed() {
        // Any unparseable candidate falls the whole set back to lexicographic.
        let versions = vec!["v2".to_string(), "nightly".to_string()];
        assert_eq!(pick_latest_version(&versions), Some("v2".to_string()));
    }

    #[test]
    fn test_pick_latest_version_empty() {
        assert_eq!(pick_latest_version(&[]), None);
    }

    #[tokio::test]
    async fn test_file_watcher_aborts_cleanly() {
        let tmp_dir = std::env::temp_dir().join(format!("lite-server-fw-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&tmp_dir).await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry,
            tmp_dir.clone(),
            inference_queue,
            "warn".to_string(),
            callback_runner,
        ));

        let (tx, _rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(true));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload, false));

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
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry,
            tmp_dir.clone(),
            inference_queue,
            "warn".to_string(),
            callback_runner,
        ));

        let (tx, mut rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(true));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload, false));

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

    // --- compute_target_versions ---

    fn rm(name: &str, version: &str) -> RepoModel {
        RepoModel {
            name: name.to_string(),
            version: version.to_string(),
        }
    }

    #[test]
    fn test_compute_target_versions_all() {
        let models = vec![rm("m", "1"), rm("m", "2"), rm("m", "3")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "all".to_string(),
            ..Default::default()
        };
        assert_eq!(
            compute_target_versions(&models, Some(&strategy)),
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
    }

    #[test]
    fn test_compute_target_versions_latest() {
        let models = vec![rm("m", "v2"), rm("m", "v10"), rm("m", "v1")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "latest".to_string(),
            ..Default::default()
        };
        assert_eq!(
            compute_target_versions(&models, Some(&strategy)),
            vec!["v10".to_string()]
        );
    }

    #[test]
    fn test_compute_target_versions_explicit_intersection() {
        let models = vec![rm("m", "1"), rm("m", "2")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: vec!["2".to_string(), "3".to_string()],
            ..Default::default()
        };
        assert_eq!(
            compute_target_versions(&models, Some(&strategy)),
            vec!["2".to_string()]
        );
    }

    #[test]
    fn test_compute_target_versions_no_strategy_loads_all_disk_versions() {
        // A model in load_models without a strategy entry loads every
        // version found on disk (legacy default).
        let models = vec![rm("m", "1"), rm("m", "2")];
        assert_eq!(
            compute_target_versions(&models, None),
            vec!["1".to_string(), "2".to_string()]
        );
    }

    #[test]
    fn test_compute_target_versions_explicit_empty_versions_to_load() {
        // Strategy present with explicit policy but no versions_to_load:
        // nothing loads (empty intersection), unlike the no-strategy case.
        let models = vec![rm("m", "1"), rm("m", "2")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "explicit".to_string(),
            ..Default::default()
        };
        assert!(compute_target_versions(&models, Some(&strategy)).is_empty());
    }

    // ===== B1: ensemble detection via string contains is fragile =====

    /// B1 (P2): `scan_repo_models` detects ensemble models via
    /// `content.contains("ensemble:")`. This matches comments,
    /// documentation strings, and other non-structural occurrences,
    /// causing false positives.
    #[tokio::test]
    async fn test_scan_repo_models_ensemble_detection_false_positive_on_comment() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ensemble-fp-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("test_model").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();

        // A config.yaml where "ensemble:" only appears in a comment / description.
        // A real model.py also exists, so the directory would be picked up as a
        // standard model — but the string-contains check marks it as ensemble.
        let config = r#"# This model is NOT an ensemble; the line below is a comment.
# ensemble: would be here if it were one
max_batch_size: 4
batch_timeout: 0.05
"#;
        tokio::fs::write(model_dir.join("config.yaml"), config)
            .await
            .unwrap();
        tokio::fs::write(model_dir.join("model.py"), "class MyAPI(LitAPI): pass")
            .await
            .unwrap();

        let result = scan_repo_models(&tmp).await;

        // BUG: the comment mentioning "ensemble:" triggers the string-contains
        // check. The directory contains model.py so it would be discovered
        // anyway, but the ensemble flag on the returned model is wrong.
        // We can't observe the ensemble flag directly (it's not in RepoModel),
        // but we can verify the model IS discovered (it should be, since
        // model.py exists). The real defect — that a comment could cause
        // incorrect ensemble classification — is demonstrated by the fact
        // that removing model.py would still cause discovery if the
        // string-contains check fires, but with model.py present it masks
        // the issue. See the next test for the pure false-positive case.
        assert_eq!(result.len(), 1, "model with model.py must be discovered");
        assert_eq!(result[0].name, "test_model");
        assert_eq!(result[0].version, "1");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// B1b: pure false positive — no model.py, only config.yaml with
    /// "ensemble:" in a comment. The string-contains check incorrectly
    /// treats this as an ensemble model and discovers it.
    #[tokio::test]
    async fn test_scan_repo_models_ensemble_detection_pure_false_positive() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ensemble-fp2-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("no_py_model").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();

        // config.yaml where "ensemble:" only appears in a YAML comment.
        // No model.py — a genuine ensemble would have `ensemble:` as a YAML
        // key, but this has it as a comment/description string.
        let config = r#"description: "This is not an ensemble model"
# ensemble:
#   steps: ...
max_batch_size: 4
"#;
        tokio::fs::write(model_dir.join("config.yaml"), config)
            .await
            .unwrap();

        let result = scan_repo_models(&tmp).await;

        // BUG: `content.contains("ensemble:")` matches the comment
        // "# ensemble:", so this directory is incorrectly discovered as
        // an ensemble model even though it has no model.py and the
        // ensemble key is commented out.
        assert!(
            result.is_empty(),
            "B1b REGRESSION: directory with ensemble: in a comment must NOT be \
             discovered as a model. scan_repo_models uses string-contains \
             which matches commented-out ensemble keys. Got {} model(s).",
            result.len()
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== P1/P2: watcher lifecycle events → reconcile trigger (control_mode = "auto") =====

    use crate::config::ModelConfig;
    use crate::registry::types::ModelType;

    fn build_test_worker_manager(
        repo: PathBuf,
        registry: Arc<ModelRegistry>,
    ) -> Arc<WorkerManager> {
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        Arc::new(WorkerManager::new(
            registry,
            repo,
            inference_queue,
            "warn".to_string(),
            callback_runner,
        ))
    }

    fn test_repo_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("lite-server-p12-{}-{}", tag, std::process::id()))
    }

    #[tokio::test]
    async fn auto_mode_new_version_triggers_reconcile_instead_of_loading() {
        let tmp = test_repo_dir("new");
        let version_dir = tmp.join("m").join("1");
        tokio::fs::create_dir_all(&version_dir).await.unwrap();
        tokio::fs::write(version_dir.join("config.yaml"), "hot_reload: true\n").await.unwrap();
        tokio::fs::write(version_dir.join("model.py"), "class M: pass").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let (tx, mut rx) = mpsc::channel::<()>(8);
        let trigger = Some(tx);
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![version_dir.join("config.yaml")],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ModelTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            registry.get("m", Some("1")).is_none(),
            "auto mode: watcher must not load versions directly — reconcile decides"
        );
        assert!(
            rx.try_recv().is_ok(),
            "auto mode: lifecycle event must trigger reconcile"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn auto_mode_removed_version_triggers_reconcile_instead_of_unloading() {
        let tmp = test_repo_dir("removed");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        // Version dir does NOT exist (already deleted); registry still lists it.
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register("m", "1", ModelConfig::default(), ModelType::LitAPI, tmp.join("m").join("1"))
            .unwrap();
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let (tx, mut rx) = mpsc::channel::<()>(8);
        let trigger = Some(tx);
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![tmp.join("m").join("1").join("model.py")],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ModelTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            registry.get("m", Some("1")).is_some(),
            "auto mode: watcher must not unload versions directly — reconcile decides"
        );
        assert!(
            rx.try_recv().is_ok(),
            "auto mode: removal must trigger reconcile"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn auto_mode_live_version_file_change_is_file_level_not_lifecycle() {
        let tmp = test_repo_dir("live");
        let version_dir = tmp.join("m").join("1");
        tokio::fs::create_dir_all(&version_dir).await.unwrap();
        let model_py = version_dir.join("model.py");
        tokio::fs::write(&model_py, "class M: pass").await.unwrap();

        // hot_reload=false: the file-level reload path is a no-op, so no
        // workers are spawned; the assertion targets the trigger channel.
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register("m", "1", ModelConfig::default(), ModelType::LitAPI, version_dir.clone())
            .unwrap();
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let (tx, mut rx) = mpsc::channel::<()>(8);
        let trigger = Some(tx);
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![model_py],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ModelTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            rx.try_recv().is_err(),
            "file change on a live version must go down the hot-reload path, not trigger reconcile"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn manual_mode_removed_version_unloads_directly() {
        // Regression: manual (non-auto) mode keeps the legacy direct-unload
        // behavior — the "dir gone → unload" invariant does not wait for a
        // reconciler that only runs at startup.
        let tmp = test_repo_dir("manual");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register("m", "1", ModelConfig::default(), ModelType::LitAPI, tmp.join("m").join("1"))
            .unwrap();
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let trigger: Option<mpsc::Sender<()>> = None;
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![tmp.join("m").join("1").join("model.py")],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ModelTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            registry.get("m", Some("1")).is_none(),
            "manual mode: removed version dir must be unloaded directly"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== P2: reconcile loop (tick resync + event trigger) =====

    #[tokio::test(start_paused = true)]
    async fn reconcile_loop_trigger_runs_despite_long_poll_interval() {
        let (tx, rx) = mpsc::channel::<()>(8);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = tokio::spawn(reconcile_loop(Duration::from_secs(3600), rx, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        }));

        tx.send(()).await.unwrap();
        // Auto-advances past the coalesce window; the 3600s tick never fires.
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "event trigger must run reconcile even with a 3600s poll interval"
        );
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_loop_coalesces_trigger_burst_into_one_run() {
        let (tx, rx) = mpsc::channel::<()>(8);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = tokio::spawn(reconcile_loop(Duration::from_secs(3600), rx, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        }));

        for _ in 0..5 {
            tx.send(()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "a burst of triggers within the coalesce window must produce one reconcile run"
        );
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_loop_ticks_without_triggers() {
        let (_tx, rx) = mpsc::channel::<()>(8);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = tokio::spawn(reconcile_loop(Duration::from_secs(1), rx, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        }));

        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "resync ticks must keep firing without triggers"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn file_watcher_forwards_events_when_always_forward_without_hot_reload() {
        // control_mode=auto: lifecycle events must reach the reconciler even
        // when no loaded model has hot_reload enabled.
        let tmp_dir_raw = std::env::temp_dir().join(format!("lite-server-fw-auto-{}", std::process::id()));
        tokio::fs::create_dir_all(&tmp_dir_raw).await.unwrap();
        let tmp_dir = tmp_dir_raw.canonicalize().unwrap();
        let sub_dir = tmp_dir.join("model").join("1");
        tokio::fs::create_dir_all(&sub_dir).await.unwrap();
        let model_py = sub_dir.join("model.py");
        tokio::fs::write(&model_py, "original").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let worker_manager = build_test_worker_manager(tmp_dir.clone(), registry);

        let (tx, mut rx) = mpsc::channel::<Vec<PathBuf>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload, true));

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        tokio::fs::write(&model_py, "modified").await.unwrap();

        let result = tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            rx.recv()
        ).await;
        assert!(
            result.is_ok(),
            "auto mode: watcher must forward events even with no hot_reload models"
        );

        handle.abort();
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }
}
