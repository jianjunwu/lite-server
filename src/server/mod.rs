use crate::callback::{CallbackRunner, ServerContext};
use crate::config::Config;
use crate::error::AppError;
use crate::http;
use crate::inference_queue::InferenceQueue;
use crate::metrics::prometheus;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};
use tracing::{info, error, warn};

mod reconcile;
mod scanner;
mod watcher;
use reconcile::*;
use watcher::*;

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
        // 有意的 Mutex poison recovery:即使持锁线程 panic,也要继续记录关停时间,
        // 不属于规范禁止的 unwrap 路径(不会 panic)
        *self.start_time.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }

    pub fn elapsed(&self) -> Option<Duration> {
        // 同上:poison recovery,非 panic 路径
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
         .with_unload_grace(Duration::from_secs_f32(config.server.timeout))
         .with_server_tunables(config.tunables.clone()));

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
        // P2-1 扩展（D32）：GIE/EPP 语义 gauge，命名经 metrics.metric_namespace
        // 可配置；非法 namespace 启动快速失败。
        prometheus::register_gie_metrics(&self.config.metrics.metric_namespace)
            .map_err(|e| AppError::Config(format!("invalid metrics.metric_namespace: {}", e)))?;

        // P3-1：共享 RateLimiter 构造上移（HTTP/gRPC 同一实例 + 60s cleanup）。
        let rate_limiter = std::sync::Arc::new(crate::rate_limit::RateLimiter::new(
            self.config.rate_limit.max_buckets,
        ));
        {
            let limiter = rate_limiter.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    tick.tick().await;
                    let removed = limiter.cleanup_stale(std::time::Duration::from_secs(600));
                    if removed > 0 {
                        tracing::debug!(removed, "rate limiter: evicted stale buckets");
                    }
                }
            });
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
        // C3 (P4-2): shared draining flag — set true at the start of graceful
        // shutdown so /livez, /readyz fail and new HTTP inference is rejected
        // (503) while in-flight work drains. gRPC health fail-fast is driven
        // separately via WorkerManager::mark_draining.
        let draining = Arc::new(AtomicBool::new(false));

        // P5-1: build rotating TLS stores before binding anything — invalid
        // PEM / key mismatch / empty CA bundle fail startup here (fail fast).
        // Structural checks (pairing, UDS exclusivity) already ran in
        // Config::validate; tls_settings() is None unless cert+key are set.
        let http_tls = match self.config.server.tls_settings() {
            Some(s) => Some(Arc::new(crate::tls::TlsConfigStore::load(
                &s,
                crate::tls::TlsProtocol::Http,
            )?)),
            None => None,
        };
        let grpc_tls = if self.config.grpc.enabled {
            match self.config.grpc.tls_settings() {
                Some(s) => Some(Arc::new(crate::tls::TlsConfigStore::load(
                    &s,
                    crate::tls::TlsProtocol::Grpc,
                )?)),
                None => None,
            }
        } else {
            None
        };

        // Start HTTP server as spawned task with graceful shutdown channel
        let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
        let has_hot_reload_for_http = has_hot_reload.clone();
        let draining_for_http = draining.clone();
        let mut http_handle = tokio::spawn(http::start_http_server(
            self.config.clone(),
            self.registry.clone(),
            self.worker_manager.clone(),
            self.inference_queue.clone(),
            http_shutdown_rx,
            shutdown_state.clone(),
            draining_for_http,
            self.callback_runner.clone(),
            has_hot_reload_for_http,
            rate_limiter.clone(),
            http_tls.clone(),
        ));

        // metrics always needs a TCP host (UDS not supported); when HTTP uses a
        // Unix socket, fall back to loopback. gRPC resolves its own bind target
        // from grpc.host / server.host (P4-1, may itself be a `unix:/path`).
        let tcp_host = if crate::config::unix_socket_path(&self.config.server.host).is_some() {
            "127.0.0.1".to_string()
        } else {
            self.config.server.host.clone()
        };
        let grpc_host = crate::grpc::resolve_grpc_host(
            self.config.grpc.host.as_deref(),
            &self.config.server.host,
        );

        // Start metrics server if enabled
        let mut metrics_handle = if self.config.metrics.enabled {
            Some(tokio::spawn(start_metrics_server(
                tcp_host.clone(),
                self.config.server.metrics_port,
            )))
        } else {
            None
        };

        // Start gRPC server if enabled (P4-2: graceful-shutdown channel mirrors
        // HTTP's; sent in parallel during drain).
        let (grpc_shutdown_tx, grpc_shutdown_rx) = tokio::sync::oneshot::channel();
        let mut grpc_handle = if self.config.grpc.enabled {
            Some(tokio::spawn(crate::grpc::start_grpc_server(
                grpc_host,
                self.config.server.grpc_port,
                self.registry.clone(),
                self.worker_manager.clone(),
                self.config.features.streaming_metrics,
                self.config.features.canary_override,
                self.callback_runner.clone(),
                shutdown_state.clone(),
                Duration::from_secs_f64(self.config.server.timeout as f64),
                self.config.grpc.clone(),
                rate_limiter.clone(),
                grpc_tls.clone(),
                self.config.clone(),
                has_hot_reload.clone(),
                grpc_shutdown_rx,
            )))
        } else {
            None
        };

        // P5-1 证书热轮换（蓝图 D28）：SIGHUP 立即触发 + 10s 内容轮询兜底
        // （覆盖 k8s secret symlink 交换）；任何一侧启用 TLS 才启动。
        crate::tls::spawn_cert_reloader(
            [http_tls.clone(), grpc_tls.clone()]
                .into_iter()
                .flatten()
                .collect(),
            std::time::Duration::from_secs(10),
        );

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
            Duration::from_secs_f32(self.config.tunables.watcher_debounce_secs),
            self.registry.clone(),
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
                    self.config.tunables.clone(),
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
        let server_tunables = self.config.tunables.clone();
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
                    &server_tunables,
                    &has_hot_reload_for_reload,
                    &reconcile_tx,
                ).await {
                    warn!("Hot reload processing error: {}", e);
                }
            }
        });

        // Self-terminate if our parent dies — robust to the parent being
        // SIGKILLed (atexit/Drop can't catch SIGKILL). A surviving child polls
        // its parent pid; once reparented, it falls through to the same
        // graceful-shutdown path below (which reaps python workers). Gated by
        // a test-only env flag; production never sets it, so the watchdog
        // future is disabled (never polled) there.
        let die_with_parent = std::env::var("LITESERVER_DIE_WITH_PARENT")
            .map(|v| v == "1")
            .unwrap_or(false);
        #[cfg(unix)]
        let parent_pid = unsafe { libc::getppid() };
        #[cfg(not(unix))]
        let parent_pid = 0i32;

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
            _ = parent_died(parent_pid), if die_with_parent => "parent_died".to_string(),
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

        // C3 (P4-2): fail health fast so the LB stops sending new traffic before
        // the drain window — /livez, /readyz return 503 (HTTP draining flag) and
        // the gRPC overall Health service goes NOT_SERVING (mark_draining pushes
        // immediately rather than waiting for the next coordinator tick).
        draining.store(true, Ordering::Relaxed);
        self.worker_manager.mark_draining().await;

        // Abort background tasks
        watcher_handle.abort();
        timeline_handle.abort();
        reload_handle.abort();
        if let Some(h) = poller_handle {
            h.abort();
        }

        // Notify HTTP and gRPC servers to start graceful shutdown (drain in
        // parallel). gRPC serve_with_shutdown stops accepting new streams and
        // sends GOAWAY; HTTP stops accepting new connections. C3 (P4-2): the
        // draining flag set above also fails readyz/livez + gRPC health so the
        // LB stops sending new traffic during the drain window.
        let _ = http_shutdown_tx.send(());
        let _ = grpc_shutdown_tx.send(());

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

        // Drain HTTP and gRPC in parallel, each bounded by graceful_timeout;
        // anything still running after the grace window is force-aborted below.
        let graceful_timeout = Duration::from_secs_f32(self.config.server.graceful_timeout);
        let http_fut = async {
            match tokio::time::timeout(graceful_timeout, &mut http_handle).await {
                Ok(Ok(Ok(()))) => info!("HTTP server shut down gracefully"),
                Ok(Ok(Err(e))) => error!("HTTP server error during shutdown: {}", e),
                Ok(Err(e)) => error!("HTTP task panicked during shutdown: {}", e),
                Err(_) => warn!(
                    "HTTP server graceful shutdown timed out after {}s ({} requests still pending)",
                    self.config.server.graceful_timeout,
                    shutdown_state.pending()
                ),
            }
        };
        let grpc_fut = async {
            if let Some(h) = grpc_handle.as_mut() {
                match tokio::time::timeout(graceful_timeout, h).await {
                    Ok(Ok(Ok(()))) => info!("gRPC server shut down gracefully"),
                    Ok(Ok(Err(e))) => error!("gRPC server error during shutdown: {}", e),
                    Ok(Err(e)) => error!("gRPC task panicked during shutdown: {}", e),
                    Err(_) => warn!(
                        "gRPC server graceful shutdown timed out after {}s",
                        self.config.server.graceful_timeout
                    ),
                }
            }
        };
        tokio::join!(http_fut, grpc_fut);

        monitor_handle.abort();

        // Abort metrics (no graceful protocol). Force-abort HTTP/gRPC if the
        // grace window expired mid-drain.
        if let Some(h) = metrics_handle {
            h.abort();
        }
        http_handle.abort();
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
            &self.config.tunables,
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

/// Resolve once this process is reparented — i.e. its original parent died.
/// Polls `getppid` at 1Hz: one trivial syscall in a standalone task, never on
/// the inference hot path. Used only when `LITESERVER_DIE_WITH_PARENT` is set
/// (test-only), so production never polls this.
async fn parent_died(parent_pid: i32) {
    #[cfg(unix)]
    {
        // Race closure: if the parent died during our fork→exec→run startup
        // (before we could capture its real pid), getppid() is already 1 and a
        // naive "getppid() != parent_pid" watcher would never fire. Treat
        // already-orphaned (captured ppid 1) as "parent dead" → exit now.
        // (ppid 0 means we ourselves are init — no parent to watch; the loop's
        // `getppid() != 0` is always false, correctly never firing.)
        if parent_pid == 1 {
            return;
        }
        loop {
            if unsafe { libc::getppid() } != parent_pid {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    #[cfg(not(unix))]
    {
        // No portable parent-death signal on Windows; the flag is test-only
        // (unix), so this branch is effectively unused. Never resolve.
        let _ = parent_pid;
        std::future::pending::<()>().await;
    }
}

#[cfg(windows)]
async fn shutdown_signal() {
    signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    info!("signal received, starting graceful shutdown");
}







#[cfg(test)]
mod tests {
    use super::*;

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
}
