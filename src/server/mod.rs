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
mod tmp_cleanup;
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

    /// RN-8 (resource-leak-plan): RAII pending guard. A handler panic unwinds
    /// through the middleware (axum/hyper do not catch_unwind), so a manual
    /// inc/dec pair would leak the count and graceful shutdown would stall on
    /// the backstop timeout. The guard decrements on drop, panic or not.
    pub fn pending_guard(self: &Arc<Self>) -> PendingGuard {
        self.inc_pending();
        PendingGuard {
            state: Arc::clone(self),
        }
    }

    pub fn pending(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }
}

/// RN-8: see `ShutdownState::pending_guard`.
pub struct PendingGuard {
    state: Arc<ShutdownState>,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.state.dec_pending();
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
pub(crate) fn parse_lenient_semver(v: &str) -> Option<semver::Version> {
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
pub(crate) fn pick_latest_version(versions: &[String]) -> Option<String> {
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
        // P8-1: one shared sequence→worker affinity map for the queue path and
        // the streaming direct path (the latter reaches it via
        // `inference_queue.sequence_registry()`). Built from server config.
        let sequence_registry = Arc::new(crate::sequence::SequenceRegistry::new(
            std::time::Duration::from_secs_f64(config.server.sequence_ttl_secs as f64),
            config.server.max_sequences,
        ));
        let balance = crate::inference_queue::BalanceConfig {
            abs_threshold: config.server.balance_abs_threshold,
            rel_threshold: config.server.balance_rel_threshold,
        };
        let inference_queue = Arc::new(InferenceQueue::with_sequence(
            sequence_registry.clone(),
            balance,
        ));
        let callback_runner = Arc::new({
            // C1: per-callback timeout from config (0 = off).
            let mut r = CallbackRunner::new();
            r.set_dispatch_timeout(
                crate::deadline::idle_budget(config.callbacks.timeout_secs),
            );
            r
        });
        // P0 (D6): install the ensemble plan cache on the production path;
        // lifecycle unload/reload invalidates it (D23 single collection point).
        let ensemble_plans = Arc::new(crate::ensemble::EnsemblePlanCache::new());
        // P10 (D40): global streaming-DAG semaphore; 0 = unlimited (no cap).
        let streaming_capacity = (config.server.max_concurrent_streaming_dags > 0).then(|| {
            Arc::new(crate::ensemble::StreamingCapacityState::new(
                config.server.max_concurrent_streaming_dags,
            ))
        });
        let mut wm = WorkerManager::new(
            registry.clone(),
            repo_path,
            inference_queue.clone(),
            config.logging.level.clone(),
            callback_runner.clone(),
        ).with_server_http(Self::loopback_http_base(&config))
         .with_unload_grace(Duration::from_secs_f32(config.server.timeout))
         .with_server_tunables(config.tunables.clone())
         .with_custom_metrics(config.features.custom_metrics)
         .with_model_defaults(config.model_defaults.clone())
         .with_stream_channel_size(config.server.stream_channel_size)
         .with_ensemble_plans(ensemble_plans);
        if let Some(capacity) = streaming_capacity {
            wm = wm.with_streaming_capacity(capacity);
        }
        let worker_manager = Arc::new(wm);

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

    pub async fn run(
        &self,
        shutdown_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> Result<(), AppError> {
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

        // P8-1: reap expired / over-cap sequence→worker affinity entries every
        // 60s so unauthenticated sequence hints cannot grow the map unbounded.
        {
            let seq_reg = self.inference_queue.sequence_registry().clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    tick.tick().await;
                    let removed = seq_reg.cleanup();
                    if removed > 0 {
                        tracing::debug!(removed, "sequence registry: reaped stale affinity entries");
                    }
                }
            });
        }

        // Start reload listener for max_requests auto-recycle
        self.worker_manager.start_reload_listener().await;

        // Start respawn listener for health-check-kill-triggered worker restarts
        self.worker_manager.start_respawn_listener().await;

        // H7: sweep crash leftovers (staging dirs, swap backups, stale pack
        // temp dirs, dead-pid sockets) before the initial model load — a
        // restored swap backup must be visible to the scanner.
        let (cleaned, restored) = tmp_cleanup::startup_tmp_cleanup(
            &std::path::PathBuf::from(&self.config.model_repository.path),
            &std::env::temp_dir(),
            std::time::SystemTime::now(),
            tmp_cleanup::DEFAULT_MAX_AGE,
        )
        .await;
        if cleaned > 0 || restored > 0 {
            info!(removed = cleaned, restored, "startup tmp cleanup swept crash residue");
        }

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
            // The watcher still runs (unconditionally, below) — without
            // hot_reload models it gates events: lifecycle-only (dir-gone
            // unloads, auto-mode reconcile).
            info!("Hot reload: no models with hot_reload enabled — watcher active for lifecycle events only");
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
            // K2: ka <= 0 disables keep-alive — on the TLS path that means
            // dropping the h2 ALPN offer (h2 has no Connection: close).
            Some(s) => Some(Arc::new(crate::tls::TlsConfigStore::load_with_h1_only(
                &s,
                crate::tls::TlsProtocol::Http,
                self.config.server.keepalive_timeout <= 0.0,
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
            http::HttpServerOptions {
                config: self.config.clone(),
                registry: self.registry.clone(),
                worker_manager: self.worker_manager.clone(),
                inference_queue: self.inference_queue.clone(),
                shutdown_state: shutdown_state.clone(),
                draining: draining_for_http,
                callback_runner: self.callback_runner.clone(),
                has_hot_reload: has_hot_reload_for_http,
                rate_limiter: rate_limiter.clone(),
                tls: http_tls.clone(),
            },
            http_shutdown_rx,
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
                crate::grpc::GrpcServerOptions {
                    host: grpc_host,
                    port: self.config.server.grpc_port,
                    registry: self.registry.clone(),
                    worker_manager: self.worker_manager.clone(),
                    streaming_metrics: self.config.features.streaming_metrics,
                    canary_override: self.config.features.canary_override,
                    callback_runner: self.callback_runner.clone(),
                    shutdown_state: shutdown_state.clone(),
                    server_timeout: Duration::from_secs_f64(self.config.server.timeout as f64),
                    grpc_config: self.config.grpc.clone(),
                    rate_limiter: rate_limiter.clone(),
                    tls: grpc_tls.clone(),
                    config: self.config.clone(),
                    has_hot_reload: has_hot_reload.clone(),
                },
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

        // Start timeline sampler (uses list_loaded_keys to avoid cloning ModelVersion).
        // No-op when features.timeline is off: the task returns immediately and the
        // abort below is safe on a completed handle. The /metrics/timeline routes
        // are also unmounted (routes.rs), so sampling would serve no one.
        let timeline_enabled = self.config.features.timeline;
        let registry_for_timeline = self.registry.clone();
        let timeline_handle = tokio::spawn(async move {
            if !timeline_enabled {
                return;
            }
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
        let (watch_tx, mut watch_rx) = mpsc::channel::<Vec<(PathBuf, WatchEventKind)>>(32);
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
            while let Some(events) = watch_rx.recv().await {
                if let Err(e) = process_watch_events(
                    events,
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

        // Wait for any server to exit or shutdown signal.
        //
        // B1: a startup error (e.g. a port-bind conflict) must NOT `return Err`
        // directly — that skips the graceful teardown below and leaves the ZMQ
        // worker actors (spawn_blocking loops that exit only when their command
        // channel closes) live. On the Python CLI path the tokio Runtime then
        // drops normally and BlockingPool::shutdown dead-locks waiting for those
        // actors, wedging the process with no error and orphaned workers. The
        // binary path dodges this only because main.rs exits via
        // std::process::exit(1). Capture the error here and fall through to the
        // same teardown a normal shutdown runs (it unloads workers, which clears
        // the ZMQ client map in unload_version and lets the actors exit), then
        // return Err at the end.
        let mut startup_error: Option<AppError> = None;
        // Programmatic stop: `stop_server()` (Python) signals this oneshot from
        // another thread. Hoisted as `async move` — an inline block can't move
        // `shutdown_rx` by value into the select! arm. `None` (binary path)
        // never resolves.
        let stop_fut = async move {
            if let Some(rx) = shutdown_rx {
                let _ = rx.await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let shutdown_reason = tokio::select! {
            _ = stop_fut => "stop_server".to_string(),
            result = &mut http_handle => {
                match result {
                    Ok(Ok(())) => "http_server_finished".to_string(),
                    Ok(Err(e)) => { startup_error = Some(e); "http_server_error".to_string() }
                    Err(e) => {
                        startup_error = Some(AppError::Internal(format!("HTTP task panicked: {}", e)));
                        "http_server_panic".to_string()
                    }
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
                    Ok(Err(e)) => { startup_error = Some(e); "metrics_server_error".to_string() }
                    Err(e) => {
                        startup_error = Some(AppError::Internal(format!("Metrics task panicked: {}", e)));
                        "metrics_server_panic".to_string()
                    }
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
                    Ok(Err(e)) => { startup_error = Some(e); "grpc_server_error".to_string() }
                    Err(e) => {
                        startup_error = Some(AppError::Internal(format!("gRPC task panicked: {}", e)));
                        "grpc_server_panic".to_string()
                    }
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
        // B1: only drain on a graceful shutdown. On a startup error the failing
        // server handle already completed and re-polling its JoinHandle panics
        // ("JoinHandle polled after completion"); there is also nothing in-flight
        // to drain since the server never served. Dropping the drain futures on
        // the error path also releases the handle borrows before the abort()
        // calls below.
        if startup_error.is_none() {
            tokio::join!(http_fut, grpc_fut);
        } else {
            drop(http_fut);
            drop(grpc_fut);
        }

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

        // P-TRACE: flush OTel traces/metrics before reaping workers. The 0.30
        // BatchSpanProcessor/PeriodicReader run on dedicated threads; force_flush +
        // shutdown run on a blocking thread with a timeout cap so a slow collector
        // cannot stall the graceful-shutdown window (蓝图 §4.3 force_flush 带超时;
        // avoids the Drop-timeout deadlock in opentelemetry-rust #2715).
        crate::telemetry::shutdown().await;

        // cache_registry: snapshot strategy + active-version pins before
        // worker_manager.shutdown() unloads everything (registry.remove empties
        // the version table). A failure here must never block shutdown.
        if self.config.server.cache_registry {
            let repo_path = PathBuf::from(&self.config.model_repository.path);
            if let Err(e) = crate::registry::cache::save(&self.registry, &repo_path).await {
                warn!("registry cache snapshot failed: {e}");
            }
        }

        self.worker_manager.shutdown().await;

        // B1: surface a captured startup error now that the teardown above has
        // released the worker/ZMQ resources (so the Runtime can drop cleanly on
        // the Python CLI path instead of dead-locking on live ZMQ actors).
        if let Some(e) = startup_error {
            return Err(e);
        }
        Ok(())
    }

    /// Check if any loaded model has hot_reload enabled.
    fn registry_has_hot_reload_models(&self) -> bool {
        self.registry.list_loaded().iter().any(|(_, _, mv)| mv.config.hot_reload)
    }

    async fn load_initial_models(&self) -> Result<HashSet<(PathBuf, std::time::SystemTime)>, AppError> {
        let repo_path = PathBuf::from(&self.config.model_repository.path);
        let orch = &self.config.orchestration;

        // cache_registry: restore strategy + active-version pins from the on-disk
        // snapshot BEFORE the config-strategy loop, so config wins for strategy
        // and reconcile's default_version branch can still override the pin. The
        // version table is intentionally not restored (see registry::cache).
        if self.config.server.cache_registry {
            let restored = crate::registry::cache::restore(&self.registry, &repo_path).await;
            if restored > 0 {
                info!(restored, "restored model strategies from registry cache");
            }
        }

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

    /// RN-8 (resource-leak-plan): the pending guard must decrement on drop —
    /// including the unwind path (a handler panic must not leak the in-flight
    /// count; graceful shutdown waits on it).
    #[test]
    fn test_rn8_pending_guard_decrements_on_drop_and_unwind() {
        let state = Arc::new(ShutdownState::new());
        {
            let _g = state.pending_guard();
            assert_eq!(state.pending(), 1);
        }
        assert_eq!(state.pending(), 0, "guard drop must decrement");

        let state2 = Arc::clone(&state);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _g = state2.pending_guard();
            panic!("simulated handler panic");
        }));
        assert!(result.is_err());
        assert_eq!(state.pending(), 0, "unwind must still decrement");
    }
}
