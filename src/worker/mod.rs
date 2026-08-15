pub mod protocol;

mod health;
mod hooks;
mod lifecycle;
mod process;
mod routing;

pub use hooks::execute_hook;
pub use routing::{pick_worker_random, pick_worker_skip_ejected};
pub(crate) use routing::pick_streaming_worker;

use crate::callback::CallbackRunner;
use crate::inference_queue::{
    model_version_key, parse_model_version_key, InferenceQueue, OutlierState, ReloadSignal,
    RespawnSignal,
};
use crate::registry::ModelRegistry;
use crate::transport::zmq::WorkerZmqClient;
use crate::worker::protocol::{InferenceResponse, RouteDecl};
use dashmap::DashMap;
use health::GrpcHealthHandle;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::info;

/// Global pending response map: uid -> oneshot sender
pub type PendingMap = Arc<DashMap<String, oneshot::Sender<InferenceResponse>>>;

/// Tracks fire-and-forget lifecycle hook tasks (execute_hook) so
/// WorkerManager::shutdown can abort them instead of leaving them dangling
/// past server teardown (L2). std Mutex: execute_hook is sync.
pub type HookTasks = Arc<std::sync::Mutex<tokio::task::JoinSet<()>>>;

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
    // Custom @route declarations per model version (phase 2). Keyed by
    // model_version_key; upserted at worker handshake, cleared on unload.
    route_table: Arc<RwLock<HashMap<String, Vec<RouteDecl>>>>,
    // Reload channel for max_requests auto-recycle
    reload_tx: mpsc::Sender<ReloadSignal>,
    reload_rx: tokio::sync::Mutex<Option<mpsc::Receiver<ReloadSignal>>>,

    // Respawn channel for health-check-kill-triggered worker restarts
    respawn_tx: mpsc::Sender<RespawnSignal>,
    respawn_rx: tokio::sync::Mutex<Option<mpsc::Receiver<RespawnSignal>>>,
    // Log level passed to Python workers
    log_level: String,
    // Callback runner for lifecycle events
    callback_runner: Arc<CallbackRunner>,
    // Status coordinator tasks per model version (phase 3). Reconciles
    // Ready/Degraded from the outlier-ejection state at the version's
    // health_check_interval cadence; no task when the interval is 0
    // (status then stays purely event-driven).
    status_coordinators: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
    // gRPC Health reporter, installed by start_grpc_server (phase 3).
    // None until the gRPC server is up; pushes are no-ops before that.
    grpc_health: GrpcHealthHandle,
    // Loopback HTTP base URL of this server (e.g. http://127.0.0.1:8000),
    // passed to workers as --server-http so @route handlers can query the
    // hosting server via ctx.server (phase 2b). None for unix-socket HTTP.
    server_http: Option<String>,
    // Grace period for draining in-flight requests on unload (§4.2).
    // Defaults to the server.timeout default; overridden via
    // `set_unload_grace` at startup.
    unload_grace: Duration,
    // Server-level tunables from server.yaml `tunables:` (worker stderr
    // diagnostics bounds, FILE_CHANGED hook timeout).
    server_tunables: crate::config::ServerTunables,
    // Whether worker-declared custom metrics are registered. From
    // `features.custom_metrics` (config.rs); false = registration skipped
    // (recording then no-ops on unregistered ids — prometheus.rs).
    custom_metrics: bool,
    // Fire-and-forget lifecycle hook tasks, aborted on shutdown (L2).
    hook_tasks: HookTasks,
    // Server-level `model_defaults` overrides, applied to configs re-read
    // from disk by reload_model (batch 0: the validate-then-swap disk path).
    model_defaults: crate::config::ModelTunables,
    /// B1: set true at shutdown start so worker monitors know to suppress
    /// ERROR-level "exited unexpectedly" for workers killed during drain.
    draining: Arc<std::sync::atomic::AtomicBool>,
    // P0 (D6): ensemble plan cache, invalidated from the lifecycle single
    // collection point (unload_version / reload_model, D23).
    ensemble_plans: Option<Arc<crate::ensemble::EnsemblePlanCache>>,
    // P10 (D40): global streaming-DAG semaphore (server.max_concurrent_streaming_dags).
    // None = unlimited (0) or bare unit-test WorkerManager.
    streaming_capacity: Option<Arc<crate::ensemble::StreamingCapacityState>>,
    // RN-14: per-stream chunk channel depth for newly constructed worker
    // stream channels (server.stream_channel_size).
    stream_channel_size: usize,
}

struct WorkerProcess {
    worker_id: u32,
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Resolves once the worker process has been reaped. unload/shutdown must
    /// await this so no orphaned worker survives to steal the re-bound socket.
    done_rx: Option<oneshot::Receiver<()>>,
    /// Max seconds to wait for the OS to reap this killed worker (§3).
    kill_timeout: Duration,
}

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
            route_table: Arc::new(RwLock::new(HashMap::new())),
            reload_tx,
            reload_rx: tokio::sync::Mutex::new(Some(reload_rx)),
            respawn_tx,
            respawn_rx: tokio::sync::Mutex::new(Some(respawn_rx)),
            log_level,
            callback_runner,
            status_coordinators: Arc::new(RwLock::new(HashMap::new())),
            grpc_health: Arc::new(RwLock::new(None)),
            server_http: None,
            unload_grace: Duration::from_secs(30),
            server_tunables: crate::config::ServerTunables::default(),
            custom_metrics: false,
            hook_tasks: Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            model_defaults: crate::config::ModelTunables::default(),
            // P0 (D6): ensemble plan cache. None when the caller did not
            // install one (unit-test WorkerManager::new) — execute_ensemble
            // then loads plans uncached (behaviour parity).
            ensemble_plans: None,
            // P10 (D40): installed on the production path (server/mod.rs)
            // from server.max_concurrent_streaming_dags; 0/None = unlimited.
            streaming_capacity: None,
            // RN-14: default matches ServerConfig; the production path
            // overrides via with_stream_channel_size.
            stream_channel_size: 64,
        }
    }

    /// P0 (D6): install the ensemble plan cache. Invalidated from the
    /// lifecycle single collection point (unload_version / reload_model,
    /// D23) — see `ensemble_plans` accessor.
    pub fn with_ensemble_plans(mut self, cache: Arc<crate::ensemble::EnsemblePlanCache>) -> Self {
        self.ensemble_plans = Some(cache);
        self
    }

    /// P0: accessor for the ensemble plan cache (None in tests that build a
    /// bare WorkerManager).
    pub fn ensemble_plans(&self) -> Option<Arc<crate::ensemble::EnsemblePlanCache>> {
        self.ensemble_plans.clone()
    }

    /// P10 (D40): install the global streaming-DAG semaphore.
    pub fn with_streaming_capacity(mut self, capacity: Arc<crate::ensemble::StreamingCapacityState>) -> Self {
        self.streaming_capacity = Some(capacity);
        self
    }

    /// P10 (D40): accessor for the streaming-DAG semaphore (None = unlimited).
    pub fn streaming_capacity(&self) -> Option<Arc<crate::ensemble::StreamingCapacityState>> {
        self.streaming_capacity.clone()
    }

    /// Set the loopback HTTP base URL workers receive as --server-http
    /// (ctx.server for @route handlers). Builder-style so existing
    /// WorkerManager::new call sites stay untouched.
    pub fn with_server_http(mut self, server_http: Option<String>) -> Self {
        self.server_http = server_http;
        self
    }

    /// Set the unload drain grace period (§4.2), from `server.timeout`.
    pub fn with_unload_grace(mut self, grace: Duration) -> Self {
        self.unload_grace = grace;
        self
    }

    /// Set server-level tunables (worker stderr diagnostics, FILE_CHANGED
    /// timeout). Builder-style, from `tunables:` in server.yaml.
    pub fn with_server_tunables(mut self, tunables: crate::config::ServerTunables) -> Self {
        self.server_tunables = tunables;
        self
    }

    /// Set whether worker-declared custom metrics are registered
    /// (`features.custom_metrics`).
    pub fn with_custom_metrics(mut self, enabled: bool) -> Self {
        self.custom_metrics = enabled;
        self
    }

    /// RN-14 (resource-leak-plan): per-stream chunk channel depth
    /// (`server.stream_channel_size`), applied to every worker stream channel
    /// created from now on. Values < 1 clamp to 1.
    pub fn with_stream_channel_size(mut self, size: usize) -> Self {
        self.stream_channel_size = size.max(1);
        self
    }

    /// Set server-level `model_defaults` overrides. reload_model re-reads
    /// config.yaml from disk and applies these before validating (batch 0) —
    /// the same application the initial load and reconcile perform, so a
    /// disk-triggered reload never drops CLI/global defaults (B4).
    pub fn with_model_defaults(mut self, model_defaults: crate::config::ModelTunables) -> Self {
        self.model_defaults = model_defaults;
        self
    }

    /// Accessor for the unified inference queue. Used by the gRPC service to
    /// route unary inference through the same queue as REST (#1), so gRPC
    /// inherits batching, least-loaded selection, outlier ejection, retry, and
    /// max_requests recycling.
    pub fn inference_queue(&self) -> &Arc<InferenceQueue> {
        &self.inference_queue
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

    /// Test-only hook: populate the zmq client map without spawning workers
    /// (unit tests that drive streaming handlers against a mock worker).
    #[cfg(test)]
    pub(crate) async fn insert_zmq_clients_for_test(
        &self,
        model_name: &str,
        version: &str,
        clients: Vec<Arc<WorkerZmqClient>>,
    ) {
        let key = model_version_key(model_name, version);
        self.zmq_clients.write().await.insert(key, clients);
    }

    pub async fn shutdown(&self) {
        // B1: signal all worker monitors that we're in draining mode so they
        // suppress spurious "exited unexpectedly" errors during teardown.
        self.draining.store(true, std::sync::atomic::Ordering::Relaxed);
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

        // L2: abort fire-and-forget hook tasks so they don't dangle past
        // server teardown; bounded reap so a task mid-abort doesn't stall us.
        {
            let mut set = self.hook_tasks.lock().unwrap_or_else(|e| e.into_inner());
            set.abort_all();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let drained = {
                let mut set = self.hook_tasks.lock().unwrap_or_else(|e| e.into_inner());
                while set.try_join_next().is_some() {}
                set.is_empty()
            };
            if drained || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
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
}
