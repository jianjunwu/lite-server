use bytes::Bytes;
use crate::config::ModelConfig;
use crate::error::AppError;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::WorkerInfo;
use crate::transport::zmq::WorkerZmqClient;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Pre-sized key for model_version lookups — single allocation, no reallocation.
/// Uses null-byte separator (\x00) which cannot appear in validated model names or versions.
/// Used on the hot path (try_submit / has_queue) to avoid repeated format! overhead.
#[inline]
pub fn model_version_key(model: &str, version: &str) -> String {
    let mut key = String::with_capacity(model.len() + 1 + version.len());
    key.push_str(model);
    key.push('\x00');
    key.push_str(version);
    key
}

/// Parse a model_version key back into (model_name, version).
/// The separator is \x00 which cannot appear in validated identifiers.
#[inline]
pub fn parse_model_version_key(key: &str) -> (&str, &str) {
    let mut parts = key.splitn(2, '\x00');
    (parts.next().unwrap_or(""), parts.next().unwrap_or(""))
}

/// Compute jittered max_requests to prevent thundering herd on worker recycle.
/// When jitter > 0, the actual threshold is `max_requests ± random(0, jitter)`.
/// When max_requests is 0 (disabled) or jitter is 0, returns the original value.
pub fn compute_jittered_max_requests(max_requests: usize, jitter: usize) -> usize {
    if max_requests == 0 || jitter == 0 {
        return max_requests;
    }
    use rand::Rng;
    let offset = rand::thread_rng().gen_range(-(jitter as i64)..=(jitter as i64));
    (max_requests as i64 + offset).max(1) as usize
}

/// A single item waiting in the inference queue.
pub struct QueueItem {
    pub uid: String,
    pub data: Bytes, // JSON-encoded request body (zero-copy shared buffer)
    pub meta: Option<Arc<pb::RequestMeta>>, // shared via Arc (refcount-cloned)
    pub response_tx: oneshot::Sender<pb::Response>,
    /// RAII decrement for the per-version in-flight counter, attached by
    /// [`InferenceQueue::try_submit`] on acceptance. Drops with the item —
    /// after its response is sent, or when the item is discarded — so no
    /// completion path can leak the count (§4.2 graceful drain).
    pub inflight_guard: Option<InflightGuard>,
}

/// Decrements the per-version in-flight counter on drop (§4.2).
pub struct InflightGuard(Arc<AtomicUsize>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Error types for queue operations.
#[derive(Debug)]
pub enum QueueError {
    Closed,
    NotFound,
    Full,
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::Closed => write!(f, "queue closed"),
            QueueError::NotFound => write!(f, "queue not found for model"),
            QueueError::Full => write!(f, "queue full"),
        }
    }
}

// ===== Outlier Detection (inspired by Envoy) =====

/// Per-worker ejection state.
struct EjectedWorker {
    ejected_at: Instant,
}

/// Per-worker outlier detection tracking.
struct WorkerOutlier {
    consecutive_errors: AtomicUsize,
    ejected: Mutex<Option<EjectedWorker>>,
}

/// Configurable outlier-ejection parameters (§3). `Default` preserves the prior
/// hardcoded behavior, so existing `OutlierState::new` callers are unaffected.
#[derive(Clone)]
pub struct EjectionConfig {
    /// Consecutive errors before a worker is ejected. 0 = never eject.
    pub error_threshold: usize,
    /// How long a worker stays ejected before auto-recovery.
    pub timeout: Duration,
    /// Max % of workers ejectable at once (1-100).
    pub max_percent: usize,
}

impl Default for EjectionConfig {
    fn default() -> Self {
        Self {
            error_threshold: 3,
            timeout: Duration::from_secs(30),
            max_percent: 50,
        }
    }
}

/// Shared outlier detection state for a model version's workers.
pub struct OutlierState {
    workers: Vec<WorkerOutlier>,
    // Precomputed thresholds (read-only after construction)
    consecutive_threshold: usize,
    base_ejection_time: Duration,
    max_ejection_percent: usize,
}

impl OutlierState {
    /// Default ejection parameters (threshold 3, 30s, 50%) — prior behavior.
    pub fn new(num_workers: usize) -> Self {
        Self::with_config(num_workers, &EjectionConfig::default())
    }

    /// Configurable ejection parameters (§3). `error_threshold == 0` disables ejection.
    pub fn with_config(num_workers: usize, config: &EjectionConfig) -> Self {
        Self {
            workers: (0..num_workers)
                .map(|_| WorkerOutlier {
                    consecutive_errors: AtomicUsize::new(0),
                    ejected: Mutex::new(None),
                })
                .collect(),
            consecutive_threshold: config.error_threshold,
            base_ejection_time: config.timeout,
            max_ejection_percent: config.max_percent,
        }
    }

    /// Record a successful request — reset consecutive error count.
    pub fn record_success(&self, worker_idx: usize) {
        if let Some(w) = self.workers.get(worker_idx) {
            w.consecutive_errors.store(0, Ordering::Relaxed);
        }
    }

    /// Current consecutive error count for a worker (0 for unknown index).
    pub fn consecutive_errors(&self, worker_idx: usize) -> usize {
        self.workers
            .get(worker_idx)
            .map(|w| w.consecutive_errors.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Zero the consecutive error count without touching ejection state.
    /// Used after escalating to kill+respawn so the kill signal is not
    /// re-sent every probe interval while the replacement starts.
    pub fn reset_error_count(&self, worker_idx: usize) {
        if let Some(w) = self.workers.get(worker_idx) {
            w.consecutive_errors.store(0, Ordering::Relaxed);
        }
    }

    /// Clear error count AND ejection state — used when the worker's process
    /// has been replaced (respawn), so the new process starts clean.
    pub fn reset(&self, worker_idx: usize) {
        if let Some(w) = self.workers.get(worker_idx) {
            w.consecutive_errors.store(0, Ordering::Relaxed);
            let mut guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
            *guard = None;
        }
    }

    /// Record an error — increment the consecutive error count, ejecting the
    /// worker at the threshold. Returns `true` only when this call newly
    /// ejects the worker, so callers can record the per-(model, version)
    /// ejection metric exactly once (§4.6).
    pub fn record_error(&self, worker_idx: usize) -> bool {
        let w = match self.workers.get(worker_idx) {
            Some(w) => w,
            None => return false,
        };
        let count = w.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
        // threshold == 0 ⇒ outlier ejection disabled (§3).
        self.consecutive_threshold > 0
            && count >= self.consecutive_threshold
            && self.maybe_eject(worker_idx)
    }

    /// Eject a worker if not already ejected and below max ejection percent.
    /// Returns `true` when this call performed the ejection.
    fn maybe_eject(&self, worker_idx: usize) -> bool {
        let w = &self.workers[worker_idx];
        let mut guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_some() {
            return false; // already ejected
        }
        // Count currently ejected workers (non-blocking, approximate is fine)
        let mut ejected_count = 0;
        for (i, other) in self.workers.iter().enumerate() {
            if i == worker_idx {
                continue;
            }
            let is_ej = match other.ejected.try_lock() {
                Ok(g) => g.is_some(),
                Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner().is_some(),
                Err(std::sync::TryLockError::WouldBlock) => false,
            };
            if is_ej {
                ejected_count += 1;
            }
        }
        let max_ejected = (self.workers.len() * self.max_ejection_percent / 100).max(1);
        if ejected_count < max_ejected {
            *guard = Some(EjectedWorker {
                ejected_at: Instant::now(),
            });
            info!("Worker {} ejected after {} consecutive errors", worker_idx, self.consecutive_threshold);
            return true;
        }
        false
    }

    /// Check if a worker is currently ejected, recovering if ejection time has passed.
    pub fn is_ejected(&self, worker_idx: usize) -> bool {
        let w = match self.workers.get(worker_idx) {
            Some(w) => w,
            None => return false,
        };
        let mut guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref ejected) = *guard {
            if ejected.ejected_at.elapsed() >= self.base_ejection_time {
                // Recovery: clear ejection state and reset errors
                *guard = None;
                w.consecutive_errors.store(0, Ordering::Relaxed);
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    /// Count currently active (non-ejected) workers.
    pub fn active_count(&self) -> usize {
        let mut count = 0;
        for w in &self.workers {
            let guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
            if guard.is_none() {
                count += 1;
            }
        }
        count
    }
}

/// Signal to trigger a model version reload (worker auto-recycle).
#[derive(Debug)]
pub struct ReloadSignal {
    pub model_name: String,
    /// The version whose queue hit max_requests. The listener must recycle
    /// exactly this version — not the model's active version.
    pub version: String,
}

/// Signal to kill + respawn a single worker (health-check kill escalation).
#[derive(Debug)]
pub struct RespawnSignal {
    pub model_name: String,
    pub version: String,
    pub worker_id: u32,
    /// Metric/log label identifying the trigger (e.g. "health_check").
    pub reason: &'static str,
}

/// Per-(model, version) queue state held in [`InferenceQueue::queues`].
/// Factored out of an anonymous tuple so the field types are self-documenting.
struct VersionQueue {
    tx: mpsc::Sender<QueueItem>,
    collector: std::sync::Arc<tokio::task::JoinHandle<()>>,
    outlier: Arc<OutlierState>,
    /// Health-checker task handle (abortable); `None` if health checks disabled.
    health_checker: Option<std::sync::Arc<tokio::task::JoinHandle<()>>>,
    /// Requests accepted but not yet completed (§4.2 graceful drain).
    inflight_requests: Arc<AtomicUsize>,
}

/// Handle to a draining queue (§4.2). The collector stays alive so
/// already-accepted items finish; call [`DrainHandle::wait_idle`] to await
/// completion, then [`DrainHandle::abort`] to stop the collector + health
/// checker for good.
pub struct DrainHandle {
    inflight_requests: Arc<AtomicUsize>,
    collector: std::sync::Arc<tokio::task::JoinHandle<()>>,
    health_checker: Option<std::sync::Arc<tokio::task::JoinHandle<()>>>,
}

impl DrainHandle {
    /// Wait until every accepted request has completed. No timeout of its
    /// own — callers wrap with `tokio::time::timeout` to enforce a grace
    /// period.
    pub async fn wait_idle(&self) {
        while self.inflight_requests.load(Ordering::Relaxed) > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Stop the collector and health checker.
    pub fn abort(self) {
        self.collector.abort();
        if let Some(h) = self.health_checker {
            h.abort();
        }
    }
}

/// Per-model-version inference queue with batch aggregation.
pub struct InferenceQueue {
    queues: DashMap<String, VersionQueue>,
}

impl Default for InferenceQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl InferenceQueue {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
        }
    }

    /// Register a model version and start its batch collector task.
    pub fn register_model(
        &self,
        model_name: &str,
        version: &str,
        config: &ModelConfig,
        workers: Vec<WorkerInfo>,
        zmq_clients: Vec<Arc<WorkerZmqClient>>,
        reload_tx: mpsc::Sender<ReloadSignal>,
        outlier: Arc<OutlierState>,
        respawn_tx: Option<mpsc::Sender<RespawnSignal>>,
    ) {
        let key = model_version_key(model_name, version);

        // Abort and remove stale collector + health checker if exists
        if let Some((_, v)) = self.queues.remove(&key) {
            v.collector.abort();
            if let Some(h) = v.health_checker {
                h.abort();
            }
        }

        let max_queue_size = config.max_queue_size.max(1);
        let (tx, rx) = mpsc::channel(max_queue_size);

        let max_batch = config.max_batch_size;
        let batch_timeout = Duration::from_secs_f64(config.batch_timeout as f64);
        let adaptive = config.adaptive_batching;
        let min_timeout = Duration::from_secs_f64(config.min_batch_timeout as f64);
        let queue_threshold = config.adaptive_queue_threshold;
        let request_timeout = Duration::from_secs_f64(config.request_timeout as f64);
        let max_requests = config.max_requests;
        let max_requests_jitter = config.max_requests_jitter;
        let max_retries = config.max_retries;
        let health_interval = Duration::from_secs_f64(config.health_check_interval as f64);
        let health_probe_timeout = Duration::from_secs_f64(config.health_check_timeout as f64);
        let health_kill_threshold = config.health_check_kill_threshold;
        if health_kill_threshold > 0 && health_interval <= Duration::ZERO {
            warn!(
                model = %model_name, version = %version,
                "health_check_kill_threshold is set but health_check_interval is 0 — kill escalation will never run"
            );
        }

        let worker_count = workers.len();
        let handle = tokio::spawn(batch_collector(
            rx,
            max_batch,
            batch_timeout,
            adaptive,
            min_timeout,
            queue_threshold,
            zmq_clients.clone(),
            model_name.to_string(),
            version.to_string(),
            request_timeout,
            max_requests,
            max_requests_jitter,
            reload_tx,
            max_retries,
            outlier.clone(),
        ));

        // Spawn health checker if interval > 0
        let health_handle = if health_interval > Duration::ZERO {
            Some(std::sync::Arc::new(tokio::spawn(health_checker(
                health_interval,
                health_probe_timeout,
                zmq_clients,
                outlier.clone(),
                model_name.to_string(),
                version.to_string(),
                health_kill_threshold,
                respawn_tx,
            ))))
        } else {
            None
        };

        self.queues.insert(
            key,
            VersionQueue {
                tx,
                collector: std::sync::Arc::new(handle),
                outlier,
                health_checker: health_handle,
                inflight_requests: Arc::new(AtomicUsize::new(0)),
            },
        );
        info!(
            model = %model_name,
            version = %version,
            workers = worker_count,
            max_batch = max_batch,
            "model registered"
        );
    }

    /// Begin a graceful drain (§4.2): remove the queue from the registry so
    /// new submissions fail with [`QueueError::NotFound`], but keep the
    /// collector alive to finish already-accepted items. The returned handle
    /// reports in-flight completion and owns the final abort.
    pub fn begin_drain(&self, model_name: &str, version: &str) -> Option<DrainHandle> {
        let key = model_version_key(model_name, version);
        self.queues.remove(&key).map(|(_, v)| {
            info!(model = %model_name, version = %version, "model queue draining");
            DrainHandle {
                inflight_requests: v.inflight_requests,
                collector: v.collector,
                health_checker: v.health_checker,
            }
        })
    }

    /// Submit a single request to the queue (non-blocking).
    /// Returns QueueError::Full immediately if the queue is at capacity.
    pub fn try_submit(
        &self,
        model_name: &str,
        version: &str,
        mut item: QueueItem,
    ) -> Result<(), QueueError> {
        let key = model_version_key(model_name, version);
        let (sender, counter) = {
            let entry = self
                .queues
                .get(&key)
                .ok_or(QueueError::NotFound)?;
            (entry.tx.clone(), entry.inflight_requests.clone())
        };
        // Count from acceptance (not dispatch) so a drain starting right after
        // try_submit returns still observes this request.
        counter.fetch_add(1, Ordering::Relaxed);
        item.inflight_guard = Some(InflightGuard(counter));
        sender
            .try_send(item)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    debug!(model = %model_name, version = %version, "queue full");
                    QueueError::Full
                }
                mpsc::error::TrySendError::Closed(_) => {
                    debug!(model = %model_name, version = %version, "queue closed");
                    QueueError::Closed
                }
            })
    }

    /// Check if a queue exists for the model version.
    pub fn has_queue(&self, model_name: &str, version: &str) -> bool {
        let key = model_version_key(model_name, version);
        self.queues.contains_key(&key)
    }

    /// Get outlier detection state for a model version.
    pub fn get_outlier_state(&self, model_name: &str, version: &str) -> Option<Arc<OutlierState>> {
        let key = model_version_key(model_name, version);
        self.queues.get(&key).map(|entry| entry.outlier.clone())
    }
}

/// Pick the worker with the lowest inflight count (least-loaded), skipping ejected workers.
fn pick_worker_least_loaded(
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    exclude: &[usize],
) -> usize {
    if inflight.len() == 1 {
        return 0;
    }

    let mut best_active: Option<(usize, usize)> = None;
    let mut best_ejected: Option<(usize, usize)> = None;
    let mut best_excluded: Option<(usize, usize)> = None;

    for (i, counter) in inflight.iter().enumerate() {
        let load = counter.load(Ordering::Relaxed);
        // `exclude` holds workers that already failed this retry cycle —
        // soft-avoid them so a retry lands on a different worker, but keep
        // them as last-resort fallback when every worker was tried.
        let slot = if exclude.contains(&i) {
            &mut best_excluded
        } else if outlier.is_ejected(i) {
            &mut best_ejected
        } else {
            &mut best_active
        };
        let replace = match *slot {
            None => true,
            Some((_, best_load)) => load < best_load,
        };
        if replace {
            *slot = Some((i, load));
        }
    }

    // Prefer active worker; then ejected; finally an excluded (just-failed) one
    best_active
        .or(best_ejected)
        .or(best_excluded)
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

/// Map one item of a worker BatchResponse to the pb::Response delivered to
/// its caller.  `resp_item` of None means the worker omitted this uid.
/// Per-item status / status_code / media_type / headers propagate so each
/// caller of an aggregated batch sees its own response metadata.
fn batch_item_to_single_response(
    uid: String,
    resp_item: Option<&pb::BatchItemResponse>,
    metrics: Option<pb::Metrics>,
) -> pb::Response {
    match resp_item {
        Some(item) => pb::Response {
            uid,
            payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                data: item.data.clone(),
                headers: item.headers.clone(),
                status: item.status.clone(),
                status_code: item.status_code,
                media_type: item.media_type.clone(),
            })),
            metrics,
        },
        None => pb::Response {
            uid,
            payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                status: Some(pb::Status {
                    code: "Error".to_string(),
                    message: "missing in batch response".to_string(),
                }),
                ..Default::default()
            })),
            metrics: None,
        },
    }
}

/// Failure of one dispatch attempt against a specific worker. The failed
/// items are left in the batch so the caller can retry them elsewhere.
struct BatchError {
    worker_idx: usize,
    code: String,
    message: String,
    /// The worker's original error response, forwarded verbatim when retries
    /// are exhausted so status_code / headers / metrics survive to the caller.
    /// Present only for Single-response worker errors.
    single: Option<(pb::SingleResponse, Option<pb::Metrics>)>,
}

/// Send an error response to every remaining item in `batch` and drain it.
fn fail_batch_items(batch: &mut Vec<QueueItem>, err: &BatchError) {
    for queue_item in batch.drain(..) {
        let (single, metrics) = match &err.single {
            Some((s, m)) => (s.clone(), m.clone()),
            None => (
                pb::SingleResponse {
                    data: Default::default(),
                    headers: Default::default(),
                    status: Some(pb::Status {
                        code: err.code.clone(),
                        message: err.message.clone(),
                    }),
                    ..Default::default()
                },
                None,
            ),
        };
        let _ = queue_item.response_tx.send(pb::Response {
            uid: queue_item.uid,
            payload: Some(pb::response::Payload::Single(single)),
            metrics,
        });
    }
}

/// Send a batch to a single worker. Returns Err on transport/worker failure.
/// On success, all items are drained and responses sent to callers.
/// On worker-level failure (timeout, transport error, 5xx/no-status Error,
/// malformed response), the items are LEFT in the batch for the caller to
/// retry; the returned [`BatchError`] carries what to report if retries are
/// exhausted. Errors that retrying cannot fix are NOT retried: a Single Error
/// with a 4xx status_code (request-level) is forwarded verbatim, and a Batch
/// response with per-item errors delivers each item's own response — both
/// drain the batch.
async fn do_send_batch(
    batch: &mut Vec<QueueItem>,
    zmq_clients: &[Arc<WorkerZmqClient>],
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    model_name: &str,
    version: &str,
    request_timeout: Duration,
    exclude: &[usize],
) -> Result<(), BatchError> {
    if batch.is_empty() {
        return Ok(());
    }

    let batch_size = batch.len();
    let worker_idx = pick_worker_least_loaded(inflight, outlier, exclude);
    debug!(
        model = %model_name,
        version = %version,
        batch_size = batch_size,
        worker_idx = worker_idx,
        "sending batch"
    );
    inflight[worker_idx].fetch_add(1, Ordering::Relaxed);
    let zmq_client = &zmq_clients[worker_idx];
    prometheus::observe_batch_size(model_name, version, batch_size);

    // Build protobuf request (Single if batch.len() == 1, else Batch)
    let request = if batch.len() == 1 {
        let item = &batch[0];
        pb::Request {
            uid: item.uid.clone(),
            meta: item.meta.as_deref().cloned(),
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: item.data.clone(),
            })),
        }
    } else {
        let items: Vec<pb::BatchItem> = batch
            .iter()
            .map(|item| pb::BatchItem {
                uid: item.uid.clone(),
                data: item.data.clone(),
            })
            .collect();
        pb::Request {
            uid: format!("batch-{}", uuid::Uuid::new_v4()),
            meta: batch.first().and_then(|i| i.meta.as_deref().cloned()),
            payload: Some(pb::request::Payload::Batch(pb::BatchRequest { items })),
        }
    };

    let send_start = Instant::now();
    // Single timeout layer: the configured request_timeout is passed down to
    // the ZMQ wait itself, so it is the real effective bound (previously the
    // outer wrapper raced a hardcoded inner timeout and the shorter one won).
    let result = if request_timeout > Duration::ZERO {
        zmq_client.send_with_timeout(request, request_timeout).await
    } else {
        zmq_client.send(request).await
    };
    prometheus::observe_inference_duration(model_name, version, send_start.elapsed().as_secs_f64());

    match result {
        Ok(resp) => {
            // Worker metrics are recorded once per logical request by the HTTP
            // handler (handlers.rs infer_handler / stream Done frames) — do NOT
            // record here, or every metric would be double-counted.
            match resp.payload {
                Some(pb::response::Payload::Batch(batch_resp)) => {
                    let resp_map: std::collections::HashMap<String, pb::BatchItemResponse> =
                        batch_resp
                            .items
                            .into_iter()
                            .map(|item| (item.uid.clone(), item))
                            .collect();

                    let mut all_ok = true;
                    for queue_item in batch.drain(..) {
                        let resp_item = resp_map.get(&queue_item.uid);
                        let item_failed = resp_item.is_none()
                            || resp_item
                                .and_then(|item| item.status.as_ref())
                                .map(|s| s.code.as_str())
                                == Some("Error");
                        if item_failed {
                            all_ok = false;
                        }
                        let single_resp = batch_item_to_single_response(
                            queue_item.uid,
                            resp_item,
                            resp.metrics.clone(),
                        );
                        let _ = queue_item.response_tx.send(single_resp);
                    }

                    inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                    if all_ok {
                        outlier.record_success(worker_idx);
                        debug!(model = %model_name, version = %version, worker_idx = worker_idx, "batch ok");
                        Ok(())
                    } else {
                        if outlier.record_error(worker_idx) {
                    prometheus::inc_worker_ejection(model_name, version);
                }
                        // Per-item responses (real errors included) were
                        // already delivered above — nothing left to retry.
                        Err(BatchError {
                            worker_idx,
                            code: "Error".to_string(),
                            message: "one or more batch items failed".to_string(),
                            single: None,
                        })
                    }
                }
                Some(pb::response::Payload::Single(single_resp)) => {
                    let is_error = single_resp.status.as_ref().map(|s| s.code.as_str()) == Some("Error");
                    if is_error && (400..500).contains(&single_resp.status_code) {
                        // Request-level error (e.g. BadRequestError) — deterministic,
                        // retrying on another worker can't help. Forward the
                        // worker's response verbatim (status_code/headers intact).
                        if let Some(queue_item) = batch.pop() {
                            let _ = queue_item.response_tx.send(pb::Response {
                                uid: queue_item.uid,
                                payload: Some(pb::response::Payload::Single(single_resp)),
                                metrics: resp.metrics,
                            });
                        }
                        inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                        if outlier.record_error(worker_idx) {
                    prometheus::inc_worker_ejection(model_name, version);
                }
                        Err(BatchError {
                            worker_idx,
                            code: "Error".to_string(),
                            message: "request-level error".to_string(),
                            single: None,
                        })
                    } else if is_error {
                        // Worker-level failure: keep the item for retry on
                        // another worker, carrying the worker's real response
                        // for verbatim delivery if retries are exhausted.
                        let st = single_resp.status.clone().unwrap_or_default();
                        inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                        if outlier.record_error(worker_idx) {
                    prometheus::inc_worker_ejection(model_name, version);
                }
                        Err(BatchError {
                            worker_idx,
                            code: st.code,
                            message: st.message,
                            single: Some((single_resp, resp.metrics)),
                        })
                    } else {
                        if let Some(queue_item) = batch.pop() {
                            let _ = queue_item.response_tx.send(pb::Response {
                                uid: queue_item.uid,
                                payload: Some(pb::response::Payload::Single(single_resp)),
                                metrics: resp.metrics,
                            });
                        }
                        inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                        outlier.record_success(worker_idx);
                        debug!(model = %model_name, version = %version, worker_idx = worker_idx, "batch ok");
                        Ok(())
                    }
                }
                _ => {
                    warn!("Unexpected response type from worker for {} {}", model_name, version);
                    inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                    if outlier.record_error(worker_idx) {
                    prometheus::inc_worker_ejection(model_name, version);
                }
                    Err(BatchError {
                        worker_idx,
                        code: "Error".to_string(),
                        message: "unexpected response type".to_string(),
                        single: None,
                    })
                }
            }
        }
        Err(e) => {
            error!(
                "Batch request failed for {} {}: {}",
                model_name, version, e
            );
            let (code, message) = if matches!(e, AppError::InferenceTimeout(_)) {
                ("Timeout".to_string(), "request timeout".to_string())
            } else {
                ("Error".to_string(), e.to_string())
            };
            inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
            if outlier.record_error(worker_idx) {
                prometheus::inc_worker_ejection(model_name, version);
            }
            Err(BatchError {
                worker_idx,
                code,
                message,
                single: None,
            })
        }
    }
}

/// Send a batch with retry on failure. Retries on a different worker (if available).
async fn send_batch_with_retry(
    mut batch: Vec<QueueItem>,
    zmq_clients: &[Arc<WorkerZmqClient>],
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    model_name: &str,
    version: &str,
    request_timeout: Duration,
    request_count: &AtomicUsize,
    max_requests: usize,
    reload_tx: &mpsc::Sender<ReloadSignal>,
    max_retries: usize,
) {
    if batch.is_empty() {
        return;
    }

    // Decrement queue depth once per item — retries must not decrement again
    for _ in 0..batch.len() {
        prometheus::dec_queue_depth(model_name, version);
    }

    let batch_size = batch.len();

    // Fast path: single worker, or retries disabled (max_retries == 0)
    if zmq_clients.len() <= 1 || max_retries == 0 {
        let result = do_send_batch(&mut batch, zmq_clients, inflight, outlier, model_name, version, request_timeout, &[]).await;
        match result {
            Ok(()) => check_max_requests(request_count, batch_size, max_requests, model_name, version, reload_tx).await,
            Err(e) => fail_batch_items(&mut batch, &e),
        }
        return;
    }

    let mut excluded: Vec<usize> = Vec::new();
    let mut last_err: Option<BatchError> = None;
    for attempt in 0..max_retries {
        if attempt > 0 {
            prometheus::inc_retry(model_name, version);
        }
        match do_send_batch(&mut batch, zmq_clients, inflight, outlier, model_name, version, request_timeout, &excluded).await {
            Ok(()) => {
                check_max_requests(request_count, batch_size, max_requests, model_name, version, reload_tx).await;
                return;
            }
            Err(e) => {
                if batch.is_empty() {
                    return; // per-item responses already delivered — nothing to retry
                }
                warn!(
                    model = %model_name,
                    version = %version,
                    attempt = attempt + 1,
                    worker_idx = e.worker_idx,
                    "batch failed, retrying on another worker"
                );
                excluded.push(e.worker_idx);
                last_err = Some(e);
            }
        }
    }
    // All retries exhausted — report the last failure to remaining items.
    if let Some(e) = last_err {
        fail_batch_items(&mut batch, &e);
    }
}

/// Check if worker hit max_requests and signal reload.
#[inline]
async fn check_max_requests(
    request_count: &AtomicUsize,
    batch_size: usize,
    max_requests: usize,
    model_name: &str,
    version: &str,
    reload_tx: &mpsc::Sender<ReloadSignal>,
) {
    if max_requests == 0 {
        return;
    }
    let prev = request_count.fetch_add(batch_size, Ordering::Relaxed);
    if prev < max_requests && prev + batch_size >= max_requests {
        info!("Worker hit max_requests ({}), signaling reload for {} {}", max_requests, model_name, version);
        let _ = reload_tx.try_send(ReloadSignal {
            model_name: model_name.to_string(),
            version: version.to_string(),
        });
    }
}

/// Compute adaptive batch timeout based on current batch size, queue depth, and config.
///
/// Algorithm:
/// - If batch is already full -> return zero (dispatch immediately)
/// - If batch is >50% full -> aggressively reduce timeout
/// - Scale timeout by queue pressure: more pending requests = shorter timeout
/// - Never go below min_timeout
pub fn compute_adaptive_timeout(
    batch_len: usize,
    queue_depth: usize,
    max_batch_size: usize,
    base_timeout: Duration,
    min_timeout: Duration,
    queue_threshold: usize,
) -> Duration {
    if batch_len >= max_batch_size {
        return Duration::ZERO;
    }

    // Fill ratio: how full is the current batch?
    let fill_ratio = batch_len as f32 / max_batch_size.max(1) as f32;

    // Queue pressure: how many items are waiting behind us?
    let queue_pressure = if queue_threshold > 0 {
        (queue_depth as f32 / queue_threshold as f32).min(1.0)
    } else {
        0.0
    };

    // Combined pressure: use the max of fill ratio and queue pressure
    let pressure = fill_ratio.max(queue_pressure);

    // Scale timeout inversely with pressure:
    // pressure 0.0 -> full timeout
    // pressure 0.5 -> half timeout
    // pressure 1.0 -> min timeout
    let scale = 1.0 - pressure * 0.99; // never go fully to 0, keep at least 1% for fairness
    let scaled = base_timeout.mul_f32(scale.max(0.0));

    scaled.max(min_timeout)
}

/// Background task that collects requests into batches and dispatches them.
async fn batch_collector(
    mut rx: mpsc::Receiver<QueueItem>,
    max_batch_size: usize,
    base_timeout: Duration,
    adaptive: bool,
    min_timeout: Duration,
    queue_threshold: usize,
    zmq_clients: Vec<Arc<WorkerZmqClient>>,
    model_name: String,
    version: String,
    request_timeout: Duration,
    max_requests: usize,
    max_requests_jitter: usize,
    reload_tx: mpsc::Sender<ReloadSignal>,
    max_retries: usize,
    outlier: Arc<OutlierState>,
) {
    let worker_inflight: Vec<Arc<AtomicUsize>> = (0..zmq_clients.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let request_count = Arc::new(AtomicUsize::new(0));

    // Compute jittered max_requests to prevent thundering herd on recycle
    let max_requests = compute_jittered_max_requests(max_requests, max_requests_jitter);

    if max_batch_size <= 1 {
        // Fast path: no batching, send immediately and concurrently
        while let Some(item) = rx.recv().await {
            prometheus::inc_queue_depth(&model_name, &version);
            let batch = vec![item];
            let zmq_clients = zmq_clients.clone();
            let worker_inflight = worker_inflight.clone();
            let outlier = outlier.clone();
            let model_name = model_name.clone();
            let version = version.clone();
            let request_count = request_count.clone();
            let reload_tx = reload_tx.clone();
            tokio::spawn(async move {
                send_batch_with_retry(batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries).await;
            });
        }
        return;
    }

    let mut batch: Vec<QueueItem> = Vec::with_capacity(max_batch_size);
    let mut deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            biased;
            Some(item) = rx.recv() => {
                prometheus::inc_queue_depth(&model_name, &version);
                batch.push(item);
                if batch.len() >= max_batch_size {
                    let current_batch = std::mem::take(&mut batch);
                    let zmq_clients = zmq_clients.clone();
                    let worker_inflight = worker_inflight.clone();
                    let outlier = outlier.clone();
                    let model_name = model_name.clone();
                    let version = version.clone();
                    let request_count = request_count.clone();
                    let reload_tx = reload_tx.clone();
                    tokio::spawn(async move {
                        send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries).await;
                    });
                    deadline = None;
                } else if deadline.is_none() {
                    let timeout = if adaptive {
                        let queue_depth = rx.len();
                        compute_adaptive_timeout(
                            batch.len(),
                            queue_depth,
                            max_batch_size,
                            base_timeout,
                            min_timeout,
                            queue_threshold,
                        )
                    } else {
                        base_timeout
                    };
                    deadline = Some(tokio::time::Instant::now() + timeout);
                }
            }
            _ = async {
                if let Some(d) = deadline {
                    tokio::time::sleep_until(d).await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if deadline.is_some() => {
                if !batch.is_empty() {
                    debug!(model = %model_name, version = %version, batch_size = batch.len(), "batch timeout");
                    let current_batch = std::mem::take(&mut batch);
                    let zmq_clients = zmq_clients.clone();
                    let worker_inflight = worker_inflight.clone();
                    let outlier = outlier.clone();
                    let model_name = model_name.clone();
                    let version = version.clone();
                    let request_count = request_count.clone();
                    let reload_tx = reload_tx.clone();
                    tokio::spawn(async move {
                        send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries).await;
                    });
                }
                deadline = None;
            }
            else => break,
        }
    }

    // Drain remaining items
    if !batch.is_empty() {
        let current_batch = std::mem::take(&mut batch);
        let zmq_clients = zmq_clients.clone();
        let worker_inflight = worker_inflight.clone();
        let outlier = outlier.clone();
        let model_name = model_name.clone();
        let version = version.clone();
        let request_count = request_count.clone();
        let reload_tx = reload_tx.clone();
        tokio::spawn(async move {
            send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries).await;
        });
    }
}

/// Background task that periodically probes workers for health.
/// Probes ALL workers concurrently (including ejected ones) for early recovery.
///
/// Failure handling is a two-level escalation on the shared consecutive-error
/// count: reaching the ejection threshold stops routing to the worker
/// ([`OutlierState`]); reaching `kill_threshold` (0 = never) kills + respawns
/// the process via `respawn_tx`.
async fn health_checker(
    interval: Duration,
    probe_timeout: Duration,
    zmq_clients: Vec<Arc<WorkerZmqClient>>,
    outlier: Arc<OutlierState>,
    model_name: String,
    version: String,
    kill_threshold: usize,
    respawn_tx: Option<mpsc::Sender<RespawnSignal>>,
) {
    let uid_prefix = format!("health-{}-{}", model_name, version);
    loop {
        tokio::time::sleep(interval).await;
        // Probe all workers concurrently to avoid O(N * timeout) sequential delay
        let futs: Vec<_> = zmq_clients.iter().enumerate().map(|(idx, client)| {
            let client = client.clone();
            let outlier = outlier.clone();
            let model_name = model_name.clone();
            let version = version.clone();
            let respawn_tx = respawn_tx.clone();
            let uid = format!("{}-{}", uid_prefix, idx);
            async move {
                let was_ejected = outlier.is_ejected(idx);
                let request = pb::Request {
                    uid,
                    meta: None,
                    payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                        data: Default::default(),
                    })),
                };
                let result = tokio::time::timeout(probe_timeout, client.send(request)).await;
                match result {
                    Ok(Ok(resp)) => {
                        let is_error = resp.payload.as_ref().and_then(|p| match p {
                            pb::response::Payload::Single(s) => s.status.as_ref(),
                            _ => None,
                        }).map(|s| s.code.as_str()) == Some("Error");
                        if is_error {
                            if outlier.record_error(idx) {
                                prometheus::inc_worker_ejection(&model_name, &version);
                            }
                            prometheus::inc_health_check(&model_name, &version, "error");
                            escalate_to_kill(&outlier, idx, kill_threshold, &respawn_tx, &model_name, &version).await;
                        } else {
                            outlier.record_success(idx);
                            prometheus::inc_health_check(&model_name, &version, "ok");
                            if was_ejected {
                                info!(
                                    "Worker {} for {} {} recovered via active health check",
                                    idx, model_name, version
                                );
                            }
                        }
                    }
                    Ok(Err(_)) | Err(_) => {
                        if outlier.record_error(idx) {
                            prometheus::inc_worker_ejection(&model_name, &version);
                        }
                        prometheus::inc_health_check(&model_name, &version, "error");
                        escalate_to_kill(&outlier, idx, kill_threshold, &respawn_tx, &model_name, &version).await;
                        if !was_ejected && outlier.is_ejected(idx) {
                            warn!(model = %model_name, version = %version, worker_idx = idx, "worker ejected");
                        }
                    }
                }
                prometheus::set_worker_health(
                    &model_name,
                    &version,
                    idx,
                    !outlier.is_ejected(idx),
                );
            }
        }).collect();
        futures::future::join_all(futs).await;
    }
}

/// Kill escalation after a failed probe: once the shared consecutive-error
/// count reaches `kill_threshold`, ask the worker manager to kill + respawn
/// the process. `kill_threshold == 0` disables killing (ejection-only).
async fn escalate_to_kill(
    outlier: &OutlierState,
    idx: usize,
    kill_threshold: usize,
    respawn_tx: &Option<mpsc::Sender<RespawnSignal>>,
    model_name: &str,
    version: &str,
) {
    if kill_threshold == 0 || outlier.consecutive_errors(idx) < kill_threshold {
        return;
    }
    let Some(tx) = respawn_tx.as_ref() else {
        return;
    };
    error!(
        model = %model_name, version = %version, worker_idx = idx,
        consecutive_failures = outlier.consecutive_errors(idx),
        kill_threshold,
        "Health probe failures reached kill threshold, respawning worker"
    );
    let _ = tx.send(RespawnSignal {
        model_name: model_name.to_string(),
        version: version.to_string(),
        worker_id: idx as u32,
        reason: "health_check",
    }).await;
    // Throttle re-sends while the respawn is in flight: the replacement must
    // accumulate kill_threshold fresh failures before the next escalation.
    // Ejection state is left intact so traffic keeps avoiding the dead slot.
    outlier.reset_error_count(idx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // ===== Key pre-computation tests =====

    #[test]
    fn model_version_key_format() {
        assert_eq!(model_version_key("my_model", "1"), "my_model\x001");
        assert_eq!(model_version_key("resnet", "v2"), "resnet\x00v2");
        assert_eq!(model_version_key("", ""), "\x00");
    }

    #[test]
    fn model_version_key_roundtrip() {
        // Test roundtrip with underscores in both name and version
        let test_cases = vec![
            ("simple", "1"),
            ("bert_base_2024", "03_v1"),   // underscored model & version
            ("my-model", "1.0.0-beta"),    // semver-style version
        ];
        for (model, version) in test_cases {
            let key = model_version_key(model, version);
            let (parsed_model, parsed_version) = parse_model_version_key(&key);
            assert_eq!(parsed_model, model,
                "model_name roundtrip failed for '{}_{}'", model, version);
            assert_eq!(parsed_version, version,
                "version roundtrip failed for '{}_{}'", model, version);
        }
    }

    #[test]
    fn model_version_key_single_allocation() {
        // String::with_capacity should pre-allocate exactly the right size
        let key = model_version_key("model", "1");
        assert_eq!(key.len(), key.capacity(), "no over-allocation");
    }

    #[tokio::test]
    async fn model_version_key_used_in_try_submit() {
        // Verify that try_submit internally uses the same key format
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            max_batch_size: 1,
            batch_timeout: 0.0,
            adaptive_batching: false,
            min_batch_timeout: 0.0,
            adaptive_queue_threshold: 0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let outlier = Arc::new(OutlierState::new(0));
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier, None);

        // has_queue should find it using the same key
        assert!(queue.has_queue("test_model", "1"));
        assert!(!queue.has_queue("test_model", "2"));
    }

    #[test]
    fn test_adaptive_timeout_full_batch() {
        let base = Duration::from_millis(50);
        let min = Duration::from_millis(1);
        let result = compute_adaptive_timeout(8, 0, 8, base, min, 10);
        assert_eq!(result, Duration::ZERO, "full batch should dispatch immediately");
    }

    #[test]
    fn test_adaptive_timeout_low_load() {
        let base = Duration::from_millis(50);
        let min = Duration::from_millis(1);
        // Empty batch, no queue -> should use ~full timeout
        let result = compute_adaptive_timeout(0, 0, 8, base, min, 10);
        assert!(result >= Duration::from_millis(49), "low load should use near-full timeout");
        // Allow tiny floating-point drift from mul_f32
        assert!(result <= base + Duration::from_micros(1), "should never exceed base timeout");
    }

    #[test]
    fn test_adaptive_timeout_high_queue_pressure() {
        let base = Duration::from_millis(50);
        let min = Duration::from_millis(1);
        // Empty batch but queue depth exceeds threshold -> should use min timeout
        let result = compute_adaptive_timeout(1, 20, 8, base, min, 10);
        assert_eq!(result, min, "high queue pressure should use min timeout");
    }

    #[test]
    fn test_adaptive_timeout_fill_pressure() {
        let base = Duration::from_millis(50);
        let min = Duration::from_millis(1);
        // Batch 50% full -> timeout should be significantly reduced
        let result = compute_adaptive_timeout(4, 0, 8, base, min, 10);
        assert!(result < base, "50% fill should reduce timeout");
        assert!(result >= min, "should respect min timeout");
    }

    #[test]
    fn test_adaptive_timeout_respects_minimum() {
        let base = Duration::from_secs(1);
        let min = Duration::from_millis(5);
        // Extreme pressure: scale = 0.01 -> scaled = 10ms, which is > min (5ms)
        let result = compute_adaptive_timeout(7, 100, 8, base, min, 10);
        assert!(result >= min, "should never go below min_timeout");
        // At extreme pressure result should be close to min but not clamped unless scaled < min
        assert!(result < base / 2, "extreme pressure should significantly reduce timeout");
    }

    #[test]
    fn test_adaptive_timeout_zero_threshold() {
        let base = Duration::from_millis(50);
        let min = Duration::from_millis(1);
        // queue_threshold=0 disables queue pressure consideration
        let result = compute_adaptive_timeout(1, 100, 8, base, min, 0);
        assert!(result > min, "zero threshold should ignore queue depth");
    }

    #[test]
    fn test_adaptive_timeout_monotonic() {
        // More queue pressure should always yield <= less pressure
        let base = Duration::from_millis(100);
        let min = Duration::from_millis(1);
        let t1 = compute_adaptive_timeout(2, 0, 8, base, min, 10);
        let t2 = compute_adaptive_timeout(2, 5, 8, base, min, 10);
        let t3 = compute_adaptive_timeout(2, 10, 8, base, min, 10);
        assert!(t1 >= t2, "increasing queue depth should reduce timeout");
        assert!(t2 >= t3, "increasing queue depth should reduce timeout");
    }

    #[tokio::test]
    async fn test_register_model_aborts_old_collector() {
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            max_batch_size: 1,
            batch_timeout: 0.0,
            adaptive_batching: false,
            min_batch_timeout: 0.0,
            adaptive_queue_threshold: 0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);

        let outlier = Arc::new(OutlierState::new(0));
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx.clone(), outlier, None);
        let first_handle = queue.queues.get(&model_version_key("test_model", "1")).unwrap().collector.clone();
        assert!(!first_handle.is_finished());

        // Re-register should abort the first collector
        let outlier = Arc::new(OutlierState::new(0));
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier, None);
        let second_handle = queue.queues.get(&model_version_key("test_model", "1")).unwrap().collector.clone();

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(first_handle.is_finished(), "old collector should be aborted");
        assert!(!second_handle.is_finished(), "new collector should still be running");
    }

    // ===== Graceful drain tests (§4.2) =====

    fn drain_test_endpoint(name: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("lite-server-dq-{}-{}.sock", name, std::process::id()))
                    .display()
            )
        }
        #[cfg(windows)]
        {
            format!("tcp://127.0.0.1:{}", 34000 + std::process::id() % 1000)
        }
    }

    /// Spawn a PAIR-socket worker that answers every unary request with an
    /// empty Single response after `delay` (simulating in-flight work).
    fn spawn_echo_worker(endpoint: String, delay: Duration) -> std::thread::JoinHandle<()> {
        use prost::Message;
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                std::thread::sleep(delay);
                let resp = pb::Response {
                    uid: req.uid,
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: Bytes::new(),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        })
    }

    #[tokio::test]
    async fn test_begin_drain_rejects_new_but_completes_inflight() {
        let endpoint = drain_test_endpoint("drain");
        let _worker = spawn_echo_worker(endpoint.clone(), Duration::from_millis(300));

        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            max_batch_size: 1,
            batch_timeout: 0.0,
            adaptive_batching: false,
            min_batch_timeout: 0.0,
            adaptive_queue_threshold: 0,
            health_check_interval: 0.0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let outlier = Arc::new(OutlierState::new(1));
        let client = Arc::new(WorkerZmqClient::new(endpoint));
        queue.register_model("m", "1", &config, vec![], vec![client], reload_tx, outlier, None);
        // Let the PAIR connect establish.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Submit a request; the echo worker holds it ~300ms.
        let (resp_tx, resp_rx) = oneshot::channel();
        queue
            .try_submit("m", "1", QueueItem {
                uid: "inflight".to_string(),
                data: Bytes::new(),
                meta: None,
                response_tx: resp_tx,
                inflight_guard: None,
            })
            .unwrap();

        // Drain: new submissions are rejected immediately...
        let drain = queue.begin_drain("m", "1").expect("queue must exist");
        let (late_tx, _late_rx) = oneshot::channel();
        let err = queue
            .try_submit("m", "1", QueueItem {
                uid: "late".to_string(),
                data: Bytes::new(),
                meta: None,
                response_tx: late_tx,
                inflight_guard: None,
            })
            .unwrap_err();
        assert!(matches!(err, QueueError::NotFound));

        // ...while the in-flight request still gets its real response.
        tokio::time::timeout(Duration::from_secs(5), drain.wait_idle())
            .await
            .expect("in-flight request must drain");
        let resp = tokio::time::timeout(Duration::from_secs(1), resp_rx)
            .await
            .expect("response must arrive promptly after drain")
            .expect("in-flight response channel must not be dropped");
        assert!(matches!(resp.payload, Some(pb::response::Payload::Single(_))));
        drain.abort();
    }

    #[tokio::test]
    async fn test_begin_drain_idle_queue_drains_immediately() {
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            max_batch_size: 1,
            batch_timeout: 0.0,
            adaptive_batching: false,
            min_batch_timeout: 0.0,
            adaptive_queue_threshold: 0,
            health_check_interval: 0.0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let outlier = Arc::new(OutlierState::new(0));
        queue.register_model("m", "1", &config, vec![], vec![], reload_tx, outlier, None);

        let drain = queue.begin_drain("m", "1").expect("queue must exist");
        tokio::time::timeout(Duration::from_millis(500), drain.wait_idle())
            .await
            .expect("idle queue must drain immediately");

        // Draining an unknown version yields no handle.
        assert!(queue.begin_drain("m", "2").is_none());
        drain.abort();
    }

    #[tokio::test]
    async fn test_drain_abort_stops_collector() {
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            max_batch_size: 1,
            batch_timeout: 0.0,
            adaptive_batching: false,
            min_batch_timeout: 0.0,
            adaptive_queue_threshold: 0,
            health_check_interval: 0.0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);

        let outlier = Arc::new(OutlierState::new(0));
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier, None);
        let handle = queue.queues.get(&model_version_key("test_model", "1")).unwrap().collector.clone();
        assert!(!handle.is_finished());

        let drain = queue.begin_drain("test_model", "1").expect("queue must exist");
        drain.abort();

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(handle.is_finished(), "collector should be aborted after drain abort");
    }

    // ===== Phase 1: Bytes-based zero-copy tests =====

    #[test]
    fn test_queue_item_data_is_bytes() {
        // QueueItem.data should be Bytes, not Vec<u8>
        let data = Bytes::from(vec![1u8, 2, 3, 4]);
        let (tx, _rx) = oneshot::channel();
        let item = QueueItem {
            uid: "test".to_string(),
            data: data.clone(),
            meta: None,
            response_tx: tx,
                  inflight_guard: None,
        };
        // Bytes::clone shares the same underlying buffer
        let cloned_data = item.data.clone();
        assert_eq!(item.data.as_ptr(), cloned_data.as_ptr(),
            "Bytes::clone should share the same buffer (zero-copy)");
    }

    #[test]
    fn test_bytes_clone_is_refcount_not_data_copy() {
        // Cloning Bytes should NOT copy the underlying data
        let payload = vec![0u8; 4096]; // 4KB payload
        let data = Bytes::from(payload);
        let ptr = data.as_ptr();

        // Simulate what send_batch does: clone N times for batch items
        let clones: Vec<Bytes> = (0..10).map(|_| data.clone()).collect();

        for (i, clone) in clones.iter().enumerate() {
            assert_eq!(clone.as_ptr(), ptr,
                "clone[{}] should share the same buffer, not copy", i);
        }
        // All clones + original share the same refcount
        assert_eq!(data.as_ptr(), ptr);
    }

    #[test]
    fn test_bytes_to_vec_for_protobuf() {
        // Bytes must be convertible to Vec<u8> for prost-generated proto types
        let data = Bytes::from(vec![10u8, 20, 30]);
        let vec_data: Vec<u8> = data.to_vec();
        assert_eq!(vec_data, vec![10, 20, 30]);

        // Verify it works with SingleRequest.data: Bytes
        let single = pb::SingleRequest { data: data.clone() };
        assert_eq!(single.data, Bytes::from_static(&[10, 20, 30]));
    }

    #[test]
    fn test_queue_item_with_meta_bytes_shares_buffer() {
        // When QueueItem has both data and meta.payload pointing to same Bytes,
        // cloning should not duplicate the payload
        let payload_bytes = Bytes::from(vec![1u8; 1024]);
        let (tx, _rx) = oneshot::channel();

        let meta = Arc::new(pb::RequestMeta {
            route: "/infer".to_string(),
            headers: Default::default(),
            client_ip: "127.0.0.1".to_string(),
            request_id: "req-1".to_string(),
            timestamp_ns: 0,
            payload: payload_bytes.clone(),
            ..Default::default()
        });

        let item = QueueItem {
            uid: "u1".to_string(),
            data: payload_bytes.clone(),
            meta: Some(meta),
            response_tx: tx,
                  inflight_guard: None,
        };

        // Cloning the item's data is zero-copy
        let data_clone = item.data.clone();
        assert_eq!(item.data.as_ptr(), data_clone.as_ptr());

        // Cloning meta is also zero-copy (Arc refcount)
        let meta_clone = item.meta.clone();
        let meta_ptr = item.meta.as_ref().unwrap().as_ref() as *const pb::RequestMeta;
        let clone_ptr = meta_clone.as_ref().unwrap().as_ref() as *const pb::RequestMeta;
        assert_eq!(meta_ptr, clone_ptr);
    }

    #[test]
    fn test_bytes_from_bytes_is_zero_copy_equivalent_to_clone() {
        // bytes::Bytes::from(Bytes) should be a zero-copy refcount bump,
        // equivalent to .clone(). Proves the redundant wrapper is harmless but unnecessary.
        let payload = Bytes::from(vec![0u8; 512]);
        let via_from = payload.clone();
        let via_clone = payload.clone();

        // Both share the same underlying buffer
        assert_eq!(payload.as_ptr(), via_from.as_ptr());
        assert_eq!(payload.as_ptr(), via_clone.as_ptr());
        // They are logically equal
        assert_eq!(via_from, via_clone);
    }

    #[tokio::test]
    async fn test_batch_send_with_bytes_data() {
        // Full integration: QueueItem with Bytes flows through channel and send_batch
        // builds correct protobuf request without data corruption
        let (tx, mut rx) = mpsc::channel::<QueueItem>(10);

        let payload = Bytes::from(r#"{"input":"hello"}"#.as_bytes().to_vec());
        let (resp_tx, resp_rx) = oneshot::channel();
        let item = QueueItem {
            uid: "req-1".to_string(),
            data: payload.clone(),
            meta: None,
            response_tx: resp_tx,
                  inflight_guard: None,
        };
        tx.try_send(item).unwrap();

        // Receive and verify data integrity
        let received = rx.recv().await.unwrap();
        assert_eq!(received.data, payload);
        assert_eq!(received.uid, "req-1");

        // Build proto request from Bytes data (simulates send_batch logic)
        let proto_data = received.data.clone();
        let single = pb::SingleRequest { data: proto_data };
        assert_eq!(single.data, Bytes::from_static(r#"{"input":"hello"}"#.as_bytes()));

        // Send a mock response
        let _ = received.response_tx.send(pb::Response {
            uid: "req-1".to_string(),
            payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                data: Bytes::from_static(b"ok"),
                headers: Default::default(),
                status: Some(pb::Status { code: "Ok".to_string(), message: String::new() }),
            
                ..Default::default()
            })),
            metrics: None,
        });

        let resp = resp_rx.await.unwrap();
        assert_eq!(resp.uid, "req-1");
    }

    // ===== batch_item_to_single_response: per-item field propagation =====

    #[test]
    fn test_batch_item_to_single_propagates_per_item_fields() {
        // A batch item carrying its own status_code / media_type / headers
        // (e.g. early 400 from on_request, custom headers from on_response)
        // must reach the caller's SingleResponse intact.
        let headers = std::collections::HashMap::from([(
            "x-item".to_string(),
            "bad".to_string(),
        )]);
        let item = pb::BatchItemResponse {
            uid: "u1".to_string(),
            data: Bytes::from_static(b"{\"error\":\"bad\"}"),
            status: Some(pb::Status {
                code: "Ok".to_string(),
                message: String::new(),
            }),
            status_code: 400,
            media_type: "text/html".to_string(),
            headers,
        };

        let resp = batch_item_to_single_response("u1".to_string(), Some(&item), None);
        let Some(pb::response::Payload::Single(single)) = resp.payload else {
            panic!("expected Single payload");
        };
        assert_eq!(single.status.unwrap().code, "Ok");
        assert_eq!(single.status_code, 400);
        assert_eq!(single.media_type, "text/html");
        assert_eq!(single.headers.get("x-item").map(|s| s.as_str()), Some("bad"));
        assert_eq!(single.data, Bytes::from_static(b"{\"error\":\"bad\"}"));
    }

    #[test]
    fn test_batch_item_to_single_missing_item_is_error() {
        // A uid absent from the worker's BatchResponse maps to an Error
        // SingleResponse (and must not inherit batch metrics).
        let resp = batch_item_to_single_response("u2".to_string(), None, None);
        let Some(pb::response::Payload::Single(single)) = resp.payload else {
            panic!("expected Single payload");
        };
        let status = single.status.unwrap();
        assert_eq!(status.code, "Error");
        assert_eq!(status.message, "missing in batch response");
        assert!(resp.metrics.is_none());
    }

    // ===== Phase 2: Arc<RequestMeta> zero-copy tests =====

    #[test]
    fn test_queue_item_meta_is_arc() {
        // QueueItem.meta should be Option<Arc<RequestMeta>>, not Option<RequestMeta>
        let meta = pb::RequestMeta {
            route: "/infer".to_string(),
            headers: {
                let mut h = std::collections::HashMap::new();
                h.insert("content-type".to_string(), "application/json".to_string());
                h
            },
            client_ip: "127.0.0.1".to_string(),
            request_id: "req-1".to_string(),
            timestamp_ns: 1234567890,
            payload: Bytes::from_static(&[1u8, 2, 3]),
            ..Default::default()
        };
        let (tx, _rx) = oneshot::channel();
        let item = QueueItem {
            uid: "test".to_string(),
            data: Bytes::new(),
            meta: Some(Arc::new(meta)),
            response_tx: tx,
                  inflight_guard: None,
        };
        // Cloning meta should be a refcount bump, not a deep copy
        let meta_clone = item.meta.clone();
        let meta_ptr = item.meta.as_ref().unwrap().as_ref() as *const pb::RequestMeta;
        let clone_ptr = meta_clone.as_ref().unwrap().as_ref() as *const pb::RequestMeta;
        assert_eq!(meta_ptr, clone_ptr,
            "Arc<RequestMeta> clone should share the same allocation");
    }

    #[test]
    fn test_arc_meta_batch_clone_is_cheap() {
        // Simulates send_batch batch path: N items share meta via Arc
        let meta = Arc::new(pb::RequestMeta {
            route: "/infer".to_string(),
            headers: {
                let mut h = std::collections::HashMap::new();
                for i in 0..50 {
                    h.insert(format!("header-{}", i), format!("value-{}", i));
                }
                h
            },
            client_ip: "10.0.0.1".to_string(),
            request_id: "req-batch".to_string(),
            timestamp_ns: 9999999,
            payload: Bytes::from(vec![0u8; 4096]), // 4KB payload
            ..Default::default()
        });

        // Simulate batch items each holding Arc<RequestMeta>
        let items: Vec<(String, Arc<pb::RequestMeta>)> = (0..100)
            .map(|i| (format!("uid-{}", i), Arc::clone(&meta)))
            .collect();

        // All should share the same allocation
        let ptr = meta.as_ref() as *const pb::RequestMeta;
        for (uid, item_meta) in &items {
            assert_eq!(item_meta.as_ref() as *const pb::RequestMeta, ptr,
                "batch item {} should share meta via Arc", uid);
        }
    }

    // ===== Outlier Detection tests =====

    #[tokio::test]
    async fn test_outlier_eject_after_consecutive_errors() {
        let outlier = OutlierState::new(3);
        // Not ejected initially
        assert!(!outlier.is_ejected(0));

        // Below threshold — not ejected
        outlier.record_error(0);
        outlier.record_error(0);
        assert!(!outlier.is_ejected(0));

        // At threshold — ejected
        outlier.record_error(0);
        assert!(outlier.is_ejected(0));
    }

    #[test]
    fn test_outlier_never_ejects_when_threshold_zero() {
        // §3: ejection_error_threshold == 0 disables outlier ejection entirely.
        let cfg = EjectionConfig {
            error_threshold: 0,
            timeout: Duration::from_secs(30),
            max_percent: 50,
        };
        let outlier = OutlierState::with_config(2, &cfg);
        for _ in 0..10 {
            assert!(!outlier.record_error(0), "threshold 0 must never eject");
        }
        assert!(!outlier.is_ejected(0));
    }

    #[test]
    fn test_outlier_ejects_at_configured_threshold() {
        // §3: the threshold is configurable (here 2, not the default 3).
        let cfg = EjectionConfig {
            error_threshold: 2,
            timeout: Duration::from_secs(30),
            max_percent: 50,
        };
        let outlier = OutlierState::with_config(1, &cfg);
        assert!(!outlier.record_error(0)); // 1st error, below threshold 2
        assert!(outlier.record_error(0)); // 2nd error → eject
        assert!(outlier.is_ejected(0));
    }

    #[test]
    fn test_outlier_consecutive_errors_accessor() {
        let outlier = OutlierState::new(2);
        assert_eq!(outlier.consecutive_errors(0), 0);
        outlier.record_error(0);
        outlier.record_error(0);
        assert_eq!(outlier.consecutive_errors(0), 2);
        outlier.record_success(0);
        assert_eq!(outlier.consecutive_errors(0), 0);
        // Unknown index reads as 0 rather than panicking.
        assert_eq!(outlier.consecutive_errors(99), 0);
    }

    #[test]
    fn test_outlier_reset_clears_count_and_ejection() {
        // Used when a worker's process is replaced: the new process must
        // start with neither an error history nor an ejection.
        let outlier = OutlierState::new(1);
        outlier.record_error(0);
        outlier.record_error(0);
        outlier.record_error(0);
        assert!(outlier.is_ejected(0));
        outlier.reset(0);
        assert!(!outlier.is_ejected(0));
        assert_eq!(outlier.consecutive_errors(0), 0);
        // Unknown index is a no-op.
        outlier.reset(99);
    }

    #[test]
    fn test_outlier_reset_error_count_keeps_ejection() {
        // After a kill signal is sent, the count is throttled but the dead
        // slot must stay ejected until the replacement proves healthy.
        let outlier = OutlierState::new(1);
        outlier.record_error(0);
        outlier.record_error(0);
        outlier.record_error(0);
        assert!(outlier.is_ejected(0));
        outlier.reset_error_count(0);
        assert_eq!(outlier.consecutive_errors(0), 0);
        assert!(outlier.is_ejected(0), "ejection must survive error-count reset");
    }

    // ===== Health-check kill escalation =====

    /// Endpoint with a bound client but no worker ever connecting — every
    /// probe times out. Models a hung worker.
    fn unreachable_endpoint(name: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("lite-server-hc-{}-{}.sock", name, std::process::id()))
                    .display()
            )
        }
        #[cfg(windows)]
        {
            format!("tcp://127.0.0.1:{}", 35000 + std::process::id() % 1000)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_health_checker_escalates_to_kill() {
        // kill_threshold=2, ejection threshold=1: the first failed probe
        // ejects, the second triggers a kill+respawn signal.
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            health_check_interval: 0.05,
            health_check_timeout: 0.05,
            health_check_kill_threshold: 2,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let (respawn_tx, mut respawn_rx) = mpsc::channel(8);
        let ejection_cfg = EjectionConfig {
            error_threshold: 1,
            timeout: Duration::from_secs(60),
            max_percent: 100,
        };
        let outlier = Arc::new(OutlierState::with_config(1, &ejection_cfg));
        let outlier_probe = outlier.clone();
        let client = Arc::new(WorkerZmqClient::new(unreachable_endpoint("kill")));
        queue.register_model(
            "m", "1", &config, vec![], vec![client], reload_tx, outlier,
            Some(respawn_tx),
        );

        let sig = tokio::time::timeout(Duration::from_secs(10), respawn_rx.recv())
            .await
            .expect("kill escalation must signal respawn")
            .expect("respawn channel must stay open");
        assert_eq!(sig.model_name, "m");
        assert_eq!(sig.version, "1");
        assert_eq!(sig.worker_id, 0);
        assert_eq!(sig.reason, "health_check");
        assert!(outlier_probe.is_ejected(0), "first failure must have ejected the worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_health_checker_no_kill_when_threshold_zero() {
        // kill_threshold=0 (default) — failures eject but never respawn.
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            health_check_interval: 0.05,
            health_check_timeout: 0.05,
            health_check_kill_threshold: 0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let (respawn_tx, mut respawn_rx) = mpsc::channel(8);
        let outlier = Arc::new(OutlierState::new(1));
        let client = Arc::new(WorkerZmqClient::new(unreachable_endpoint("nokill")));
        queue.register_model(
            "m", "1", &config, vec![], vec![client], reload_tx, outlier,
            Some(respawn_tx),
        );

        let result = tokio::time::timeout(Duration::from_millis(500), respawn_rx.recv()).await;
        assert!(result.is_err(), "kill_threshold=0 must never signal respawn");
    }

    #[tokio::test]
    async fn test_record_error_returns_true_only_on_new_ejection() {
        // Callers use the return value to record the per-(model, version)
        // ejection metric exactly once per ejection (§4.6).
        let outlier = OutlierState::new(1);
        assert!(!outlier.record_error(0), "below threshold");
        assert!(!outlier.record_error(0), "below threshold");
        assert!(outlier.record_error(0), "third consecutive error ejects");
        assert!(!outlier.record_error(0), "already ejected — not a new ejection");
    }

    #[tokio::test]
    async fn test_outlier_success_resets_error_count() {
        let outlier = OutlierState::new(2);
        outlier.record_error(0);
        outlier.record_error(0);
        // Reset by success
        outlier.record_success(0);
        // Need 3 more errors to eject
        outlier.record_error(0);
        outlier.record_error(0);
        assert!(!outlier.is_ejected(0), "should not eject after reset");
        outlier.record_error(0);
        assert!(outlier.is_ejected(0), "should eject after 3 consecutive errors post-reset");
    }

    #[tokio::test]
    async fn test_outlier_active_count() {
        let outlier = OutlierState::new(3);
        assert_eq!(outlier.active_count(), 3);

        // Eject one worker
        for _ in 0..3 {
            outlier.record_error(0);
        }
        assert_eq!(outlier.active_count(), 2);
    }

    #[tokio::test]
    async fn test_outlier_max_ejection_percent() {
        // 4 workers, max 50% ejection = max 2 ejected
        let outlier = OutlierState::new(4);

        // Eject workers 0 and 1
        for i in 0..2 {
            for _ in 0..3 {
                outlier.record_error(i);
            }
        }
        assert!(outlier.is_ejected(0));
        assert!(outlier.is_ejected(1));

        // Worker 2 should NOT be ejected (at max)
        for _ in 0..3 {
            outlier.record_error(2);
        }
        assert!(!outlier.is_ejected(2), "should respect max_ejection_percent");
    }

    #[test]
    fn test_outlier_survives_mutex_poison() {
        let outlier = OutlierState::new(1);
        // Poison the mutex by panicking while holding the lock
        let result = std::panic::catch_unwind(|| {
            let _guard = outlier.workers[0].ejected.lock().unwrap();
            panic!("intentional panic to poison mutex");
        });
        assert!(result.is_err(), "should have panicked");

        // After poisoning, is_ejected should NOT panic
        assert!(!outlier.is_ejected(0), "is_ejected should survive mutex poison");
        // active_count should also not panic
        assert_eq!(outlier.active_count(), 1);
        // record_error -> maybe_eject should also not panic
        outlier.record_error(0);
        outlier.record_error(0);
        outlier.record_error(0);
        assert!(outlier.is_ejected(0), "should eject after threshold");
    }

    // ===== pick_worker with outlier tests =====

    #[tokio::test]
    async fn test_pick_worker_skips_ejected() {
        let outlier = OutlierState::new(3);
        let inflight: Vec<Arc<AtomicUsize>> = (0..3)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();

        // Eject worker 0
        for _ in 0..3 {
            outlier.record_error(0);
        }

        // Should skip ejected worker 0, pick 1 or 2 (both load 0)
        let picked = pick_worker_least_loaded(&inflight, &outlier, &[]);
        assert!(picked == 1 || picked == 2, "should skip ejected worker, got {}", picked);
    }

    #[tokio::test]
    async fn test_pick_worker_least_loaded_skips_ejected() {
        let outlier = OutlierState::new(3);
        let inflight: Vec<Arc<AtomicUsize>> = vec![
            Arc::new(AtomicUsize::new(0)), // worker 0 — ejected
            Arc::new(AtomicUsize::new(5)), // worker 1 — high load
            Arc::new(AtomicUsize::new(1)), // worker 2 — low load
        ];

        // Eject worker 0
        for _ in 0..3 {
            outlier.record_error(0);
        }

        // Should pick worker 2 (lowest load among active)
        let picked = pick_worker_least_loaded(&inflight, &outlier, &[]);
        assert_eq!(picked, 2, "should pick lowest-load active worker");
    }

    #[tokio::test]
    async fn test_pick_worker_all_ejected_falls_back() {
        let outlier = OutlierState::new(2);
        let inflight: Vec<Arc<AtomicUsize>> = (0..2)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();

        // Eject both workers
        for i in 0..2 {
            for _ in 0..3 {
                outlier.record_error(i);
            }
        }

        // Both ejected, but max_ejection_percent=50 means only 1 can be ejected
        // Actually with 2 workers, max_ejected = max(2*50/100, 1) = 1
        // So only worker 0 should be ejected, worker 1 should still be active
        let picked = pick_worker_least_loaded(&inflight, &outlier, &[]);
        assert_eq!(picked, 1, "second worker should still be active");
    }

    #[tokio::test]
    async fn test_pick_worker_single_worker_ignores_outlier() {
        let outlier = OutlierState::new(1);
        let inflight: Vec<Arc<AtomicUsize>> = vec![Arc::new(AtomicUsize::new(0))];

        // Even if ejected, single worker fast path returns 0
        for _ in 0..3 {
            outlier.record_error(0);
        }
        let picked = pick_worker_least_loaded(&inflight, &outlier, &[]);
        assert_eq!(picked, 0, "single worker should always return 0");
    }

    // ===== Request Timeout tests =====

    #[test]
    fn test_request_timeout_zero_is_disabled() {
        // Duration::ZERO means no timeout — verify the condition
        let timeout = Duration::ZERO;
        assert!(timeout.is_zero());
        assert!(!timeout.gt(&Duration::ZERO));
    }

    #[test]
    fn test_request_timeout_positive_is_enabled() {
        let timeout = Duration::from_secs(30);
        assert!(!timeout.is_zero());
        assert!(timeout.gt(&Duration::ZERO));
    }

    #[tokio::test]
    async fn test_request_timeout_triggers_on_slow_response() {
        // Simulate a slow ZMQ client by using tokio::time::pause
        tokio::time::pause();

        let timeout = Duration::from_millis(100);

        // Simulate: timeout expires before response arrives
        let result = tokio::time::timeout(timeout, async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<_, ()>(())
        }).await;

        assert!(result.is_err(), "should timeout when response takes too long");
    }

    // ===== Max Requests tests =====

    #[tokio::test]
    async fn test_check_max_requests_sends_signal() {
        let (tx, mut rx) = mpsc::channel::<ReloadSignal>(8);
        let counter = AtomicUsize::new(0);

        // First batch: counter goes from 0 to 3, max_requests=5 → no signal
        check_max_requests(&counter, 3, 5, "model_a", "1", &tx).await;
        assert!(rx.try_recv().is_err(), "should not signal yet");

        // Second batch: counter goes from 3 to 6, crosses 5 → signal
        check_max_requests(&counter, 3, 5, "model_a", "1", &tx).await;
        let signal = rx.try_recv().unwrap();
        assert_eq!(signal.model_name, "model_a");
        // The signal must name the version that hit the threshold, so the
        // listener recycles that version instead of the active one.
        assert_eq!(signal.version, "1");
    }

    #[tokio::test]
    async fn test_check_max_requests_disabled_when_zero() {
        let (tx, mut rx) = mpsc::channel::<ReloadSignal>(8);
        let counter = AtomicUsize::new(0);

        // max_requests=0 means disabled
        for _ in 0..10 {
            check_max_requests(&counter, 1, 0, "model_b", "1", &tx).await;
        }
        assert!(rx.try_recv().is_err(), "should never signal when max_requests=0");
        assert_eq!(counter.load(Ordering::Relaxed), 0, "counter should not increment when disabled");
    }

    #[tokio::test]
    async fn test_check_max_requests_only_signals_once() {
        let (tx, mut rx) = mpsc::channel::<ReloadSignal>(8);
        let counter = AtomicUsize::new(0);

        // Cross threshold at batch 3
        check_max_requests(&counter, 3, 5, "m", "1", &tx).await;
        // Already crossed, next call should not signal again
        check_max_requests(&counter, 3, 5, "m", "1", &tx).await;
        check_max_requests(&counter, 3, 5, "m", "1", &tx).await;

        // Only one signal should have been sent
        assert!(rx.try_recv().is_ok(), "first signal should arrive");
        assert!(rx.try_recv().is_err(), "should not send duplicate signals");
    }

    #[tokio::test]
    async fn test_check_max_requests_batch_size_exact() {
        let (tx, mut rx) = mpsc::channel::<ReloadSignal>(8);
        let counter = AtomicUsize::new(0);

        // Single batch that exactly hits max_requests
        check_max_requests(&counter, 10, 10, "exact", "2", &tx).await;
        let signal = rx.try_recv().unwrap();
        assert_eq!(signal.model_name, "exact");
        assert_eq!(signal.version, "2");
    }

    // ===== Shared OutlierState tests =====

    #[tokio::test]
    async fn test_outlier_state_shared_between_register_and_accessor() {
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_batch_size: 1,
            batch_timeout: 0.0,
            max_queue_size: 10,
            health_check_interval: 0.0, // disable health checker for this test
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let outlier = Arc::new(OutlierState::new(2));

        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier.clone(), None);

        let retrieved = queue.get_outlier_state("test_model", "1").unwrap();
        // Must be the same Arc
        assert!(Arc::ptr_eq(&outlier, &retrieved));

        // Mutations via one handle are visible via the other
        // Record enough errors to trigger ejection (threshold is 3)
        outlier.record_error(0);
        outlier.record_error(0);
        outlier.record_error(0);
        assert!(retrieved.is_ejected(0), "worker should be ejected via shared state");
        assert_eq!(outlier.active_count(), 1);
    }

    #[test]
    fn test_get_outlier_state_nonexistent() {
        let queue = InferenceQueue::new();
        assert!(queue.get_outlier_state("no_such_model", "1").is_none());
    }

    // ===== Health checker tests =====

    #[tokio::test]
    async fn test_health_check_interval_zero_skips_checker() {
        // When health_check_interval is 0, register_model should not spawn a health checker.
        // We verify by checking that the queue still works (no panic, no extra tasks).
        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_batch_size: 1,
            batch_timeout: 0.0,
            max_queue_size: 10,
            health_check_interval: 0.0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let outlier = Arc::new(OutlierState::new(1));
        queue.register_model("m", "1", &config, vec![], vec![], reload_tx, outlier, None);
        assert!(queue.has_queue("m", "1"));
    }

    // ===== max_requests_jitter tests =====

    #[test]
    fn test_jitter_zero_means_exact() {
        // When jitter is 0, should return exact max_requests
        assert_eq!(compute_jittered_max_requests(100, 0), 100);
        assert_eq!(compute_jittered_max_requests(1, 0), 1);
    }

    #[test]
    fn test_jitter_disabled_when_max_requests_zero() {
        // When max_requests is 0 (disabled), jitter has no effect
        assert_eq!(compute_jittered_max_requests(0, 10), 0);
        assert_eq!(compute_jittered_max_requests(0, 0), 0);
    }

    #[test]
    fn test_jittered_max_requests_within_range() {
        // With max_requests=100, jitter=10, result should be in [90, 110]
        for _ in 0..200 {
            let result = compute_jittered_max_requests(100, 10);
            assert!((90..=110).contains(&result),
                "jittered value {} out of expected range [90, 110]", result);
        }
    }

    #[test]
    fn test_jittered_max_requests_never_zero() {
        // Even with large jitter and small max_requests, result should be >= 1
        for _ in 0..100 {
            let result = compute_jittered_max_requests(1, 5);
            assert!(result >= 1, "jittered value must be >= 1, got {}", result);
        }
    }

    // ===== B1: Retry mechanism dead code =====

    /// Spawn a PAIR-socket worker that answers every request with an Error
    /// status. Used by the retry-dead-code test to simulate a failing worker.
    fn spawn_error_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        use prost::Message;
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("error worker socket");
            s.connect(&endpoint).expect("error worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resp = pb::Response {
                    uid: req.uid,
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: Bytes::new(),
                        status: Some(pb::Status {
                            code: "Error".to_string(),
                            message: "worker always fails".to_string(),
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        })
    }

    /// B1 (P0): The retry mechanism in send_batch_with_retry is dead code.
    ///
    /// do_send_batch drains all batch items (sends error responses to every
    /// caller) in every one of its Err(()) return paths.  When
    /// send_batch_with_retry receives Err(()), `batch.is_empty()` is always
    /// true, so the retry loop exits immediately — the for loop body for
    /// attempt > 0 is unreachable.
    ///
    /// This test sets up two workers: worker 0 always returns Error, worker 1
    /// returns Ok.  With the retry mechanism working, the failing request to
    /// worker 0 would be retried on worker 1 and the caller would receive Ok.
    /// Because the retry is dead, the caller receives Error from worker 0
    /// and worker 1 is never contacted.
    #[tokio::test]
    async fn test_b1_retry_mechanism_dead_code() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let ep0 = drain_test_endpoint("b1-err-0");
        let ep1 = drain_test_endpoint("b1-ok-1");

        // Worker 0: always returns Error
        let _w0 = spawn_error_worker(ep0.clone());

        // Worker 1: returns Ok, tracks whether it was ever contacted
        let w1_contacted = Arc::new(AtomicBool::new(false));
        let w1_contacted_clone = w1_contacted.clone();
        let ep1_clone = ep1.clone();
        let _w1 = std::thread::spawn(move || {
            use prost::Message;
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("ok worker socket");
            s.connect(&ep1_clone).expect("ok worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                w1_contacted_clone.store(true, Ordering::SeqCst);
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resp = pb::Response {
                    uid: req.uid,
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: Bytes::from_static(b"{\"ok\":true}"),
                        status: Some(pb::Status {
                            code: "Ok".to_string(),
                            message: String::new(),
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        });

        let queue = InferenceQueue::new();
        let config = ModelConfig {
            max_queue_size: 10,
            max_batch_size: 1,
            batch_timeout: 0.0,
            adaptive_batching: false,
            min_batch_timeout: 0.0,
            adaptive_queue_threshold: 0,
            health_check_interval: 0.0,
            request_timeout: 5.0,
            ..Default::default()
        };
        let (reload_tx, _reload_rx) = mpsc::channel(8);
        let outlier = Arc::new(OutlierState::new(2));
        let client0 = Arc::new(WorkerZmqClient::new(ep0));
        let client1 = Arc::new(WorkerZmqClient::new(ep1));
        queue.register_model(
            "m", "1", &config, vec![],
            vec![client0, client1],
            reload_tx, outlier, None,
        );
        // Let both PAIR connections establish.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        queue
            .try_submit("m", "1", QueueItem {
                uid: "b1-test".to_string(),
                data: Bytes::from_static(b"{}"),
                meta: None,
                response_tx: resp_tx,
                inflight_guard: None,
            })
            .unwrap();

        let resp = tokio::time::timeout(Duration::from_secs(10), resp_rx)
            .await
            .expect("response must arrive promptly")
            .expect("response channel must not be dropped");

        // BUG: The response is Error from worker 0, proving the retry never
        // handed the request to the healthy worker 1.
        let code = match resp.payload {
            Some(pb::response::Payload::Single(ref s)) => {
                s.status.as_ref().map(|st| st.code.as_str()).unwrap_or("")
            }
            _ => "",
        };
        assert_eq!(
            code, "Ok",
            "B1 REGRESSION: expected Ok (retry would forward to healthy worker 1), \
             got {code}. Retry mechanism is still dead code — do_send_batch \
             drains the batch before send_batch_with_retry can retry."
        );
        // Confirm worker 1 was never contacted — retry never fired.
        assert!(
            w1_contacted.load(Ordering::SeqCst),
            "B1 REGRESSION: healthy worker 1 was never contacted — \
             send_batch_with_retry never retried after worker 0 failed"
        );
    }

    /// B2 (P1): request_timeout must not be silently capped by the ZMQ-level
    /// backstop timeout.
    ///
    /// Regression: `zmq_client.send()` used to apply a hardcoded inner timeout
    /// of 60 s (`ZMQ_RESPONSE_TIMEOUT`) while the outer request_timeout wrapped
    /// it, so the effective timeout was min(request_timeout, 60s). A user
    /// configuring request_timeout=120 got a 60s timeout with a misleading
    /// "ZMQ response timeout" error message.
    ///
    /// Fixed by (a) raising the backstop constant well above any realistic
    /// configured timeout and (b) passing the caller's request_timeout down to
    /// the ZMQ wait via `send_with_timeout`, making it the single real bound.
    /// This test guards the invariant that the backstop never caps a
    /// configured request_timeout.
    #[test]
    fn test_b2_request_timeout_capped_at_zmq_response_timeout() {
        // The ZMQ-level backstop timeout, referenced from its definition so
        // this test tracks the real constant.
        let zmq_timeout = crate::transport::zmq::ZMQ_RESPONSE_TIMEOUT;

        // Simulate: a user configures request_timeout = 120 s.
        let configured: f64 = 120.0;
        let request_timeout = Duration::from_secs_f64(configured);

        // The effective timeout must be the user-configured value: the ZMQ
        // backstop may only fire when no request_timeout is configured.
        let expected = request_timeout; // what the user expects
        let actual_effective = std::cmp::min(request_timeout, zmq_timeout);
        assert_eq!(
            actual_effective, expected,
            "B2 REGRESSION: effective timeout ({:?}) != configured request_timeout ({:?}). \
             The ZMQ_RESPONSE_TIMEOUT backstop ({:?}) silently caps request_timeout — \
             it must stay above the maximum realistic request_timeout.",
            actual_effective, expected, zmq_timeout
        );
    }

    #[test]
    fn test_jittered_max_requests_varies() {
        // Multiple calls should produce different values (probabilistic)
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            seen.insert(compute_jittered_max_requests(100, 20));
        }
        assert!(seen.len() > 1, "jitter should produce varied values, got {:?}", seen);
    }
}
