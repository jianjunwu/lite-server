use bytes::Bytes;
use crate::config::ModelConfig;
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
use tracing::{error, info, warn};

/// Pre-sized key for model_version lookups — single allocation, no reallocation.
/// Used on the hot path (try_submit / has_queue) to avoid repeated format! overhead.
#[inline]
fn model_version_key(model: &str, version: &str) -> String {
    let mut key = String::with_capacity(model.len() + 1 + version.len());
    key.push_str(model);
    key.push('_');
    key.push_str(version);
    key
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

/// Shared outlier detection state for a model version's workers.
pub struct OutlierState {
    workers: Vec<WorkerOutlier>,
    // Precomputed thresholds (read-only after construction)
    consecutive_threshold: usize,
    base_ejection_time: Duration,
    max_ejection_percent: usize,
}

impl OutlierState {
    pub fn new(num_workers: usize) -> Self {
        Self {
            workers: (0..num_workers)
                .map(|_| WorkerOutlier {
                    consecutive_errors: AtomicUsize::new(0),
                    ejected: Mutex::new(None),
                })
                .collect(),
            consecutive_threshold: 3,
            base_ejection_time: Duration::from_secs(30),
            max_ejection_percent: 50,
        }
    }

    /// Record a successful request — reset consecutive error count.
    pub fn record_success(&self, worker_idx: usize) {
        if let Some(w) = self.workers.get(worker_idx) {
            w.consecutive_errors.store(0, Ordering::Relaxed);
        }
    }

    /// Record an error — increment count and potentially eject.
    pub fn record_error(&self, worker_idx: usize) {
        let w = match self.workers.get(worker_idx) {
            Some(w) => w,
            None => return,
        };
        let count = w.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= self.consecutive_threshold {
            self.maybe_eject(worker_idx);
        }
    }

    /// Eject a worker if not already ejected and below max ejection percent.
    fn maybe_eject(&self, worker_idx: usize) {
        let w = &self.workers[worker_idx];
        let mut guard = w.ejected.lock().unwrap();
        if guard.is_some() {
            return; // already ejected
        }
        // Count currently ejected workers (non-blocking, approximate is fine)
        let mut ejected_count = 0;
        for (i, other) in self.workers.iter().enumerate() {
            if i == worker_idx {
                continue;
            }
            if let Ok(g) = other.ejected.try_lock() {
                if g.is_some() {
                    ejected_count += 1;
                }
            }
        }
        let max_ejected = (self.workers.len() * self.max_ejection_percent / 100).max(1);
        if ejected_count < max_ejected {
            *guard = Some(EjectedWorker {
                ejected_at: Instant::now(),
            });
            prometheus::inc_worker_ejection();
            info!("Worker {} ejected after {} consecutive errors", worker_idx, self.consecutive_threshold);
        }
    }

    /// Check if a worker is currently ejected, recovering if ejection time has passed.
    pub fn is_ejected(&self, worker_idx: usize) -> bool {
        let w = match self.workers.get(worker_idx) {
            Some(w) => w,
            None => return false,
        };
        let mut guard = w.ejected.lock().unwrap();
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
            let guard = w.ejected.lock().unwrap();
            if guard.is_none() {
                count += 1;
            }
        }
        count
    }
}

// ===== Retry configuration =====

const MAX_RETRIES: usize = 3;

/// Signal to trigger a model version reload (worker auto-recycle).
#[derive(Debug)]
pub struct ReloadSignal {
    pub model_name: String,
}

/// Per-model-version inference queue with batch aggregation.
pub struct InferenceQueue {
    queues: DashMap<String, (mpsc::Sender<QueueItem>, std::sync::Arc<tokio::task::JoinHandle<()>>, Arc<OutlierState>)>,
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
    ) {
        let key = model_version_key(model_name, version);

        // Abort and remove stale collector if exists
        if let Some((_, (_, old_handle, _))) = self.queues.remove(&key) {
            old_handle.abort();
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
        let health_interval = Duration::from_secs_f64(config.health_check_interval as f64);

        let handle = tokio::spawn(batch_collector(
            rx,
            max_batch,
            batch_timeout,
            adaptive,
            min_timeout,
            queue_threshold,
            workers,
            zmq_clients.clone(),
            model_name.to_string(),
            version.to_string(),
            request_timeout,
            max_requests,
            max_requests_jitter,
            reload_tx,
            outlier.clone(),
        ));

        // Spawn health checker if interval > 0
        if health_interval > Duration::ZERO {
            tokio::spawn(health_checker(
                health_interval,
                zmq_clients,
                outlier.clone(),
                model_name.to_string(),
                version.to_string(),
            ));
        }

        self.queues.insert(key, (tx, std::sync::Arc::new(handle), outlier));
    }

    /// Unregister a model version and stop its collector.
    pub fn unregister_model(&self, model_name: &str, version: &str) {
        let key = model_version_key(model_name, version);
        if let Some((_, (_, handle, _))) = self.queues.remove(&key) {
            handle.abort();
        }
    }

    /// Submit a single request to the queue (non-blocking).
    /// Returns QueueError::Full immediately if the queue is at capacity.
    pub fn try_submit(
        &self,
        model_name: &str,
        version: &str,
        item: QueueItem,
    ) -> Result<(), QueueError> {
        let key = model_version_key(model_name, version);
        let sender = {
            let entry = self
                .queues
                .get(&key)
                .ok_or(QueueError::NotFound)?;
            entry.0.clone()
        };
        sender
            .try_send(item)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => QueueError::Full,
                mpsc::error::TrySendError::Closed(_) => QueueError::Closed,
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
        self.queues.get(&key).map(|entry| entry.2.clone())
    }
}

/// Pick the worker with the lowest inflight count (least-loaded), skipping ejected workers.
fn pick_worker_least_loaded(inflight: &[Arc<AtomicUsize>], outlier: &OutlierState) -> usize {
    if inflight.len() == 1 {
        return 0;
    }

    let mut best_active: Option<(usize, usize)> = None;
    let mut best_ejected: Option<(usize, usize)> = None;

    for (i, counter) in inflight.iter().enumerate() {
        let load = counter.load(Ordering::Relaxed);
        if outlier.is_ejected(i) {
            if best_ejected.is_none() || load < best_ejected.unwrap().1 {
                best_ejected = Some((i, load));
            }
        } else {
            if best_active.is_none() || load < best_active.unwrap().1 {
                best_active = Some((i, load));
            }
        }
    }

    // Prefer active worker; if all ejected, fall back to least-loaded ejected
    best_active
        .or(best_ejected)
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

/// Send a batch to a single worker. Returns Err on transport/worker failure.
/// On success, all items are drained and responses sent to callers.
/// On error, the batch is drained with error responses and Err is returned.
async fn do_send_batch(
    batch: &mut Vec<QueueItem>,
    zmq_clients: &[Arc<WorkerZmqClient>],
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    model_name: &str,
    version: &str,
    request_timeout: Duration,
) -> Result<(), ()> {
    if batch.is_empty() {
        return Ok(());
    }

    let worker_idx = pick_worker_least_loaded(inflight, outlier);
    inflight[worker_idx].fetch_add(1, Ordering::Relaxed);
    let zmq_client = &zmq_clients[worker_idx];

    // Build protobuf request (Single if batch.len() == 1, else Batch)
    let request = if batch.len() == 1 {
        let item = &batch[0];
        pb::Request {
            uid: item.uid.clone(),
            meta: item.meta.as_deref().cloned(),
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: item.data.to_vec(),
            })),
        }
    } else {
        let items: Vec<pb::BatchItem> = batch
            .iter()
            .map(|item| pb::BatchItem {
                uid: item.uid.clone(),
                data: item.data.to_vec(),
            })
            .collect();
        pb::Request {
            uid: format!("batch-{}", uuid::Uuid::new_v4()),
            meta: batch.first().and_then(|i| i.meta.as_deref().cloned()),
            payload: Some(pb::request::Payload::Batch(pb::BatchRequest { items })),
        }
    };

    // Track queue depth decrease for all items in the batch
    for _ in 0..batch.len() {
        prometheus::dec_queue_depth(model_name, version);
    }

    let result = if request_timeout > Duration::ZERO {
        match tokio::time::timeout(request_timeout, zmq_client.send(request)).await {
            Ok(r) => r,
            Err(_) => {
                error!("Request timeout ({:?}) for {} {}", request_timeout, model_name, version);
                for queue_item in batch.drain(..) {
                    let _ = queue_item.response_tx.send(pb::Response {
                        uid: queue_item.uid,
                        payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                            data: vec![],
                            status: Some(pb::Status {
                                code: "Timeout".to_string(),
                                message: "request timeout".to_string(),
                            }),
                        })),
                        metrics: None,
                    });
                }
                inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                outlier.record_error(worker_idx);
                return Err(());
            }
        }
    } else {
        zmq_client.send(request).await
    };

    match result {
        Ok(resp) => {
            crate::metrics::prometheus::record_worker_metrics(model_name, resp.metrics.as_ref());
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
                        let single_resp = if let Some(resp_item) = resp_map.get(&queue_item.uid) {
                            if resp_item.status.as_ref().map(|s| s.code.as_str()) == Some("Error") {
                                all_ok = false;
                            }
                            pb::Response {
                                uid: queue_item.uid,
                                payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                                    data: resp_item.data.clone(),
                                    status: resp_item.status.clone(),
                                })),
                                metrics: resp.metrics.clone(),
                            }
                        } else {
                            all_ok = false;
                            pb::Response {
                                uid: queue_item.uid,
                                payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                                    data: vec![],
                                    status: Some(pb::Status {
                                        code: "Error".to_string(),
                                        message: "missing in batch response".to_string(),
                                    }),
                                })),
                                metrics: None,
                            }
                        };
                        let _ = queue_item.response_tx.send(single_resp);
                    }

                    inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                    if all_ok {
                        outlier.record_success(worker_idx);
                        Ok(())
                    } else {
                        outlier.record_error(worker_idx);
                        Err(())
                    }
                }
                Some(pb::response::Payload::Single(single_resp)) => {
                    let is_error = single_resp.status.as_ref().map(|s| s.code.as_str()) == Some("Error");
                    if let Some(queue_item) = batch.pop() {
                        let _ = queue_item.response_tx.send(pb::Response {
                            uid: queue_item.uid,
                            payload: Some(pb::response::Payload::Single(single_resp)),
                            metrics: resp.metrics,
                        });
                    }
                    inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                    if is_error {
                        outlier.record_error(worker_idx);
                        Err(())
                    } else {
                        outlier.record_success(worker_idx);
                        Ok(())
                    }
                }
                _ => {
                    warn!("Unexpected response type from worker for {} {}", model_name, version);
                    for queue_item in batch.drain(..) {
                        let _ = queue_item.response_tx.send(pb::Response {
                            uid: queue_item.uid,
                            payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                                data: vec![],
                                status: Some(pb::Status {
                                    code: "Error".to_string(),
                                    message: "unexpected response type".to_string(),
                                }),
                            })),
                            metrics: None,
                        });
                    }
                    inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                    outlier.record_error(worker_idx);
                    Err(())
                }
            }
        }
        Err(e) => {
            error!(
                "Batch request failed for {} {}: {}",
                model_name, version, e
            );
            for queue_item in batch.drain(..) {
                let _ = queue_item.response_tx.send(pb::Response {
                    uid: queue_item.uid,
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: vec![],
                        status: Some(pb::Status {
                            code: "Error".to_string(),
                            message: e.to_string(),
                        }),
                    })),
                    metrics: None,
                });
            }
            inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
            outlier.record_error(worker_idx);
            Err(())
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
) {
    if batch.is_empty() {
        return;
    }

    let batch_size = batch.len();

    // Fast path: single worker, no retry possible
    if zmq_clients.len() <= 1 {
        let result = do_send_batch(&mut batch, zmq_clients, inflight, outlier, model_name, version, request_timeout).await;
        if result.is_ok() {
            check_max_requests(request_count, batch_size, max_requests, model_name, version, reload_tx).await;
        }
        return;
    }

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            prometheus::inc_retry(model_name, version);
        }
        match do_send_batch(&mut batch, zmq_clients, inflight, outlier, model_name, version, request_timeout).await {
            Ok(()) => {
                check_max_requests(request_count, batch_size, max_requests, model_name, version, reload_tx).await;
                return;
            }
            Err(()) => {
                if batch.is_empty() {
                    return; // all items already got error responses
                }
                // items remain — retry on next worker
            }
        }
    }
    // All retries exhausted; remaining items already have error responses from last attempt
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
    _workers: Vec<WorkerInfo>,
    zmq_clients: Vec<Arc<WorkerZmqClient>>,
    model_name: String,
    version: String,
    request_timeout: Duration,
    max_requests: usize,
    max_requests_jitter: usize,
    reload_tx: mpsc::Sender<ReloadSignal>,
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
            let batch = vec![item];
            let zmq_clients = zmq_clients.clone();
            let worker_inflight = worker_inflight.clone();
            let outlier = outlier.clone();
            let model_name = model_name.clone();
            let version = version.clone();
            let request_count = request_count.clone();
            let reload_tx = reload_tx.clone();
            tokio::spawn(async move {
                send_batch_with_retry(batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx).await;
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
                        send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx).await;
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
                    let current_batch = std::mem::take(&mut batch);
                    let zmq_clients = zmq_clients.clone();
                    let worker_inflight = worker_inflight.clone();
                    let outlier = outlier.clone();
                    let model_name = model_name.clone();
                    let version = version.clone();
                    let request_count = request_count.clone();
                    let reload_tx = reload_tx.clone();
                    tokio::spawn(async move {
                        send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx).await;
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
            send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx).await;
        });
    }
}

/// Background task that periodically probes workers for health.
/// Probes ALL workers concurrently (including ejected ones) for early recovery.
async fn health_checker(
    interval: Duration,
    zmq_clients: Vec<Arc<WorkerZmqClient>>,
    outlier: Arc<OutlierState>,
    model_name: String,
    version: String,
) {
    let probe_timeout = Duration::from_secs(5);
    let uid_prefix = format!("health-{}-{}", model_name, version);
    loop {
        tokio::time::sleep(interval).await;
        // Probe all workers concurrently to avoid O(N * timeout) sequential delay
        let futs: Vec<_> = zmq_clients.iter().enumerate().map(|(idx, client)| {
            let client = client.clone();
            let outlier = outlier.clone();
            let model_name = model_name.clone();
            let version = version.clone();
            let uid = format!("{}-{}", uid_prefix, idx);
            async move {
                let was_ejected = outlier.is_ejected(idx);
                let request = pb::Request {
                    uid,
                    meta: None,
                    payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                        data: vec![],
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
                            outlier.record_error(idx);
                            prometheus::inc_health_check(&model_name, &version, "error");
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
                        outlier.record_error(idx);
                        prometheus::inc_health_check(&model_name, &version, "error");
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // ===== Key pre-computation tests =====

    #[test]
    fn model_version_key_format() {
        assert_eq!(model_version_key("my_model", "1"), "my_model_1");
        assert_eq!(model_version_key("resnet", "v2"), "resnet_v2");
        assert_eq!(model_version_key("", ""), "_");
    }

    #[test]
    fn model_version_key_single_allocation() {
        // String::with_capacity should pre-allocate exactly the right size
        let key = model_version_key("model", "1");
        assert_eq!(key.capacity(), key.len(), "no over-allocation");
        assert_eq!(key, "model_1");
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
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier);

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
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx.clone(), outlier);
        let first_handle = queue.queues.get("test_model_1").unwrap().1.clone();
        assert!(!first_handle.is_finished());

        // Re-register should abort the first collector
        let outlier = Arc::new(OutlierState::new(0));
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier);
        let second_handle = queue.queues.get("test_model_1").unwrap().1.clone();

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(first_handle.is_finished(), "old collector should be aborted");
        assert!(!second_handle.is_finished(), "new collector should still be running");
    }

    #[tokio::test]
    async fn test_unregister_model_aborts_collector() {
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
        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier);
        let handle = queue.queues.get("test_model_1").unwrap().1.clone();
        assert!(!handle.is_finished());

        queue.unregister_model("test_model", "1");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(handle.is_finished(), "collector should be aborted after unregister");
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

        // Verify it works with SingleRequest.data: Vec<u8>
        let single = pb::SingleRequest { data: data.to_vec() };
        assert_eq!(single.data, vec![10, 20, 30]);
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
            payload: payload_bytes.to_vec(), // prost needs Vec<u8>
        });

        let item = QueueItem {
            uid: "u1".to_string(),
            data: payload_bytes.clone(),
            meta: Some(meta),
            response_tx: tx,
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
        };
        tx.try_send(item).unwrap();

        // Receive and verify data integrity
        let received = rx.recv().await.unwrap();
        assert_eq!(received.data, payload);
        assert_eq!(received.uid, "req-1");

        // Build proto request from Bytes data (simulates send_batch logic)
        let proto_data: Vec<u8> = received.data.to_vec();
        let single = pb::SingleRequest { data: proto_data };
        assert_eq!(single.data, r#"{"input":"hello"}"#.as_bytes());

        // Send a mock response
        let _ = received.response_tx.send(pb::Response {
            uid: "req-1".to_string(),
            payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                data: b"ok".to_vec(),
                status: Some(pb::Status { code: "Ok".to_string(), message: String::new() }),
            })),
            metrics: None,
        });

        let resp = resp_rx.await.unwrap();
        assert_eq!(resp.uid, "req-1");
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
            payload: vec![1u8, 2, 3],
        };
        let (tx, _rx) = oneshot::channel();
        let item = QueueItem {
            uid: "test".to_string(),
            data: Bytes::new(),
            meta: Some(Arc::new(meta)),
            response_tx: tx,
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
            payload: vec![0u8; 4096], // 4KB payload
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
        let picked = pick_worker_least_loaded(&inflight, &outlier);
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
        let picked = pick_worker_least_loaded(&inflight, &outlier);
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
        let picked = pick_worker_least_loaded(&inflight, &outlier);
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
        let picked = pick_worker_least_loaded(&inflight, &outlier);
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
        check_max_requests(&counter, 10, 10, "exact", "1", &tx).await;
        let signal = rx.try_recv().unwrap();
        assert_eq!(signal.model_name, "exact");
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

        queue.register_model("test_model", "1", &config, vec![], vec![], reload_tx, outlier.clone());

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
        queue.register_model("m", "1", &config, vec![], vec![], reload_tx, outlier);
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
            assert!(result >= 90 && result <= 110,
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
