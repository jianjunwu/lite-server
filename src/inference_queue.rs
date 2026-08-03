use bytes::Bytes;
use crate::config::ModelConfig;
use crate::error::AppError;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::WorkerInfo;
use crate::sequence::SequenceRegistry;
use crate::transport::zmq::WorkerZmqClient;
use dashmap::DashMap;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot, Notify};
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
    /// 入队时刻（P2-1 扩缩指标）：首次派发时采样 queue_wait_seconds。
    pub enqueued_at: Instant,
}

/// Decrements the per-version in-flight counter and the
/// `liteserver_in_flight_requests` gauge on drop (§4.2 + P2-1 扩缩指标).
pub struct InflightGuard {
    counter: Arc<AtomicUsize>,
    model: String,
    version: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
        prometheus::dec_in_flight(&self.model, &self.version);
    }
}

/// Error types for queue operations.
#[derive(Debug)]
pub enum QueueError {
    Closed,
    NotFound,
    Full,
    /// B3 direct-mode: `x-lite-worker-id` named a worker that does not exist or
    /// is currently ejected. Rejected at submit so a bad pin fails fast with a
    /// clear error (HTTP 400 / gRPC InvalidArgument) instead of silently
    /// rerouting deep in the collector.
    InvalidWorker(String),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::Closed => write!(f, "queue closed"),
            QueueError::NotFound => write!(f, "queue not found for model"),
            QueueError::Full => write!(f, "queue full"),
            QueueError::InvalidWorker(msg) => write!(f, "invalid direct worker pin: {msg}"),
        }
    }
}

// ===== Outlier Detection (inspired by Envoy) =====

/// Per-worker ejection state.
struct EjectedWorker {
    ejected_at: Instant,
    /// false = Open（熔断中，不可选）；true = HalfOpen（退避期满，试探放行——
    /// 一次失败立即以更久退避重熔断，一次成功即闭合并清零退避级数）。
    half_open: bool,
}

/// Per-worker outlier detection tracking.
struct WorkerOutlier {
    consecutive_errors: AtomicUsize,
    /// 连续熔断级数：无成功闭合的累计熔断次数。退避时长 =
    /// `min(base × 2^(series−1), max_timeout)`（sgl-router 清单：指数退避）。
    ejection_series: AtomicUsize,
    ejected: Mutex<Option<EjectedWorker>>,
}

/// Configurable outlier-ejection parameters (§3). `Default` preserves the prior
/// hardcoded behavior, so existing `OutlierState::new` callers are unaffected.
#[derive(Clone)]
pub struct EjectionConfig {
    /// Consecutive errors before a worker is ejected. 0 = never eject.
    pub error_threshold: usize,
    /// Base ejection duration before the first half-open probe. Subsequent
    /// ejections without a successful close back off exponentially:
    /// `min(timeout × 2^(series−1), max_timeout)`.
    pub timeout: Duration,
    /// Max % of workers ejectable at once (1-100).
    pub max_percent: usize,
    /// Cap for the exponential ejection backoff (per-worker 熔断器, B1).
    pub max_timeout: Duration,
}

impl Default for EjectionConfig {
    fn default() -> Self {
        Self {
            error_threshold: 3,
            timeout: Duration::from_secs(30),
            max_percent: 50,
            max_timeout: Duration::from_secs(300),
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
    max_ejection_time: Duration,
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
                    ejection_series: AtomicUsize::new(0),
                    ejected: Mutex::new(None),
                })
                .collect(),
            consecutive_threshold: config.error_threshold,
            base_ejection_time: config.timeout,
            max_ejection_percent: config.max_percent,
            // 防御：上限不得小于 base（配置失误时退化为固定退避）。
            max_ejection_time: config.max_timeout.max(config.timeout),
        }
    }

    /// 第 `series` 次连续熔断的退避时长：`base × 2^(series−1)`，封顶 max。
    /// 指数封顶 2^20——再高也被 max 截断，且避免 Duration::mul_f64 溢出 panic。
    fn backoff(&self, series: usize) -> Duration {
        let shift = series.saturating_sub(1).min(20) as i32;
        self.base_ejection_time.mul_f64(2f64.powi(shift)).min(self.max_ejection_time)
    }

    /// Record a successful request — reset consecutive error count. A success
    /// while half-open closes the circuit: ejection cleared AND the backoff
    /// series reset, so the next ejection starts from the base timeout again.
    pub fn record_success(&self, worker_idx: usize) {
        if let Some(w) = self.workers.get(worker_idx) {
            w.consecutive_errors.store(0, Ordering::Relaxed);
            let mut guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(*guard, Some(ref e) if e.half_open) {
                *guard = None;
                w.ejection_series.store(0, Ordering::Relaxed);
                info!("Worker {worker_idx} circuit closed after a successful half-open probe");
            }
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
            w.ejection_series.store(0, Ordering::Relaxed);
            let mut guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
            *guard = None;
        }
    }

    /// Record an error — increment the consecutive error count, ejecting the
    /// worker at the threshold. Returns `true` only when this call newly
    /// ejects the worker, so callers can record the per-(model, version)
    /// ejection metric exactly once (§4.6).
    ///
    /// 半开特例（熔断器）：半开态的试探请求失败 → 立即重熔断（不等阈值），
    /// 退避级数 +1（指数退避拉长），同样返回 `true`（新熔断事件）。
    pub fn record_error(&self, worker_idx: usize) -> bool {
        let w = match self.workers.get(worker_idx) {
            Some(w) => w,
            None => return false,
        };
        {
            let mut guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(*guard, Some(ref e) if e.half_open) {
                let series = w.ejection_series.fetch_add(1, Ordering::Relaxed) + 1;
                *guard = Some(EjectedWorker { ejected_at: Instant::now(), half_open: false });
                w.consecutive_errors.store(0, Ordering::Relaxed);
                info!(
                    "Worker {worker_idx} circuit re-opened after a failed half-open probe \
                     (series {series}, backoff {:?})",
                    self.backoff(series)
                );
                return true;
            }
            // Open 中的 worker 不经正常挑选（仅 all-ejected 兜底路径可达）——
            // 失败只计数，不改变既有熔断。
            if guard.is_some() {
                w.consecutive_errors.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }
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
            let series = w.ejection_series.fetch_add(1, Ordering::Relaxed) + 1;
            *guard = Some(EjectedWorker { ejected_at: Instant::now(), half_open: false });
            info!(
                "Worker {worker_idx} ejected after {} consecutive errors (series {series}, backoff {:?})",
                self.consecutive_threshold,
                self.backoff(series)
            );
            return true;
        }
        false
    }

    /// Check if a worker is currently ejected, recovering if ejection time has passed.
    ///
    /// 熔断语义：Open 退避期满 → 转 HalfOpen（返回 false = 可试探，错误计数清零
    /// 让试探独立计）；HalfOpen 返回 false（试探放行中）；Open 未期满 → true。
    /// 注意 HalfOpen 不等于恢复——试探结果由 record_success/record_error 收口。
    pub fn is_ejected(&self, worker_idx: usize) -> bool {
        let w = match self.workers.get(worker_idx) {
            Some(w) => w,
            None => return false,
        };
        let mut guard = w.ejected.lock().unwrap_or_else(|e| e.into_inner());
        match *guard {
            None => false,
            Some(ref e) if e.half_open => false,
            Some(ref e) => {
                let series = w.ejection_series.load(Ordering::Relaxed);
                if e.ejected_at.elapsed() >= self.backoff(series) {
                    *guard = Some(EjectedWorker { ejected_at: e.ejected_at, half_open: true });
                    w.consecutive_errors.store(0, Ordering::Relaxed);
                    false
                } else {
                    true
                }
            }
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

// ===== P-FLOW B1 (§4.0.9): priority-aware bounded channel =====
//
// A multi-producer, single-consumer channel that dispatches the HIGHEST-priority
// pending item first (ties broken FIFO by insertion sequence). It is a faithful
// drop-in for the previous `mpsc::channel<QueueItem>`: same `try_send`/`recv`/
// `len`/close surface, so the batch collector is unchanged except for the
// `rx.recv()`/`rx.len()` call sites. With all items at the default priority 0
// (no `x-lite-priority` header) the heap degenerates to plain FIFO, so existing
// behaviour is preserved.

/// A queue item tagged with its scheduling priority and insertion sequence.
struct OrdItem {
    /// Higher value = dispatched first (Triton `priority_levels` semantics;
    /// parsed from the `x-lite-priority` request header, default 0).
    priority: i32,
    /// Monotonic insertion counter — FIFO tiebreak within a priority.
    seq: u64,
    item: QueueItem,
}

impl Ord for OrdItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first; equal priority → lower seq (older) first.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for OrdItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for OrdItem {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for OrdItem {}

struct PriorityInner {
    heap: Mutex<BinaryHeap<OrdItem>>,
    cap: usize,
    seq: AtomicU64,
    /// Live sender count; reaching 0 signals close (recv returns `None` once
    /// drained), mirroring `mpsc` so the batch collector self-exits on drain.
    senders: AtomicUsize,
    notify: Notify,
}

/// Producer handle (cloned per `try_submit`, like `mpsc::Sender`).
pub(crate) struct PrioritySender {
    inner: Arc<PriorityInner>,
}

/// Consumer handle held by the batch-collector task.
pub(crate) struct PriorityReceiver {
    inner: Arc<PriorityInner>,
}

/// Create a bounded priority channel with the given capacity (min 1).
pub(crate) fn priority_channel(cap: usize) -> (PrioritySender, PriorityReceiver) {
    let inner = Arc::new(PriorityInner {
        heap: Mutex::new(BinaryHeap::new()),
        cap: cap.max(1),
        seq: AtomicU64::new(0),
        senders: AtomicUsize::new(1),
        notify: Notify::new(),
    });
    (
        PrioritySender {
            inner: inner.clone(),
        },
        PriorityReceiver { inner },
    )
}

impl PrioritySender {
    /// Push `item` at `priority`. Returns `Full` at capacity, mirroring
    /// `mpsc::Sender::try_send`.
    fn try_send(&self, item: QueueItem, priority: i32) -> Result<(), QueueError> {
        let mut heap = self.inner.heap.lock().unwrap_or_else(|e| e.into_inner());
        if heap.len() >= self.inner.cap {
            return Err(QueueError::Full);
        }
        let seq = self.inner.seq.fetch_add(1, Ordering::Relaxed);
        heap.push(OrdItem {
            priority,
            seq,
            item,
        });
        drop(heap);
        self.inner.notify.notify_one();
        Ok(())
    }
}

impl Clone for PrioritySender {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for PrioritySender {
    fn drop(&mut self) {
        if self.inner.senders.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Last sender gone → wake a parked consumer so it observes close.
            self.inner.notify.notify_one();
        }
    }
}

impl PriorityReceiver {
    /// Pop the highest-priority pending item, or `None` once all senders have
    /// dropped and the queue is drained.
    async fn recv(&self) -> Option<QueueItem> {
        loop {
            // Register interest BEFORE checking state so a push or close that
            // happens between the heap check and the await is not lost.
            let notified = self.inner.notify.notified();
            if let Some(ord) = self.inner.heap.lock().unwrap_or_else(|e| e.into_inner()).pop() {
                return Some(ord.item);
            }
            if self.inner.senders.load(Ordering::Relaxed) == 0 {
                return None;
            }
            notified.await;
        }
    }

    fn len(&self) -> usize {
        self.inner.heap.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// Parse the request's scheduling priority from its `x-lite-priority` header
/// (default 0). Consumed here in B1; P8-1 only defined the field.
fn item_priority(item: &QueueItem) -> i32 {
    item.meta
        .as_ref()
        .and_then(|m| m.headers.get("x-lite-priority"))
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0)
}

/// B3 direct-mode: parse the `x-lite-worker-id` pin off one item's meta headers
/// (malformed values drop to `None` = no pin, hints are best-effort).
fn direct_pin(item: &QueueItem) -> Option<usize> {
    item.meta
        .as_ref()
        .and_then(|m| m.headers.get("x-lite-worker-id"))
        .and_then(|v| v.parse::<u32>().ok())
        .map(|v| v as usize)
}

/// Batch-level direct pin: `Some(w)` when every pin-carrying item agrees on
/// worker `w` (items without a pin don't vote). Conflicting pins → `None`
/// (fall back to normal selection — batches are not split for pins).
fn batch_direct_pin(batch: &[QueueItem]) -> Option<usize> {
    let mut pins = batch.iter().filter_map(direct_pin);
    let first = pins.next()?;
    if pins.all(|p| p == first) {
        Some(first)
    } else {
        warn!("conflicting x-lite-worker-id pins within one batch — falling back to normal worker selection");
        None
    }
}

/// P-FLOW B1 (§4.0.9): reject a request that waited past `queue_timeout` by
/// replying 503 (numeric `status.message` maps to HTTP 503 / gRPC Unavailable
/// in both handlers) and dropping it. The item's `InflightGuard` decrements the
/// in-flight counter on drop.
fn reject_queue_timeout(item: QueueItem) {
    debug!(
        uid = %item.uid,
        waited_secs = item.enqueued_at.elapsed().as_secs_f64(),
        "queue timeout REJECT"
    );
    let _ = item.response_tx.send(pb::Response {
        uid: item.uid.clone(),
        payload: Some(pb::response::Payload::Single(pb::SingleResponse {
            status: Some(pb::Status {
                code: "Error".to_string(),
                message: "503".to_string(),
            }),
            ..Default::default()
        })),
        metrics: None,
    });
}

/// If queue-timeout REJECT is armed and `item` has waited past the deadline,
/// reject it and return `None`; otherwise return `Some(item)`.
fn check_queue_timeout(
    item: QueueItem,
    queue_timeout: Duration,
    action: crate::config::QueueTimeoutAction,
) -> Option<QueueItem> {
    if queue_timeout > Duration::ZERO
        && action == crate::config::QueueTimeoutAction::Reject
        && item.enqueued_at.elapsed() > queue_timeout
    {
        reject_queue_timeout(item);
        None
    } else {
        Some(item)
    }
}

/// Per-(model, version) queue state held in [`InferenceQueue::queues`].
/// Factored out of an anonymous tuple so the field types are self-documenting.
struct VersionQueue {
    tx: PrioritySender,
    collector: std::sync::Arc<tokio::task::JoinHandle<()>>,
    outlier: Arc<OutlierState>,
    /// Health-checker task handle (abortable); `None` if health checks disabled.
    health_checker: Option<std::sync::Arc<tokio::task::JoinHandle<()>>>,
    /// Requests accepted but not yet completed (§4.2 graceful drain).
    inflight_requests: Arc<AtomicUsize>,
    /// Worker count at registration — bounds-checks B3 direct pins at submit.
    worker_count: usize,
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
    /// P8-1: shared sequence→worker affinity map. The batch collector records
    /// and consults it here; the streaming direct path shares the same `Arc`
    /// (threaded out via [`Self::sequence_registry`]).
    sequence_registry: Arc<SequenceRegistry>,
    /// P8-1 (B2): affinity load-balance thresholds.
    balance: BalanceConfig,
}

impl Default for InferenceQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl InferenceQueue {
    pub fn new() -> Self {
        Self::with_sequence(
            Arc::new(SequenceRegistry::new(Duration::from_secs(3600), 65536)),
            BalanceConfig::default(),
        )
    }

    /// Construct with an explicit shared [`SequenceRegistry`] and balance
    /// thresholds so the queue path and the streaming direct path share one
    /// affinity map. [`Self::new`] delegates here with defaults.
    pub(crate) fn with_sequence(
        sequence_registry: Arc<SequenceRegistry>,
        balance: BalanceConfig,
    ) -> Self {
        Self {
            queues: DashMap::new(),
            sequence_registry,
            balance,
        }
    }

    /// Borrow the shared sequence registry (used by the streaming direct path).
    pub fn sequence_registry(&self) -> &Arc<SequenceRegistry> {
        &self.sequence_registry
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
        let (tx, rx) = priority_channel(max_queue_size);

        let max_batch = config.max_batch_size;
        let batch_timeout = Duration::from_secs_f64(config.batch_timeout as f64);
        let adaptive = config.adaptive_batching;
        let min_timeout = Duration::from_secs_f64(config.min_batch_timeout as f64);
        let queue_threshold = config.adaptive_queue_threshold;
        let request_timeout = Duration::from_secs_f64(config.request_timeout as f64);
        let max_requests = config.max_requests;
        let max_requests_jitter = config.max_requests_jitter;
        let max_retries = config.max_retries;
        let queue_timeout = Duration::from_secs_f64(config.queue_timeout_secs.max(0.0) as f64);
        let queue_timeout_action = config.queue_timeout_action;
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
        // B3 direct-pin 边界检查口径：dispatch 索引空间是 zmq_clients（生产上
        // 与 worker_infos 等长；单测常传空 worker_infos + 真实 clients）。
        let dispatch_workers = zmq_clients.len();
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
            queue_timeout,
            queue_timeout_action,
            outlier.clone(),
            self.sequence_registry.clone(),
            self.balance,
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
                worker_count: dispatch_workers,
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
            // B3 direct-mode (x-lite-worker-id): 提交即校验 pin——worker 不存在
            // 或已剔除 → fail-fast（HTTP 400 / gRPC InvalidArgument），不让坏 pin
            // 潜入 collector 静默改路由。校验在计数前，拒绝不占容量/指标。
            if let Some(w) = direct_pin(&item) {
                if w >= entry.worker_count {
                    return Err(QueueError::InvalidWorker(format!(
                        "x-lite-worker-id {w} out of range for {model_name} {version} (workers: {})",
                        entry.worker_count
                    )));
                }
                if entry.outlier.is_ejected(w) {
                    return Err(QueueError::InvalidWorker(format!(
                        "x-lite-worker-id {w} is ejected for {model_name} {version}"
                    )));
                }
            }
            (entry.tx.clone(), entry.inflight_requests.clone())
        };
        // Count from acceptance (not dispatch) so a drain starting right after
        // try_submit returns still observes this request.
        counter.fetch_add(1, Ordering::Relaxed);
        prometheus::inc_in_flight(model_name, version);
        item.inflight_guard = Some(InflightGuard {
            counter,
            model: model_name.to_string(),
            version: version.to_string(),
        });
        let priority = item_priority(&item);
        match sender.try_send(item, priority) {
            Ok(()) => Ok(()),
            Err(QueueError::Full) => {
                debug!(model = %model_name, version = %version, "queue full");
                Err(QueueError::Full)
            }
            // PrioritySender never returns Closed (it has no receiver-drop signal);
            // kept for exhaustiveness with the shared error type.
            Err(other) => Err(other),
        }
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

/// P8-1 (B2) load-balance thresholds for affinity fallback, mirroring SGLang
/// `--balance-abs/rel-threshold`: an affinity worker is abandoned for
/// power-of-two selection when its in-flight count exceeds the least-loaded
/// worker's by more than the absolute or relative threshold. `0` disables that
/// axis. `Copy` so it threads through the collector cheaply.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BalanceConfig {
    pub abs_threshold: u32,
    pub rel_threshold: f32,
}

impl Default for BalanceConfig {
    fn default() -> Self {
        Self {
            abs_threshold: 2,
            rel_threshold: 1.5,
        }
    }
}

/// Resolved affinity hint for one dispatch, derived from the batch's sequenced
/// items. `preferred` is `Some(w)` only when every sequenced item that has a
/// live registry hit agrees on the same worker. Owns `seq` so it does not
/// borrow `batch` (the batch is mutated later in dispatch).
struct Affinity {
    seq: String,
    preferred: Option<usize>,
}

/// Compute the batch's affinity hint from the `sequence_id` each item carries
/// on its `RequestMeta`. Returns `None` when no item carries a sequence_id —
/// non-affinity dispatch, which the picker then routes exactly as before.
///
/// B3 内容亲和：无 `sequence_id` 的项回落 `x-lite-affinity-key`——作为纯
/// rendezvous 哈希 key（无注册表跟踪，preferred=None，无状态确定性路由）；
/// 两类 key 同时存在时 `sequence_id` 优先（它是 affinity_key 的特例）。
fn batch_affinity(
    batch: &[QueueItem],
    reg: &SequenceRegistry,
    model: &str,
    version: &str,
) -> Option<Affinity> {
    let mut first_seq: Option<String> = None;
    let mut agreed: Option<usize> = None;
    let mut disagree = false;
    for item in batch {
        let meta = item.meta.as_ref();
        let seq = meta.and_then(|m| m.sequence_id.as_deref());
        let key = seq.or_else(|| {
            meta.and_then(|m| m.headers.get("x-lite-affinity-key"))
                .map(|s| s.as_str())
                .filter(|s| !s.is_empty())
        });
        let Some(key) = key else { continue };
        if first_seq.is_none() {
            first_seq = Some(key.to_string());
        }
        // 注册表粘性只跟 sequence_id（affinity_key 本身已是确定性哈希路由）。
        if let Some(seq) = seq {
            if let Some(w) = reg.lookup(seq, model, version) {
                match agreed {
                    None => agreed = Some(w),
                    Some(cur) if cur != w => disagree = true,
                    _ => {}
                }
            }
        }
    }
    Some(Affinity {
        seq: first_seq?,
        preferred: if disagree { None } else { agreed },
    })
}

/// P8-1 worker selection. Non-affinity (`affinity == None`) is **identical** to
/// [`pick_worker_least_loaded`] (acceptance: requests without `sequence_id`
/// route exactly as before). Affinity dispatch honors the preferred worker when
/// it is healthy and within the load thresholds; otherwise it falls back to
/// power-of-two selection (overload, B2) or rendezvous hashing (preferred gone
/// or ejected — smooth rehash, B2) over the live workers.
fn pick_worker(
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    exclude: &[usize],
    affinity: Option<Affinity>,
    balance: BalanceConfig,
) -> usize {
    let (seq, preferred) = match affinity {
        None => return pick_worker_least_loaded(inflight, outlier, exclude),
        Some(a) => (a.seq, a.preferred),
    };
    if inflight.len() <= 1 {
        return 0;
    }
    match preferred {
        Some(w) if w < inflight.len() && !outlier.is_ejected(w) && !exclude.contains(&w) => {
            if affinity_overloaded(inflight, outlier, exclude, w, balance) {
                power_of_two_pick(inflight, outlier, exclude)
            } else {
                w
            }
        }
        _ => rendezvous_pick(&seq, inflight.len(), outlier, exclude)
            .unwrap_or_else(|| pick_worker_least_loaded(inflight, outlier, exclude)),
    }
}

/// Is affinity worker `w` loaded enough beyond the least-loaded live worker to
/// justify abandoning stickiness? (B2 absolute + relative thresholds.)
fn affinity_overloaded(
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    exclude: &[usize],
    w: usize,
    balance: BalanceConfig,
) -> bool {
    let mut min = usize::MAX;
    for (i, c) in inflight.iter().enumerate() {
        if i == w || outlier.is_ejected(i) || exclude.contains(&i) {
            continue;
        }
        let l = c.load(Ordering::Relaxed);
        if l < min {
            min = l;
        }
    }
    if min == usize::MAX {
        return false; // no alternative live worker
    }
    let wload = inflight[w].load(Ordering::Relaxed);
    if balance.abs_threshold > 0
        && wload > min.saturating_add(balance.abs_threshold as usize)
    {
        return true;
    }
    if balance.rel_threshold > 0.0 && min > 0 {
        let limit = (min as f64) * (balance.rel_threshold as f64);
        if (wload as f64) > limit {
            return true;
        }
    }
    false
}

/// Power-of-two selection (B2): sample two distinct live workers, keep the
/// less-loaded. Falls back to least-loaded if fewer than two are live.
fn power_of_two_pick(
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    exclude: &[usize],
) -> usize {
    let live: Vec<usize> = (0..inflight.len())
        .filter(|&i| !outlier.is_ejected(i) && !exclude.contains(&i))
        .collect();
    if live.len() < 2 {
        return pick_worker_least_loaded(inflight, outlier, exclude);
    }
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let a = live[rng.gen_range(0..live.len())];
    let b = loop {
        let c = live[rng.gen_range(0..live.len())];
        if c != a {
            break c;
        }
    };
    if inflight[a].load(Ordering::Relaxed) <= inflight[b].load(Ordering::Relaxed) {
        a
    } else {
        b
    }
}

/// Rendezvous hashing (HRW) over live workers — consistent hashing's bounded
/// redistribution with no ring data structure: only the sequences whose
/// preferred worker died move, and they spread deterministically across the
/// survivors. Returns `None` if no worker is live.
pub(crate) fn rendezvous_pick(
    seq: &str,
    num_workers: usize,
    outlier: &OutlierState,
    exclude: &[usize],
) -> Option<usize> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut best: Option<(u64, usize)> = None;
    for i in 0..num_workers {
        if outlier.is_ejected(i) || exclude.contains(&i) {
            continue;
        }
        let mut h = DefaultHasher::new();
        seq.hash(&mut h);
        i.hash(&mut h);
        let hv = h.finish();
        match best {
            None => best = Some((hv, i)),
            Some((bh, _)) if hv > bh => best = Some((hv, i)),
            _ => {}
        }
    }
    best.map(|(_, i)| i)
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
/// P2-1 扩缩指标：worker 饱和度 = 最热 worker 的并发 in-flight batch 数
/// （label 白名单不含 worker_id，故以聚合 gauge 呈现，§6.5 约束 10）。
/// 在 inflight 计数每次增/减后调用。
fn update_worker_saturation(model_name: &str, version: &str, inflight: &[Arc<AtomicUsize>]) {
    let max = inflight
        .iter()
        .map(|c| c.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0);
    prometheus::set_worker_saturation(model_name, version, max as f64);
}

async fn do_send_batch(
    batch: &mut Vec<QueueItem>,
    zmq_clients: &[Arc<WorkerZmqClient>],
    inflight: &[Arc<AtomicUsize>],
    outlier: &OutlierState,
    model_name: &str,
    version: &str,
    request_timeout: Duration,
    exclude: &[usize],
    sequence_registry: &SequenceRegistry,
    balance: BalanceConfig,
) -> Result<(), BatchError> {
    if batch.is_empty() {
        return Ok(());
    }

    let batch_size = batch.len();
    // B3 direct-mode pin（x-lite-worker-id）优先于一切挑选：pin 在提交时已校验
    // （不存在/已剔除→400），此处仅剩提交后竞态（worker 被剔除/重试排除）——
    // warn 降级正常挑选，可用性优先于 hint。
    let pin = batch_direct_pin(batch.as_slice());
    let worker_idx = match pin {
        Some(w) if w < inflight.len() && !outlier.is_ejected(w) && !exclude.contains(&w) => w,
        Some(w) => {
            warn!(
                worker_idx = w,
                "direct pin became invalid after submit (ejected/excluded) — falling back to normal worker selection"
            );
            // P8-1: bias worker selection toward the affinity worker when the batch
            // carries sequence_ids, falling back to least-loaded (no sequence_id ⇒
            // `batch_affinity` returns None ⇒ `pick_worker` behaves exactly as before).
            let affinity = batch_affinity(batch.as_slice(), sequence_registry, model_name, version);
            pick_worker(inflight, outlier, exclude, affinity, balance)
        }
        None => {
            // P8-1: bias worker selection toward the affinity worker when the batch
            // carries sequence_ids, falling back to least-loaded (no sequence_id ⇒
            // `batch_affinity` returns None ⇒ `pick_worker` behaves exactly as before).
            let affinity = batch_affinity(batch.as_slice(), sequence_registry, model_name, version);
            pick_worker(inflight, outlier, exclude, affinity, balance)
        }
    };
    debug!(
        model = %model_name,
        version = %version,
        batch_size = batch_size,
        worker_idx = worker_idx,
        "sending batch"
    );
    inflight[worker_idx].fetch_add(1, Ordering::Relaxed);
    update_worker_saturation(model_name, version, inflight);
    let zmq_client = &zmq_clients[worker_idx];
    prometheus::observe_batch_size(model_name, version, batch_size);
    // P6 GetModelStats: count logical inferences per worker (batch size here;
    // per-attempt like observe_inference_duration above, so retries count too).
    prometheus::record_worker_inference(model_name, version, worker_idx, batch_size);
    // P8-1: record sequence_id → chosen worker for every sequenced item so the
    // next same-sequence request biases here (last-writer-wins; a retry to a
    // different worker updates the mapping to the actual handler).
    for item in batch.iter() {
        if let Some(seq) = item.meta.as_ref().and_then(|m| m.sequence_id.as_deref()) {
            sequence_registry.record(seq, model_name, version, worker_idx);
        }
    }

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
                    update_worker_saturation(model_name, version, inflight);
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
                        update_worker_saturation(model_name, version, inflight);
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
                        update_worker_saturation(model_name, version, inflight);
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
                        update_worker_saturation(model_name, version, inflight);
                        outlier.record_success(worker_idx);
                        debug!(model = %model_name, version = %version, worker_idx = worker_idx, "batch ok");
                        Ok(())
                    }
                }
                _ => {
                    warn!("Unexpected response type from worker for {} {}", model_name, version);
                    inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
                    update_worker_saturation(model_name, version, inflight);
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
            update_worker_saturation(model_name, version, inflight);
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
    sequence_registry: &SequenceRegistry,
    balance: BalanceConfig,
) {
    if batch.is_empty() {
        return;
    }

    // Decrement queue depth once per item — retries must not decrement again
    for _ in 0..batch.len() {
        prometheus::dec_queue_depth(model_name, version);
    }
    // P2-1 扩缩指标：提交 → 首次派发（含攒批等待）采样一次；重试不重复采样。
    for item in &batch {
        prometheus::observe_queue_wait(model_name, version, item.enqueued_at.elapsed().as_secs_f64());
    }

    let batch_size = batch.len();

    // Fast path: single worker, or retries disabled (max_retries == 0)
    if zmq_clients.len() <= 1 || max_retries == 0 {
        let result = do_send_batch(&mut batch, zmq_clients, inflight, outlier, model_name, version, request_timeout, &[], sequence_registry, balance).await;
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
        match do_send_batch(&mut batch, zmq_clients, inflight, outlier, model_name, version, request_timeout, &excluded, sequence_registry, balance).await {
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
    rx: PriorityReceiver,
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
    queue_timeout: Duration,
    queue_timeout_action: crate::config::QueueTimeoutAction,
    outlier: Arc<OutlierState>,
    sequence_registry: Arc<SequenceRegistry>,
    balance: BalanceConfig,
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
            let Some(item) = check_queue_timeout(item, queue_timeout, queue_timeout_action) else {
                continue;
            };
            let batch = vec![item];
            let zmq_clients = zmq_clients.clone();
            let worker_inflight = worker_inflight.clone();
            let outlier = outlier.clone();
            let sequence_registry = sequence_registry.clone();
            let model_name = model_name.clone();
            let version = version.clone();
            let request_count = request_count.clone();
            let reload_tx = reload_tx.clone();
            tokio::spawn(async move {
                send_batch_with_retry(batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries, &sequence_registry, balance).await;
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
                let Some(item) = check_queue_timeout(item, queue_timeout, queue_timeout_action) else {
                    continue;
                };
                batch.push(item);
                if batch.len() >= max_batch_size {
                    let current_batch = std::mem::take(&mut batch);
                    let zmq_clients = zmq_clients.clone();
                    let worker_inflight = worker_inflight.clone();
                    let outlier = outlier.clone();
                    let sequence_registry = sequence_registry.clone();
                    let model_name = model_name.clone();
                    let version = version.clone();
                    let request_count = request_count.clone();
                    let reload_tx = reload_tx.clone();
                    tokio::spawn(async move {
                        send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries, &sequence_registry, balance).await;
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
                    let sequence_registry = sequence_registry.clone();
                    let model_name = model_name.clone();
                    let version = version.clone();
                    let request_count = request_count.clone();
                    let reload_tx = reload_tx.clone();
                    tokio::spawn(async move {
                        send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries, &sequence_registry, balance).await;
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
        let sequence_registry = sequence_registry.clone();
        let model_name = model_name.clone();
        let version = version.clone();
        let request_count = request_count.clone();
        let reload_tx = reload_tx.clone();
        tokio::spawn(async move {
            send_batch_with_retry(current_batch, &zmq_clients, &worker_inflight, &outlier, &model_name, &version, request_timeout, &request_count, max_requests, &reload_tx, max_retries, &sequence_registry, balance).await;
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

    // ===== P8-1 picker tests =====

    fn mk_inflight(loads: &[usize]) -> Vec<Arc<AtomicUsize>> {
        loads
            .iter()
            .map(|&l| Arc::new(AtomicUsize::new(l)))
            .collect()
    }

    fn eject(outlier: &OutlierState, idx: usize) {
        for _ in 0..EjectionConfig::default().error_threshold {
            outlier.record_error(idx);
        }
        assert!(outlier.is_ejected(idx), "test setup: worker {idx} should be ejected");
    }

    /// Acceptance #1: with no `sequence_id`, `pick_worker` is byte-identical to
    /// the pre-existing `pick_worker_least_loaded` across load / exclude / eject
    /// states — stickiness never pertains the non-affinity path.
    #[test]
    fn pick_worker_non_affinity_matches_least_loaded() {
        let bal = BalanceConfig::default();
        let outlier = OutlierState::new(4);
        let inflight = mk_inflight(&[3, 1, 2, 0]);
        for exclude in [vec![], vec![3usize], vec![1usize, 2]] {
            let got = pick_worker(&inflight, &outlier, &exclude, None, bal);
            let want = pick_worker_least_loaded(&inflight, &outlier, &exclude);
            assert_eq!(got, want, "non-affinity must match least-loaded (exclude={exclude:?})");
        }
        // with one ejected worker too
        let outlier2 = OutlierState::new(4);
        eject(&outlier2, 0);
        let inflight2 = mk_inflight(&[0, 5, 5, 5]);
        let got = pick_worker(&inflight2, &outlier2, &[], None, bal);
        let want = pick_worker_least_loaded(&inflight2, &outlier2, &[]);
        assert_eq!(got, want);
    }

    #[test]
    fn pick_worker_affinity_honors_healthy_preferred() {
        let bal = BalanceConfig::default();
        let outlier = OutlierState::new(3);
        let inflight = mk_inflight(&[0, 0, 0]); // balanced → preferred not overloaded
        let aff = Some(Affinity {
            seq: "s1".to_string(),
            preferred: Some(2),
        });
        assert_eq!(pick_worker(&inflight, &outlier, &[], aff, bal), 2);
    }

    #[test]
    fn pick_worker_affinity_falls_back_when_preferred_ejected() {
        let bal = BalanceConfig::default();
        let outlier = OutlierState::new(3);
        eject(&outlier, 1); // preferred worker ejected
        let inflight = mk_inflight(&[0, 0, 0]);
        let chosen = pick_worker(
            &inflight,
            &outlier,
            &[],
            Some(Affinity { seq: "s1".to_string(), preferred: Some(1) }),
            bal,
        );
        assert_ne!(chosen, 1, "must not route to an ejected preferred worker");
        // HRW fallback is deterministic for the same sequence
        let chosen2 = pick_worker(
            &inflight,
            &outlier,
            &[],
            Some(Affinity { seq: "s1".to_string(), preferred: Some(1) }),
            bal,
        );
        assert_eq!(chosen, chosen2);
    }

    #[test]
    fn pick_worker_affinity_power_of_two_when_overloaded() {
        // preferred=0 at load 10, others idle → far beyond abs threshold (2)
        let bal = BalanceConfig {
            abs_threshold: 2,
            rel_threshold: 1.5,
        };
        let outlier = OutlierState::new(3);
        let inflight = mk_inflight(&[10, 0, 0]);
        let chosen = pick_worker(
            &inflight,
            &outlier,
            &[],
            Some(Affinity { seq: "s1".to_string(), preferred: Some(0) }),
            bal,
        );
        assert_ne!(chosen, 0, "an overloaded preferred worker must be abandoned");
        assert!(chosen == 1 || chosen == 2);
    }

    #[test]
    fn rendezvous_is_deterministic_and_skips_ejected() {
        let outlier = OutlierState::new(4);
        eject(&outlier, 0);
        let inflight = mk_inflight(&[0; 4]);
        let a = rendezvous_pick("seq-a", inflight.len(), &outlier, &[]).unwrap();
        let b = rendezvous_pick("seq-a", inflight.len(), &outlier, &[]).unwrap();
        assert_eq!(a, b, "same sequence → same worker (deterministic)");
        assert_ne!(a, 0, "an ejected worker is never chosen");
    }

    // ===== B3 hint 消费：affinity_key 内容亲和 + direct_worker_id 直连钉住 =====

    fn meta_with_headers(headers: &[(&str, &str)]) -> Arc<pb::RequestMeta> {
        Arc::new(pb::RequestMeta {
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        })
    }

    fn hint_item_rx(
        uid: &str,
        meta: Arc<pb::RequestMeta>,
    ) -> (QueueItem, oneshot::Receiver<pb::Response>) {
        let (tx, rx) = oneshot::channel();
        let item = QueueItem {
            uid: uid.to_string(),
            data: Bytes::new(),
            meta: Some(meta),
            response_tx: tx,
            inflight_guard: None,
            enqueued_at: Instant::now(),
        };
        (item, rx)
    }

    fn hint_item(uid: &str, meta: Arc<pb::RequestMeta>) -> QueueItem {
        hint_item_rx(uid, meta).0
    }

    #[test]
    fn batch_affinity_uses_affinity_key_when_no_sequence_id() {
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        let batch =
            vec![hint_item("a", meta_with_headers(&[("x-lite-affinity-key", "tenant-42")]))];
        let aff = batch_affinity(&batch, &reg, "m", "1").expect("affinity from header key");
        assert_eq!(aff.seq, "tenant-42");
        assert_eq!(aff.preferred, None, "affinity_key 无注册表跟踪 → 纯内容哈希路由");
    }

    #[test]
    fn batch_affinity_sequence_id_wins_over_affinity_key() {
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        reg.record("seq-1", "m", "1", 2);
        let meta = pb::RequestMeta {
            sequence_id: Some("seq-1".to_string()),
            headers: [("x-lite-affinity-key".to_string(), "k".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let batch = vec![hint_item("a", Arc::new(meta))];
        let aff = batch_affinity(&batch, &reg, "m", "1").unwrap();
        assert_eq!(aff.seq, "seq-1", "sequence_id 是 affinity_key 的特例，优先");
        assert_eq!(aff.preferred, Some(2), "sequence_id 保留注册表粘性");
    }

    #[test]
    fn affinity_key_routes_deterministically_via_rendezvous() {
        let outlier = OutlierState::new(4);
        let inflight = mk_inflight(&[0; 4]);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        let pick = |uid: &str| {
            let batch =
                vec![hint_item(uid, meta_with_headers(&[("x-lite-affinity-key", "tenant-42")]))];
            let aff = batch_affinity(&batch, &reg, "m", "1");
            pick_worker(&inflight, &outlier, &[], aff, BalanceConfig::default())
        };
        assert_eq!(pick("a"), pick("b"), "同一 affinity_key → 同一 worker（无状态确定性）");
    }

    #[test]
    fn batch_direct_pin_requires_unanimous() {
        let single =
            vec![hint_item("a", meta_with_headers(&[("x-lite-worker-id", "1")]))];
        assert_eq!(batch_direct_pin(&single), Some(1));

        let conflict = vec![
            hint_item("a", meta_with_headers(&[("x-lite-worker-id", "0")])),
            hint_item("b", meta_with_headers(&[("x-lite-worker-id", "1")])),
        ];
        assert_eq!(batch_direct_pin(&conflict), None, "冲突 pin → 退回正常挑选");

        let mixed = vec![
            hint_item("a", meta_with_headers(&[("x-lite-worker-id", "1")])),
            hint_item("b", meta_with_headers(&[])),
        ];
        assert_eq!(batch_direct_pin(&mixed), Some(1), "部分携带 pin → 按携带项");
    }

    /// 注册一个 2-worker 版本（echo worker 以各自 tag 回应，可分辨落点）。
    async fn two_worker_queue(name: &str) -> (InferenceQueue, Arc<OutlierState>) {
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
        let outlier = Arc::new(OutlierState::new(2));
        let mut clients = Vec::new();
        for (i, tag) in [b"w0".as_slice(), b"w1".as_slice()].iter().enumerate() {
            let endpoint = drain_test_endpoint(&format!("{name}-{i}"));
            spawn_tagged_worker(endpoint.clone(), tag.to_vec());
            clients.push(Arc::new(WorkerZmqClient::new(endpoint)));
        }
        queue.register_model("m", "1", &config, vec![], clients, reload_tx, outlier.clone(), None);
        tokio::time::sleep(Duration::from_millis(200)).await;
        (queue, outlier)
    }

    /// PAIR worker：每个请求回一个 data=tag 的 Single（分辨请求落点用）。
    fn spawn_tagged_worker(endpoint: String, tag: Vec<u8>) -> std::thread::JoinHandle<()> {
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
                let resp = pb::Response {
                    uid: req.uid,
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: Bytes::from(tag.clone()),
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
    async fn try_submit_rejects_direct_pin_out_of_range() {
        let (queue, _outlier) = two_worker_queue("pin-range").await;
        let item = hint_item("p", meta_with_headers(&[("x-lite-worker-id", "5")]));
        let err = queue.try_submit("m", "1", item).unwrap_err();
        assert!(
            matches!(err, QueueError::InvalidWorker(_)),
            "pin 到不存在的 worker → InvalidWorker，got {err:?}"
        );
        // 拒绝不消耗队列容量/计数——随后合法提交必须成功。
        let ok = hint_item("ok", meta_with_headers(&[]));
        assert!(queue.try_submit("m", "1", ok).is_ok());
    }

    #[tokio::test]
    async fn try_submit_rejects_direct_pin_to_ejected_worker() {
        let (queue, outlier) = two_worker_queue("pin-ejected").await;
        for _ in 0..EjectionConfig::default().error_threshold {
            outlier.record_error(1);
        }
        assert!(outlier.is_ejected(1), "setup: worker 1 ejected");

        let item = hint_item("p", meta_with_headers(&[("x-lite-worker-id", "1")]));
        let err = queue.try_submit("m", "1", item).unwrap_err();
        assert!(matches!(err, QueueError::InvalidWorker(_)), "pin 已剔除 worker → InvalidWorker");

        // pin 健康 worker 仍放行。
        let ok = hint_item("ok", meta_with_headers(&[("x-lite-worker-id", "0")]));
        assert!(queue.try_submit("m", "1", ok).is_ok());
    }

    #[tokio::test]
    async fn dispatch_honors_direct_worker_pin() {
        let (queue, _outlier) = two_worker_queue("pin-dispatch").await;
        for (pin, want) in [(1u32, b"w1".as_slice()), (0, b"w0".as_slice())] {
            let (item, rx) =
                hint_item_rx("p", meta_with_headers(&[("x-lite-worker-id", &pin.to_string())]));
            queue.try_submit("m", "1", item).unwrap();
            let resp = tokio::time::timeout(Duration::from_secs(5), rx)
                .await
                .expect("response in time")
                .expect("channel open");
            let data = match resp.payload {
                Some(pb::response::Payload::Single(s)) => s.data,
                other => panic!("expected Single, got {other:?}"),
            };
            assert_eq!(data, want, "pin={pin} must land on worker {pin}");
        }
    }

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
                enqueued_at: Instant::now(),
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
                enqueued_at: Instant::now(),
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
                enqueued_at: Instant::now(),
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
                enqueued_at: Instant::now(),
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
        // Full integration: QueueItem with Bytes flows through the priority
        // channel and send_batch builds correct protobuf request without data
        // corruption.
        let (tx, rx) = priority_channel(10);

        let payload = Bytes::from(r#"{"input":"hello"}"#.as_bytes().to_vec());
        let (resp_tx, resp_rx) = oneshot::channel();
        let item = QueueItem {
            uid: "req-1".to_string(),
            data: payload.clone(),
            meta: None,
            response_tx: resp_tx,
                  inflight_guard: None,
                enqueued_at: Instant::now(),
        };
        tx.try_send(item, 0).unwrap();

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
        // (e.g. early 400 from before_decode_request, custom headers from after_encode_response)
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
                enqueued_at: Instant::now(),
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
            max_timeout: Duration::from_secs(300),
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
            max_timeout: Duration::from_secs(300),
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

    // ===== 熔断器：指数退避 + 半开试探（B1 sgl-router 清单兑现）=====

    /// 短超时熔断配置：threshold=1 即熔断。base 200ms / 上限 2000ms（不是 60ms：
    /// 半开/退避测试用真实 sleep,60ms base + 80ms sleep 只留 20ms 余量,macOS
    /// CI 定时器抖动会翻转边界断言;200ms base 留 ~100ms 余量,稳）。
    fn breaker_cfg() -> EjectionConfig {
        EjectionConfig {
            error_threshold: 1,
            timeout: Duration::from_millis(200),
            max_percent: 50,
            max_timeout: Duration::from_millis(2_000),
        }
    }

    #[test]
    fn half_open_probe_failure_reopens_with_longer_backoff() {
        // A failed half-open probe must re-open the circuit with a LONGER
        // (exponential) backoff: the ejection series increments, so
        // backoff(series) doubles. Assert the series + backoff VALUE directly
        // — the old sleep-based "still ejected at 2×base / half-open after
        // 2×base" checks flaked on macOS CI (>30% timer variance). Only the
        // half-open TRANSITION uses a real sleep (one timing point, generous
        // margin) and it was reliable across CI runs.
        let outlier = OutlierState::with_config(1, &breaker_cfg());
        outlier.record_error(0); // threshold 1 → eject, series 1, backoff = base
        assert!(outlier.is_ejected(0));
        assert_eq!(outlier.workers[0].ejection_series.load(Ordering::Relaxed), 1);

        std::thread::sleep(Duration::from_millis(300)); // > 200ms base → half-open
        assert!(!outlier.is_ejected(0), "退避期满 → 半开（可试探）");

        // 半开试探失败 → 立即重熔断（不等 threshold）；series 1→2，退避升为 2×base。
        assert!(outlier.record_error(0), "half-open probe failure is a new ejection event");
        assert!(outlier.is_ejected(0));
        assert_eq!(
            outlier.workers[0].ejection_series.load(Ordering::Relaxed),
            2,
            "half-open probe failure doubles the backoff series"
        );
        assert_eq!(
            outlier.backoff(2),
            outlier.backoff(1) * 2,
            "backoff(2) is exactly 2× backoff(1) — exponential, not capped here"
        );
    }

    #[test]
    fn half_open_probe_success_closes_and_resets_backoff_series() {
        // Assert the series counter directly (timing-free) instead of via
        // wall-clock sleeps — see half_open_probe_failure_reopens_with_longer_backoff.
        let outlier = OutlierState::with_config(1, &breaker_cfg());
        outlier.record_error(0);
        assert_eq!(outlier.workers[0].ejection_series.load(Ordering::Relaxed), 1);
        std::thread::sleep(Duration::from_millis(300));
        assert!(!outlier.is_ejected(0)); // 半开

        outlier.record_success(0); // 试探成功 → 闭合 + 退避级数清零
        assert!(!outlier.is_ejected(0));
        assert_eq!(outlier.consecutive_errors(0), 0);
        assert_eq!(
            outlier.workers[0].ejection_series.load(Ordering::Relaxed),
            0,
            "success closes the circuit and resets the backoff series"
        );

        // 级数已清零：再次熔断回到 series 1（base 退避），而非 2×base。
        outlier.record_error(0);
        assert!(outlier.is_ejected(0));
        assert_eq!(
            outlier.workers[0].ejection_series.load(Ordering::Relaxed),
            1,
            "series reset → next ejection starts at base backoff (series 1, not 2)"
        );
    }

    #[test]
    fn backoff_capped_at_max_timeout() {
        let cfg = EjectionConfig {
            error_threshold: 1,
            timeout: Duration::from_millis(50),
            max_percent: 50,
            max_timeout: Duration::from_millis(120),
        };
        let outlier = OutlierState::with_config(1, &cfg);
        // series: 1→50ms, 2→100ms, 3→min(200,120)=120ms, 4→cap 120ms
        for (round, expected_ms) in [50u64, 100, 120, 120].iter().enumerate() {
            outlier.record_error(0);
            assert!(outlier.is_ejected(0), "round {round}: must be ejected");
            std::thread::sleep(Duration::from_millis(expected_ms + 30));
            assert!(
                !outlier.is_ejected(0),
                "round {round}: after {expected_ms}ms+margin must be half-open (cap=120ms)"
            );
        }
    }

    #[test]
    fn closed_worker_success_clears_errors_without_touching_series() {
        // 闭合态普通成功：清零连续错误（既有语义），不影响退避级数。
        let outlier = OutlierState::with_config(1, &breaker_cfg());
        outlier.record_error(0);
        assert!(outlier.is_ejected(0));
        outlier.reset(0); // 模拟进程替换：全清
        outlier.record_success(0);
        assert!(!outlier.is_ejected(0));
        assert_eq!(outlier.consecutive_errors(0), 0);
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
            max_timeout: Duration::from_secs(300),
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
                enqueued_at: Instant::now(),
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

    // ===== P2-1 扩展: 扩缩一等指标 =====

    /// Register a single-worker model backed by a slow echo worker (delay per
    /// request), returning the queue. Caller submits and awaits the response.
    fn scaling_metric_queue(model: &str, endpoint_name: &str, delay: Duration) -> InferenceQueue {
        let endpoint = drain_test_endpoint(endpoint_name);
        let _worker = spawn_echo_worker(endpoint.clone(), delay);
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
        queue.register_model(model, "1", &config, vec![], vec![client], reload_tx, outlier, None);
        queue
    }

    fn submit_one(queue: &InferenceQueue, model: &str, uid: &str) -> oneshot::Receiver<pb::Response> {
        let (resp_tx, resp_rx) = oneshot::channel();
        queue
            .try_submit(model, "1", QueueItem {
                uid: uid.to_string(),
                data: Bytes::new(),
                meta: None,
                response_tx: resp_tx,
                inflight_guard: None,
                enqueued_at: Instant::now(),
            })
            .unwrap();
        resp_rx
    }

    #[tokio::test]
    async fn should_track_in_flight_requests_gauge() {
        let queue = scaling_metric_queue("ifm", "ifm", Duration::from_millis(300));
        tokio::time::sleep(Duration::from_millis(200)).await;

        let gauge = prometheus::IN_FLIGHT_REQUESTS.with_label_values(&["ifm", "1"]);
        let base = gauge.get();

        let resp_rx = submit_one(&queue, "ifm", "ifm-1");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(gauge.get() >= base + 1.0, "accepted request must count in-flight, got {}", gauge.get());

        let _resp = tokio::time::timeout(Duration::from_secs(5), resp_rx)
            .await.expect("response must arrive").expect("channel open");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(gauge.get(), base, "completed request must leave the gauge");
    }

    #[tokio::test]
    async fn should_observe_queue_wait_seconds_on_dispatch() {
        let queue = scaling_metric_queue("qwm", "qwm", Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(200)).await;

        let before = prometheus::QUEUE_WAIT_SECONDS
            .with_label_values(&["qwm", "1"]).get_sample_count();
        let resp_rx = submit_one(&queue, "qwm", "qwm-1");
        let _resp = tokio::time::timeout(Duration::from_secs(5), resp_rx)
            .await.expect("response must arrive").expect("channel open");

        let after = prometheus::QUEUE_WAIT_SECONDS
            .with_label_values(&["qwm", "1"]).get_sample_count();
        assert_eq!(after, before + 1, "each dispatched item must observe one queue-wait sample");
    }

    #[tokio::test]
    async fn should_report_worker_saturation_while_batch_in_flight() {
        let queue = scaling_metric_queue("swm", "swm", Duration::from_millis(300));
        tokio::time::sleep(Duration::from_millis(200)).await;

        let gauge = prometheus::WORKER_SATURATION.with_label_values(&["swm", "1"]);
        let resp_rx = submit_one(&queue, "swm", "swm-1");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(gauge.get() >= 1.0, "hottest worker must report saturation >= 1 while in flight, got {}", gauge.get());

        let _resp = tokio::time::timeout(Duration::from_secs(5), resp_rx)
            .await.expect("response must arrive").expect("channel open");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(gauge.get(), 0.0, "saturation must return to 0 after completion");
    }

    // ===== P-FLOW B1 (§4.0.9): priority queue + queue-timeout REJECT =====

    fn b1_item(uid: &str, priority_header: Option<i32>) -> QueueItem {
        let (response_tx, _response_rx) = oneshot::channel();
        let mut headers = std::collections::HashMap::<String, String>::new();
        if let Some(p) = priority_header {
            headers.insert("x-lite-priority".to_string(), p.to_string());
        }
        QueueItem {
            uid: uid.to_string(),
            data: Bytes::new(),
            meta: Some(Arc::new(pb::RequestMeta {
                headers,
                ..Default::default()
            })),
            response_tx,
            inflight_guard: None,
            enqueued_at: Instant::now(),
        }
    }

    #[tokio::test]
    async fn b1_priority_channel_dispatches_highest_priority_first() {
        let (tx, rx) = priority_channel(8);
        // Push in mixed order; high must come out first.
        tx.try_send(b1_item("low", Some(0)), 0).unwrap();
        tx.try_send(b1_item("high", Some(9)), 9).unwrap();
        tx.try_send(b1_item("mid", Some(5)), 5).unwrap();
        assert_eq!(rx.recv().await.unwrap().uid, "high");
        assert_eq!(rx.recv().await.unwrap().uid, "mid");
        assert_eq!(rx.recv().await.unwrap().uid, "low");
    }

    #[tokio::test]
    async fn b1_priority_channel_fifo_tiebreak_within_same_priority() {
        let (tx, rx) = priority_channel(8);
        tx.try_send(b1_item("a", Some(1)), 1).unwrap();
        tx.try_send(b1_item("b", Some(1)), 1).unwrap();
        tx.try_send(b1_item("c", Some(1)), 1).unwrap();
        assert_eq!(rx.recv().await.unwrap().uid, "a");
        assert_eq!(rx.recv().await.unwrap().uid, "b");
        assert_eq!(rx.recv().await.unwrap().uid, "c");
    }

    #[tokio::test]
    async fn b1_priority_channel_cap_returns_full() {
        let (tx, rx) = priority_channel(1);
        tx.try_send(b1_item("x", None), 0).unwrap();
        assert_eq!(rx.len(), 1);
        assert!(matches!(
            tx.try_send(b1_item("y", None), 0),
            Err(QueueError::Full)
        ));
    }

    #[tokio::test]
    async fn b1_priority_channel_close_returns_none_after_drain() {
        let (tx, rx) = priority_channel(8);
        tx.try_send(b1_item("x", None), 0).unwrap();
        drop(tx); // last sender gone → close
        assert!(rx.recv().await.is_some(), "drain remaining item");
        assert!(
            rx.recv().await.is_none(),
            "closed channel must return None once drained"
        );
    }

    #[test]
    fn b1_item_priority_reads_header_default_zero() {
        assert_eq!(item_priority(&b1_item("x", Some(7))), 7);
        assert_eq!(item_priority(&b1_item("x", None)), 0);
    }

    fn enqueued_ago(uid: &str, ago: Duration) -> QueueItem {
        let (response_tx, response_rx) = oneshot::channel();
        std::mem::forget(response_rx); // caller may or may not observe the reply
        QueueItem {
            uid: uid.to_string(),
            data: Bytes::new(),
            meta: None,
            response_tx,
            inflight_guard: None,
            enqueued_at: Instant::now()
                .checked_sub(ago)
                .unwrap_or_else(Instant::now),
        }
    }

    #[tokio::test]
    async fn b1_check_queue_timeout_rejects_expired_when_reject() {
        let (response_tx, response_rx) = oneshot::channel();
        let item = QueueItem {
            uid: "late".into(),
            data: Bytes::new(),
            meta: None,
            response_tx,
            inflight_guard: None,
            enqueued_at: Instant::now()
                .checked_sub(Duration::from_secs(2))
                .unwrap(),
        };
        let opt = check_queue_timeout(
            item,
            Duration::from_millis(100),
            crate::config::QueueTimeoutAction::Reject,
        );
        assert!(opt.is_none(), "expired item must be rejected");
        let resp = response_rx.await.unwrap();
        match resp.payload {
            Some(pb::response::Payload::Single(s)) => {
                assert_eq!(s.status.unwrap().message, "503", "reject maps to 503");
            }
            _ => panic!("expected single response payload"),
        }
    }

    #[tokio::test]
    async fn b1_check_queue_timeout_passes_within_deadline() {
        let opt = check_queue_timeout(
            enqueued_ago("ok", Duration::ZERO),
            Duration::from_secs(10),
            crate::config::QueueTimeoutAction::Reject,
        );
        assert!(opt.is_some(), "fresh item must not be rejected");
    }

    #[tokio::test]
    async fn b1_check_queue_timeout_delay_does_not_reject() {
        let opt = check_queue_timeout(
            enqueued_ago("late", Duration::from_secs(5)),
            Duration::from_millis(100),
            crate::config::QueueTimeoutAction::Delay,
        );
        assert!(opt.is_some(), "Delay action must not reject even when expired");
    }

    #[tokio::test]
    async fn b1_check_queue_timeout_disabled_when_zero() {
        let opt = check_queue_timeout(
            enqueued_ago("late", Duration::from_secs(5)),
            Duration::ZERO,
            crate::config::QueueTimeoutAction::Reject,
        );
        assert!(opt.is_some(), "queue_timeout=0 must not reject");
    }
}
