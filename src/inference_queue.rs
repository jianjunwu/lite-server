use crate::config::ModelConfig;
use crate::error::AppError;
use crate::metrics::prometheus;
use crate::registry::types::WorkerInfo;
use crate::transport::uds::send_batch_to_worker;
use crate::worker::protocol::{
    BatchInferenceResponse, BatchItem, BatchResponseItem, InferenceRequest, InferenceResponse,
    RequestPayload, ResponseStatus,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, trace, warn};

/// A single item waiting in the inference queue.
pub struct QueueItem {
    pub uid: String,
    pub data: serde_json::Value,
    pub response_tx: oneshot::Sender<InferenceResponse>,
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

/// Per-model-version inference queue with batch aggregation.
pub struct InferenceQueue {
    queues: DashMap<String, mpsc::Sender<QueueItem>>,
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
    ) {
        let key = format!("{}_{}", model_name, version);

        // Remove stale queue if exists
        self.queues.remove(&key);

        let max_queue_size = config.max_queue_size.max(1);
        let (tx, rx) = mpsc::channel(max_queue_size);

        let max_batch = config.max_batch_size;
        let batch_timeout = Duration::from_secs_f64(config.batch_timeout as f64);
        let adaptive = config.adaptive_batching;
        let min_timeout = Duration::from_secs_f64(config.min_batch_timeout as f64);
        let queue_threshold = config.adaptive_queue_threshold;

        self.queues.insert(key, tx);

        tokio::spawn(batch_collector(
            rx,
            max_batch,
            batch_timeout,
            adaptive,
            min_timeout,
            queue_threshold,
            workers,
            model_name.to_string(),
            version.to_string(),
        ));
    }

    /// Unregister a model version and stop its collector.
    pub fn unregister_model(&self, model_name: &str, version: &str) {
        let key = format!("{}_{}", model_name, version);
        self.queues.remove(&key);
    }

    /// Submit a single request to the queue (non-blocking).
    /// Returns QueueError::Full immediately if the queue is at capacity.
    pub fn try_submit(
        &self,
        model_name: &str,
        version: &str,
        item: QueueItem,
    ) -> Result<(), QueueError> {
        let key = format!("{}_{}", model_name, version);
        let sender = {
            let entry = self
                .queues
                .get(&key)
                .ok_or(QueueError::NotFound)?
                .clone();
            entry
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
        let key = format!("{}_{}", model_name, version);
        self.queues.contains_key(&key)
    }
}

/// Pick the worker with the lowest inflight count (least-loaded).
fn pick_worker_least_loaded(inflight: &[Arc<AtomicUsize>]) -> usize {
    if inflight.len() == 1 {
        return 0;
    }
    let mut min_idx = 0;
    let mut min_val = inflight[0].load(Ordering::Relaxed);
    for (i, count) in inflight.iter().enumerate().skip(1) {
        let val = count.load(Ordering::Relaxed);
        if val < min_val {
            min_val = val;
            min_idx = i;
        }
    }
    min_idx
}

/// Send a batch of requests to a worker and distribute responses.
async fn send_batch(
    batch: &mut Vec<QueueItem>,
    workers: &[WorkerInfo],
    inflight: &[Arc<AtomicUsize>],
    model_name: &str,
    version: &str,
) {
    if batch.is_empty() {
        return;
    }

    let worker_idx = pick_worker_least_loaded(inflight);
    let worker = &workers[worker_idx];
    inflight[worker_idx].fetch_add(1, Ordering::Relaxed);

    // Build batch items
    let items: Vec<BatchItem> = batch
        .iter()
        .map(|item| BatchItem {
            uid: item.uid.clone(),
            data: item.data.clone(),
        })
        .collect();

    let request = InferenceRequest {
        uid: format!("batch-{}", uuid::Uuid::new_v4()),
        payload: RequestPayload::BatchInfer { items },
    };

    // Track queue depth decrease for all items in the batch
    for _ in 0..batch.len() {
        prometheus::dec_queue_depth(model_name, version);
    }

    let result = send_batch_to_worker(&worker.uds_path, request).await;

    match result {
        Ok(batch_resp) => {
            // Record worker-reported batch metrics
            crate::metrics::prometheus::record_worker_metrics(model_name, &batch_resp.metrics);

            // Build uid -> response item map for O(1) lookup
            let resp_map: std::collections::HashMap<String, BatchResponseItem> = batch_resp
                .items
                .into_iter()
                .map(|item| (item.uid.clone(), item))
                .collect();

            for queue_item in batch.drain(..) {
                let resp = if let Some(resp_item) = resp_map.get(&queue_item.uid) {
                    InferenceResponse {
                        uid: queue_item.uid,
                        data: resp_item.data.clone(),
                        status: ResponseStatus {
                            code: resp_item.status.code.clone(),
                            message: resp_item.status.message.clone(),
                        },
                        worker_id: resp_item.worker_id,
                        metrics: None,
                    }
                } else {
                    InferenceResponse {
                        uid: queue_item.uid,
                        data: None,
                        status: ResponseStatus::error("missing in batch response"),
                        worker_id: worker_idx as u32,
                        metrics: None,
                    }
                };
                let _ = queue_item.response_tx.send(resp);
            }
        }
        Err(e) => {
            error!(
                "Batch request failed for {} {}: {}",
                model_name, version, e
            );
            for queue_item in batch.drain(..) {
                let resp = InferenceResponse {
                    uid: queue_item.uid,
                    data: None,
                    status: ResponseStatus::error(e.to_string()),
                    worker_id: worker_idx as u32,
                    metrics: None,
                };
                let _ = queue_item.response_tx.send(resp);
            }
        }
    }

    inflight[worker_idx].fetch_sub(1, Ordering::Relaxed);
}

/// Compute adaptive batch timeout based on current batch size, queue depth, and config.
///
/// Algorithm:
/// - If batch is already full → return zero (dispatch immediately)
/// - If batch is >50% full → aggressively reduce timeout
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
    // pressure 0.0 → full timeout
    // pressure 0.5 → half timeout
    // pressure 1.0 → min timeout
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
    workers: Vec<WorkerInfo>,
    model_name: String,
    version: String,
) {
    if max_batch_size <= 1 {
        // Fast path: no batching, send immediately
        let worker_inflight: Vec<Arc<AtomicUsize>> = (0..workers.len())
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();

        while let Some(item) = rx.recv().await {
            let mut batch = vec![item];
            send_batch(&mut batch, &workers, &worker_inflight, &model_name, &version).await;
        }
        return;
    }

    let mut batch: Vec<QueueItem> = Vec::with_capacity(max_batch_size);
    let worker_inflight: Vec<Arc<AtomicUsize>> = (0..workers.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    let mut deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            biased;
            Some(item) = rx.recv() => {
                prometheus::inc_queue_depth(&model_name, &version);
                batch.push(item);
                if batch.len() >= max_batch_size {
                    send_batch(&mut batch, &workers, &worker_inflight, &model_name, &version).await;
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
                    send_batch(&mut batch, &workers, &worker_inflight, &model_name, &version).await;
                }
                deadline = None;
            }
            else => break,
        }
    }

    // Drain remaining items
    if !batch.is_empty() {
        send_batch(&mut batch, &workers, &worker_inflight, &model_name, &version).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Empty batch, no queue → should use ~full timeout
        let result = compute_adaptive_timeout(0, 0, 8, base, min, 10);
        assert!(result >= Duration::from_millis(49), "low load should use near-full timeout");
        // Allow tiny floating-point drift from mul_f32
        assert!(result <= base + Duration::from_micros(1), "should never exceed base timeout");
    }

    #[test]
    fn test_adaptive_timeout_high_queue_pressure() {
        let base = Duration::from_millis(50);
        let min = Duration::from_millis(1);
        // Empty batch but queue depth exceeds threshold → should use min timeout
        let result = compute_adaptive_timeout(1, 20, 8, base, min, 10);
        assert_eq!(result, min, "high queue pressure should use min timeout");
    }

    #[test]
    fn test_adaptive_timeout_fill_pressure() {
        let base = Duration::from_millis(50);
        let min = Duration::from_millis(1);
        // Batch 50% full → timeout should be significantly reduced
        let result = compute_adaptive_timeout(4, 0, 8, base, min, 10);
        assert!(result < base, "50% fill should reduce timeout");
        assert!(result >= min, "should respect min timeout");
    }

    #[test]
    fn test_adaptive_timeout_respects_minimum() {
        let base = Duration::from_secs(1);
        let min = Duration::from_millis(5);
        // Extreme pressure: scale = 0.01 → scaled = 10ms, which is > min (5ms)
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
}
