//! Custom @route declarations per model version (upserted at worker
//! handshake, cleared on unload) and the random worker-picking helpers
//! shared by the gRPC/HTTP streaming paths.

use super::WorkerManager;
use crate::inference_queue::{model_version_key, rendezvous_pick, OutlierState};
use crate::proto::liteserver as pb;
use crate::sequence::SequenceRegistry;
use crate::worker::protocol::{is_reserved_route, RouteDecl};

impl WorkerManager {
    /// Upsert custom @route declarations for a model version, received from the
    /// Python worker handshake. Routes colliding with a system-reserved leaf
    /// (`is_reserved_route`) are skipped with a warning — they must not shadow
    /// the inference / admin / streaming contract. Idempotent across workers of
    /// the same version (all run identical LitAPI code).
    pub async fn upsert_routes(
        &self,
        model_name: &str,
        version: &str,
        routes: Vec<RouteDecl>,
    ) {
        let mut accepted: Vec<RouteDecl> = Vec::with_capacity(routes.len());
        for r in routes {
            if is_reserved_route(&r.route) {
                tracing::warn!(
                    model = %model_name, version = %version, route = %r.route,
                    "custom route rejected: collides with a system-reserved leaf"
                );
                continue;
            }
            accepted.push(r);
        }
        let key = model_version_key(model_name, version);
        let mut guard = self.route_table.write().await;
        guard.insert(key, accepted);
    }

    /// Custom routes declared for a model version (empty if none / unloaded).
    pub async fn get_routes(&self, model_name: &str, version: &str) -> Vec<RouteDecl> {
        let key = model_version_key(model_name, version);
        self.route_table
            .read()
            .await
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    /// Drop custom routes for a model version (on unload / removal).
    pub async fn clear_routes(&self, model_name: &str, version: &str) {
        let key = model_version_key(model_name, version);
        self.route_table.write().await.remove(&key);
    }
}

/// Pick a random worker index.
pub fn pick_worker_random(num_workers: usize) -> usize {
    use rand::Rng;
    rand::thread_rng().gen_range(0..num_workers.max(1))
}

/// Pick a random non-ejected worker index. Falls back to any worker if all are ejected.
pub fn pick_worker_skip_ejected(num_workers: usize, outlier: &OutlierState) -> usize {
    use rand::Rng;
    if num_workers <= 1 {
        return 0;
    }

    let mut rng = rand::thread_rng();
    let start = rng.gen_range(0..num_workers);

    // Try to find a non-ejected, live worker starting from random offset
    for i in 0..num_workers {
        let idx = (start + i) % num_workers;
        if !outlier.is_ejected(idx) && !outlier.is_dead(idx) {
            return idx;
        }
    }

    // All ejected — fall back to random
    start
}

/// Error from [`pick_streaming_worker`]: a direct pin (`x-lite-worker-id`)
/// named a worker that does not exist or is ejected. The call site maps this to
/// HTTP 400 (`AppError::Validation`) / gRPC `InvalidArgument` — parity with the
/// queue's `QueueError::InvalidWorker`.
#[derive(Debug, Clone)]
pub(crate) enum PickError {
    /// Bad client pin (`x-lite-worker-id`) — callers map to 400 /
    /// InvalidArgument (client error, not retryable as-is).
    InvalidPin(String),
    /// Every worker process has exited — callers map to 503 / Unavailable
    /// (server-side, retryable after operator intervention).
    NoLiveWorkers(String),
}

/// Streaming worker selection (task F): one shared pick replacing the four
/// line-for-line copies in HTTP SSE/WS (`open_worker_stream`) + gRPC
/// stream/decoupled/bidi. Priority mirrors the unary queue path
/// (`inference_queue`): `x-lite-worker-id` direct pin > sequence_id stickiness >
/// `x-lite-affinity-key` rendezvous > skip-ejected/random.
///
/// A direct pin to an out-of-range or ejected worker fails fast with
/// [`PickError`] (so a bad pin can't silently reroute); the softer hints
/// (sequence / affinity) are best-effort and fall back. When the request carries
/// a sequence_id the chosen worker is recorded for future stickiness.
///
/// `meta.headers` is the same map forwarded to the worker, so the hints match
/// what the client sent (call sites must invoke this before `meta` moves into
/// the stream-open request).
pub(crate) fn pick_streaming_worker(
    meta: &pb::RequestMeta,
    num_workers: usize,
    outlier: Option<&OutlierState>,
    seq_registry: &SequenceRegistry,
    model: &str,
    version: &str,
) -> Result<usize, PickError> {
    // 0. Crash-death gate: with every worker process dead, no pick can ever
    //    succeed — fail fast instead of routing to a silent PAIR socket.
    if let Some(o) = outlier {
        if o.all_dead() {
            return Err(PickError::NoLiveWorkers(format!(
                "all workers for {model} {version} have exited"
            )));
        }
    }

    // 1. x-lite-worker-id direct pin — validated, fail-fast (parity with the
    //    queue's try_submit direct-pin check at submit time).
    if let Some(w) = meta
        .headers
        .get("x-lite-worker-id")
        .and_then(|v| v.parse::<usize>().ok())
    {
        let ejected = outlier.map(|o| o.is_ejected(w) || o.is_dead(w)).unwrap_or(false);
        if w >= num_workers {
            return Err(PickError::InvalidPin(format!(
                "x-lite-worker-id {w} out of range (workers: {num_workers}) for {model} {version}"
            )));
        }
        if ejected {
            return Err(PickError::InvalidPin(format!(
                "x-lite-worker-id {w} is ejected or dead for {model} {version}"
            )));
        }
        return Ok(w);
    }

    // 2. sequence_id stickiness (best-effort) → 3. affinity_key rendezvous →
    //    skip-ejected/random.
    // P8-1 审计 B1：proto3 零值（空串）归一化为「未携带」，否则零值客户端
    // 被归组到同一粘性 key。
    let seq_normalized = crate::sequence::normalize_sequence_id(meta.sequence_id.as_deref());
    let preferred = seq_normalized.and_then(|seq| {
        let w = seq_registry.lookup(seq, model, version)?;
        let unusable = outlier
            .map(|o| o.is_ejected(w) || o.is_dead(w))
            .unwrap_or(false);
        (w < num_workers && !unusable).then_some(w)
    });
    let worker_id = preferred.unwrap_or_else(|| match outlier {
        Some(o) => meta
            .headers
            .get("x-lite-affinity-key")
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .and_then(|key| rendezvous_pick(key, num_workers, o, &[]))
            .unwrap_or_else(|| pick_worker_skip_ejected(num_workers, o)),
        None => pick_worker_random(num_workers),
    });

    // Record the sticky mapping so the next request with this sequence_id lands
    // on the same worker (matches the prior inline behavior).
    if let Some(seq) = seq_normalized {
        seq_registry.record(seq, model, version, worker_id);
    }
    Ok(worker_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use std::sync::Arc;

    #[test]
    fn test_pick_worker_random_single() {
        // With 1 worker, always returns 0
        for _ in 0..100 {
            assert_eq!(pick_worker_random(1), 0);
        }
    }

    #[test]
    fn test_pick_worker_random_zero_treated_as_one() {
        // 0 workers should still return 0 (max(1) fallback)
        assert_eq!(pick_worker_random(0), 0);
    }

    #[test]
    fn test_pick_worker_random_distribution() {
        // With multiple workers, all should be picked at least once
        let n = 4;
        let mut seen = vec![false; n];
        for _ in 0..1000 {
            let idx = pick_worker_random(n);
            assert!(idx < n, "idx {} >= num_workers {}", idx, n);
            seen[idx] = true;
        }
        assert!(seen.iter().all(|&s| s), "not all workers were picked");
    }

    // ===== crash-death routing gates =====

    #[test]
    fn test_pick_worker_skip_ejected_skips_dead() {
        let outlier = OutlierState::new(2);
        outlier.mark_dead(0);
        for _ in 0..20 {
            assert_eq!(
                pick_worker_skip_ejected(2, &outlier),
                1,
                "a dead worker must never be picked while a live one exists"
            );
        }
    }

    #[test]
    fn test_pick_streaming_worker_all_dead_errs() {
        let outlier = OutlierState::new(2);
        outlier.mark_dead(0);
        outlier.mark_dead(1);
        let meta = pb::RequestMeta::default();
        let seq = SequenceRegistry::new(std::time::Duration::from_secs(60), 16);
        let err = pick_streaming_worker(&meta, 2, Some(&outlier), &seq, "m", "1")
            .expect_err("all workers dead must fail the pick");
        match err {
            PickError::NoLiveWorkers(msg) => assert!(
                msg.contains("exited"),
                "the pick error must name the cause: {msg}"
            ),
            other => panic!("expected NoLiveWorkers, got {other:?}"),
        }
    }

    #[test]
    fn test_pick_streaming_worker_pin_to_dead_errs() {
        let outlier = OutlierState::new(2);
        outlier.mark_dead(1);
        let mut meta = pb::RequestMeta::default();
        meta.headers.insert("x-lite-worker-id".to_string(), "1".to_string());
        let seq = SequenceRegistry::new(std::time::Duration::from_secs(60), 16);
        let err = pick_streaming_worker(&meta, 2, Some(&outlier), &seq, "m", "1")
            .expect_err("pinning a dead worker must fail the pick");
        match err {
            PickError::InvalidPin(msg) => assert!(msg.contains("x-lite-worker-id"), "got: {msg}"),
            other => panic!("expected InvalidPin, got {other:?}"),
        }
    }

    // ===== pick_worker_skip_ejected tests =====

    #[test]
    fn test_pick_worker_skip_ejected_all_active() {
        let outlier = OutlierState::new(4);
        // All active — should return valid index
        for _ in 0..10 {
            let idx = pick_worker_skip_ejected(4, &outlier);
            assert!(idx < 4, "index {} out of range", idx);
        }
    }

    #[test]
    fn test_pick_worker_skip_ejected_avoids_ejected() {
        let outlier = OutlierState::new(3);
        // Eject worker 0
        for _ in 0..3 {
            outlier.record_error(0);
        }
        assert!(outlier.is_ejected(0));

        // Should never pick ejected worker 0
        for _ in 0..100 {
            let idx = pick_worker_skip_ejected(3, &outlier);
            assert!(idx == 1 || idx == 2, "should skip ejected worker 0, got {}", idx);
        }
    }

    #[test]
    fn test_pick_worker_skip_ejected_single_worker() {
        let outlier = OutlierState::new(1);
        // Single worker always returns 0
        assert_eq!(pick_worker_skip_ejected(1, &outlier), 0);

        // Even if ejected
        outlier.record_error(0);
        assert_eq!(pick_worker_skip_ejected(1, &outlier), 0);
    }

    #[test]
    fn test_pick_worker_skip_ejected_all_ejected_fallback() {
        let outlier = OutlierState::new(2);
        // Eject worker 0 (max 50% of 2 = 1 ejection allowed)
        for _ in 0..3 {
            outlier.record_error(0);
        }
        // Worker 1 still active, should pick it
        for _ in 0..20 {
            assert_eq!(pick_worker_skip_ejected(2, &outlier), 1);
        }
    }

    // ===== route table tests =====

    #[tokio::test]
    async fn test_route_table_upsert_get_clear_and_reserved_skip() {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let wm = WorkerManager::new(
            registry,
            std::path::PathBuf::new(),
            inference_queue,
            "info".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        let routes = vec![
            RouteDecl { route: "/status".to_string(), methods: vec!["GET".to_string()] },
            // reserved leaf → skipped at ingest
            RouteDecl { route: "/infer".to_string(), methods: vec!["POST".to_string()] },
        ];
        wm.upsert_routes("m", "1", routes).await;

        let got = wm.get_routes("m", "1").await;
        assert_eq!(got.len(), 1, "reserved leaf must be skipped");
        assert_eq!(got[0].route, "/status");
        // version isolation
        assert!(wm.get_routes("m", "2").await.is_empty());

        // clear on unload removes them
        wm.clear_routes("m", "1").await;
        assert!(wm.get_routes("m", "1").await.is_empty());
    }

    // ===== Task F: pick_streaming_worker (B3 hint consumption) =====

    use std::collections::HashMap;
    use std::time::Duration;

    fn hint_meta(headers: &[(&str, &str)], sequence_id: Option<&str>) -> pb::RequestMeta {
        let mut h = HashMap::new();
        for (k, v) in headers {
            h.insert((*k).to_string(), (*v).to_string());
        }
        pb::RequestMeta {
            headers: h,
            sequence_id: sequence_id.map(|s| s.to_string()),
            ..Default::default()
        }
    }

    /// Drive `record_error` until `idx` is ejected (threshold-agnostic).
    fn force_eject(outlier: &OutlierState, idx: usize) {
        for _ in 0..1000 {
            outlier.record_error(idx);
            if outlier.is_ejected(idx) {
                return;
            }
        }
        panic!("worker {idx} did not eject within 1000 errors");
    }

    #[test]
    fn pick_respects_valid_direct_pin() {
        let outlier = OutlierState::new(2);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        let meta = hint_meta(&[("x-lite-worker-id", "1")], None);
        let w = pick_streaming_worker(&meta, 2, Some(&outlier), &reg, "m", "1").unwrap();
        assert_eq!(w, 1, "a valid in-range pin connects directly");
    }

    #[test]
    fn pick_rejects_direct_pin_out_of_range() {
        let outlier = OutlierState::new(2);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        let meta = hint_meta(&[("x-lite-worker-id", "5")], None);
        let err = pick_streaming_worker(&meta, 2, Some(&outlier), &reg, "m", "1").unwrap_err();
        match err {
            PickError::InvalidPin(msg) => assert!(msg.contains("out of range"), "expected out-of-range error, got: {msg}"),
            other => panic!("expected InvalidPin, got {other:?}"),
        }
    }

    #[test]
    fn pick_rejects_direct_pin_to_ejected_worker() {
        let outlier = OutlierState::new(2);
        force_eject(&outlier, 1);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        let meta = hint_meta(&[("x-lite-worker-id", "1")], None);
        let err = pick_streaming_worker(&meta, 2, Some(&outlier), &reg, "m", "1").unwrap_err();
        match err {
            PickError::InvalidPin(msg) => assert!(msg.contains("ejected"), "expected ejected error, got: {msg}"),
            other => panic!("expected InvalidPin, got {other:?}"),
        }
    }

    #[test]
    fn pick_affinity_key_is_deterministic() {
        let outlier = OutlierState::new(4);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        let a = hint_meta(&[("x-lite-affinity-key", "tenant-42")], None);
        let b = hint_meta(&[("x-lite-affinity-key", "tenant-42")], None);
        let wa = pick_streaming_worker(&a, 4, Some(&outlier), &reg, "m", "1").unwrap();
        let wb = pick_streaming_worker(&b, 4, Some(&outlier), &reg, "m", "1").unwrap();
        assert_eq!(wa, wb, "same affinity_key must route to the same worker");
    }

    #[test]
    fn pick_sequence_stickiness_wins_over_affinity_key() {
        let outlier = OutlierState::new(4);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        // Pre-record: sequence "seq-1" is sticky to worker 2.
        reg.record("seq-1", "m", "1", 2);
        // Request carries BOTH a sequence_id and an affinity_key: stickiness
        // (worker 2) must win over the affinity rendezvous hash.
        let meta = hint_meta(&[("x-lite-affinity-key", "tenant-42")], Some("seq-1"));
        let w = pick_streaming_worker(&meta, 4, Some(&outlier), &reg, "m", "1").unwrap();
        assert_eq!(w, 2, "sequence_id stickiness must win over affinity_key");
    }

    #[test]
    fn pick_records_sequence_stickiness_for_future_calls() {
        let outlier = OutlierState::new(2);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        // No prior mapping; a sequence_id request must record its chosen worker.
        let meta = hint_meta(&[], Some("seq-rec"));
        let w = pick_streaming_worker(&meta, 2, Some(&outlier), &reg, "m", "1").unwrap();
        assert_eq!(reg.lookup("seq-rec", "m", "1"), Some(w), "chosen worker must be recorded");
    }

    // ===== /audit P8-1 举证测试（2026-08-09）=====

    /// 数据维度：流式直连路径同样不得把 proto3 零值 `Some("")` 当真实 sequence。
    /// 当前 `record("", ...)` 会在注册表建空串条目，把后续所有显式置空的
    /// 客户端粘性归组到同一 worker（HTTP 侧空 x-sequence-id 被丢弃，无此问题）。
    #[test]
    fn test_audit_data_empty_sequence_id_not_recorded_by_streaming_pick() {
        let outlier = OutlierState::new(2);
        let reg = SequenceRegistry::new(Duration::from_secs(60), 16);
        let meta = hint_meta(&[], Some("")); // proto3 zero value, explicitly set
        let _ = pick_streaming_worker(&meta, 2, Some(&outlier), &reg, "m", "1").unwrap();
        assert!(
            reg.is_empty(),
            "empty sequence_id must be treated as absent — no registry entry may be created"
        );
    }
}
