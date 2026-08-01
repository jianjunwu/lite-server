//! Custom @route declarations per model version (upserted at worker
//! handshake, cleared on unload) and the random worker-picking helpers
//! shared by the gRPC/HTTP streaming paths.

use super::WorkerManager;
use crate::inference_queue::{model_version_key, OutlierState};
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

    // Try to find a non-ejected worker starting from random offset
    for i in 0..num_workers {
        let idx = (start + i) % num_workers;
        if !outlier.is_ejected(idx) {
            return idx;
        }
    }

    // All ejected — fall back to random
    start
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
}
