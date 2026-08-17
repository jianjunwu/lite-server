//! Version health: gRPC Health reporter sync, Ready/Degraded status
//! coordination from outlier-ejection state, and load-failure marking.

use super::WorkerManager;
use crate::inference_queue::{model_version_key, OutlierState};
use crate::registry::types::VersionStatus;
use crate::registry::ModelRegistry;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// gRPC Health reporter state: the tonic reporter plus the set of per-model
/// services currently published, so unloaded models can be cleared.
pub(super) struct GrpcHealthState {
    reporter: tonic_health::server::HealthReporter,
    known_models: std::collections::HashSet<String>,
    /// C3 (P4-2): once true, the overall "" service is forced NOT_SERVING so the
    /// gRPC LB health check摘流 during graceful shutdown.
    draining: bool,
}

/// Shared handle to the optional gRPC Health state (None until the gRPC
/// server installs its reporter).
pub(super) type GrpcHealthHandle = Arc<RwLock<Option<GrpcHealthState>>>;

/// Push registry health to the gRPC Health service (phase 3):
/// - service `""` — whole server: SERVING while any version is Ready/Degraded
/// - service `"{model}"` — SERVING when the model's active version is
///   Ready/Degraded; Loading/Failed/no-active → NOT_SERVING
///
/// Degraded counts as SERVING: it still takes traffic. Services for models
/// no longer in the registry are cleared.
async fn sync_grpc_health(registry: &ModelRegistry, handle: &GrpcHealthHandle) {
    use tonic_health::ServingStatus;

    let mut guard = handle.write().await;
    let Some(state) = guard.as_mut() else {
        return;
    };
    let status = registry.server_status();

    let overall = if status.has_serving() {
        ServingStatus::Serving
    } else {
        ServingStatus::NotServing
    };
    // C3 (P4-2): once draining, force the overall "" service to NOT_SERVING so
    // the gRPC LB health check摘流; per-model statuses still reflect reality.
    let overall = if state.draining {
        ServingStatus::NotServing
    } else {
        overall
    };
    state.reporter.set_service_status("", overall).await;

    let mut current: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_models: std::collections::HashSet<String> = std::collections::HashSet::new();
    for e in &status.entries {
        // Per-version service "{model}/{version}" (§4.5).
        let version_svc = format!("{}/{}", e.name, e.version);
        let version_serving = matches!(
            e.status,
            VersionStatus::Ready | VersionStatus::Degraded
        );
        let version_status = if version_serving {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        // P-WARM (§4.3): mirror the per-version readiness to Prometheus so a
        // WarmingUp/Failed version is visible to scrapers (gRPC health only
        // carries a binary serving/not-serving per service).
        crate::metrics::prometheus::set_model_ready(&e.name, &e.version, version_serving);
        state
            .reporter
            .set_service_status(version_svc.clone(), version_status)
            .await;
        current.insert(version_svc);

        // Per-model service "{model}": follows the active version.
        if !seen_models.insert(e.name.clone()) {
            continue; // already pushed this model
        }
        let serving = registry
            .get_active_version(&e.name)
            .and_then(|v| {
                status
                    .entries
                    .iter()
                    .find(|e2| e2.name == e.name && e2.version == v)
            })
            .map(|e2| matches!(e2.status, VersionStatus::Ready | VersionStatus::Degraded))
            .unwrap_or(false);
        let svc_status = if serving {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        state.reporter.set_service_status(e.name.clone(), svc_status).await;
        current.insert(e.name.clone());
    }

    let stale: Vec<String> = state
        .known_models
        .difference(&current)
        .cloned()
        .collect();
    for name in stale {
        state.reporter.clear_service_status(&name).await;
    }
    state.known_models = current;
}

/// Compute the reconciled status for a version from its outlier-ejection
/// state (status coordinator, phase 3).
///
/// Returns `Some(new_status)` only when the status should change. Versions in
/// Pending/Loading/Failed/Unloading are event-driven and return `None` —
/// Failed in particular is load-phase only and must not be rewritten at
/// runtime (outlier ejection auto-recovers, so runtime impairment is always
/// Degraded). A missing outlier state means "no ejection info" and counts
/// every worker as healthy.
fn reconcile_version_status(
    registry: &ModelRegistry,
    model_name: &str,
    version: &str,
    outlier: Option<&OutlierState>,
) -> Option<VersionStatus> {
    let mv = registry.get(model_name, Some(version))?;
    match mv.status {
        VersionStatus::Ready | VersionStatus::Degraded => {}
        _ => return None,
    }
    let total = mv.workers.len();
    let healthy = match outlier {
        Some(o) => mv
            .workers
            .iter()
            .filter(|w| !o.is_ejected(w.worker_id as usize))
            .count(),
        None => total,
    };
    let target = if total > 0 && healthy == total {
        VersionStatus::Ready
    } else {
        VersionStatus::Degraded
    };
    (target != mv.status).then_some(target)
}

impl WorkerManager {
    /// Get outlier detection state for a model version (used for streaming worker selection).
    pub async fn get_outlier_state(
        &self,
        model_name: &str,
        version: &str,
    ) -> Option<Arc<OutlierState>> {
        let key = model_version_key(model_name, version);
        let guard = self.outlier_states.read().await;
        guard.get(&key).cloned()
    }

    /// Mark a version Failed during load (startup crash / timeout). Failed is
    /// load-phase only; runtime impairment is Degraded via the coordinator.
    /// Also counts the load failure — spawn/handshake failures otherwise
    /// never reach MODEL_LOAD_TOTAL (only the warmup path recorded it), so
    /// the load failure rate was silently underestimated.
    pub(super) fn mark_load_failed(&self, model_name: &str, version: &str) {
        if let Err(e) = self.registry.set_status(model_name, version, VersionStatus::Failed) {
            warn!(model = %model_name, version = %version, "failed to mark version failed: {}", e);
        }
        crate::metrics::prometheus::record_model_load(model_name, version, false);
        self.queue_grpc_health_sync();
    }

    /// Mark a version Degraded after a runtime worker loss (e.g. respawn
    /// failure). The coordinator takes it from there; outlier recovery can
    /// bring it back to Ready.
    pub(super) fn mark_degraded(&self, model_name: &str, version: &str) {
        if let Err(e) = self.registry.set_status(model_name, version, VersionStatus::Degraded) {
            warn!(model = %model_name, version = %version, "failed to mark version degraded: {}", e);
        }
        self.queue_grpc_health_sync();
    }

    /// Start the status coordinator for a model version: every `interval`,
    /// reconcile Ready/Degraded from the outlier-ejection state. Called at
    /// load when `health_check_interval > 0`; an interval of 0 means no
    /// periodic coordination — status is then purely event-driven.
    pub(super) async fn start_status_coordinator(&self, model_name: &str, version: &str, interval: Duration) {
        let key = model_version_key(model_name, version);
        let task_key = key.clone();
        let registry = self.registry.clone();
        let outlier_states = self.outlier_states.clone();
        let grpc_health = self.grpc_health.clone();
        let model = model_name.to_string();
        let ver = version.to_string();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let outlier = {
                    let guard = outlier_states.read().await;
                    guard.get(&task_key).cloned()
                };
                if let Some(new_status) =
                    reconcile_version_status(&registry, &model, &ver, outlier.as_deref())
                {
                    info!(
                        model = %model, version = %ver, status = ?new_status,
                        "status coordinator: version status changed"
                    );
                    if let Err(e) = registry.set_status(&model, &ver, new_status) {
                        // Version unloaded mid-tick; the task is aborted on
                        // unload, so losing this race is expected and benign.
                        tracing::debug!(
                            model = %model, version = %ver,
                            "status coordinator: set_status failed: {}", e
                        );
                    }
                }
                // Keep gRPC Health fresh even when no transition happened —
                // the overall "" service depends on every version.
                sync_grpc_health(&registry, &grpc_health).await;
            }
        });
        self.status_coordinators.write().await.insert(key, handle);
    }

    /// Stop the status coordinator for a model version (on unload).
    pub(super) async fn stop_status_coordinator(&self, model_name: &str, version: &str) {
        let key = model_version_key(model_name, version);
        if let Some(handle) = self.status_coordinators.write().await.remove(&key) {
            handle.abort();
        }
    }

    /// Install the gRPC Health reporter (called once by start_grpc_server).
    pub async fn set_grpc_health_reporter(
        &self,
        reporter: tonic_health::server::HealthReporter,
    ) {
        *self.grpc_health.write().await = Some(GrpcHealthState {
            reporter,
            known_models: std::collections::HashSet::new(),
            draining: false,
        });
    }

    /// C3 (P4-2): mark the server as draining — subsequent gRPC Health syncs
    /// report the overall "" service as NOT_SERVING (gRPC LB摘流), and an
    /// immediate sync pushes it right away rather than waiting for the next
    /// coordinator tick. No-op before the reporter is installed.
    pub async fn mark_draining(&self) {
        if let Some(state) = self.grpc_health.write().await.as_mut() {
            state.draining = true;
        }
        self.sync_grpc_health().await;
    }

    /// Push the current registry health to the gRPC Health service. No-op
    /// until the reporter is installed.
    pub async fn sync_grpc_health(&self) {
        sync_grpc_health(&self.registry, &self.grpc_health).await;
    }

    /// Fire-and-forget variant for sync contexts (error paths that can't
    /// await, e.g. `inspect_err` closures).
    fn queue_grpc_health_sync(&self) {
        let registry = self.registry.clone();
        let handle = self.grpc_health.clone();
        tokio::spawn(async move {
            sync_grpc_health(&registry, &handle).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::types::{ModelType, WorkerInfo, WorkerStatus};

    // ===== status coordinator (reconcile_version_status) =====

    /// Registry with one version in Ready and `workers` ready workers.
    fn ready_registry(workers: u32) -> ModelRegistry {
        let reg = ModelRegistry::new();
        reg.register(
            "m",
            "1",
            crate::config::ModelConfig::default(),
            ModelType::LitAPI,
            std::env::temp_dir(),
        )
        .unwrap();
        reg.set_workers(
            "m",
            "1",
            (0..workers)
                .map(|i| WorkerInfo {
                    worker_id: i,
                    device: "cpu:0".to_string(),
                    endpoint: format!("ipc:///tmp/reconcile-test-{}.sock", i),
                    pid: None,
                    status: WorkerStatus::Ready,
                    capacity: None,
                })
                .collect(),
        )
        .unwrap();
        reg.mark_ready("m", "1").unwrap();
        reg
    }

    #[test]
    fn reconcile_ready_all_healthy_no_change() {
        let reg = ready_registry(2);
        let outlier = OutlierState::new(2);
        assert_eq!(reconcile_version_status(&reg, "m", "1", Some(&outlier)), None);
    }

    #[test]
    fn reconcile_ready_with_ejected_worker_degrades() {
        let reg = ready_registry(2);
        let outlier = OutlierState::new(2);
        for _ in 0..3 {
            outlier.record_error(0);
        }
        assert!(outlier.is_ejected(0));
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            Some(VersionStatus::Degraded)
        );
    }

    #[test]
    fn reconcile_degraded_recovers_to_ready() {
        let reg = ready_registry(2);
        reg.set_status("m", "1", VersionStatus::Degraded).unwrap();
        let outlier = OutlierState::new(2);
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            Some(VersionStatus::Ready)
        );
    }

    #[test]
    fn reconcile_failed_is_not_rewritten_at_runtime() {
        let reg = ready_registry(2);
        reg.set_status("m", "1", VersionStatus::Failed).unwrap();
        let outlier = OutlierState::new(2);
        for _ in 0..3 {
            outlier.record_error(0);
        }
        assert_eq!(reconcile_version_status(&reg, "m", "1", Some(&outlier)), None);
    }

    #[test]
    fn reconcile_loading_is_event_driven_not_reconciled() {
        let reg = ready_registry(2);
        reg.set_status("m", "1", VersionStatus::Loading).unwrap();
        let outlier = OutlierState::new(2);
        assert_eq!(reconcile_version_status(&reg, "m", "1", Some(&outlier)), None);
    }

    #[test]
    fn reconcile_without_outlier_state_counts_all_healthy() {
        let reg = ready_registry(2);
        assert_eq!(reconcile_version_status(&reg, "m", "1", None), None);
    }

    #[test]
    fn reconcile_ready_with_zero_workers_degrades() {
        let reg = ready_registry(0);
        let outlier = OutlierState::new(0);
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            Some(VersionStatus::Degraded)
        );
    }

    #[test]
    fn reconcile_unloaded_version_no_change() {
        let reg = ModelRegistry::new();
        let outlier = OutlierState::new(1);
        assert_eq!(reconcile_version_status(&reg, "m", "1", Some(&outlier)), None);
    }
}
