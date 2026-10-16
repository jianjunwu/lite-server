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
/// - service `""` — whole server: SERVING while any version is serving
/// - service `"{model}"` — SERVING when the model's active version is serving;
///   Loading/Failed/no-active → NOT_SERVING
///
/// "Serving" is exactly [`VersionStatus::is_serving`] — `Ready` or `Impaired`.
/// `Degraded` is NOT serving (no worker left in rotation), and the same
/// predicate gates the request paths, so a client can never get a 200 from
/// health and a 503 from inference. Services for models no longer in the
/// registry are cleared.
async fn sync_grpc_health(registry: &ModelRegistry, handle: &GrpcHealthHandle) {
    use tonic_health::ServingStatus;

    let status = registry.server_status();

    // P-WARM (§4.3): mirror per-version readiness to Prometheus BEFORE the
    // reporter check below. The gauge tracks the state machine, not gRPC: with
    // gRPC disabled (`grpc.enabled: false` / `--no-grpc`) there is no reporter,
    // so doing this inside the reporter scope left `liteserver_model_ready`
    // never written — an orchestrator or alert scraping it saw nothing while
    // /readyz and inference behaved normally. Same predicate as the gRPC status
    // and the request gate, so the three cannot disagree.
    for e in &status.entries {
        crate::metrics::prometheus::set_model_ready(&e.name, &e.version, e.status.is_serving());
    }

    let mut guard = handle.write().await;
    let Some(state) = guard.as_mut() else {
        return;
    };

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
        let version_serving = e.status.is_serving();
        let version_status = if version_serving {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        // The P-WARM readiness gauge is written above, outside the reporter
        // scope (gRPC health only carries a binary serving/not-serving per
        // service, and the gauge must exist even when gRPC does not).
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
            .map(|e2| e2.status.is_serving())
            .unwrap_or(false);
        let svc_status = if serving {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        state
            .reporter
            .set_service_status(e.name.clone(), svc_status)
            .await;
        current.insert(e.name.clone());
    }

    let stale: Vec<String> = state.known_models.difference(&current).cloned().collect();
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
    // Only the states this coordinator owns are reconciled; the load-phase
    // states (Pending/Loading/WarmingUp/Failed/Unloading) belong to the load
    // path, and Failed deliberately does not self-heal here.
    if !matches!(
        mv.status,
        VersionStatus::Ready | VersionStatus::Impaired | VersionStatus::Degraded
    ) {
        return None;
    }
    // An ensemble owns no workers of its own: its readiness is its
    // DEPENDENCIES'. Every step must be runnable for any request to succeed, so
    // "some sub-models down" is not a partially-serving state — the ensemble
    // refuses traffic, which is also what the executor does per step. Recording
    // the deps (at plan parse) is what makes this derivable; while they are
    // still UNKNOWN this returns no change, i.e. the load-time `Ready`, and the
    // executor's per-step gate remains the only protection — exactly the
    // previous behaviour, so nothing regresses on that path.
    if mv.model_type == crate::registry::types::ModelType::Ensemble {
        let deps = registry.ensemble_deps(model_name, version)?;
        let steps_runnable = deps.iter().all(|(dep_model, dep_version)| {
            match registry.get(dep_model, dep_version.as_deref()) {
                // Absent (or no active version): the executor auto-loads
                // sub-models on demand (`ensure_sub_model_loaded`), so this is
                // NOT a serving failure — treating it as one would gate out the
                // very request whose execution triggers the load.
                None => true,
                Some(dep) => match dep.status {
                    // Settled non-serving: this step cannot run, and no
                    // in-flight load is going to change that on its own.
                    VersionStatus::Degraded | VersionStatus::Failed => false,
                    // Serving, or still working its way there (a load is in
                    // flight) — not a settled failure.
                    _ => true,
                },
            }
        });
        let target = if steps_runnable {
            VersionStatus::Ready
        } else {
            VersionStatus::Degraded
        };
        return (target != mv.status).then_some(target);
    }

    let total = mv.workers.len();
    let healthy = match outlier {
        Some(o) => mv
            .workers
            .iter()
            .filter(|w| o.is_usable(w.worker_id as usize))
            .count(),
        None => total,
    };
    let target = if total == 0 {
        // A LitAPI version with no workers at all cannot serve. (An ensemble
        // has no workers by design and was handled above.)
        VersionStatus::Degraded
    } else if healthy == total {
        VersionStatus::Ready
    } else if healthy == 0 {
        // Nothing usable left in rotation. The outlier breaker cannot recover
        // this on its own, so the version genuinely stops serving.
        VersionStatus::Degraded
    } else {
        // Some workers ejected, the rest still taking traffic — the ORDINARY
        // outcome of the outlier breaker, so it must stay servable. Reporting
        // this as Degraded is what previously made one tripped worker 503 an
        // entire model while every readiness surface called it healthy.
        VersionStatus::Impaired
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
        if let Err(e) = self
            .registry
            .set_status(model_name, version, VersionStatus::Failed)
        {
            warn!(model = %model_name, version = %version, "failed to mark version failed: {}", e);
        }
        crate::metrics::prometheus::record_model_load(model_name, version, false);
        self.queue_grpc_health_sync();
    }

    /// Re-derive and apply this version's serving state right now from the
    /// current worker facts (ejection + death), instead of waiting for a
    /// coordinator tick. The coordinator may be disabled entirely
    /// (`health_check_interval == 0`), and an event-driven caller must not
    /// leave a stale state standing in the meantime.
    ///
    /// Uses the same derivation as the coordinator (`reconcile_version_status`),
    /// so a partially-impaired version lands on `Impaired` — still serving —
    /// and only a version with no usable worker left lands on `Degraded`.
    pub(super) async fn reconcile_status_now(&self, model_name: &str, version: &str) {
        let outlier = self.get_outlier_state(model_name, version).await;
        match reconcile_version_status(&self.registry, model_name, version, outlier.as_deref()) {
            // `mark_ready` rather than `set_status` so a genuine recovery keeps
            // the original load timestamp and clears any stale failure reason.
            Some(VersionStatus::Ready) => {
                if let Err(e) = self.registry.mark_ready(model_name, version) {
                    warn!(
                        model = %model_name, version = %version,
                        "failed to mark version ready: {}", e
                    );
                }
                self.queue_grpc_health_sync();
            }
            Some(other) => self.set_version_status(model_name, version, other),
            None => {}
        }
    }

    /// Apply an explicit serving state for a version and refresh gRPC health.
    ///
    /// Sync so the worker-respawn error paths (which cannot await, being
    /// `inspect_err` closures) can re-assert the state the caller derived
    /// before the spawn attempt. Callers are responsible for having derived
    /// `status` from the worker facts — do not hand-code a state here.
    pub(super) fn set_version_status(
        &self,
        model_name: &str,
        version: &str,
        status: VersionStatus,
    ) {
        if let Err(e) = self.registry.set_status(model_name, version, status) {
            warn!(
                model = %model_name, version = %version, status = ?status,
                "failed to set version status: {}", e
            );
        }
        self.queue_grpc_health_sync();
    }

    /// Start the status coordinator for a model version: every `interval`,
    /// reconcile Ready/Degraded from the outlier-ejection state. Called at
    /// load when `health_check_interval > 0`; an interval of 0 means no
    /// periodic coordination — status is then purely event-driven.
    pub(super) async fn start_status_coordinator(
        &self,
        model_name: &str,
        version: &str,
        interval: Duration,
    ) {
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
    pub async fn set_grpc_health_reporter(&self, reporter: tonic_health::server::HealthReporter) {
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
    ///
    /// Also cancels in-flight loads: this is the earliest shutdown signal
    /// (sent before the HTTP/gRPC drain window), so a worker stuck in setup()
    /// starts unwinding immediately instead of holding the load until
    /// startup_timeout.
    pub async fn mark_draining(&self) {
        self.shutdown_token.cancel();
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
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            None
        );
    }

    #[test]
    fn reconcile_ready_with_one_ejected_worker_becomes_impaired() {
        // A partially-impaired version is the ORDINARY outcome of the outlier
        // breaker (default threshold: 3 consecutive errors), so it must stay
        // servable — `Impaired`, not `Degraded`. `Degraded` now means "no
        // worker left"; emitting it here would take the whole model out of
        // service because a single slot tripped, while every readiness surface
        // still reported it healthy.
        let reg = ready_registry(2);
        let outlier = OutlierState::new(2);
        for _ in 0..3 {
            outlier.record_error(0);
        }
        assert!(outlier.is_ejected(0));
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            Some(VersionStatus::Impaired)
        );
    }

    #[test]
    fn reconcile_ready_with_every_worker_ejected_degrades() {
        // Nothing usable left: the circuit cannot recover this on its own, so
        // the version genuinely stops serving.
        let reg = ready_registry(2);
        let outlier = OutlierState::new(2);
        // `force_eject` because the default max-ejection-percent cap (50% of
        // 2 slots = 1) would refuse the second outlier-driven ejection.
        assert!(outlier.force_eject(0));
        assert!(outlier.force_eject(1));
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            Some(VersionStatus::Degraded)
        );
    }

    #[test]
    fn reconcile_ready_with_a_dead_worker_counts_it_unusable() {
        // The crash monitor marks a slot dead WITHOUT ejecting it (`mark_dead`
        // deliberately leaves the circuit alone so the health checker keeps
        // observing the slot), so counting ejections alone would report a
        // version as fully healthy while every request to it fails on the dead
        // worker. This is the readiness half of that defect.
        let reg = ready_registry(1);
        let outlier = OutlierState::new(1);
        outlier.mark_dead(0);
        assert!(!outlier.is_ejected(0), "test setup: death does not eject");
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            Some(VersionStatus::Degraded)
        );
    }

    #[test]
    fn reconcile_ready_with_recycling_worker_stays_ready() {
        // Rolling recycle claims the slot WITHOUT ejecting it (H1): the
        // version must not flap to Degraded for a planned, healthy-sibling
        // worker swap.
        let reg = ready_registry(2);
        let outlier = OutlierState::new(2);
        assert!(outlier.claim_recycle(0));
        assert!(!outlier.is_ejected(0), "test setup: recycle never ejects");
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            None
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
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            None
        );
    }

    #[test]
    fn reconcile_loading_is_event_driven_not_reconciled() {
        let reg = ready_registry(2);
        reg.set_status("m", "1", VersionStatus::Loading).unwrap();
        let outlier = OutlierState::new(2);
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            None
        );
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
        assert_eq!(
            reconcile_version_status(&reg, "m", "1", Some(&outlier)),
            None
        );
    }

    /// An ensemble owns no workers, so its readiness has to come from its
    /// DEPENDENCIES. The executor gates every step on the sub-model being
    /// strictly ready, so an ensemble that advertises `Ready` while a sub-model
    /// is down reports one thing to `/readyz` (and gRPC Health) and does another
    /// to every client — all requests 503.
    #[test]
    fn reconcile_ensemble_follows_its_sub_models() {
        let reg = ModelRegistry::new();
        reg.register(
            "ens",
            "1",
            crate::config::ModelConfig::default(),
            ModelType::Ensemble,
            std::env::temp_dir(),
        )
        .unwrap();
        reg.mark_ready("ens", "1").unwrap();
        reg.register(
            "sub",
            "1",
            crate::config::ModelConfig::default(),
            ModelType::LitAPI,
            std::env::temp_dir(),
        )
        .unwrap();
        reg.mark_ready("sub", "1").unwrap();
        reg.activate_version("sub", "1").unwrap();

        // Deps UNKNOWN (plan not parsed yet) → no change, so the load-time
        // Ready stands and the executor's per-step gate remains the protection.
        assert_eq!(reconcile_version_status(&reg, "ens", "1", None), None);

        // `None` version = resolved at execution time, i.e. the sub-model's
        // active version — how the executor resolves it too.
        reg.set_ensemble_deps("ens", "1", vec![("sub".to_string(), None)]);
        assert_eq!(
            reconcile_version_status(&reg, "ens", "1", None),
            None,
            "a dependency that is serving must leave the ensemble Ready"
        );

        // A dependency that is not loaded at all must NOT degrade the ensemble:
        // the executor auto-loads sub-models on demand, so gating the request
        // here would block the very request that triggers the load. Same for a
        // load in flight.
        reg.set_ensemble_deps(
            "ens",
            "1",
            vec![("never_loaded".to_string(), None)],
        );
        assert_eq!(
            reconcile_version_status(&reg, "ens", "1", None),
            None,
            "an unloaded sub-model is auto-loadable, not a serving failure"
        );
        reg.set_status("sub", "1", VersionStatus::WarmingUp).unwrap();
        reg.set_ensemble_deps("ens", "1", vec![("sub".to_string(), None)]);
        assert_eq!(
            reconcile_version_status(&reg, "ens", "1", None),
            None,
            "a sub-model that is still loading must not degrade the ensemble"
        );
        reg.mark_ready("sub", "1").unwrap();

        // The sub-model loses its last worker → the ensemble is not serving
        // either, and must say so. Apply the transition the coordinator would,
        // so the recovery below is a real round-trip rather than a no-op against
        // an unchanged status.
        reg.set_status("sub", "1", VersionStatus::Degraded).unwrap();
        let down = reconcile_version_status(&reg, "ens", "1", None);
        assert_eq!(
            down,
            Some(VersionStatus::Degraded),
            "an ensemble whose dependency cannot serve must not advertise itself as ready"
        );
        reg.set_status("ens", "1", down.unwrap()).unwrap();

        // ... and it recovers when the dependency does.
        reg.mark_ready("sub", "1").unwrap();
        assert_eq!(
            reconcile_version_status(&reg, "ens", "1", None),
            Some(VersionStatus::Ready)
        );
    }

    /// P-WARM: the per-version readiness gauge must be written even when gRPC
    /// health is disabled. It used to be set only INSIDE the reporter scope,
    /// which returns early when no reporter is installed (`grpc.enabled: false`
    /// / `--no-grpc`) — so `liteserver_model_ready` was never emitted at all,
    /// and an orchestrator or alert scraping it saw nothing while `/readyz` and
    /// inference behaved perfectly normally.
    #[tokio::test]
    async fn model_ready_gauge_is_written_without_a_grpc_reporter() {
        const MODEL: &str = "gauge-without-grpc";
        crate::metrics::prometheus::remove_model_ready(MODEL, "1");
        assert_eq!(
            crate::metrics::prometheus::MODEL_READY
                .with_label_values(&[MODEL, "1"])
                .get(),
            0.0,
            "precondition: the gauge starts clear, so a stale value cannot pass this test"
        );

        let reg = ModelRegistry::new();
        reg.register(
            MODEL,
            "1",
            crate::config::ModelConfig::default(),
            ModelType::LitAPI,
            std::env::temp_dir(),
        )
        .unwrap();
        reg.mark_ready(MODEL, "1").unwrap();

        // No reporter installed — the early return this test exists for.
        let handle: GrpcHealthHandle = Arc::new(tokio::sync::RwLock::new(None));
        sync_grpc_health(&reg, &handle).await;

        assert_eq!(
            crate::metrics::prometheus::MODEL_READY
                .with_label_values(&[MODEL, "1"])
                .get(),
            1.0,
            "the serving-state gauge must be written whether or not gRPC health is running"
        );
        crate::metrics::prometheus::remove_model_ready(MODEL, "1");
    }
}
