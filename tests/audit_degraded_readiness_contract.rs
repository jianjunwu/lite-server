//! Defect pin (2026-10-16 bug hunt): the readiness surfaces and the
//! request-path gate must never disagree about whether a version serves.
//!
//! The original defect: `reconcile_version_status` (`src/worker/health.rs`)
//! flipped a version to `Degraded` as soon as ONE worker was ejected — the
//! ordinary outcome of the outlier breaker, whose default budget is 3
//! consecutive errors (`src/config.rs`, `ejection_error_threshold`), reconciled
//! every 15s. Four places documented `Degraded` as "still takes traffic"
//! (`VersionStatus::Degraded`'s own doc, `sync_grpc_health`, `has_serving`,
//! `active_version_is_ready`), while every request gate asked the strict
//! `ModelRegistry::is_ready`, which accepted only `Ready`. So `/readyz`, gRPC
//! Health and `/v2/models/{m}/health` reported the version as serving while
//! every inference to it returned 503. The documented self-heal — "a later real
//! request re-warms the slot lazily" — could not run either, because no request
//! could get past the gate to re-warm.
//!
//! The agreed resolution (user decision) was to SPLIT the state rather than
//! widen the gate:
//!   - `Impaired` (new) = some workers ejected, the rest still serving. What a
//!     partially-impaired version now becomes. Request gates accept it.
//!   - `Degraded` = NO worker left in rotation. Genuinely not serving; request
//!     gates reject it, and readiness must agree.
//!
//! These pins hold both halves of that contract, so neither state can drift
//! back into disagreeing with the readiness rollups.

use lite_server::config::ModelConfig;
use lite_server::inference_queue::{EjectionConfig, OutlierState};
use lite_server::registry::types::{ModelType, VersionStatus};
use lite_server::registry::ModelRegistry;
use std::path::PathBuf;

/// A version that went Ready, became the active version, and then reached
/// `status` — the state the coordinator derives after an outlier ejection.
fn active_version_in_state(model: &str, version: &str, status: VersionStatus) -> ModelRegistry {
    let registry = ModelRegistry::new();
    registry
        .register(
            model,
            version,
            ModelConfig::default(),
            ModelType::LitAPI,
            PathBuf::from(format!("/tmp/{model}/{version}")),
        )
        .expect("register must accept a fresh version");
    registry
        .mark_ready(model, version)
        .expect("mark_ready must succeed on a registered version");
    assert!(
        registry
            .activate_version(model, version)
            .expect("activate_version must not fail on a registered version"),
        "precondition: a Ready version can be activated"
    );
    registry
        .set_status(model, version, status)
        .expect("status must be settable on an activated version");
    registry
}

/// Both halves of the contract, checked together so the two axes cannot be
/// fixed independently and silently drift apart again: whatever
/// `server_status()` calls serving, the request gate must accept — and
/// whatever it does not, the request gate must reject.
#[test]
fn readiness_and_the_request_gate_agree_on_every_state() {
    for (status, serves) in [
        (VersionStatus::Ready, true),
        // Partially impaired: the surviving workers still take traffic.
        (VersionStatus::Impaired, true),
        // No worker left in rotation: nothing can answer.
        (VersionStatus::Degraded, false),
    ] {
        let registry = active_version_in_state("m", "1", status);

        let status_report = registry.server_status();
        let entry = status_report
            .entries
            .iter()
            .find(|e| e.name == "m" && e.version == "1")
            .expect("the registered version must appear in server_status");
        assert_eq!(
            entry.status, status,
            "precondition: the version is {status:?}"
        );

        assert_eq!(
            status_report.has_serving(),
            serves,
            "`has_serving` (readiness rollup, gRPC Health) must report {status:?} as serving={serves}"
        );
        assert_eq!(
            registry.active_version_is_ready("m"),
            serves,
            "`active_version_is_ready` must report {status:?} as serving={serves}"
        );
        assert_eq!(
            registry.is_ready("m", Some("1")),
            serves,
            "the request gate `is_ready` must agree with readiness for {status:?}: a model \
             answering 200 on /readyz while every inference 503s (or the reverse) is the \
             defect these pins exist to prevent"
        );
    }
}

/// The trigger, under default settings: one worker's consecutive error budget
/// (default threshold 3, `ejection_error_threshold`) ejects that worker and
/// leaves its siblings healthy — a partially-impaired version, which is why
/// the state above has to keep serving. `reconcile_version_status` derives
/// `Impaired` for exactly this shape.
#[test]
fn one_worker_hitting_the_default_error_budget_leaves_a_partially_impaired_version() {
    let outlier = OutlierState::with_config(2, &EjectionConfig::default());
    assert_eq!(
        EjectionConfig::default().error_threshold,
        3,
        "precondition: the default consecutive-error budget is the value pinned in config.rs"
    );

    for _ in 0..3 {
        let _ = outlier.record_error(0);
    }

    assert!(
        outlier.is_ejected(0),
        "three consecutive errors (the default budget) must eject the worker"
    );
    assert!(
        outlier.is_usable(1),
        "the sibling is untouched, so the version is partially impaired rather than \
         fully degraded — this is the shape that must keep serving"
    );
}
