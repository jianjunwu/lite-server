//! M4 (admin-enhancement plan §3.5): vendor-neutral accelerator metrics.
//!
//! Model code reports device readings through the worker `Metrics` piggyback
//! channel (the same mechanism as `tokens_generated_total`); the core never
//! links a vendor SDK. Four fixed families with exactly two labels,
//! `device` + `accel`:
//!
//! - `lite_server_accelerator_utilization_percent`
//! - `lite_server_accelerator_memory_used_bytes`
//! - `lite_server_accelerator_memory_total_bytes`
//! - `lite_server_accelerator_temperature_celsius`
//!
//! §6.5 invariants:
//! - Label whitelist (#10): `device` + `accel` enter the whitelist via the
//!   2026-08-26 M4 ruling (`accel` is a bounded vendor tag — cuda/mlu/npu/…;
//!   `device` is a slot id). docs/observability.md is the review record.
//! - Cardinality (#11.1): label values are worker-controlled strings, so both
//!   the Prometheus vecs and the latest-value store are gated by
//!   MAX_ACCELERATOR_DEVICES — pairs beyond the cap are dropped before any
//!   series is created (one-shot warn, AGG-1 pattern).
//! - Purge (#11.1): the families carry no (model, version) labels, so
//!   remove_version_metrics cannot address them — a device may be shared by
//!   several versions, and per-version purge would be wrong. The cardinality
//!   cap is the bounding mechanism; staleness after unload is observable via
//!   `updated_at` on the JSON endpoint.
//! - Emission frequency (#12.2): a report is one gauge set per device per
//!   response that carries readings — bounded by the device cap, never per
//!   frame/chunk (the worker buffers latest-per-device between responses).

use lazy_static::lazy_static;
use prometheus::GaugeVec;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::proto::liteserver::AcceleratorReading;

/// §6.5 #11.1: hard cap on distinct (device, accel) pairs. Real hosts have
/// single-digit devices; 64 leaves headroom while bounding the series and
/// the store against arbitrary worker-reported strings.
pub(crate) const MAX_ACCELERATOR_DEVICES: usize = 64;

/// Label values are worker-controlled; bound their length so a pathological
/// model cannot mint huge series keys.
const MAX_LABEL_LEN: usize = 64;

lazy_static! {
    pub static ref ACCELERATOR_UTILIZATION_PERCENT: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "lite_server_accelerator_utilization_percent",
            "Accelerator compute utilization (0-100), reported by model code"
        ),
        &["device", "accel"]
    ).unwrap();
    pub static ref ACCELERATOR_MEMORY_USED_BYTES: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "lite_server_accelerator_memory_used_bytes",
            "Accelerator device memory in use (bytes), reported by model code"
        ),
        &["device", "accel"]
    ).unwrap();
    pub static ref ACCELERATOR_MEMORY_TOTAL_BYTES: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "lite_server_accelerator_memory_total_bytes",
            "Accelerator device memory capacity (bytes), reported by model code"
        ),
        &["device", "accel"]
    ).unwrap();
    pub static ref ACCELERATOR_TEMPERATURE_CELSIUS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "lite_server_accelerator_temperature_celsius",
            "Accelerator device temperature (Celsius), reported by model code"
        ),
        &["device", "accel"]
    ).unwrap();
}

/// Register the accelerator families with the given registry. Called from
/// `prometheus::register_metrics`; registration is not feature-gated (an
/// empty vec exports no series) — gating happens at record time.
pub(crate) fn register(registry: &prometheus::Registry) -> Result<(), prometheus::Error> {
    registry.register(Box::new(ACCELERATOR_UTILIZATION_PERCENT.clone()))?;
    registry.register(Box::new(ACCELERATOR_MEMORY_USED_BYTES.clone()))?;
    registry.register(Box::new(ACCELERATOR_MEMORY_TOTAL_BYTES.clone()))?;
    registry.register(Box::new(ACCELERATOR_TEMPERATURE_CELSIUS.clone()))?;
    Ok(())
}

/// Latest reading per (device, accel), served by GET /metrics/accelerator.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct AcceleratorSnapshot {
    pub device: String,
    pub accel: String,
    pub utilization_percent: Option<f64>,
    pub memory_used_bytes: Option<f64>,
    pub memory_total_bytes: Option<f64>,
    pub temperature_celsius: Option<f64>,
    /// Epoch seconds of the last report carrying this device.
    pub updated_at: f64,
}

#[derive(Default)]
struct Store {
    readings: HashMap<(String, String), AcceleratorSnapshot>,
    cap_warned: bool,
}

lazy_static! {
    static ref STORE: std::sync::RwLock<Store> = std::sync::RwLock::new(Store::default());
}

/// features.accelerator_metrics gate for the RECORD path (route mounting is
/// gated separately in routes.rs). Set once at server startup; the default
/// matches FeaturesConfig::default so tests recording without a server see
/// the production default.
static ENABLED: AtomicBool = AtomicBool::new(true);

pub fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
}

fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Record readings reported by a worker (Metrics.accelerator). No-op when
/// the feature is off. Readings with empty/oversized device or accel strings
/// are skipped; new (device, accel) pairs beyond MAX_ACCELERATOR_DEVICES are
/// dropped before any series is created.
pub fn record_readings(readings: &[AcceleratorReading]) {
    if readings.is_empty() || !enabled() {
        return;
    }
    let now = now_secs();
    let mut store = STORE.write().unwrap_or_else(|e| e.into_inner());
    for r in readings {
        if r.device.is_empty()
            || r.accel.is_empty()
            || r.device.len() > MAX_LABEL_LEN
            || r.accel.len() > MAX_LABEL_LEN
        {
            continue;
        }
        let key = (r.device.clone(), r.accel.clone());
        if !store.readings.contains_key(&key) && store.readings.len() >= MAX_ACCELERATOR_DEVICES {
            if !store.cap_warned {
                store.cap_warned = true;
                tracing::warn!(
                    cap = MAX_ACCELERATOR_DEVICES,
                    "accelerator device cap reached; dropping readings for new (device, accel) pairs"
                );
            }
            continue;
        }
        let labels: [&str; 2] = [&r.device, &r.accel];
        if let Some(v) = r.utilization_percent {
            ACCELERATOR_UTILIZATION_PERCENT
                .with_label_values(&labels)
                .set(v as f64);
        }
        if let Some(v) = r.memory_used_bytes {
            ACCELERATOR_MEMORY_USED_BYTES.with_label_values(&labels).set(v);
        }
        if let Some(v) = r.memory_total_bytes {
            ACCELERATOR_MEMORY_TOTAL_BYTES.with_label_values(&labels).set(v);
        }
        if let Some(v) = r.temperature_celsius {
            ACCELERATOR_TEMPERATURE_CELSIUS
                .with_label_values(&labels)
                .set(v as f64);
        }
        store.readings.insert(
            key,
            AcceleratorSnapshot {
                device: r.device.clone(),
                accel: r.accel.clone(),
                utilization_percent: r.utilization_percent.map(|v| v as f64),
                memory_used_bytes: r.memory_used_bytes,
                memory_total_bytes: r.memory_total_bytes,
                temperature_celsius: r.temperature_celsius.map(|v| v as f64),
                updated_at: now,
            },
        );
    }
}

/// Latest snapshot per (device, accel), sorted for a stable endpoint payload.
/// Empty when nothing has been reported (or the feature is off).
pub fn latest() -> Vec<AcceleratorSnapshot> {
    let store = STORE.read().unwrap_or_else(|e| e.into_inner());
    let mut out: Vec<_> = store.readings.values().cloned().collect();
    out.sort_by(|a, b| (&a.accel, &a.device).cmp(&(&b.accel, &b.device)));
    out
}

#[cfg(test)]
pub(crate) fn reset_for_tests() {
    let mut store = STORE.write().unwrap_or_else(|e| e.into_inner());
    for (device, accel) in store.readings.keys() {
        let labels: [&str; 2] = [device, accel];
        let _ = ACCELERATOR_UTILIZATION_PERCENT.remove_label_values(&labels);
        let _ = ACCELERATOR_MEMORY_USED_BYTES.remove_label_values(&labels);
        let _ = ACCELERATOR_MEMORY_TOTAL_BYTES.remove_label_values(&labels);
        let _ = ACCELERATOR_TEMPERATURE_CELSIUS.remove_label_values(&labels);
    }
    store.readings.clear();
    store.cap_warned = false;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn reading(device: &str, accel: &str) -> AcceleratorReading {
        AcceleratorReading {
            device: device.to_string(),
            accel: accel.to_string(),
            utilization_percent: Some(42.5),
            memory_used_bytes: Some(1.5e9),
            memory_total_bytes: Some(8.0e9),
            temperature_celsius: Some(65.0),
        }
    }

    #[test]
    #[serial(accelerator)]
    fn should_register_families_with_device_and_accel_labels_only() {
        crate::metrics::prometheus::register_metrics().ok();
        record_readings(&[reading("0", "cuda")]);
        let text = crate::metrics::prometheus::gather_metrics();
        for name in [
            "lite_server_accelerator_utilization_percent",
            "lite_server_accelerator_memory_used_bytes",
            "lite_server_accelerator_memory_total_bytes",
            "lite_server_accelerator_temperature_celsius",
        ] {
            let line = text
                .lines()
                .find(|l| l.starts_with(&format!("{}{{", name)))
                .unwrap_or_else(|| panic!("{name} series missing after a report"));
            assert!(
                line.contains("device=\"0\"") && line.contains("accel=\"cuda\""),
                "{name} must carry exactly the device/accel labels: {line}"
            );
            assert!(
                !line.contains("model=") && !line.contains("version="),
                "accelerator families must not grow model/version labels: {line}"
            );
        }
        reset_for_tests();
    }

    #[test]
    #[serial(accelerator)]
    fn should_store_latest_reading_per_device() {
        reset_for_tests();
        record_readings(&[reading("0", "cuda"), reading("1", "cuda")]);
        let mut newer = reading("0", "cuda");
        newer.utilization_percent = Some(90.0);
        record_readings(&[newer]);

        let snapshots = latest();
        assert_eq!(snapshots.len(), 2);
        let d0 = snapshots.iter().find(|s| s.device == "0").unwrap();
        assert_eq!(d0.utilization_percent, Some(90.0));
        assert!(d0.updated_at > 0.0);
        reset_for_tests();
    }

    #[test]
    #[serial(accelerator)]
    fn should_keep_unreported_fields_absent() {
        reset_for_tests();
        let mut r = reading("0", "npu");
        r.temperature_celsius = None;
        r.memory_total_bytes = None;
        record_readings(&[r]);

        let snapshots = latest();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].temperature_celsius, None);
        assert_eq!(snapshots[0].memory_total_bytes, None);
        assert_eq!(snapshots[0].utilization_percent, Some(42.5));
        reset_for_tests();
    }

    #[test]
    #[serial(accelerator)]
    fn should_skip_readings_with_empty_or_oversized_labels() {
        reset_for_tests();
        let mut empty_device = reading("", "cuda");
        empty_device.utilization_percent = Some(1.0);
        let mut empty_accel = reading("0", "");
        empty_accel.utilization_percent = Some(1.0);
        let mut huge_device = reading(&"x".repeat(MAX_LABEL_LEN + 1), "cuda");
        huge_device.utilization_percent = Some(1.0);
        record_readings(&[empty_device, empty_accel, huge_device]);
        assert!(latest().is_empty());
        reset_for_tests();
    }

    #[test]
    #[serial(accelerator)]
    fn should_drop_new_devices_beyond_the_cardinality_cap() {
        reset_for_tests();
        for i in 0..MAX_ACCELERATOR_DEVICES {
            record_readings(&[reading(&format!("dev-{i}"), "cuda")]);
        }
        assert_eq!(latest().len(), MAX_ACCELERATOR_DEVICES);
        // One more distinct pair must be dropped, not created.
        record_readings(&[reading("dev-overflow", "cuda")]);
        let snapshots = latest();
        assert_eq!(snapshots.len(), MAX_ACCELERATOR_DEVICES);
        assert!(!snapshots.iter().any(|s| s.device == "dev-overflow"));
        // Existing pairs still update under a full store.
        let mut update = reading("dev-0", "cuda");
        update.utilization_percent = Some(7.0);
        record_readings(&[update]);
        let d0 = latest().into_iter().find(|s| s.device == "dev-0").unwrap();
        assert_eq!(d0.utilization_percent, Some(7.0));
        reset_for_tests();
    }

    #[test]
    #[serial(accelerator)]
    fn should_not_record_when_feature_disabled() {
        reset_for_tests();
        set_enabled(false);
        record_readings(&[reading("0", "cuda")]);
        assert!(latest().is_empty());
        set_enabled(true);
        record_readings(&[reading("0", "cuda")]);
        assert_eq!(latest().len(), 1);
        reset_for_tests();
    }
}
