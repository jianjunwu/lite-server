use lazy_static::lazy_static;
use prometheus::{CounterVec, GaugeVec};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

// Max data points per model timeline (ring buffer capacity)
const MAX_TIMELINE_POINTS: usize = 30;
const SAMPLE_INTERVAL_SECS: f64 = 10.0;

// ---------------------------------------------------------------------------
// Timeline
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub struct TimelineEntry {
    pub timestamp: f64,
    pub qps: f64,
    pub p99_ms: f64,
    pub queue_depth: i64,
    pub active_workers: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TimelineSnapshot {
    pub model: String,
    pub version: String,
    pub entries: Vec<TimelineEntry>,
}

pub struct TimelineAggregator {
    /// model_version key -> ring buffer of entries
    data: Mutex<HashMap<String, VecDeque<TimelineEntry>>>,
    /// Last sample timestamp per key
    last_sample: Mutex<HashMap<String, f64>>,
    /// Latency samples per key (sliding window, seconds)
    latency_samples: Mutex<HashMap<String, VecDeque<f64>>>,
    /// Last request count per key (for QPS delta)
    last_counts: Mutex<HashMap<String, f64>>,
    /// Last check timestamp
    last_check: Mutex<f64>,
}

impl TimelineAggregator {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            last_sample: Mutex::new(HashMap::new()),
            latency_samples: Mutex::new(HashMap::new()),
            last_counts: Mutex::new(HashMap::new()),
            last_check: Mutex::new(now_secs()),
        }
    }

    /// Record a latency sample from request handling.
    pub fn record_latency(&self, model: &str, version: &str, duration_secs: f64) {
        let key = format!("{}_{}", model, version);
        let mut samples = self.latency_samples.lock().unwrap();
        let deque = samples.entry(key).or_insert_with(|| VecDeque::with_capacity(1000));
        deque.push_back(duration_secs);
        // Keep last 1000 samples (~1-2 minutes at high throughput)
        while deque.len() > 1000 {
            deque.pop_front();
        }
    }

    /// Sample current metrics into the timeline. Call periodically (e.g. every 10s).
    pub fn sample(&self, model: &str, version: &str) {
        let key = format!("{}_{}", model, version);
        let now = now_secs();

        // Throttle to SAMPLE_INTERVAL_SECS
        {
            let last_map = self.last_sample.lock().unwrap();
            if let Some(ts) = last_map.get(&key) {
                if now - ts < SAMPLE_INTERVAL_SECS {
                    return;
                }
            }
        }

        // Compute QPS from request count delta
        let qps = self.compute_qps(&key, model, version, now);

        // Compute p99 from latency samples
        let p99_ms = self.compute_p99_ms(&key);

        // Read queue depth and active workers from Prometheus gauges
        let queue_depth = read_gauge(&super::prometheus::QUEUE_DEPTH, &[model, version]);
        let active_workers = read_gauge(&super::prometheus::ACTIVE_WORKERS, &[model, version]);

        let entry = TimelineEntry {
            timestamp: now,
            qps,
            p99_ms,
            queue_depth: queue_depth as i64,
            active_workers: active_workers as i64,
        };

        {
            let mut data = self.data.lock().unwrap();
            let deque = data.entry(key.clone()).or_insert_with(|| {
                VecDeque::with_capacity(MAX_TIMELINE_POINTS)
            });
            if deque.len() >= MAX_TIMELINE_POINTS {
                deque.pop_front();
            }
            deque.push_back(entry);
        }

        {
            let mut last_map = self.last_sample.lock().unwrap();
            last_map.insert(key, now);
        }
    }

    /// Get timeline entries for a specific model version.
    pub fn get_timeline(&self, model: &str, version: &str) -> Vec<TimelineEntry> {
        let key = format!("{}_{}", model, version);
        let data = self.data.lock().unwrap();
        data.get(&key).cloned().unwrap_or_default().into_iter().collect()
    }

    /// Get all known model_version keys.
    pub fn keys(&self) -> Vec<String> {
        let data = self.data.lock().unwrap();
        data.keys().cloned().collect()
    }

    /// Get latest snapshot for every known key.
    pub fn all_snapshots(&self) -> Vec<TimelineSnapshot> {
        let data = self.data.lock().unwrap();
        data.iter()
            .filter_map(|(key, entries)| {
                let parts: Vec<&str> = key.splitn(2, '_').collect();
                if parts.len() != 2 {
                    return None;
                }
                Some(TimelineSnapshot {
                    model: parts[0].to_string(),
                    version: parts[1].to_string(),
                    entries: entries.iter().cloned().collect(),
                })
            })
            .collect()
    }

    fn compute_qps(&self, key: &str, model: &str, version: &str, now: f64) -> f64 {
        // Sum all success + error request counts
        let current_count = read_counter(&super::prometheus::REQUESTS_TOTAL, &[model, version, "2xx"])
            + read_counter(&super::prometheus::REQUESTS_TOTAL, &[model, version, "5xx"]);

        let mut last_counts = self.last_counts.lock().unwrap();
        let last_count = last_counts.get(key).copied().unwrap_or(current_count);
        let elapsed = {
            let mut last_check = self.last_check.lock().unwrap();
            let dt = now - *last_check;
            *last_check = now;
            dt.max(0.001)
        };

        let qps = (current_count - last_count).max(0.0) / elapsed;
        last_counts.insert(key.to_string(), current_count);
        round(qps, 2)
    }

    fn compute_p99_ms(&self, key: &str) -> f64 {
        let samples = self.latency_samples.lock().unwrap();
        let deque = match samples.get(key) {
            Some(d) if d.len() >= 2 => d,
            _ => return 0.0,
        };
        let mut sorted: Vec<f64> = deque.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = sorted.len();
        let p99 = sorted[((n as f64 * 0.99) as usize).min(n - 1)];
        round(p99 * 1000.0, 1)
    }
}

lazy_static! {
    pub static ref TIMELINE: TimelineAggregator = TimelineAggregator::new();
}

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub struct Alert {
    pub model: String,
    pub version: String,
    pub rule: String,
    pub message: String,
    pub severity: String, // warning | critical
    pub timestamp: f64,
    pub value: f64,
    pub threshold: f64,
}

pub struct AlertEngine {
    thresholds: AlertThresholds,
}

#[derive(Clone, Copy)]
pub struct AlertThresholds {
    pub queue_depth_warning: i64,
    pub queue_depth_critical: i64,
    pub p99_ms_warning: f64,
    pub p99_ms_critical: f64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            queue_depth_warning: 100,
            queue_depth_critical: 500,
            p99_ms_warning: 500.0,
            p99_ms_critical: 2000.0,
        }
    }
}

impl AlertEngine {
    pub fn new(thresholds: AlertThresholds) -> Self {
        Self { thresholds }
    }

    pub fn evaluate(&self, timeline: &TimelineAggregator) -> Vec<Alert> {
        let mut alerts = Vec::new();
        let now = now_secs();

        for snapshot in timeline.all_snapshots() {
            if let Some(latest) = snapshot.entries.last() {
                // Queue depth alerts
                if latest.queue_depth >= self.thresholds.queue_depth_critical {
                    alerts.push(Alert {
                        model: snapshot.model.clone(),
                        version: snapshot.version.clone(),
                        rule: "queue_depth".to_string(),
                        message: format!(
                            "Queue depth {} exceeds critical threshold {}",
                            latest.queue_depth, self.thresholds.queue_depth_critical
                        ),
                        severity: "critical".to_string(),
                        timestamp: now,
                        value: latest.queue_depth as f64,
                        threshold: self.thresholds.queue_depth_critical as f64,
                    });
                } else if latest.queue_depth >= self.thresholds.queue_depth_warning {
                    alerts.push(Alert {
                        model: snapshot.model.clone(),
                        version: snapshot.version.clone(),
                        rule: "queue_depth".to_string(),
                        message: format!(
                            "Queue depth {} exceeds warning threshold {}",
                            latest.queue_depth, self.thresholds.queue_depth_warning
                        ),
                        severity: "warning".to_string(),
                        timestamp: now,
                        value: latest.queue_depth as f64,
                        threshold: self.thresholds.queue_depth_warning as f64,
                    });
                }

                // P99 latency alerts
                if latest.p99_ms >= self.thresholds.p99_ms_critical {
                    alerts.push(Alert {
                        model: snapshot.model.clone(),
                        version: snapshot.version.clone(),
                        rule: "p99_latency".to_string(),
                        message: format!(
                            "P99 latency {:.1}ms exceeds critical threshold {:.1}ms",
                            latest.p99_ms, self.thresholds.p99_ms_critical
                        ),
                        severity: "critical".to_string(),
                        timestamp: now,
                        value: latest.p99_ms,
                        threshold: self.thresholds.p99_ms_critical,
                    });
                } else if latest.p99_ms >= self.thresholds.p99_ms_warning {
                    alerts.push(Alert {
                        model: snapshot.model.clone(),
                        version: snapshot.version.clone(),
                        rule: "p99_latency".to_string(),
                        message: format!(
                            "P99 latency {:.1}ms exceeds warning threshold {:.1}ms",
                            latest.p99_ms, self.thresholds.p99_ms_warning
                        ),
                        severity: "warning".to_string(),
                        timestamp: now,
                        value: latest.p99_ms,
                        threshold: self.thresholds.p99_ms_warning,
                    });
                }
            }
        }

        alerts
    }
}

// ---------------------------------------------------------------------------
// Version Compare
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub struct VersionComparison {
    pub model: String,
    pub versions: Vec<VersionMetrics>,
}

#[derive(Clone, Debug, Serialize)]
pub struct VersionMetrics {
    pub version: String,
    pub avg_qps: f64,
    pub avg_p99_ms: f64,
    pub avg_queue_depth: f64,
    pub avg_active_workers: f64,
    pub sample_count: usize,
}

pub struct VersionComparator;

impl VersionComparator {
    pub fn compare(timeline: &TimelineAggregator, model: &str) -> Option<VersionComparison> {
        let _prefix = format!("{}_", model);
        let data = timeline.all_snapshots();
        let mut versions: Vec<VersionMetrics> = Vec::new();

        for snap in data {
            if !snap.model.eq(model) {
                continue;
            }
            if snap.entries.is_empty() {
                continue;
            }
            let n = snap.entries.len() as f64;
            let avg_qps = snap.entries.iter().map(|e| e.qps).sum::<f64>() / n;
            let avg_p99 = snap.entries.iter().map(|e| e.p99_ms).sum::<f64>() / n;
            let avg_queue = snap.entries.iter().map(|e| e.queue_depth as f64).sum::<f64>() / n;
            let avg_workers = snap.entries.iter().map(|e| e.active_workers as f64).sum::<f64>() / n;
            versions.push(VersionMetrics {
                version: snap.version,
                avg_qps: round(avg_qps, 2),
                avg_p99_ms: round(avg_p99, 1),
                avg_queue_depth: round(avg_queue, 1),
                avg_active_workers: round(avg_workers, 1),
                sample_count: snap.entries.len(),
            });
        }

        if versions.is_empty() {
            return None;
        }

        Some(VersionComparison {
            model: model.to_string(),
            versions,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn round(v: f64, decimals: i32) -> f64 {
    let multiplier = 10f64.powi(decimals);
    (v * multiplier).round() / multiplier
}

fn read_counter(counter: &CounterVec, labels: &[&str]) -> f64 {
    counter.with_label_values(labels).get()
}

fn read_gauge(gauge: &GaugeVec, labels: &[&str]) -> f64 {
    gauge.with_label_values(labels).get()
}
