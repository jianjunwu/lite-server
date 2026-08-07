use dashmap::DashMap;
use lazy_static::lazy_static;
use prometheus::{CounterVec, GaugeVec};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

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
    /// G6:活跃流式连接(STREAMING_CONNECTIONS 跨 protocol 求和;
    /// streaming_metrics 关时 gauge 恒 0,随之 0——继承既有门控语义)。
    pub active_streams: i64,
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
    /// Latency samples per key (sliding window, seconds) — DashMap shards eliminate cross-key contention.
    latency_samples: DashMap<String, std::sync::Mutex<VecDeque<f64>>>,
    /// Last request count per key (for QPS delta)
    last_counts: Mutex<HashMap<String, f64>>,
    /// Last check timestamp per key (for QPS delta)
    last_check: Mutex<HashMap<String, f64>>,
}

impl Default for TimelineAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl TimelineAggregator {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            last_sample: Mutex::new(HashMap::new()),
            latency_samples: DashMap::new(),
            last_counts: Mutex::new(HashMap::new()),
            last_check: Mutex::new(HashMap::new()),
        }
    }

    /// Record a latency sample from request handling. Lock-free across keys; per-key mutex is held briefly.
    pub fn record_latency(&self, model: &str, version: &str, duration_secs: f64) {
        let key = format!("{}_{}", model, version);
        let entry = self.latency_samples.entry(key).or_insert_with(|| {
            std::sync::Mutex::new(VecDeque::with_capacity(1000))
        });
        let mut deque = entry.value().lock().unwrap();
        deque.push_back(duration_secs);
        // Keep last 1000 samples (~1-2 minutes at high throughput)
        while deque.len() > 1000 {
            deque.pop_front();
        }
    }

    /// Sample current metrics into the timeline. Call periodically (e.g. every 10s).
    pub async fn sample(&self, model: &str, version: &str) {
        let key = format!("{}_{}", model, version);
        let now = now_secs();

        // Throttle to SAMPLE_INTERVAL_SECS
        {
            let last_map = self.last_sample.lock().await;
            if let Some(ts) = last_map.get(&key) {
                if now - ts < SAMPLE_INTERVAL_SECS {
                    return;
                }
            }
        }

        // Compute QPS from request count delta
        let qps = self.compute_qps(&key, model, version, now).await;

        // Compute p99 from latency samples
        let p99_ms = self.compute_p99_ms(&key);

        // Read queue depth and active workers from Prometheus gauges
        let queue_depth = read_gauge(&super::prometheus::QUEUE_DEPTH, &[model, version]);
        let active_workers = read_gauge(&super::prometheus::ACTIVE_WORKERS, &[model, version]);

        // G6:活跃流式连接——跨 protocol 求和(read_gauge 传固定 label 会因
        // 基数不符 panic,须遍历子序列过滤)。
        let active_streams = read_active_streams(model, version);

        let entry = TimelineEntry {
            timestamp: now,
            qps,
            p99_ms,
            queue_depth: queue_depth as i64,
            active_workers: active_workers as i64,
            active_streams,
        };

        {
            let mut data = self.data.lock().await;
            let deque = data.entry(key.clone()).or_insert_with(|| {
                VecDeque::with_capacity(MAX_TIMELINE_POINTS)
            });
            if deque.len() >= MAX_TIMELINE_POINTS {
                deque.pop_front();
            }
            deque.push_back(entry);
        }

        {
            let mut last_map = self.last_sample.lock().await;
            last_map.insert(key, now);
        }
    }

    /// Get timeline entries for a specific model version.
    pub async fn get_timeline(&self, model: &str, version: &str) -> Vec<TimelineEntry> {
        let key = format!("{}_{}", model, version);
        let data = self.data.lock().await;
        data.get(&key).cloned().unwrap_or_default().into_iter().collect()
    }

    /// Get all known model_version keys.
    pub async fn keys(&self) -> Vec<String> {
        let data = self.data.lock().await;
        data.keys().cloned().collect()
    }

    /// Get latest snapshot for every known key.
    pub async fn all_snapshots(&self) -> Vec<TimelineSnapshot> {
        let data = self.data.lock().await;
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

    async fn compute_qps(&self, key: &str, model: &str, version: &str, now: f64) -> f64 {
        // Sum request counts across all status families — status_family()
        // (handlers.rs) labels 2xx/3xx/4xx/5xx, and 3xx/4xx requests are real
        // throughput that must count toward QPS.
        let current_count = read_counter(&super::prometheus::REQUESTS_TOTAL, &[model, version, "2xx"])
            + read_counter(&super::prometheus::REQUESTS_TOTAL, &[model, version, "3xx"])
            + read_counter(&super::prometheus::REQUESTS_TOTAL, &[model, version, "4xx"])
            + read_counter(&super::prometheus::REQUESTS_TOTAL, &[model, version, "5xx"]);

        let mut last_counts = self.last_counts.lock().await;
        let last_count = last_counts.get(key).copied().unwrap_or(current_count);
        let elapsed = {
            let mut last_check = self.last_check.lock().await;
            let dt = now - last_check.get(key).copied().unwrap_or(now);
            last_check.insert(key.to_string(), now);
            dt.max(0.001)
        };

        let qps = (current_count - last_count).max(0.0) / elapsed;
        last_counts.insert(key.to_string(), current_count);
        round(qps, 2)
    }

    fn compute_p99_ms(&self, key: &str) -> f64 {
        let entry = match self.latency_samples.get(key) {
            Some(e) => e,
            None => return 0.0,
        };
        let deque = match entry.value().lock() {
            Ok(g) => g,
            Err(_) => return 0.0,
        };
        if deque.len() < 2 {
            return 0.0;
        }
        let mut samples: Vec<f64> = deque.iter().copied().filter(|v| !v.is_nan()).collect();
        if samples.len() < 2 {
            return 0.0;
        }
        let n = samples.len();
        let p99_idx = ((n as f64 * 0.99) as usize).min(n - 1);
        let (_, p99, _) = samples.select_nth_unstable_by(p99_idx, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });
        round(*p99 * 1000.0, 1)
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

    pub async fn evaluate(&self, timeline: &TimelineAggregator) -> Vec<Alert> {
        let mut alerts = Vec::new();
        let now = now_secs();

        for snapshot in timeline.all_snapshots().await {
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
    pub async fn compare(timeline: &TimelineAggregator, model: &str) -> Option<VersionComparison> {
        let _prefix = format!("{}_", model);
        let data = timeline.all_snapshots().await;
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

/// G6:STREAMING_CONNECTIONS 跨 protocol 求和(按 model/version 过滤)。
/// 不硬编码 protocol 值——耐未来新增(评审更正:read_gauge 的
/// with_label_values 要求 3 个 label,传 2 个会基数不符 panic)。
fn read_active_streams(model: &str, version: &str) -> i64 {
    let families = super::prometheus::REGISTRY.gather();
    let Some(family) = families
        .iter()
        .find(|mf| mf.get_name() == "liteserver_streaming_connections")
    else {
        return 0;
    };
    let mut total = 0.0;
    for m in family.get_metric() {
        let labels_ok = m.get_label().iter().any(|l| l.get_name() == "model" && l.get_value() == model)
            && m.get_label().iter().any(|l| l.get_name() == "version" && l.get_value() == version);
        if labels_ok {
            total += m.get_gauge().get_value();
        }
    }
    total as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// G6 (批次 4):timeline sample 后 entry.active_streams 反映 STREAMING_CONNECTIONS
    /// gauge 跨 protocol 求和(不硬编码 protocol 值)。
    #[tokio::test]
    async fn sample_records_active_streams() {
        use crate::metrics::prometheus;
        // read_active_streams 走 REGISTRY.gather——先注册(AlreadyReg 忽略)。
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        // 3 个 protocol 各 1 个连接。
        prometheus::record_stream_open("asm", "1", "sse", "test-s", false);
        prometheus::record_stream_open("asm", "1", "websocket", "test-w", false);
        prometheus::record_stream_open("asm", "1", "http2", "test-h", false);
        // 第一次 sample 不受节流限制。
        agg.sample("asm", "1").await;
        let entries = agg.get_timeline("asm", "1").await;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].active_streams, 3,
            "active_streams must sum across protocols"
        );
        // cleanup
        prometheus::record_stream_close("asm", "1", "sse");
        prometheus::record_stream_close("asm", "1", "websocket");
        prometheus::record_stream_close("asm", "1", "http2");
    }

    #[test]
    fn test_record_latency_stores_sample() {
        let agg = TimelineAggregator::new();
        agg.record_latency("m", "1", 0.05);
        agg.record_latency("m", "1", 0.10);

        let entry = agg.latency_samples.get("m_1").unwrap();
        let deque = entry.value().lock().unwrap();
        assert_eq!(deque.len(), 2);
        assert_eq!(deque[0], 0.05);
        assert_eq!(deque[1], 0.10);
    }

    #[test]
    fn test_record_latency_caps_at_1000() {
        let agg = TimelineAggregator::new();
        for i in 0..1100 {
            agg.record_latency("m", "1", i as f64 * 0.001);
        }
        let entry = agg.latency_samples.get("m_1").unwrap();
        let deque = entry.value().lock().unwrap();
        assert_eq!(deque.len(), 1000);
        // First 100 should have been evicted
        assert_eq!(deque[0], 0.1);
    }

    #[test]
    fn test_compute_p99_ignores_nan() {
        let agg = TimelineAggregator::new();
        agg.record_latency("m", "1", 0.05);
        agg.record_latency("m", "1", f64::NAN);
        agg.record_latency("m", "1", 0.10);

        let p99 = agg.compute_p99_ms("m_1");
        assert!((50.0..=110.0).contains(&p99), "p99 should be around 100ms, got {}", p99);
    }

    #[test]
    fn test_compute_p99_matches_full_sort() {
        let agg = TimelineAggregator::new();
        // Insert 1000 sorted values 0.001..1.0
        for i in 1..=1000 {
            agg.record_latency("m", "1", i as f64 * 0.001);
        }
        let p99 = agg.compute_p99_ms("m_1");
        // 99th percentile of 1000 sorted values = index 990 (0-based) = 0.991s = 991ms
        assert_eq!(p99, 991.0, "p99 of 1000 sorted samples should be 991ms");
    }

    #[tokio::test]
    async fn test_get_timeline_returns_empty_for_unknown() {
        let agg = TimelineAggregator::new();
        let entries = agg.get_timeline("unknown", "1").await;
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_keys_returns_known_keys() {
        let agg = TimelineAggregator::new();
        // keys() reads from `data` map, which is populated by sample()
        // Since sample() needs prometheus gauges, we insert directly
        {
            let mut data = agg.data.lock().await;
            data.insert("model_a_1".to_string(), VecDeque::new());
            data.insert("model_b_2".to_string(), VecDeque::new());
        }
        let keys = agg.keys().await;
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"model_a_1".to_string()));
        assert!(keys.contains(&"model_b_2".to_string()));
    }

    #[tokio::test]
    async fn test_concurrent_record_and_read_no_deadlock() {
        let agg = TimelineAggregator::new();

        let (_r1, _r2) = tokio::join!(
            async {
                for i in 0..100 {
                    agg.record_latency("m", "1", i as f64 * 0.001);
                }
            },
            async {
                for _ in 0..10 {
                    let _ = agg.all_snapshots().await;
                }
            }
        );

        // Both should complete without deadlock
    }

    #[tokio::test]
    async fn test_all_snapshots_returns_data() {
        let agg = TimelineAggregator::new();
        agg.record_latency("m", "1", 0.05);
        agg.record_latency("m", "1", 0.10);

        // sample() requires prometheus gauges registered, so just test all_snapshots with data
        let snapshots = agg.all_snapshots().await;
        // Data map is empty because sample() hasn't been called — only latency_samples has data
        assert!(snapshots.is_empty());
    }

    // ===== Audit: B2 — QPS counts all status families =====

    /// `compute_qps` must sum `REQUESTS_TOTAL` across all four status
    /// families produced by `status_family` (handlers.rs): 2xx/3xx/4xx/5xx.
    /// Regression test for the defect where only 2xx + 5xx were counted,
    /// undercounting throughput by the 3xx/4xx rate.
    #[tokio::test]
    async fn test_qps_counts_all_status_families() {
        use crate::metrics::prometheus;

        // Ensure counters are registered
        let _ = prometheus::register_metrics();

        let agg = TimelineAggregator::new();
        let model = "b2_qps_all";
        let version = "1";
        let key = "b2_qps_all_1";

        // Seed last_counts / last_check at t=100.0 (delta is zero)
        let qps0 = agg.compute_qps(key, model, version, 100.0).await;
        assert_eq!(qps0, 0.0);

        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).inc_by(100.0);
        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "3xx"]).inc_by(5.0);
        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "4xx"]).inc_by(20.0);
        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "5xx"]).inc_by(10.0);

        // 135 requests over 1 second — 3xx/4xx included
        let qps = agg.compute_qps(key, model, version, 101.0).await;
        assert_eq!(qps, 135.0, "QPS must count 3xx/4xx requests, got {}", qps);
    }
}
