use dashmap::DashMap;
use lazy_static::lazy_static;
use prometheus::{CounterVec, GaugeVec};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

// Max data points per model timeline (ring buffer capacity) — Round2 B6:
// these are the DEFAULTS; runtime values come from `metrics.*` config via
// `TimelineAggregator::configure`.
const MAX_TIMELINE_POINTS: usize = 30;
const SAMPLE_INTERVAL_SECS: f64 = 10.0;
const P99_WINDOW_MAX_SAMPLES: usize = 1000;

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

/// Per-key latency sample window: (recorded_at_secs, duration_secs) pairs —
/// the timestamp feeds the optional age bound (B6).
type LatencySamples = DashMap<(String, String), std::sync::Mutex<VecDeque<(f64, f64)>>>;

pub struct TimelineAggregator {
    /// (model, version) key -> ring buffer of entries. Structured key: both
    /// model and version may contain `_` (validation.rs), so any string
    /// separator would make the key un-splittable (round2 audit B1).
    data: Mutex<HashMap<(String, String), VecDeque<TimelineEntry>>>,
    /// Last sample timestamp per key
    last_sample: Mutex<HashMap<(String, String), f64>>,
    /// Latency samples per key — DashMap shards eliminate cross-key contention.
    latency_samples: LatencySamples,
    /// Last request count per key (for QPS delta)
    last_counts: Mutex<HashMap<(String, String), f64>>,
    /// Last check timestamp per key (for QPS delta)
    last_check: Mutex<HashMap<(String, String), f64>>,
    /// B6 runtime knobs (atomic: record_latency is on the request hot path).
    max_points: std::sync::atomic::AtomicUsize,
    sample_interval_secs: std::sync::atomic::AtomicU64,
    p99_max_samples: std::sync::atomic::AtomicUsize,
    /// f64 bits; 0.0 = age bound off.
    p99_max_age_secs: std::sync::atomic::AtomicU64,
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
            max_points: std::sync::atomic::AtomicUsize::new(MAX_TIMELINE_POINTS),
            sample_interval_secs: std::sync::atomic::AtomicU64::new(SAMPLE_INTERVAL_SECS as u64),
            p99_max_samples: std::sync::atomic::AtomicUsize::new(P99_WINDOW_MAX_SAMPLES),
            p99_max_age_secs: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Round2 B6: apply `metrics.*` window config (called once at server
    /// startup; the TIMELINE singleton predates config load, so the knobs are
    /// runtime-set atomics rather than constructor params).
    pub fn configure(
        &self,
        max_points: usize,
        sample_interval_secs: u64,
        p99_max_samples: usize,
        p99_max_age_secs: f64,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        self.max_points.store(max_points, Relaxed);
        self.sample_interval_secs.store(sample_interval_secs, Relaxed);
        self.p99_max_samples.store(p99_max_samples, Relaxed);
        self.p99_max_age_secs.store(p99_max_age_secs.to_bits(), Relaxed);
    }

    fn p99_max_age(&self) -> f64 {
        f64::from_bits(self.p99_max_age_secs.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Record a latency sample from request handling. Lock-free across keys; per-key mutex is held briefly.
    pub fn record_latency(&self, model: &str, version: &str, duration_secs: f64) {
        use std::sync::atomic::Ordering::Relaxed;
        let now = now_secs();
        let max_samples = self.p99_max_samples.load(Relaxed);
        let max_age = self.p99_max_age();
        let key = (model.to_string(), version.to_string());
        let entry = self.latency_samples.entry(key).or_insert_with(|| {
            std::sync::Mutex::new(VecDeque::with_capacity(max_samples.min(1024)))
        });
        let mut deque = entry.value().lock().unwrap();
        deque.push_back((now, duration_secs));
        // Count bound (B6: configurable; default 1000).
        while deque.len() > max_samples {
            deque.pop_front();
        }
        // Age bound (B6: 0 = off) — evict stale front entries.
        if max_age > 0.0 {
            while let Some((ts, _)) = deque.front() {
                if now - ts > max_age {
                    deque.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    /// Sample current metrics into the timeline. Call periodically (e.g. every 10s).
    pub async fn sample(&self, model: &str, version: &str) {
        let key = (model.to_string(), version.to_string());
        let now = now_secs();

        // Throttle to the configured sample interval (0 = no throttle).
        let interval = self.sample_interval_secs.load(std::sync::atomic::Ordering::Relaxed);
        {
            let last_map = self.last_sample.lock().await;
            if let Some(ts) = last_map.get(&key) {
                if now - ts < interval as f64 {
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
            let max_points = self.max_points.load(std::sync::atomic::Ordering::Relaxed).max(1);
            let deque = data.entry(key.clone()).or_insert_with(|| {
                VecDeque::with_capacity(max_points)
            });
            while deque.len() >= max_points {
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
        let key = (model.to_string(), version.to_string());
        let data = self.data.lock().await;
        data.get(&key).cloned().unwrap_or_default().into_iter().collect()
    }

    /// Get all known (model, version) keys.
    pub async fn keys(&self) -> Vec<(String, String)> {
        let data = self.data.lock().await;
        data.keys().cloned().collect()
    }

    /// Round2 B2: drop all per-key state on version unload — the five maps
    /// would otherwise grow without bound across load/unload cycles.
    pub async fn remove(&self, model: &str, version: &str) {
        let key = (model.to_string(), version.to_string());
        self.data.lock().await.remove(&key);
        self.last_sample.lock().await.remove(&key);
        self.latency_samples.remove(&key);
        self.last_counts.lock().await.remove(&key);
        self.last_check.lock().await.remove(&key);
    }

    /// Get latest snapshot for every known key.
    pub async fn all_snapshots(&self) -> Vec<TimelineSnapshot> {
        let data = self.data.lock().await;
        data.iter()
            .map(|((model, version), entries)| TimelineSnapshot {
                model: model.clone(),
                version: version.clone(),
                entries: entries.iter().cloned().collect(),
            })
            .collect()
    }

    async fn compute_qps(&self, key: &(String, String), model: &str, version: &str, now: f64) -> f64 {
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
            last_check.insert(key.clone(), now);
            dt.max(0.001)
        };

        let qps = (current_count - last_count).max(0.0) / elapsed;
        last_counts.insert(key.clone(), current_count);
        round(qps, 2)
    }

    fn compute_p99_ms(&self, key: &(String, String)) -> f64 {
        let entry = match self.latency_samples.get(key) {
            Some(e) => e,
            None => return 0.0,
        };
        let mut deque = match entry.value().lock() {
            Ok(g) => g,
            Err(_) => return 0.0,
        };
        // B6: drop expired samples at read time too (low-QPS keys record
        // rarely, so record-time eviction alone would keep them visible).
        let max_age = self.p99_max_age();
        if max_age > 0.0 {
            let now = now_secs();
            while let Some((ts, _)) = deque.front() {
                if now - ts > max_age {
                    deque.pop_front();
                } else {
                    break;
                }
            }
        }
        if deque.len() < 2 {
            return 0.0;
        }
        let mut samples: Vec<f64> = deque.iter().map(|(_, v)| *v).filter(|v| !v.is_nan()).collect();
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

    fn k(model: &str, version: &str) -> (String, String) {
        (model.to_string(), version.to_string())
    }

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

        let entry = agg.latency_samples.get(&k("m", "1")).unwrap();
        let deque = entry.value().lock().unwrap();
        assert_eq!(deque.len(), 2);
        assert_eq!(deque[0].1, 0.05);
        assert_eq!(deque[1].1, 0.10);
    }

    #[test]
    fn test_record_latency_caps_at_1000() {
        let agg = TimelineAggregator::new();
        for i in 0..1100 {
            agg.record_latency("m", "1", i as f64 * 0.001);
        }
        let entry = agg.latency_samples.get(&k("m", "1")).unwrap();
        let deque = entry.value().lock().unwrap();
        assert_eq!(deque.len(), 1000);
        // First 100 should have been evicted
        assert_eq!(deque[0].1, 0.1);
    }

    #[test]
    fn test_compute_p99_ignores_nan() {
        let agg = TimelineAggregator::new();
        agg.record_latency("m", "1", 0.05);
        agg.record_latency("m", "1", f64::NAN);
        agg.record_latency("m", "1", 0.10);

        let p99 = agg.compute_p99_ms(&k("m", "1"));
        assert!((50.0..=110.0).contains(&p99), "p99 should be around 100ms, got {}", p99);
    }

    #[test]
    fn test_compute_p99_matches_full_sort() {
        let agg = TimelineAggregator::new();
        // Insert 1000 sorted values 0.001..1.0
        for i in 1..=1000 {
            agg.record_latency("m", "1", i as f64 * 0.001);
        }
        let p99 = agg.compute_p99_ms(&k("m", "1"));
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
            data.insert(k("model_a", "1"), VecDeque::new());
            data.insert(k("model_b", "2"), VecDeque::new());
        }
        let keys = agg.keys().await;
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&k("model_a", "1")));
        assert!(keys.contains(&k("model_b", "2")));
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
        let key = k(model, version);

        // Seed last_counts / last_check at t=100.0 (delta is zero)
        let qps0 = agg.compute_qps(&key, model, version, 100.0).await;
        assert_eq!(qps0, 0.0);

        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).inc_by(100.0);
        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "3xx"]).inc_by(5.0);
        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "4xx"]).inc_by(20.0);
        prometheus::REQUESTS_TOTAL.with_label_values(&[model, version, "5xx"]).inc_by(10.0);

        // 135 requests over 1 second — 3xx/4xx included
        let qps = agg.compute_qps(&key, model, version, 101.0).await;
        assert_eq!(qps, 135.0, "QPS must count 3xx/4xx requests, got {}", qps);
    }

    // ===== Audit round2: B1 — underscore in model name must survive the key round-trip =====

    /// Model names may contain `_` (validation.rs allows it), so the internal
    /// `model_version` string key is ambiguous: all_snapshots must report the
    /// original model/version, not a split at the first underscore.
    #[tokio::test]
    async fn test_all_snapshots_preserves_underscored_model_name() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.record_latency("snap_model", "1", 0.05);
        agg.sample("snap_model", "1").await;

        let snapshots = agg.all_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].model, "snap_model", "model name must not be split at `_`");
        assert_eq!(snapshots[0].version, "1");
    }

    /// Alerts must name the real model, not the mis-split fragment.
    #[tokio::test]
    async fn test_alerts_report_underscored_model_name() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        prometheus::QUEUE_DEPTH
            .with_label_values(&["alert_model", "1"])
            .set(200.0);
        agg.sample("alert_model", "1").await;

        let engine = AlertEngine::new(AlertThresholds::default());
        let alerts = engine.evaluate(&agg).await;
        let alert = alerts
            .iter()
            .find(|a| a.rule == "queue_depth")
            .expect("queue_depth alert expected");
        assert_eq!(alert.model, "alert_model");
        assert_eq!(alert.version, "1");
        prometheus::QUEUE_DEPTH
            .with_label_values(&["alert_model", "1"])
            .set(0.0);
    }

    /// compare-versions must match the full underscored model name.
    #[tokio::test]
    async fn test_compare_versions_matches_underscored_model() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.sample("cmp_model", "1").await;

        let cmp = VersionComparator::compare(&agg, "cmp_model")
            .await
            .expect("comparison for underscored model must not be empty");
        assert_eq!(cmp.model, "cmp_model");
        assert_eq!(cmp.versions.len(), 1);
        assert_eq!(cmp.versions[0].version, "1");
    }

    // ===== Round2 B2: unload cleanup =====

    /// remove() must drop all per-key state (timeline, latency samples, QPS
    /// deltas) so repeated load/unload cycles cannot grow the maps.
    #[tokio::test]
    async fn test_remove_clears_all_key_state() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.record_latency("rm_model", "1", 0.05);
        agg.sample("rm_model", "1").await;
        assert!(!agg.get_timeline("rm_model", "1").await.is_empty());
        assert!(agg.latency_samples.contains_key(&k("rm_model", "1")));

        agg.remove("rm_model", "1").await;

        assert!(agg.get_timeline("rm_model", "1").await.is_empty());
        assert!(!agg.latency_samples.contains_key(&k("rm_model", "1")));
        assert!(!agg.keys().await.contains(&k("rm_model", "1")));
        assert!(!agg.last_counts.lock().await.contains_key(&k("rm_model", "1")));
        assert!(!agg.last_check.lock().await.contains_key(&k("rm_model", "1")));
        assert!(!agg.last_sample.lock().await.contains_key(&k("rm_model", "1")));
    }

    // ===== Round2 B6: configurable windows =====

    /// configure() resizes the timeline ring (and a 0s sample interval
    /// disables throttling so successive samples land).
    #[tokio::test]
    async fn test_configure_resizes_timeline_window() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.configure(2, 0, 1000, 0.0);
        agg.sample("cfg_model", "1").await;
        agg.sample("cfg_model", "1").await;
        agg.sample("cfg_model", "1").await;
        assert_eq!(agg.get_timeline("cfg_model", "1").await.len(), 2);
    }

    /// The p99 sliding window honors the configured sample cap.
    #[test]
    fn test_configure_p99_max_samples() {
        let agg = TimelineAggregator::new();
        agg.configure(30, 10, 3, 0.0);
        for i in 0..5 {
            agg.record_latency("cfg2", "1", i as f64 * 0.001);
        }
        let entry = agg.latency_samples.get(&k("cfg2", "1")).unwrap();
        assert_eq!(entry.value().lock().unwrap().len(), 3);
    }

    /// With an age bound configured, stale samples are evicted on record.
    #[test]
    fn test_p99_window_age_eviction() {
        let agg = TimelineAggregator::new();
        agg.configure(30, 10, 1000, 10.0); // 10s age bound
        {
            let entry = agg
                .latency_samples
                .entry(k("cfg3", "1"))
                .or_insert_with(|| std::sync::Mutex::new(VecDeque::with_capacity(8)));
            entry
                .value()
                .lock()
                .unwrap()
                .push_back((now_secs() - 100.0, 0.5)); // stale sample
        }
        agg.record_latency("cfg3", "1", 0.7);
        let entry = agg.latency_samples.get(&k("cfg3", "1")).unwrap();
        let deque = entry.value().lock().unwrap();
        assert_eq!(deque.len(), 1, "stale sample must be evicted on record");
        assert_eq!(deque[0].1, 0.7);
    }
}
