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
    // ===== M3: fields sampled from the existing Prometheus registry (zero
    // new recording points; blueprint 6.5 #10/#12 — no new labels, no new
    // hot-path instrumentation). Older instances omit them entirely; the UI
    // treats absent fields as "instance version not supported".
    /// Accepted but unfinished requests (IN_FLIGHT_REQUESTS gauge).
    pub in_flight: i64,
    /// Max concurrent in-flight batches across workers (WORKER_SATURATION).
    pub worker_saturation: f64,
    /// Bucket-interpolated p99 of STREAMING_TTFT across protocols. Process-
    /// lifetime histogram (not a sliding window like p99_ms); 0 without
    /// streaming traffic.
    pub ttft_p99_ms: f64,
    /// Same derivation as ttft_p99_ms, from STREAMING_TBT.
    pub tbt_p99_ms: f64,
    /// STREAM_OUTPUT_BYTES_TOTAL summed across stream_kind, rated over the
    /// sample window (same delta pattern as qps).
    pub stream_bytes_per_s: f64,
    /// lite_server_tokens_generated_total rate; null until the model reports
    /// tokens via the worker callback channel.
    pub tokens_per_s: Option<f64>,
    /// RSS summed over the version's live workers, in MiB
    /// (WORKERS_RSS_BYTES / 2^20).
    pub rss_mb: f64,
    /// Process-wide CPU usage (PROCESS_CPU_SECONDS_TOTAL delta / wall delta).
    /// Cumulative across cores, so values may exceed 100.
    pub cpu_percent: f64,
    /// RETRIES_TOTAL rate over the sample window.
    pub retries_per_s: f64,
    /// WORKER_EJECTIONS_TOTAL rate over the sample window.
    pub ejections_per_s: f64,
}

/// M3: last-seen counter values per key, the rate baselines for the counter-
/// derived timeline fields (stream bytes / tokens / retries / ejections).
/// QPS keeps its own last_counts/last_check pair (pre-existing).
#[derive(Clone, Copy, Default)]
struct CounterBaseline {
    stream_bytes: f64,
    /// None until the model's token counter series exists.
    tokens: Option<f64>,
    retries: f64,
    ejections: f64,
}

/// Tick-level sampling context (M2/L1): the per-tick work — sysinfo refresh,
/// registry gather, process-wide CPU rate — done ONCE and shared by every key
/// sampled in the tick. cpu_percent is process-scoped, so all keys of a tick
/// must see the same value; per-key computation gave the 2nd+ key a few-ms
/// rate window (noise spikes) and repeated the refresh/gather per key.
struct TickContext {
    families: Vec<prometheus::proto::MetricFamily>,
    cpu_percent: f64,
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

/// AGG-1: hard cap on distinct (model, version) latency-sample keys. Unknown-
/// model request paths (404/401/429/stream-reject) create keys that the
/// loaded-model unload path (`remove`) never reaps; without a cap a client
/// enumerating model names grows the map linearly. 1024 keys x <=1000 samples
/// x 16B bounds the map at ~16MB — far above any real active version count.
pub(crate) const MAX_LATENCY_KEYS: usize = 1024;

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
    /// M3: rate baselines for the counter-derived fields (B2: reaped on unload).
    last_counters: Mutex<HashMap<(String, String), CounterBaseline>>,
    /// M3: process-wide CPU baseline — (cpu_seconds, wall_secs). Global, not
    /// per key: the CPU counter is process-scoped.
    last_cpu: Mutex<Option<(f64, f64)>>,
    /// B6 runtime knobs (atomic: record_latency is on the request hot path).
    max_points: std::sync::atomic::AtomicUsize,
    sample_interval_secs: std::sync::atomic::AtomicU64,
    p99_max_samples: std::sync::atomic::AtomicUsize,
    /// f64 bits; 0.0 = age bound off.
    p99_max_age_secs: std::sync::atomic::AtomicU64,
    /// AGG-1: one-shot flag so the key-cap drop logs exactly one warning.
    latency_cap_warned: std::sync::atomic::AtomicBool,
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
            last_counters: Mutex::new(HashMap::new()),
            last_cpu: Mutex::new(None),
            max_points: std::sync::atomic::AtomicUsize::new(MAX_TIMELINE_POINTS),
            sample_interval_secs: std::sync::atomic::AtomicU64::new(SAMPLE_INTERVAL_SECS as u64),
            p99_max_samples: std::sync::atomic::AtomicUsize::new(P99_WINDOW_MAX_SAMPLES),
            p99_max_age_secs: std::sync::atomic::AtomicU64::new(0),
            latency_cap_warned: std::sync::atomic::AtomicBool::new(false),
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

    /// M3: retention window covered by the ring buffer, in seconds
    /// (max_points x sample_interval). Surfaced as X-Timeline-Coverage so
    /// clients can clamp their selectable time ranges honestly.
    pub fn coverage_secs(&self) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        self.max_points.load(Relaxed) as u64 * self.sample_interval_secs.load(Relaxed)
    }

    /// M3: configured sampling interval (X-Timeline-Interval).
    pub fn sample_interval_secs(&self) -> u64 {
        self.sample_interval_secs
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Record a latency sample from request handling. Lock-free across keys; per-key mutex is held briefly.
    pub fn record_latency(&self, model: &str, version: &str, duration_secs: f64) {
        use std::sync::atomic::Ordering::Relaxed;
        let now = now_secs();
        let max_samples = self.p99_max_samples.load(Relaxed);
        let max_age = self.p99_max_age();
        let key = (model.to_string(), version.to_string());
        // AGG-1: fast path checks key existence (single-shard read, same cost
        // class as `entry`); only a FIRST-seen key pays the cross-shard len()
        // for the cardinality cap. New keys beyond the cap are dropped —
        // unknown-model requests must not grow the map without bound.
        if !self.latency_samples.contains_key(&key)
            && self.latency_samples.len() >= MAX_LATENCY_KEYS
        {
            if !self.latency_cap_warned.swap(true, Relaxed) {
                tracing::warn!(
                    cap = MAX_LATENCY_KEYS,
                    "latency sample key cap reached; dropping samples for new (model, version) keys"
                );
            }
            return;
        }
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

    /// Tick-level work, once per tick: refresh process/worker memory + CPU
    /// gauges (so rss_mb / cpu_percent are fresh even when no Prometheus
    /// scraper hits /metrics), gather the registry, and advance the
    /// process-wide CPU baseline.
    async fn begin_tick(&self) -> TickContext {
        super::prometheus::refresh_process_metrics();
        let families = super::prometheus::REGISTRY.gather();
        let cpu_percent = {
            let now = now_secs();
            let cpu_secs = super::prometheus::PROCESS_CPU_SECONDS_TOTAL.get();
            let mut last = self.last_cpu.lock().await;
            let pct = match *last {
                Some((prev_cpu, prev_wall)) if cpu_secs >= prev_cpu && now > prev_wall => {
                    round((cpu_secs - prev_cpu) / (now - prev_wall) * 100.0, 1)
                }
                _ => 0.0,
            };
            *last = Some((cpu_secs, now));
            pct
        };
        TickContext {
            families,
            cpu_percent,
        }
    }

    /// Sample current metrics into the timeline. Call periodically (e.g. every 10s).
    pub async fn sample(&self, model: &str, version: &str) {
        let ctx = self.begin_tick().await;
        self.sample_with_ctx(model, version, &ctx).await;
    }

    /// Sample every key of one tick, sharing the tick-level work (M2/L1) —
    /// the server sampling loop's entry point.
    pub async fn sample_tick(&self, keys: &[(String, String)]) {
        let ctx = self.begin_tick().await;
        for (model, version) in keys {
            self.sample_with_ctx(model, version, &ctx).await;
        }
    }

    async fn sample_with_ctx(&self, model: &str, version: &str, ctx: &TickContext) {
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

        // Capture the rate window BEFORE compute_qps advances last_check, so
        // every counter-derived rate below shares one elapsed window.
        let prev_check = { self.last_check.lock().await.get(&key).copied() };
        let elapsed = (now - prev_check.unwrap_or(now)).max(0.001);

        // Compute QPS from request count delta
        let qps = self.compute_qps(&key, model, version, now).await;

        // Compute p99 from latency samples
        let p99_ms = self.compute_p99_ms(&key);

        // Read queue depth and active workers from Prometheus gauges
        let queue_depth = read_gauge(&super::prometheus::QUEUE_DEPTH, &[model, version]);
        let active_workers = read_gauge(&super::prometheus::ACTIVE_WORKERS, &[model, version]);

        // M3: the tick-level gather feeds every family-scan below (streaming
        // connections, TTFT/TBT histograms, stream bytes, tokens).
        let families = &ctx.families;

        // G6:活跃流式连接——跨 protocol 求和(read_gauge 传固定 label 会因
        // 基数不符 panic,须遍历子序列过滤)。
        let active_streams = read_active_streams(families, model, version);

        let in_flight = read_gauge(&super::prometheus::IN_FLIGHT_REQUESTS, &[model, version]) as i64;
        let worker_saturation = read_gauge(&super::prometheus::WORKER_SATURATION, &[model, version]);
        let rss_mb = round(
            super::prometheus::WORKERS_RSS_BYTES
                .with_label_values(&[model, version])
                .get() as f64
                / 1_048_576.0,
            1,
        );
        let ttft_p99_ms = histogram_p99_ms(families, "liteserver_streaming_ttft_seconds", model, version);
        let tbt_p99_ms = histogram_p99_ms(families, "liteserver_streaming_tbt_seconds", model, version);

        let stream_bytes =
            counter_family_sum(families, "liteserver_stream_output_bytes_total", model, version)
                .unwrap_or(0.0);
        let tokens = counter_family_sum(families, "lite_server_tokens_generated_total", model, version);
        let retries = read_counter(&super::prometheus::RETRIES_TOTAL, &[model, version]);
        let ejections = read_counter(&super::prometheus::WORKER_EJECTIONS_TOTAL, &[model, version]);

        let (stream_bytes_per_s, tokens_per_s, retries_per_s, ejections_per_s) = {
            let mut last = self.last_counters.lock().await;
            let prev = last.get(&key).copied();
            // First sample after the counter already advanced (model served
            // traffic before the sampler's first tick): baseline on the
            // current values — the same pattern qps (unwrap_or(current)) and
            // tokens use — instead of rating the lifetime counter over the
            // 1ms fallback window.
            let baseline = prev.unwrap_or(CounterBaseline {
                stream_bytes,
                tokens,
                retries,
                ejections,
            });
            let rate = |cur: f64, base: f64| round((cur - base).max(0.0) / elapsed, 2);
            let result = (
                rate(stream_bytes, baseline.stream_bytes),
                // The rate exists only while the token counter series does;
                // the first sample after it appears is the baseline (0.0).
                tokens.map(|t| rate(t, baseline.tokens.unwrap_or(t))),
                rate(retries, baseline.retries),
                rate(ejections, baseline.ejections),
            );
            last.insert(
                key.clone(),
                CounterBaseline {
                    stream_bytes,
                    tokens,
                    retries,
                    ejections,
                },
            );
            result
        };

        // M2: process-wide CPU rate computed once per tick (begin_tick) and
        // shared by every key.
        let cpu_percent = ctx.cpu_percent;

        let entry = TimelineEntry {
            timestamp: now,
            qps,
            p99_ms,
            queue_depth: queue_depth as i64,
            active_workers: active_workers as i64,
            active_streams,
            in_flight,
            worker_saturation,
            ttft_p99_ms,
            tbt_p99_ms,
            stream_bytes_per_s,
            tokens_per_s,
            rss_mb,
            cpu_percent,
            retries_per_s,
            ejections_per_s,
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
        self.last_counters.lock().await.remove(&key);
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
fn read_active_streams(
    families: &[prometheus::proto::MetricFamily],
    model: &str,
    version: &str,
) -> i64 {
    let Some(family) = families
        .iter()
        .find(|mf| mf.get_name() == "liteserver_streaming_connections")
    else {
        return 0;
    };
    let mut total = 0.0;
    for m in family.get_metric() {
        if labels_match(m, model, version) {
            total += m.get_gauge().get_value();
        }
    }
    total as i64
}

fn labels_match(m: &prometheus::proto::Metric, model: &str, version: &str) -> bool {
    let labels = m.get_label();
    labels
        .iter()
        .any(|l| l.get_name() == "model" && l.get_value() == model)
        && labels
            .iter()
            .any(|l| l.get_name() == "version" && l.get_value() == version)
}

/// M3: sum a counter family's series for one (model, version) across any
/// extra labels (e.g. stream_kind). Gather-based: with_label_values would
/// need the extra label values and would CREATE zero series on read. None
/// when the family has no series for the key at all (e.g. a model that never
/// reported tokens — the null-vs-zero distinction the UI keys on).
fn counter_family_sum(
    families: &[prometheus::proto::MetricFamily],
    name: &str,
    model: &str,
    version: &str,
) -> Option<f64> {
    let family = families.iter().find(|mf| mf.get_name() == name)?;
    let mut total = 0.0;
    let mut found = false;
    for m in family.get_metric() {
        if labels_match(m, model, version) {
            total += m.get_counter().get_value();
            found = true;
        }
    }
    found.then_some(total)
}

/// M3: approximate p99 (ms) from a histogram family's Prometheus buckets,
/// merging all series of one (model, version) across extra labels (protocol).
/// Same-layout buckets make cumulative counts additive across series; the
/// quantile is linearly interpolated inside the hit bucket
/// (histogram_quantile-style). Scope note: the histogram is process-lifetime,
/// not a sliding window like compute_p99_ms.
fn histogram_p99_ms(
    families: &[prometheus::proto::MetricFamily],
    name: &str,
    model: &str,
    version: &str,
) -> f64 {
    let Some(family) = families.iter().find(|mf| mf.get_name() == name) else {
        return 0.0;
    };
    // (upper_bound, merged cumulative count); +Inf buckets are excluded.
    let mut buckets: Vec<(f64, u64)> = Vec::new();
    let mut total: u64 = 0;
    for m in family.get_metric() {
        if !labels_match(m, model, version) {
            continue;
        }
        let h = m.get_histogram();
        total += h.get_sample_count();
        for b in h.get_bucket() {
            let upper = b.get_upper_bound();
            if upper.is_infinite() {
                continue;
            }
            match buckets.iter_mut().find(|(u, _)| *u == upper) {
                Some((_, cum)) => *cum += b.get_cumulative_count(),
                None => buckets.push((upper, b.get_cumulative_count())),
            }
        }
    }
    if total < 2 || buckets.is_empty() {
        return 0.0;
    }
    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let rank = 0.99 * total as f64;
    let mut prev_upper = 0.0;
    let mut prev_cum: u64 = 0;
    for (upper, cum) in &buckets {
        if (*cum as f64) >= rank {
            let span = (*cum - prev_cum) as f64;
            let frac = if span > 0.0 {
                ((rank - prev_cum as f64) / span).clamp(0.0, 1.0)
            } else {
                0.0
            };
            return round((prev_upper + (upper - prev_upper) * frac) * 1000.0, 1);
        }
        prev_upper = *upper;
        prev_cum = *cum;
    }
    // Rank above the last finite bucket: report the last finite upper bound.
    round(prev_upper * 1000.0, 1)
}

/// M3: keep every `step`-th point, anchored at the END so the freshest point
/// always survives (the UI renders "now" from the last entry). step <= 1 is
/// a no-op; the handler rejects step=0 before calling this.
pub fn downsample_entries(entries: Vec<TimelineEntry>, step: usize) -> Vec<TimelineEntry> {
    if step <= 1 || entries.len() <= 1 {
        return entries;
    }
    let keep_from = (entries.len() - 1) % step;
    entries
        .into_iter()
        .enumerate()
        .filter(|(i, _)| i % step == keep_from)
        .map(|(_, e)| e)
        .collect()
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

    /// P1 AGG-1 evidence (project-resource-leak-sweep-0815.md): `record_latency`
    /// inserts an arbitrary (model, version) key on first touch (:107-132) and
    /// the ONLY cleanup, `remove` (:206-213), is invoked from the loaded-model
    /// unload path. Unknown-model requests (404/401/429/stream-reject) create
    /// keys that never age out — with the default `p99_max_age = 0` the age
    /// bound is off, so a client enumerating model names grows memory linearly.
    ///
    /// Fixed code caps unknown keys at MAX_LATENCY_KEYS; this test FAILS (RED)
    /// until the leak is addressed.
    #[test]
    fn test_record_latency_unknown_model_keys_grow_unbounded() {
        let agg = TimelineAggregator::new();
        for i in 0..10_000 {
            agg.record_latency(&format!("unknown-{}", i), &format!("v{}", i), 0.01);
        }
        let n = agg.latency_samples.len();
        assert!(
            n <= MAX_LATENCY_KEYS,
            "AGG-1: record_latency held {n} keys for never-loaded models — \
             unbounded growth from unknown-model request paths"
        );
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

    // ===== M3: extended timeline fields =====

    /// in_flight / worker_saturation / rss_mb are read from the existing
    /// Prometheus gauges at sample time (zero new recording points).
    #[tokio::test]
    async fn test_sample_records_extended_gauge_fields() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        let (m, v) = ("m3_gauges", "1");
        prometheus::IN_FLIGHT_REQUESTS
            .with_label_values(&[m, v])
            .set(7.0);
        prometheus::WORKER_SATURATION
            .with_label_values(&[m, v])
            .set(3.0);
        prometheus::WORKERS_RSS_BYTES
            .with_label_values(&[m, v])
            .set(2 * 1024 * 1024 * 1024); // 2 GiB across the version's workers

        agg.sample(m, v).await;
        let entries = agg.get_timeline(m, v).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].in_flight, 7);
        assert_eq!(entries[0].worker_saturation, 3.0);
        assert!(
            (entries[0].rss_mb - 2048.0).abs() < 0.01,
            "rss_mb must convert bytes to MiB, got {}",
            entries[0].rss_mb
        );
        assert!(entries[0].cpu_percent >= 0.0);

        prometheus::IN_FLIGHT_REQUESTS
            .with_label_values(&[m, v])
            .set(0.0);
        prometheus::WORKER_SATURATION
            .with_label_values(&[m, v])
            .set(0.0);
        prometheus::WORKERS_RSS_BYTES
            .with_label_values(&[m, v])
            .set(0);
    }

    /// tokens_per_s stays null until the model reports
    /// lite_server_tokens_generated_total via the worker callback channel.
    #[tokio::test]
    async fn test_sample_tokens_per_s_none_without_callback_report() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.sample("m3_no_tokens", "1").await;
        let entries = agg.get_timeline("m3_no_tokens", "1").await;
        assert_eq!(entries.len(), 1);
        assert!(entries[0].tokens_per_s.is_none());
        let json = serde_json::to_value(&entries[0]).unwrap();
        assert!(
            json["tokens_per_s"].is_null(),
            "unreported tokens_per_s must serialize as null"
        );
    }

    /// Once the model reports tokens, tokens_per_s becomes a per-second rate
    /// (same delta-over-elapsed pattern as QPS; first sample is the baseline).
    #[tokio::test]
    async fn test_sample_tokens_per_s_rate_after_callback_report() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.configure(30, 0, 1000, 0.0); // interval 0 = no throttle
        let (m, v) = ("m3_tokens", "1");
        let report = crate::proto::liteserver::Metrics {
            tokens_generated: 10,
            ..Default::default()
        };
        prometheus::record_worker_metrics(m, v, Some(&report));
        agg.sample(m, v).await; // baseline
        prometheus::record_worker_metrics(m, v, Some(&report));
        agg.sample(m, v).await;

        let entries = agg.get_timeline(m, v).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].tokens_per_s, Some(0.0));
        let rate = entries[1]
            .tokens_per_s
            .expect("reported tokens must yield a rate");
        assert!(rate > 0.0, "token rate must be positive after +10, got {rate}");
    }

    /// retries / ejections are per-second rates over the existing counters.
    #[tokio::test]
    async fn test_sample_retry_and_ejection_rates() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.configure(30, 0, 1000, 0.0);
        let (m, v) = ("m3_retry", "1");
        agg.sample(m, v).await; // baseline
        prometheus::inc_retry(m, v);
        prometheus::inc_retry(m, v);
        prometheus::inc_worker_ejection(m, v);
        agg.sample(m, v).await;

        let entries = agg.get_timeline(m, v).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].retries_per_s, 0.0);
        assert_eq!(entries[0].ejections_per_s, 0.0);
        assert!(entries[1].retries_per_s > 0.0);
        assert!(entries[1].ejections_per_s > 0.0);
    }

    /// stream_bytes_per_s sums liteserver_stream_output_bytes_total across
    /// stream_kind and rates it over the sample window.
    #[tokio::test]
    async fn test_sample_stream_bytes_rate() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.configure(30, 0, 1000, 0.0);
        let (m, v) = ("m3_bytes", "1");
        agg.sample(m, v).await; // baseline
        prometheus::STREAM_OUTPUT_BYTES_TOTAL
            .with_label_values(&[m, v, "sse"])
            .inc_by(1024.0);
        prometheus::STREAM_OUTPUT_BYTES_TOTAL
            .with_label_values(&[m, v, "ws"])
            .inc_by(1024.0);
        agg.sample(m, v).await;

        let entries = agg.get_timeline(m, v).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].stream_bytes_per_s, 0.0);
        assert!(
            entries[1].stream_bytes_per_s > 0.0,
            "stream byte rate must sum across stream_kind"
        );
    }

    /// ttft_p99_ms / tbt_p99_ms derive from the existing streaming histograms
    /// (bucket-aggregated across protocol, interpolated within the bucket).
    #[tokio::test]
    async fn test_sample_ttft_tbt_p99_from_histograms() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        let (m, v) = ("m3_ttft", "1");
        for _ in 0..100 {
            prometheus::record_stream_ttft(m, v, "sse", 0.05);
            prometheus::record_stream_tbt(m, v, "ws", 0.02);
        }
        agg.sample(m, v).await;
        let entries = agg.get_timeline(m, v).await;
        assert_eq!(entries.len(), 1);
        assert!(
            (25.0..=100.0).contains(&entries[0].ttft_p99_ms),
            "ttft p99 of 100x50ms samples must land near 50ms, got {}",
            entries[0].ttft_p99_ms
        );
        assert!(
            (10.0..=50.0).contains(&entries[0].tbt_p99_ms),
            "tbt p99 of 100x20ms samples must land near 20ms, got {}",
            entries[0].tbt_p99_ms
        );
    }

    /// No streaming traffic -> histogram-derived p99 reads 0 (same convention
    /// as the existing p99_ms field).
    #[tokio::test]
    async fn test_sample_ttft_tbt_p99_zero_without_traffic() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.sample("m3_no_stream", "1").await;
        let entries = agg.get_timeline("m3_no_stream", "1").await;
        assert_eq!(entries[0].ttft_p99_ms, 0.0);
        assert_eq!(entries[0].tbt_p99_ms, 0.0);
    }

    // ===== M2/L1: tick-level sampling =====

    /// Spin one core for `dur` so process CPU seconds measurably advance.
    fn burn_cpu(dur: std::time::Duration) {
        let start = std::time::Instant::now();
        let mut acc = 0.0f64;
        while start.elapsed() < dur {
            acc += (acc + 1.0).sqrt();
        }
        std::hint::black_box(acc);
    }

    /// M2: cpu_percent is process-wide, so every key sampled in the same tick
    /// must report the SAME value from one shared rate window. Per-key
    /// computation made the 2nd+ key's window a few milliseconds, turning
    /// cpu_percent into noise spikes on multi-version deployments.
    #[tokio::test]
    async fn test_sample_tick_shares_cpu_percent_across_keys() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.configure(30, 0, 1000, 0.0); // interval 0 = no throttle
        let keys = vec![k("tick_m", "1"), k("tick_m", "2")];

        agg.sample_tick(&keys).await; // baseline tick
        burn_cpu(std::time::Duration::from_millis(250));
        agg.sample_tick(&keys).await;

        let e1 = agg.get_timeline("tick_m", "1").await;
        let e2 = agg.get_timeline("tick_m", "2").await;
        assert_eq!(e1.len(), 2);
        assert_eq!(e2.len(), 2);
        assert!(
            e1[1].cpu_percent > 0.0,
            "a 250ms CPU burn between ticks must register (got {})",
            e1[1].cpu_percent
        );
        assert_eq!(
            e1[1].cpu_percent, e2[1].cpu_percent,
            "same-tick keys must share one process-wide cpu_percent"
        );
    }

    /// L1: one tick samples every loaded key (the server loop entry point).
    #[tokio::test]
    async fn test_sample_tick_records_all_keys() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        let keys = vec![k("tick_all", "1"), k("tick_all", "2"), k("tick_all", "3")];
        agg.sample_tick(&keys).await;
        for (_, v) in &keys {
            assert_eq!(agg.get_timeline("tick_all", v).await.len(), 1);
        }
    }

    // ===== M3: step downsampling =====

    fn m3_entries(n: usize) -> Vec<TimelineEntry> {
        (0..n)
            .map(|i| TimelineEntry {
                timestamp: 1000.0 + i as f64,
                qps: i as f64,
                p99_ms: 0.0,
                queue_depth: 0,
                active_workers: 0,
                active_streams: 0,
                in_flight: 0,
                worker_saturation: 0.0,
                ttft_p99_ms: 0.0,
                tbt_p99_ms: 0.0,
                stream_bytes_per_s: 0.0,
                tokens_per_s: None,
                rss_mb: 0.0,
                cpu_percent: 0.0,
                retries_per_s: 0.0,
                ejections_per_s: 0.0,
            })
            .collect()
    }

    #[test]
    fn test_downsample_step_one_keeps_all() {
        let out = downsample_entries(m3_entries(10), 1);
        assert_eq!(out.len(), 10);
    }

    /// Every Nth point counting from the END, so the freshest point is always
    /// kept: 10 points at step 3 keep indices 0, 3, 6, 9.
    #[test]
    fn test_downsample_keeps_every_nth_from_latest() {
        let out = downsample_entries(m3_entries(10), 3);
        let ts: Vec<f64> = out.iter().map(|e| e.timestamp).collect();
        assert_eq!(ts, vec![1000.0, 1003.0, 1006.0, 1009.0]);
    }

    #[test]
    fn test_downsample_step_beyond_len_keeps_latest_only() {
        let out = downsample_entries(m3_entries(5), 100);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].timestamp, 1004.0, "the latest point must survive");
    }

    /// step=0 is rejected at the handler; the pure function stays total.
    #[test]
    fn test_downsample_step_zero_treated_as_one() {
        let out = downsample_entries(m3_entries(4), 0);
        assert_eq!(out.len(), 4);
    }

    // ===== M3: coverage =====

    #[test]
    fn test_coverage_secs_reflects_configure() {
        let agg = TimelineAggregator::new();
        agg.configure(1440, 60, 1000, 0.0);
        assert_eq!(agg.coverage_secs(), 1440 * 60);
        assert_eq!(agg.sample_interval_secs(), 60);
    }

    /// remove() must also drop the M3 rate-baseline state for the key.
    #[tokio::test]
    async fn test_remove_clears_m3_rate_state() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.sample("m3_rm", "1").await;
        assert!(agg.last_counters.lock().await.contains_key(&k("m3_rm", "1")));
        agg.remove("m3_rm", "1").await;
        assert!(!agg.last_counters.lock().await.contains_key(&k("m3_rm", "1")));
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

    // ===== audit repro: first-sample rate spike =====

    /// Audit M1: a counter that advanced BEFORE the sampler's first tick
    /// (model loaded and serving before the timeline task started) must
    /// establish the baseline — like qps (unwrap_or(current)) and tokens
    /// (unwrap_or(t)) already do — not rate the lifetime counter over the
    /// 1ms fallback window. Repro: 2 retries before the first sample must
    /// not become thousands per second.
    #[tokio::test]
    async fn test_first_sample_after_traffic_baselines_retry_counter() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.configure(30, 0, 1000, 0.0);
        let (m, v) = ("m3_first_retry", "1");
        prometheus::inc_retry(m, v);
        prometheus::inc_retry(m, v);
        agg.sample(m, v).await;

        let entries = agg.get_timeline(m, v).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].retries_per_s, 0.0,
            "first sample must baseline the counter (got {} — lifetime counter / 1ms window)",
            entries[0].retries_per_s
        );
    }

    /// Audit M1, stream-bytes path: same zero-baseline bug via the
    /// counter_family_sum branch (10MB streamed pre-sample must not report
    /// as ~10GB/s on the first point).
    #[tokio::test]
    async fn test_first_sample_after_traffic_baselines_stream_bytes() {
        use crate::metrics::prometheus;
        let _ = prometheus::register_metrics();
        let agg = TimelineAggregator::new();
        agg.configure(30, 0, 1000, 0.0);
        let (m, v) = ("m3_first_bytes", "1");
        prometheus::STREAM_OUTPUT_BYTES_TOTAL
            .with_label_values(&[m, v, "sse"])
            .inc_by(10_000_000.0);
        agg.sample(m, v).await;

        let entries = agg.get_timeline(m, v).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].stream_bytes_per_s, 0.0,
            "first sample must baseline the counter (got {})",
            entries[0].stream_bytes_per_s
        );
    }
}
