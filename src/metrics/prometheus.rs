use lazy_static::lazy_static;
use prometheus::{
    CounterVec, GaugeVec, HistogramOpts, HistogramVec, Registry, TextEncoder,
};
use std::collections::HashMap;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Request metrics
    pub static ref REQUESTS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_requests_total",
            "Total HTTP requests"
        ),
        &["model", "version", "status"]
    ).unwrap();

    pub static ref REQUEST_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_request_duration_seconds",
            "End-to-end request latency"
        // S7/D8:桶追加 30/60/120——S1 落地后分钟级流时长经 record_request_end
        // 进入该 histogram(现桶顶 10s 会让长流全落 +Inf)。纯增量,既有观测不迁移。
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]),
        &["model", "version"]
    ).unwrap();

    // Queue metrics
    pub static ref QUEUE_DEPTH: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_queue_depth",
            "Current request queue size"
        ),
        &["model", "version"]
    ).unwrap();

    // Model metrics
    pub static ref MODEL_LOAD_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_model_load_total",
            "Model load/unload events"
        ),
        &["model", "version", "action", "status"]
    ).unwrap();

    pub static ref VERSION_SWITCHES_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_version_switches_total",
            "Active version changes"
        ),
        &["model", "from", "to"]
    ).unwrap();

    pub static ref ACTIVE_WORKERS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_active_workers",
            "Number of alive inference workers"
        ),
        &["model", "version"]
    ).unwrap();

    pub static ref VERSION_WEIGHT: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_version_weight",
            "Traffic weight per model version (§4.3 weighted routing)"
        ),
        &["model", "version"]
    ).unwrap();

    // P-WARM (§4.3): per-version readiness. 1 while serving (Ready/Degraded),
    // 0 otherwise (Pending/Loading/WarmingUp/Failed/Unloading) — so a version
    // held in WarmingUp or parked in Failed is visible to scrapers/LB checks.
    pub static ref MODEL_READY: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_model_ready",
            "1 when the version is serving (Ready/Degraded), 0 otherwise (incl. WarmingUp/Failed)"
        ),
        &["model", "version"]
    ).unwrap();

    // Ensemble metrics
    pub static ref ENSEMBLE_STEP_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_ensemble_step_latency_seconds",
            "Per-step latency in ensemble DAG"
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        &["ensemble", "step", "model", "version"]
    ).unwrap();

    // Outlier detection metrics
    pub static ref WORKER_EJECTIONS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_worker_ejections_total",
            "Total worker ejections due to consecutive failures"
        ),
        &["model", "version"]
    ).unwrap();

    pub static ref RETRIES_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_retries_total",
            "Total request retries"
        ),
        &["model", "version"]
    ).unwrap();

    // Streaming metrics
    pub static ref STREAMING_CONNECTIONS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_streaming_connections",
            "Active bidirectional streaming connections"
        ),
        &["model", "version", "protocol"]
    ).unwrap();

    pub static ref STREAMING_TTFT: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_streaming_ttft_seconds",
            "Time to first token in streaming"
        // S7:桶追加 5/10/30/60——大模型冷启动 TTFT>2.5s(旧桶顶)不落 +Inf。
        // 纯增量,既有观测不迁移。
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
        &["model", "version", "protocol"]
    ).unwrap();

    pub static ref STREAMING_TBT: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_streaming_tbt_seconds",
            "Time between tokens in streaming"
        // S7:桶追加 1/2.5/5——慢解码 chunk 间隔不落 +Inf(旧桶顶 0.5s)。
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        &["model", "version", "protocol"]
    ).unwrap();

    pub static ref STREAMING_CHUNKS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_streaming_chunks_total",
            "Total streaming output chunks"
        ),
        &["model", "version", "protocol"]
    ).unwrap();

    // S2:客户端中断的流(与 requests_total 2xx 并存——服务器确实处理了,
    // 由独立 counter 承担区分责任,D1)。门控随 features.streaming_metrics(D9)。
    pub static ref STREAM_CANCELLED_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_stream_cancelled_total",
            "Client-interrupted streaming connections (S2)"
        ),
        &["model", "version", "protocol"]
    ).unwrap();

    // S4:流错误/停滞计数(S4)。kind 是封闭枚举(worker_error|deadline|idle|
    // protocol|panic),由 StreamCloseReason::error_kind 映射;cancel/done/
    // worker_eof 不进。stream_kind 是 S5 的 6 值封闭枚举(sse|ws|http2|
    // grpc_stream|grpc_bidi|grpc_decoupled)——仅新增指标带,既有 protocol
    // label 值不改(D2)。门控随 streaming_metrics(D9)。
    pub static ref STREAM_ERRORS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_stream_errors_total",
            "Stream errors by kind (S4)"
        ),
        &["model", "version", "stream_kind", "kind"]
    ).unwrap();

    // S6:流时长(open→close)直方图;桶覆盖秒~分钟级流时长。
    pub static ref STREAM_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_stream_duration_seconds",
            "Stream open-to-close duration (S6)"
        ).buckets(vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]),
        &["model", "version", "stream_kind"]
    ).unwrap();

    // S6:流输出字节(Σ chunk.data.len())。
    pub static ref STREAM_OUTPUT_BYTES_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_stream_output_bytes_total",
            "Total stream output bytes (S6)"
        ),
        &["model", "version", "stream_kind"]
    ).unwrap();

    // Health check metrics
    pub static ref HEALTH_CHECK_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_health_check_total",
            "Total active health check probes"
        ),
        &["model", "version", "result"]
    ).unwrap();

    pub static ref WORKER_HEALTH_STATUS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_worker_health_status",
            "Worker health status (1=healthy, 0=ejected)"
        ),
        &["model", "version", "worker_id"]
    ).unwrap();

    // P6 GetModelStats.WorkerStats.inference_count — per-worker inference
    // dispatches. The worker_id label is precedent (WORKER_HEALTH_STATUS) and
    // was explicitly approved for this counter (蓝图 §6.5 约束 10 评审).
    pub static ref WORKER_INFERENCE_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_worker_inference_total",
            "Inferences dispatched per worker (P6 Admin GetModelStats)"
        ),
        &["model", "version", "worker_id"]
    ).unwrap();

    pub static ref WORKER_RESPAWNS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_worker_respawns_total",
            "Total worker respawns"
        ),
        &["model", "version", "reason"]
    ).unwrap();

    // Shutdown tracking
    pub static ref SHUTDOWN_PENDING_REQUESTS: prometheus::IntGauge = prometheus::IntGauge::new(
        "liteserver_shutdown_pending_requests",
        "Number of in-flight requests during shutdown"
    ).unwrap();

    // ===== P2-1 扩展: 扩缩一等指标（autoscaling 信号）=====
    // label 仅封闭枚举 model/version（§6.5 约束 10）。

    /// 已接受但未完成的请求数（per model/version），含排队中与处理中。
    pub static ref IN_FLIGHT_REQUESTS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_in_flight_requests",
            "Requests accepted but not yet completed (queued + processing)"
        ),
        &["model", "version"]
    ).unwrap();

    /// 排队等待时长（提交 → 首次派发，含攒批等待）。
    pub static ref QUEUE_WAIT_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_queue_wait_seconds",
            "Time a request waits in the queue before first dispatch"
        ).buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]),
        &["model", "version"]
    ).unwrap();

    /// HTTP request body size histogram (D11: Content-Type dispatch
    /// observability). `content_type` = "json" | "raw" | "triton_binary"
    /// (阶段 1);`route` = matched-path pattern. Buckets cover 1 KB to 256 MB
    /// so the distribution is visible for small JSON payloads through large
    /// tensor bodies.
    pub static ref HTTP_REQUEST_BODY_BYTES: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "lite_server_http_request_body_bytes",
            "HTTP request body size in bytes by content-type and route"
        ).buckets(vec![
            1_024.0, 4_096.0, 16_384.0, 65_536.0, 262_144.0,
            1_048_576.0, 4_194_304.0, 16_777_216.0, 67_108_864.0,
            268_435_456.0,
        ]),
        &["content_type", "route"]
    ).unwrap();

    /// Worker 饱和度：各 worker 并发 in-flight batch 数的最大值（最热
    /// worker）。label 白名单不含 worker_id，故以聚合 gauge 呈现；
    /// autoscaler 结合 liteserver_active_workers 解读。
    pub static ref WORKER_SATURATION: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_worker_saturation",
            "Max concurrent in-flight batches across workers (hottest worker)"
        ),
        &["model", "version"]
    ).unwrap();
}

// Custom metrics reported by Python workers — std::sync::Mutex is sufficient
// since the critical section is a brief HashMap lookup/insert (no async needed).
lazy_static! {
    static ref CUSTOM_COUNTERS: std::sync::Mutex<HashMap<String, CounterVec>> = std::sync::Mutex::new(HashMap::new());
    static ref CUSTOM_GAUGES: std::sync::Mutex<HashMap<String, GaugeVec>> = std::sync::Mutex::new(HashMap::new());
    static ref CUSTOM_HISTOGRAMS: std::sync::Mutex<HashMap<String, HistogramVec>> = std::sync::Mutex::new(HashMap::new());
}

// Pre-allocated custom metric objects, indexed by numeric ID (from register_metric).
// Vec is append-only; registration is idempotent (duplicate specs reuse existing index).
lazy_static! {
    static ref CUSTOM_GAUGE_OBJECTS: std::sync::Mutex<Vec<GaugeVec>> = std::sync::Mutex::new(Vec::new());
    static ref CUSTOM_COUNTER_OBJECTS: std::sync::Mutex<Vec<CounterVec>> = std::sync::Mutex::new(Vec::new());
    static ref CUSTOM_HISTOGRAM_OBJECTS: std::sync::Mutex<Vec<HistogramVec>> = std::sync::Mutex::new(Vec::new());
    static ref CUSTOM_METRIC_INDEX: std::sync::Mutex<HashMap<String, (String, usize)>> = std::sync::Mutex::new(HashMap::new());
}

pub fn register_metrics() -> Result<(), prometheus::Error> {
    REGISTRY.register(Box::new(REQUESTS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(REQUEST_DURATION.clone()))?;
    REGISTRY.register(Box::new(QUEUE_DEPTH.clone()))?;
    REGISTRY.register(Box::new(MODEL_LOAD_TOTAL.clone()))?;
    REGISTRY.register(Box::new(VERSION_SWITCHES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(ACTIVE_WORKERS.clone()))?;
    REGISTRY.register(Box::new(VERSION_WEIGHT.clone()))?;
    REGISTRY.register(Box::new(MODEL_READY.clone()))?;
    REGISTRY.register(Box::new(ENSEMBLE_STEP_LATENCY.clone()))?;
    REGISTRY.register(Box::new(WORKER_EJECTIONS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(RETRIES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(STREAMING_CONNECTIONS.clone()))?;
    REGISTRY.register(Box::new(STREAMING_TTFT.clone()))?;
    REGISTRY.register(Box::new(STREAMING_TBT.clone()))?;
    REGISTRY.register(Box::new(STREAMING_CHUNKS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(STREAM_CANCELLED_TOTAL.clone()))?;
    REGISTRY.register(Box::new(STREAM_ERRORS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(STREAM_DURATION_SECONDS.clone()))?;
    REGISTRY.register(Box::new(STREAM_OUTPUT_BYTES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(INFERENCE_DURATION.clone()))?;
    REGISTRY.register(Box::new(BATCH_SIZE.clone()))?;
    REGISTRY.register(Box::new(HEALTH_CHECK_TOTAL.clone()))?;
    REGISTRY.register(Box::new(WORKER_HEALTH_STATUS.clone()))?;
    REGISTRY.register(Box::new(WORKER_INFERENCE_TOTAL.clone()))?;
    REGISTRY.register(Box::new(WORKER_RESPAWNS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(SHUTDOWN_PENDING_REQUESTS.clone()))?;
    REGISTRY.register(Box::new(IN_FLIGHT_REQUESTS.clone()))?;
    REGISTRY.register(Box::new(QUEUE_WAIT_SECONDS.clone()))?;
    REGISTRY.register(Box::new(WORKER_SATURATION.clone()))?;
    REGISTRY.register(Box::new(HTTP_REQUEST_BODY_BYTES.clone()))?;
    Ok(())
}

// ===== GIE/EPP 指标语义对齐（P2-1 扩展, D32）=====
//
// `/metrics` 暴露语义对齐的 TotalQueuedRequests 与 KVCacheUtilization gauge，
// 命名经 `metrics.metric_namespace` 可配置兼容 `vllm:*` 模式（vLLM 指标名
// 2025–2026 发生过重命名，兼容层不硬编码 vllm 名——后缀是本服务的稳定语义
// 名，namespace 是部署侧适配 EPP 的杠杆）。
//
// TotalQueuedRequests 映射既有 liteserver_queue_depth：在 inc/dec_queue_depth
// 内同步镜像（构造上即同步，无双写漂移）。KVCacheUtilization 无 KV 概念，
// 上报 N/A (NaN)。
lazy_static! {
    /// 已注册的 GIE TotalQueuedRequests gauge（namespace → vec）。生产只有
    /// 一个 namespace（启动注册一次）；测试可注册多个——镜像写入全部，
    /// 避免测试间注册顺序竞争。
    static ref GIE_TOTAL_QUEUED_REQUESTS: std::sync::RwLock<std::collections::HashMap<String, GaugeVec>> =
        std::sync::RwLock::new(std::collections::HashMap::new());
}

/// Prometheus metric name 的合法 namespace 段（`[a-zA-Z_:][a-zA-Z0-9_:]*`）。
fn is_valid_metric_namespace(ns: &str) -> bool {
    let mut chars = ns.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// 注册 GIE/EPP 语义 gauge：`{namespace}:total_queued_requests{model,version}`
/// 与 `{namespace}:kv_cache_utilization`（NaN）。server 启动时调用一次；
/// 重复注册同 namespace 幂等。非法 namespace 报错（启动配置快速失败）。
pub fn register_gie_metrics(namespace: &str) -> Result<(), prometheus::Error> {
    if !is_valid_metric_namespace(namespace) {
        return Err(prometheus::Error::Msg(format!(
            "metrics.metric_namespace '{}' is not a valid Prometheus name segment \
             ([a-zA-Z_:][a-zA-Z0-9_:]*)",
            namespace
        )));
    }
    let mut guard = GIE_TOTAL_QUEUED_REQUESTS
        .write()
        .unwrap_or_else(|e| e.into_inner());
    if guard.contains_key(namespace) {
        return Ok(()); // 同 namespace 重复注册幂等
    }
    let queued = GaugeVec::new(
        prometheus::Opts::new(
            format!("{}:total_queued_requests", namespace),
            "GIE/EPP TotalQueuedRequests: requests waiting in queue (mirrors liteserver_queue_depth)",
        ),
        &["model", "version"],
    )?;
    REGISTRY.register(Box::new(queued.clone()))?;
    let kv = prometheus::Gauge::new(
        format!("{}:kv_cache_utilization", namespace),
        "GIE/EPP KVCacheUtilization: NaN — lite-server has no KV-cache concept (N/A)",
    )?;
    kv.set(f64::NAN);
    REGISTRY.register(Box::new(kv))?;
    guard.insert(namespace.to_string(), queued);
    Ok(())
}

pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = String::new();
    encoder.encode_utf8(&metric_families, &mut buffer).unwrap();
    buffer
}

// ===== Request metrics =====

pub fn record_request_start(_model: &str, _version: &str) {
    // liteserver uses explicit inc_queue_depth in queue manager
    // Kept for backward compat during transition
}

pub fn record_request_end(model: &str, version: &str, status: &str, duration_secs: f64) {
    REQUESTS_TOTAL.with_label_values(&[model, version, status]).inc();
    REQUEST_DURATION.with_label_values(&[model, version]).observe(duration_secs);
    super::aggregator::TIMELINE.record_latency(model, version, duration_secs);
    // P-TRACE C4: OTel metrics overlay (no-op unless telemetry.metrics_enabled +
    // feature). Exemplar-ready plumbing; opentelemetry_sdk 0.30 stubs exemplars.
    crate::telemetry::record_request_duration(status, duration_secs);
}

/// Record HTTP request body size with content-type and route labels (D11).
pub fn record_request_body_bytes(content_type: &str, route: &str, size_bytes: usize) {
    HTTP_REQUEST_BODY_BYTES
        .with_label_values(&[content_type, route])
        .observe(size_bytes as f64);
}

// ===== Outlier detection metrics =====

pub fn inc_worker_ejection(model: &str, version: &str) {
    WORKER_EJECTIONS_TOTAL.with_label_values(&[model, version]).inc();
}

pub fn inc_retry(model: &str, version: &str) {
    RETRIES_TOTAL.with_label_values(&[model, version]).inc();
}

// ===== Queue metrics =====

pub fn inc_queue_depth(model: &str, version: &str) {
    QUEUE_DEPTH.with_label_values(&[model, version]).inc();
    mirror_gie_queued(model, version, 1.0);
}

pub fn dec_queue_depth(model: &str, version: &str) {
    QUEUE_DEPTH.with_label_values(&[model, version]).dec();
    mirror_gie_queued(model, version, -1.0);
}

/// GIE TotalQueuedRequests 镜像（构造上与 liteserver_queue_depth 同步，无双写
/// 漂移）；register_gie_metrics 之前调用为 no-op。
fn mirror_gie_queued(model: &str, version: &str, delta: f64) {
    let guard = GIE_TOTAL_QUEUED_REQUESTS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    for g in guard.values() {
        g.with_label_values(&[model, version]).add(delta);
    }
}

// ===== 扩缩一等指标（P2-1 扩展）=====

/// 排队等待时长采样（提交 → 首次派发）。
pub fn observe_queue_wait(model: &str, version: &str, secs: f64) {
    QUEUE_WAIT_SECONDS.with_label_values(&[model, version]).observe(secs);
}

/// Worker 饱和度（最热 worker 的并发 in-flight batch 数）。
pub fn set_worker_saturation(model: &str, version: &str, value: f64) {
    WORKER_SATURATION.with_label_values(&[model, version]).set(value);
}

/// 已接受未完成请求数（in_flight_requests）增减。
pub fn inc_in_flight(model: &str, version: &str) {
    IN_FLIGHT_REQUESTS.with_label_values(&[model, version]).inc();
}

pub fn dec_in_flight(model: &str, version: &str) {
    IN_FLIGHT_REQUESTS.with_label_values(&[model, version]).dec();
}

// ===== Model metrics =====

pub fn record_model_load(model: &str, version: &str, success: bool) {
    let status = if success { "success" } else { "fail" };
    MODEL_LOAD_TOTAL.with_label_values(&[model, version, "load", status]).inc();
}

pub fn record_model_unload(model: &str, version: &str) {
    MODEL_LOAD_TOTAL.with_label_values(&[model, version, "unload", "success"]).inc();
}

pub fn set_active_workers(model: &str, version: &str, count: f64) {
    ACTIVE_WORKERS.with_label_values(&[model, version]).set(count);
}

pub fn set_version_weight(model: &str, version: &str, weight: f64) {
    VERSION_WEIGHT.with_label_values(&[model, version]).set(weight);
}

pub fn remove_version_weight(model: &str, version: &str) {
    let _ = VERSION_WEIGHT.remove_label_values(&[model, version]);
}

/// P-WARM (§4.3): set per-version readiness (1 = serving Ready/Degraded,
/// 0 = not serving, incl. WarmingUp/Failed). Called from the health sync on
/// every status transition so the gauge tracks the live state machine.
pub fn set_model_ready(model: &str, version: &str, ready: bool) {
    MODEL_READY
        .with_label_values(&[model, version])
        .set(if ready { 1.0 } else { 0.0 });
}

/// P-WARM: drop the readiness gauge label when a version is unloaded.
pub fn remove_model_ready(model: &str, version: &str) {
    let _ = MODEL_READY.remove_label_values(&[model, version]);
}

pub fn record_version_switch(model: &str, from: &str, to: &str) {
    VERSION_SWITCHES_TOTAL.with_label_values(&[model, from, to]).inc();
}

// ===== Ensemble metrics =====

pub fn record_ensemble_step_latency(ensemble: &str, step: &str, model: &str, version: &str, latency_secs: f64) {
    ENSEMBLE_STEP_LATENCY.with_label_values(&[ensemble, step, model, version]).observe(latency_secs);
}

// ===== Streaming metrics =====

/// `stream_id`/`decoupled` 仅用于 G5 生命周期日志(metrics label 不含)。
/// 门控继承:调用点都在 `if stream_metrics` 内。
pub fn record_stream_open(model: &str, version: &str, protocol: &str, stream_id: &str, decoupled: bool) {
    STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).inc();
    // G5:流 open 生命周期事件。
    tracing::info!(
        model,
        version,
        protocol,
        stream_id,
        decoupled,
        "stream opened"
    );
}

pub fn record_stream_chunk(model: &str, version: &str, protocol: &str) {
    STREAMING_CHUNKS_TOTAL.with_label_values(&[model, version, protocol]).inc();
    // G2:OTel 流式镜像(双重门控:调用点在 streaming_metrics 内,OTel 侧
    // metrics_enabled 关时 meter 为 no-op)。
    crate::telemetry::record_stream_chunks(protocol, 1);
}

pub fn record_stream_close(model: &str, version: &str, protocol: &str) {
    STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).dec();
}

// ===== S1/S2 (批次 1):流收口统一记录 =====

/// 流关闭收口枚举(评审优化):各"静默 break"点**只置 reason**,close 收口统一
/// 消费。family 映射 + cancelled(S2)/errors(S4,批次 3)/duration/bytes(S6,
/// 批次 3)单一来源;G5 close 日志的 reason 同源。HTTP 路径 family 由
/// [`StreamCloseReason::status_family`] 派生;gRPC 的 Error 帧按其 grpc code
/// 映射覆盖(既有语义,不在此枚举内)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamCloseReason {
    /// 正常 Done 帧完成。
    Done,
    /// Error 帧 / worker 报错。
    Error,
    /// 客户端中断(下游 send 失败 / 断开 / decoupled cancel 帧)。
    Cancel,
    /// 整体 deadline 超时(RecvElapsed::Deadline)。
    Deadline,
    /// chunk idle 超时(RecvElapsed::Idle)。
    Idle,
    /// worker 无 Done 直接 EOF。
    WorkerEof,
    /// WS 协议违规(decoupled 数据帧 / 未知控制帧)。
    Protocol,
    /// writer 任务 panic(仅 WS 可达)。
    Panic,
}

impl StreamCloseReason {
    /// D7 family 映射:done/worker_eof/cancel→2xx;error/deadline/idle/panic→5xx;
    /// protocol→4xx。gRPC 的 Error 帧除外(按其 grpc code 映射)。
    pub fn status_family(self) -> &'static str {
        match self {
            StreamCloseReason::Done
            | StreamCloseReason::WorkerEof
            | StreamCloseReason::Cancel => "2xx",
            StreamCloseReason::Error
            | StreamCloseReason::Deadline
            | StreamCloseReason::Idle
            | StreamCloseReason::Panic => "5xx",
            StreamCloseReason::Protocol => "4xx",
        }
    }

    /// S4:errors kind 映射——error→worker_error、deadline→deadline、
    /// idle→idle、protocol→protocol、panic→panic(仅 WS 可达);
    /// cancel/done/worker_eof 不进 errors(None)。
    pub fn error_kind(self) -> Option<&'static str> {
        match self {
            StreamCloseReason::Error => Some("worker_error"),
            StreamCloseReason::Deadline => Some("deadline"),
            StreamCloseReason::Idle => Some("idle"),
            StreamCloseReason::Protocol => Some("protocol"),
            StreamCloseReason::Panic => Some("panic"),
            _ => None,
        }
    }
}

/// 流 close 收口(S1/S2/S4/S6):无条件 `record_request_end`(不门控);门控内
/// (streaming_metrics)记 cancelled(S2)、errors(S4)、duration/bytes(S6)与
/// `record_stream_close`。
/// `protocol` 是 cancelled 的 label(批次 1,既有 protocol 值);`stream_kind`
/// 是 S5 的 6 值封闭枚举,供 errors/duration/bytes 使用(D2:既有 protocol
/// label 值不改)。`output_bytes` 是流内 Σ chunk.data.len()(chunk 处累加)。
/// `chunks` 是流内 chunk 数(同处累加,仅 G5 close 日志字段,非 metric label;
/// WS writer panic 臂传 0——panic 时不可知,与 output_bytes 同)。
/// `family` 由调用方传:HTTP 用 `reason.status_family()`,gRPC 的 Error 帧
/// 按 grpc code 映射覆盖(既有语义)。exactly-once 由任务尾部单一收口保证。
#[allow(clippy::too_many_arguments)] // metrics 收口 API:参数即 label/事件字段,struct 化改全部调用点
pub fn record_stream_terminal(
    model: &str,
    version: &str,
    protocol: &str,
    stream_kind: &str,
    open_time: std::time::Instant,
    family: &str,
    reason: StreamCloseReason,
    streaming_metrics: bool,
    output_bytes: u64,
    chunks: u64,
) {
    let duration_secs = open_time.elapsed().as_secs_f64();
    record_request_end(model, version, family, duration_secs);
    // G5:流 close 生命周期事件(reason 即收口枚举,单一来源);error/cancel
    // 降级 warn。不门控——排查通道,metrics 关时仍可见。
    tracing::info!(
        model,
        version,
        protocol,
        stream_kind,
        reason = ?reason,
        duration_secs,
        chunks,
        output_bytes,
        "stream closed"
    );
    if let Some(kind) = reason.error_kind() {
        tracing::warn!(
            model, version, stream_kind, kind,
            "stream ended with error"
        );
    } else if reason == StreamCloseReason::Cancel {
        tracing::warn!(
            model, version, protocol,
            "stream cancelled by client"
        );
    }
    if streaming_metrics {
        if reason == StreamCloseReason::Cancel {
            STREAM_CANCELLED_TOTAL
                .with_label_values(&[model, version, protocol])
                .inc();
        }
        if let Some(kind) = reason.error_kind() {
            STREAM_ERRORS_TOTAL
                .with_label_values(&[model, version, stream_kind, kind])
                .inc();
        }
        STREAM_DURATION_SECONDS
            .with_label_values(&[model, version, stream_kind])
            .observe(duration_secs);
        // G2:OTel 流式镜像(双重门控:streaming_metrics ∩ telemetry.metrics_enabled)。
        crate::telemetry::record_stream_duration(stream_kind, duration_secs);
        if output_bytes > 0 {
            STREAM_OUTPUT_BYTES_TOTAL
                .with_label_values(&[model, version, stream_kind])
                .inc_by(output_bytes as f64);
        }
        record_stream_close(model, version, protocol);
    }
}

/// S1(b)/D7:open 前的早期拒绝也计一次请求(对齐 gRPC wrapper 双点语义)。
/// family 由调用方按 `AppError::http_status` 映射;version 未解析时传请求
/// 原值(可为空串)。与 close 收口互斥——open 失败不会抵达收口点,无重复计数。
pub fn record_stream_rejected(model: &str, version: &str, family: &str, duration_secs: f64) {
    record_request_end(model, version, family, duration_secs);
}

// Pre-defined worker metrics (liteserver compatible)
lazy_static! {
    pub static ref INFERENCE_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_inference_duration_seconds",
            "Worker round-trip latency (queue dispatch → response)"
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["model", "version"]
    ).unwrap();

    pub static ref BATCH_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_batch_size",
            "Actual batch size processed"
        ).buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0]),
        &["model", "version"]
    ).unwrap();
}

pub fn observe_inference_duration(model: &str, version: &str, secs: f64) {
    INFERENCE_DURATION.with_label_values(&[model, version]).observe(secs);
}

pub fn observe_batch_size(model: &str, version: &str, size: usize) {
    BATCH_SIZE.with_label_values(&[model, version]).observe(size as f64);
}

// ===== Custom metrics from Python workers =====

/// Record metrics reported by a Python worker, tagged by the serving
/// (model, version) of the response (§4.6).
pub fn record_worker_metrics(model: &str, version: &str, metrics: Option<&crate::proto::liteserver::Metrics>) {
    if let Some(m) = metrics {
        if m.prefill_ms > 0.0 {
            let mut guard = CUSTOM_GAUGES.lock().unwrap_or_else(|e| e.into_inner());
            let gauge = guard.entry("prefill_ms".to_string()).or_insert_with(|| {
                let g = GaugeVec::new(
                    prometheus::Opts::new("lite_server_prefill_ms", "Prefill latency in ms"),
                    &["model", "version"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(g.clone()));
                g
            });
            gauge.with_label_values(&[model, version]).set(m.prefill_ms as f64);
        }
        if m.decode_ms > 0.0 {
            let mut guard = CUSTOM_GAUGES.lock().unwrap_or_else(|e| e.into_inner());
            let gauge = guard.entry("decode_ms".to_string()).or_insert_with(|| {
                let g = GaugeVec::new(
                    prometheus::Opts::new("lite_server_decode_ms", "Decode latency in ms"),
                    &["model", "version"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(g.clone()));
                g
            });
            gauge.with_label_values(&[model, version]).set(m.decode_ms as f64);
        }
        if m.tokens_generated > 0 {
            let mut guard = CUSTOM_COUNTERS.lock().unwrap_or_else(|e| e.into_inner());
            let counter = guard.entry("tokens_generated".to_string()).or_insert_with(|| {
                let c = CounterVec::new(
                    prometheus::Opts::new("lite_server_tokens_generated_total", "Total tokens generated"),
                    &["model", "version"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(c.clone()));
                c
            });
            counter.with_label_values(&[model, version]).inc_by(m.tokens_generated as f64);
        }
        // Pre-registered custom metrics (numeric ID path)
        record_custom_metrics(model, version, &m.gauges, &m.counters, &m.histograms);
    }
}

// ===== Pre-registered custom metrics (numeric ID path) =====

/// Pre-register custom metric objects during model setup.
/// Each spec is (name, type) where type is "gauge", "counter", or "histogram".
/// Objects are stored in order — the index IS the numeric ID used at record time.
pub fn register_custom_metrics(specs: &[(&str, &str)]) {
    let mut gauges = CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut counters = CUSTOM_COUNTER_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut histograms = CUSTOM_HISTOGRAM_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut index = CUSTOM_METRIC_INDEX.lock().unwrap_or_else(|e| e.into_inner());

    for (name, metric_type) in specs {
        let key = format!("{}:{}", name, metric_type);
        if index.contains_key(&key) {
            continue; // already registered — idempotent
        }
        match *metric_type {
            "gauge" => {
                let idx = gauges.len();
                let g = GaugeVec::new(
                    prometheus::Opts::new(
                        format!("lite_server_{}", name),
                        format!("Custom gauge: {}", name),
                    ),
                    &["model", "version"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(g.clone()));
                gauges.push(g);
                index.insert(key, ("gauge".to_string(), idx));
            }
            "counter" => {
                let idx = counters.len();
                let c = CounterVec::new(
                    prometheus::Opts::new(
                        format!("lite_server_{}_total", name),
                        format!("Custom counter: {}", name),
                    ),
                    &["model", "version"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(c.clone()));
                counters.push(c);
                index.insert(key, ("counter".to_string(), idx));
            }
            "histogram" => {
                let idx = histograms.len();
                let h = HistogramVec::new(
                    HistogramOpts::new(
                        format!("lite_server_{}", name),
                        format!("Custom histogram: {}", name),
                    )
                    .buckets(vec![
                        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    ]),
                    &["model", "version"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(h.clone()));
                histograms.push(h);
                index.insert(key, ("histogram".to_string(), idx));
            }
            _ => {}
        }
    }
}

/// Record pre-registered custom metrics — hot path, O(1) per metric, no HashMap.
pub fn record_custom_metrics(
    model: &str,
    version: &str,
    gauges: &[crate::proto::liteserver::MetricValue],
    counters: &[crate::proto::liteserver::MetricValue],
    histograms: &[crate::proto::liteserver::MetricValue],
) {
    if !gauges.is_empty() {
        let guard = CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
        for mv in gauges {
            if let Some(g) = guard.get(mv.id as usize) {
                g.with_label_values(&[model, version]).set(mv.value as f64);
            }
        }
    }
    if !counters.is_empty() {
        let guard = CUSTOM_COUNTER_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
        for mv in counters {
            if let Some(c) = guard.get(mv.id as usize) {
                c.with_label_values(&[model, version]).inc_by(mv.value as f64);
            }
        }
    }
    if !histograms.is_empty() {
        let guard = CUSTOM_HISTOGRAM_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
        for mv in histograms {
            if let Some(h) = guard.get(mv.id as usize) {
                h.with_label_values(&[model, version]).observe(mv.value as f64);
            }
        }
    }
}

// ===== Streaming metric recording functions =====

pub fn record_stream_ttft(model: &str, version: &str, protocol: &str, ttft_secs: f64) {
    STREAMING_TTFT.with_label_values(&[model, version, protocol]).observe(ttft_secs);
    // G2:OTel 镜像。
    crate::telemetry::record_stream_ttft(protocol, ttft_secs);
}

pub fn record_stream_tbt(model: &str, version: &str, protocol: &str, tbt_secs: f64) {
    STREAMING_TBT.with_label_values(&[model, version, protocol]).observe(tbt_secs);
    // G2:OTel 镜像。
    crate::telemetry::record_stream_tbt(protocol, tbt_secs);
}

// ===== Health check metric recording functions =====

pub fn inc_health_check(model: &str, version: &str, result: &str) {
    HEALTH_CHECK_TOTAL.with_label_values(&[model, version, result]).inc();
}

pub fn set_worker_health(model: &str, version: &str, worker_id: usize, healthy: bool) {
    let id_str = worker_id.to_string();
    WORKER_HEALTH_STATUS.with_label_values(&[model, version, &id_str]).set(if healthy { 1.0 } else { 0.0 });
}

/// Per-worker inference dispatch count (P6 GetModelStats). Incremented at every
/// dispatch site: the queue's batch dispatch (unary infer) and the gRPC direct
/// paths (batch/stream/bidi). `count` = logical inferences in the dispatch
/// (batch size for batched sends, 1 for a stream/bidi open).
pub fn record_worker_inference(model: &str, version: &str, worker_id: usize, count: usize) {
    let id_str = worker_id.to_string();
    WORKER_INFERENCE_TOTAL
        .with_label_values(&[model, version, &id_str])
        .inc_by(count as f64);
}

/// Read the per-worker inference count for GetModelStats (0 when unseen).
pub fn worker_inference_count(model: &str, version: &str, worker_id: usize) -> u64 {
    let id_str = worker_id.to_string();
    WORKER_INFERENCE_TOTAL
        .with_label_values(&[model, version, &id_str])
        .get() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_stream_ttft_records_value() {
        let model = "ttft_model";
        let version = "1";
        let protocol = "sse";
        let before = STREAMING_TTFT.with_label_values(&[model, version, protocol]).get_sample_count();
        record_stream_ttft(model, version, protocol, 0.05);
        let after = STREAMING_TTFT.with_label_values(&[model, version, protocol]).get_sample_count();
        assert_eq!(after, before + 1);
    }

    #[test]
    fn test_record_stream_tbt_records_value() {
        let model = "tbt_model";
        let version = "1";
        let protocol = "websocket";
        let before = STREAMING_TBT.with_label_values(&[model, version, protocol]).get_sample_count();
        record_stream_tbt(model, version, protocol, 0.01);
        let after = STREAMING_TBT.with_label_values(&[model, version, protocol]).get_sample_count();
        assert_eq!(after, before + 1);
    }

    #[test]
    fn test_record_stream_open_increments_connections() {
        let model = "conn_model";
        let version = "1";
        let protocol = "sse";
        let before = STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).get();
        record_stream_open(model, version, protocol, "test-stream", false);
        let after = STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).get();
        assert_eq!(after, before + 1.0);
        // cleanup
        record_stream_close(model, version, protocol);
    }

    #[test]
    fn test_record_stream_close_decrements_connections() {
        let model = "close_model";
        let version = "1";
        let protocol = "sse";
        record_stream_open(model, version, protocol, "test-stream", false);
        record_stream_close(model, version, protocol);
        // net zero — gauge should be back to its value before open
        // (other tests may have incremented, so just check relative)
    }

    #[test]
    fn test_record_stream_chunk_increments_total() {
        let model = "chunk_model";
        let version = "1";
        let protocol = "grpc";
        let before = STREAMING_CHUNKS_TOTAL.with_label_values(&[model, version, protocol]).get();
        record_stream_chunk(model, version, protocol);
        let after = STREAMING_CHUNKS_TOTAL.with_label_values(&[model, version, protocol]).get();
        assert_eq!(after, before + 1.0);
    }

    #[test]
    fn test_streaming_metrics_distinguished_by_protocol() {
        record_stream_open("proto_m", "1", "sse", "test-s", false);
        record_stream_open("proto_m", "1", "websocket", "test-w", false);
        record_stream_open("proto_m", "1", "grpc", "test-g", false);

        let sse = STREAMING_CONNECTIONS.with_label_values(&["proto_m", "1", "sse"]).get();
        let ws = STREAMING_CONNECTIONS.with_label_values(&["proto_m", "1", "websocket"]).get();
        let grpc = STREAMING_CONNECTIONS.with_label_values(&["proto_m", "1", "grpc"]).get();

        assert!(sse >= 1.0);
        assert!(ws >= 1.0);
        assert!(grpc >= 1.0);

        // cleanup
        record_stream_close("proto_m", "1", "sse");
        record_stream_close("proto_m", "1", "websocket");
        record_stream_close("proto_m", "1", "grpc");
    }

    #[test]
    fn test_record_worker_metrics_sync_no_await() {
        use crate::proto::liteserver::Metrics;
        let metrics = Metrics {
            prefill_ms: 10.0,
            decode_ms: 5.0,
            tokens_generated: 100,
            gauges: vec![],
            counters: vec![],
            histograms: vec![],
        };
        let m = Some(&metrics);

        // Must compile and run without .await — proves std::sync::Mutex not tokio::sync::Mutex
        record_worker_metrics("sync_m1", "1", m);
        record_worker_metrics("sync_m2", "1", m);
        record_worker_metrics("sync_m3", "1", m);
    }

    #[test]
    fn test_register_custom_metrics_and_record() {
        use crate::proto::liteserver::MetricValue;

        register_custom_metrics(&[
            ("rcmr_gauge", "gauge"),
            ("rcmr_counter", "counter"),
            ("rcmr_histogram", "histogram"),
        ]);

        // Look up the IDs assigned by register_custom_metrics
        let index = CUSTOM_METRIC_INDEX.lock().unwrap();
        let gauge_id = index.get("rcmr_gauge:gauge").unwrap().1;
        let counter_id = index.get("rcmr_counter:counter").unwrap().1;
        let histogram_id = index.get("rcmr_histogram:histogram").unwrap().1;
        drop(index);

        let gauges = vec![MetricValue { id: gauge_id as i32, value: 42.0 }];
        let counters = vec![MetricValue { id: counter_id as i32, value: 5.0 }];
        let histograms = vec![MetricValue { id: histogram_id as i32, value: 0.15 }];
        record_custom_metrics("rcmr_model", "1", &gauges, &counters, &histograms);

        let output = gather_metrics();
        assert!(output.contains("lite_server_rcmr_gauge"), "gauge missing: {}", output);
        assert!(output.contains("lite_server_rcmr_counter_total"), "counter missing: {}", output);
        assert!(output.contains("lite_server_rcmr_histogram"), "histogram missing: {}", output);
    }

    #[test]
    fn test_record_worker_metrics_with_custom_fields() {
        use crate::proto::liteserver::{MetricValue, Metrics};

        register_custom_metrics(&[("rwmc_gauge", "gauge")]);

        let index = CUSTOM_METRIC_INDEX.lock().unwrap();
        let gauge_id = index.get("rwmc_gauge:gauge").unwrap().1;
        drop(index);

        let metrics = Metrics {
            prefill_ms: 0.0,
            decode_ms: 0.0,
            tokens_generated: 0,
            gauges: vec![MetricValue { id: gauge_id as i32, value: 99.5 }],
            counters: vec![],
            histograms: vec![],
        };
        record_worker_metrics("rwmc_model", "1", Some(&metrics));

        let output = gather_metrics();
        assert!(output.contains("lite_server_rwmc_gauge"), "custom gauge missing: {}", output);
    }

    #[test]
    fn test_inc_queue_depth_increments_gauge() {
        let model = "qd_inc_model";
        let version = "1";
        let before = QUEUE_DEPTH.with_label_values(&[model, version]).get();
        inc_queue_depth(model, version);
        let after = QUEUE_DEPTH.with_label_values(&[model, version]).get();
        assert_eq!(after, before + 1.0, "inc_queue_depth should increment gauge by 1");
    }

    #[test]
    fn test_dec_queue_depth_decrements_gauge() {
        let model = "qd_dec_model";
        let version = "1";
        // First inc so we start from a known positive value
        inc_queue_depth(model, version);
        let before = QUEUE_DEPTH.with_label_values(&[model, version]).get();
        dec_queue_depth(model, version);
        let after = QUEUE_DEPTH.with_label_values(&[model, version]).get();
        assert_eq!(after, before - 1.0, "dec_queue_depth should decrement gauge by 1");
    }

    #[test]
    fn test_inc_dec_queue_depth_net_zero() {
        let model = "qd_balance";
        let version = "1";
        inc_queue_depth(model, version);
        inc_queue_depth(model, version);
        inc_queue_depth(model, version);
        dec_queue_depth(model, version);
        dec_queue_depth(model, version);
        dec_queue_depth(model, version);
        let value = QUEUE_DEPTH.with_label_values(&[model, version]).get();
        // With equal inc and dec, the gauge should have returned to its original
        // value (0 for a fresh label set in this test binary).
        assert_eq!(value, 0.0, "balanced inc/dec should net to zero, got {}", value);
    }

    #[test]
    fn test_queue_depth_goes_negative_on_extra_dec() {
        // This test documents the bug: calling dec more times than inc
        // causes the gauge to go negative. The fix ensures inc/dec are
        // always called symmetrically in inference_queue.rs.
        let model = "qd_negative";
        let version = "1";
        inc_queue_depth(model, version); // +1
        dec_queue_depth(model, version); // 0
        dec_queue_depth(model, version); // -1 (extra dec, e.g. from retry)
        let value = QUEUE_DEPTH.with_label_values(&[model, version]).get();
        assert!(value < 0.0, "extra dec should produce negative value, got {}", value);
    }

    // ===== Audit: B3 — mutex poisoning in record_worker_metrics =====

    /// `record_worker_metrics` recovers from a poisoned `CUSTOM_GAUGES` mutex
    /// via `unwrap_or_else(|e| e.into_inner())`, consistent with other
    /// subsystems (OutlierState, SizeRotatingAppender).
    ///
    /// Marked `#[ignore]` because poisoning global `lazy_static!` mutexes
    /// permanently corrupts the static and breaks subsequent tests.
    #[test]
    #[ignore = "run explicitly: poisons global static, breaks subsequent tests"]
    fn test_record_worker_metrics_survives_mutex_poison() {
        use crate::proto::liteserver::Metrics;

        // Poison the CUSTOM_GAUGES mutex
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = CUSTOM_GAUGES.lock().unwrap();
            panic!("intentional poison");
        }));

        let metrics = Metrics {
            prefill_ms: 10.0,
            decode_ms: 0.0,
            tokens_generated: 0,
            gauges: vec![],
            counters: vec![],
            histograms: vec![],
        };

        // Must NOT panic — the poisoned lock is recovered via into_inner().
        record_worker_metrics("poison_m", "1", Some(&metrics));
    }

    // ===== Audit: B1 — worker metrics recorded exactly once per request =====

    /// Regression test for the double-recording defect: `record_worker_metrics`
    /// used to be called from **two** call sites for every non-streaming
    /// inference request (`inference_queue::do_send_batch` and
    /// `http::handlers::infer_handler`), counting `tokens_generated` and all
    /// custom metrics 2×.  The `do_send_batch` call site has been removed;
    /// the HTTP handler is now the sole recording point.
    ///
    /// This test pins the single-call semantics: one call records 1× the value.
    #[test]
    fn test_record_worker_metrics_single_call_counts_once() {
        use crate::proto::liteserver::Metrics;

        let metrics = Metrics {
            prefill_ms: 0.0,
            decode_ms: 0.0,
            tokens_generated: 50,
            gauges: vec![],
            counters: vec![],
            histograms: vec![],
        };

        let model = "b1_single_m";
        let version = "1";
        let before = CUSTOM_COUNTERS.lock().unwrap_or_else(|e| e.into_inner())
            .get("tokens_generated")
            .map(|c| c.with_label_values(&[model, version]).get())
            .unwrap_or(0.0);

        record_worker_metrics(model, version, Some(&metrics));

        let after = CUSTOM_COUNTERS.lock().unwrap_or_else(|e| e.into_inner())
            .get("tokens_generated")
            .map(|c| c.with_label_values(&[model, version]).get())
            .unwrap_or(0.0);

        let delta = after - before;
        assert_eq!(delta, 50.0,
            "single call should record 1x tokens_generated, got {}", delta);
    }

    // ===== Audit: B4 — record_request_end is a plain synchronous fn =====

    /// `record_request_end` performs only synchronous operations
    /// (`REQUESTS_TOTAL.inc()`, `REQUEST_DURATION.observe()`, and
    /// `TIMELINE.record_latency()`), so it is a plain `fn` — no async
    /// runtime required.
    #[test]
    fn test_record_request_end_is_sync_not_async() {
        // Direct call from a sync test context: this only compiles and runs
        // because record_request_end is not async.
        record_request_end("b4_model", "1", "2xx", 0.05);
        assert!(REQUESTS_TOTAL.with_label_values(&["b4_model", "1", "2xx"]).get() >= 1.0);
    }

    /// `record_custom_metrics` recovers from poisoned `CUSTOM_*_OBJECTS`
    /// mutexes the same way as `record_worker_metrics`.
    /// Marked `#[ignore]` for the same reason as above.
    #[test]
    #[ignore = "run explicitly: poisons global static, breaks subsequent tests"]
    fn test_record_custom_metrics_survives_mutex_poison() {
        use crate::proto::liteserver::MetricValue;

        // Register so the vec is non-empty, forcing the locked path
        register_custom_metrics(&[("poison_cm", "gauge")]);
        let index = CUSTOM_METRIC_INDEX.lock().unwrap();
        let gauge_id = index.get("poison_cm:gauge").unwrap().1;
        drop(index);

        // Poison the CUSTOM_GAUGE_OBJECTS mutex
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = CUSTOM_GAUGE_OBJECTS.lock().unwrap();
            panic!("intentional poison");
        }));

        let gauges = vec![MetricValue { id: gauge_id as i32, value: 42.0 }];
        // Must NOT panic — the poisoned lock is recovered via into_inner().
        record_custom_metrics("poison_cm_m", "1", &gauges, &[], &[]);
    }

    // ===== P2-1 扩展: GIE/EPP 指标语义对齐 =====

    #[test]
    fn should_expose_total_queued_requests_under_configured_namespace() {
        register_gie_metrics("gie_p21a").unwrap();
        let model = "gie_q_model";
        let version = "1";
        inc_queue_depth(model, version);
        let output = gather_metrics();
        assert!(
            output.contains("gie_p21a:total_queued_requests{model=\"gie_q_model\",version=\"1\"} 1"),
            "GIE TotalQueuedRequests gauge missing or wrong value: {}", output
        );
    }

    #[test]
    fn should_mirror_queue_depth_inc_and_dec_into_gie_gauge() {
        register_gie_metrics("gie_p21b").unwrap();
        let model = "gie_m_model";
        let version = "1";
        inc_queue_depth(model, version);
        inc_queue_depth(model, version);
        dec_queue_depth(model, version);
        let guard = GIE_TOTAL_QUEUED_REQUESTS.read().unwrap_or_else(|e| e.into_inner());
        let gauge = guard.get("gie_p21b").expect("gie_p21b gauge registered");
        assert_eq!(gauge.with_label_values(&[model, version]).get(), 1.0);
    }

    #[test]
    fn should_expose_kv_cache_utilization_as_nan() {
        // lite-server 无 KV cache 概念——按 D32 上报 N/A (NaN)。
        register_gie_metrics("gie_p21c").unwrap();
        let output = gather_metrics();
        assert!(
            output.contains("gie_p21c:kv_cache_utilization NaN"),
            "KVCacheUtilization must be exposed as NaN (N/A): {}", output
        );
    }

    #[test]
    fn should_reject_invalid_metric_namespace() {
        assert!(register_gie_metrics("").is_err(), "empty namespace must be rejected");
        assert!(register_gie_metrics("9bad").is_err(), "leading digit must be rejected");
        assert!(register_gie_metrics("bad-name").is_err(), "dash must be rejected");
    }

    #[test]
    fn should_accept_liteserver_and_vllm_namespaces() {
        assert!(register_gie_metrics("liteserver_itself_ok").is_ok());
        assert!(register_gie_metrics("vllm").is_ok());
    }

    // ===== P2-1 扩展: 扩缩一等指标 =====

    #[test]
    fn should_record_in_flight_requests_gauge() {
        let model = "inflight_g_model";
        let version = "1";
        let gauge = IN_FLIGHT_REQUESTS.with_label_values(&[model, version]);
        let before = gauge.get();
        gauge.inc();
        assert_eq!(gauge.get(), before + 1.0);
        gauge.dec();
        assert_eq!(gauge.get(), before);
    }

    #[test]
    fn should_observe_queue_wait_seconds() {
        let model = "qwait_model";
        let version = "1";
        let before = QUEUE_WAIT_SECONDS.with_label_values(&[model, version]).get_sample_count();
        observe_queue_wait(model, version, 0.05);
        let after = QUEUE_WAIT_SECONDS.with_label_values(&[model, version]).get_sample_count();
        assert_eq!(after, before + 1);
    }

    #[test]
    fn should_set_worker_saturation() {
        let model = "sat_model";
        let version = "1";
        set_worker_saturation(model, version, 2.0);
        assert_eq!(WORKER_SATURATION.with_label_values(&[model, version]).get(), 2.0);
        set_worker_saturation(model, version, 0.0);
        assert_eq!(WORKER_SATURATION.with_label_values(&[model, version]).get(), 0.0);
    }

    // ===== S1/S2: StreamCloseReason 收口 + record_stream_terminal =====

    #[test]
    fn stream_close_reason_maps_status_family() {
        use StreamCloseReason::*;
        assert_eq!(Done.status_family(), "2xx");
        assert_eq!(WorkerEof.status_family(), "2xx");
        assert_eq!(Cancel.status_family(), "2xx");
        assert_eq!(Error.status_family(), "5xx");
        assert_eq!(Deadline.status_family(), "5xx");
        assert_eq!(Idle.status_family(), "5xx");
        assert_eq!(Panic.status_family(), "5xx");
        assert_eq!(Protocol.status_family(), "4xx");
    }

    /// S1 不门控:streaming_metrics=false 时 record_request_end 仍须记录。
    #[test]
    fn record_stream_terminal_records_request_end_ungated() {
        let model = "term_ungated";
        let version = "1";
        let before = REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).get();
        record_stream_terminal(
            model, version, "sse", "sse", std::time::Instant::now(),
            StreamCloseReason::Done.status_family(), StreamCloseReason::Done, false, 0, 0,
        );
        let after = REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).get();
        assert_eq!(
            after, before + 1.0,
            "S1: record_request_end must NOT be gated by streaming_metrics"
        );
    }

    /// S2/D1 门控开:cancel → cancelled +1,requests 保持 2xx,close 递减连接。
    #[test]
    fn record_stream_terminal_cancel_counts_when_gated() {
        let model = "term_cancel_on";
        let version = "1";
        record_stream_open(model, version, "sse", "test-s", false);
        let req_before = REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).get();
        let canc_before = STREAM_CANCELLED_TOTAL.with_label_values(&[model, version, "sse"]).get();
        let conn_before = STREAMING_CONNECTIONS.with_label_values(&[model, version, "sse"]).get();
        record_stream_terminal(
            model, version, "sse", "sse", std::time::Instant::now(),
            StreamCloseReason::Cancel.status_family(), StreamCloseReason::Cancel, true, 0, 0,
        );
        assert_eq!(
            STREAM_CANCELLED_TOTAL.with_label_values(&[model, version, "sse"]).get(),
            canc_before + 1.0,
            "S2: cancelled must count when gated on"
        );
        assert_eq!(
            REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).get(),
            req_before + 1.0,
            "D1: disconnected stream keeps 2xx family + separate cancel counter"
        );
        assert_eq!(
            STREAMING_CONNECTIONS.with_label_values(&[model, version, "sse"]).get(),
            conn_before - 1.0,
            "close must decrement connections"
        );
    }

    /// D9 门控关:cancelled 与 record_stream_close 都不记录,requests_total 仍记录。
    #[test]
    fn record_stream_terminal_cancel_not_counted_when_gated_off() {
        let model = "term_cancel_off";
        let version = "1";
        record_stream_open(model, version, "websocket", "test-w", false);
        let req_before = REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).get();
        let canc_before = STREAM_CANCELLED_TOTAL.with_label_values(&[model, version, "websocket"]).get();
        let conn_before = STREAMING_CONNECTIONS.with_label_values(&[model, version, "websocket"]).get();
        record_stream_terminal(
            model, version, "websocket", "ws", std::time::Instant::now(),
            StreamCloseReason::Cancel.status_family(), StreamCloseReason::Cancel, false, 0, 0,
        );
        assert_eq!(
            STREAM_CANCELLED_TOTAL.with_label_values(&[model, version, "websocket"]).get(),
            canc_before,
            "D9: cancelled gated by streaming_metrics"
        );
        assert_eq!(
            STREAMING_CONNECTIONS.with_label_values(&[model, version, "websocket"]).get(),
            conn_before,
            "record_stream_close stays gated (open was gated too)"
        );
        assert_eq!(
            REQUESTS_TOTAL.with_label_values(&[model, version, "2xx"]).get(),
            req_before + 1.0,
            "requests_total must still record with metrics off"
        );
    }

    /// D7:WS 协议违规 → 4xx family。
    #[test]
    fn record_stream_terminal_protocol_maps_4xx() {
        let model = "term_proto";
        let version = "1";
        let before = REQUESTS_TOTAL.with_label_values(&[model, version, "4xx"]).get();
        record_stream_terminal(
            model, version, "websocket", "ws", std::time::Instant::now(),
            StreamCloseReason::Protocol.status_family(), StreamCloseReason::Protocol, false, 0, 0,
        );
        assert_eq!(
            REQUESTS_TOTAL.with_label_values(&[model, version, "4xx"]).get(),
            before + 1.0,
            "WS protocol violation maps to 4xx (D7)"
        );
    }

    /// WS writer panic(仅 WS 可达)→ 5xx。
    #[test]
    fn record_stream_terminal_panic_maps_5xx() {
        let model = "term_panic";
        let version = "1";
        let before = REQUESTS_TOTAL.with_label_values(&[model, version, "5xx"]).get();
        record_stream_terminal(
            model, version, "websocket", "ws", std::time::Instant::now(),
            StreamCloseReason::Panic.status_family(), StreamCloseReason::Panic, false, 0, 0,
        );
        assert_eq!(
            REQUESTS_TOTAL.with_label_values(&[model, version, "5xx"]).get(),
            before + 1.0,
            "WS writer panic maps to 5xx"
        );
    }

    /// S7/D8:REQUEST_DURATION 桶追加 30/60/120——S1 落地后分钟级流时长经
    /// record_request_end 进入该 histogram,不能全落 +Inf(旧桶顶 10s)。
    #[test]
    fn request_duration_buckets_extend_to_minute_streams() {
        // gather 走 REGISTRY——先注册(AlreadyReg 忽略,aggregator 测试同款
        // 先例);否则单独/子集运行时 family 缺失(B4 修复)。
        let _ = register_metrics();
        let model = "dur_minute_m";
        let version = "1";
        record_request_end(model, version, "2xx", 15.0);
        let families = REGISTRY.gather();
        let family = families
            .iter()
            .find(|mf| mf.get_name() == "liteserver_request_duration_seconds")
            .expect("REQUEST_DURATION must be registered");
        let mut found = false;
        for m in family.get_metric() {
            if !m.get_label()
                .iter()
                .any(|l| l.get_name() == "model" && l.get_value() == model)
            {
                continue;
            }
            found = true;
            let hist = m.get_histogram();
            let le30 = hist
                .get_bucket()
                .iter()
                .find(|b| (*b).get_upper_bound() == 30.0)
                .unwrap_or_else(|| panic!("le=30 bucket missing in {model} series"));
            assert!(
                le30.get_cumulative_count() >= 1,
                "15s observation must land in le=30 bucket"
            );
            // gather 不输出显式 +Inf 桶——用总样本数对比 le30 累积计数证明无溢出。
            assert_eq!(
                hist.get_sample_count(),
                le30.get_cumulative_count(),
                "no overflow beyond le=30 for a 15s observation"
            );
        }
        assert!(found, "histogram series for {model} not found");
    }

    /// gather 指定 model/version 序列的直方图桶累积计数(不存在/未观测 → 0)。
    fn histogram_bucket_count(metric_name: &str, model: &str, le: f64) -> u64 {
        let families = REGISTRY.gather();
        let family = families
            .iter()
            .find(|mf| mf.get_name() == metric_name)
            .unwrap_or_else(|| panic!("{metric_name} must be registered"));
        for m in family.get_metric() {
            if m.get_label()
                .iter()
                .any(|l| l.get_name() == "model" && l.get_value() == model)
            {
                let hist = m.get_histogram();
                return hist
                    .get_bucket()
                    .iter()
                    .find(|b| (*b).get_upper_bound() == le)
                    .map(|b| b.get_cumulative_count())
                    .unwrap_or(0);
            }
        }
        0
    }

    /// S7:TTFT 桶追加 5/10/30/60——大模型冷启动 TTFT>2.5s(旧桶顶)不落 +Inf。
    #[test]
    fn streaming_ttft_buckets_extend_for_cold_start() {
        // 同上:先注册,单独/子集运行不依赖其他测试的注册副作用(B4 修复)。
        let _ = register_metrics();
        let model = "ttft_bucket_m";
        let version = "1";
        record_stream_ttft(model, version, "sse", 5.0);
        let le5 = histogram_bucket_count("liteserver_streaming_ttft_seconds", model, 5.0);
        assert!(le5 >= 1, "5.0s TTFT must land in the new le=5 bucket (got {le5})");
    }

    /// S7:TBT 桶追加 1/2.5/5——慢解码 chunk 间隔不落 +Inf(旧桶顶 0.5s)。
    #[test]
    fn streaming_tbt_buckets_extend_for_slow_decode() {
        // 同上:先注册(B4 修复)。
        let _ = register_metrics();
        let model = "tbt_bucket_m";
        let version = "1";
        record_stream_tbt(model, version, "sse", 1.0);
        let le1 = histogram_bucket_count("liteserver_streaming_tbt_seconds", model, 1.0);
        assert!(le1 >= 1, "1.0s TBT must land in the new le=1 bucket (got {le1})");
    }

    // ===== G1 (批次 4):OPEN_CONNECTIONS 死代码已删除 =====

    /// G1:删除后 gather 不再暴露 open_connections(编译级 + 输出级双重保证)。
    #[test]
    fn open_connections_removed_from_gather() {
        // 先注册——否则空注册表下断言恒真(子集运行空转,B4 修复)。
        let _ = register_metrics();
        let output = gather_metrics();
        assert!(
            !output.contains("open_connections"),
            "G1: dead OPEN_CONNECTIONS gauge must be removed: {}",
            output
        );
    }

    // ===== S4/S5/S6 (批次 3):errors 计数 + stream_kind label + duration/bytes =====

    #[test]
    fn stream_close_reason_maps_error_kind() {
        use StreamCloseReason::*;
        assert_eq!(Error.error_kind(), Some("worker_error"));
        assert_eq!(Deadline.error_kind(), Some("deadline"));
        assert_eq!(Idle.error_kind(), Some("idle"));
        assert_eq!(Protocol.error_kind(), Some("protocol"));
        assert_eq!(Panic.error_kind(), Some("panic"));
        assert_eq!(Done.error_kind(), None);
        assert_eq!(WorkerEof.error_kind(), None);
        assert_eq!(Cancel.error_kind(), None);
    }

    /// S4:Error reason → STREAM_ERRORS_TOTAL{kind=worker_error} +1(门控开)。
    #[test]
    fn record_stream_terminal_error_kind_counts_when_gated() {
        let model = "term_err_on";
        let version = "1";
        let before = STREAM_ERRORS_TOTAL
            .with_label_values(&[model, version, "sse", "worker_error"])
            .get();
        record_stream_terminal(
            model, version, "sse", "sse", std::time::Instant::now(),
            "5xx", StreamCloseReason::Error, true, 0, 0,
        );
        assert_eq!(
            STREAM_ERRORS_TOTAL.with_label_values(&[model, version, "sse", "worker_error"]).get(),
            before + 1.0,
            "S4: Error frame must count kind=worker_error"
        );
    }

    /// S4:cancel/done/worker_eof 不进 errors。
    #[test]
    fn record_stream_terminal_cancel_does_not_count_error_kind() {
        let model = "term_err_cancel";
        let version = "1";
        for reason in [
            StreamCloseReason::Cancel,
            StreamCloseReason::Done,
            StreamCloseReason::WorkerEof,
        ] {
            let family = reason.status_family();
            let before = STREAM_ERRORS_TOTAL
                .with_label_values(&[model, version, "sse", "x"])
                .get();
            record_stream_terminal(
                model, version, "sse", "sse", std::time::Instant::now(),
                family, reason, true, 0, 0,
            );
            assert_eq!(
                STREAM_ERRORS_TOTAL.with_label_values(&[model, version, "sse", "x"]).get(),
                before,
                "{reason:?} must not count as a stream error"
            );
        }
    }

    /// S6:duration 有观测、bytes 累加(门控开)。
    #[test]
    fn record_stream_terminal_records_duration_and_bytes_when_gated() {
        let model = "term_s6_on";
        let version = "1";
        let before_dur = STREAM_DURATION_SECONDS
            .with_label_values(&[model, version, "sse"])
            .get_sample_count();
        let before_bytes = STREAM_OUTPUT_BYTES_TOTAL
            .with_label_values(&[model, version, "sse"])
            .get();
        record_stream_terminal(
            model, version, "sse", "sse", std::time::Instant::now(),
            "2xx", StreamCloseReason::Done, true, 42, 3,
        );
        assert_eq!(
            STREAM_DURATION_SECONDS.with_label_values(&[model, version, "sse"]).get_sample_count(),
            before_dur + 1,
            "S6: close must observe stream duration"
        );
        assert_eq!(
            STREAM_OUTPUT_BYTES_TOTAL.with_label_values(&[model, version, "sse"]).get(),
            before_bytes + 42.0,
            "S6: chunk bytes must accumulate"
        );
    }

    /// D9 门控矩阵:cancelled/errors/duration/bytes 零记录,requests_total 仍记录。
    #[test]
    fn record_stream_terminal_gated_off_skips_errors_duration_bytes() {
        let model = "term_all_off";
        let version = "1";
        let e_before = STREAM_ERRORS_TOTAL
            .with_label_values(&[model, version, "websocket", "protocol"])
            .get();
        let d_before = STREAM_DURATION_SECONDS
            .with_label_values(&[model, version, "websocket"])
            .get_sample_count();
        let b_before = STREAM_OUTPUT_BYTES_TOTAL
            .with_label_values(&[model, version, "websocket"])
            .get();
        let r_before = REQUESTS_TOTAL.with_label_values(&[model, version, "4xx"]).get();
        record_stream_terminal(
            model, version, "websocket", "ws", std::time::Instant::now(),
            "4xx", StreamCloseReason::Protocol, false, 7, 1,
        );
        assert_eq!(
            STREAM_ERRORS_TOTAL.with_label_values(&[model, version, "websocket", "protocol"]).get(),
            e_before,
            "D9: errors gated by streaming_metrics"
        );
        assert_eq!(
            STREAM_DURATION_SECONDS.with_label_values(&[model, version, "websocket"]).get_sample_count(),
            d_before,
            "D9: duration gated by streaming_metrics"
        );
        assert_eq!(
            STREAM_OUTPUT_BYTES_TOTAL.with_label_values(&[model, version, "websocket"]).get(),
            b_before,
            "D9: bytes gated by streaming_metrics"
        );
        assert_eq!(
            REQUESTS_TOTAL.with_label_values(&[model, version, "4xx"]).get(),
            r_before + 1.0,
            "S1: requests_total must NOT be gated"
        );
    }

    // ===== P6: per-worker inference counter (GetModelStats source) =====

    #[test]
    fn should_record_worker_inference_total_per_worker() {
        let model = "wim_model";
        let version = "1";
        let before0 = worker_inference_count(model, version, 0);
        let before1 = worker_inference_count(model, version, 1);
        // Dispatches accumulate per (model, version, worker_id).
        record_worker_inference(model, version, 0, 3);
        record_worker_inference(model, version, 0, 2);
        record_worker_inference(model, version, 1, 5);
        assert_eq!(
            worker_inference_count(model, version, 0),
            before0 + 5,
            "worker 0 accumulates across dispatches"
        );
        assert_eq!(
            worker_inference_count(model, version, 1),
            before1 + 5,
            "worker 1 is tracked separately from worker 0"
        );
        // Unseen worker/label reads as 0 (counter lazily instantiated).
        assert_eq!(worker_inference_count(model, version, 99), 0);
    }

    // ===== G5 (批次 4 审计修复):close 日志携带 chunks 字段 =====

    /// 方案 .claude/observability-gaps.md §2.12:close 事件须携带
    /// `时长/chunks/bytes + reason`。断言 "stream closed" 事件的字段清单含
    /// chunks 且值即收口传入的 per-stream chunk 数。
    #[test]
    fn g5_close_log_carries_chunks_field() {
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default)]
        struct CloseEvents(Vec<(String, Vec<(String, String)>)>);
        struct CaptureLayer(std::sync::Arc<std::sync::Mutex<CloseEvents>>);
        struct FieldPairs {
            fields: Vec<(String, String)>,
            message: Option<String>,
        }
        impl tracing::field::Visit for FieldPairs {
            // record_str/record_i64/... 的默认实现均转发 record_debug。
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.fields
                    .push((field.name().to_string(), format!("{value:?}")));
                if field.name() == "message" {
                    self.message = Some(format!("{value:?}"));
                }
            }
        }
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut v = FieldPairs { fields: Vec::new(), message: None };
                event.record(&mut v);
                self.0
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .0
                    .push((v.message.unwrap_or_default(), v.fields));
            }
        }

        let captured = std::sync::Arc::new(std::sync::Mutex::new(CloseEvents::default()));
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(CaptureLayer(captured.clone())),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            // 确定性:并行套件中其他测试可能先把同一 callsite 的 interest 缓存
            // 成 NEVER(此后宏短路,scoped dispatch 不再被咨询)。在 scoped
            // default 生效期间 rebuild——DISPATCHERS 含本 scoped dispatch,
            // interest 被重算为 sometimes/always,事件必然可达本 layer。
            tracing::callsite::rebuild_interest_cache();
            record_stream_terminal(
                "g5_chunks_model",
                "1",
                "sse",
                "sse",
                std::time::Instant::now(),
                "2xx",
                StreamCloseReason::Done,
                true,
                7,
                3,
            );
        });

        let events = captured.lock().unwrap_or_else(|e| e.into_inner());
        let close: Vec<_> = events
            .0
            .iter()
            .filter(|(msg, _)| msg.contains("stream closed"))
            .collect();
        assert_eq!(
            close.len(),
            1,
            "expected exactly one stream-closed event; all events: {:?}",
            events.0
        );
        assert!(
            close[0].1.iter().any(|(k, v)| k == "chunks" && v == "3"),
            "G5 (plan §2.12): close log must carry the per-stream chunks value; got fields {:?}",
            close[0].1
        );
    }
}
