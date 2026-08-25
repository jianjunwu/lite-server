use lazy_static::lazy_static;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntGauge, IntGaugeVec,
    Registry, TextEncoder,
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

    // G6 (P-WARM): warmup observability. Duration covers every terminal run
    // (success AND failure — a failed warmup's wall time is diagnostic too);
    // the counter's status label is the closed WarmupStatus enum (§6.5 #10).
    pub static ref MODEL_WARMUP_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_model_warmup_total",
            "Warmup runs by terminal status"
        ),
        &["model", "version", "status"]
    ).unwrap();

    pub static ref MODEL_WARMUP_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "liteserver_model_warmup_duration_seconds",
            "Warmup wall time (all samples x iterations x workers)"
        ).buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]),
        &["model", "version"]
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
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
        // Round2 B4:桶追加 10/30/60——冷启动子模型/慢 step >5s 不落 +Inf
        // (与 S7 REQUEST_DURATION/TTFT 同模式,纯增量)。
        // depth (m6/E1): nesting depth of the ensemble containing the step —
        // bounded by the depth limit (8), so cardinality stays contained.
        &["ensemble", "step", "model", "version", "depth"]
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

    // Round2 A6: dedicated stream rejection counter. `reason` is a bounded
    // enum: "concurrency_limit" (P10 ensemble streaming DAG cap — a capacity
    // signal) | "early_reject" (S1b pre-open rejections: resolve/auth/
    // rate-limit/not-ready/first-frame). NOT gated on streaming_metrics —
    // rejection accounting parallels requests_total (D9: only the detailed
    // stream lifecycle metrics are gated).
    pub static ref STREAM_REJECTED_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_stream_rejected_total",
            "Stream requests rejected before open, by reason (concurrency_limit|early_reject)"
        ),
        &["model", "version", "reason"]
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

    pub static ref RECYCLE_STREAMS_EVICTED_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_recycle_streams_evicted_total",
            "In-flight streams force-evicted by a rolling-recycle stream-drain timeout"
        ),
        &["model", "version"]
    ).unwrap();

    pub static ref WORKER_RESPAWNS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_worker_respawns_total",
            "Total worker respawns"
        ),
        &["model", "version", "reason"]
    ).unwrap();

    /// Respawn attempts that FAILED (spawn/handshake of the replacement) —
    /// WORKER_RESPAWNS_TOTAL counts successes only, so without this a worker
    /// that can never come back is invisible in metrics (only a log line).
    pub static ref WORKER_RESPAWN_FAILURES_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_worker_respawn_failures_total",
            "Total failed worker respawn attempts"
        ),
        &["model", "version", "reason"]
    ).unwrap();

    // Shutdown tracking
    pub static ref SHUTDOWN_PENDING_REQUESTS: prometheus::IntGauge = prometheus::IntGauge::new(
        "liteserver_shutdown_pending_requests",
        "Number of in-flight requests during shutdown"
    ).unwrap();

    /// 1 while the server is draining (SIGTERM/SIGINT received, health
    /// endpoints failing) — lets alerts catch a pod stuck in the drain window.
    pub static ref DRAINING: prometheus::IntGauge = prometheus::IntGauge::new(
        "liteserver_draining",
        "1 while the server is in the graceful-shutdown drain window"
    ).unwrap();

    /// Request-drain duration (shutdown start → HTTP/gRPC drain finished).
    pub static ref SHUTDOWN_DRAIN_SECONDS: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "liteserver_shutdown_drain_seconds",
            "Graceful-shutdown request-drain duration"
        ).buckets(vec![0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0])
    ).unwrap();

    /// Streams closed cleanly by the shutdown negotiated close (wrap-up
    /// within the grace window — the client saw a normal stream end).
    pub static ref SHUTDOWN_STREAMS_CLOSED_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_shutdown_streams_closed_total",
            "In-flight streams closed cleanly by the shutdown negotiated close"
        ),
        &["model", "version"]
    ).unwrap();

    /// Shutdown counterpart of RECYCLE_STREAMS_EVICTED_TOTAL: streams still
    /// open after the shutdown grace window, terminated with an error frame.
    pub static ref SHUTDOWN_STREAMS_EVICTED_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_shutdown_streams_evicted_total",
            "In-flight streams force-evicted after the shutdown stream grace window"
        ),
        &["model", "version"]
    ).unwrap();

    // P10 (D40): concurrent streaming ensemble DAGs (global, no model label —
    // same口径 as the semaphore; per-model visibility comes from the existing
    // record_stream_open/terminal model labels, aligning with the m4 label
    // cardinality constraint).
    pub static ref ENSEMBLE_STREAMING_ACTIVE: prometheus::IntGauge = prometheus::IntGauge::new(
        "ensemble_streaming_active",
        "Number of concurrent streaming ensemble DAGs (P10 semaphore in use)"
    ).unwrap();

    // m4: sub-model autoload wait (P6 cold-start tracking).
    pub static ref ENSEMBLE_AUTOLOAD_WAIT: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "ensemble_autoload_wait_seconds",
            "Sub-model autoload wait duration for ensemble DAG steps"
        )
    ).unwrap();

    // m4 (§4.2): cumulative backpressure time on chain inter-hop channels.
    pub static ref ENSEMBLE_PIPELINE_CHANNEL_SATURATION: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "ensemble_pipeline_channel_saturation_seconds",
            "Cumulative time a pipeline chain inter-hop channel was full"
        )
    ).unwrap();

    // m4 (§4.2): pipeline chain depth (streaming steps on the chain).
    pub static ref ENSEMBLE_PIPELINE_CHAIN_DEPTH: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "ensemble_pipeline_chain_depth",
            "Streaming steps on a pipeline chain"
        )
    ).unwrap();

    // m4 (§4.3): bidi upstream aggregation observability.
    pub static ref ENSEMBLE_BIDI_AGGREGATE_BYTES: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "ensemble_bidi_aggregate_bytes",
            "Bytes aggregated for a bidi ensemble request"
        )
    ).unwrap();
    pub static ref ENSEMBLE_BIDI_AGGREGATE_SECONDS: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "ensemble_bidi_aggregate_seconds",
            "Elapsed time aggregating a bidi ensemble request"
        )
    ).unwrap();

    // ===== P-TRACE 导出健康（对账 A5）：OTel 管线自身的可观测 =====
    // ended→exported 的差值≈丢弃（BSP 队列满丢弃不可直接观测，以差值逼近）；
    // export_failures 直接计数导出失败。telemetry 关闭时恒 0。
    pub static ref OTEL_SPANS_ENDED_TOTAL: prometheus::IntCounter = prometheus::IntCounter::new(
        "liteserver_otel_spans_ended_total",
        "Total spans ended (entered the OTel export pipeline)"
    ).unwrap();
    pub static ref OTEL_SPANS_EXPORTED_TOTAL: prometheus::IntCounter = prometheus::IntCounter::new(
        "liteserver_otel_spans_exported_total",
        "Total spans successfully exported to the OTLP collector"
    ).unwrap();
    pub static ref OTEL_EXPORT_FAILURES_TOTAL: prometheus::IntCounter = prometheus::IntCounter::new(
        "liteserver_otel_export_failures_total",
        "Total failed OTLP span export batches"
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

    /// C1 (resource-leak-plan): callback dispatches dropped by the
    /// concurrency gate (reason="concurrency") or cut by callbacks.timeout_secs
    /// (reason="timeout"). Callbacks are fire-and-forget; these counters make
    /// the loss visible.
    pub static ref CALLBACK_DISPATCH_DROPPED: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "liteserver_callback_dispatch_dropped_total",
            "Callback dispatches dropped (concurrency cap) or cut (timeout)"
        ),
        &["reason"]
    ).unwrap();

    /// L4 (resource-leak-plan): open HTTP connections by transport
    /// (tcp|tls|uds). Connection-level observability was missing entirely —
    /// request-level metrics cannot see idle keep-alive connections, TLS
    /// handshakes, or slowloris holds. Incremented at accept, decremented
    /// when the connection task ends (K1's reaper makes the decrement
    /// observable on idle close).
    ///
    /// Blind spot: the tls series counts only POST-handshake connections
    /// (CountedTlsStream is constructed after the handshake completes), so a
    /// slowloris stall mid-handshake is invisible here. That phase is bounded
    /// by the RN-1 handshake semaphore; observe it via the semaphore if
    /// needed, not by widening this gauge's label set.
    pub static ref HTTP_CONNECTIONS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "liteserver_http_connections",
            "Open HTTP connections by transport"
        ),
        &["transport"]
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
// Vec shrinks only via M6b unload deregistration (swap_remove + index
// fix-up); registration is idempotent (duplicate specs reuse existing index).
lazy_static! {
    static ref CUSTOM_GAUGE_OBJECTS: std::sync::Mutex<Vec<GaugeVec>> = std::sync::Mutex::new(Vec::new());
    static ref CUSTOM_COUNTER_OBJECTS: std::sync::Mutex<Vec<CounterVec>> = std::sync::Mutex::new(Vec::new());
    static ref CUSTOM_HISTOGRAM_OBJECTS: std::sync::Mutex<Vec<HistogramVec>> = std::sync::Mutex::new(Vec::new());
    static ref CUSTOM_METRIC_INDEX: std::sync::Mutex<HashMap<String, (String, usize)>> = std::sync::Mutex::new(HashMap::new());
    /// M6b: live (model, version) references per pre-registered family key
    /// ("name:type") — a family object is deregistered once its last
    /// referencing version unloads (see deregister_unreferenced_custom_families).
    static ref CUSTOM_FAMILY_REFS: std::sync::Mutex<HashMap<String, std::collections::HashSet<(String, String)>>> =
        std::sync::Mutex::new(HashMap::new());
    /// Worker-local per-type metric id → global CUSTOM_*_OBJECTS position,
    /// per live (model, version). Workers assign ids as per-type ordinals of
    /// their OWN declaration order (python api.py register_metric) while the
    /// object vecs are process-global — without this translation a second
    /// model declaring distinct names would report into the wrong family.
    static ref CUSTOM_MODEL_IDS: std::sync::Mutex<HashMap<(String, String), std::sync::Arc<ModelMetricIds>>> =
        std::sync::Mutex::new(HashMap::new());
}

/// Per-(model, version) translation tables, built at register time in the
/// worker's spec declaration order (per type).
#[derive(Clone)]
struct ModelMetricIds {
    gauges: Vec<usize>,
    counters: Vec<usize>,
    histograms: Vec<usize>,
}

pub fn register_metrics() -> Result<(), prometheus::Error> {
    // Serialize concurrent callers (tests + server startup): an AlreadyReg
    // early-return via `?` must never let a caller observe a half-registered
    // registry — with the lock, the first caller completes the whole list
    // before the second one fails fast on the first duplicate.
    static REGISTER_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = REGISTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    REGISTRY.register(Box::new(REQUESTS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(REQUEST_DURATION.clone()))?;
    REGISTRY.register(Box::new(QUEUE_DEPTH.clone()))?;
    REGISTRY.register(Box::new(MODEL_LOAD_TOTAL.clone()))?;
    REGISTRY.register(Box::new(MODEL_WARMUP_TOTAL.clone()))?;
    REGISTRY.register(Box::new(MODEL_WARMUP_DURATION.clone()))?;
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
    REGISTRY.register(Box::new(STREAM_REJECTED_TOTAL.clone()))?;
    REGISTRY.register(Box::new(INFERENCE_DURATION.clone()))?;
    REGISTRY.register(Box::new(BATCH_SIZE.clone()))?;
    REGISTRY.register(Box::new(HEALTH_CHECK_TOTAL.clone()))?;
    REGISTRY.register(Box::new(WORKER_HEALTH_STATUS.clone()))?;
    REGISTRY.register(Box::new(WORKER_INFERENCE_TOTAL.clone()))?;
    REGISTRY.register(Box::new(RECYCLE_STREAMS_EVICTED_TOTAL.clone()))?;
    REGISTRY.register(Box::new(WORKER_RESPAWNS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(WORKER_RESPAWN_FAILURES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(SHUTDOWN_PENDING_REQUESTS.clone()))?;
    REGISTRY.register(Box::new(DRAINING.clone()))?;
    REGISTRY.register(Box::new(SHUTDOWN_DRAIN_SECONDS.clone()))?;
    REGISTRY.register(Box::new(SHUTDOWN_STREAMS_CLOSED_TOTAL.clone()))?;
    REGISTRY.register(Box::new(SHUTDOWN_STREAMS_EVICTED_TOTAL.clone()))?;
    // P10 (D40) + m4: ensemble streaming capacity gauge and batch-0/1
    // histograms — unregistered collectors never reach /metrics.
    REGISTRY.register(Box::new(ENSEMBLE_STREAMING_ACTIVE.clone()))?;
    REGISTRY.register(Box::new(ENSEMBLE_AUTOLOAD_WAIT.clone()))?;
    REGISTRY.register(Box::new(ENSEMBLE_BIDI_AGGREGATE_BYTES.clone()))?;
    REGISTRY.register(Box::new(ENSEMBLE_BIDI_AGGREGATE_SECONDS.clone()))?;
    REGISTRY.register(Box::new(OTEL_SPANS_ENDED_TOTAL.clone()))?;
    REGISTRY.register(Box::new(OTEL_SPANS_EXPORTED_TOTAL.clone()))?;
    REGISTRY.register(Box::new(OTEL_EXPORT_FAILURES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(IN_FLIGHT_REQUESTS.clone()))?;
    REGISTRY.register(Box::new(QUEUE_WAIT_SECONDS.clone()))?;
    REGISTRY.register(Box::new(WORKER_SATURATION.clone()))?;
    REGISTRY.register(Box::new(HTTP_REQUEST_BODY_BYTES.clone()))?;
    REGISTRY.register(Box::new(HTTP_CONNECTIONS.clone()))?;
    // Round2 B3: build info + process metrics. INFO is set here (not lazily)
    // so it is exported from process start even before the first scrape.
    REGISTRY.register(Box::new(INFO.clone()))?;
    REGISTRY.register(Box::new(PROCESS_RSS_BYTES.clone()))?;
    REGISTRY.register(Box::new(PROCESS_VIRT_BYTES.clone()))?;
    REGISTRY.register(Box::new(PROCESS_CPU_SECONDS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(PROCESS_START_TIME_SECONDS.clone()))?;
    REGISTRY.register(Box::new(PROCESS_THREADS.clone()))?;
    REGISTRY.register(Box::new(WORKER_RSS_BYTES.clone()))?;
    REGISTRY.register(Box::new(WORKER_VIRT_BYTES.clone()))?;
    REGISTRY.register(Box::new(WORKERS_RSS_BYTES.clone()))?;
    INFO.with_label_values(&[env!("CARGO_PKG_VERSION")]).set(1.0);
    REGISTRY.register(Box::new(CALLBACK_DISPATCH_DROPPED.clone()))?;
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
pub(crate) fn is_valid_metric_namespace(ns: &str) -> bool {
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
    refresh_process_metrics();
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = String::new();
    encoder.encode_utf8(&metric_families, &mut buffer).unwrap();
    buffer
}

// ===== Process & build info metrics (round2 B3) =====

lazy_static! {
    /// Build info; always 1. Deployment/version identification for rollouts.
    pub static ref INFO: GaugeVec = GaugeVec::new(
        prometheus::Opts::new("liteserver_info", "Server build info; value is always 1"),
        &["version"]
    ).unwrap();
    pub static ref PROCESS_RSS_BYTES: IntGauge = IntGauge::new(
        "liteserver_process_resident_memory_bytes", "Resident memory of the server process (bytes)"
    ).unwrap();
    pub static ref PROCESS_VIRT_BYTES: IntGauge = IntGauge::new(
        "liteserver_process_virtual_memory_bytes", "Virtual memory of the server process (bytes)"
    ).unwrap();
    pub static ref PROCESS_CPU_SECONDS_TOTAL: Counter = Counter::new(
        "liteserver_process_cpu_seconds_total", "Total CPU time of the server process (seconds, cumulative across cores)"
    ).unwrap();
    pub static ref PROCESS_START_TIME_SECONDS: Gauge = Gauge::new(
        "liteserver_process_start_time_seconds", "Server process start time (seconds since Unix epoch)"
    ).unwrap();
    /// Thread count — populated from sysinfo tasks (Linux/Android only; 0 elsewhere).
    pub static ref PROCESS_THREADS: IntGauge = IntGauge::new(
        "liteserver_process_threads", "Thread count of the server process (Linux/Android only; 0 where unsupported)"
    ).unwrap();
    /// Per-worker RSS. worker_id label follows the WORKER_HEALTH_STATUS
    /// precedent (bounded slot ids, not unbounded values).
    pub static ref WORKER_RSS_BYTES: IntGaugeVec = IntGaugeVec::new(
        prometheus::Opts::new(
            "liteserver_worker_resident_memory_bytes",
            "Resident memory of a single worker process (bytes)"
        ),
        &["model", "version", "worker_id"]
    ).unwrap();
    pub static ref WORKER_VIRT_BYTES: IntGaugeVec = IntGaugeVec::new(
        prometheus::Opts::new(
            "liteserver_worker_virtual_memory_bytes",
            "Virtual memory of a single worker process (bytes)"
        ),
        &["model", "version", "worker_id"]
    ).unwrap();
    /// RSS summed over the live workers of one (model, version) — alerting on
    // per-version memory without a PromQL sum.
    pub static ref WORKERS_RSS_BYTES: IntGaugeVec = IntGaugeVec::new(
        prometheus::Opts::new(
            "liteserver_workers_resident_memory_bytes",
            "Resident memory summed over all live workers of a model version (bytes)"
        ),
        &["model", "version"]
    ).unwrap();
}

/// Registry key for a sampled worker process: (model, version, worker_id).
pub type WorkerKey = (String, String, String);

/// A registered worker process. `start_time` is primed on the first sample
/// and re-checked afterwards: a mismatch means the OS recycled the PID and the
/// entry is stale (treated as dead). Reset on every (re)registration so a
/// respawn re-primes.
pub struct WorkerPidEntry {
    pub pid: sysinfo::Pid,
    pub start_time: Option<u64>,
}

/// Defensive bound on the worker PID registry (AGG-1-style). Worker counts
// are config-bounded in practice; the cap only guards against hook bugs.
pub const MAX_WORKER_PID_ENTRIES: usize = 4096;

/// Insert into the worker PID registry, respecting MAX_WORKER_PID_ENTRIES.
/// Overwriting an existing key (respawn) is always allowed. Returns false
/// when the insert was rejected.
fn worker_registry_insert(
    map: &mut HashMap<WorkerKey, WorkerPidEntry>,
    key: WorkerKey,
    entry: WorkerPidEntry,
) -> bool {
    if !map.contains_key(&key) && map.len() >= MAX_WORKER_PID_ENTRIES {
        return false;
    }
    map.insert(key, entry);
    true
}

/// Register a worker process for memory sampling. Called on spawn and respawn
/// (same worker_id overwrites the stale PID).
pub fn set_worker_pid(model: &str, version: &str, worker_id: u32, pid: u32) {
    let mut st = PROCESS_SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
    let key: WorkerKey = (model.to_string(), version.to_string(), worker_id.to_string());
    let entry = WorkerPidEntry {
        pid: sysinfo::Pid::from(pid as usize),
        start_time: None,
    };
    if !worker_registry_insert(&mut st.worker_pids, key, entry) {
        tracing::warn!(
            model,
            version,
            worker_id,
            "worker PID registry full ({MAX_WORKER_PID_ENTRIES} entries); memory metrics skipped"
        );
    }
}

/// Drop all worker PID entries of one (model, version) together with their
/// exported series. Must run BEFORE remove_version_metrics on the unload path:
/// the graceful kill is async, so a scrape between purge and process exit
/// would otherwise re-create the just-purged series from a live registry
/// entry.
pub fn clear_worker_pids(model: &str, version: &str) {
    let mut st = PROCESS_SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
    let keys: Vec<WorkerKey> = st
        .worker_pids
        .keys()
        .filter(|k| k.0 == model && k.1 == version)
        .cloned()
        .collect();
    for key in keys {
        st.worker_pids.remove(&key);
        let _ = WORKER_RSS_BYTES.remove_label_values(&[&key.0, &key.1, &key.2]);
        let _ = WORKER_VIRT_BYTES.remove_label_values(&[&key.0, &key.1, &key.2]);
    }
    let _ = WORKERS_RSS_BYTES.remove_label_values(&[model, version]);
}

struct ProcessSamplerState {
    system: sysinfo::System,
    pid: sysinfo::Pid,
    last_cpu_ms: u64,
    primed: bool,
    worker_pids: HashMap<WorkerKey, WorkerPidEntry>,
}

lazy_static! {
    static ref PROCESS_SAMPLER: std::sync::Mutex<ProcessSamplerState> = std::sync::Mutex::new(ProcessSamplerState {
        system: sysinfo::System::new(),
        pid: sysinfo::Pid::from(std::process::id() as usize),
        last_cpu_ms: 0,
        primed: false,
        worker_pids: HashMap::new(),
    });
}

/// Refresh process metrics from sysinfo. Called by `gather_metrics` on every
/// scrape — scrape-time freshness without a background task. CPU seconds are
/// derived from sysinfo's cumulative `accumulated_cpu_time` (CPU-ms), so the
/// counter stays monotonic; the first refresh only primes the baseline.
///
/// Worker processes registered via set_worker_pid are sampled in the same
/// sysinfo refresh. A registered PID absent from the refreshed map is
/// DEFINITIVELY dead (refresh removes dead processes), so its series are
/// removed and the entry dropped — self-healing for crash/eject/kill paths
/// that bypass clear_worker_pids. The per-version aggregate only sums live
/// workers; when the last worker of a version dies the aggregate series is
/// removed too.
pub fn refresh_process_metrics() {
    let mut st = PROCESS_SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
    let pid = st.pid;
    let mut pids: Vec<sysinfo::Pid> = Vec::with_capacity(st.worker_pids.len() + 1);
    pids.push(pid);
    for entry in st.worker_pids.values() {
        if !pids.contains(&entry.pid) {
            pids.push(entry.pid);
        }
    }
    st.system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&pids), true);
    if let Some(p) = st.system.process(pid) {
        let (rss, virt, start, cpu_ms, n_tasks) = (
            p.memory(),
            p.virtual_memory(),
            p.start_time(),
            p.accumulated_cpu_time(),
            p.tasks().map(|t| t.len()),
        );
        PROCESS_RSS_BYTES.set(rss as i64);
        PROCESS_VIRT_BYTES.set(virt as i64);
        PROCESS_START_TIME_SECONDS.set(start as f64);
        if st.primed && cpu_ms >= st.last_cpu_ms {
            PROCESS_CPU_SECONDS_TOTAL.inc_by((cpu_ms - st.last_cpu_ms) as f64 / 1000.0);
        }
        st.last_cpu_ms = cpu_ms;
        st.primed = true;
        if let Some(n) = n_tasks {
            PROCESS_THREADS.set(n as i64);
        }
    }

    // Worker sampling. Destructure the guard for disjoint field borrows:
    // iterate worker_pids mutably, read system immutably.
    let ProcessSamplerState { system, worker_pids, .. } = &mut *st;
    let mut dead: Vec<WorkerKey> = Vec::new();
    let mut sums: HashMap<(String, String), u64> = HashMap::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for (key, entry) in worker_pids.iter_mut() {
        seen.insert((key.0.clone(), key.1.clone()));
        let Some(p) = system.process(entry.pid) else {
            dead.push(key.clone());
            continue;
        };
        let start = p.start_time();
        match entry.start_time {
            // Same PID, different start_time: the OS recycled the PID — the
            // worker is gone and this is someone else's process.
            Some(primed) if primed != start => {
                dead.push(key.clone());
                continue;
            }
            None => entry.start_time = Some(start),
            _ => {}
        }
        let rss = p.memory();
        WORKER_RSS_BYTES
            .with_label_values(&[&key.0, &key.1, &key.2])
            .set(rss as i64);
        WORKER_VIRT_BYTES
            .with_label_values(&[&key.0, &key.1, &key.2])
            .set(p.virtual_memory() as i64);
        *sums.entry((key.0.clone(), key.1.clone())).or_default() += rss;
    }
    for key in dead {
        worker_pids.remove(&key);
        let _ = WORKER_RSS_BYTES.remove_label_values(&[&key.0, &key.1, &key.2]);
        let _ = WORKER_VIRT_BYTES.remove_label_values(&[&key.0, &key.1, &key.2]);
    }
    for mv in seen {
        match sums.get(&mv) {
            Some(total) => WORKERS_RSS_BYTES
                .with_label_values(&[&mv.0, &mv.1])
                .set(*total as i64),
            None => {
                let _ = WORKERS_RSS_BYTES.remove_label_values(&[&mv.0, &mv.1]);
            }
        }
    }
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

/// Constant label for reject-path recording when the requested (model,
/// version) never resolved to a registry entry (A2, leak-gap-audit-0821):
/// such a pair is never unloaded, so remove_version_metrics never reaps its
/// series and an unauthenticated client enumerating model names would grow
/// REQUESTS_TOTAL / REQUEST_DURATION (a 14-bucket histogram per series)
/// linearly and permanently — the Prometheus-side parity of the AGG-1
/// TIMELINE cap. Registered pairs (any status, including not-ready/failed)
/// keep their real labels: per-model reject observability is an intentional
/// contract. The `~` is outside the valid model name charset (validation.rs:
/// [a-zA-Z0-9_-]), so no real model can collide.
pub const UNKNOWN_MODEL_LABEL: &str = "~unknown~";

/// Reject-path label normalization: raw labels for registered pairs,
/// constant label for pairs that never resolved. Callers (HTTP/gRPC reject
/// sites) own the registry lookup.
pub fn reject_labels<'a>(registered: bool, model: &'a str, version: &'a str) -> (&'a str, &'a str) {
    if registered {
        (model, version)
    } else {
        (UNKNOWN_MODEL_LABEL, "")
    }
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
    let g = QUEUE_DEPTH.with_label_values(&[model, version]);
    g.dec();
    floor_gauge_at_zero(&g);
    mirror_gie_queued(model, version, -1.0);
}

/// Late-dec self-heal (audit 2026-08-22 B5): after remove_version_metrics
/// purges a series, a late paired dec (e.g. a detached send_batch task's
/// InflightGuard dropping post-unload) would otherwise resurrect the series
/// at -1 — and the next load of the same version would start the gauge at
/// -N. The race is inherent (the purge cannot wait for tasks it cannot
/// see), so the result is healed instead: a gauge never stays negative.
/// The read-modify-write window can miss a clamp under extreme concurrency;
/// the next dec catches it — a gauge is an observation, not a ledger.
fn floor_gauge_at_zero(g: &prometheus::Gauge) {
    if g.get() < 0.0 {
        g.set(0.0);
    }
}

/// GIE TotalQueuedRequests 镜像（构造上与 liteserver_queue_depth 同步，无双写
/// 漂移）；register_gie_metrics 之前调用为 no-op。
fn mirror_gie_queued(model: &str, version: &str, delta: f64) {
    let guard = GIE_TOTAL_QUEUED_REQUESTS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    for g in guard.values() {
        let gauge = g.with_label_values(&[model, version]);
        gauge.add(delta);
        floor_gauge_at_zero(&gauge);
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
    let g = IN_FLIGHT_REQUESTS.with_label_values(&[model, version]);
    g.dec();
    floor_gauge_at_zero(&g);
}

// ===== Model metrics =====

pub fn record_model_load(model: &str, version: &str, success: bool) {
    let status = if success { "success" } else { "fail" };
    MODEL_LOAD_TOTAL.with_label_values(&[model, version, "load", status]).inc();
}

/// G6: terminal status of a warmup run — the closed label set of
/// `liteserver_model_warmup_total` (§6.5 #10: label values are closed enums).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmupStatus {
    Success,
    Failure,
    Timeout,
}

impl WarmupStatus {
    fn as_str(&self) -> &'static str {
        match self {
            WarmupStatus::Success => "success",
            WarmupStatus::Failure => "failure",
            WarmupStatus::Timeout => "timeout",
        }
    }
}

/// G6: record one terminal warmup run — duration always observed (a failed
/// run's wall time is diagnostic too), counter bumped by status.
pub fn record_model_warmup(model: &str, version: &str, secs: f64, status: WarmupStatus) {
    MODEL_WARMUP_DURATION
        .with_label_values(&[model, version])
        .observe(secs);
    MODEL_WARMUP_TOTAL
        .with_label_values(&[model, version, status.as_str()])
        .inc();
}

pub fn record_model_unload(model: &str, version: &str) {
    MODEL_LOAD_TOTAL.with_label_values(&[model, version, "unload", "success"]).inc();
}

pub fn set_active_workers(model: &str, version: &str, count: f64) {
    ACTIVE_WORKERS.with_label_values(&[model, version]).set(count);
}

/// P10 (D40): reflect the global streaming-DAG semaphore's in-use count.
pub fn set_ensemble_streaming_active(count: usize) {
    ENSEMBLE_STREAMING_ACTIVE.set(count as i64);
}

/// m4: sub-model autoload wait duration (histogram) — the dominant cold-TTFT
/// term for ensemble DAGs (P6 tracks it).
pub fn record_ensemble_autoload_wait_seconds(secs: f64) {
    ENSEMBLE_AUTOLOAD_WAIT.observe(secs);
}

/// m4 (§4.3): bidi upstream aggregation — bytes aggregated + elapsed time.
pub fn record_ensemble_bidi_aggregate(bytes: usize, seconds: f64) {
    ENSEMBLE_BIDI_AGGREGATE_BYTES.observe(bytes as f64);
    ENSEMBLE_BIDI_AGGREGATE_SECONDS.observe(seconds);
}

/// m4 (§4.2): pipeline chain depth — streaming steps on the chain.
pub fn record_ensemble_pipeline_chain_depth(depth: usize) {
    ENSEMBLE_PIPELINE_CHAIN_DEPTH.observe(depth as f64);
}

/// m4 (§4.2): cumulative time a chain hop's channel was FULL (backpressure).
pub fn record_ensemble_pipeline_channel_saturation_seconds(secs: f64) {
    ENSEMBLE_PIPELINE_CHANNEL_SATURATION.observe(secs);
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

/// Round2 B2: drop every per-(model,version) series on version unload.
/// Long-running servers (tune/profile campaigns cycle many versions)
/// otherwise grow the in-process label set without bound; Prometheus keeps
/// the scraped history — the series simply goes stale (standard target
/// semantics). Series are enumerated via REGISTRY.gather so N-label families
/// (status/worker_id/reason/kind/…) don't need their extra values known.
///
/// Scope: built-in per-version families + the GIE queued mirror + the
/// worker-reported dynamic families and pre-registered custom families
/// (PROM-1) + ENSEMBLE_STEP_LATENCY (M8: its model/version labels belong to
/// the step's sub-model and age out with the *sub-model's* unload — this
/// purge IS that aging path). Deliberately excluded: MODEL_LOAD_TOTAL (the
/// load/unload event log — the one series operators query *after* an
/// unload), VERSION_SWITCHES_TOTAL (routing history, aged per B16).
pub fn remove_version_metrics(model: &str, version: &str) {
    let families = REGISTRY.gather();
    macro_rules! purge {
        ($vec:expr, $family:expr, $order:expr) => {
            if let Some(mf) = families.iter().find(|f| f.get_name() == $family) {
                for m in mf.get_metric() {
                    let labels = m.get_label();
                    let matches = labels
                        .iter()
                        .any(|l| l.get_name() == "model" && l.get_value() == model)
                        && labels
                            .iter()
                            .any(|l| l.get_name() == "version" && l.get_value() == version);
                    if !matches {
                        continue;
                    }
                    let values: Vec<&str> = $order
                        .iter()
                        .map(|name| {
                            labels
                                .iter()
                                .find(|l| l.get_name() == *name)
                                .map(|l| l.get_value())
                                .unwrap_or("")
                        })
                        .collect();
                    let _ = $vec.remove_label_values(&values);
                }
            }
        };
    }
    purge!(REQUESTS_TOTAL, "liteserver_requests_total", ["model", "version", "status"]);
    purge!(REQUEST_DURATION, "liteserver_request_duration_seconds", ["model", "version"]);
    purge!(QUEUE_DEPTH, "liteserver_queue_depth", ["model", "version"]);
    purge!(ACTIVE_WORKERS, "liteserver_active_workers", ["model", "version"]);
    purge!(WORKER_EJECTIONS_TOTAL, "liteserver_worker_ejections_total", ["model", "version"]);
    purge!(RETRIES_TOTAL, "liteserver_retries_total", ["model", "version"]);
    purge!(STREAMING_CONNECTIONS, "liteserver_streaming_connections", ["model", "version", "protocol"]);
    purge!(STREAMING_TTFT, "liteserver_streaming_ttft_seconds", ["model", "version", "protocol"]);
    purge!(STREAMING_TBT, "liteserver_streaming_tbt_seconds", ["model", "version", "protocol"]);
    purge!(STREAMING_CHUNKS_TOTAL, "liteserver_streaming_chunks_total", ["model", "version", "protocol"]);
    purge!(STREAM_CANCELLED_TOTAL, "liteserver_stream_cancelled_total", ["model", "version", "protocol"]);
    purge!(STREAM_ERRORS_TOTAL, "liteserver_stream_errors_total", ["model", "version", "stream_kind", "kind"]);
    purge!(STREAM_DURATION_SECONDS, "liteserver_stream_duration_seconds", ["model", "version", "stream_kind"]);
    purge!(STREAM_OUTPUT_BYTES_TOTAL, "liteserver_stream_output_bytes_total", ["model", "version", "stream_kind"]);
    purge!(STREAM_REJECTED_TOTAL, "liteserver_stream_rejected_total", ["model", "version", "reason"]);
    purge!(HEALTH_CHECK_TOTAL, "liteserver_health_check_total", ["model", "version", "result"]);
    purge!(WORKER_HEALTH_STATUS, "liteserver_worker_health_status", ["model", "version", "worker_id"]);
    purge!(WORKER_RSS_BYTES, "liteserver_worker_resident_memory_bytes", ["model", "version", "worker_id"]);
    purge!(WORKER_VIRT_BYTES, "liteserver_worker_virtual_memory_bytes", ["model", "version", "worker_id"]);
    purge!(WORKERS_RSS_BYTES, "liteserver_workers_resident_memory_bytes", ["model", "version"]);
    purge!(WORKER_INFERENCE_TOTAL, "liteserver_worker_inference_total", ["model", "version", "worker_id"]);
    purge!(WORKER_RESPAWNS_TOTAL, "liteserver_worker_respawns_total", ["model", "version", "reason"]);
    purge!(WORKER_RESPAWN_FAILURES_TOTAL, "liteserver_worker_respawn_failures_total", ["model", "version", "reason"]);
    purge!(IN_FLIGHT_REQUESTS, "liteserver_in_flight_requests", ["model", "version"]);
    purge!(QUEUE_WAIT_SECONDS, "liteserver_queue_wait_seconds", ["model", "version"]);
    purge!(WORKER_SATURATION, "liteserver_worker_saturation", ["model", "version"]);
    purge!(INFERENCE_DURATION, "liteserver_inference_duration_seconds", ["model", "version"]);
    purge!(BATCH_SIZE, "liteserver_batch_size", ["model", "version"]);
    // M8: sub-model unload ages out its ensemble step-latency series
    // (ensemble/step/depth values enumerated via gather).
    purge!(ENSEMBLE_STEP_LATENCY, "liteserver_ensemble_step_latency_seconds", ["ensemble", "step", "model", "version", "depth"]);
    // GIE TotalQueuedRequests mirror (namespace-configurable name — clean via
    // the registered vecs directly, not gather).
    let guard = GIE_TOTAL_QUEUED_REQUESTS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    for g in guard.values() {
        let _ = g.remove_label_values(&[model, version]);
    }
    // PROM-1: worker-reported dynamic families (prefill_ms / decode_ms /
    // tokens_generated_total + worker gauges/counters/histograms) and the
    // pre-registered custom metric objects. Only the (model, version) SERIES
    // are dropped — the family objects/index are process-global and shared.
    // A late in-flight Done frame may re-create one series via
    // with_label_values after the purge; the stream is terminal, so it never
    // grows beyond that single series.
    for vec in CUSTOM_GAUGES.lock().unwrap_or_else(|e| e.into_inner()).values() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_COUNTERS.lock().unwrap_or_else(|e| e.into_inner()).values() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_HISTOGRAMS.lock().unwrap_or_else(|e| e.into_inner()).values() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_COUNTER_OBJECTS.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_HISTOGRAM_OBJECTS.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    purge!(MODEL_WARMUP_TOTAL, "liteserver_model_warmup_total", ["model", "version", "status"]);
    purge!(MODEL_WARMUP_DURATION, "liteserver_model_warmup_duration_seconds", ["model", "version"]);
    // B16: routing history ages out for the unloaded version — purge
    // VERSION_SWITCHES_TOTAL series whose from OR to names it (the standard
    // purge's model+version filter can't match its label shape). Series
    // between surviving versions stay.
    if let Some(mf) = families
        .iter()
        .find(|f| f.get_name() == "liteserver_version_switches_total")
    {
        for m in mf.get_metric() {
            let labels = m.get_label();
            let get = |name: &str| {
                labels
                    .iter()
                    .find(|l| l.get_name() == name)
                    .map(|l| l.get_value())
                    .unwrap_or("")
            };
            if get("model") == model && (get("from") == version || get("to") == version) {
                let _ = VERSION_SWITCHES_TOTAL.remove_label_values(&[
                    get("model"),
                    get("from"),
                    get("to"),
                ]);
            }
        }
    }
    // M6b: deregister pre-registered custom FAMILY OBJECTS whose last live
    // (model, version) reference just unloaded (series purge above only
    // drops the label sets).
    deregister_unreferenced_custom_families(model, version);
}

/// B6 (leak-gap-audit-0821): deregister ONLY the custom-metric state of a
/// version — the failed-load funnels use this instead of the full
/// remove_version_metrics purge: a Failed version stays in the registry
/// (D33) and its built-in series (warmup outcome, rejects) are live
/// observability, but its custom FAMILY OBJECTS and refs must not leak
/// (a Failed version never sees the unload-time purge).
pub(crate) fn remove_custom_version_metrics(model: &str, version: &str) {
    for vec in CUSTOM_GAUGES.lock().unwrap_or_else(|e| e.into_inner()).values() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_COUNTERS.lock().unwrap_or_else(|e| e.into_inner()).values() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_HISTOGRAMS.lock().unwrap_or_else(|e| e.into_inner()).values() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_COUNTER_OBJECTS.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    for vec in CUSTOM_HISTOGRAM_OBJECTS.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        let _ = vec.remove_label_values(&[model, version]);
    }
    deregister_unreferenced_custom_families(model, version);
}

/// Test seam: is a custom metric family still registered (index or refs)?
#[cfg(test)]
pub(crate) fn custom_family_registered_for_test(key: &str) -> bool {
    CUSTOM_METRIC_INDEX
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(key)
        || CUSTOM_FAMILY_REFS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(key)
}

pub fn record_version_switch(model: &str, from: &str, to: &str) {
    VERSION_SWITCHES_TOTAL.with_label_values(&[model, from, to]).inc();
}

// ===== Ensemble metrics =====

pub fn record_ensemble_step_latency(ensemble: &str, step: &str, model: &str, version: &str, depth: u32, latency_secs: f64) {
    let depth_str = depth.to_string();
    ENSEMBLE_STEP_LATENCY.with_label_values(&[ensemble, step, model, version, &depth_str]).observe(latency_secs);
}

// ===== Streaming metrics =====

/// L4: HTTP connection-level gauge. `transport` is one of tcp|tls|uds.
/// Not gated on streaming_metrics — connection accounting is a server-level
/// resource signal, not a per-stream feature.
pub fn record_http_connection_open(transport: &str) {
    HTTP_CONNECTIONS.with_label_values(&[transport]).inc();
}

pub fn record_http_connection_close(transport: &str) {
    HTTP_CONNECTIONS.with_label_values(&[transport]).dec();
}

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
    /// m7 (D20): ensemble streaming step produced a binary chunk on a text
    /// endpoint (SSE/OpenAI) after the stream opened — closed with an Error
    /// frame; the static D7 binary-flag 400 was not set for this model.
    TypeMismatch,
}

impl StreamCloseReason {
    /// D7 family 映射:done/cancel→2xx;error/deadline/idle/panic/worker_eof→5xx;
    /// protocol→4xx。gRPC 的 Error 帧除外(按其 grpc code 映射)。
    /// G5:worker_eof 自 2xx 移入 5xx——worker 中途死亡(回收/健康强杀/unload)
    /// 是服务端故障,不得在指标上与正常结束(Done)混淆。
    pub fn status_family(self) -> &'static str {
        match self {
            StreamCloseReason::Done
            | StreamCloseReason::Cancel => "2xx",
            StreamCloseReason::Error
            | StreamCloseReason::Deadline
            | StreamCloseReason::Idle
            | StreamCloseReason::Panic
            | StreamCloseReason::WorkerEof
            | StreamCloseReason::TypeMismatch => "5xx",
            StreamCloseReason::Protocol => "4xx",
        }
    }

    /// S4:errors kind 映射——error→worker_error、deadline→deadline、
    /// idle→idle、protocol→protocol、panic→panic(仅 WS 可达)、
    /// worker_eof→worker_eof(G5);cancel/done 不进 errors(None)。
    pub fn error_kind(self) -> Option<&'static str> {
        match self {
            StreamCloseReason::Error => Some("worker_error"),
            StreamCloseReason::Deadline => Some("deadline"),
            StreamCloseReason::Idle => Some("idle"),
            StreamCloseReason::Protocol => Some("protocol"),
            StreamCloseReason::Panic => Some("panic"),
            StreamCloseReason::TypeMismatch => Some("type_mismatch"),
            StreamCloseReason::WorkerEof => Some("worker_eof"),
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
/// Round2 A6:同时记专属 counter(reason 有界:concurrency_limit|early_reject)。
pub fn record_stream_rejected(model: &str, version: &str, family: &str, duration_secs: f64, reason: &'static str) {
    record_request_end(model, version, family, duration_secs);
    STREAM_REJECTED_TOTAL.with_label_values(&[model, version, reason]).inc();
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
/// M6b: (model, version) is recorded as a live reference of every declared
/// family; the family object is deregistered when its last referencing
/// version unloads (remove_version_metrics).
pub fn register_custom_metrics(model: &str, version: &str, specs: &[(&str, &str)]) {
    // Canonical lock order (gauges → counters → histograms → index → refs →
    // model_ids); deregister_unreferenced_custom_families takes the same set
    // nested in the same order, so register/deregister are atomic w.r.t.
    // each other.
    let mut gauges = CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut counters = CUSTOM_COUNTER_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut histograms = CUSTOM_HISTOGRAM_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut index = CUSTOM_METRIC_INDEX.lock().unwrap_or_else(|e| e.into_inner());
    let mut refs = CUSTOM_FAMILY_REFS.lock().unwrap_or_else(|e| e.into_inner());
    let mut model_ids = CUSTOM_MODEL_IDS.lock().unwrap_or_else(|e| e.into_inner());

    let mut ids = ModelMetricIds {
        gauges: Vec::new(),
        counters: Vec::new(),
        histograms: Vec::new(),
    };
    for (name, metric_type) in specs {
        let key = format!("{}:{}", name, metric_type);
        refs.entry(key.clone())
            .or_default()
            .insert((model.to_string(), version.to_string()));
        let global_idx = match index.get(&key) {
            Some(existing) => existing.1, // already registered — idempotent
            None => match *metric_type {
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
                    idx
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
                    idx
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
                    idx
                }
                _ => continue,
            },
        };
        // Translation table: the worker's per-type declaration ordinal →
        // the family's global Vec position (record_custom_metrics resolves
        // worker-local ids through this).
        match *metric_type {
            "gauge" => ids.gauges.push(global_idx),
            "counter" => ids.counters.push(global_idx),
            "histogram" => ids.histograms.push(global_idx),
            _ => {}
        }
    }
    model_ids.insert(
        (model.to_string(), version.to_string()),
        std::sync::Arc::new(ids),
    );
}

/// M6b: drop this version's reference on every pre-registered custom family;
/// a family whose last live reference is gone is deregistered (index entry,
/// object Vec slot, REGISTRY registration). The numeric-ID index is
/// positional, so swap_remove is paired with a fix-up of the moved object's
/// index entry. Runs on the unload path only (cold).
fn deregister_unreferenced_custom_families(model: &str, version: &str) {
    // Canonical lock order: same set, same order as register_custom_metrics.
    let mut gauges = CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut counters = CUSTOM_COUNTER_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut histograms = CUSTOM_HISTOGRAM_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
    let mut index = CUSTOM_METRIC_INDEX.lock().unwrap_or_else(|e| e.into_inner());
    let mut refs = CUSTOM_FAMILY_REFS.lock().unwrap_or_else(|e| e.into_inner());
    let mut model_ids = CUSTOM_MODEL_IDS.lock().unwrap_or_else(|e| e.into_inner());

    // The unloading version's translation table is dropped regardless of
    // family refcounting (its workers are gone).
    model_ids.remove(&(model.to_string(), version.to_string()));

    let mv = (model.to_string(), version.to_string());
    // Drop the unloading version's own reference EVERYWHERE it appears. A
    // family shared across versions (the common rolling-deploy shape: v1 and
    // v2 declare the same metric) must lose this version's ref now — only
    // removing whole singleton sets leaves shared sets permanently non-empty,
    // so the family, its index entry, and the phantom refs leak forever.
    let mut emptied: Vec<String> = Vec::new();
    for (key, set) in refs.iter_mut() {
        if set.remove(&mv) && set.is_empty() {
            emptied.push(key.clone());
        }
    }
    for key in emptied {
        refs.remove(&key);
        let Some((mtype, idx)) = index.remove(&key) else {
            continue;
        };
        /// swap_remove the object at `idx`, unregister it, and fix every
        /// reference to the object moved from the last slot into `idx`:
        /// the name index entry AND every live model's translation table
        /// (CUSTOM_MODEL_IDS) — otherwise later records of an unrelated
        /// model would land in the wrong family.
        fn deregister_object<C>(
            objects: &mut Vec<C>,
            index: &mut HashMap<String, (String, usize)>,
            model_ids: &mut HashMap<(String, String), std::sync::Arc<ModelMetricIds>>,
            idx: usize,
            mtype: &str,
        ) where
            C: prometheus::core::Collector + Clone + Send + Sync + 'static,
        {
            if idx >= objects.len() {
                return;
            }
            let obj = objects.swap_remove(idx);
            let _ = REGISTRY.unregister(Box::new(obj));
            let moved_from = objects.len();
            if idx < moved_from {
                for v in index.values_mut() {
                    if v.0 == mtype && v.1 == moved_from {
                        v.1 = idx;
                        break;
                    }
                }
                for table in model_ids.values_mut() {
                    // make_mut clones when an in-flight record holds an Arc —
                    // the map converges; a racing record keeps its (now
                    // stale) snapshot for that one frame, same tolerance as
                    // the late-Done-frame series re-creation above.
                    let table = std::sync::Arc::make_mut(table);
                    let list = match mtype {
                        "gauge" => &mut table.gauges,
                        "counter" => &mut table.counters,
                        _ => &mut table.histograms,
                    };
                    for g in list.iter_mut() {
                        if *g == moved_from {
                            *g = idx;
                        }
                    }
                }
            }
        }
        match mtype.as_str() {
            "gauge" => deregister_object(&mut gauges, &mut index, &mut model_ids, idx, "gauge"),
            "counter" => deregister_object(&mut counters, &mut index, &mut model_ids, idx, "counter"),
            "histogram" => deregister_object(&mut histograms, &mut index, &mut model_ids, idx, "histogram"),
            _ => {}
        }
    }
}

/// Record pre-registered custom metrics — hot path, one map lookup + one
/// lock per non-empty type, no per-metric HashMap. `mv.id` is the WORKER's
/// per-type declaration ordinal; it is translated to the global object Vec
/// position through CUSTOM_MODEL_IDS (see register_custom_metrics). Unknown
/// model or out-of-range id → skipped (unregistered/late frames).
pub fn record_custom_metrics(
    model: &str,
    version: &str,
    gauges: &[crate::proto::liteserver::MetricValue],
    counters: &[crate::proto::liteserver::MetricValue],
    histograms: &[crate::proto::liteserver::MetricValue],
) {
    if gauges.is_empty() && counters.is_empty() && histograms.is_empty() {
        return;
    }
    let ids = CUSTOM_MODEL_IDS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&(model.to_string(), version.to_string()))
        .cloned();
    let Some(ids) = ids else { return };
    if !gauges.is_empty() {
        let guard = CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
        for mv in gauges {
            if let Some(&global) = ids.gauges.get(mv.id as usize) {
                if let Some(g) = guard.get(global) {
                    g.with_label_values(&[model, version]).set(mv.value as f64);
                }
            }
        }
    }
    if !counters.is_empty() {
        let guard = CUSTOM_COUNTER_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
        for mv in counters {
            if let Some(&global) = ids.counters.get(mv.id as usize) {
                if let Some(c) = guard.get(global) {
                    c.with_label_values(&[model, version]).inc_by(mv.value as f64);
                }
            }
        }
    }
    if !histograms.is_empty() {
        let guard = CUSTOM_HISTOGRAM_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
        for mv in histograms {
            if let Some(&global) = ids.histograms.get(mv.id as usize) {
                if let Some(h) = guard.get(global) {
                    h.with_label_values(&[model, version]).observe(mv.value as f64);
                }
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

/// G1/Q1 observability: streams force-evicted by a rolling-recycle
/// stream-drain timeout. A rising value means long streams are being cut —
/// tune `recycle_stream_drain_timeout_secs` or `max_requests`.
pub fn record_recycle_streams_evicted(model: &str, version: &str, count: u64) {
    RECYCLE_STREAMS_EVICTED_TOTAL
        .with_label_values(&[model, version])
        .inc_by(count as f64);
}

/// Mark the drain window open (shutdown received). The process exits after
/// teardown, so the gauge is never reset — it exists for scrapes and alerts
/// DURING the drain window.
pub fn set_draining(draining: bool) {
    DRAINING.set(if draining { 1 } else { 0 });
}

/// Q2-at-shutdown observability: drain duration (shutdown start → HTTP/gRPC
/// drain finished). Compare against `graceful_timeout`: a value pinned at
/// the timeout means the backstop fired.
pub fn record_shutdown_drain(seconds: f64) {
    SHUTDOWN_DRAIN_SECONDS.observe(seconds);
}

/// Streams that wrapped up within the shutdown grace window (client saw a
/// normal stream end).
pub fn record_shutdown_streams_closed(model: &str, version: &str, count: u64) {
    SHUTDOWN_STREAMS_CLOSED_TOTAL
        .with_label_values(&[model, version])
        .inc_by(count as f64);
}

/// Streams still open after the shutdown grace window, terminated with an
/// error frame. A rising value means `shutdown_stream_grace_ms` (or the
/// drain window) is too short for the models' wrap-up.
pub fn record_shutdown_streams_evicted(model: &str, version: &str, count: u64) {
    SHUTDOWN_STREAMS_EVICTED_TOTAL
        .with_label_values(&[model, version])
        .inc_by(count as f64);
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
    fn test_record_shutdown_streams_evicted_increments() {
        let before = SHUTDOWN_STREAMS_EVICTED_TOTAL
            .with_label_values(&["evict_model", "1"])
            .get();
        record_shutdown_streams_evicted("evict_model", "1", 2);
        let after = SHUTDOWN_STREAMS_EVICTED_TOTAL
            .with_label_values(&["evict_model", "1"])
            .get();
        assert_eq!(after, before + 2.0);
    }

    #[test]
    fn test_record_shutdown_streams_closed_increments() {
        let before = SHUTDOWN_STREAMS_CLOSED_TOTAL
            .with_label_values(&["closed_model", "1"])
            .get();
        record_shutdown_streams_closed("closed_model", "1", 3);
        let after = SHUTDOWN_STREAMS_CLOSED_TOTAL
            .with_label_values(&["closed_model", "1"])
            .get();
        assert_eq!(after, before + 3.0);
    }

    #[test]
    fn test_set_draining_gauge() {
        set_draining(true);
        assert_eq!(DRAINING.get(), 1);
    }

    #[test]
    fn test_record_shutdown_drain_observes() {
        let before = SHUTDOWN_DRAIN_SECONDS.get_sample_count();
        record_shutdown_drain(1.5);
        assert_eq!(SHUTDOWN_DRAIN_SECONDS.get_sample_count(), before + 1);
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
    #[serial_test::serial(custom_metrics)]
    fn test_register_custom_metrics_and_record() {
        use crate::proto::liteserver::MetricValue;

        register_custom_metrics("rcmr_model", "1", &[
            ("rcmr_gauge", "gauge"),
            ("rcmr_counter", "counter"),
            ("rcmr_histogram", "histogram"),
        ]);

        // Worker-side ids are per-type declaration ordinals: this model
        // declares one metric of each type, so every local id is 0.
        let gauges = vec![MetricValue { id: 0, value: 42.0 }];
        let counters = vec![MetricValue { id: 0, value: 5.0 }];
        let histograms = vec![MetricValue { id: 0, value: 0.15 }];
        record_custom_metrics("rcmr_model", "1", &gauges, &counters, &histograms);

        let output = gather_metrics();
        assert!(output.contains("lite_server_rcmr_gauge"), "gauge missing: {}", output);
        assert!(output.contains("lite_server_rcmr_counter_total"), "counter missing: {}", output);
        assert!(output.contains("lite_server_rcmr_histogram"), "histogram missing: {}", output);
    }

    #[test]
    #[serial_test::serial(custom_metrics)]
    fn test_record_worker_metrics_with_custom_fields() {
        use crate::proto::liteserver::{MetricValue, Metrics};

        register_custom_metrics("rwmc_model", "1", &[("rwmc_gauge", "gauge")]);

        let metrics = Metrics {
            prefill_ms: 0.0,
            decode_ms: 0.0,
            tokens_generated: 0,
            // Worker-local per-type ordinal (first gauge declared → id 0).
            gauges: vec![MetricValue { id: 0, value: 99.5 }],
            counters: vec![],
            histograms: vec![],
        };
        record_worker_metrics("rwmc_model", "1", Some(&metrics));

        let output = gather_metrics();
        assert!(output.contains("lite_server_rwmc_gauge"), "custom gauge missing: {}", output);
    }

    /// P1 PROM-1 evidence (project-resource-leak-sweep-0815.md): worker-reported
    /// families (`lite_server_prefill_ms`/`decode_ms`/`tokens_generated_total`,
    /// created at :1055-1096) and pre-registered custom families
    /// (`register_custom_metrics` :1103-1160) are DELIBERATELY excluded from the
    /// `remove_version_metrics` purge list (:762-825, doc comment: "excluded:
    /// ... worker-reported custom gauges"). Their (model, version) series
    /// survive unload and accumulate across version churn (tune/profile runs).
    ///
    /// Fixed code purges these families on unload; current code retains them —
    /// this test FAILS (RED) until the leak is addressed.
    #[test]
    #[serial_test::serial(custom_metrics)]
    fn test_worker_metric_series_survive_version_purge() {
        use crate::proto::liteserver::{MetricValue, Metrics};
        let model = "prom_purge_ev_worker";
        let version = "1";

        // Worker-reported families.
        record_worker_metrics(
            model,
            version,
            Some(&Metrics {
                prefill_ms: 5.0,
                decode_ms: 3.0,
                tokens_generated: 7,
                gauges: vec![],
                counters: vec![],
                histograms: vec![],
            }),
        );
        // Pre-registered custom family path (worker-local gauge ordinal 0).
        register_custom_metrics(model, version, &[("purge_ev_custom", "gauge")]);
        record_custom_metrics(
            model,
            version,
            &[MetricValue { id: 0, value: 1.0 }],
            &[],
            &[],
        );

        // Unload: the purge macro must drop every (model, version) series.
        remove_version_metrics(model, version);

        // After unload no series in the worker/custom families may still carry
        // the (model, version) label set.
        for family in REGISTRY.gather().iter() {
            let name = family.get_name();
            let is_worker_family = name.starts_with("lite_server_prefill_ms")
                || name.starts_with("lite_server_decode_ms")
                || name.starts_with("lite_server_tokens_generated_total")
                || name.starts_with("lite_server_purge_ev_custom");
            if !is_worker_family {
                continue;
            }
            for m in family.get_metric() {
                let labels = m.get_label();
                let hit = labels.iter().any(|l| l.get_name() == "model" && l.get_value() == model)
                    && labels
                        .iter()
                        .any(|l| l.get_name() == "version" && l.get_value() == version);
                assert!(
                    !hit,
                    "PROM-1: {name} retains series {{model={model},version={version}}} \
                     after remove_version_metrics — worker/custom families are \
                     excluded from the purge list"
                );
            }
        }
    }

    /// M8 evidence (resource-leak sweep 2026-08-16): ENSEMBLE_STEP_LATENCY
    /// carries a (ensemble, step, model, version, depth) label set, but
    /// `remove_version_metrics` never purges this family — the family's own
    /// doc comment promises the series "age out on sub-model unload" while no
    /// such removal path exists anywhere. Pinned-version ensemble churn
    /// therefore accumulates a histogram series per (sub-model, version)
    /// forever.
    ///
    /// Fixed code purges this family on unload.
    #[test]
    fn test_ensemble_step_latency_series_survive_purge() {
        // The purge enumerates series via REGISTRY.gather, so the family must
        // be registered — production startup always runs register_metrics
        // (idempotent; AlreadyReg after another test is fine).
        let _ = register_metrics();
        let model = "m8_sub";
        let version = "1";
        record_ensemble_step_latency("m8_ens", "m8_step", model, version, 1, 0.01);
        remove_version_metrics(model, version);

        // The (ensemble, step, model, version, depth) series must be gone
        // after unload. Probed directly on the Vec (not REGISTRY.gather,
        // whose registration only happens in init_metrics).
        let hit = ENSEMBLE_STEP_LATENCY
            .get_metric_with_label_values(&["m8_ens", "m8_step", model, version, "1"])
            .map(|h| h.get_sample_count() > 0)
            .unwrap_or(false);
        assert!(
            !hit,
            "M8: ENSEMBLE_STEP_LATENCY retains series \
             {{ensemble=m8_ens,step=m8_step,model={model},version={version}}} \
             after remove_version_metrics — family is missing from the purge list"
        );
    }

    /// G6: warmup runs must be observable — a duration histogram plus a
    /// closed-enum status counter, both per (model, version).
    #[test]
    fn warmup_metrics_record_duration_and_classify_status() {
        let _ = register_metrics();
        let model = "warmup_met_m";
        let version = "1";
        let counter = |status: &str| {
            MODEL_WARMUP_TOTAL
                .with_label_values(&[model, version, status])
                .get()
        };
        let (s0, f0, t0) = (counter("success"), counter("failure"), counter("timeout"));
        let c0 = MODEL_WARMUP_DURATION
            .with_label_values(&[model, version])
            .get_sample_count();

        record_model_warmup(model, version, 0.25, WarmupStatus::Success);
        record_model_warmup(model, version, 0.5, WarmupStatus::Failure);
        record_model_warmup(model, version, 5.0, WarmupStatus::Timeout);

        assert_eq!(counter("success"), s0 + 1.0);
        assert_eq!(counter("failure"), f0 + 1.0);
        assert_eq!(counter("timeout"), t0 + 1.0);
        assert_eq!(
            MODEL_WARMUP_DURATION
                .with_label_values(&[model, version])
                .get_sample_count(),
            c0 + 3,
            "every terminal run observes the duration"
        );
    }

    /// G6 (§6.5 #11): the warmup families carry (model, version) labels, so
    /// they must join the remove_version_metrics purge list — unloading a
    /// version must not leave warmup series behind.
    #[test]
    fn warmup_metrics_purged_on_version_remove() {
        let _ = register_metrics();
        let model = "warmup_purge_m";
        record_model_warmup(model, "1", 0.1, WarmupStatus::Success);
        remove_version_metrics(model, "1");

        let counter_hit = MODEL_WARMUP_TOTAL
            .get_metric_with_label_values(&[model, "1", "success"])
            .map(|c| c.get() > 0.0)
            .unwrap_or(false);
        let hist_hit = MODEL_WARMUP_DURATION
            .get_metric_with_label_values(&[model, "1"])
            .map(|h| h.get_sample_count() > 0)
            .unwrap_or(false);
        assert!(
            !counter_hit && !hist_hit,
            "warmup series for {model}/1 must be purged on unload"
        );
    }

    /// M6b evidence (resource-leak sweep 2026-08-16): `remove_version_metrics`
    /// purges the (model, version) SERIES (so the sibling test
    /// `test_worker_metric_series_survive_version_purge` passes), but the
    /// pre-registered custom metric FAMILY OBJECTS — the entries in
    /// CUSTOM_GAUGE/COUNTER/HISTOGRAM_OBJECTS, CUSTOM_METRIC_INDEX and the
    /// families REGISTERED into the REGISTRY — must also be deregistered once
    /// no live model references them. Otherwise every model that declares a
    /// metric name no previous model used adds a permanent family; /metrics
    /// output and process memory grow monotonically across model churn.
    ///
    /// Fixed code deregisters a family once its last referencing version
    /// unloads (CUSTOM_FAMILY_REFS refcounting).
    #[test]
    #[serial_test::serial(custom_metrics)]
    fn test_custom_metric_family_object_survives_unload() {
        let model = "m6b_model";
        let version = "1";
        // Content-based assertions on the UNIQUE name only — the object vec
        // and index are process-global and mutated by parallel tests, so
        // absolute len before/after is racy.
        assert!(
            !CUSTOM_METRIC_INDEX
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key("m6b_gauge:gauge"),
            "precondition: m6b_gauge must be unregistered"
        );
        // A model declares its custom metric family at worker handshake.
        register_custom_metrics(model, version, &[("m6b_gauge", "gauge")]);
        assert!(
            CUSTOM_METRIC_INDEX
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key("m6b_gauge:gauge"),
            "precondition: the index entry must be created"
        );
        assert!(
            CUSTOM_FAMILY_REFS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get("m6b_gauge:gauge")
                .is_some_and(|s| s.contains(&(model.to_string(), version.to_string()))),
            "precondition: the (model, version) reference must be recorded"
        );
        // Unload the model: its family object + index entry must be removed
        // (no other model references it). Pre-M6b code never deregistered a
        // family object — both survived forever.
        remove_version_metrics(model, version);
        assert!(
            !CUSTOM_METRIC_INDEX
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key("m6b_gauge:gauge"),
            "M6b: index entry must be removed on unload"
        );
        assert!(
            !CUSTOM_FAMILY_REFS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key("m6b_gauge:gauge"),
            "M6b: reference set must be dropped on unload"
        );
        // Idempotence: a second unload of the same version is a no-op.
        remove_version_metrics(model, version);
        assert!(
            !REGISTRY.gather().iter().any(|f| f.get_name() == "lite_server_m6b_gauge"),
            "M6b: custom family lite_server_m6b_gauge still exported after unload — \
             family objects are never deregistered"
        );
    }

    /// Custom-metric id misattribution (found during M6b review, 2026-08-16):
    /// worker-side `metric_id` is a PER-TYPE ordinal within the worker's own
    /// declaration order (python api.py `register_metric`), while the
    /// CUSTOM_*_OBJECTS vecs are PROCESS-GLOBAL. Recording indexes the global
    /// vec with the worker-local id directly, so a second model declaring
    /// DISTINCT metric names reports into the first model's family (or out
    /// of range → silently dropped).
    ///
    /// Fixed code translates worker-local ids through a per-(model, version)
    /// table built at register time (CUSTOM_MODEL_IDS).
    #[test]
    #[serial_test::serial(custom_metrics)]
    fn test_custom_metric_ids_translated_per_model() {
        use crate::proto::liteserver::MetricValue;
        // Two models, distinct gauge names; each worker declares ONE gauge,
        // so both report worker-local id 0.
        register_custom_metrics("cm_a", "1", &[("cm_a_gauge", "gauge")]);
        register_custom_metrics("cm_b", "1", &[("cm_b_gauge", "gauge")]);
        record_custom_metrics("cm_a", "1", &[MetricValue { id: 0, value: 1.0 }], &[], &[]);
        record_custom_metrics("cm_b", "1", &[MetricValue { id: 0, value: 2.0 }], &[], &[]);

        let objects = CUSTOM_GAUGE_OBJECTS.lock().unwrap_or_else(|e| e.into_inner());
        let index = CUSTOM_METRIC_INDEX.lock().unwrap_or_else(|e| e.into_inner());
        let a_idx = index.get("cm_a_gauge:gauge").unwrap().1;
        let b_idx = index.get("cm_b_gauge:gauge").unwrap().1;
        let value = |idx: usize, model: &str| {
            objects[idx]
                .get_metric_with_label_values(&[model, "1"])
                .map(|g| g.get())
                .unwrap_or(0.0)
        };
        assert_eq!(value(a_idx, "cm_a"), 1.0, "A's value must land in A's family");
        assert_eq!(
            value(a_idx, "cm_b"),
            0.0,
            "A's family must not gain B's label set (cross-model misattribution)"
        );
        assert_eq!(
            value(b_idx, "cm_b"),
            2.0,
            "B's value must land in B's family — worker-local id 0 must be \
             translated to B's global slot"
        );
        assert_eq!(value(b_idx, "cm_a"), 0.0, "B's family must not gain A's label set");
    }

    /// Shared-family refcount gap (audit 2026-08-20): two versions declaring
    /// the SAME custom metric name (the common rolling-deploy shape — v1 and
    /// v2 of one model export the same metric) share one family object.
    /// Unloading BOTH versions must deregister the family.
    ///
    /// Current code only drops a family whose ref SET is a singleton of the
    /// unloading version (deregister_unreferenced_custom_families filters
    /// `set.len() == 1 && set.contains(&mv)`) and never removes the unloading
    /// version from a shared set: after v1 unloads the set stays {v1, v2},
    /// so v2's unload also sees len 2 and the family + both phantom refs
    /// leak in REGISTRY forever.
    #[test]
    #[serial_test::serial(custom_metrics)]
    fn test_shared_custom_family_deregistered_after_last_version_unloads() {
        let key = "sharedfam_gauge:gauge";
        assert!(
            !CUSTOM_METRIC_INDEX
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(key),
            "precondition: sharedfam_gauge must be unregistered"
        );

        // v1 and v2 of the same model declare the same metric name.
        register_custom_metrics("sharedfam_m", "1", &[("sharedfam_gauge", "gauge")]);
        register_custom_metrics("sharedfam_m", "2", &[("sharedfam_gauge", "gauge")]);

        // Unloading v1 must keep the family alive — v2 still references it.
        remove_version_metrics("sharedfam_m", "1");
        assert!(
            CUSTOM_METRIC_INDEX
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(key),
            "family must survive while v2 still references it"
        );

        // Unloading v2 (the LAST referencing version) must deregister it.
        remove_version_metrics("sharedfam_m", "2");
        assert!(
            !CUSTOM_METRIC_INDEX
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(key),
            "shared family must be deregistered once no live version references it"
        );
        assert!(
            !CUSTOM_FAMILY_REFS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(key),
            "phantom (model, version) refs must not survive the final unload"
        );
        assert!(
            !REGISTRY
                .gather()
                .iter()
                .any(|f| f.get_name() == "lite_server_sharedfam_gauge"),
            "shared family still exported in /metrics after both versions unloaded"
        );
    }

    /// B16 (leak-gap-audit-0821): VERSION_SWITCHES_TOTAL is excluded from
    /// the standard purge because its from/to labels don't match the
    /// version filter — but with N versions churning, N² series then stay
    /// forever. The routing-history argument holds only for LIVE versions:
    /// series touching the unloaded version (from==v or to==v) must age
    /// out, series between surviving versions stay.
    #[test]
    fn remove_version_metrics_purges_switches_touching_the_version() {
        let _ = register_metrics();
        let model = "b16_switch_m";
        record_version_switch(model, "1", "2");
        record_version_switch(model, "2", "1");
        record_version_switch(model, "1", "3");

        remove_version_metrics(model, "2");

        assert_eq!(
            VERSION_SWITCHES_TOTAL
                .with_label_values(&[model, "1", "3"])
                .get(),
            1.0,
            "switches between surviving versions stay (routing history)"
        );
        assert_eq!(
            VERSION_SWITCHES_TOTAL
                .with_label_values(&[model, "1", "2"])
                .get(),
            0.0,
            "switches TO the unloaded version must age out"
        );
        assert_eq!(
            VERSION_SWITCHES_TOTAL
                .with_label_values(&[model, "2", "1"])
                .get(),
            0.0,
            "switches FROM the unloaded version must age out"
        );
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

    /// Audit (2026-08-22, resource-leak/observability sweep): a `dec` that
    /// lands AFTER `remove_version_metrics` has purged the series must not
    /// resurrect it at -1. Trigger: unload grace times out with requests in
    /// flight (lifecycle.rs warns "unloading anyway"), `drain.abort()` kills
    /// the collector, the purge runs, and a detached `send_batch_with_retry`
    /// task completes afterwards — its `InflightGuard::drop` calls
    /// `dec_in_flight` on the purged labels, and `with_label_values`
    /// recreates the series just to drive it negative. The next load of the
    /// same version then starts the gauge at -N.
    #[test]
    fn late_dec_after_purge_must_not_drive_gauge_negative() {
        // The purge enumerates series via REGISTRY.gather(), so the metric
        // family must be registered for the purge to act (production always
        // registers at startup; ignore AlreadyReg from a parallel test).
        let _ = register_metrics();
        let (model, version) = ("late_dec_purge", "1");
        inc_in_flight(model, version);
        remove_version_metrics(model, version);
        dec_in_flight(model, version);
        assert_eq!(
            IN_FLIGHT_REQUESTS.with_label_values(&[model, version]).get(),
            0.0,
            "a dec arriving after the purge must be a no-op, not resurrect the series at -1"
        );
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
    fn test_queue_depth_floored_at_zero_on_extra_dec() {
        // An unpaired dec (e.g. a late dec after remove_version_metrics
        // purged the series) must not drive the gauge negative — the floor
        // heals the inherent purge/late-dec race (see floor_gauge_at_zero).
        let model = "qd_negative";
        let version = "1";
        inc_queue_depth(model, version); // +1
        dec_queue_depth(model, version); // 0
        dec_queue_depth(model, version); // extra dec, floored at 0
        let value = QUEUE_DEPTH.with_label_values(&[model, version]).get();
        assert_eq!(value, 0.0, "extra dec must be floored at zero, got {}", value);
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
        register_custom_metrics("poison_cm_model", "1", &[("poison_cm", "gauge")]);

        // Poison the CUSTOM_GAUGE_OBJECTS mutex
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = CUSTOM_GAUGE_OBJECTS.lock().unwrap();
            panic!("intentional poison");
        }));

        // Worker-local per-type ordinal 0; the (model, version) must match
        // the registration so the record reaches the (poisoned) vec lock.
        let gauges = vec![MetricValue { id: 0, value: 42.0 }];
        // Must NOT panic — the poisoned lock is recovered via into_inner().
        record_custom_metrics("poison_cm_model", "1", &gauges, &[], &[]);
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
        // G5: a worker dying mid-stream is a server-side failure, NOT a
        // clean end — it must be distinguishable from Done in the metrics.
        assert_eq!(WorkerEof.status_family(), "5xx");
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

    /// Round2 B4:ENSEMBLE_STEP_LATENCY 桶追加 10/30/60——冷启动子模型/慢
    /// step >5s(旧桶顶)不落 +Inf,与 S7 REQUEST_DURATION/TTFT 同模式。
    #[test]
    fn ensemble_step_latency_buckets_extend_past_5s() {
        let _ = register_metrics();
        let model = "ens_bucket_m";
        record_ensemble_step_latency("ens_bucket_e", "s1", model, "1", 0, 15.0);
        let le30 = histogram_bucket_count("liteserver_ensemble_step_latency_seconds", model, 30.0);
        assert!(le30 >= 1, "15s step must land in the new le=30 bucket (got {le30})");
    }

    // ===== Round2 A6: dedicated stream rejection counter =====

    /// Concurrency-limit rejections (P10, ensemble streaming DAG cap) must be
    /// visible apart from other 4xx: they signal capacity exhaustion (scale
    /// up), not client errors. requests_total keeps its record (backward
    /// compat); the dedicated counter carries a bounded `reason` label.
    #[test]
    fn stream_rejected_records_dedicated_counter_with_reason() {
        let _ = register_metrics();
        let model = "a6_rej_m";
        let before = STREAM_REJECTED_TOTAL
            .with_label_values(&[model, "1", "concurrency_limit"])
            .get();
        record_stream_rejected(model, "1", "4xx", 0.001, "concurrency_limit");
        assert_eq!(
            STREAM_REJECTED_TOTAL
                .with_label_values(&[model, "1", "concurrency_limit"])
                .get(),
            before + 1.0
        );
        // requests_total{4xx} keeps its record (dashboards unchanged).
        assert!(REQUESTS_TOTAL.with_label_values(&[model, "1", "4xx"]).get() >= 1.0);
    }

    /// Reasons stay separated: an early_reject must not bump the
    /// concurrency_limit series.
    #[test]
    fn stream_rejected_reasons_are_separated() {
        let _ = register_metrics();
        let model = "a6_rej_sep";
        record_stream_rejected(model, "1", "4xx", 0.001, "early_reject");
        assert_eq!(
            STREAM_REJECTED_TOTAL
                .with_label_values(&[model, "1", "early_reject"])
                .get(),
            1.0
        );
        assert_eq!(
            STREAM_REJECTED_TOTAL
                .with_label_values(&[model, "1", "concurrency_limit"])
                .get(),
            0.0
        );
    }

    // ===== Round2 B2: version-unload label cleanup =====

    /// Unloading a version must remove every per-(model,version) series —
    /// long-running servers (tune/profile campaigns cycle many versions)
    /// otherwise grow the label set without bound. Neighbor versions stay.
    #[test]
    fn remove_version_metrics_clears_all_per_version_series() {
        let _ = register_metrics();
        let model = "b2_clean_m";
        let version = "9";
        // Seed across label shapes: 2-label, 3-label, 4-label families.
        record_request_end(model, version, "2xx", 0.01);
        record_request_end(model, version, "5xx", 0.02);
        inc_queue_depth(model, version);
        set_active_workers(model, version, 2.0);
        set_worker_health(model, version, 0, true);
        set_worker_health(model, version, 1, false);
        record_worker_inference(model, version, 0, 3);
        record_stream_rejected(model, version, "4xx", 0.001, "concurrency_limit");
        inc_health_check(model, version, "ok");
        // MODEL_LOAD_TOTAL is the lifecycle event log — it must SURVIVE the
        // cleanup (operators query it after unload).
        record_model_load(model, version, true);
        // Neighbor version must survive the cleanup.
        record_request_end(model, "8", "2xx", 0.01);

        remove_version_metrics(model, version);

        assert_eq!(
            MODEL_LOAD_TOTAL
                .with_label_values(&[model, version, "load", "success"])
                .get(),
            1.0,
            "load/unload event log must survive cleanup"
        );
        let families = REGISTRY.gather();
        let leftover: Vec<String> = families
            .iter()
            .filter(|mf| mf.get_name() != "liteserver_model_load_total")
            .flat_map(|mf| {
                mf.get_metric()
                    .iter()
                    .filter(|m| {
                        m.get_label()
                            .iter()
                            .any(|l| l.get_name() == "model" && l.get_value() == model)
                            && m.get_label()
                                .iter()
                                .any(|l| l.get_name() == "version" && l.get_value() == version)
                    })
                    .map(|m| format!("{}:{:?}", mf.get_name(), m.get_label()))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(leftover.is_empty(), "series left after cleanup: {leftover:?}");
        assert_eq!(
            REQUESTS_TOTAL.with_label_values(&[model, "8", "2xx"]).get(),
            1.0,
            "neighbor version must be untouched"
        );
    }

    // ===== Round2 B3: info + process metrics =====

    fn gather_family_value(metric_name: &str) -> Option<f64> {
        REGISTRY
            .gather()
            .iter()
            .find(|mf| mf.get_name() == metric_name)
            .and_then(|mf| mf.get_metric().first())
            .map(|m| {
                if m.has_gauge() {
                    m.get_gauge().get_value()
                } else {
                    m.get_counter().get_value()
                }
            })
    }

    /// liteserver_info{version} must be exported with the build version, value 1.
    #[test]
    fn info_metric_reports_build_version() {
        let _ = register_metrics();
        let families = REGISTRY.gather();
        let family = families
            .iter()
            .find(|mf| mf.get_name() == "liteserver_info")
            .expect("liteserver_info must be registered");
        let m = family.get_metric().first().expect("info series must exist");
        let version = m
            .get_label()
            .iter()
            .find(|l| l.get_name() == "version")
            .map(|l| l.get_value().to_string());
        assert_eq!(version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        assert_eq!(m.get_gauge().get_value(), 1.0);
    }

    /// gather_metrics() must refresh and export process metrics (RSS > 0 for
    /// the live process, monotonic CPU seconds, sane start time).
    #[test]
    fn gather_metrics_exports_process_metrics() {
        let _ = register_metrics();
        let _ = gather_metrics();
        let rss = gather_family_value("liteserver_process_resident_memory_bytes")
            .expect("process RSS must be exported");
        assert!(rss > 0.0, "RSS of the live process must be positive, got {rss}");
        let cpu = gather_family_value("liteserver_process_cpu_seconds_total")
            .expect("process CPU seconds must be exported");
        assert!(cpu >= 0.0);
        let start = gather_family_value("liteserver_process_start_time_seconds")
            .expect("process start time must be exported");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!(start > 0.0 && start <= now, "start time must be a past epoch, got {start}");
    }

    /// Thread count is populated where the platform exposes tasks (Linux).
    #[cfg(target_os = "linux")]
    #[test]
    fn gather_metrics_exports_thread_count_on_linux() {
        let _ = register_metrics();
        let _ = gather_metrics();
        let threads = gather_family_value("liteserver_process_threads")
            .expect("process threads must be exported");
        assert!(threads >= 1.0, "live process has at least one thread, got {threads}");
    }

    // ===== Worker memory metrics (per-worker RSS/VIRT + per-version aggregate) =====

    fn own_pid() -> u32 {
        std::process::id()
    }

    fn worker_key(model: &str, version: &str, worker_id: &str) -> WorkerKey {
        (model.to_string(), version.to_string(), worker_id.to_string())
    }

    /// True when the gathered output contains a series of `family` whose label
    /// set contains every (name, value) pair in `expected`.
    fn gathered_has_series(family: &str, expected: &[(&str, &str)]) -> bool {
        REGISTRY.gather().iter().any(|mf| {
            mf.get_name() == family
                && mf.get_metric().iter().any(|m| {
                    expected.iter().all(|(name, value)| {
                        m.get_label()
                            .iter()
                            .any(|l| l.get_name() == *name && l.get_value() == *value)
                    })
                })
        })
    }

    /// A registered live PID must produce positive per-worker RSS and VIRT
    /// gauges after a refresh (the test process itself plays the "worker").
    #[test]
    fn should_sample_worker_memory_for_registered_pid() {
        let _ = register_metrics();
        let (m, v, w) = ("wmem_sample_m", "1", "0");
        set_worker_pid(m, v, 0, own_pid());
        refresh_process_metrics();
        assert!(WORKER_RSS_BYTES.with_label_values(&[m, v, w]).get() > 0);
        assert!(WORKER_VIRT_BYTES.with_label_values(&[m, v, w]).get() > 0);
        clear_worker_pids(m, v);
    }

    /// The per-version aggregate must equal the sum of live per-worker RSS.
    #[test]
    fn should_aggregate_worker_rss_per_model_version() {
        let _ = register_metrics();
        let (m, v) = ("wmem_agg_m", "1");
        set_worker_pid(m, v, 0, own_pid());
        set_worker_pid(m, v, 1, own_pid());
        refresh_process_metrics();
        let w0 = WORKER_RSS_BYTES.with_label_values(&[m, v, "0"]).get();
        let w1 = WORKER_RSS_BYTES.with_label_values(&[m, v, "1"]).get();
        let agg = WORKERS_RSS_BYTES.with_label_values(&[m, v]).get();
        assert!(w0 > 0 && w1 > 0);
        // Concurrent scrapes from sibling tests may re-sample between reads;
        // allow slack while still catching a missing or doubled sum.
        assert!(
            (agg - (w0 + w1)).abs() < 64 * 1024 * 1024,
            "aggregate {agg} must equal w0+w1 {}",
            w0 + w1
        );
        clear_worker_pids(m, v);
    }

    /// A worker PID that no longer exists must have its per-worker series
    /// removed and be dropped from the registry — self-healing for the
    /// crash/eject/kill paths that never call clear_worker_pids.
    #[test]
    fn should_remove_series_when_worker_pid_dead() {
        let _ = register_metrics();
        let (m, v, w) = ("wmem_dead_m", "1", "0");
        set_worker_pid(m, v, 0, own_pid());
        refresh_process_metrics();
        assert!(WORKER_RSS_BYTES.with_label_values(&[m, v, w]).get() > 0);
        // Simulate the process dying: point the entry at a PID that cannot
        // exist (above every platform's PID_MAX).
        {
            let mut st = PROCESS_SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
            let key = worker_key(m, v, w);
            st.worker_pids.get_mut(&key).unwrap().pid = sysinfo::Pid::from(u32::MAX as usize);
        }
        refresh_process_metrics();
        assert!(!gathered_has_series("liteserver_worker_resident_memory_bytes", &[("model", m), ("version", v), ("worker_id", w)]));
        assert!(!gathered_has_series("liteserver_worker_virtual_memory_bytes", &[("model", m), ("version", v), ("worker_id", w)]));
        assert!(!gathered_has_series("liteserver_workers_resident_memory_bytes", &[("model", m), ("version", v)]));
        let st = PROCESS_SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!st.worker_pids.contains_key(&worker_key(m, v, w)));
    }

    /// PID reuse: when the sampled start_time diverges from the primed value,
    /// the OS recycled the PID — treat it as dead and drop the series.
    #[test]
    fn should_drop_series_when_pid_recycled() {
        let _ = register_metrics();
        let (m, v, w) = ("wmem_reuse_m", "1", "0");
        set_worker_pid(m, v, 0, own_pid());
        refresh_process_metrics(); // primes start_time
        {
            let mut st = PROCESS_SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
            let key = worker_key(m, v, w);
            st.worker_pids.get_mut(&key).unwrap().start_time = Some(u64::MAX);
        }
        refresh_process_metrics();
        assert!(!gathered_has_series("liteserver_worker_resident_memory_bytes", &[("model", m), ("version", v), ("worker_id", w)]));
        let st = PROCESS_SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!st.worker_pids.contains_key(&worker_key(m, v, w)));
    }

    /// Respawn re-registers the same worker_id with a new PID; the entry must
    /// be replaced (here the replacement is dead, so the series disappears
    /// instead of sampling the stale live PID).
    #[test]
    fn should_replace_pid_on_respawn_reregistration() {
        let _ = register_metrics();
        let (m, v, w) = ("wmem_respawn_m", "1", "0");
        set_worker_pid(m, v, 0, own_pid());
        refresh_process_metrics();
        assert!(WORKER_RSS_BYTES.with_label_values(&[m, v, w]).get() > 0);
        set_worker_pid(m, v, 0, u32::MAX);
        refresh_process_metrics();
        assert!(!gathered_has_series("liteserver_worker_resident_memory_bytes", &[("model", m), ("version", v), ("worker_id", w)]));
    }

    /// clear_worker_pids drops registry entries AND exported series for
    /// exactly one (model, version); other models stay live.
    #[test]
    fn should_clear_worker_pids_only_for_target_version() {
        let _ = register_metrics();
        let (ma, mb, v, w) = ("wmem_clear_ma", "wmem_clear_mb", "1", "0");
        set_worker_pid(ma, v, 0, own_pid());
        set_worker_pid(mb, v, 0, own_pid());
        refresh_process_metrics();
        assert!(WORKER_RSS_BYTES.with_label_values(&[ma, v, w]).get() > 0);
        clear_worker_pids(ma, v);
        assert!(!gathered_has_series("liteserver_worker_resident_memory_bytes", &[("model", ma), ("version", v)]));
        assert!(!gathered_has_series("liteserver_worker_virtual_memory_bytes", &[("model", ma), ("version", v)]));
        assert!(!gathered_has_series("liteserver_workers_resident_memory_bytes", &[("model", ma), ("version", v)]));
        refresh_process_metrics();
        assert!(WORKER_RSS_BYTES.with_label_values(&[mb, v, w]).get() > 0);
        clear_worker_pids(mb, v);
    }

    /// Registry insert is capped (AGG-1-style bound); overwriting an existing
    /// key is always allowed, even at the cap.
    #[test]
    fn should_reject_registry_insert_beyond_cap() {
        let mut map = std::collections::HashMap::new();
        let entry = || WorkerPidEntry { pid: sysinfo::Pid::from(1usize), start_time: None };
        for i in 0..MAX_WORKER_PID_ENTRIES {
            let key = (format!("m{i}"), "1".to_string(), "0".to_string());
            assert!(worker_registry_insert(&mut map, key, entry()));
        }
        let extra = ("overflow".to_string(), "1".to_string(), "0".to_string());
        assert!(!worker_registry_insert(&mut map, extra, entry()));
        let existing = ("m0".to_string(), "1".to_string(), "0".to_string());
        assert!(worker_registry_insert(&mut map, existing, entry()));
    }

    /// remove_version_metrics must purge all three worker-memory families.
    #[test]
    fn remove_version_metrics_purges_worker_memory_series() {
        let _ = register_metrics();
        let (m, v, w) = ("wmem_purge_m", "9", "0");
        WORKER_RSS_BYTES.with_label_values(&[m, v, w]).set(1);
        WORKER_VIRT_BYTES.with_label_values(&[m, v, w]).set(1);
        WORKERS_RSS_BYTES.with_label_values(&[m, v]).set(1);
        remove_version_metrics(m, v);
        assert!(!gathered_has_series("liteserver_worker_resident_memory_bytes", &[("model", m), ("version", v)]));
        assert!(!gathered_has_series("liteserver_worker_virtual_memory_bytes", &[("model", m), ("version", v)]));
        assert!(!gathered_has_series("liteserver_workers_resident_memory_bytes", &[("model", m), ("version", v)]));
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
        // G5: worker EOF mid-stream counts as an error (kind=worker_eof).
        assert_eq!(WorkerEof.error_kind(), Some("worker_eof"));
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
        crate::test_tracing::ensure_always_on_subscriber();
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

    // ===== /audit 2026-08-12: ensemble batch-0/1 metric registration =====

    /// Resource/observability assumption: a lazily-defined collector that is
    /// never registered is silently absent from /metrics gather() output.
    /// The P10 gauge and m4 histograms must be exported (plan §4.1/§4.3/§6
    /// D40: `ensemble_streaming_active` observes the semaphore in-use count).
    #[test]
    fn ensemble_batch01_metrics_are_registered() {
        let _ = register_metrics(); // AlreadyReg on repeat runs is fine
        let registered: std::collections::HashSet<String> = REGISTRY
            .gather()
            .iter()
            .map(|f| f.get_name().to_string())
            .collect();
        for name in [
            "ensemble_streaming_active",
            "ensemble_autoload_wait_seconds",
            "ensemble_bidi_aggregate_bytes",
            "ensemble_bidi_aggregate_seconds",
        ] {
            assert!(
                registered.contains(name),
                "metric {name} is set/observe()d but never registered — absent from /metrics"
            );
        }
    }
}
