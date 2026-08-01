use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub grpc: GrpcConfig,
    pub metrics: MetricsConfig,
    pub rate_limit: RateLimitConfig,
    pub logging: LoggingConfig,
    pub model_repository: ModelRepositoryConfig,
    pub features: FeaturesConfig,
    pub orchestration: OrchestrationConfig,
    /// CLI-provided defaults that override per-model config when set.
    pub model_defaults: ModelTunables,
    /// Server-level knobs (reconcile loop, file watcher, worker diagnostics).
    pub tunables: ServerTunables,
    /// P7-1 endpoint-class access control (admin/inference/health × http/grpc).
    /// Default (unconfigured) = admin loopback fail-closed, inference/health
    /// public. Per-model `policies.auth` (enforce_auth) is a separate, finer
    /// gate that stacks on top of this coarse class gate.
    pub access_control: AccessControlConfig,
}

/// P7-1 access-control config (蓝图 §4.2). Three endpoint classes; admin and
/// inference carry per-protocol controls, health carries a single control that
/// applies to both protocols (`health: { mode: public }`). An unset cell falls
/// back to its class default (admin → loopback fail-closed; inference/health →
/// public).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AccessControlConfig {
    pub admin: ProtocolControl,
    pub inference: ProtocolControl,
    pub health: HealthControl,
}

/// Per-protocol controls for one class (admin/inference).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProtocolControl {
    #[serde(default)]
    pub http: Option<EndpointControl>,
    #[serde(default)]
    pub grpc: Option<EndpointControl>,
}

/// Health is a single control applied to both protocols (`{ mode: public }`).
/// None = class default (health public).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, transparent)]
pub struct HealthControl(pub Option<EndpointControl>);

/// One endpoint-class × protocol cell. The explicit `mode` tag avoids serde's
/// untagged-representation pitfalls (评审低#13).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode")]
pub enum EndpointControl {
    /// Explicitly open (admin escape hatch).
    #[serde(rename = "public")]
    Public,
    /// Require an API key: `key` is the header name, the secret comes from
    /// `value` / `value_env` / `value_file` (first present wins; resolved at
    /// startup so a missing env/file fails fast).
    #[serde(rename = "key")]
    Key {
        key: String,
        #[serde(default)]
        value: Option<String>,
        #[serde(default)]
        value_env: Option<String>,
        #[serde(default)]
        value_file: Option<String>,
    },
}

/// Server-level tunables (server.yaml `tunables:` section). Defaults preserve
/// the values that were hardcoded before these became configurable (H1-H7).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerTunables {
    /// Window to coalesce a burst of filesystem events into one reconcile run.
    pub reconcile_coalesce_secs: f32,
    /// Per-model/version cooldown between hot reloads.
    pub hot_reload_cooldown_secs: f32,
    /// File watcher debounce window.
    pub watcher_debounce_secs: f32,
    /// Timeout for one worker's FILE_CHANGED hook round-trip.
    pub file_changed_timeout_secs: f32,
    /// Max bytes of a dying worker's stderr retained for crash diagnostics.
    pub worker_stderr_tail_bytes: usize,
    /// How long to wait for an exited worker to flush its stderr.
    pub worker_stderr_drain_secs: f32,
    /// Upper bound for one `python -m lite_server unpack` invocation.
    pub unpack_timeout_secs: f32,
}

impl Default for ServerTunables {
    fn default() -> Self {
        Self {
            reconcile_coalesce_secs: 2.0,
            hot_reload_cooldown_secs: 3.0,
            watcher_debounce_secs: 2.5,
            file_changed_timeout_secs: 60.0,
            worker_stderr_tail_bytes: 64 * 1024,
            worker_stderr_drain_secs: 5.0,
            unpack_timeout_secs: 120.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub http_port: u16,
    pub grpc_port: u16,
    pub metrics_port: u16,
    pub host: String,
    pub timeout: f32,
    pub threads: Option<usize>,
    pub cache_registry: bool,
    /// Max seconds to wait for in-flight requests during graceful shutdown.
    pub graceful_timeout: f32,
    /// HTTP keep-alive timeout in seconds. 0 = disable keep-alive.
    pub keepalive_timeout: f32,
    /// gzip response compression (P1-4). Default false. SSE responses are
    /// excluded (per-event flush semantics); WS upgrades are unaffected.
    pub compression: bool,
    /// TLS server certificate chain PEM path (P5-1). Must be set together with
    /// `tls_key_path`; setting only one is a startup error. Mutually exclusive
    /// with a UDS `server.host`.
    pub tls_cert_path: Option<String>,
    /// TLS private key PEM path (P5-1). See `tls_cert_path`.
    pub tls_key_path: Option<String>,
    /// Client CA bundle PEM path (P5-1): when set, client certificates are
    /// REQUIRED (mTLS). Requires cert+key (setting it alone is an error).
    pub mtls_ca_path: Option<String>,
    /// Minimum TLS version (P5-1): "1.2" (default) or "1.3".
    pub tls_min_version: Option<String>,
    /// P8-1: how long a `sequence_id → worker` affinity mapping is kept after
    /// its last use before the 60s cleanup sweep evicts it.
    pub sequence_ttl_secs: f32,
    /// P8-1: upper bound on tracked `sequence_id` entries (approximate LRU once
    /// exceeded). Bounds memory for unauthenticated sequence hints.
    pub max_sequences: usize,
    /// P8-1 (B2): when an affinity worker's in-flight count exceeds the
    /// least-loaded worker's by more than this absolute amount, fall back to
    /// power-of-two selection (SGLang `--balance-abs-threshold` semantics).
    /// 0 disables the absolute check.
    pub balance_abs_threshold: u32,
    /// P8-1 (B2): relative load-threshold complement to `balance_abs_threshold`
    /// (SGLang `--balance-rel-threshold`, as a multiplier, e.g. 1.5 = +50%).
    /// 0.0 disables the relative check.
    pub balance_rel_threshold: f32,
    /// P9-1 DecoupledInfer: server-side idle timeout (seconds) for a decoupled
    /// stream — if no chunk arrives within this window, the server closes the
    /// stream and cancels the worker (reclaims a channel the model left open).
    /// 0 disables the idle timeout (stream lives until model close / cancel).
    pub decoupled_idle_timeout_secs: f32,
    /// P-FLOW (§4.0.9): global in-flight admission cap for *inference*
    /// requests. When > 0, inference requests beyond this concurrent count are
    /// rejected with 503 / gRPC Unavailable (+ Retry-After). Health/admin
    /// endpoints are exempt (probes must stay reachable under load). 0 =
    /// unlimited (default; behavior unchanged).
    pub max_inflight: usize,
    /// P-FLOW (§4.0.9): per-request body size cap in bytes. When set, HTTP
    /// bodies exceeding it return 413 and gRPC messages return
    /// ResourceExhausted. None = platform default (axum 2MB / tonic 4MB,
    /// verified at implementation); behavior unchanged.
    pub max_request_body_bytes: Option<usize>,
    /// P-XFF: trusted-proxy CIDRs (or bare IPs) whose `X-Forwarded-For` /
    /// `X-Real-IP` headers are honored for client-IP cleansing. Empty
    /// (default) = fail-safe: the direct TCP peer is always used and client
    /// proxy headers are ignored (prevents forged-IP rate-limit bypass). A
    /// fronting gateway/proxy must be listed here for its forwarded client
    /// IPs to reach rate-limiting. Invalid entries fail at startup.
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
    /// P-CORS: global CORS policy (applied when no per-model `policies.cors`
    /// override matches, and to non-model routes). None (default) = CORS
    /// pass-through (no headers attached). See `CorsPolicy`.
    #[serde(default)]
    pub cors: Option<CorsPolicy>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_port: 8000,
            grpc_port: 8001,
            metrics_port: 8002,
            host: "0.0.0.0".to_string(),
            timeout: 30.0,
            threads: None,
            cache_registry: false,
            graceful_timeout: 30.0,
            keepalive_timeout: 5.0,
            compression: false,
            tls_cert_path: None,
            tls_key_path: None,
            mtls_ca_path: None,
            tls_min_version: None,
            sequence_ttl_secs: 3600.0,
            max_sequences: 65536,
            balance_abs_threshold: 2,
            balance_rel_threshold: 1.5,
            decoupled_idle_timeout_secs: 300.0,
            max_inflight: 0,
            max_request_body_bytes: None,
            trusted_proxies: Vec::new(),
            cors: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GrpcConfig {
    pub enabled: bool,
    pub max_workers: usize,
    /// gRPC bind target (P4-1). None = follow `server.host`, falling back to
    /// TCP `127.0.0.1` when `server.host` is a Unix socket — gRPC stays on TCP
    /// unless this is explicitly set to `unix:/path` to bind a UDS. See
    /// `grpc::resolve_grpc_host`.
    pub host: Option<String>,
    /// P7-2: separate admin bind target (`host:port` or `unix:/path`). When set,
    /// a SECOND tonic server is spawned: Admin + health bind here, the main port
    /// keeps LiteServer + health (Admin is removed from the main port). Gives
    /// transport isolation on top of P7-1's logical (class) isolation. A `unix:`
    /// admin_bind defaults to owner-only 0o600 (a world-writable admin socket
    /// would let any local user bypass fail-closed — rejected at startup).
    pub admin_bind: Option<String>,
    /// Unix socket file permission for the gRPC UDS (P4-1), as a decimal mode
    /// (e.g. 438 = 0o666). Default 0o666 for the inference socket — admin UDS
    /// uses a stricter 0o600 (P7-2). Applied via chmod on cfg(unix) only.
    pub socket_mode: u32,
    /// HTTP/2 keepalive ping interval in seconds (P1-2). None = disabled.
    pub http2_keepalive_interval_secs: Option<u64>,
    /// HTTP/2 keepalive ping-ack timeout in seconds (P1-2). Effective default
    /// is 20s when the interval is set; configuring it without an interval
    /// never takes effect (startup warns).
    pub http2_keepalive_timeout_secs: Option<u64>,
    /// HTTP/2 adaptive flow-control window (P1-2): grows the window with BDP.
    /// Default false (tonic/hyper fixed 64KB connection window).
    pub http2_adaptive_window: bool,
    /// HTTP/2 max frame size in bytes (P1-2). None = tonic default (16KB);
    /// 256KB–1MB avoids frame-splitting overhead for large/streaming payloads.
    pub http2_max_frame_size: Option<u32>,
    /// gzip response compression on the LiteServer service (P1-3).
    /// Default false; Admin/health services are never compressed.
    pub response_compression: bool,
    /// TLS server certificate chain PEM path (P5-1). Must be set together with
    /// `tls_key_path`; setting only one is a startup error. Mutually exclusive
    /// with a UDS `grpc.host`.
    pub tls_cert_path: Option<String>,
    /// TLS private key PEM path (P5-1). See `tls_cert_path`.
    pub tls_key_path: Option<String>,
    /// Client CA bundle PEM path (P5-1): when set, client certificates are
    /// REQUIRED (mTLS). Requires cert+key (setting it alone is an error).
    pub mtls_ca_path: Option<String>,
    /// Minimum TLS version (P5-1): "1.2" (default) or "1.3".
    pub tls_min_version: Option<String>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_workers: 10,
            host: None,
            admin_bind: None,
            socket_mode: 0o666,
            http2_keepalive_interval_secs: None,
            http2_keepalive_timeout_secs: None,
            http2_adaptive_window: false,
            http2_max_frame_size: None,
            response_compression: false,
            tls_cert_path: None,
            tls_key_path: None,
            mtls_ca_path: None,
            tls_min_version: None,
        }
    }
}

/// Resolved TLS file group for one side (HTTP or gRPC), P5-1. Produced by
/// `tls_settings()` only when cert+key are both set (pair consistency is
/// enforced by `Config::validate`, which runs before this is consulted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSettings {
    pub cert_path: String,
    pub key_path: String,
    pub mtls_ca_path: Option<String>,
    /// "1.2" (default) or "1.3" — validated values only.
    pub min_version: String,
}

impl ServerConfig {
    /// TLS is enabled iff cert+key are both set.
    pub fn tls_settings(&self) -> Option<TlsSettings> {
        tls_settings_from(
            self.tls_cert_path.as_deref(),
            self.tls_key_path.as_deref(),
            self.mtls_ca_path.as_deref(),
            self.tls_min_version.as_deref(),
        )
    }

    /// P-XFF: parse `trusted_proxies` into CIDR networks, fail-fast on any
    /// unparseable entry (names the offending value in the error). Empty →
    /// empty vec (fail-safe: client proxy headers ignored, peer used).
    pub fn trusted_networks(&self) -> anyhow::Result<Vec<ipnet::IpNet>> {
        self.trusted_proxies
            .iter()
            .map(|entry| {
                crate::client_ip::parse_network(entry).ok_or_else(|| {
                    anyhow::anyhow!(
                        "server.trusted_proxies entry \"{entry}\" is not a valid CIDR or IP address"
                    )
                })
            })
            .collect()
    }
}

impl GrpcConfig {
    /// TLS is enabled iff cert+key are both set.
    pub fn tls_settings(&self) -> Option<TlsSettings> {
        tls_settings_from(
            self.tls_cert_path.as_deref(),
            self.tls_key_path.as_deref(),
            self.mtls_ca_path.as_deref(),
            self.tls_min_version.as_deref(),
        )
    }
}

fn tls_settings_from(
    cert: Option<&str>,
    key: Option<&str>,
    ca: Option<&str>,
    min_version: Option<&str>,
) -> Option<TlsSettings> {
    match (cert, key) {
        (Some(c), Some(k)) => Some(TlsSettings {
            cert_path: c.to_string(),
            key_path: k.to_string(),
            mtls_ca_path: ca.map(String::from),
            min_version: min_version.unwrap_or("1.2").to_string(),
        }),
        _ => None,
    }
}

/// Shared per-side TLS structural checks (P5-1). `side` is "server" or "grpc"
/// for error messages that name the exact YAML path.
fn validate_tls_side(
    side: &str,
    cert: Option<&str>,
    key: Option<&str>,
    ca: Option<&str>,
    min_version: Option<&str>,
    is_uds: bool,
) -> anyhow::Result<()> {
    match (cert, key) {
        (Some(_), None) => anyhow::bail!(
            "{side}.tls_cert_path is set but {side}.tls_key_path is not — TLS requires both"
        ),
        (None, Some(_)) => anyhow::bail!(
            "{side}.tls_key_path is set but {side}.tls_cert_path is not — TLS requires both"
        ),
        _ => {}
    }
    let tls_enabled = cert.is_some() && key.is_some();
    if ca.is_some() && !tls_enabled {
        anyhow::bail!(
            "{side}.mtls_ca_path requires {side}.tls_cert_path and {side}.tls_key_path (mTLS without a server certificate is meaningless)"
        );
    }
    if tls_enabled && is_uds {
        anyhow::bail!(
            "{side} TLS is mutually exclusive with a UDS bind — a Unix socket is already peer-credentialed; remove the unix: host or the TLS settings"
        );
    }
    if let Some(v) = min_version {
        if v != "1.2" && v != "1.3" {
            anyhow::bail!("{side}.tls_min_version must be \"1.2\" or \"1.3\", got \"{v}\"");
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    /// GIE/EPP 指标命名 namespace（P2-1 扩展，D32）：`{namespace}:total_queued_requests`
    /// / `{namespace}:kv_cache_utilization` 暴露到 /metrics，兼容 `vllm` 模式。
    /// 非法 namespace 在启动时快速失败（register_gie_metrics 校验）。
    pub metric_namespace: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            metric_namespace: "liteserver".to_string(),
        }
    }
}

/// Server-wide rate-limiter tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Hard cap on the number of distinct rate-limit buckets (per IP / route
    /// key). Bounds memory under spoofed-source floods where every request
    /// carries a new source IP. 0 = unbounded.
    pub max_buckets: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self { max_buckets: 65_536 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub info_output: Option<String>,
    pub error_output: Option<String>,
    /// Rotation strategy: "none", "size", "daily", "hourly"
    pub rotation: String,
    /// Max log file size in MB (when rotation=size)
    pub max_size: usize,
    /// Number of rotated log files to keep
    pub backup_count: usize,
    /// Inject the system hostname into log filenames (server.log -> server-<host>.log)
    pub hostname_in_log_name: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            info_output: None,
            error_output: None,
            rotation: "none".to_string(),
            max_size: 100,
            backup_count: 7,
            hostname_in_log_name: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelRepositoryConfig {
    pub path: String,
}

impl Default for ModelRepositoryConfig {
    fn default() -> Self {
        Self {
            path: "./model_repo".to_string(),
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeaturesConfig {
    pub timeline: bool,
    pub system_overview: bool,
    pub custom_metrics: bool,
    pub benchmarks: bool,
    pub playground: bool,
    pub alerts: bool,
    pub version_compare: bool,
    pub streaming: bool,
    pub grpc_streaming: bool,
    pub sse: bool,
    pub websocket_streaming: bool,
    pub streaming_metrics: bool,
    /// P5-2 (蓝图 §4.4, D16): 允许 `x-lite-version` pin 绕过权重路由（HTTP 与
    /// gRPC 双侧门控一致）。默认 false（breaking）——生产上 pin 可绕过灰度
    /// 权重，仅灰度/调试环境显式开启。
    pub canary_override: bool,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            timeline: false,
            system_overview: true,
            custom_metrics: false,
            benchmarks: true,
            playground: false,
            alerts: true,
            version_compare: false,
            streaming: true,
            grpc_streaming: true,
            sse: true,
            websocket_streaming: true,
            streaming_metrics: true,
            canary_override: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OrchestrationConfig {
    pub control_mode: String,
    pub poll_interval: u64,
    pub load_models: Vec<String>,
    pub models: Vec<ModelStrategyConfig>,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            control_mode: "explicit".to_string(),
            // Resync backstop for the reconcile task; directory events
            // trigger near-real-time reconciles in between (§P2).
            poll_interval: 30,
            load_models: vec![],
            models: vec![],
        }
    }
}

/// Per-model tunables as optional overrides — the single definition shared by
/// `Config::model_defaults` (config.yaml) and `CliOverrides::tunables` (CLI).
/// Adding a tunable touches: one field here, one line in apply_to, one line
/// in merge_from, plus the resolved field in ModelConfig.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ModelTunables {
    pub max_queue_size: Option<usize>,
    pub max_requests: Option<usize>,
    /// Jitter range for max_requests to prevent thundering herd on worker recycle.
    pub max_requests_jitter: Option<usize>,
    pub request_timeout: Option<f32>,
    pub health_check_interval: Option<f32>,
    // Worker resilience (§3).
    pub ejection_error_threshold: Option<usize>,
    pub ejection_timeout: Option<f32>,
    pub ejection_max_percent: Option<usize>,
    pub max_retries: Option<usize>,
    pub startup_timeout: Option<f32>,
    pub health_check_timeout: Option<f32>,
    /// Consecutive health-probe failures before killing + respawning the worker.
    pub health_check_kill_threshold: Option<usize>,
    pub worker_kill_timeout: Option<f32>,
    pub hook_http_timeout: Option<f32>,
}

impl ModelTunables {
    /// Apply these defaults to a ModelConfig (per-model at load time).
    pub fn apply_to(&self, model: &mut ModelConfig) {
        if let Some(v) = self.max_queue_size {
            model.max_queue_size = v;
        }
        if let Some(v) = self.max_requests {
            model.max_requests = v;
        }
        if let Some(v) = self.max_requests_jitter {
            model.max_requests_jitter = v;
        }
        if let Some(v) = self.request_timeout {
            model.request_timeout = v;
        }
        if let Some(v) = self.health_check_interval {
            model.health_check_interval = v;
        }
        if let Some(v) = self.ejection_error_threshold {
            model.ejection_error_threshold = v;
        }
        if let Some(v) = self.ejection_timeout {
            model.ejection_timeout = v;
        }
        if let Some(v) = self.ejection_max_percent {
            model.ejection_max_percent = v;
        }
        if let Some(v) = self.max_retries {
            model.max_retries = v;
        }
        if let Some(v) = self.startup_timeout {
            model.startup_timeout = v;
        }
        if let Some(v) = self.health_check_timeout {
            model.health_check_timeout = v;
        }
        if let Some(v) = self.health_check_kill_threshold {
            model.health_check_kill_threshold = v;
        }
        if let Some(v) = self.worker_kill_timeout {
            model.worker_kill_timeout = v;
        }
        if let Some(v) = self.hook_http_timeout {
            model.hooks.hook_http_timeout = v;
        }
    }

    /// Overlay fields set in `other` on top of `self` (CLI overrides win over
    /// config.yaml model_defaults).
    pub fn merge_from(&mut self, other: &ModelTunables) {
        if other.max_queue_size.is_some() { self.max_queue_size = other.max_queue_size; }
        if other.max_requests.is_some() { self.max_requests = other.max_requests; }
        if other.max_requests_jitter.is_some() { self.max_requests_jitter = other.max_requests_jitter; }
        if other.request_timeout.is_some() { self.request_timeout = other.request_timeout; }
        if other.health_check_interval.is_some() { self.health_check_interval = other.health_check_interval; }
        if other.ejection_error_threshold.is_some() { self.ejection_error_threshold = other.ejection_error_threshold; }
        if other.ejection_timeout.is_some() { self.ejection_timeout = other.ejection_timeout; }
        if other.ejection_max_percent.is_some() { self.ejection_max_percent = other.ejection_max_percent; }
        if other.max_retries.is_some() { self.max_retries = other.max_retries; }
        if other.startup_timeout.is_some() { self.startup_timeout = other.startup_timeout; }
        if other.health_check_timeout.is_some() { self.health_check_timeout = other.health_check_timeout; }
        if other.health_check_kill_threshold.is_some() { self.health_check_kill_threshold = other.health_check_kill_threshold; }
        if other.worker_kill_timeout.is_some() { self.worker_kill_timeout = other.worker_kill_timeout; }
        if other.hook_http_timeout.is_some() { self.hook_http_timeout = other.hook_http_timeout; }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelStrategyConfig {
    pub name: String,
    pub load_policy: String,
    pub versions_to_load: Vec<String>,
    pub default_version: Option<String>,
    pub max_loaded_versions: Option<usize>,
    /// Initial traffic weights per version (§4.3 canary). Applied to
    /// `ModelEntry.weights`; versions not listed get weight 0.
    pub weights: Option<std::collections::HashMap<String, u32>>,
}

impl Default for ModelStrategyConfig {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: vec![],
            default_version: None,
            max_loaded_versions: None,
            weights: None,
        }
    }
}

/// HTTP hook configuration for worker lifecycle events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpHookConfig {
    pub url: String,
    #[serde(default = "default_http_method")]
    pub method: String,
    pub body_template: Option<String>,
}

fn default_http_method() -> String {
    "POST".to_string()
}

/// Worker lifecycle hook configuration.
/// Shell commands and HTTP hooks are both fire-and-forget, non-blocking.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerHooksConfig {
    pub on_ready: Option<String>,
    pub on_exit: Option<String>,
    pub on_error: Option<String>,
    pub on_ready_http: Option<HttpHookConfig>,
    pub on_exit_http: Option<HttpHookConfig>,
    pub on_error_http: Option<HttpHookConfig>,
    /// Seconds before a lifecycle HTTP hook request times out (§3).
    pub hook_http_timeout: f32,
}

impl Default for WorkerHooksConfig {
    fn default() -> Self {
        Self {
            on_ready: None,
            on_exit: None,
            on_error: None,
            on_ready_http: None,
            on_exit_http: None,
            on_error_http: None,
            hook_http_timeout: 5.0,
        }
    }
}

// ===== Per-model policies (declared in model config.yaml, enforced by Rust) =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitPolicy {
    pub requests_per_minute: f64,
    #[serde(default = "default_rl_key")]
    pub key: String, // "route" | "ip"
    #[serde(default)]
    pub burst: Option<f64>, // None → 1.5× rpm
}

fn default_rl_key() -> String {
    "route".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CorsPolicy {
    pub allow_origins: Vec<String>,
    pub allow_methods: Vec<String>,
    pub allow_headers: Vec<String>,
    /// P-CORS: response headers exposed to the browser (e.g. x-request-id,
    /// x-processing-time-ms). Empty = none exposed.
    pub expose_headers: Vec<String>,
    /// P-CORS: send `Access-Control-Allow-Credentials: true`. Default false.
    /// When true, `allow_origins` must NOT include `*` (a wildcard is inert
    /// under exact Origin matching, but a real browser never sends Origin `*`
    /// — configure explicit origins instead).
    pub allow_credentials: bool,
    /// P-CORS: preflight cache duration in seconds. Default 7200 (Chrome's
    /// cap; values above it are clamped by the browser).
    pub max_age_secs: u32,
}

impl Default for CorsPolicy {
    fn default() -> Self {
        Self {
            allow_origins: Vec::new(),
            allow_methods: Vec::new(),
            allow_headers: Vec::new(),
            expose_headers: Vec::new(),
            allow_credentials: false,
            max_age_secs: 7200,
        }
    }
}

fn default_auth_header() -> String {
    "X-API-Key".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthPolicy {
    /// Header carrying the API key.
    #[serde(default = "default_auth_header")]
    pub header: String,
    /// Allowed keys. Empty = any non-empty value passes. Entries of the form
    /// `${VAR}` are expanded from the environment at config load; an unset
    /// variable fails the load (fail-closed).
    #[serde(default)]
    pub keys: Vec<String>,
}

/// Presence enables per-request access logging for the model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestLogPolicy {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelPolicies {
    pub rate_limit: Option<RateLimitPolicy>,
    pub cors: Option<CorsPolicy>,
    pub auth: Option<AuthPolicy>,
    pub request_log: Option<RequestLogPolicy>,
}

impl ModelPolicies {
    /// True when no policy is configured at all.
    pub fn is_empty(&self) -> bool {
        self.rate_limit.is_none()
            && self.cors.is_none()
            && self.auth.is_none()
            && self.request_log.is_none()
    }
}

/// P-FLOW B1 (§4.0.9): action when a request waits longer than
/// `queue_timeout_secs` (Triton `QueuePolicy.timeout_action`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueueTimeoutAction {
    /// Let `request_timeout` govern (no proactive rejection) — the default.
    Delay,
    /// Return 503 / gRPC Unavailable once the queue delay elapses.
    Reject,
}

impl Default for QueueTimeoutAction {
    fn default() -> Self {
        QueueTimeoutAction::Delay
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelConfig {
    pub name: String,
    pub max_batch_size: usize,
    pub batch_timeout: f32,
    pub stream: bool,
    pub continuous_batching: bool,
    pub accelerator: Option<String>,
    pub devices: Option<serde_json::Value>,
    pub workers_per_device: Option<usize>,
    pub max_queue_size: usize,
    pub hot_reload: bool,
    pub hot_reload_patterns: Vec<String>,
    pub adaptive_batching: bool,
    pub min_batch_timeout: f32,
    pub adaptive_queue_threshold: usize,
    /// Per-request hard timeout in seconds. 0 = disabled.
    pub request_timeout: f32,
    /// Auto-restart worker after this many requests. 0 = disabled.
    pub max_requests: usize,
    /// Random jitter added to max_requests to prevent thundering herd. 0 = disabled.
    pub max_requests_jitter: usize,
    /// Active health check interval in seconds. 0 = disabled.
    pub health_check_interval: f32,
    /// Worker lifecycle hooks (shell commands and HTTP callbacks).
    pub hooks: WorkerHooksConfig,
    /// Per-model policies (rate limit / CORS / auth / request log), enforced
    /// by the Rust HTTP/gRPC layer.
    pub policies: ModelPolicies,
    // ===== Worker resilience (§3 — configurable, defaults preserve prior behavior) =====
    /// Consecutive errors before a worker is ejected. 0 = never eject.
    pub ejection_error_threshold: usize,
    /// Seconds a worker stays ejected before auto-recovery.
    pub ejection_timeout: f32,
    /// Max % of workers that may be ejected at once (1-100).
    pub ejection_max_percent: usize,
    /// Max retry attempts on a different worker for a failed batch. 0 = no retry.
    pub max_retries: usize,
    /// Max seconds to wait for a worker's "ready" handshake.
    pub startup_timeout: f32,
    /// Seconds per health-check probe before timing out.
    pub health_check_timeout: f32,
    /// Consecutive health-probe failures before killing + respawning the
    /// worker. 0 = never kill (ejection-only). Requires health_check_interval > 0.
    pub health_check_kill_threshold: usize,
    /// Seconds to wait for the OS to reap a killed worker process.
    pub worker_kill_timeout: f32,
    /// P-FLOW B1 (§4.0.9): max seconds a request may wait in the queue before
    /// `queue_timeout_action` applies. 0 = disabled (default).
    pub queue_timeout_secs: f32,
    /// P-FLOW B1: action when `queue_timeout_secs` elapses (default `delay`).
    pub queue_timeout_action: QueueTimeoutAction,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            max_batch_size: 1,
            batch_timeout: 0.0,
            stream: false,
            continuous_batching: false,
            accelerator: None,
            devices: None,
            workers_per_device: None,
            max_queue_size: 1000,
            hot_reload: false,
            hot_reload_patterns: vec!["*.py".to_string()],
            adaptive_batching: false,
            min_batch_timeout: 0.001,
            adaptive_queue_threshold: 10,
            request_timeout: 0.0,
            max_requests: 0,
            max_requests_jitter: 0,
            health_check_interval: 15.0,
            hooks: WorkerHooksConfig::default(),
            policies: ModelPolicies::default(),
            ejection_error_threshold: 3,
            ejection_timeout: 30.0,
            ejection_max_percent: 50,
            max_retries: 3,
            startup_timeout: 60.0,
            health_check_timeout: 5.0,
            health_check_kill_threshold: 0,
            worker_kill_timeout: 10.0,
            queue_timeout_secs: 0.0,
            queue_timeout_action: QueueTimeoutAction::default(),
        }
    }
}

/// Reject float values that would later panic at `Duration::from_secs_f*`.
/// A duration-like config field must be finite and non-negative (0 = disabled).
fn check_duration_secs(name: &str, v: f32) -> anyhow::Result<()> {
    if !v.is_finite() {
        anyhow::bail!("config field `{name}` must be a finite number of seconds, got {v}");
    }
    if v < 0.0 {
        anyhow::bail!("config field `{name}` must be >= 0 seconds, got {v}");
    }
    Ok(())
}

impl ModelConfig {
    /// Validate duration-like float fields so a YAML typo or negative CLI
    /// override fails fast here instead of panicking later at
    /// `Duration::from_secs_f*` when the inference queue / worker is built.
    pub fn validate(&self) -> anyhow::Result<()> {
        check_duration_secs("batch_timeout", self.batch_timeout)?;
        check_duration_secs("min_batch_timeout", self.min_batch_timeout)?;
        check_duration_secs("request_timeout", self.request_timeout)?;
        check_duration_secs("health_check_interval", self.health_check_interval)?;
        check_duration_secs("ejection_timeout", self.ejection_timeout)?;
        check_duration_secs("startup_timeout", self.startup_timeout)?;
        check_duration_secs("health_check_timeout", self.health_check_timeout)?;
        check_duration_secs("worker_kill_timeout", self.worker_kill_timeout)?;
        check_duration_secs("hook_http_timeout", self.hooks.hook_http_timeout)?;
        Ok(())
    }
}

/// Returns the Unix socket path if `host` starts with `unix:`, otherwise `None`.
pub fn unix_socket_path(host: &str) -> Option<&str> {
    host.strip_prefix("unix:")
}

pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    config.validate()?;
    Ok(config)
}

pub fn load_model_config(path: &Path) -> anyhow::Result<ModelConfig> {
    if !path.exists() {
        return Ok(ModelConfig::default());
    }
    let content = std::fs::read_to_string(path)?;
    let mut config: ModelConfig = serde_yaml::from_str(&content)?;
    expand_policy_env_vars(&mut config)?;
    Ok(config)
}

/// Expand `${VAR}` entries in `policies.auth.keys` from the environment.
/// Fail-closed: an unset variable is a load error, never an empty key.
fn expand_policy_env_vars(config: &mut ModelConfig) -> anyhow::Result<()> {
    let Some(auth) = config.policies.auth.as_mut() else {
        return Ok(());
    };
    for key in auth.keys.iter_mut() {
        if let Some(var) = key.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
            *key = std::env::var(var).map_err(|_| {
                anyhow::anyhow!(
                    "policies.auth.keys references ${{{var}}} but the variable is not set"
                )
            })?;
        }
    }
    Ok(())
}

/// Structural ensemble detection: true only when the config.yaml content has a
/// top-level `ensemble` key. A string-contains check would false-positive on
/// YAML comments or description strings mentioning "ensemble:".
pub fn config_content_is_ensemble(content: &str) -> bool {
    serde_yaml::from_str::<serde_yaml::Value>(content)
        .ok()
        .and_then(|v| v.get("ensemble").map(|_| ()))
        .is_some()
}

/// CLI 参数覆盖集。None/bool 默认值 = 不覆盖（保持 YAML 或内置默认值）。
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub port: Option<u16>,
    pub host: Option<String>,
    pub model_repo: Option<String>,
    pub threads: Option<usize>,
    pub timeout: Option<f32>,
    pub log_level: Option<String>,
    pub log_info_output: Option<String>,
    pub log_error_output: Option<String>,
    pub log_rotation: Option<String>,
    pub grpc_port: Option<u16>,
    pub metrics_port: Option<u16>,
    pub no_grpc: bool,
    pub no_metrics: bool,
    pub no_streaming_metrics: bool,
    pub graceful_timeout: Option<f32>,
    pub keepalive_timeout: Option<f32>,
    /// Per-model tunables overridden via CLI flags.
    pub tunables: ModelTunables,
}

impl Config {
    /// 用 CLI 参数覆盖当前配置，未指定的字段保持不变。
    pub fn apply_overrides(&mut self, cli: &CliOverrides) {
        if let Some(p) = cli.port {
            self.server.http_port = p;
        }
        if let Some(ref h) = cli.host {
            self.server.host = h.clone();
        }
        if let Some(ref r) = cli.model_repo {
            self.model_repository.path = r.clone();
        }
        if let Some(t) = cli.threads {
            self.server.threads = Some(t);
        }
        if let Some(t) = cli.timeout {
            self.server.timeout = t;
        }
        if let Some(ref l) = cli.log_level {
            self.logging.level = l.clone();
        }
        if let Some(ref p) = cli.log_info_output {
            self.logging.info_output = Some(p.clone());
        }
        if let Some(ref p) = cli.log_error_output {
            self.logging.error_output = Some(p.clone());
        }
        if let Some(ref r) = cli.log_rotation {
            self.logging.rotation = r.clone();
        }
        if let Some(gp) = cli.grpc_port {
            self.server.grpc_port = gp;
        }
        if let Some(mp) = cli.metrics_port {
            self.server.metrics_port = mp;
        }
        if cli.no_grpc {
            self.grpc.enabled = false;
        }
        if cli.no_metrics {
            self.metrics.enabled = false;
        }
        if cli.no_streaming_metrics {
            self.features.streaming_metrics = false;
        }
        self.model_defaults.merge_from(&cli.tunables);
        if let Some(t) = cli.graceful_timeout {
            self.server.graceful_timeout = t;
        }
        if let Some(t) = cli.keepalive_timeout {
            self.server.keepalive_timeout = t;
        }
    }

    /// Apply CLI model defaults to a ModelConfig (called per-model at load time).
    pub fn apply_model_defaults(&self, model: &mut ModelConfig) {
        self.model_defaults.apply_to(model);
    }

    /// Validate server-level duration-like fields (ServerConfig + ServerTunables).
    /// Per-model fields are validated on `ModelConfig::validate` at load time;
    /// `model_defaults` reach a `ModelConfig` only through `apply_to`, so they are
    /// checked there rather than enumerated here.
    pub fn validate(&self) -> anyhow::Result<()> {
        check_duration_secs("server.timeout", self.server.timeout)?;
        check_duration_secs("server.graceful_timeout", self.server.graceful_timeout)?;
        check_duration_secs("server.keepalive_timeout", self.server.keepalive_timeout)?;
        check_duration_secs("server.sequence_ttl_secs", self.server.sequence_ttl_secs)?;
        if self.server.balance_rel_threshold < 0.0 || !self.server.balance_rel_threshold.is_finite() {
            anyhow::bail!(
                "config field `server.balance_rel_threshold` must be a finite non-negative multiplier, got {}",
                self.server.balance_rel_threshold
            );
        }
        check_duration_secs("tunables.reconcile_coalesce_secs", self.tunables.reconcile_coalesce_secs)?;
        check_duration_secs("tunables.hot_reload_cooldown_secs", self.tunables.hot_reload_cooldown_secs)?;
        check_duration_secs("tunables.watcher_debounce_secs", self.tunables.watcher_debounce_secs)?;
        check_duration_secs("tunables.file_changed_timeout_secs", self.tunables.file_changed_timeout_secs)?;
        check_duration_secs("tunables.worker_stderr_drain_secs", self.tunables.worker_stderr_drain_secs)?;
        check_duration_secs("tunables.unpack_timeout_secs", self.tunables.unpack_timeout_secs)?;
        self.validate_tls()?;
        Ok(())
    }

    /// P5-1 TLS/mTLS structural validation (both sides, same rules):
    /// cert/key must be set as a pair; `mtls_ca_path` requires the pair;
    /// TLS is mutually exclusive with a UDS bind; `tls_min_version` is
    /// "1.2" or "1.3" only. File contents are checked later at TLS store load.
    fn validate_tls(&self) -> anyhow::Result<()> {
        validate_tls_side(
            "server",
            self.server.tls_cert_path.as_deref(),
            self.server.tls_key_path.as_deref(),
            self.server.mtls_ca_path.as_deref(),
            self.server.tls_min_version.as_deref(),
            unix_socket_path(&self.server.host).is_some(),
        )?;
        let grpc_is_uds = unix_socket_path(&crate::grpc::resolve_grpc_host(
            self.grpc.host.as_deref(),
            &self.server.host,
        ))
        .is_some();
        validate_tls_side(
            "grpc",
            self.grpc.tls_cert_path.as_deref(),
            self.grpc.tls_key_path.as_deref(),
            self.grpc.mtls_ca_path.as_deref(),
            self.grpc.tls_min_version.as_deref(),
            grpc_is_uds,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // --- Default values ---

    #[test]
    fn test_server_config_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.http_port, 8000);
        assert_eq!(cfg.grpc_port, 8001);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.timeout, 30.0);
        assert_eq!(cfg.graceful_timeout, 30.0);
        assert_eq!(cfg.keepalive_timeout, 5.0);
    }

    #[test]
    fn test_config_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.server.http_port, 8000);
        assert!(cfg.grpc.enabled);
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.model_repository.path, "./model_repo");
        assert!(cfg.features.streaming);
        assert!(cfg.features.websocket_streaming);
        assert!(cfg.features.streaming_metrics);
        // P5-2 (蓝图 §4.4, D16): x-lite-version pin 默认关闭（breaking）。
        assert!(!cfg.features.canary_override);
        assert_eq!(cfg.orchestration.control_mode, "explicit");
        assert_eq!(cfg.orchestration.poll_interval, 30);
    }

    #[test]
    fn test_canary_override_deserializes_from_yaml() {
        let cfg: Config = serde_yaml::from_str("features:\n  canary_override: true\n").unwrap();
        assert!(cfg.features.canary_override);
        // Unset → default false (breaking 默认关).
        let cfg: Config = serde_yaml::from_str("features:\n  streaming: true\n").unwrap();
        assert!(!cfg.features.canary_override);
    }

    #[test]
    fn test_model_config_defaults() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.max_batch_size, 1);
        assert!(!cfg.stream);
        assert!(!cfg.continuous_batching);
        assert_eq!(cfg.max_queue_size, 1000);
    }

    // --- load_config ---

    #[test]
    fn test_load_config_valid_yaml() {
        let dir = std::env::temp_dir().join("lite-server-config-test-valid");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("server.yaml");
        fs::write(&path, "server:\n  http_port: 9090\n  host: 127.0.0.1\n").unwrap();

        let cfg = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.server.http_port, 9090);
        assert_eq!(cfg.server.host, "127.0.0.1");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_config_missing_file() {
        let result = load_config("/tmp/nonexistent-lite-server-config.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_invalid_yaml() {
        let dir = std::env::temp_dir().join("lite-server-config-test-invalid");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("bad.yaml");
        fs::write(&path, "server: [invalid").unwrap();

        let result = load_config(path.to_str().unwrap());
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_config_partial_fields() {
        let dir = std::env::temp_dir().join("lite-server-config-test-partial");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("partial.yaml");
        fs::write(&path, "server:\n  http_port: 7777\n").unwrap();

        let cfg = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.server.http_port, 7777);
        // Other fields should be defaults
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.grpc_port, 8001);

        let _ = fs::remove_dir_all(&dir);
    }

    // --- load_model_config ---

    #[test]
    fn test_load_model_config_valid() {
        let dir = std::env::temp_dir().join("lite-server-model-config-valid");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("config.yaml");
        fs::write(&path, "max_batch_size: 4\nstream: true\n").unwrap();

        let cfg = load_model_config(&path).unwrap();
        assert_eq!(cfg.max_batch_size, 4);
        assert!(cfg.stream);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_model_config_missing_file_returns_default() {
        let path = Path::new("/tmp/nonexistent-lite-server-model-config.yaml");
        let cfg = load_model_config(path).unwrap();
        assert_eq!(cfg.max_batch_size, 1);
        assert!(!cfg.stream);
    }

    #[test]
    fn test_load_model_config_empty_file_returns_default() {
        let dir = std::env::temp_dir().join("lite-server-model-config-empty");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("config.yaml");
        fs::write(&path, "").unwrap();

        let cfg = load_model_config(&path).unwrap();
        assert_eq!(cfg.max_batch_size, 1);

        let _ = fs::remove_dir_all(&dir);
    }

    // --- Serialization roundtrip ---

    #[test]
    fn test_orchestration_section_parses_poll_interval() {
        let yaml = "orchestration:\n  control_mode: auto\n  poll_interval: 10\n  load_models:\n    - m1\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.orchestration.control_mode, "auto");
        assert_eq!(cfg.orchestration.poll_interval, 10);
        assert_eq!(cfg.orchestration.load_models, vec!["m1".to_string()]);
    }

    #[test]
    fn test_config_yaml_roundtrip() {
        let cfg = Config::default();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.server.http_port, cfg.server.http_port);
        assert_eq!(parsed.server.host, cfg.server.host);
        assert_eq!(parsed.grpc.enabled, cfg.grpc.enabled);
    }

    #[test]
    fn test_grpc_http2_config_parses() {
        let yaml = "grpc:\n  http2_keepalive_interval_secs: 30\n  http2_keepalive_timeout_secs: 5\n  http2_adaptive_window: true\n  http2_max_frame_size: 1048576\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.grpc.http2_keepalive_interval_secs, Some(30));
        assert_eq!(cfg.grpc.http2_keepalive_timeout_secs, Some(5));
        assert!(cfg.grpc.http2_adaptive_window);
        assert_eq!(cfg.grpc.http2_max_frame_size, Some(1048576));
    }

    #[test]
    fn should_default_metric_namespace_to_liteserver() {
        let cfg = MetricsConfig::default();
        assert_eq!(cfg.metric_namespace, "liteserver");
    }

    #[test]
    fn should_parse_custom_metric_namespace() {
        let yaml = "metrics:\n  metric_namespace: vllm\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.metrics.metric_namespace, "vllm");
        assert!(cfg.metrics.enabled, "enabled keeps its default when unset");
    }

    #[test]
    fn test_grpc_http2_config_defaults_off() {
        // P1-2: everything off by default — no keepalive, tonic-default window/frame.
        let cfg = Config::default();
        assert_eq!(cfg.grpc.http2_keepalive_interval_secs, None);
        assert_eq!(cfg.grpc.http2_keepalive_timeout_secs, None);
        assert!(!cfg.grpc.http2_adaptive_window);
        assert_eq!(cfg.grpc.http2_max_frame_size, None);
    }

    #[test]
    fn test_grpc_admin_bind_parse_and_default() {
        // P7-2: admin_bind is unset by default (single server, all 3 services).
        assert!(Config::default().grpc.admin_bind.is_none());
        let yaml = "grpc:\n  admin_bind: unix:/var/run/lite-server-admin.sock\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            cfg.grpc.admin_bind.as_deref(),
            Some("unix:/var/run/lite-server-admin.sock")
        );
        let yaml = "grpc:\n  admin_bind: 127.0.0.1:19090\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.grpc.admin_bind.as_deref(), Some("127.0.0.1:19090"));
    }

    #[test]
    fn test_grpc_response_compression_config() {
        // P1-3: off by default; opt-in via grpc.response_compression.
        assert!(!Config::default().grpc.response_compression);
        let yaml = "grpc:\n  response_compression: true\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.grpc.response_compression);
    }

    #[test]
    fn test_server_compression_config() {
        // P1-4: off by default; opt-in via server.compression.
        assert!(!Config::default().server.compression);
        let yaml = "server:\n  compression: true\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.server.compression);
    }

    // ===== P-XFF: trusted_proxies =====

    #[test]
    fn trusted_proxies_defaults_empty() {
        // fail-safe default: empty → client proxy headers ignored, peer used.
        assert!(Config::default().server.trusted_proxies.is_empty());
        assert!(ServerConfig::default().trusted_networks().unwrap().is_empty());
    }

    #[test]
    fn trusted_proxies_parses_cidr_and_bare_ip() {
        let cfg: Config = serde_yaml::from_str(
            "server:\n  trusted_proxies: [\"10.0.0.0/8\", \"192.168.0.1\"]\n",
        )
        .unwrap();
        let nets = cfg.server.trusted_networks().unwrap();
        assert_eq!(nets.len(), 2);
        use std::net::IpAddr;
        let ip = |s: &str| -> IpAddr { s.parse().unwrap() };
        assert!(nets[0].contains(&ip("10.1.2.3")));
        assert!(nets[1].contains(&ip("192.168.0.1")));
    }

    #[test]
    fn trusted_proxies_invalid_entry_fails_fast() {
        let cfg: Config =
            serde_yaml::from_str("server:\n  trusted_proxies: [\"not-a-network\"]\n").unwrap();
        let err = cfg.server.trusted_networks().unwrap_err();
        assert!(
            err.to_string().contains("not-a-network"),
            "error must name the offending entry: {err}"
        );
    }

    #[test]
    fn test_logging_hostname_in_log_name_from_yaml() {
        // Explicit `true` in server.yaml is honored.
        let yaml = "logging:\n  hostname_in_log_name: true\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.logging.hostname_in_log_name);

        // Omitting the field falls back to the serde default (false).
        let yaml = "logging:\n  level: debug\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(!cfg.logging.hostname_in_log_name);
    }

    #[test]
    fn test_examples_logging_yaml_loads_with_hostname() {
        // The 11_logging example ships hostname_in_log_name: true; ensure it loads.
        let cfg = load_config("examples/11_logging/server.yaml").unwrap();
        assert!(cfg.logging.hostname_in_log_name);
        assert!(cfg.logging.info_output.is_some());
    }

    #[test]
    fn test_model_config_yaml_roundtrip() {
        let cfg = ModelConfig {
            max_batch_size: 8,
            stream: true,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.max_batch_size, 8);
        assert!(parsed.stream);
    }

    // --- apply_overrides ---

    #[test]
    fn test_apply_overrides_partial() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            port: Some(9090),
            host: Some("127.0.0.1".to_string()),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.server.http_port, 9090);
        assert_eq!(cfg.server.host, "127.0.0.1");
        // Unchanged fields keep defaults
        assert_eq!(cfg.server.grpc_port, 8001);
        assert!(cfg.grpc.enabled);
    }

    #[test]
    fn test_apply_overrides_none_keeps_defaults() {
        let mut cfg = Config::default();
        let overrides = CliOverrides::default();
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.server.http_port, 8000);
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert!(cfg.grpc.enabled);
        assert!(cfg.metrics.enabled);
        assert!(cfg.features.streaming_metrics);
    }

    #[test]
    fn test_apply_overrides_bool_flags() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            no_grpc: true,
            no_metrics: true,
            no_streaming_metrics: true,
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert!(!cfg.grpc.enabled);
        assert!(!cfg.metrics.enabled);
        assert!(!cfg.features.streaming_metrics);
    }

    #[test]
    fn test_apply_overrides_log_level_propagates() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            log_level: Some("debug".to_string()),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.logging.level, "debug");
    }

    // --- Model defaults ---

    #[test]
    fn test_model_defaults_none_by_default() {
        let cfg = Config::default();
        assert!(cfg.model_defaults.max_queue_size.is_none());
        assert!(cfg.model_defaults.max_requests.is_none());
        assert!(cfg.model_defaults.request_timeout.is_none());
        assert!(cfg.model_defaults.health_check_interval.is_none());
    }

    #[test]
    fn test_apply_overrides_model_defaults() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            tunables: ModelTunables {
                max_queue_size: Some(500),
                max_requests: Some(10000),
                request_timeout: Some(60.0),
                ..Default::default()
            },
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.model_defaults.max_queue_size, Some(500));
        assert_eq!(cfg.model_defaults.max_requests, Some(10000));
        assert_eq!(cfg.model_defaults.request_timeout, Some(60.0));
    }

    #[test]
    fn test_apply_model_defaults_overrides_model_config() {
        let cfg = Config {
            model_defaults: ModelTunables {
                max_queue_size: Some(200),
                max_requests: Some(5000),
                request_timeout: Some(45.0),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut model = ModelConfig::default();
        assert_eq!(model.max_queue_size, 1000); // default
        assert_eq!(model.max_requests, 0); // default
        assert_eq!(model.request_timeout, 0.0); // default

        cfg.apply_model_defaults(&mut model);
        assert_eq!(model.max_queue_size, 200);
        assert_eq!(model.max_requests, 5000);
        assert_eq!(model.request_timeout, 45.0);
    }

    #[test]
    fn test_apply_model_defaults_none_keeps_model_config() {
        let cfg = Config::default(); // all model_defaults are None

        let mut model = ModelConfig {
            max_queue_size: 500,
            max_requests: 100,
            request_timeout: 10.0,
            ..Default::default()
        };

        cfg.apply_model_defaults(&mut model);
        assert_eq!(model.max_queue_size, 500); // unchanged
        assert_eq!(model.max_requests, 100); // unchanged
        assert_eq!(model.request_timeout, 10.0); // unchanged
    }

    #[test]
    fn test_model_defaults_yaml_roundtrip() {
        let cfg = Config {
            model_defaults: ModelTunables {
                max_queue_size: Some(300),
                max_requests: Some(8000),
                request_timeout: Some(120.0),
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.model_defaults.max_queue_size, Some(300));
        assert_eq!(parsed.model_defaults.max_requests, Some(8000));
        assert_eq!(parsed.model_defaults.request_timeout, Some(120.0));
    }

    // --- ServerTunables (tunables: section, H1-H7) ---

    #[test]
    fn test_server_tunables_defaults_match_former_hardcoded_values() {
        let t = ServerTunables::default();
        assert_eq!(t.reconcile_coalesce_secs, 2.0);
        assert_eq!(t.hot_reload_cooldown_secs, 3.0);
        assert_eq!(t.watcher_debounce_secs, 2.5);
        assert_eq!(t.file_changed_timeout_secs, 60.0);
        assert_eq!(t.worker_stderr_tail_bytes, 64 * 1024);
        assert_eq!(t.worker_stderr_drain_secs, 5.0);
        assert_eq!(t.unpack_timeout_secs, 120.0);
    }

    #[test]
    fn test_server_tunables_yaml_roundtrip() {
        let yaml = "tunables:\n  reconcile_coalesce_secs: 1.5\n  unpack_timeout_secs: 60.0\n";
        let parsed: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.tunables.reconcile_coalesce_secs, 1.5);
        assert_eq!(parsed.tunables.unpack_timeout_secs, 60.0);
        // Unset fields fall back to the former hardcoded values.
        assert_eq!(parsed.tunables.hot_reload_cooldown_secs, 3.0);
        assert_eq!(parsed.tunables.watcher_debounce_secs, 2.5);
    }

    // --- health_check_interval ---

    #[test]
    fn test_health_check_interval_default() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.health_check_interval, 15.0);
    }

    #[test]
    fn test_health_check_interval_zero_disables() {
        let cfg = ModelConfig {
            health_check_interval: 0.0,
            ..Default::default()
        };
        assert_eq!(cfg.health_check_interval, 0.0);
    }

    #[test]
    fn test_apply_overrides_health_check_interval() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            tunables: ModelTunables {
                health_check_interval: Some(30.0),
                ..Default::default()
            },
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);
        assert_eq!(cfg.model_defaults.health_check_interval, Some(30.0));
    }

    #[test]
    fn test_model_tunables_merge_from_cli_wins() {
        let mut yaml_defaults = ModelTunables {
            max_queue_size: Some(100),
            request_timeout: Some(30.0),
            ..Default::default()
        };
        let cli = ModelTunables {
            max_queue_size: Some(500),
            ..Default::default()
        };
        yaml_defaults.merge_from(&cli);
        assert_eq!(yaml_defaults.max_queue_size, Some(500), "CLI field wins");
        assert_eq!(yaml_defaults.request_timeout, Some(30.0), "unset CLI field preserves YAML value");
    }

    #[test]
    fn test_apply_overrides_merges_into_existing_model_defaults() {
        // config.yaml model_defaults + CLI flags: CLI wins per-field, other
        // YAML defaults survive.
        let mut cfg = Config {
            model_defaults: ModelTunables {
                max_queue_size: Some(100),
                max_retries: Some(5),
                ..Default::default()
            },
            ..Default::default()
        };
        let overrides = CliOverrides {
            tunables: ModelTunables {
                max_queue_size: Some(500),
                ..Default::default()
            },
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);
        assert_eq!(cfg.model_defaults.max_queue_size, Some(500));
        assert_eq!(cfg.model_defaults.max_retries, Some(5));
    }

    #[test]
    fn test_apply_model_defaults_health_check_interval() {
        let cfg = Config {
            model_defaults: ModelTunables {
                health_check_interval: Some(10.0),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut model = ModelConfig::default();
        assert_eq!(model.health_check_interval, 15.0); // default
        cfg.apply_model_defaults(&mut model);
        assert_eq!(model.health_check_interval, 10.0);
    }

    #[test]
    fn test_health_check_interval_yaml_roundtrip() {
        let cfg = ModelConfig {
            health_check_interval: 20.0,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.health_check_interval, 20.0);
    }

    // --- health_check_kill_threshold ---

    #[test]
    fn test_health_check_kill_threshold_default() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.health_check_kill_threshold, 0, "kill should be disabled by default");
    }

    #[test]
    fn test_health_check_kill_threshold_yaml_roundtrip() {
        let yaml = "health_check_kill_threshold: 5\n";
        let cfg: ModelConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.health_check_kill_threshold, 5);
    }

    #[test]
    fn test_apply_model_defaults_health_check_kill_threshold() {
        let cfg = Config {
            model_defaults: ModelTunables {
                health_check_kill_threshold: Some(4),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut model = ModelConfig::default();
        cfg.apply_model_defaults(&mut model);
        assert_eq!(model.health_check_kill_threshold, 4);
    }

    // --- per-model policies ---

    // ===== P-CORS: CorsPolicy extended fields + server.cors =====

    #[test]
    fn cors_policy_defaults_credentials_off_max_age_7200() {
        let p = CorsPolicy::default();
        assert!(!p.allow_credentials);
        assert_eq!(p.max_age_secs, 7200);
        assert!(p.expose_headers.is_empty());
    }

    #[test]
    fn server_cors_defaults_none() {
        assert!(Config::default().server.cors.is_none());
    }

    #[test]
    fn server_cors_parses_from_yaml() {
        let yaml = "server:\n  cors:\n    allow_origins: [\"https://app.example.com\"]\n    allow_methods: [\"GET\", \"POST\"]\n    allow_headers: [\"content-type\"]\n    expose_headers: [\"x-request-id\"]\n    allow_credentials: true\n    max_age_secs: 3600\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        let cors = cfg.server.cors.expect("server.cors parsed");
        assert_eq!(cors.allow_origins, vec!["https://app.example.com"]);
        assert_eq!(cors.allow_methods, vec!["GET", "POST"]);
        assert_eq!(cors.expose_headers, vec!["x-request-id"]);
        assert!(cors.allow_credentials);
        assert_eq!(cors.max_age_secs, 3600);
    }


    #[test]
    fn test_policies_default_empty() {
        let cfg = ModelConfig::default();
        assert!(cfg.policies.is_empty());
    }

    #[test]
    fn test_policies_yaml_roundtrip() {
        let yaml = r#"
name: m
policies:
  rate_limit: { requests_per_minute: 100, key: ip, burst: 20 }
  cors:
    allow_origins: ["https://app.example"]
    allow_methods: ["POST"]
    allow_headers: ["content-type"]
  auth: { header: "Authorization", keys: ["sk-a", "sk-b"] }
  request_log: {}
"#;
        let cfg: ModelConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!cfg.policies.is_empty());
        let rl = cfg.policies.rate_limit.unwrap();
        assert_eq!(rl.requests_per_minute, 100.0);
        assert_eq!(rl.key, "ip");
        assert_eq!(rl.burst, Some(20.0));
        let cors = cfg.policies.cors.unwrap();
        assert_eq!(cors.allow_origins, vec!["https://app.example"]);
        let auth = cfg.policies.auth.unwrap();
        assert_eq!(auth.header, "Authorization");
        assert_eq!(auth.keys, vec!["sk-a", "sk-b"]);
        assert!(cfg.policies.request_log.is_some());
    }

    #[test]
    fn test_auth_policy_defaults() {
        let yaml = "policies:\n  auth: {}\n";
        let cfg: ModelConfig = serde_yaml::from_str(yaml).unwrap();
        let auth = cfg.policies.auth.unwrap();
        assert_eq!(auth.header, "X-API-Key");
        assert!(auth.keys.is_empty(), "empty keys = any non-empty value passes");
    }

    #[test]
    fn test_auth_keys_env_expansion() {
        std::env::set_var("LITE_TEST_POLICY_KEY", "sk-from-env");
        let dir = std::env::temp_dir().join("lite-server-policy-env-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.yaml");
        std::fs::write(&path, "policies:\n  auth:\n    keys: [\"${LITE_TEST_POLICY_KEY}\", \"sk-static\"]\n").unwrap();
        let cfg = load_model_config(&path).unwrap();
        let auth = cfg.policies.auth.unwrap();
        assert_eq!(auth.keys, vec!["sk-from-env", "sk-static"]);
        std::env::remove_var("LITE_TEST_POLICY_KEY");
    }

    #[test]
    fn test_auth_keys_env_expansion_fail_closed() {
        std::env::remove_var("LITE_TEST_POLICY_MISSING");
        let dir = std::env::temp_dir().join("lite-server-policy-env-fail-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.yaml");
        std::fs::write(&path, "policies:\n  auth:\n    keys: [\"${LITE_TEST_POLICY_MISSING}\"]\n").unwrap();
        let err = load_model_config(&path).unwrap_err();
        assert!(
            err.to_string().contains("LITE_TEST_POLICY_MISSING"),
            "error must name the missing variable: {err}"
        );
    }

    // --- Unix socket path detection ---

    #[test]
    fn test_unix_socket_path_detects_unix_prefix() {
        assert_eq!(unix_socket_path("unix:/tmp/lite-server.sock"), Some("/tmp/lite-server.sock"));
        assert_eq!(unix_socket_path("unix:./lite-server.sock"), Some("./lite-server.sock"));
    }

    #[test]
    fn test_unix_socket_path_returns_none_for_tcp_host() {
        assert_eq!(unix_socket_path("0.0.0.0"), None);
        assert_eq!(unix_socket_path("127.0.0.1"), None);
        assert_eq!(unix_socket_path("[::1]"), None);
    }

    // --- Config YAML with new fields ---

    #[test]
    fn test_load_config_with_unix_host_and_timeouts() {
        let dir = std::env::temp_dir().join("lite-server-config-test-unix");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("server.yaml");
        fs::write(
            &path,
            "server:\n  host: unix:/tmp/test.sock\n  graceful_timeout: 60.0\n  keepalive_timeout: 10.0\n",
        )
        .unwrap();

        let cfg = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.server.host, "unix:/tmp/test.sock");
        assert_eq!(cfg.server.graceful_timeout, 60.0);
        assert_eq!(cfg.server.keepalive_timeout, 10.0);

        let _ = fs::remove_dir_all(&dir);
    }

    // --- CLI overrides for new fields ---

    #[test]
    fn test_apply_overrides_graceful_timeout() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            graceful_timeout: Some(60.0),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);
        assert_eq!(cfg.server.graceful_timeout, 60.0);
    }

    #[test]
    fn test_apply_overrides_keepalive_timeout() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            keepalive_timeout: Some(0.0),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);
        assert_eq!(cfg.server.keepalive_timeout, 0.0);
    }

    #[test]
    fn test_new_fields_yaml_roundtrip() {
        let cfg = Config {
            server: ServerConfig {
                host: "unix:/tmp/test.sock".to_string(),
                graceful_timeout: 60.0,
                keepalive_timeout: 10.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.server.host, "unix:/tmp/test.sock");
        assert_eq!(parsed.server.graceful_timeout, 60.0);
        assert_eq!(parsed.server.keepalive_timeout, 10.0);
    }

    // --- max_requests_jitter ---

    #[test]
    fn test_max_requests_jitter_default() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.max_requests_jitter, 0);
    }

    #[test]
    fn test_max_requests_jitter_yaml_roundtrip() {
        let cfg = ModelConfig {
            max_requests: 100,
            max_requests_jitter: 10,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.max_requests, 100);
        assert_eq!(parsed.max_requests_jitter, 10);
    }

    // --- Worker Hooks ---

    #[test]
    fn test_worker_hooks_default_empty() {
        let cfg = ModelConfig::default();
        assert!(cfg.hooks.on_ready.is_none());
        assert!(cfg.hooks.on_exit.is_none());
        assert!(cfg.hooks.on_error.is_none());
    }

    #[test]
    fn test_worker_hooks_yaml_roundtrip() {
        let cfg = ModelConfig {
            hooks: WorkerHooksConfig {
                on_ready: Some("echo ready".to_string()),
                on_exit: Some("echo exit".to_string()),
                on_error: Some("echo error".to_string()),
                on_ready_http: Some(HttpHookConfig {
                    url: "http://localhost/ready".to_string(),
                    method: "POST".to_string(),
                    body_template: Some(r#"{"model":"$MODEL"}"#.to_string()),
                }),
                on_exit_http: None,
                on_error_http: None,
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.hooks.on_ready, Some("echo ready".to_string()));
        assert_eq!(parsed.hooks.on_exit, Some("echo exit".to_string()));
        assert!(parsed.hooks.on_ready_http.is_some());
        assert_eq!(parsed.hooks.on_ready_http.as_ref().unwrap().method, "POST");
    }

    #[test]
    fn test_http_hook_config_default_method() {
        // When method is not specified, should default to POST
        let yaml = r#"url: "http://localhost/hook""#;
        let parsed: HttpHookConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.method, "POST");
    }

    // --- rate_limit config (#7) ---

    #[test]
    fn test_rate_limit_config_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.rate_limit.max_buckets, 65_536);
    }

    #[test]
    fn test_rate_limit_config_yaml_roundtrip() {
        let cfg = Config {
            rate_limit: RateLimitConfig { max_buckets: 4096 },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.rate_limit.max_buckets, 4096);
    }

    #[test]
    fn test_negative_float_tunable_is_rejected_at_config_boundary() {
        // B1: a negative or non-finite float tunable must be rejected at the
        // config boundary (load_config / apply_overrides / load_model) so it
        // never reaches Duration::from_secs_f* and panics the server.
        //
        // serde accepts the value unchanged (ModelConfig is #[serde(default)]),
        // so without validation a YAML typo or `--health-check-interval -1`
        // would crash at model load.
        let cfg: ModelConfig =
            serde_yaml::from_str("health_check_interval: -1.0\n").expect("parses");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("health_check_interval"),
            "validation should name the offending field; got: {err}"
        );

        // Non-finite values are rejected too (NaN/Inf also panic Duration):
        let mut cfg = ModelConfig::default();
        cfg.request_timeout = f32::NAN;
        assert!(cfg.validate().is_err());

        // B1 regression: hook_http_timeout is NOT validated — negative/NaN
        // values panic at Duration::from_secs_f32 in worker/mod.rs:98 when an
        // HTTP lifecycle hook fires. All 8 other duration fields are checked.
        let cfg: ModelConfig =
            serde_yaml::from_str("hooks:\n  hook_http_timeout: -1.0\n").expect("parses");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("hook_http_timeout"),
            "B1: hook_http_timeout must be validated; got: {err}"
        );

        // Valid defaults pass:
        assert!(ModelConfig::default().validate().is_ok());

        // Server-level + tunables fields are validated on Config too:
        let mut cfg = Config::default();
        cfg.server.timeout = -1.0;
        assert!(cfg.validate().is_err());
        cfg.server.timeout = 30.0;
        cfg.tunables.watcher_debounce_secs = f32::INFINITY;
        assert!(cfg.validate().is_err());
        assert!(Config::default().validate().is_ok());
    }

    // ===== P5-1: TLS/mTLS config validation =====

    fn tls_err(cfg: &Config) -> String {
        cfg.validate().unwrap_err().to_string()
    }

    #[test]
    fn tls_disabled_by_default_is_valid() {
        assert!(Config::default().validate().is_ok());
        assert!(ServerConfig::default().tls_settings().is_none());
        assert!(GrpcConfig::default().tls_settings().is_none());
    }

    #[test]
    fn tls_cert_without_key_is_error_http_and_grpc() {
        let mut cfg = Config::default();
        cfg.server.tls_cert_path = Some("/c.pem".into());
        assert!(tls_err(&cfg).contains("server.tls_cert_path"));

        let mut cfg = Config::default();
        cfg.grpc.tls_cert_path = Some("/c.pem".into());
        assert!(tls_err(&cfg).contains("grpc.tls_cert_path"));
    }

    #[test]
    fn tls_key_without_cert_is_error_http_and_grpc() {
        let mut cfg = Config::default();
        cfg.server.tls_key_path = Some("/k.pem".into());
        assert!(tls_err(&cfg).contains("server.tls_key_path"));

        let mut cfg = Config::default();
        cfg.grpc.tls_key_path = Some("/k.pem".into());
        assert!(tls_err(&cfg).contains("grpc.tls_key_path"));
    }

    #[test]
    fn mtls_ca_alone_is_error_http_and_grpc() {
        // 蓝图测试项：mtls_ca_path 单独配置报错（无 cert/key 的 mTLS 无意义）。
        let mut cfg = Config::default();
        cfg.server.mtls_ca_path = Some("/ca.pem".into());
        assert!(tls_err(&cfg).contains("server.mtls_ca_path"));

        let mut cfg = Config::default();
        cfg.grpc.mtls_ca_path = Some("/ca.pem".into());
        assert!(tls_err(&cfg).contains("grpc.mtls_ca_path"));
    }

    #[test]
    fn tls_and_uds_are_mutually_exclusive() {
        // HTTP: server.host 为 UDS + TLS → 报错。
        let mut cfg = Config::default();
        cfg.server.host = "unix:/tmp/x.sock".into();
        cfg.server.tls_cert_path = Some("/c.pem".into());
        cfg.server.tls_key_path = Some("/k.pem".into());
        assert!(tls_err(&cfg).contains("UDS"));

        // gRPC: 显式 grpc.host unix: + mtls_ca → 报错（蓝图测试项 UDS+mtls_ca）。
        let mut cfg = Config::default();
        cfg.grpc.host = Some("unix:/tmp/g.sock".into());
        cfg.grpc.tls_cert_path = Some("/c.pem".into());
        cfg.grpc.tls_key_path = Some("/k.pem".into());
        cfg.grpc.mtls_ca_path = Some("/ca.pem".into());
        assert!(tls_err(&cfg).contains("UDS"));

        // server.host 为 UDS 时 gRPC 回退 TCP 127.0.0.1（P4-1）——gRPC TLS 合法。
        let mut cfg = Config::default();
        cfg.server.host = "unix:/tmp/x.sock".into();
        cfg.grpc.tls_cert_path = Some("/c.pem".into());
        cfg.grpc.tls_key_path = Some("/k.pem".into());
        assert!(cfg.validate().is_ok(), "gRPC on TCP fallback may use TLS");
    }

    #[test]
    fn tls_min_version_must_be_12_or_13() {
        for bad in ["1.1", "1.0", "tls13", "v1.3"] {
            let mut cfg = Config::default();
            cfg.server.tls_min_version = Some(bad.into());
            assert!(tls_err(&cfg).contains("tls_min_version"), "{bad}");

            let mut cfg = Config::default();
            cfg.grpc.tls_min_version = Some(bad.into());
            assert!(tls_err(&cfg).contains("tls_min_version"), "{bad}");
        }
        for ok in ["1.2", "1.3"] {
            let mut cfg = Config::default();
            cfg.server.tls_cert_path = Some("/c.pem".into());
            cfg.server.tls_key_path = Some("/k.pem".into());
            cfg.server.tls_min_version = Some(ok.into());
            assert!(cfg.validate().is_ok(), "{ok}");
        }
    }

    #[test]
    fn tls_settings_resolve_defaults_and_fields() {
        let cfg = ServerConfig {
            tls_cert_path: Some("/c.pem".into()),
            tls_key_path: Some("/k.pem".into()),
            mtls_ca_path: Some("/ca.pem".into()),
            ..Default::default()
        };
        let s = cfg.tls_settings().expect("cert+key set");
        assert_eq!(s.cert_path, "/c.pem");
        assert_eq!(s.key_path, "/k.pem");
        assert_eq!(s.mtls_ca_path.as_deref(), Some("/ca.pem"));
        assert_eq!(s.min_version, "1.2", "default min version");
    }
}
