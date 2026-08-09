//! P7-1 endpoint-class access control (蓝图 §4.2, D13/D14).
//!
//! Coarse gate in front of every HTTP route and gRPC RPC, keyed by endpoint
//! CLASS (`Admin` / `Inference` / `Health`) × protocol (`http` / `grpc`).
//! Default (unconfigured) is **fail-closed for admin** (loopback only) and
//! **public for inference/health** — see D14. This is independent of, and stacks
//! in front of, the per-model `policies.auth` gate (`enforce_auth`).
//!
//! loopback is decided from the transport peer address, NEVER from `X-Forwarded-
//! For` (XFF is client-forgable; aligned with P-XFF). UDS has no peer address →
//! treated as loopback.

use crate::callback::Protocol;
use crate::config::{AccessControlConfig, EndpointControl};
use crate::error::AppError;
use std::sync::Arc;
use subtle::{Choice, ConstantTimeEq};

/// Endpoint class for access-control decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointClass {
    Admin,
    Inference,
    Health,
}

impl EndpointClass {
    /// OTel span 属性 `endpoint.class` 的取值（P-TRACE 分类采样，
    /// telemetry::otel::PerClassSampler 依据此值分流采样率）。
    pub fn as_str(&self) -> &'static str {
        match self {
            EndpointClass::Admin => "admin",
            EndpointClass::Inference => "inference",
            EndpointClass::Health => "health",
        }
    }
}

/// Map a wire-protocol to the access-control protocol axis. SSE/WebSocket ride
/// the HTTP stack (same middleware), so they share the `http` cell.
fn protocol_axis(protocol: Protocol) -> ProtocolAxis {
    match protocol {
        Protocol::Grpc => ProtocolAxis::Grpc,
        Protocol::Http | Protocol::Sse | Protocol::WebSocket | Protocol::Http2 => ProtocolAxis::Http,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtocolAxis {
    Http,
    Grpc,
}

/// Abstract read-only header lookup so `check` is testable without coupling to
/// axum/tonic header types. Impl'd for `axum::http::HeaderMap` (HTTP middleware)
/// and `tonic::metadata::MetadataMap` (gRPC interceptor); `HashMap` in tests.
pub trait HeaderLookup {
    fn header(&self, name: &str) -> Option<String>;
}

impl HeaderLookup for axum::http::HeaderMap {
    fn header(&self, name: &str) -> Option<String> {
        self.get(name).and_then(|v| v.to_str().ok()).map(String::from)
    }
}

impl HeaderLookup for tonic::metadata::MetadataMap {
    fn header(&self, name: &str) -> Option<String> {
        self.get(name).and_then(|v| v.to_str().ok()).map(String::from)
    }
}

/// One resolved cell (class × protocol) after env/file/value resolution.
#[derive(Debug, Clone, Default)]
enum ResolvedControl {
    /// Unset → fall back to the class default (admin=loopback, else public).
    #[default]
    Unconfigured,
    Public,
    /// Require a key: compare the named header to `secret` in constant time.
    /// `fingerprint` = SHA-256 hex 前缀（12 字符）of `secret`——审计归因用
    /// （D27），日志不落密钥本体。
    Key { header: String, secret: Vec<u8>, fingerprint: String },
}

#[derive(Debug, Clone, Default)]
struct CellPolicy {
    http: ResolvedControl,
    grpc: ResolvedControl,
}

/// Resolved access-control policy. Cheap to clone (small); shared via `Arc`.
#[derive(Debug, Clone, Default)]
pub struct AccessControl {
    admin: CellPolicy,
    inference: CellPolicy,
    health: CellPolicy,
}

impl AccessControl {
    /// Resolve a config into a runtime policy: reads `value_env` / `value_file`
    /// eagerly so a missing env var or unreadable file fails fast at startup.
    pub fn build(cfg: &AccessControlConfig) -> Result<Self, AppError> {
        let health = resolve_one(cfg.health.0.as_ref())?;
        Ok(Self {
            admin: resolve_cell(cfg.admin.http.as_ref(), cfg.admin.grpc.as_ref())?,
            inference: resolve_cell(cfg.inference.http.as_ref(), cfg.inference.grpc.as_ref())?,
            health: CellPolicy { http: health.clone(), grpc: health },
        })
    }

    fn cell(&self, class: EndpointClass, axis: ProtocolAxis) -> &ResolvedControl {
        let cp = match class {
            EndpointClass::Admin => &self.admin,
            EndpointClass::Inference => &self.inference,
            EndpointClass::Health => &self.health,
        };
        match axis {
            ProtocolAxis::Http => &cp.http,
            ProtocolAxis::Grpc => &cp.grpc,
        }
    }

    /// Authorize an (class, protocol) request given the presented headers and
    /// whether the transport peer is loopback.
    pub fn check(
        &self,
        class: EndpointClass,
        protocol: Protocol,
        headers: &dyn HeaderLookup,
        is_loopback: bool,
    ) -> bool {
        let axis = protocol_axis(protocol);
        match self.cell(class, axis) {
            ResolvedControl::Public => true,
            ResolvedControl::Key { header, secret, .. } => match headers.header(header) {
                Some(presented) => ct_eq(presented.as_bytes(), secret),
                None => false,
            },
            // D14: unconfigured admin → loopback fail-closed; inference/health → public.
            ResolvedControl::Unconfigured => match class {
                EndpointClass::Admin => is_loopback,
                EndpointClass::Inference | EndpointClass::Health => true,
            },
        }
    }

    /// True if the admin class would deny a non-loopback peer under the current
    /// policy — used by startup pre-checks to warn operators who relied on the
    /// pre-P7-1 "bind = open" admin behavior (蓝图 §6.2 迁移保障).
    pub fn admin_denies_non_loopback(&self) -> bool {
        matches!(
            self.cell(EndpointClass::Admin, ProtocolAxis::Http),
            ResolvedControl::Unconfigured
        ) && matches!(
            self.cell(EndpointClass::Admin, ProtocolAxis::Grpc),
            ResolvedControl::Unconfigured
        )
    }

    /// SHA-256 指纹（12 hex）of the configured key for (class, protocol)——
    /// 审计归因（D27 key 指纹字段）：标识用了哪把 key（轮换前后可区分），
    /// 日志不落密钥本体。非 key 模式（public/未配置）返回 None。
    pub fn key_fingerprint(&self, class: EndpointClass, protocol: Protocol) -> Option<&str> {
        match self.cell(class, protocol_axis(protocol)) {
            ResolvedControl::Key { fingerprint, .. } => Some(fingerprint),
            _ => None,
        }
    }
}

fn resolve_cell(
    http: Option<&EndpointControl>,
    grpc: Option<&EndpointControl>,
) -> Result<CellPolicy, AppError> {
    Ok(CellPolicy {
        http: resolve_one(http)?,
        grpc: resolve_one(grpc)?,
    })
}

fn resolve_one(ctrl: Option<&EndpointControl>) -> Result<ResolvedControl, AppError> {
    Ok(match ctrl {
        None => ResolvedControl::Unconfigured,
        Some(EndpointControl::Public) => ResolvedControl::Public,
        Some(EndpointControl::Key { key, value, value_env, value_file }) => {
            if key.trim().is_empty() {
                return Err(AppError::Config(
                    "access_control key mode requires a non-empty header name (`key`)".into(),
                ));
            }
            let secret = resolve_secret(value.as_deref(), value_env.as_deref(), value_file.as_deref())?;
            ResolvedControl::Key { fingerprint: secret_fingerprint(&secret), header: key.clone(), secret }
        }
    })
}

/// Resolve the secret from the first present source (value > value_env >
/// value_file). An empty/unset/empty-file source is an error — Key mode with no
/// usable secret would silently deny everyone, so fail fast at startup.
fn resolve_secret(
    value: Option<&str>,
    value_env: Option<&str>,
    value_file: Option<&str>,
) -> Result<Vec<u8>, AppError> {
    if let Some(v) = value {
        if !v.is_empty() {
            return Ok(v.as_bytes().to_vec());
        }
    }
    if let Some(env) = value_env {
        return match std::env::var(env) {
            Ok(v) if !v.is_empty() => Ok(v.into_bytes()),
            _ => Err(AppError::Config(format!(
                "access_control value_env '{env}' is unset or empty"
            ))),
        };
    }
    if let Some(path) = value_file {
        let bytes = std::fs::read(path).map_err(|e| {
            AppError::Config(format!("access_control value_file '{path}' unreadable: {e}"))
        })?;
        let v = String::from_utf8_lossy(&bytes).trim().to_string();
        if v.is_empty() {
            return Err(AppError::Config(format!(
                "access_control value_file '{path}' is empty"
            )));
        }
        return Ok(v.into_bytes());
    }
    Err(AppError::Config(
        "access_control key mode requires one of value / value_env / value_file".into(),
    ))
}

/// Constant-time byte comparison (timing side-channel on the secret). Length
/// inequality short-circuits — the header NAME and presence are already
/// observable on the wire; the value bytes are compared without data-dependent
/// branching.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = Choice::from(1);
    for (x, y) in a.iter().zip(b.iter()) {
        acc &= x.ct_eq(y);
    }
    bool::from(acc)
}

/// Constant-time "is `value` one of `keys`" for the per-model API-key list.
/// Unlike `keys.iter().any(|k| k == value)`, this compares EVERY key in full
/// regardless of an early hit — a plain `==`/`any` short-circuits at the first
/// matching byte and at the first matching key, so a correct key returns
/// faster than a wrong one and leaks via timing. `ct_eq` is constant-time per
/// byte; folding with `|=` (not `||`) never short-circuits across keys.
pub(crate) fn ct_contains(keys: &[String], value: &str) -> bool {
    let v = value.as_bytes();
    let mut found = false;
    for k in keys {
        found |= ct_eq(k.as_bytes(), v);
    }
    found
}

/// OpenAI-compact 专属鉴权门(openai-compact 方案 2026-08-09):只锁 /v1
/// 5 端点,不影响其他协议。与 access-control Key 模式共享 EndpointControl
/// 形状与常量时间比对,差异:header 为 `authorization` 时接受 `Bearer <key>`
/// (RFC 6750 剥前缀后比对,官方 openai SDK 的标准形式),无 loopback 豁免
/// (配了就要 key,本地开发不配即可)。由 openai_compact::mount 独家挂载
/// 为 /v1 路由的 route_layer;解析在启动期完成(缺 secret 源 fail-fast)。
///
/// 常量时间性质:header 缺失/scheme 存在性在线上本就可见,提前返回不泄漏
/// secret;secret 字节始终经 `ct_eq` 全量比对。
#[derive(Debug, Clone)]
pub struct OpenaiAuthGate {
    header: String,
    secret: Vec<u8>,
}

impl OpenaiAuthGate {
    /// Resolve the /v1 gate from config. `None`/`Public` → no gate (explicitly
    /// open); `Key` → fail-fast resolution of the secret source.
    pub fn build(ctrl: Option<&EndpointControl>) -> Result<Option<Self>, AppError> {
        match ctrl {
            None | Some(EndpointControl::Public) => Ok(None),
            Some(EndpointControl::Key { key, value, value_env, value_file }) => {
                if key.trim().is_empty() {
                    return Err(AppError::Config(
                        "openai_compact.auth key mode requires a non-empty header name (`key`)".into(),
                    ));
                }
                let secret = resolve_secret(value.as_deref(), value_env.as_deref(), value_file.as_deref())?;
                Ok(Some(Self { header: key.clone(), secret }))
            }
        }
    }

    /// Authorize a /v1 request: `authorization` header → accept `Bearer <key>`
    /// (scheme case-insensitive) or the bare value; any other configured header
    /// → full-value comparison (access-control semantics).
    pub fn check(&self, headers: &dyn HeaderLookup) -> bool {
        let Some(presented) = headers.header(&self.header) else {
            return false;
        };
        if self.header.eq_ignore_ascii_case("authorization") {
            if let Some(token) = strip_bearer(&presented) {
                if ct_eq(token.as_bytes(), &self.secret) {
                    return true;
                }
            }
        }
        ct_eq(presented.as_bytes(), &self.secret)
    }

    /// 401 归因消息用(不落密钥本体)。
    pub(crate) fn header_name(&self) -> &str {
        &self.header
    }
}

/// Strip an RFC 6750 `Bearer ` prefix (scheme is case-insensitive); any other
/// form → None (falls through to the full-value comparison).
fn strip_bearer(value: &str) -> Option<&str> {
    if let Some(rest) = value.strip_prefix("Bearer ") {
        return Some(rest);
    }
    value.strip_prefix("bearer ")
}

/// Classify an HTTP request path into an endpoint class (蓝图 §4.2). Health
/// probes are an exact allowlist; inference = anything `access_log_target`
/// treats as a model inference/custom-route path; everything else is admin.
pub fn classify_http_path(path: &str) -> EndpointClass {
    // /v2/health/live|ready 是 livez/readyz 的别名路由(routes.rs,批次 3)
    // ——k8s 探针/KServe SDK 远程探活的目标路径,与本体同类(审计修复 B6)。
    if matches!(
        path,
        "/health" | "/livez" | "/readyz" | "/startupz" | "/v2/health/live" | "/v2/health/ready"
    ) {
        return EndpointClass::Health;
    }
    if crate::http::routes::access_log_target(path).is_some() {
        return EndpointClass::Inference;
    }
    // 批次 5(openai-compact):/v1/* 归 inference 族——否则 fail-closed
    // 默认把 /v1 当 Admin,拒绝公共访问。
    if path.starts_with("/v1/") {
        return EndpointClass::Inference;
    }
    EndpointClass::Admin
}

/// Convenience alias used by callers that already hold the shared policy.
pub type SharedAccessControl = Arc<AccessControl>;

/// SHA-256 hex 前缀（12 字符）of a secret。高熵 API key 的截断哈希可安全
/// 入日志做归因；与 tls.rs 证书指纹同族（sha2 直依赖）。
fn secret_fingerprint(secret: &[u8]) -> String {
    use sha2::Digest;
    use std::fmt::Write as _;
    let digest = sha2::Sha256::digest(secret);
    let mut out = String::with_capacity(12);
    for b in &digest[..6] {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::Protocol;
    use crate::config::{AccessControlConfig, EndpointControl, HealthControl, ProtocolControl};
    use std::collections::HashMap;

    // HashMap impl for tests (production uses HeaderMap / MetadataMap).
    impl HeaderLookup for HashMap<String, String> {
        fn header(&self, name: &str) -> Option<String> {
            self.get(name).cloned()
        }
    }

    fn hdrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn key_ctrl(header: &str, secret: &str) -> EndpointControl {
        EndpointControl::Key { key: header.to_string(), value: Some(secret.to_string()), value_env: None, value_file: None }
    }

    // ===== default semantics (D14 fail-closed admin) =====

    #[test]
    fn classify_http_path_v1_is_inference() {
        // 批次 5(openai-compact):/v1/* 归 inference 族——否则 fail-closed
        // 默认把 /v1 当 Admin,拒绝公共访问。
        assert_eq!(classify_http_path("/v1/chat/completions"), EndpointClass::Inference);
        assert_eq!(classify_http_path("/v1/models"), EndpointClass::Inference);
        assert_eq!(classify_http_path("/v1/models/foo"), EndpointClass::Inference);
        assert_eq!(classify_http_path("/health"), EndpointClass::Health);
        assert_eq!(classify_http_path("/v2/models/m/infer"), EndpointClass::Inference);
    }

    /// /audit protocol-compat 举证(2026-08-08,当前 FAIL):
    /// /v2/health/live|ready 是 livez/readyz 的别名路由(routes.rs:117-118,
    /// 批次 3 为 k8s 探针/KServe 生态而加),但分类落 Admin——默认
    /// fail-closed 下非 loopback 探针被 401 拒绝,draining_gate 的探针
    /// 豁免表(http/mod.rs draining_gate)同样不含这两个路径,排水期
    /// liveness 别名 503 会诱发 k8s 误杀。别名应与本体同类(Health)。
    #[test]
    fn test_audit_v2_health_aliases_classify_as_health() {
        assert_eq!(classify_http_path("/v2/health/live"), EndpointClass::Health);
        assert_eq!(classify_http_path("/v2/health/ready"), EndpointClass::Health);
    }

    #[test]
    fn default_admin_loopback_passes_non_loopback_denies() {
        let ac = AccessControl::default();
        let h: HashMap<String, String> = HashMap::new();
        assert!(ac.check(EndpointClass::Admin, Protocol::Http, &h, true), "loopback admin passes");
        assert!(!ac.check(EndpointClass::Admin, Protocol::Http, &h, false), "non-loopback admin denied (fail-closed)");
    }

    #[test]
    fn default_inference_and_health_public() {
        let ac = AccessControl::default();
        let h: HashMap<String, String> = HashMap::new();
        assert!(ac.check(EndpointClass::Inference, Protocol::Http, &h, false));
        assert!(ac.check(EndpointClass::Health, Protocol::Http, &h, false));
        assert!(ac.check(EndpointClass::Inference, Protocol::Grpc, &h, false));
    }

    // ===== key mode =====

    #[test]
    fn key_correct_value_passes_wrong_denies() {
        let ac = AccessControl::build(&AccessControlConfig {
            admin: ProtocolControl {
                http: Some(key_ctrl("x-api-key", "s3cr3t")),
                grpc: Some(key_ctrl("api-key", "s3cr3t")),
            },
            ..Default::default()
        })
        .unwrap();
        assert!(ac.check(EndpointClass::Admin, Protocol::Http, &hdrs(&[("x-api-key", "s3cr3t")]), false));
        assert!(!ac.check(EndpointClass::Admin, Protocol::Http, &hdrs(&[("x-api-key", "wrong")]), false));
        assert!(!ac.check(EndpointClass::Admin, Protocol::Http, &HashMap::<String, String>::new(), false), "missing key denied");
        // gRPC uses its own header name.
        assert!(ac.check(EndpointClass::Admin, Protocol::Grpc, &hdrs(&[("api-key", "s3cr3t")]), false));
        assert!(!ac.check(EndpointClass::Admin, Protocol::Grpc, &hdrs(&[("api-key", "nope")]), false));
    }

    #[test]
    fn key_mode_denies_loopback_without_key() {
        // Key mode requires the key regardless of loopback.
        let ac = AccessControl::build(&AccessControlConfig {
            inference: ProtocolControl { http: Some(key_ctrl("x-api-key", "k")), grpc: None },
            ..Default::default()
        })
        .unwrap();
        assert!(!ac.check(EndpointClass::Inference, Protocol::Http, &HashMap::<String, String>::new(), true));
    }

    #[test]
    fn public_explicit_escape_hatch() {
        let ac = AccessControl::build(&AccessControlConfig {
            admin: ProtocolControl { http: Some(EndpointControl::Public), grpc: Some(EndpointControl::Public) },
            ..Default::default()
        })
        .unwrap();
        assert!(ac.check(EndpointClass::Admin, Protocol::Http, &HashMap::<String, String>::new(), false), "public admin open to non-loopback");
    }

    // ===== health shorthand applies to both protocols =====

    #[test]
    fn health_shorthand_applies_to_both_protocols() {
        let ac = AccessControl::build(&AccessControlConfig {
            health: HealthControl(Some(key_ctrl("x-api-key", "h"))),
            ..Default::default()
        })
        .unwrap();
        assert!(ac.check(EndpointClass::Health, Protocol::Http, &hdrs(&[("x-api-key", "h")]), false));
        assert!(ac.check(EndpointClass::Health, Protocol::Grpc, &hdrs(&[("x-api-key", "h")]), false));
        assert!(!ac.check(EndpointClass::Health, Protocol::Http, &HashMap::<String, String>::new(), false));
    }

    // ===== build-time resolution / fail-fast =====

    #[test]
    fn key_mode_without_secret_source_fails_build() {
        let ac = AccessControlConfig {
            admin: ProtocolControl {
                http: Some(EndpointControl::Key { key: "x-api-key".to_string(), value: None, value_env: None, value_file: None }),
                grpc: None,
            },
            ..Default::default()
        };
        assert!(AccessControl::build(&ac).is_err(), "Key mode with no secret source must fail fast");
    }

    #[test]
    fn key_mode_unset_env_fails_build() {
        let ac = AccessControlConfig {
            admin: ProtocolControl {
                http: Some(EndpointControl::Key {
                    key: "x-api-key".to_string(),
                    value: None,
                    value_env: Some("P71_DEFINITELY_UNSET_VAR".to_string()),
                    value_file: None,
                }),
                grpc: None,
            },
            ..Default::default()
        };
        assert!(AccessControl::build(&ac).is_err(), "unset value_env must fail fast");
    }

    // ===== constant-time compare sanity =====

    #[test]
    fn ct_eq_matches_and_rejects() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(ct_eq(b"", b""));
    }

    // ===== HTTP path classification =====

    #[test]
    fn classify_health_inference_admin() {
        use crate::access_control::classify_http_path;
        assert_eq!(classify_http_path("/health"), EndpointClass::Health);
        assert_eq!(classify_http_path("/readyz"), EndpointClass::Health);
        assert_eq!(classify_http_path("/v2/models/m/infer"), EndpointClass::Inference);
        assert_eq!(classify_http_path("/v2/models/m/versions/2/events"), EndpointClass::Inference);
        assert_eq!(classify_http_path("/v2/models/m/summarize"), EndpointClass::Inference, "custom @route → inference");
        assert_eq!(classify_http_path("/v2/repository/models/m/versions/1/load"), EndpointClass::Admin);
        assert_eq!(classify_http_path("/metrics"), EndpointClass::Admin);
        assert_eq!(classify_http_path("/info"), EndpointClass::Admin);
        assert_eq!(classify_http_path("/v2/models/m/ready"), EndpointClass::Admin);
    }

    // AUDIT (P0): bare `POST /v2/models/:m/reload` is a registered state-changing
    // ADMIN route (routes.rs:133) → reload_model_handler (admin.rs:353), which has
    // NO enforce_auth/secondary gate. It MUST classify as Admin so that the
    // default fail-closed policy (admin unconfigured → loopback-only, D14) protects
    // it. `reload` is missing from access_log_target's admin-leaf exclusion list
    // (routes.rs:31: only ready|health|routing|activate|compare), so the bare form
    // falls to the custom-@route `_` arm → Some → **Inference** → unconfigured
    // inference is PUBLIC (check() line 141) → a remote, unauthenticated caller can
    // trigger a model reload (reinit storm / DoS). The versioned form is already
    // Admin via the `["versions", ..]` arm. This test asserts the correct class and
    // FAILS on the current code (returns Inference).
    #[test]
    fn classify_bare_reload_is_admin_not_inference() {
        use crate::access_control::classify_http_path;
        assert_eq!(
            classify_http_path("/v2/models/m/reload"),
            EndpointClass::Admin,
            "bare /reload is an admin route; Inference classification lets a remote \
             unauthenticated caller bypass admin fail-closed"
        );
        // Regression guard: the versioned form is already correct.
        assert_eq!(
            classify_http_path("/v2/models/m/versions/1/reload"),
            EndpointClass::Admin
        );
    }

    // ===== admin_denies_non_loopback helper =====

    #[test]
    fn admin_denies_non_loopback_helper() {
        assert!(AccessControl::default().admin_denies_non_loopback());
        let ac = AccessControl::build(&AccessControlConfig {
            admin: ProtocolControl { http: Some(EndpointControl::Public), grpc: Some(EndpointControl::Public) },
            ..Default::default()
        })
        .unwrap();
        assert!(!ac.admin_denies_non_loopback());
    }

    // ===== D27 key 指纹（审计归因）=====

    #[test]
    fn key_fingerprint_is_sha256_prefix_of_configured_secret() {
        let ac = AccessControl::build(&AccessControlConfig {
            admin: ProtocolControl { http: Some(key_ctrl("x-admin-key", "secret-token-123")), grpc: None },
            ..Default::default()
        })
        .unwrap();
        let fp = ac
            .key_fingerprint(EndpointClass::Admin, Protocol::Http)
            .expect("key 模式必须有指纹");
        assert_eq!(fp.len(), 12);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
        // 与 secret 的 SHA-256 前缀一致（独立重算，防实现自证）
        use sha2::Digest;
        let digest = sha2::Sha256::digest(b"secret-token-123");
        let expected: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(fp, expected);
        // 不落明文
        assert!(!fp.contains("secret"));
    }

    #[test]
    fn key_fingerprint_none_for_public_and_unconfigured() {
        // public → None
        let ac = AccessControl::build(&AccessControlConfig {
            admin: ProtocolControl { http: Some(EndpointControl::Public), grpc: None },
            ..Default::default()
        })
        .unwrap();
        assert_eq!(ac.key_fingerprint(EndpointClass::Admin, Protocol::Http), None);
        // 未配置 → None
        let ac = AccessControl::default();
        assert_eq!(ac.key_fingerprint(EndpointClass::Admin, Protocol::Http), None);
        // key 只配在 http 轴时 grpc 轴 → None
        let ac = AccessControl::build(&AccessControlConfig {
            admin: ProtocolControl { http: Some(key_ctrl("x-admin-key", "tok")), grpc: None },
            ..Default::default()
        })
        .unwrap();
        assert_eq!(ac.key_fingerprint(EndpointClass::Admin, Protocol::Grpc), None);
    }

    #[test]
    fn key_fingerprint_distinguishes_rotated_keys() {
        let build = |secret: &str| {
            AccessControl::build(&AccessControlConfig {
                admin: ProtocolControl { http: Some(key_ctrl("x-admin-key", secret)), grpc: None },
                ..Default::default()
            })
            .unwrap()
        };
        let old = build("old-token");
        let new = build("new-token");
        assert_ne!(
            old.key_fingerprint(EndpointClass::Admin, Protocol::Http),
            new.key_fingerprint(EndpointClass::Admin, Protocol::Http),
            "轮换前后指纹必须可区分"
        );
    }

    // ===== OpenaiAuthGate(openai-compact 专属门,只锁 /v1 5 端点)=====
    //
    // 与 access-control Key 模式共享 EndpointControl 形状与常量时间比对,
    // 差异:header 为 authorization 时接受 `Bearer <key>`(RFC 6750 剥前缀),
    // 无 loopback 豁免(配了就要 key),由 openai_compact::mount 独家挂载。

    fn gate_ctrl(header: &str, secret: &str) -> EndpointControl {
        EndpointControl::Key { key: header.to_string(), value: Some(secret.to_string()), value_env: None, value_file: None }
    }

    #[test]
    fn openai_gate_unconfigured_and_public_build_to_none() {
        assert!(OpenaiAuthGate::build(None).unwrap().is_none());
        assert!(OpenaiAuthGate::build(Some(&EndpointControl::Public)).unwrap().is_none());
    }

    #[test]
    fn openai_gate_bearer_prefix_stripped_accepts() {
        let gate = OpenaiAuthGate::build(Some(&gate_ctrl("authorization", "sk-secret")))
            .unwrap()
            .expect("key 模式必须产出 gate");
        // 官方 openai SDK 的标准形式:Authorization: Bearer <key>。
        assert!(gate.check(&hdrs(&[("authorization", "Bearer sk-secret")])));
        // RFC 6750 scheme 大小写不敏感。
        assert!(gate.check(&hdrs(&[("authorization", "bearer sk-secret")])));
        // 裸值等值保留(直接配完整 "Bearer xxx" 的兼容)。
        assert!(gate.check(&hdrs(&[("authorization", "sk-secret")])));
    }

    #[test]
    fn openai_gate_bearer_rejects_wrong_missing_empty() {
        let gate = OpenaiAuthGate::build(Some(&gate_ctrl("authorization", "sk-secret")))
            .unwrap()
            .unwrap();
        assert!(!gate.check(&hdrs(&[("authorization", "Bearer wrong")])));
        assert!(!gate.check(&hdrs(&[("authorization", "Bearer ")])));
        assert!(!gate.check(&hdrs(&[])), "缺 header 必须拒绝");
        assert!(!gate.check(&hdrs(&[("authorization", "")])));
    }

    #[test]
    fn openai_gate_non_authorization_header_raw_only() {
        // 自定义 header(如 x-api-key)保持 access-control 语义:全值比对,
        // 不剥 Bearer。
        let gate = OpenaiAuthGate::build(Some(&gate_ctrl("x-api-key", "k")))
            .unwrap()
            .unwrap();
        assert!(gate.check(&hdrs(&[("x-api-key", "k")])));
        assert!(!gate.check(&hdrs(&[("x-api-key", "Bearer k")])));
    }

    #[test]
    fn openai_gate_configured_header_case_insensitive_for_scheme() {
        // 配置写 "Authorization" 时同样启用 Bearer 语义(eq_ignore_ascii_case)。
        // header 查找本身的大小写不敏感由生产 HeaderMap 提供(HashMap 测试
        // impl 是大小写敏感的),这里只验证 gate 的 scheme 判定。
        let gate = OpenaiAuthGate::build(Some(&gate_ctrl("Authorization", "sk-x")))
            .unwrap()
            .unwrap();
        assert!(gate.check(&hdrs(&[("Authorization", "Bearer sk-x")])));
        assert!(gate.check(&hdrs(&[("Authorization", "bearer sk-x")])));
    }

    #[test]
    fn openai_gate_fail_fast_on_missing_secret() {
        let ctrl = EndpointControl::Key {
            key: "authorization".to_string(),
            value: None,
            value_env: None,
            value_file: None,
        };
        assert!(OpenaiAuthGate::build(Some(&ctrl)).is_err(), "无 secret 源必须 fail-fast");
    }

    #[test]
    fn openai_gate_fail_fast_on_unset_env() {
        let ctrl = EndpointControl::Key {
            key: "authorization".to_string(),
            value: None,
            value_env: Some("OAI_GATE_DEFINITELY_UNSET".to_string()),
            value_file: None,
        };
        assert!(OpenaiAuthGate::build(Some(&ctrl)).is_err(), "value_env 未设必须 fail-fast");
    }

    #[test]
    fn openai_gate_fail_fast_on_empty_header_name() {
        let ctrl = EndpointControl::Key {
            key: String::new(),
            value: Some("sk-x".to_string()),
            value_env: None,
            value_file: None,
        };
        assert!(OpenaiAuthGate::build(Some(&ctrl)).is_err(), "空 header 名必须 fail-fast");
    }
}
