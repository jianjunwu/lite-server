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
        Protocol::Http | Protocol::Sse | Protocol::WebSocket => ProtocolAxis::Http,
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
    Key { header: String, secret: Vec<u8> },
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
            ResolvedControl::Key { header, secret } => match headers.header(header) {
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
            ResolvedControl::Key { header: key.clone(), secret }
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

/// Classify an HTTP request path into an endpoint class (蓝图 §4.2). Health
/// probes are an exact allowlist; inference = anything `access_log_target`
/// treats as a model inference/custom-route path; everything else is admin.
pub fn classify_http_path(path: &str) -> EndpointClass {
    if matches!(path, "/health" | "/livez" | "/readyz" | "/startupz") {
        return EndpointClass::Health;
    }
    if crate::http::routes::access_log_target(path).is_some() {
        return EndpointClass::Inference;
    }
    EndpointClass::Admin
}

/// Convenience alias used by callers that already hold the shared policy.
pub type SharedAccessControl = Arc<AccessControl>;

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
}
