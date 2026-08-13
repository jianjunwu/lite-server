//! P-DEADLINE (蓝图 §4.0.10 / §4.4): per-request deadline resolution + helpers.
//!
//! Unified timeout model so scattered timeouts stop acting independently. A
//! client may bound a single request — HTTP `x-lite-timeout` (relative seconds,
//! float) or gRPC standard `grpc-timeout` metadata — else the server falls back
//! to `server.timeout`. The resolved deadline is carried to the worker as an
//! absolute UNIX-nanosecond timestamp (`RequestMeta.deadline_unix_ns`, additive)
//! and used to bound unary / ensemble / streaming waits.
//!
//! Streaming two-stage bound (方案 C): the OVERALL deadline activates only when
//! the client specified one (default config leaves long streams unbounded by
//! overall deadline); the chunk-idle reclaim is ALWAYS on (decoupled parity,
//! `decoupled_idle_timeout_secs`) so a stuck stream is recovered instead of
//! hanging unbounded.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;
use tonic::metadata::MetadataMap;

/// HTTP header carrying a client-specified relative timeout in seconds (float).
/// Blueprint §4.0.10 names this header (P-DEADLINE 定名).
pub const HTTP_TIMEOUT_HEADER: &str = "x-lite-timeout";

/// The gRPC standard deadline metadata key.
const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

/// A resolved per-request deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedDeadline {
    /// Absolute UNIX-ns deadline. `None` = no deadline (unbounded wait).
    pub unix_ns: Option<i64>,
    /// True iff the client explicitly specified the deadline
    /// (`x-lite-timeout` / `grpc-timeout`). False for the `server.timeout`
    /// fallback. The streaming OVERALL deadline is gated on this flag (so the
    /// default config does not truncate long LLM streams); the chunk-idle
    /// reclaim is always on regardless — see [`idle_budget`].
    pub client_specified: bool,
}

impl ResolvedDeadline {
    /// No deadline at all (unbounded).
    pub const NONE: ResolvedDeadline = ResolvedDeadline {
        unix_ns: None,
        client_specified: false,
    };
}

fn now_unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// `now + secs` as an absolute UNIX-ns deadline.
fn from_relative_secs(secs: f64) -> i64 {
    // B1: a huge-but-finite client timeout (e.g. 1e10 s ≈ 317 years) saturates
    // the f64→i64 cast to i64::MAX; a plain `+` then overflows (debug panic,
    // release wrap to a negative deadline = every wait expires instantly).
    // Saturate instead: an effectively-infinite deadline stays in the future.
    now_unix_ns().saturating_add((secs * 1_000_000_000.0) as i64)
}

/// `server.timeout` fallback when the client specified nothing.
fn fallback(server_timeout: f32) -> ResolvedDeadline {
    if server_timeout.is_finite() && server_timeout > 0.0 {
        ResolvedDeadline {
            unix_ns: Some(from_relative_secs(server_timeout as f64)),
            client_specified: false,
        }
    } else {
        ResolvedDeadline::NONE
    }
}

/// Resolve a deadline from HTTP headers. A valid client `x-lite-timeout`
/// (relative seconds, float) wins; otherwise `server.timeout > 0` is the
/// fallback; otherwise there is no deadline.
pub fn resolve_from_http(headers: &HeaderMap, server_timeout: f32) -> ResolvedDeadline {
    if let Some(secs) = headers
        .get(HTTP_TIMEOUT_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
    {
        if secs.is_finite() && secs > 0.0 {
            return ResolvedDeadline {
                unix_ns: Some(from_relative_secs(secs)),
                client_specified: true,
            };
        }
    }
    fallback(server_timeout)
}

/// Parse a gRPC standard `grpc-timeout` value (`<digits><unit>`,
/// unit ∈ H/M/S/m/u/n) into seconds.
fn parse_grpc_timeout(value: &str) -> Option<f64> {
    // gRPC spec: value is digits followed by exactly one unit char.
    let mut chars = value.chars();
    let unit = chars.next_back()?;
    let digits = chars.as_str();
    if digits.is_empty() {
        return None;
    }
    let val: f64 = digits.parse().ok()?;
    let secs = match unit {
        'H' => val * 3600.0,
        'M' => val * 60.0,
        'S' => val,
        'm' => val * 0.001,
        'u' => val * 0.000_001,
        'n' => val * 0.000_000_001,
        _ => return None,
    };
    if secs.is_finite() && secs > 0.0 {
        Some(secs)
    } else {
        None
    }
}

/// Resolve a deadline from gRPC metadata. A valid client `grpc-timeout` wins;
/// otherwise `server.timeout > 0` is the fallback; otherwise no deadline.
pub fn resolve_from_grpc(metadata: &MetadataMap, server_timeout: f32) -> ResolvedDeadline {
    if let Some(secs) = metadata
        .get(GRPC_TIMEOUT_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_grpc_timeout)
    {
        return ResolvedDeadline {
            unix_ns: Some(from_relative_secs(secs)),
            client_specified: true,
        };
    }
    fallback(server_timeout)
}

/// Remaining duration until the deadline (`deadline - now`).
/// `None` when there is no deadline. `Some(Duration::ZERO)` when it has
/// already expired (callers treat ZERO as "elapsed").
pub fn remaining(unix_ns: Option<i64>) -> Option<Duration> {
    let deadline = unix_ns?;
    let now = now_unix_ns();
    if deadline <= now {
        return Some(Duration::ZERO);
    }
    Some(Duration::from_nanos((deadline - now) as u64))
}

/// Monotonic `Instant` of the deadline (`now + remaining`), for streaming
/// forward loops that must capture a single absolute point before spawning.
/// `None` when there is no deadline.
pub fn to_instant(unix_ns: Option<i64>) -> Option<Instant> {
    Instant::now().checked_add(remaining(unix_ns)?)
}

/// D35 (E5, ensemble batch 3): fold two optional wall-clock caps — the
/// tighter one wins. The adapters combine their client-derived overall
/// deadline with an ensemble step's `timeout_secs` cap before the forward
/// loop spawns.
pub fn min_instant(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, y) => x.or(y),
    }
}

/// Convert a config idle budget (seconds, 0/neg = disabled) into the
/// `Option<Duration>` the streaming recv helper consumes.
pub fn idle_budget(idle_secs: f32) -> Option<Duration> {
    if idle_secs.is_finite() && idle_secs > 0.0 {
        Some(Duration::from_secs_f32(idle_secs))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn hmap(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        m
    }

    fn grpc(pairs: &[(&str, &str)]) -> MetadataMap {
        let mut m = MetadataMap::new();
        for (k, v) in pairs {
            m.insert(
                tonic::metadata::MetadataKey::from_bytes(k.as_bytes()).unwrap(),
                tonic::metadata::MetadataValue::try_from(*v).unwrap(),
            );
        }
        m
    }

    #[test]
    fn http_client_header_resolves_client_specified() {
        let d = resolve_from_http(&hmap(&[("x-lite-timeout", "0.3")]), 30.0);
        assert!(d.client_specified, "client header => client_specified");
        let ns = d.unix_ns.expect("deadline present");
        // ~0.3s in the future, i.e. within [0.25, 0.35]s of now.
        let rem = ns - now_unix_ns();
        assert!(
            rem > 250_000_000 && rem < 350_000_000,
            "expected ~0.3s remaining, got {rem} ns"
        );
    }

    #[test]
    fn http_no_header_falls_back_to_server_timeout() {
        let d = resolve_from_http(&hmap(&[]), 5.0);
        assert!(!d.client_specified, "fallback => not client_specified");
        let rem = d.unix_ns.unwrap() - now_unix_ns();
        assert!(rem > 4_000_000_000 && rem < 6_000_000_000, "fallback ~5s, got {rem}");
    }

    #[test]
    fn http_no_header_no_server_timeout_is_unbounded() {
        let d = resolve_from_http(&hmap(&[]), 0.0);
        assert_eq!(d, ResolvedDeadline::NONE);
        let d2 = resolve_from_http(&hmap(&[]), -1.0);
        assert_eq!(d2, ResolvedDeadline::NONE);
    }

    #[test]
    fn http_invalid_or_nonpositive_header_falls_back() {
        // garbage / zero / negative header must NOT be taken as client deadline.
        for bad in ["garbage", "0", "-5", "nan", "inf"] {
            let d = resolve_from_http(&hmap(&[("x-lite-timeout", bad)]), 10.0);
            assert!(!d.client_specified, "{bad} should fall back, got {d:?}");
            assert!(d.unix_ns.is_some(), "{bad} should fall back to server.timeout");
        }
    }

    #[test]
    fn grpc_timeout_units_parse() {
        // 300 milliseconds
        let d = resolve_from_grpc(&grpc(&[("grpc-timeout", "300m")]), 30.0);
        assert!(d.client_specified);
        let rem = d.unix_ns.unwrap() - now_unix_ns();
        assert!(rem > 250_000_000 && rem < 350_000_000, "300ms, got {rem}");
        // 2 seconds
        let d = resolve_from_grpc(&grpc(&[("grpc-timeout", "2S")]), 30.0);
        let rem = d.unix_ns.unwrap() - now_unix_ns();
        assert!(rem > 1_900_000_000 && rem < 2_100_000_000, "2S, got {rem}");
        // microseconds / nanoseconds / minutes / hours parse without panic
        assert!(resolve_from_grpc(&grpc(&[("grpc-timeout", "500u")]), 30.0).unix_ns.is_some());
        assert!(resolve_from_grpc(&grpc(&[("grpc-timeout", "1000n")]), 30.0).unix_ns.is_some());
        assert!(resolve_from_grpc(&grpc(&[("grpc-timeout", "1M")]), 30.0).unix_ns.is_some());
        assert!(resolve_from_grpc(&grpc(&[("grpc-timeout", "1H")]), 30.0).unix_ns.is_some());
    }

    #[test]
    fn grpc_invalid_timeout_falls_back() {
        let d = resolve_from_grpc(&grpc(&[("grpc-timeout", "nope")]), 10.0);
        assert!(!d.client_specified);
        assert!(d.unix_ns.is_some());
        // missing value
        let d2 = resolve_from_grpc(&grpc(&[("grpc-timeout", "S")]), 10.0);
        assert!(!d2.client_specified);
    }

    #[test]
    fn remaining_none_when_no_deadline() {
        assert_eq!(remaining(None), None);
    }

    #[test]
    fn remaining_zero_when_expired() {
        // a deadline in the past
        let past = now_unix_ns() - 1_000_000_000;
        assert_eq!(remaining(Some(past)), Some(Duration::ZERO));
    }

    #[test]
    fn remaining_positive_when_future() {
        let future = now_unix_ns() + 500_000_000;
        let rem = remaining(Some(future)).unwrap();
        assert!(rem <= Duration::from_millis(500));
        assert!(rem > Duration::from_millis(400));
    }

    #[test]
    fn to_instant_roundtrips_remaining() {
        let future = now_unix_ns() + 200_000_000;
        let inst = to_instant(Some(future)).expect("some instant");
        let drift = inst.saturating_duration_since(Instant::now());
        assert!(drift <= Duration::from_millis(200));
        assert!(drift > Duration::from_millis(100));
    }

    #[test]
    fn to_instant_none_when_no_deadline() {
        assert_eq!(to_instant(None), None);
    }

    #[test]
    fn huge_client_timeout_must_not_overflow_to_instant_expiry() {
        // `x-lite-timeout` is parsed as f64 with only finite>0 validation;
        // a huge-but-finite value (e.g. 1e10 s ≈ 317 years) saturates to
        // i64::MAX in `(secs * 1e9) as i64`, and `now_unix_ns() + i64::MAX`
        // overflows: debug panics, release wraps to a negative deadline that
        // makes `remaining()` return Duration::ZERO — every wait for this
        // client expires immediately (504). The invalid-timeout fallback
        // semantics ("bad header falls back to server.timeout") require this
        // to be handled, not overflow.
        let d = resolve_from_http(&hmap(&[("x-lite-timeout", "10000000000")]), 30.0);
        assert!(d.client_specified);
        let rem = remaining(d.unix_ns).expect("deadline present");
        assert!(!rem.is_zero(), "huge timeout must not resolve to already-expired");
        // grpc-timeout with a huge hour count hits the same arithmetic.
        let d = resolve_from_grpc(&grpc(&[("grpc-timeout", "10000000000H")]), 30.0);
        assert!(d.client_specified);
        let rem = remaining(d.unix_ns).expect("deadline present");
        assert!(!rem.is_zero(), "huge grpc-timeout must not resolve to already-expired");
    }
}
