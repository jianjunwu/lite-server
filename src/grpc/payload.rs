//! FD-1 (audit 2026-08-06): gRPC payload content-type dispatch + gateway-side
//! JSON validation — parity with HTTP ApiBody (D3) and h2 bidi B1. The proto
//! `headers` map is the same map the worker reads via `RequestMeta.headers`
//! (Python `_payload_content_type`, worker/streaming.py:48), so the gateway
//! and the worker share one dispatch rule.

use std::collections::HashMap;
use tonic::Status;

/// h2 bidi transport framing type — names the LPM frame protocol, NOT the
/// payload (mirrors `BIDI_CONTENT_TYPE` in http/handlers/bidi.rs and
/// `_BIDI_FRAMING_CONTENT_TYPE` in python/lite_server/worker/streaming.py).
const BIDI_FRAMING_CONTENT_TYPE: &str = "application/x-lite-bidi";

/// Case-insensitive header lookup over the proto `headers` map.
///
/// The map holds whatever key the client spelled (`Content-Type` vs
/// `content-type`), so a literal `get` misses the capitalized spelling —
/// every consumer of this map must look up through here. `grpc/auth.rs`
/// compares header names the same way.
pub(crate) fn header_value<'a>(
    headers: &'a HashMap<String, String>,
    name: &str,
) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// D43 (R24): the media type the request root carries, for a `passthrough`
/// step to re-state alongside the verbatim bytes — `None` for a JSON (or
/// absent) content-type, mirroring HTTP's `RequestBody::content_type`.
/// Both transports must agree that a JSON payload is never opaque binary.
pub(crate) fn request_content_type(headers: &HashMap<String, String>) -> Option<String> {
    let ct = header_value(headers, "content-type")?;
    if grpc_payload_is_json(headers) {
        return None;
    }
    Some(ct.to_string())
}

/// Mirror Python `_payload_content_type` + `_is_json_content_type`
/// (pipeline.py:82). Returns true when the payload MUST be valid JSON; false
/// when it MUST be treated as opaque bytes.
pub(crate) fn grpc_payload_is_json(headers: &HashMap<String, String>) -> bool {
    let Some(ct) = header_value(headers, "content-type") else {
        return true; // missing → JSON default (D2 parity)
    };
    let base = ct.split(';').next().unwrap_or("").trim().to_lowercase();
    if base == BIDI_FRAMING_CONTENT_TYPE {
        return true; // framing CT → JSON default (mirror Python)
    }
    is_json_content_type_str(ct)
}

/// `&str` variant of http::handlers::is_json_content_type (D1/D9 — Rust is
/// the authoritative dispatcher): `application/json` and `application/*+json`,
/// case-insensitive, parameters ignored; parse failure → false (raw).
fn is_json_content_type_str(value: &str) -> bool {
    let Ok(mime) = value.parse::<mime::Mime>() else {
        return false;
    };
    mime.type_() == mime::APPLICATION
        && (mime.subtype() == mime::JSON || mime.suffix().is_some_and(|s| s == mime::JSON))
}

/// FD-2: D11-style `body_kind` telemetry label ("json" | "raw") from the same
/// dispatch as [`grpc_payload_is_json`].
pub(crate) fn body_kind_label(headers: &HashMap<String, String>) -> &'static str {
    if grpc_payload_is_json(headers) {
        "json"
    } else {
        "raw"
    }
}

/// FD-1: validate a JSON-dispatched payload at the gateway so malformed JSON
/// is rejected with InvalidArgument before a worker stream is opened (HTTP
/// returns 400 in ApiBody/B1). Empty payloads skip validation (Python
/// `_parse_request_json(b"")` → {}). The returned Status is unwrapped — call
/// sites wrap it with `err()` like their neighbors.
pub(crate) fn validate_json_payload(
    headers: &HashMap<String, String>,
    data: &[u8],
) -> Result<(), Status> {
    if !data.is_empty() && grpc_payload_is_json(headers) {
        serde_json::from_slice::<&serde_json::value::RawValue>(data)
            .map_err(|e| Status::invalid_argument(format!("invalid JSON payload: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// BUG: the proto `headers` map is looked up with the literal lowercase
    /// key `"content-type"`, so a client that spells the header with HTTP
    /// casing (`Content-Type`) is treated as "no content-type" and falls into
    /// the JSON default.
    ///
    /// The same request's auth fallback (`grpc/auth.rs`) deliberately compares
    /// header names with `eq_ignore_ascii_case` "per HTTP header semantics",
    /// and the Python mirror (`_payload_content_type` via `Headers`) is
    /// case-insensitive — so the gateway diverges from both the worker and
    /// its own auth rule. A REST→gRPC bridge sending `Content-Type: image/png`
    /// with binary bytes gets `invalid JSON payload`, and in the ensemble
    /// branch the declared content type is dropped before the sub-model sees
    /// it.
    #[test]
    fn payload_is_json_is_case_insensitive_on_the_header_name() {
        assert!(
            !grpc_payload_is_json(&h(&[("Content-Type", "application/octet-stream")])),
            "a capitalized `Content-Type: application/octet-stream` must dispatch \
             as raw, not fall through to the missing-header JSON default"
        );
        assert!(
            !grpc_payload_is_json(&h(&[("Content-Type", "image/png")])),
            "`Content-Type: image/png` must dispatch as raw"
        );
    }

    /// Dispatch truth table — mirrors the Python `_payload_content_type` /
    /// `_is_json_content_type` contract (and the HTTP bidi 7-state matrix).
    #[test]
    fn payload_is_json_matrix() {
        // missing → JSON default
        assert!(grpc_payload_is_json(&h(&[])));
        assert!(grpc_payload_is_json(&h(&[(
            "content-type",
            "application/json"
        )])));
        assert!(grpc_payload_is_json(&h(&[(
            "content-type",
            "application/vnd.api+json; charset=utf-8"
        )])));
        // framing CT (with/without params) → JSON default
        assert!(grpc_payload_is_json(&h(&[(
            "content-type",
            "application/x-lite-bidi"
        )])));
        assert!(grpc_payload_is_json(&h(&[(
            "content-type",
            "application/x-lite-bidi; charset=utf-8"
        )])));
        // raw dispatch
        assert!(!grpc_payload_is_json(&h(&[(
            "content-type",
            "application/octet-stream"
        )])));
        assert!(!grpc_payload_is_json(&h(&[("content-type", "image/png")])));
        // text subtypes are NOT json (D1); garbage → raw
        assert!(!grpc_payload_is_json(&h(&[("content-type", "text/json")])));
        assert!(!grpc_payload_is_json(&h(&[(
            "content-type",
            "not-a-valid/content-type!!!"
        )])));
    }

    /// FIX 4: the case-insensitive lookup the ensemble call sites must use —
    /// a literal `get("content-type")` misses a capitalized spelling.
    #[test]
    fn header_value_is_case_insensitive() {
        assert_eq!(
            header_value(&h(&[("Content-Type", "image/png")]), "content-type"),
            Some("image/png")
        );
        assert_eq!(header_value(&h(&[]), "content-type"), None);
    }

    /// FIX 5: `request_content_type` is JSON-aware — `None` for a JSON (or
    /// absent) content-type, like HTTP's `RequestBody::content_type`, so a
    /// passthrough step never treats a JSON root as opaque binary.
    #[test]
    fn request_content_type_is_none_for_json() {
        assert_eq!(request_content_type(&h(&[])), None);
        assert_eq!(
            request_content_type(&h(&[("content-type", "application/json")])),
            None
        );
        assert_eq!(
            request_content_type(&h(&[("Content-Type", "application/json")])),
            None
        );
        assert_eq!(
            request_content_type(&h(&[("Content-Type", "image/png")])).as_deref(),
            Some("image/png"),
            "a binary media type keeps the declared value, any casing"
        );
        assert_eq!(
            request_content_type(&h(&[("content-type", "application/octet-stream")])).as_deref(),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn validate_skips_empty_payload() {
        assert!(validate_json_payload(&h(&[]), b"").is_ok());
    }

    #[test]
    fn validate_rejects_malformed_json() {
        let err = validate_json_payload(&h(&[]), b"not-json{").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn validate_passes_raw_payload() {
        assert!(validate_json_payload(
            &h(&[("content-type", "application/octet-stream")]),
            b"\x00\xff"
        )
        .is_ok());
    }

    #[test]
    fn validate_passes_valid_json() {
        assert!(validate_json_payload(&h(&[]), br#"{"a":1}"#).is_ok());
    }
}
