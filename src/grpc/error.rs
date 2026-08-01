//! Error mapping: worker/AppError error signals → gRPC status codes, with
//! graded severity logging (P1-1) and retry-after metadata (§4.0.9).

use crate::error::AppError;
use tonic::metadata::MetadataValue;
use tonic::Status;

/// Map an HTTP status code (from the worker's Status.message) to a gRPC code.
pub(super) fn http_status_to_grpc_code(http_status: u16) -> tonic::Code {
    match http_status {
        400 | 422 => tonic::Code::InvalidArgument,
        401 => tonic::Code::Unauthenticated,
        403 => tonic::Code::PermissionDenied,
        404 => tonic::Code::NotFound,
        409 => tonic::Code::AlreadyExists,
        429 => tonic::Code::ResourceExhausted,
        503 => tonic::Code::Unavailable,
        504 => tonic::Code::DeadlineExceeded,
        _ => tonic::Code::Internal,
    }
}

/// Map an [`AppError`] to a gRPC [`Status`] for the Admin service (D4:
/// centralized mapping — admin RPCs reuse this instead of inlining). Derives
/// the gRPC code from the same HTTP status the REST path returns
/// ([`AppError::IntoResponse`]) and carries only the sanitized public message
/// (no internal detail / path leak, D4 details 白名单). Used by P6 Admin RPCs.
pub(crate) fn app_error_to_grpc_status(e: &AppError) -> Status {
    // ModelError carries its own status_code + model-author-facing detail.
    if let AppError::ModelError(d) = e {
        let code = http_status_to_grpc_code(d.status_code);
        return Status::new(code, format!("[{}] {}", d.error_type, d.detail));
    }
    let http_status: u16 = match e {
        AppError::ModelNotFound(_) | AppError::VersionNotFound(_, _) | AppError::RouteNotFound => 404,
        AppError::ModelNotReady(_) | AppError::QueueFull(_) => 503,
        AppError::VersionAlreadyLoaded(_, _) => 409,
        AppError::InferenceTimeout(_) => 504,
        AppError::Validation(_)
        | AppError::Config(_)
        | AppError::Serialization(_)
        | AppError::InvalidRequestBody(_)
        | AppError::InvalidQueryParam(_) => 400,
        AppError::FrameTooLarge => 413,
        AppError::RateLimitExceeded { .. } => 429,
        AppError::Unauthorized(_) => 401,
        AppError::MethodNotAllowed => 405,
        _ => 500,
    };
    Status::new(http_status_to_grpc_code(http_status), e.pub_error_message())
}

/// Map an error_type string (from a structured stream error) to a gRPC code.
pub(super) fn error_type_to_grpc_code(error_type: &str) -> tonic::Code {
    match error_type {
        "invalid_request_error" => tonic::Code::InvalidArgument,
        "authentication_error" => tonic::Code::Unauthenticated,
        "permission_denied_error" => tonic::Code::PermissionDenied,
        "not_found_error" => tonic::Code::NotFound,
        "service_unavailable" | "model_not_ready" => tonic::Code::Unavailable,
        // P9-1: a decoupled stream on a model without predict_decoupled.
        "not_implemented" => tonic::Code::FailedPrecondition,
        _ => tonic::Code::Internal,
    }
}

/// Parsed fields from a structured model error JSON payload.
pub(super) struct ParsedModelError {
    pub error_type: String,
    pub message: String,
    pub code: Option<String>,
    pub param: Option<String>,
}

/// Extract structured error fields from a model error JSON payload.
pub(super) fn try_parse_model_error(data: &serde_json::Value) -> Option<ParsedModelError> {
    let err = data.get("error")?;
    let error_type = err.get("type")?.as_str()?.to_string();
    let message = err.get("message")?.as_str()?.to_string();
    let code = err.get("code").and_then(|c| c.as_str()).map(String::from);
    let param = err.get("param").and_then(|p| p.as_str()).map(String::from);
    Some(ParsedModelError { error_type, message, code, param })
}

/// Build a gRPC Status from a parsed model error. The message keeps the
/// legacy `[error_type] message` format; code/param are attached as standard
/// gRPC ErrorInfo details so clients can read them programmatically.
pub(super) fn model_error_status(code: tonic::Code, parsed: &ParsedModelError) -> Status {
    use tonic_types::{ErrorDetails, StatusExt};

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("error_type".to_string(), parsed.error_type.clone());
    if let Some(p) = &parsed.param {
        metadata.insert("param".to_string(), p.clone());
    }
    // Same fallback as AppError::error_code() for ModelError
    let reason = parsed.code.as_deref().unwrap_or(&parsed.error_type);
    Status::with_error_details(
        code,
        format!("[{}] {}", parsed.error_type, parsed.message),
        ErrorDetails::with_error_info(reason, "lite-server", metadata),
    )
}

/// Whether a gRPC status code is a client-class error (P1-1). Mirrors the
/// HTTP 4xx/5xx split in error.rs: client-class codes log at info, server
/// faults at error. ResourceExhausted (429) is client-class so a saturated
/// rate limiter doesn't flood error logs; Cancelled is client-initiated.
pub(super) fn is_client_class(code: tonic::Code) -> bool {
    use tonic::Code::*;
    matches!(
        code,
        InvalidArgument
            | NotFound
            | OutOfRange
            | Unauthenticated
            | PermissionDenied
            | ResourceExhausted
            | Cancelled
    )
}

/// gRPC 状态码 → 请求指标 status 族（P2-1，蓝图 §4.3 P2-1）：成功 → "2xx"；
/// 客户端类（与 `is_client_class` 同集——InvalidArgument/NotFound/OutOfRange/
/// Unauthenticated/PermissionDenied/ResourceExhausted(限流)/Cancelled）→ "4xx"；
/// 其余服务端故障（Internal/Unavailable/DeadlineExceeded 等）→ "5xx"。
/// queue-full/过载返 Unavailable 天然落 "5xx"（§4.0.9 收口，D5：无 protocol label）。
pub(super) fn grpc_code_to_status_family(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "2xx",
        c if is_client_class(c) => "4xx",
        _ => "5xx",
    }
}

/// Log a gRPC error status with graded severity, then return it (P1-1 parity
/// with HTTP error.rs:256-270 — gRPC handlers previously logged nothing).
pub(crate) fn err(status: Status) -> Status {
    if is_client_class(status.code()) {
        tracing::info!(
            code = ?status.code(),
            message = %status.message(),
            "grpc request error"
        );
    } else {
        tracing::error!(
            code = ?status.code(),
            message = %status.message(),
            "grpc request error"
        );
    }
    status
}

/// Attach a `retry-after` trailing metadata (seconds) to a Status — the gRPC
/// analogue of HTTP's `Retry-After` header for load-shedding / admission
/// rejection (§4.0.9; same metadata key the rate limiter uses).
pub(crate) fn with_retry_after(mut status: Status, secs: u32) -> Status {
    if let Ok(v) = MetadataValue::try_from(secs.to_string().as_str()) {
        status.metadata_mut().insert("retry-after", v);
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic_types::StatusExt;

    // --- P1-1: graded error logging (client-class → info, server faults → error) ---

    #[test]
    fn test_is_client_class_codes_log_at_info() {
        use tonic::Code;
        // Client-class codes (HTTP 4xx analogues) — including ResourceExhausted
        // (429): a saturated rate limiter must not flood error logs.
        for code in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::OutOfRange,
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::Cancelled,
        ] {
            assert!(is_client_class(code), "{code:?} should be client-class");
        }
    }

    #[test]
    fn test_server_fault_codes_log_at_error() {
        use tonic::Code;
        for code in [
            Code::Internal,
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::Unknown,
            Code::DataLoss,
            Code::Unimplemented,
        ] {
            assert!(!is_client_class(code), "{code:?} should be a server fault");
        }
    }

    // --- grpc_code_to_status_family ---

    #[test]
    fn should_map_success_to_2xx() {
        assert_eq!(grpc_code_to_status_family(tonic::Code::Ok), "2xx");
    }

    #[test]
    fn should_map_client_error_codes_to_4xx() {
        // 蓝图映射: InvalidArgument/NotFound/OutOfRange/Unauthenticated/
        // PermissionDenied/ResourceExhausted → "4xx"（ResourceExhausted
        // 专给限流，§4.0.9）。Cancelled 是客户端主动断开，同类。
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::OutOfRange,
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
            tonic::Code::ResourceExhausted,
            tonic::Code::Cancelled,
        ] {
            assert_eq!(grpc_code_to_status_family(code), "4xx", "{code:?}");
        }
    }

    #[test]
    fn should_map_server_fault_codes_to_5xx() {
        // 蓝图映射: Internal/Unavailable/DeadlineExceeded → "5xx"；
        // queue-full/过载返 Unavailable 天然落此族（§4.0.9）。
        for code in [
            tonic::Code::Internal,
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
        ] {
            assert_eq!(grpc_code_to_status_family(code), "5xx", "{code:?}");
        }
    }

    // --- worker error signal → gRPC code ---

    #[test]
    fn test_http_status_to_grpc_code() {
        assert_eq!(http_status_to_grpc_code(400), tonic::Code::InvalidArgument);
        assert_eq!(http_status_to_grpc_code(422), tonic::Code::InvalidArgument);
        assert_eq!(http_status_to_grpc_code(401), tonic::Code::Unauthenticated);
        assert_eq!(http_status_to_grpc_code(403), tonic::Code::PermissionDenied);
        assert_eq!(http_status_to_grpc_code(404), tonic::Code::NotFound);
        assert_eq!(http_status_to_grpc_code(429), tonic::Code::ResourceExhausted);
        assert_eq!(http_status_to_grpc_code(503), tonic::Code::Unavailable);
        assert_eq!(http_status_to_grpc_code(504), tonic::Code::DeadlineExceeded);
        // Unknown HTTP status falls back to Internal
        assert_eq!(http_status_to_grpc_code(418), tonic::Code::Internal);
        assert_eq!(http_status_to_grpc_code(500), tonic::Code::Internal);
    }

    #[test]
    fn test_error_type_to_grpc_code() {
        assert_eq!(error_type_to_grpc_code("invalid_request_error"), tonic::Code::InvalidArgument);
        assert_eq!(error_type_to_grpc_code("authentication_error"), tonic::Code::Unauthenticated);
        assert_eq!(error_type_to_grpc_code("permission_denied_error"), tonic::Code::PermissionDenied);
        assert_eq!(error_type_to_grpc_code("not_found_error"), tonic::Code::NotFound);
        assert_eq!(error_type_to_grpc_code("service_unavailable"), tonic::Code::Unavailable);
        assert_eq!(error_type_to_grpc_code("model_not_ready"), tonic::Code::Unavailable);
        // P9-1: decoupled stream on a model without predict_decoupled.
        assert_eq!(error_type_to_grpc_code("not_implemented"), tonic::Code::FailedPrecondition);
        // Unknown falls back to Internal
        assert_eq!(error_type_to_grpc_code("UNKNOWN_CODE"), tonic::Code::Internal);
    }

    #[test]
    fn test_model_error_status_carries_error_info() {
        let parsed = ParsedModelError {
            error_type: "invalid_request_error".into(),
            message: "bad input".into(),
            code: Some("invalid_input".into()),
            param: Some("temperature".into()),
        };
        let status = model_error_status(tonic::Code::InvalidArgument, &parsed);
        // Message format unchanged for backward compatibility
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "[invalid_request_error] bad input");
        // Structured details: standard gRPC ErrorInfo
        let info = status.get_details_error_info().expect("should carry ErrorInfo");
        assert_eq!(info.reason, "invalid_input");
        assert_eq!(info.domain, "lite-server");
        assert_eq!(info.metadata.get("error_type").map(String::as_str),
            Some("invalid_request_error"));
        assert_eq!(info.metadata.get("param").map(String::as_str), Some("temperature"));
    }

    #[test]
    fn test_model_error_status_reason_falls_back_to_error_type() {
        let parsed = ParsedModelError {
            error_type: "model_error".into(),
            message: "boom".into(),
            code: None,
            param: None,
        };
        let status = model_error_status(tonic::Code::Internal, &parsed);
        let info = status.get_details_error_info().expect("should carry ErrorInfo");
        assert_eq!(info.reason, "model_error");
        assert!(!info.metadata.contains_key("param"));
    }

    #[test]
    fn test_try_parse_model_error_valid() {
        let data = serde_json::json!({
            "error": {
                "type": "INVALID_INPUT",
                "message": "input must be non-negative",
                "code": "invalid_input",
                "param": "temperature",
            }
        });
        let result = try_parse_model_error(&data);
        assert!(result.is_some());
        let p = result.unwrap();
        assert_eq!(p.error_type, "INVALID_INPUT");
        assert_eq!(p.message, "input must be non-negative");
        assert_eq!(p.code.as_deref(), Some("invalid_input"));
        assert_eq!(p.param.as_deref(), Some("temperature"));
    }

    #[test]
    fn test_try_parse_model_error_minimal() {
        // Only required fields (type + message), no code/param — still valid
        let data = serde_json::json!({
            "error": {"type": "X", "message": "Y"}
        });
        let result = try_parse_model_error(&data);
        assert!(result.is_some());
        let p = result.unwrap();
        assert_eq!(p.error_type, "X");
        assert_eq!(p.message, "Y");
        assert_eq!(p.code, None);
        assert_eq!(p.param, None);
    }

    #[test]
    fn test_try_parse_model_error_legacy_format() {
        // Legacy format: {"error": "plain string"} — not parsable as model error
        let data = serde_json::json!({"error": "TypeError: something"});
        let result = try_parse_model_error(&data);
        assert!(result.is_none());
    }

    #[test]
    fn test_try_parse_model_error_missing_fields() {
        let data = serde_json::json!({"error": {"type": "X"}});
        assert!(try_parse_model_error(&data).is_none());

        let data = serde_json::json!({"error": {"message": "X"}});
        assert!(try_parse_model_error(&data).is_none());

        let data = serde_json::json!({"other": "stuff"});
        assert!(try_parse_model_error(&data).is_none());
    }
}
