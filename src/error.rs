use crate::protocol::CanonicalError;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::collections::HashMap;
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("model not found: {0}")]
    ModelNotFound(String),

    #[error("model not ready: {0}")]
    ModelNotReady(String),

    #[error("model version not found: {0}/{1}")]
    VersionNotFound(String, String),

    #[error("model version already loaded: {0}/{1}")]
    VersionAlreadyLoaded(String, String),

    #[error("inference timeout: {0}")]
    InferenceTimeout(String),

    #[error("queue full: {0}")]
    QueueFull(String),

    #[error("worker crashed: {0}")]
    WorkerCrashed(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("python error: {0}")]
    Python(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("frame too large")]
    FrameTooLarge,

    #[error("internal error: {0}")]
    Internal(String),

    /// No route matched the request path (router fallback).
    #[error("route not found")]
    RouteNotFound,

    /// Route exists but does not support the request method.
    #[error("method not allowed")]
    MethodNotAllowed,

    /// Request body could not be read or parsed as JSON (axum extractor
    /// rejection). The detail is client-facing: it describes the client's
    /// own request body and carries no internal information.
    #[error("invalid request body: {0}")]
    InvalidRequestBody(String),

    /// Query string could not be deserialized (axum extractor rejection).
    /// The detail is client-facing for the same reason as InvalidRequestBody.
    #[error("invalid query parameter: {0}")]
    InvalidQueryParam(String),

    /// Request body exceeded `server.max_request_body_bytes` (HTTP 413).
    #[error("payload too large: {max_size} bytes max")]
    PayloadTooLarge { max_size: usize, actual_size: Option<u64> },

    /// Content-Encoding (compression) is not supported on inference routes
    /// (HTTP 415). The detail is client-facing.
    #[error("unsupported media type: {0}")]
    UnsupportedMediaType(String),

    /// Rate limit exceeded — HTTP 429 with Retry-After header.
    #[error("rate limit exceeded")]
    RateLimitExceeded { retry_after_secs: u64 },

    /// Missing or invalid API key — HTTP 401. The detail names the header
    /// that was checked; it is client-facing (mirrors the old Python-side
    /// RequireApiKey messages).
    #[error("{0}")]
    Unauthorized(String),

    /// Model-initiated error with explicit HTTP status and client-facing message.
    /// Unlike WorkerCrashed, the message is NOT sanitized — the model author
    /// intentionally exposes it. Boxed so `AppError` stays small (this payload
    /// carries several Strings + a header map — without boxing it dominates the
    /// enum size and every `Result<_, AppError>` blows past the large-Err
    /// threshold).
    #[error("{0}")]
    ModelError(Box<ModelErrorData>),
}

/// Owned payload for [`AppError::ModelError`].
#[derive(Debug)]
pub struct ModelErrorData {
    pub status_code: u16,
    pub error_type: String,
    pub detail: String,
    pub code: Option<String>,
    pub param: Option<String>,
    /// Extra response headers from the model's HTTPException (e.g.
    /// Retry-After on 429/503), forwarded to the client verbatim minus
    /// hop-by-hop / library-managed headers.
    pub headers: Option<HashMap<String, String>>,
}

impl std::fmt::Display for ModelErrorData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "model error ({}): {}", self.status_code, self.detail)
    }
}

impl AppError {
    /// Return a sanitized, public-facing error message.
    /// Never includes file paths, stack traces, or other internal details.
    pub fn pub_error_message(&self) -> &'static str {
        match self {
            AppError::ModelNotFound(_) => "model not found",
            AppError::ModelNotReady(_) => "model not ready",
            AppError::VersionNotFound(_, _) => "model version not found",
            AppError::VersionAlreadyLoaded(_, _) => "model version already loaded",
            AppError::InferenceTimeout(_) => "inference timeout",
            AppError::QueueFull(_) => "queue full",
            AppError::WorkerCrashed(_) => "service temporarily unavailable",
            AppError::Validation(_) => "validation error",
            AppError::Config(_) => "invalid configuration",
            AppError::Transport(_) => "transport error",
            AppError::Python(_) => "internal server error",
            AppError::Io(_) => "internal server error",
            AppError::Serialization(_) => "serialization error",
            AppError::FrameTooLarge => "message too large",
            AppError::Internal(_) => "internal server error",
            AppError::RouteNotFound => "route not found",
            AppError::MethodNotAllowed => "method not allowed",
            AppError::InvalidRequestBody(_) => "invalid request body",
            AppError::InvalidQueryParam(_) => "invalid query parameter",
            AppError::PayloadTooLarge { .. } => "payload too large",
            AppError::UnsupportedMediaType(_) => "unsupported media type",
            AppError::RateLimitExceeded { .. } => "rate limit exceeded",
            AppError::Unauthorized(_) => "unauthorized",
            // ModelError is handled specially in IntoResponse
            // and never reaches this point, but provide a fallback.
            AppError::ModelError(_) => "model error",
        }
    }

    /// Return the message shown to the client. For most variants this is the
    /// sanitized static message; InvalidRequestBody passes the parse detail
    /// through because it describes the client's own request body.
    fn client_message(&self) -> String {
        match self {
            AppError::InvalidRequestBody(detail) => detail.clone(),
            AppError::InvalidQueryParam(detail) => detail.clone(),
            AppError::PayloadTooLarge { max_size, actual_size } => {
                if let Some(actual) = actual_size {
                    format!(
                        "request body too large: {} bytes exceeds the {} bytes limit",
                        actual, max_size
                    )
                } else {
                    format!(
                        "request body exceeds the {} bytes limit",
                        max_size
                    )
                }
            }
            AppError::UnsupportedMediaType(detail) => detail.clone(),
            AppError::Unauthorized(detail) => detail.clone(),
            _ => self.pub_error_message().to_string(),
        }
    }

    /// Return a machine-readable error code for programmatic handling.
    /// Follows OpenAI convention: snake_case string unique per error condition.
    pub fn error_code(&self) -> &str {
        match self {
            AppError::ModelNotFound(_) => "model_not_found",
            AppError::ModelNotReady(_) => "model_not_ready",
            AppError::VersionNotFound(_, _) => "version_not_found",
            AppError::VersionAlreadyLoaded(_, _) => "version_already_loaded",
            AppError::InferenceTimeout(_) => "timeout",
            AppError::QueueFull(_) => "queue_full",
            AppError::WorkerCrashed(_) => "internal_error",
            AppError::Validation(_) => "invalid_parameter_value",
            AppError::Config(_) => "invalid_configuration",
            AppError::Serialization(_) => "parse_error",
            AppError::FrameTooLarge => "content_size_limit_exceeded",
            AppError::Transport(_) => "internal_error",
            AppError::Python(_) => "internal_error",
            AppError::Io(_) => "internal_error",
            AppError::Internal(_) => "internal_error",
            AppError::RouteNotFound => "route_not_found",
            AppError::MethodNotAllowed => "method_not_allowed",
            AppError::InvalidRequestBody(_) => "invalid_request_body",
            AppError::InvalidQueryParam(_) => "invalid_query_param",
            AppError::PayloadTooLarge { .. } => "payload_too_large",
            AppError::UnsupportedMediaType(_) => "unsupported_media_type",
            AppError::RateLimitExceeded { .. } => "rate_limit_exceeded",
            AppError::Unauthorized(_) => "unauthorized",
            // ModelError code comes from the Python worker; fallback to its error_type
            AppError::ModelError(d) => d.code.as_deref().unwrap_or(d.error_type.as_str()),
        }
    }

    /// Return the parameter name that caused the error, if applicable.
    pub fn param(&self) -> Option<&str> {
        match self {
            AppError::ModelError(d) => d.param.as_deref(),
            _ => None,
        }
    }

    /// S1(b)/D7:HTTP status 映射——`IntoResponse` 的 status 单源(流式早期
    /// 拒绝的 family 判定与响应状态保持一致,两处不会漂移)。
    pub fn http_status(&self) -> StatusCode {
        if let AppError::ModelError(d) = self {
            return StatusCode::from_u16(d.status_code)
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        }
        if matches!(self, AppError::PayloadTooLarge { .. }) {
            return StatusCode::PAYLOAD_TOO_LARGE;
        }
        match self {
            AppError::ModelNotFound(_) => StatusCode::NOT_FOUND,
            AppError::ModelNotReady(_) => StatusCode::SERVICE_UNAVAILABLE,
            AppError::VersionNotFound(_, _) => StatusCode::NOT_FOUND,
            AppError::VersionAlreadyLoaded(_, _) => StatusCode::CONFLICT,
            AppError::InferenceTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
            AppError::QueueFull(_) => StatusCode::SERVICE_UNAVAILABLE,
            AppError::WorkerCrashed(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Validation(_) => StatusCode::BAD_REQUEST,
            AppError::Config(_) => StatusCode::BAD_REQUEST,
            AppError::Transport(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Python(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Serialization(_) => StatusCode::BAD_REQUEST,
            AppError::FrameTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::RouteNotFound => StatusCode::NOT_FOUND,
            AppError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            AppError::InvalidRequestBody(_) => StatusCode::BAD_REQUEST,
            AppError::InvalidQueryParam(_) => StatusCode::BAD_REQUEST,
            AppError::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            AppError::RateLimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            AppError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            AppError::ModelError(_) => unreachable!(), // 上面早返
        }
    }
}

impl AppError {
    /// 迁移为与协议无关的错误值表(D11)。字段值按现状 `IntoResponse` 的
    /// 映射**原样搬移**(P2.0 零行为变化,字节快照门禁),只移动 JSON 拼装
    /// 不移动映射逻辑;wire 拼装全部迁入 `protocol::render` 分派。
    pub(crate) fn into_canonical(self) -> CanonicalError {
        // ModelError:模型作者显式暴露的状态/消息/header(不 sanitize),
        // 日志级别 info(模型主动拒绝,非服务器故障)。
        if let AppError::ModelError(d) = &self {
            return CanonicalError {
                status: StatusCode::from_u16(d.status_code)
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                error_type: d.error_type.clone(),
                // Option 保留:模型未提供 code 时 wire 输出 "code": null
                // (现状字节,快照门禁)。
                code: d.code.clone(),
                message: d.detail.clone(),
                param: d.param.clone(),
                extra: None,
                headers: d.headers.clone(),
                from_model: true,
                log_detail: d.detail.clone(),
            };
        }
        // PayloadTooLarge:413 + extra{max_size, actual_size}——各协议
        // renderer 决定是否合入(Legacy 合入;Kserve 按规范丢弃)。
        if let AppError::PayloadTooLarge { max_size, actual_size } = &self {
            return CanonicalError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                error_type: "invalid_request_error".to_string(),
                code: Some(self.error_code().to_string()),
                message: self.client_message(),
                param: None,
                extra: Some(json!({ "max_size": max_size, "actual_size": actual_size })),
                headers: None,
                from_model: false,
                log_detail: self.to_string(),
            };
        }
        let status = self.http_status();
        let error_type = match &self {
            AppError::ModelNotFound(_) => "not_found_error",
            AppError::ModelNotReady(_) => "model_not_ready",
            AppError::VersionNotFound(_, _) => "not_found_error",
            AppError::VersionAlreadyLoaded(_, _) => "conflict_error",
            AppError::InferenceTimeout(_) => "server_error",
            AppError::QueueFull(_) => "queue_full",
            AppError::WorkerCrashed(_) => "server_error",
            AppError::Validation(_) => "invalid_request_error",
            AppError::Config(_) => "invalid_request_error",
            AppError::Transport(_) => "server_error",
            AppError::Python(_) => "server_error",
            AppError::Io(_) => "server_error",
            AppError::Serialization(_) => "invalid_request_error",
            AppError::FrameTooLarge => "invalid_request_error",
            AppError::Internal(_) => "server_error",
            AppError::RouteNotFound => "not_found_error",
            AppError::MethodNotAllowed => "method_not_allowed",
            AppError::InvalidRequestBody(_) => "invalid_request_error",
            AppError::InvalidQueryParam(_) => "invalid_request_error",
            AppError::PayloadTooLarge { .. } => "invalid_request_error",
            AppError::UnsupportedMediaType(_) => "invalid_request_error",
            AppError::RateLimitExceeded { .. } => "rate_limit_exceeded",
            AppError::Unauthorized(_) => "authentication_error",
            // 上面早返,应不可达。
            AppError::ModelError(_) => unreachable!(),
        };
        // Retry-After:QueueFull 恒 1 秒;RateLimitExceeded 取配置秒数。
        let headers = if matches!(self, AppError::QueueFull(_)) {
            Some(HashMap::from([("retry-after".to_string(), "1".to_string())]))
        } else if let AppError::RateLimitExceeded { retry_after_secs } = &self {
            Some(HashMap::from([(
                "retry-after".to_string(),
                retry_after_secs.to_string(),
            )]))
        } else {
            None
        };
        CanonicalError {
            status,
            error_type: error_type.to_string(),
            code: Some(self.error_code().to_string()),
            message: self.client_message(),
            param: self.param().map(String::from),
            extra: None,
            headers,
            from_model: false,
            log_detail: self.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // Legacy shim(P2.0):全部既有调用点(测试、gRPC admin、fallback、单测
        // 构造的 router)零改动、byte-identical。协议感知路径走 ProtocolError
        // (axum IntoResponse 无请求上下文,协议须挂在错误上携带——D11)。
        crate::protocol::render(self.into_canonical(), crate::protocol::ApiProtocol::Legacy)
    }
}

/// 协议感知的错误边界(D11):错误自身携带语义协议,render 分派在
/// `src/protocol/`(protocol/ 不反向依赖核心)。
#[derive(Debug)]
pub struct ProtocolError {
    pub error: AppError,
    pub protocol: crate::protocol::ApiProtocol,
}

impl From<AppError> for ProtocolError {
    fn from(error: AppError) -> Self {
        ProtocolError { error, protocol: crate::protocol::ApiProtocol::Legacy }
    }
}

impl IntoResponse for ProtocolError {
    fn into_response(self) -> Response {
        crate::protocol::render(self.error.into_canonical(), self.protocol)
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError::Internal(err.to_string())
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for AppError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        AppError::Internal(err.to_string())
    }
}

/// Map axum `Bytes` body-extraction rejection to [`AppError`].
/// Length-limit (body too large) → `PayloadTooLarge`; everything else → `InvalidRequestBody`.
/// `actual_size` is the client-advertised Content-Length when present (D4/D5:
/// lets clients self-correct); chunked/unknown-length bodies carry None.
pub(crate) fn map_body_rejection(
    rejection: axum::extract::rejection::BytesRejection,
    max_size: usize,
    actual_size: Option<u64>,
) -> AppError {
    use axum::response::IntoResponse;
    let status = rejection.into_response().status();
    if status == axum::http::StatusCode::PAYLOAD_TOO_LARGE {
        AppError::PayloadTooLarge {
            max_size,
            actual_size,
        }
    } else {
        AppError::InvalidRequestBody("failed to read request body".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pub_error_message_does_not_leak_paths() {
        let err = AppError::ModelNotFound(
            "bert at /home/user/models/bert".to_string());
        assert_eq!(err.pub_error_message(), "model not found");
        assert!(!err.pub_error_message().contains("/home/user"));

        let err = AppError::Io(
            std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"));
        assert_eq!(err.pub_error_message(), "internal server error");
        assert!(!err.pub_error_message().contains("file not found"));

        let err = AppError::Internal("something broke at line 42".to_string());
        assert_eq!(err.pub_error_message(), "internal server error");
    }

    #[test]
    fn test_pub_error_message_variants() {
        assert_eq!(
            AppError::ModelNotReady("x".to_string()).pub_error_message(),
            "model not ready"
        );
        assert_eq!(
            AppError::VersionNotFound("a".to_string(), "b".to_string()).pub_error_message(),
            "model version not found"
        );
        assert_eq!(
            AppError::InferenceTimeout("x".to_string()).pub_error_message(),
            "inference timeout"
        );
        assert_eq!(
            AppError::QueueFull("x".to_string()).pub_error_message(),
            "queue full"
        );
        assert_eq!(
            AppError::WorkerCrashed("x".to_string()).pub_error_message(),
            "service temporarily unavailable"
        );
        assert_eq!(
            AppError::Validation("x".to_string()).pub_error_message(),
            "validation error"
        );
        assert_eq!(
            AppError::Config("x".to_string()).pub_error_message(),
            "invalid configuration"
        );
        assert_eq!(
            AppError::Transport("x".to_string()).pub_error_message(),
            "transport error"
        );
        assert_eq!(
            AppError::Python("x".to_string()).pub_error_message(),
            "internal server error"
        );
        assert_eq!(
            AppError::Serialization(
                serde_json::from_str::<serde_json::Value>("invalid").unwrap_err()
            ).pub_error_message(),
            "serialization error"
        );
    }

    #[tokio::test]
    async fn test_queue_full_has_retry_after_header() {
        use axum::response::IntoResponse;
        let err = AppError::QueueFull("queue full".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let retry_after = response.headers().get("retry-after");
        assert!(retry_after.is_some(), "QueueFull should have Retry-After header");
        assert_eq!(retry_after.unwrap().to_str().unwrap(), "1");
    }

    #[tokio::test]
    async fn test_non_queue_full_no_retry_after_header() {
        use axum::response::IntoResponse;
        let err = AppError::ModelNotFound("test".to_string());
        let response = err.into_response();
        assert!(response.headers().get("retry-after").is_none(),
            "non-QueueFull errors should not have Retry-After header");
    }

    // ===== ModelError tests =====

    #[tokio::test]
    async fn test_model_error_passthrough_message() {
        use axum::response::IntoResponse;
        let err = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 400,
            error_type: "INVALID_INPUT".to_string(),
            detail: "input must be non-negative".to_string(),
            code: None,
            param: None,
            headers: None,
        }));
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Read body — the message should NOT be sanitized
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "INVALID_INPUT");
        assert_eq!(body["error"]["message"], "input must be non-negative");
    }

    #[tokio::test]
    async fn test_model_error_various_status_codes() {
        use axum::response::IntoResponse;
        // 503
        let err = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 503,
            error_type: "MODEL_NOT_READY".to_string(),
            detail: "model loading".to_string(),
            code: None,
            param: None,
            headers: None,
        }));
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // 404
        let err = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 404,
            error_type: "NOT_FOUND".to_string(),
            detail: "item not in vocab".to_string(),
            code: None,
            param: None,
            headers: None,
        }));
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_worker_crashed_still_sanitized() {
        use axum::response::IntoResponse;
        let err = AppError::WorkerCrashed("internal traceback details".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body_bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "server_error");
        // Must NOT leak internal details
        assert!(body["error"]["message"].as_str().unwrap().contains("unavailable"));
        assert!(!body["error"]["message"].as_str().unwrap().contains("traceback"));
        // Verify code + param are present
        assert_eq!(body["error"]["code"], "internal_error");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    // ===== New tests for code/param =====

    #[tokio::test]
    async fn test_error_response_has_code_field() {
        use axum::response::IntoResponse;
        let err = AppError::ModelNotFound("bert".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["code"], "model_not_found");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[test]
    fn test_error_code_values_all_variants() {
        assert_eq!(AppError::ModelNotFound("x".into()).error_code(), "model_not_found");
        assert_eq!(AppError::ModelNotReady("x".into()).error_code(), "model_not_ready");
        assert_eq!(AppError::VersionNotFound("a".into(), "b".into()).error_code(), "version_not_found");
        assert_eq!(AppError::VersionAlreadyLoaded("a".into(), "b".into()).error_code(), "version_already_loaded");
        assert_eq!(AppError::InferenceTimeout("x".into()).error_code(), "timeout");
        assert_eq!(AppError::QueueFull("x".into()).error_code(), "queue_full");
        assert_eq!(AppError::WorkerCrashed("x".into()).error_code(), "internal_error");
        assert_eq!(AppError::Validation("x".into()).error_code(), "invalid_parameter_value");
        assert_eq!(AppError::Config("x".into()).error_code(), "invalid_configuration");
        assert_eq!(AppError::Internal("x".into()).error_code(), "internal_error");
        assert_eq!(AppError::Transport("x".into()).error_code(), "internal_error");
        assert_eq!(AppError::Python("x".into()).error_code(), "internal_error");
        assert_eq!(AppError::Serialization(
            serde_json::from_str::<serde_json::Value>("invalid").unwrap_err()
        ).error_code(), "parse_error");
        assert_eq!(AppError::FrameTooLarge.error_code(), "content_size_limit_exceeded");
    }

    #[test]
    fn test_param_defaults_to_none() {
        assert_eq!(AppError::ModelNotFound("x".into()).param(), None);
        assert_eq!(AppError::Validation("x".into()).param(), None);
    }

    #[tokio::test]
    async fn test_model_error_with_code_and_param() {
        use axum::response::IntoResponse;
        let err = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 400,
            error_type: "invalid_request_error".into(),
            detail: "bad input".into(),
            code: Some("invalid_input".into()),
            param: Some("temperature".into()),
            headers: None,
        }));
        let response = err.into_response();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["code"], "invalid_input");
        assert_eq!(body["error"]["param"], "temperature");
    }

    #[tokio::test]
    async fn test_model_error_with_code_no_param() {
        use axum::response::IntoResponse;
        let err = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 400,
            error_type: "invalid_request_error".into(),
            detail: "bad input".into(),
            code: Some("invalid_input".into()),
            param: None,
            headers: None,
        }));
        let response = err.into_response();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["code"], "invalid_input");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_model_error_forwards_headers() {
        use axum::response::IntoResponse;
        let mut hdrs = std::collections::HashMap::new();
        hdrs.insert("retry-after".to_string(), "5".to_string());
        hdrs.insert("x-trace".to_string(), "abc".to_string());
        let err = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 503,
            error_type: "model_error".to_string(),
            detail: "overloaded".to_string(),
            code: None,
            param: None,
            headers: Some(hdrs),
        }));
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get("retry-after").unwrap(), "5");
        assert_eq!(response.headers().get("x-trace").unwrap(), "abc");
    }

    // ===== Framework-level errors (route/method/body) =====

    #[tokio::test]
    async fn test_route_not_found_response() {
        use axum::response::IntoResponse;
        let response = AppError::RouteNotFound.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "not_found_error");
        assert_eq!(body["error"]["code"], "route_not_found");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_method_not_allowed_response() {
        use axum::response::IntoResponse;
        let response = AppError::MethodNotAllowed.into_response();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "method_not_allowed");
        assert_eq!(body["error"]["code"], "method_not_allowed");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_version_already_loaded_response() {
        use axum::response::IntoResponse;
        let err = AppError::VersionAlreadyLoaded("m1".into(), "1".into());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "conflict_error");
        assert_eq!(body["error"]["code"], "version_already_loaded");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_invalid_request_body_passthrough_detail() {
        use axum::response::IntoResponse;
        // Client should see the parse detail — it describes their own request
        // body and carries no internal information.
        let err = AppError::InvalidRequestBody(
            "Failed to parse the request body as JSON: expected value at line 1 column 1".into());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_request_body");
        assert!(body["error"]["message"].as_str().unwrap().contains("expected value"));
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_invalid_query_param_passthrough_detail() {
        use axum::response::IntoResponse;
        let err = AppError::InvalidQueryParam(
            "Failed to deserialize query string: invalid type".into());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_query_param");
        assert!(body["error"]["message"].as_str().unwrap().contains("invalid type"));
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[test]
    fn test_framework_error_codes() {
        assert_eq!(AppError::RouteNotFound.error_code(), "route_not_found");
        assert_eq!(AppError::MethodNotAllowed.error_code(), "method_not_allowed");
        assert_eq!(AppError::InvalidRequestBody("x".into()).error_code(), "invalid_request_body");
        assert_eq!(AppError::InvalidQueryParam("x".into()).error_code(), "invalid_query_param");
    }

    // ===== C7: client errors (incl. 429) log at info, server errors at error =====

    #[derive(Default, Clone)]
    struct Counters {
        error: u64,
        warn: u64,
        info: u64,
    }

    /// A `tracing` layer that tallies emitted events by level, so tests can
    /// assert which log macro the IntoResponse path actually invoked.
    struct LevelCounter {
        inner: std::sync::Arc<std::sync::Mutex<Counters>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LevelCounter {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut c = self.inner.lock().unwrap();
            match *event.metadata().level() {
                tracing::Level::ERROR => c.error += 1,
                tracing::Level::WARN => c.warn += 1,
                tracing::Level::INFO => c.info += 1,
                _ => {}
            }
        }
    }

    fn event_counts<F: FnOnce()>(run: F) -> Counters {
        use tracing_subscriber::prelude::*;
        let counters = std::sync::Arc::new(std::sync::Mutex::new(Counters::default()));
        let layer = LevelCounter { inner: counters.clone() };
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, run);
        let c = counters.lock().unwrap().clone();
        c
    }

    #[tokio::test]
    async fn test_client_errors_log_at_info_not_error() {
        use axum::response::IntoResponse;
        // 429 (rate limit) must not spam at ERROR under load — log at INFO.
        let c = event_counts(|| { let _ = AppError::RateLimitExceeded { retry_after_secs: 1 }.into_response(); });
        assert_eq!(c.error, 0, "429 must not log at error");
        assert_eq!(c.info, 1, "429 should log at info");

        // 4xx validation likewise.
        let c = event_counts(|| { let _ = AppError::InvalidRequestBody("bad".into()).into_response(); });
        assert_eq!(c.error, 0);
        assert_eq!(c.info, 1);
    }

    #[tokio::test]
    async fn test_server_errors_still_log_at_error() {
        use axum::response::IntoResponse;
        let c = event_counts(|| { let _ = AppError::WorkerCrashed("boom".into()).into_response(); });
        assert_eq!(c.error, 1, "5xx must still log at error");
        assert_eq!(c.info, 0);
    }
}
