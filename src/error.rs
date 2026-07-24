use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("model not found: {0}")]
    ModelNotFound(String),

    #[error("model not ready: {0}")]
    ModelNotReady(String),

    #[error("model version not found: {0}/{1}")]
    VersionNotFound(String, String),

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

    /// Rate limit exceeded — HTTP 429 with Retry-After header.
    #[error("rate limit exceeded")]
    RateLimitExceeded { retry_after_secs: u64 },

    /// Model-initiated error with explicit HTTP status and client-facing message.
    /// Unlike WorkerCrashed, the message is NOT sanitized — the model author
    /// intentionally exposes it.
    #[error("model error ({status_code}): {detail}")]
    ModelError {
        status_code: u16,
        error_type: String,
        detail: String,
        code: Option<String>,
        param: Option<String>,
    },
}

impl AppError {
    /// Return a sanitized, public-facing error message.
    /// Never includes file paths, stack traces, or other internal details.
    pub fn pub_error_message(&self) -> &'static str {
        match self {
            AppError::ModelNotFound(_) => "model not found",
            AppError::ModelNotReady(_) => "model not ready",
            AppError::VersionNotFound(_, _) => "model version not found",
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
            AppError::RateLimitExceeded { .. } => "rate limit exceeded",
            // ModelError is handled specially in IntoResponse
            // and never reaches this point, but provide a fallback.
            AppError::ModelError { .. } => "model error",
        }
    }

    /// Return the message shown to the client. For most variants this is the
    /// sanitized static message; InvalidRequestBody passes the parse detail
    /// through because it describes the client's own request body.
    fn client_message(&self) -> String {
        match self {
            AppError::InvalidRequestBody(detail) => detail.clone(),
            AppError::InvalidQueryParam(detail) => detail.clone(),
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
            AppError::RateLimitExceeded { .. } => "rate_limit_exceeded",
            // ModelError code comes from the Python worker; fallback to its error_type
            AppError::ModelError { code, error_type, .. } => {
                code.as_deref().unwrap_or(error_type.as_str())
            }
        }
    }

    /// Return the parameter name that caused the error, if applicable.
    pub fn param(&self) -> Option<&str> {
        match self {
            AppError::ModelError { param, .. } => param.as_deref(),
            _ => None,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // Model errors carry a model-author-facing message — return it
        // directly without sanitization.
        if let AppError::ModelError {
            status_code,
            error_type,
            detail,
            ref code,
            ref param,
        } = &self
        {
            let status = StatusCode::from_u16(*status_code)
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            // Log at info level — not a server fault, the model intentionally
            // rejected the request.
            tracing::info!(
                status = %status_code,
                error_type = %error_type,
                code = ?code,
                detail = %detail,
                "model error"
            );
            let error_obj = json!({
                "type": error_type,
                "message": detail,
                "code": code,
                "param": param,
            });
            let body = Json(json!({ "error": error_obj }));
            return (status, body).into_response();
        }

        let (status, error_type) = match &self {
            AppError::ModelNotFound(_) => (StatusCode::NOT_FOUND, "not_found_error"),
            AppError::ModelNotReady(_) => (StatusCode::SERVICE_UNAVAILABLE, "model_not_ready"),
            AppError::VersionNotFound(_, _) => (StatusCode::NOT_FOUND, "not_found_error"),
            AppError::InferenceTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "server_error"),
            AppError::QueueFull(_) => (StatusCode::SERVICE_UNAVAILABLE, "queue_full"),
            AppError::WorkerCrashed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "server_error"),
            AppError::Validation(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
            AppError::Config(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
            AppError::Transport(_) => (StatusCode::INTERNAL_SERVER_ERROR, "server_error"),
            AppError::Python(_) => (StatusCode::INTERNAL_SERVER_ERROR, "server_error"),
            AppError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "server_error"),
            AppError::Serialization(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
            AppError::FrameTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "invalid_request_error"),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "server_error"),
            AppError::RouteNotFound => (StatusCode::NOT_FOUND, "not_found_error"),
            AppError::MethodNotAllowed => (StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
            AppError::InvalidRequestBody(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
            AppError::InvalidQueryParam(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
            AppError::RateLimitExceeded { .. } => (StatusCode::TOO_MANY_REQUESTS, "rate_limit_exceeded"),
            // Handled above via early return; should never reach here.
            AppError::ModelError { .. } => unreachable!(),
        };

        // Log full internal details for operational debugging.
        // The sanitized message is what goes to the client.
        tracing::error!(error_type = %error_type, code = %self.error_code(), detail = %self.to_string(), "request error");

        let body = Json(json!({
            "error": {
                "type": error_type,
                "message": self.client_message(),
                "code": self.error_code(),
                "param": self.param(),
            }
        }));

        let mut response = (status, body).into_response();

        // Add Retry-After header for 503 responses (queue full)
        if matches!(self, AppError::QueueFull(_)) {
            response.headers_mut().insert(
                "retry-after",
                axum::http::HeaderValue::from_static("1"),
            );
        }

        // Add Retry-After header for 429 responses (rate limit)
        if let AppError::RateLimitExceeded { retry_after_secs } = &self {
            if let Ok(val) = axum::http::HeaderValue::from_str(
                &retry_after_secs.to_string(),
            ) {
                response.headers_mut().insert("retry-after", val);
            }
        }

        response
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
        let err = AppError::ModelError {
            status_code: 400,
            error_type: "INVALID_INPUT".to_string(),
            detail: "input must be non-negative".to_string(),
            code: None,
            param: None,
        };
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
        let err = AppError::ModelError {
            status_code: 503,
            error_type: "MODEL_NOT_READY".to_string(),
            detail: "model loading".to_string(),
            code: None,
            param: None,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // 404
        let err = AppError::ModelError {
            status_code: 404,
            error_type: "NOT_FOUND".to_string(),
            detail: "item not in vocab".to_string(),
            code: None,
            param: None,
        };
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
        let err = AppError::ModelError {
            status_code: 400,
            error_type: "invalid_request_error".into(),
            detail: "bad input".into(),
            code: Some("invalid_input".into()),
            param: Some("temperature".into()),
        };
        let response = err.into_response();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["code"], "invalid_input");
        assert_eq!(body["error"]["param"], "temperature");
    }

    #[tokio::test]
    async fn test_model_error_with_code_no_param() {
        use axum::response::IntoResponse;
        let err = AppError::ModelError {
            status_code: 400,
            error_type: "invalid_request_error".into(),
            detail: "bad input".into(),
            code: Some("invalid_input".into()),
            param: None,
        };
        let response = err.into_response();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["code"], "invalid_input");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
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
}
