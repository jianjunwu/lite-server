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

    /// Model-initiated error with explicit HTTP status and client-facing message.
    /// Unlike WorkerCrashed, the message is NOT sanitized — the model author
    /// intentionally exposes it.
    #[error("model error ({0}): {2}")]
    ModelError(u16, String, String),  // status_code, error_type, detail
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
            // ModelError is handled specially in IntoResponse
            // and never reaches this point, but provide a fallback.
            AppError::ModelError(_, _, _) => "model error",
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // Model errors carry a model-author-facing message — return it
        // directly without sanitization.
        if let AppError::ModelError(status_code, error_type, message) = &self {
            let status = StatusCode::from_u16(*status_code)
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            // Log at info level — not a server fault, the model intentionally
            // rejected the request.
            tracing::info!(
                status = %status_code,
                error_type = %error_type,
                detail = %message,
                "model error"
            );
            let body = Json(json!({
                "error": {
                    "type": error_type,
                    "message": message,
                }
            }));
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
            // Handled above via early return; should never reach here.
            AppError::ModelError(..) => unreachable!(),
        };

        // Log full internal details for operational debugging.
        // The sanitized message is what goes to the client.
        tracing::error!(error_type = %error_type, detail = %self.to_string(), "request error");

        let body = Json(json!({
            "error": {
                "type": error_type,
                "message": self.pub_error_message(),
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
        let err = AppError::ModelError(
            400,
            "INVALID_INPUT".to_string(),
            "input must be non-negative".to_string(),
        );
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
        let err = AppError::ModelError(
            503,
            "MODEL_NOT_READY".to_string(),
            "model loading".to_string(),
        );
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // 404
        let err = AppError::ModelError(
            404,
            "NOT_FOUND".to_string(),
            "item not in vocab".to_string(),
        );
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
    }
}
