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
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            AppError::ModelNotFound(_) => (StatusCode::NOT_FOUND, "MODEL_NOT_FOUND"),
            AppError::ModelNotReady(_) => (StatusCode::SERVICE_UNAVAILABLE, "MODEL_NOT_READY"),
            AppError::VersionNotFound(_, _) => (StatusCode::NOT_FOUND, "VERSION_NOT_FOUND"),
            AppError::InferenceTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "INFERENCE_TIMEOUT"),
            AppError::QueueFull(_) => (StatusCode::SERVICE_UNAVAILABLE, "QUEUE_FULL"),
            AppError::WorkerCrashed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "WORKER_CRASHED"),
            AppError::Validation(_) => (StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
            AppError::Config(_) => (StatusCode::BAD_REQUEST, "CONFIG_ERROR"),
            AppError::Transport(_) => (StatusCode::INTERNAL_SERVER_ERROR, "TRANSPORT_ERROR"),
            AppError::Python(_) => (StatusCode::INTERNAL_SERVER_ERROR, "PYTHON_ERROR"),
            AppError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "IO_ERROR"),
            AppError::Serialization(_) => (StatusCode::BAD_REQUEST, "SERIALIZATION_ERROR"),
            AppError::FrameTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "FRAME_TOO_LARGE"),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
        };

        // Log full internal details for operational debugging.
        // The sanitized message is what goes to the client.
        tracing::error!(error_code = %code, detail = %self.to_string(), "request error");

        let body = Json(json!({
            "error": {
                "code": code,
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
}
