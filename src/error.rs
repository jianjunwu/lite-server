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

    #[error("internal error: {0}")]
    Internal(String),
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
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
        };

        let body = Json(json!({
            "error": {
                "code": code,
                "message": self.to_string(),
            }
        }));

        (status, body).into_response()
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
