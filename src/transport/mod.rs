pub mod uds;

use crate::error::AppError;
use crate::worker::protocol::{InferenceRequest, InferenceResponse};
pub trait Transport: Send + Sync {
    fn send(
        &self,
        request: InferenceRequest,
    ) -> impl std::future::Future<Output = Result<InferenceResponse, AppError>> + Send;
}
