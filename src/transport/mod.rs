pub mod uds;
pub mod zmq;

use crate::error::AppError;
use crate::proto::liteserver as pb;
use std::future::Future;
use tokio::sync::mpsc;

/// Unified transport interface for worker communication.
pub trait WorkerTransport: Send + Sync {
    /// Send a unary/batch request and wait for a single response.
    fn send(
        &self,
        request: pb::Request,
    ) -> impl Future<Output = Result<pb::Response, AppError>> + Send;

    /// Send a streaming request and receive chunks via a channel.
    fn send_stream(
        &self,
        request: pb::Request,
    ) -> impl Future<Output = Result<mpsc::Receiver<pb::StreamResponse>, AppError>> + Send;

    /// Close the transport connection.
    fn close(&self);
}
