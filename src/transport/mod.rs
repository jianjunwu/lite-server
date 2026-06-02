pub mod zmq;

use crate::error::AppError;
use crate::proto::liteserver as pb;
use std::future::Future;
use tokio::sync::mpsc;

/// Derive a deterministic localhost port from a path string (Windows only).
/// Used as a fallback when Unix Domain Sockets are not available.
/// Uses FNV-1a for cross-language consistency.
#[cfg(windows)]
pub fn derive_port_from_path(path: &str) -> u16 {
    let mut hash: u32 = 0x811c9dc5;
    for b in path.bytes() {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    30000 + (hash % 30000) as u16
}

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
