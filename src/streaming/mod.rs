use crate::error::AppError;
use crate::proto::liteserver as pb;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

const STREAM_CHANNEL_SIZE: usize = 64;

/// Handle for an active stream.
pub struct StreamHandle {
    pub chunk_rx: mpsc::Receiver<pb::StreamResponse>,
    _cancel_tx: oneshot::Sender<()>,
}

/// Engine that manages the lifecycle of streaming requests.
pub struct StreamingEngine {
    streams: DashMap<String, mpsc::Sender<pb::StreamResponse>>,
}

impl Default for StreamingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingEngine {
    pub fn new() -> Self {
        Self {
            streams: DashMap::new(),
        }
    }

    /// Register a new stream and return a handle for receiving chunks.
    pub fn register_stream(&self, stream_id: String) -> StreamHandle {
        let (chunk_tx, chunk_rx) = mpsc::channel(STREAM_CHANNEL_SIZE);
        let (cancel_tx, _cancel_rx) = oneshot::channel();
        self.streams.insert(stream_id, chunk_tx);
        StreamHandle {
            chunk_rx,
            _cancel_tx: cancel_tx,
        }
    }

    /// Route a chunk to the appropriate stream.
    pub fn route_chunk(&self, stream_id: &str, chunk: pb::StreamResponse) -> bool {
        if let Some(entry) = self.streams.get(stream_id) {
            if entry.try_send(chunk).is_err() {
                self.streams.remove(stream_id);
                return false;
            }
            true
        } else {
            false
        }
    }

    /// Mark a stream as done and remove it.
    pub fn finish_stream(&self, stream_id: &str) {
        self.streams.remove(stream_id);
    }

    /// Get the sender for a stream (used by transport layer).
    pub fn get_sender(&self, stream_id: &str) -> Option<mpsc::Sender<pb::StreamResponse>> {
        self.streams.get(stream_id).map(|e| e.clone())
    }

    /// Cancel a stream.
    pub fn cancel_stream(&self, stream_id: &str) {
        self.streams.remove(stream_id);
    }

    /// Check if a stream exists.
    pub fn has_stream(&self, stream_id: &str) -> bool {
        self.streams.contains_key(stream_id)
    }
}

/// Build a protobuf StreamRequest::Open.
pub fn build_stream_open(stream_id: String, data: Vec<u8>, meta: Option<pb::RequestMeta>) -> pb::Request {
    pb::Request {
        uid: format!("stream-open-{}", stream_id),
        meta: meta.clone(),
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Open(pb::StreamOpen { data, meta })),
        })),
    }
}

/// Build a protobuf StreamRequest::Chunk.
pub fn build_stream_chunk(stream_id: String, data: Vec<u8>) -> pb::Request {
    pb::Request {
        uid: format!("stream-chunk-{}", stream_id),
        meta: None,
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Chunk(pb::StreamChunk { data })),
        })),
    }
}

/// Build a protobuf StreamRequest::Close.
pub fn build_stream_close(stream_id: String) -> pb::Request {
    pb::Request {
        uid: format!("stream-close-{}", stream_id),
        meta: None,
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Close(pb::StreamClose {})),
        })),
    }
}

/// Build a protobuf StreamRequest::Cancel.
pub fn build_stream_cancel(stream_id: String) -> pb::Request {
    pb::Request {
        uid: format!("stream-cancel-{}", stream_id),
        meta: None,
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Cancel(pb::StreamCancel {})),
        })),
    }
}
