use crate::proto::liteserver as pb;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

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
        // Clone the sender so the DashMap read guard is dropped before any mutation.
        let sender = match self.streams.get(stream_id) {
            Some(entry) => entry.clone(),
            None => return false,
        };
        if sender.try_send(chunk).is_err() {
            self.streams.remove(stream_id);
            return false;
        }
        true
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- StreamingEngine lifecycle ---

    #[test]
    fn test_register_and_has_stream() {
        let engine = StreamingEngine::new();
        assert!(!engine.has_stream("s1"));

        let _handle = engine.register_stream("s1".to_string());
        assert!(engine.has_stream("s1"));
    }

    #[test]
    fn test_finish_stream_removes() {
        let engine = StreamingEngine::new();
        let _handle = engine.register_stream("s1".to_string());
        assert!(engine.has_stream("s1"));

        engine.finish_stream("s1");
        assert!(!engine.has_stream("s1"));
    }

    #[test]
    fn test_cancel_stream_removes() {
        let engine = StreamingEngine::new();
        let _handle = engine.register_stream("s1".to_string());

        engine.cancel_stream("s1");
        assert!(!engine.has_stream("s1"));
    }

    #[test]
    fn test_finish_nonexistent_is_noop() {
        let engine = StreamingEngine::new();
        engine.finish_stream("nope"); // should not panic
    }

    #[test]
    fn test_cancel_nonexistent_is_noop() {
        let engine = StreamingEngine::new();
        engine.cancel_stream("nope"); // should not panic
    }

    #[tokio::test]
    async fn test_route_chunk_delivers_to_handle() {
        let engine = StreamingEngine::new();
        let mut handle = engine.register_stream("s1".to_string());

        let chunk = pb::StreamResponse {
            stream_id: "s1".to_string(),
            payload: Some(pb::stream_response::Payload::Chunk(
                pb::StreamChunkResponse {
                    data: b"hello".to_vec(),
                    is_final: false,
                },
            )),
        };

        let routed = engine.route_chunk("s1", chunk);
        assert!(routed);

        let received = handle.chunk_rx.recv().await.unwrap();
        match received.payload {
            Some(pb::stream_response::Payload::Chunk(c)) => {
                assert_eq!(c.data, b"hello");
            }
            _ => panic!("expected chunk"),
        }
    }

    #[test]
    fn test_route_chunk_to_nonexistent_returns_false() {
        let engine = StreamingEngine::new();
        let chunk = pb::StreamResponse {
            stream_id: "nope".to_string(),
            payload: Some(pb::stream_response::Payload::Done(pb::StreamDone { metrics: None })),
        };
        assert!(!engine.route_chunk("nope", chunk));
    }

    #[tokio::test]
    async fn test_route_chunk_channel_full_removes_stream() {
        // Use a tiny channel to test full-buffer behavior without sending 64 items
        let engine = StreamingEngine::new();
        let (chunk_tx, mut chunk_rx) = mpsc::channel(2); // tiny capacity
        engine.streams.insert(
            "s-tiny".to_string(),
            chunk_tx,
        );

        // Fill the tiny channel
        for _ in 0..2 {
            let chunk = pb::StreamResponse {
                stream_id: "s-tiny".to_string(),
                payload: Some(pb::stream_response::Payload::Done(pb::StreamDone { metrics: None })),
            };
            assert!(engine.route_chunk("s-tiny", chunk));
        }

        // Next one should fail (channel full) and remove the stream
        let chunk = pb::StreamResponse {
            stream_id: "s-tiny".to_string(),
            payload: Some(pb::stream_response::Payload::Done(pb::StreamDone { metrics: None })),
        };
        assert!(!engine.route_chunk("s-tiny", chunk));
        assert!(!engine.has_stream("s-tiny"));

        // Drain
        while chunk_rx.try_recv().is_ok() {}
    }

    #[test]
    fn test_get_sender_returns_some_for_active() {
        let engine = StreamingEngine::new();
        let _handle = engine.register_stream("s1".to_string());
        assert!(engine.get_sender("s1").is_some());
    }

    #[test]
    fn test_get_sender_returns_none_for_missing() {
        let engine = StreamingEngine::new();
        assert!(engine.get_sender("nope").is_none());
    }

    // --- build_stream_* helpers ---

    #[test]
    fn test_build_stream_open() {
        let req = build_stream_open("s1".to_string(), b"data".to_vec(), None);
        assert_eq!(req.uid, "stream-open-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert_eq!(s.stream_id, "s1");
                match s.action {
                    Some(pb::stream_request::Action::Open(o)) => {
                        assert_eq!(o.data, b"data");
                    }
                    _ => panic!("expected open action"),
                }
            }
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_chunk() {
        let req = build_stream_chunk("s1".to_string(), b"chunk".to_vec());
        assert_eq!(req.uid, "stream-chunk-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => match s.action {
                Some(pb::stream_request::Action::Chunk(c)) => {
                    assert_eq!(c.data, b"chunk");
                }
                _ => panic!("expected chunk action"),
            },
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_close() {
        let req = build_stream_close("s1".to_string());
        assert_eq!(req.uid, "stream-close-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert!(matches!(s.action, Some(pb::stream_request::Action::Close(_))));
            }
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_cancel() {
        let req = build_stream_cancel("s1".to_string());
        assert_eq!(req.uid, "stream-cancel-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert!(matches!(s.action, Some(pb::stream_request::Action::Cancel(_))));
            }
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_open_with_meta() {
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: Default::default(),
            client_ip: "1.2.3.4".to_string(),
            request_id: "r1".to_string(),
            timestamp_ns: 100,
            payload: vec![],
        };
        let req = build_stream_open("s1".to_string(), b"d".to_vec(), Some(meta));
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => match s.action {
                Some(pb::stream_request::Action::Open(o)) => {
                    assert!(o.meta.is_some());
                    assert_eq!(o.meta.unwrap().client_ip, "1.2.3.4");
                }
                _ => panic!("expected open"),
            },
            _ => panic!("expected stream"),
        }
    }
}
