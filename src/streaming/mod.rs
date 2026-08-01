use crate::proto::liteserver as pb;

/// Build a protobuf StreamRequest::Open.
///
/// `decoupled` (P9-1): when true, sets the additive `StreamOpen.decoupled`
/// flag so the worker keeps the channel open after `predict_decoupled`
/// returns (model-controlled lifetime). False/None preserves the existing
/// stream_predict semantics on the wire.
pub fn build_stream_open(
    stream_id: String,
    data: bytes::Bytes,
    meta: Option<pb::RequestMeta>,
    decoupled: bool,
) -> pb::Request {
    pb::Request {
        uid: format!("stream-open-{}", stream_id),
        meta: meta.clone(),
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Open(pb::StreamOpen {
                data,
                meta,
                decoupled: if decoupled { Some(true) } else { None },
            })),
        })),
    }
}

/// Build a protobuf StreamRequest::Chunk.
pub fn build_stream_chunk(stream_id: String, data: bytes::Bytes) -> pb::Request {
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

/// Why a bounded stream recv was terminated by P-DEADLINE (蓝图 §4.0.4 两段式).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvElapsed {
    /// The overall per-request deadline fired.
    Deadline,
    /// The per-chunk idle budget fired.
    Idle,
}

/// Receive the next worker stream chunk under a P-DEADLINE two-stage bound.
///
/// `deadline`: absolute overall deadline (None = no overall bound).
/// `idle`: per-recv idle budget (None = no idle bound). When both are `None`
/// this is a plain `recv()` — i.e. streaming behavior is unchanged when no
/// client deadline is active. Regular streams gate both bounds on a client
/// spec; decoupled keeps its always-on idle and only adds the overall bound
/// when a client deadline is present.
///
/// Each call bounds THIS recv by `min(remaining-to-deadline, idle)`, so a
/// stalled stream trips the idle bound even before the overall deadline.
/// Returns `Ok(None)` when the worker closed the stream, `Ok(Some(chunk))`
/// for the next chunk, or `Err(RecvElapsed)` when a bound fired (caller logs
/// + breaks to end the stream, triggering the existing cancel/close cleanup).
pub async fn recv_chunk(
    rx: &mut tokio::sync::mpsc::Receiver<pb::StreamResponse>,
    deadline: Option<std::time::Instant>,
    idle: Option<std::time::Duration>,
) -> Result<Option<pb::StreamResponse>, RecvElapsed> {
    let now = std::time::Instant::now();
    // Overall deadline: already-expired → stop immediately; else cap this recv.
    if let Some(d) = deadline {
        if d <= now {
            return Err(RecvElapsed::Deadline);
        }
    }
    let to_deadline = deadline.map(|d| d - now);
    let per_recv = match (to_deadline, idle) {
        (None, None) => return Ok(rx.recv().await),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => Some(if a < b { a } else { b }),
    };
    match tokio::time::timeout(per_recv.unwrap(), rx.recv()).await {
        Ok(v) => Ok(v),
        Err(_) => Err(match deadline {
            // If the overall deadline has now passed, attribute it to Deadline;
            // otherwise the (shorter) idle bound fired.
            Some(d) if d <= std::time::Instant::now() => RecvElapsed::Deadline,
            _ => RecvElapsed::Idle,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- build_stream_* helpers ---

    #[test]
    fn test_build_stream_open() {
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"data"), None, false);
        assert_eq!(req.uid, "stream-open-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert_eq!(s.stream_id, "s1");
                match s.action {
                    Some(pb::stream_request::Action::Open(o)) => {
                        assert_eq!(o.data, &b"data"[..]);
                        // decoupled=false → field absent (wire backward-compat).
                        assert_eq!(o.decoupled, None);
                    }
                    _ => panic!("expected open action"),
                }
            }
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_open_decoupled() {
        // P9-1: decoupled=true sets the additive flag the worker reads to
        // keep the channel open after predict_decoupled returns.
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"data"), None, true);
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => match s.action {
                Some(pb::stream_request::Action::Open(o)) => {
                    assert_eq!(o.decoupled, Some(true));
                }
                _ => panic!("expected open action"),
            },
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_chunk() {
        let req = build_stream_chunk("s1".to_string(), bytes::Bytes::from_static(b"chunk"));
        assert_eq!(req.uid, "stream-chunk-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => match s.action {
                Some(pb::stream_request::Action::Chunk(c)) => {
                    assert_eq!(c.data, &b"chunk"[..]);
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
            payload: Default::default(),
            ..Default::default()
        };
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"d"), Some(meta), false);
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

    // --- recv_chunk (P-DEADLINE two-stage) ---

    fn rx_pair() -> (
        tokio::sync::mpsc::Sender<pb::StreamResponse>,
        tokio::sync::mpsc::Receiver<pb::StreamResponse>,
    ) {
        tokio::sync::mpsc::channel(4)
    }
    fn chunk() -> pb::StreamResponse {
        pb::StreamResponse {
            stream_id: "s".to_string(),
            payload: Some(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                data: bytes::Bytes::from_static(b"x"),
                is_final: false,
            })),
        }
    }

    #[tokio::test]
    async fn recv_chunk_no_bounds_is_plain_recv() {
        let (tx, mut rx) = rx_pair();
        tx.send(chunk()).await.unwrap();
        drop(tx); // close → next recv is None
        let first = recv_chunk(&mut rx, None, None).await.unwrap();
        assert!(first.is_some());
        let second = recv_chunk(&mut rx, None, None).await.unwrap();
        assert!(second.is_none(), "channel closed → None");
    }

    #[tokio::test]
    async fn recv_chunk_expired_deadline_is_immediate() {
        let (_tx, mut rx) = rx_pair();
        let past = std::time::Instant::now() - std::time::Duration::from_millis(1);
        let elapsed = recv_chunk(&mut rx, Some(past), None).await.unwrap_err();
        assert_eq!(elapsed, RecvElapsed::Deadline);
    }

    #[tokio::test]
    async fn recv_chunk_idle_fires_on_stall() {
        let (_tx, mut rx) = rx_pair();
        // No sender, tiny idle → must trip Idle promptly.
        let elapsed = recv_chunk(
            &mut rx,
            None,
            Some(std::time::Duration::from_millis(20)),
        )
        .await
        .unwrap_err();
        assert_eq!(elapsed, RecvElapsed::Idle);
    }

    #[tokio::test]
    async fn recv_chunk_chunk_arrives_in_time() {
        let (tx, mut rx) = rx_pair();
        tx.send(chunk()).await.unwrap();
        let got = recv_chunk(
            &mut rx,
            Some(std::time::Instant::now() + std::time::Duration::from_secs(1)),
            Some(std::time::Duration::from_secs(1)),
        )
        .await
        .unwrap();
        assert!(got.is_some());
    }

    #[tokio::test]
    async fn recv_chunk_deadline_fires_when_shorter_than_idle() {
        let (_tx, mut rx) = rx_pair();
        // overall deadline is the tighter bound and fires first.
        let elapsed = recv_chunk(
            &mut rx,
            Some(std::time::Instant::now() + std::time::Duration::from_millis(20)),
            Some(std::time::Duration::from_secs(60)),
        )
        .await
        .unwrap_err();
        assert_eq!(elapsed, RecvElapsed::Deadline);
    }
}
