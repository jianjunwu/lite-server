use crate::proto::liteserver as pb;

/// Build a protobuf StreamRequest::Open.
pub fn build_stream_open(stream_id: String, data: bytes::Bytes, meta: Option<pb::RequestMeta>) -> pb::Request {
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- build_stream_* helpers ---

    #[test]
    fn test_build_stream_open() {
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"data"), None);
        assert_eq!(req.uid, "stream-open-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert_eq!(s.stream_id, "s1");
                match s.action {
                    Some(pb::stream_request::Action::Open(o)) => {
                        assert_eq!(o.data, &b"data"[..]);
                    }
                    _ => panic!("expected open action"),
                }
            }
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
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"d"), Some(meta));
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
