//! LPM (Lite Protocol Message) frame codec for HTTP/2 bidirectional streaming.
//!
//! Frame format:
//! ```text
//! +--------+------------------+-----------------+
//! | 1B flag| 4B length (BE)   | prost BidiChunk |
//! |  = 0   |  = N             | N bytes         |
//! +--------+------------------+-----------------+
//! ```
//!
//! - flag: reserved for future compression; non-zero → `LpmError::BadFlag`.
//! - length: big-endian u32 payload length in bytes.
//! - payload: prost-encoded `BidiChunk` message.
//! - `MAX_LPM_FRAME` = 16 MiB — oversized frames are rejected before allocation.

use crate::proto::liteserver as pb;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

/// Hard upper bound on a single LPM frame (header + payload). Prevents OOM.
pub const MAX_LPM_FRAME: u32 = 16 * 1024 * 1024; // 16 MiB

/// Size of the LPM frame header: 1 byte flag + 4 bytes length (BE).
const HEADER_SIZE: usize = 5;

#[derive(Debug, PartialEq, Eq)]
pub enum LpmError {
    /// Flag byte is non-zero (compression reserved, not yet supported).
    BadFlag,
    /// Declared payload length exceeds `MAX_LPM_FRAME`.
    TooLarge,
    /// Header or payload data is truncated (need more bytes).
    Truncated,
    /// Payload is zero-length (invalid — every frame must carry a message).
    EmptyPayload,
    /// Payload failed to decode as a valid `BidiChunk`.
    Decode(prost::DecodeError),
}

impl std::fmt::Display for LpmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LpmError::BadFlag => write!(f, "LPM frame flag byte is non-zero"),
            LpmError::TooLarge => write!(f, "LPM frame exceeds max size"),
            LpmError::Truncated => write!(f, "LPM frame data truncated"),
            LpmError::EmptyPayload => write!(f, "LPM frame payload is empty"),
            LpmError::Decode(e) => write!(f, "LPM frame decode error: {e}"),
        }
    }
}

/// Encode a `BidiChunk` into an LPM frame (`Bytes`).
///
/// Always succeeds — the only fallible step is prost encoding, which is
/// infallible for valid protobuf messages.
pub fn encode_frame(chunk: &pb::BidiChunk) -> Bytes {
    let payload = chunk.encode_to_vec();
    let len = payload.len() as u32;
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(0); // flag = 0
    buf.put_u32(len);
    buf.put_slice(&payload);
    buf.freeze()
}

/// Try to decode ONE LPM frame from `buf`.
///
/// Returns:
/// - `Ok(Some(chunk))` — a complete frame was decoded and consumed from `buf`.
/// - `Ok(None)` — not enough data yet; `buf` is unmodified (caller should
///   wait for more bytes).
/// - `Err(LpmError)` — the frame is malformed; data was consumed.
///
/// On error the caller should close the stream — there is no recovery
/// mechanism for frame-level errors.
pub fn try_decode_frame(buf: &mut BytesMut) -> Result<Option<pb::BidiChunk>, LpmError> {
    // Need at least the 5-byte header.
    if buf.len() < HEADER_SIZE {
        return Ok(None);
    }

    let flag = buf[0];
    if flag != 0 {
        buf.advance(buf.len()); // consume all — unrecoverable
        return Err(LpmError::BadFlag);
    }

    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len > MAX_LPM_FRAME {
        buf.advance(buf.len());
        return Err(LpmError::TooLarge);
    }
    if len == 0 {
        buf.advance(HEADER_SIZE);
        return Err(LpmError::EmptyPayload);
    }

    let total = HEADER_SIZE + len as usize;
    if buf.len() < total {
        return Ok(None); // need more bytes
    }

    // Advance past the header and extract the payload.
    buf.advance(HEADER_SIZE);
    let payload = buf.split_to(len as usize);

    let chunk = pb::BidiChunk::decode(payload).map_err(LpmError::Decode)?;
    Ok(Some(chunk))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::{BidiChunk, BidiClose, BidiData, BidiOpen};

    fn roundtrip(chunk: &BidiChunk) -> BidiChunk {
        let frame = encode_frame(chunk);
        let mut buf = BytesMut::from(frame.as_ref());
        let decoded = try_decode_frame(&mut buf)
            .expect("roundtrip decode should succeed")
            .expect("roundtrip should yield a frame");
        assert!(buf.is_empty(), "roundtrip should consume all bytes");
        decoded
    }

    #[test]
    fn lpm_roundtrip_data_chunk() {
        let chunk = BidiChunk {
            stream_id: "s1".into(),
            payload: Some(pb::bidi_chunk::Payload::Data(BidiData {
                data: Bytes::from_static(b"hello"),
            })),
        };
        let decoded = roundtrip(&chunk);
        assert_eq!(decoded.stream_id, "s1");
        match decoded.payload {
            Some(pb::bidi_chunk::Payload::Data(d)) => {
                assert_eq!(d.data, Bytes::from_static(b"hello"));
            }
            _ => panic!("expected Data payload"),
        }
    }

    #[test]
    fn lpm_roundtrip_open() {
        let chunk = BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Open(BidiOpen {
                model_name: "m".into(),
                version: "1".into(),
                initial_data: Bytes::from_static(b"{}"),
                ..Default::default()
            })),
        };
        let decoded = roundtrip(&chunk);
        match decoded.payload {
            Some(pb::bidi_chunk::Payload::Open(o)) => {
                assert_eq!(o.model_name, "m");
                assert_eq!(o.version, "1");
            }
            _ => panic!("expected Open payload"),
        }
    }

    #[test]
    fn lpm_roundtrip_close() {
        let chunk = BidiChunk {
            stream_id: "c1".into(),
            payload: Some(pb::bidi_chunk::Payload::Close(BidiClose {})),
        };
        let decoded = roundtrip(&chunk);
        assert_eq!(decoded.stream_id, "c1");
        assert!(matches!(decoded.payload, Some(pb::bidi_chunk::Payload::Close(_))));
    }

    #[test]
    fn lpm_truncated_header_returns_none() {
        let mut buf = BytesMut::from(&b"\x00\x00\x00"[..]);
        let result = try_decode_frame(&mut buf).expect("should not error");
        assert!(result.is_none(), "truncated header → None (need more data)");
    }

    #[test]
    fn lpm_truncated_body_returns_none() {
        // Header: flag=0, len=10, but only 3 bytes of payload.
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u8(0);
        buf.put_u32(10);
        buf.put_slice(b"abc");
        let result = try_decode_frame(&mut buf).expect("should not error on truncation");
        assert!(result.is_none(), "truncated body → None (need more data)");
    }

    #[test]
    fn lpm_oversized_length_is_rejected() {
        let mut buf = BytesMut::with_capacity(5);
        buf.put_u8(0);
        buf.put_u32(MAX_LPM_FRAME + 1);
        let err = try_decode_frame(&mut buf).unwrap_err();
        assert_eq!(err, LpmError::TooLarge);
    }

    #[test]
    fn lpm_bad_flag_is_rejected() {
        let mut buf = BytesMut::from(&b"\x01\x00\x00\x00\x05hello"[..]);
        let err = try_decode_frame(&mut buf).unwrap_err();
        assert_eq!(err, LpmError::BadFlag);
    }

    #[test]
    fn lpm_empty_payload_is_rejected() {
        let mut buf = BytesMut::with_capacity(5);
        buf.put_u8(0);
        buf.put_u32(0);
        let err = try_decode_frame(&mut buf).unwrap_err();
        assert_eq!(err, LpmError::EmptyPayload);
    }

    #[test]
    fn lpm_two_frames_back_to_back() {
        let c1 = BidiChunk {
            stream_id: "a".into(),
            payload: Some(pb::bidi_chunk::Payload::Close(BidiClose {})),
        };
        let c2 = BidiChunk {
            stream_id: "b".into(),
            payload: Some(pb::bidi_chunk::Payload::Close(BidiClose {})),
        };
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&encode_frame(&c1));
        combined.extend_from_slice(&encode_frame(&c2));

        let d1 = try_decode_frame(&mut combined)
            .expect("decode first")
            .expect("first should be Some");
        assert_eq!(d1.stream_id, "a");
        let d2 = try_decode_frame(&mut combined)
            .expect("decode second")
            .expect("second should be Some");
        assert_eq!(d2.stream_id, "b");
        assert!(combined.is_empty(), "both frames consumed");
    }

    #[test]
    fn lpm_garbage_payload_decode_error() {
        let mut buf = BytesMut::with_capacity(5 + 3);
        buf.put_u8(0);
        buf.put_u32(3);
        buf.put_slice(b"xyz"); // not valid protobuf
        let err = try_decode_frame(&mut buf).unwrap_err();
        assert!(matches!(err, LpmError::Decode(_)));
    }
}
