//! Opt-in gzip request-body decompression (`server.request_decompression`).
//!
//! Pure stream transform: the request body is replaced with a lazy
//! gzip-decoding stream before extraction, so `DefaultBodyLimit` caps
//! DECOMPRESSED bytes (zip-bomb guard) and decode errors surface as body
//! errors → `Bytes` rejection → the existing `map_body_rejection` envelope.
//! `/bidi` routes are exempt (frame timeliness); their own D6 check keeps
//! rendering the 415 envelope.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Replaces the request body with a lazy gzip-decoding stream.
/// No spawn, no buffering: pure stream transform, freed on drop.
fn gzip_decoded_body(body: axum::body::Body) -> axum::body::Body {
    use futures::StreamExt;
    use tokio_util::io::{ReaderStream, StreamReader};
    let byte_stream = body
        .into_data_stream() // trailers dropped (unused in this codebase)
        .map(|frame| frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let decoder = async_compression::tokio::bufread::GzipDecoder::new(StreamReader::new(byte_stream));
    axum::body::Body::from_stream(ReaderStream::new(decoder))
}

pub(crate) async fn request_decompression_middleware(req: Request, next: Next) -> Response {
    use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
    let Some(raw) = req.headers().get(CONTENT_ENCODING) else {
        return next.run(req).await; // no encoding: zero-cost pass-through
    };
    // `/bidi` exempt (mirrors request_body_timeout_middleware, http/mod.rs):
    // frame streams must not be buffered by a decoder; the bidi handler's own
    // D6 check renders the 415 envelope.
    if req.uri().path().ends_with("/bidi") {
        return next.run(req).await;
    }
    let encoding = raw.to_str().map(str::trim).unwrap_or("").to_ascii_lowercase();
    match encoding.as_str() {
        "identity" | "" => {
            // Strip and pass through (identity = no encoding per RFC 9110).
            let (mut parts, body) = req.into_parts();
            parts.headers.remove(CONTENT_ENCODING);
            next.run(Request::from_parts(parts, body)).await
        }
        "gzip" => {
            let (mut parts, body) = req.into_parts();
            parts.headers.remove(CONTENT_ENCODING);
            parts.headers.remove(CONTENT_LENGTH); // stale after decode
            // An exactly-empty body carries no gzip frame to decode; some
            // clients send Content-Encoding unconditionally. Pass it through
            // instead of surfacing the decoder's unexpected-EOF as a 400.
            use axum::body::HttpBody as _;
            let body = if body.size_hint().exact() == Some(0) {
                body
            } else {
                gzip_decoded_body(body)
            };
            next.run(Request::from_parts(parts, body)).await
        }
        // Unified protocol-aware 415 envelope via the existing render chain.
        _ => {
            tracing::debug!(encoding = %encoding, "rejecting unsupported content encoding");
            let protocol = crate::http::handlers::rejection_protocol(&req);
            crate::error::ProtocolError {
                error: crate::error::AppError::UnsupportedMediaType(format!(
                    "unsupported content encoding '{encoding}'; only gzip is supported"
                )),
                protocol,
            }
            .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, Bytes};
    use axum::extract::rejection::BytesRejection;
    use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    /// gzip-encode test payloads with the same codec stack the middleware
    /// decodes with (async-compression), avoiding a new dev-dependency.
    async fn gzip_encode(data: &[u8]) -> Vec<u8> {
        use tokio::io::AsyncWriteExt;
        let mut enc = async_compression::tokio::write::GzipEncoder::new(Vec::new());
        enc.write_all(data).await.unwrap();
        enc.shutdown().await.unwrap();
        enc.into_inner()
    }

    async fn echo_body(body: Bytes) -> Bytes {
        body
    }

    /// Reports whether CONTENT_ENCODING survived the middleware + echoes the
    /// body ("<seen>:<body>").
    async fn encoding_probe(headers: HeaderMap, body: Bytes) -> Response {
        let seen = headers.contains_key(CONTENT_ENCODING);
        format!("{seen}:{}", String::from_utf8_lossy(&body)).into_response()
    }

    /// Mirrors the inference handler's extraction + rejection mapping path:
    /// body errors render through `map_body_rejection` into the Legacy
    /// envelope.
    async fn bytes_or_envelope(body: Result<Bytes, BytesRejection>) -> Response {
        match body {
            Ok(b) => b.into_response(),
            Err(rej) => crate::error::ProtocolError {
                error: crate::error::map_body_rejection(rej, 1024, None),
                protocol: crate::protocol::ApiProtocol::Legacy,
            }
            .into_response(),
        }
    }

    /// Mimics the bidi handler's D6 gate: any Content-Encoding → 415.
    async fn bidi_gate(req: Request) -> Response {
        if req.headers().contains_key(CONTENT_ENCODING) {
            return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
        }
        StatusCode::OK.into_response()
    }

    /// KServe Triton binary split: returns only the raw tail past IHCL.
    async fn kserve_binary_tail(headers: HeaderMap, body: Bytes) -> Response {
        let n: usize = headers
            .get(crate::http::handlers::INFERENCE_HEADER_CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        body.slice(n..).into_response()
    }

    fn app_with(route: &'static str, handler: axum::routing::MethodRouter) -> Router {
        Router::new()
            .route(route, handler)
            .layer(axum::middleware::from_fn(request_decompression_middleware))
    }

    async fn response_bytes(resp: Response) -> Vec<u8> {
        use http_body_util::BodyExt;
        resp.into_body().collect().await.unwrap().to_bytes().to_vec()
    }

    fn post_req(uri: &str, body: Vec<u8>) -> Request {
        Request::builder().method("POST").uri(uri).body(Body::from(body)).unwrap()
    }

    fn gzip_req(uri: &str, body: Vec<u8>) -> Request {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(CONTENT_ENCODING, "gzip")
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn should_pass_through_when_no_content_encoding() {
        let app = app_with("/echo", post(echo_body));
        let resp = app.oneshot(post_req("/echo", b"hello".to_vec())).await.unwrap();
        assert_eq!(response_bytes(resp).await, b"hello");
    }

    #[tokio::test]
    async fn should_decode_gzip_body_when_enabled() {
        let app = app_with("/echo", post(echo_body));
        let payload = gzip_encode(br#"{"a":1}"#).await;
        let resp = app.oneshot(gzip_req("/echo", payload)).await.unwrap();
        assert_eq!(response_bytes(resp).await, br#"{"a":1}"#);
    }

    #[tokio::test]
    async fn should_decode_gzip_without_content_length() {
        // No Content-Length header (chunked-style delivery) must still decode.
        let app = app_with("/echo", post(echo_body));
        let payload = gzip_encode(b"chunked-gzip").await;
        let req = gzip_req("/echo", payload);
        assert!(req.headers().get(CONTENT_LENGTH).is_none());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(response_bytes(resp).await, b"chunked-gzip");
    }

    #[tokio::test]
    async fn should_pass_identity_through_when_enabled() {
        // identity = no encoding (RFC 9110): strip the header, body untouched.
        let app = app_with("/probe", post(encoding_probe));
        let req = Request::builder()
            .method("POST")
            .uri("/probe")
            .header(CONTENT_ENCODING, "identity")
            .body(Body::from(b"plain".to_vec()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(response_bytes(resp).await, b"false:plain");
    }

    #[tokio::test]
    async fn should_return_415_envelope_for_unsupported_encoding() {
        use crate::protocol::ApiProtocol;
        use crate::request_context::RequestContext;
        for (protocol, error_is_object) in [
            (ApiProtocol::Legacy, true),
            (ApiProtocol::Kserve, false),
            (ApiProtocol::OpenaiCompact, true),
        ] {
            let app = app_with("/echo", post(echo_body));
            let req = Request::builder()
                .method("POST")
                .uri("/echo")
                .header(CONTENT_ENCODING, "br")
                .body(Body::from(b"x".to_vec()))
                .unwrap();
            let (mut parts, body) = req.into_parts();
            let mut cx = RequestContext::from_http_parts(&parts, &[]);
            cx.api_protocol = Some(protocol);
            parts.extensions.insert(cx);
            let req = Request::from_parts(parts, body);

            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
            let body: serde_json::Value =
                serde_json::from_slice(&response_bytes(resp).await).unwrap();
            assert_eq!(
                body["error"].is_object(),
                error_is_object,
                "envelope shape mismatch for {protocol:?}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn should_return_400_envelope_for_corrupt_gzip() {
        let app = app_with("/u", post(bytes_or_envelope));
        let resp = app
            .oneshot(gzip_req("/u", b"not a gzip stream".to_vec()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn should_return_413_envelope_when_decompressed_exceeds_limit() {
        // 4 KiB of zeros compresses to tens of bytes but must trip the 1 KiB
        // cap AFTER decoding (zip-bomb guard).
        let app = Router::new()
            .route("/u", post(bytes_or_envelope))
            .layer(axum::extract::DefaultBodyLimit::max(1024))
            .layer(axum::middleware::from_fn(request_decompression_middleware));
        let bomb = gzip_encode(&vec![0u8; 4096]).await;
        assert!(bomb.len() < 1024);
        let resp = app.oneshot(gzip_req("/u", bomb)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn should_keep_415_on_bidi_even_when_enabled() {
        let app = app_with("/v2/models/m/bidi", post(bidi_gate));
        let payload = gzip_encode(b"frame").await;
        let resp = app
            .oneshot(gzip_req("/v2/models/m/bidi", payload))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn should_decode_kserve_triton_binary_over_gzip() {
        // IHCL is computed over the DECOMPRESSED bytes; decode is 1:1 so the
        // head/tail split is preserved.
        let head = br#"{"inputs":[{"name":"x","shape":[1],"datatype":"UINT8","parameters":{"binary_data_size":4}}]}"#;
        let tail = [0xDE, 0xAD, 0xBE, 0xEF];
        let mut raw = head.to_vec();
        raw.extend_from_slice(&tail);
        let payload = gzip_encode(&raw).await;

        let app = app_with("/v2/models/m/infer", post(kserve_binary_tail));
        let req = Request::builder()
            .method("POST")
            .uri("/v2/models/m/infer")
            .header(CONTENT_ENCODING, "gzip")
            .header(
                crate::http::handlers::INFERENCE_HEADER_CONTENT_LENGTH,
                head.len().to_string(),
            )
            .body(Body::from(payload))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(response_bytes(resp).await, tail);
    }

    #[tokio::test]
    async fn should_pass_empty_body_with_gzip_header() {
        let app = app_with("/echo", post(echo_body));
        let resp = app.oneshot(gzip_req("/echo", Vec::new())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
