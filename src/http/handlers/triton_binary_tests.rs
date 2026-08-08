//! Triton Binary Tensor Data Extension extractor 测试(阶段 1,批次 1)。
//!
//! 门禁:kserve wire 夹具移植(`test_infer_type.py:262` 的 546B JSON 头 +
//! 14B 二进制尾,Rust 测试常量)+ 全部 G19 wire 健壮性规则。

use super::*;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

struct TestState {
    max_body: usize,
}

impl HasBodyLimit for TestState {
    fn max_body_bytes(&self) -> usize {
        self.max_body
    }
}

impl HasBodyLimit for Arc<TestState> {
    fn max_body_bytes(&self) -> usize {
        self.max_body
    }
}

/// kserve-master `test_infer_type.py:262` wire 夹具移植:
/// 546B JSON 头(input1 走 JSON data、input2/input3 二进制)+
/// `\x04\x00\x00\x00test`(BYTES 4B 长度前缀 + "test")+ 6B FP16 LE。
/// Σ binary_data_size = 8 + 6 = 14 == tail 长度。
const KSERVE_WIRE_HEAD: &str = r#"{"id":"4be4e82f-5500-420a-a5c5-ac86841e271b","model_name":"test_model","inputs":[{"name":"input1","shape":[3],"datatype":"INT32","parameters":{"test-str":"dummy"},"data":[1,2,3]},{"name":"input2","shape":[1],"datatype":"BYTES","parameters":{"test-int":2,"binary_data_size":8}},{"name":"input3","shape":[3],"datatype":"FP16","parameters":{"binary_data_size":6}}],"outputs":[{"name":"output-0","parameters":{"test-str":"dummy","test-bool":true,"test-int":100}},{"name":"output-1","parameters":{"test-str":"dummy","test-bool":true,"test-int":100}}]}"#;
const KSERVE_WIRE_TAIL: &[u8] = b"\x04\x00\x00\x00test\xcd<f@fB";

fn test_app(max_body: usize) -> axum::Router {
    let state = Arc::new(TestState { max_body });
    axum::Router::new()
        .route("/echo-kind", axum::routing::post(
            |ApiBody(body): ApiBody| async move { body.kind().to_string() },
        ))
        .route("/echo-bytes", axum::routing::post(
            |ApiBody(body): ApiBody| async move { axum::body::Body::from(body.bytes()) },
        ))
        .route("/echo-head-len", axum::routing::post(
            |ApiBody(body): ApiBody| async move {
                body.json_head_len().map_or_else(|| "none".to_string(), |n| n.to_string())
            },
        ))
        .route("/echo-tail-len", axum::routing::post(
            |ApiBody(body): ApiBody| async move {
                match body {
                    RequestBody::TritonBinary { body, json_head_len } => {
                        (body.len() - json_head_len).to_string()
                    }
                    _ => "not-triton".to_string(),
                }
            },
        ))
        .layer(axum::extract::DefaultBodyLimit::max(max_body))
        .with_state(state)
}

/// POST 并返回响应。body = 完整 wire(JSON 头 + 二进制尾)。
async fn post(app: &axum::Router, body: Vec<u8>, header_len: Option<usize>) -> axum::response::Response {
    let mut builder = Request::builder()
        .uri("/echo-kind")
        .method("POST")
        .header("content-type", "application/octet-stream");
    if let Some(n) = header_len {
        builder = builder.header("inference-header-content-length", n.to_string());
    }
    app.clone().oneshot(builder.body(Body::from(body)).unwrap()).await.unwrap()
}

async fn read_body(response: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap().to_vec()
}

/// 走全部四个 echo 路由,断言 kind/head-len/tail-len/bytes 回显(head/tail
/// 逐字节不变)。
async fn assert_triton_split(
    app: &axum::Router,
    body: Vec<u8>,
    head_len: usize,
) {
    let head = body[..head_len].to_vec();
    let tail = body[head_len..].to_vec();
    // kind
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", head_len.to_string())
            .body(Body::from(body.clone())).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(read_body(resp).await, b"triton_binary");

    // head-len
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-head-len").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", head_len.to_string())
            .body(Body::from(body.clone())).unwrap(),
    ).await.unwrap();
    assert_eq!(read_body(resp).await, head_len.to_string().as_bytes());

    // tail-len
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-tail-len").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", head_len.to_string())
            .body(Body::from(body.clone())).unwrap(),
    ).await.unwrap();
    assert_eq!(read_body(resp).await, tail.len().to_string().as_bytes());

    // bytes 全量回显(逐字节不变)
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-bytes").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", head_len.to_string())
            .body(Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let echoed = read_body(resp).await;
    assert_eq!(&echoed[..head_len], &head, "JSON 头逐字节不变");
    assert_eq!(&echoed[head_len..], &tail, "二进制尾逐字节不变");
}

// ===== 门禁:kserve wire 夹具移植(mixed inputs 一起覆盖) =====

/// 夹具本身:546B JSON 头(input1 JSON data + input2/3 二进制)+ 14B tail
/// → TritonBinary,Σ 8+6=14 校验通过,切分正确。
#[tokio::test]
async fn test_triton_binary_kserve_wire_fixture() {
    let head = KSERVE_WIRE_HEAD;
    assert_eq!(head.len(), 546, "fixture JSON head must be 546 bytes (test_infer_type.py:262)");
    assert_eq!(KSERVE_WIRE_TAIL.len(), 14);
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(KSERVE_WIRE_TAIL);

    let app = test_app(64 * 1024 * 1024);
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "546")
            .body(Body::from(body.clone())).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(read_body(resp).await, b"triton_binary");

    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-head-len").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "546")
            .body(Body::from(body.clone())).unwrap(),
    ).await.unwrap();
    assert_eq!(read_body(resp).await, b"546");

    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-bytes").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "546")
            .body(Body::from(body)).unwrap(),
    ).await.unwrap();
    let echoed = read_body(resp).await;
    assert_eq!(&echoed[..546], head.as_bytes(), "JSON 头逐字节不变");
    assert_eq!(&echoed[546..], KSERVE_WIRE_TAIL, "二进制尾逐字节不变");
}

// ===== G19 wire 健壮性 =====

#[tokio::test]
async fn test_triton_binary_single_input() {
    let head = r#"{"inputs":[{"name":"x","shape":[2],"datatype":"FP32","parameters":{"binary_data_size":8}}]}"#;
    let tail = [0u8, 1, 2, 3, 4, 5, 6, 7];
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&tail);
    let app = test_app(64 * 1024 * 1024);
    assert_triton_split(&app, body, head.len()).await;
}

#[tokio::test]
async fn test_triton_binary_multi_input() {
    let head = r#"{"inputs":[{"name":"a","shape":[2],"datatype":"FP32","parameters":{"binary_data_size":4}},{"name":"b","shape":[3],"datatype":"FP32","parameters":{"binary_data_size":6}}]}"#;
    let tail = [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&tail);
    let app = test_app(64 * 1024 * 1024);
    assert_triton_split(&app, body, head.len()).await;
}

#[tokio::test]
async fn test_triton_binary_size_mismatch() {
    // Σ = 12 ≠ tail 10 → 400
    let head = r#"{"inputs":[{"name":"a","shape":[2],"datatype":"FP32","parameters":{"binary_data_size":6}},{"name":"b","shape":[3],"datatype":"FP32","parameters":{"binary_data_size":6}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 10]);
    let app = test_app(64 * 1024 * 1024);
    let resp = post(&app, body, Some(head.len())).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_triton_binary_malformed_header() {
    let app = test_app(64 * 1024 * 1024);
    // 非数字
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "abc")
            .body(Body::from(r#"{"inputs":[]}"#)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    // 超 body len
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "1000")
            .body(Body::from(r#"{"inputs":[]}"#)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_triton_binary_zero_header() {
    // C3:N==0 → 落回既有 Content-Type 分流(octet-stream → Raw),byte-identical
    let app = test_app(64 * 1024 * 1024);
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "0")
            .body(Body::from(vec![1u8, 2, 3])).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(read_body(resp).await, b"raw");

    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-bytes").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "0")
            .body(Body::from(vec![1u8, 2, 3])).unwrap(),
    ).await.unwrap();
    assert_eq!(read_body(resp).await, vec![1u8, 2, 3]);
}

#[tokio::test]
async fn test_triton_binary_invalid_json_head() {
    let app = test_app(64 * 1024 * 1024);
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", "5")
            .body(Body::from(b"not-a-json!!!tail".to_vec())).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_triton_binary_no_header_regression() {
    // 无 header → 既有 json/raw 分流 byte-identical(回归)
    let app = test_app(64 * 1024 * 1024);
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"x":1}"#)).unwrap(),
    ).await.unwrap();
    assert_eq!(read_body(resp).await, b"json");
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .body(Body::from(vec![9u8])).unwrap(),
    ).await.unwrap();
    assert_eq!(read_body(resp).await, b"raw");
}

#[tokio::test]
async fn test_triton_binary_missing_size_param() {
    // input 无 binary_data_size 且无 data → 400
    let head = r#"{"inputs":[{"name":"x","shape":[2],"datatype":"FP32"}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 8]);
    let app = test_app(64 * 1024 * 1024);
    let resp = post(&app, body, Some(head.len())).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_triton_binary_mixed_inputs() {
    // 部分 input 走 JSON data、部分二进制 → Σ 只加二进制者(夹具覆盖,单测简版)
    let head = r#"{"inputs":[{"name":"json_in","shape":[2],"datatype":"INT32","data":[1,2]},{"name":"bin_in","shape":[1],"datatype":"BYTES","parameters":{"binary_data_size":6}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(b"\x04\x00\x00\x00hi");
    let app = test_app(64 * 1024 * 1024);
    assert_triton_split(&app, body, head.len()).await;
}

#[tokio::test]
async fn test_triton_binary_size_overflow() {
    // 巨大 binary_data_size(2^63)→ checked arithmetic 400,不 wrap 不 panic
    let head = r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","parameters":{"binary_data_size":9223372036854775808}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 4]);
    let app = test_app(64 * 1024 * 1024);
    let resp = post(&app, body, Some(head.len())).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_triton_binary_duplicate_input_name() {
    let head = r#"{"inputs":[{"name":"x","shape":[1],"datatype":"FP32","parameters":{"binary_data_size":4}},{"name":"x","shape":[1],"datatype":"FP32","parameters":{"binary_data_size":4}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 8]);
    let app = test_app(64 * 1024 * 1024);
    let resp = post(&app, body, Some(head.len())).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_triton_binary_negative_size() {
    // 负数
    let head = r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","parameters":{"binary_data_size":-1}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 4]);
    let app = test_app(64 * 1024 * 1024);
    let resp = post(&app, body, Some(head.len())).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 浮点
    let head = r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","parameters":{"binary_data_size":2.5}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 4]);
    let resp = post(&app, body, Some(head.len())).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_triton_binary_empty_tail() {
    // N == body.len()、全 size 0 → 合法
    let head = r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","parameters":{"binary_data_size":0}}]}"#;
    let app = test_app(64 * 1024 * 1024);
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/octet-stream")
            .header("inference-header-content-length", head.len().to_string())
            .body(Body::from(head.as_bytes().to_vec())).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(read_body(resp).await, b"triton_binary");
}

#[tokio::test]
async fn test_triton_binary_header_beats_content_type() {
    // header + application/json → 仍走 TritonBinary(容错,Triton 客户端恒发 octet-stream)
    let head = r#"{"inputs":[{"name":"x","shape":[2],"datatype":"FP32","parameters":{"binary_data_size":8}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 8]);
    let app = test_app(64 * 1024 * 1024);
    let resp = app.clone().oneshot(
        Request::builder().uri("/echo-kind").method("POST")
            .header("content-type", "application/json")
            .header("inference-header-content-length", head.len().to_string())
            .body(Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(read_body(resp).await, b"triton_binary");
}

#[tokio::test]
async fn test_triton_binary_body_limit() {
    // 超 max_request_body_bytes → 413(既有层)
    let head = r#"{"inputs":[{"name":"x","shape":[2],"datatype":"FP32","parameters":{"binary_data_size":200}}]}"#;
    let mut body = head.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 200]);
    let app = test_app(100);
    let resp = post(&app, body, Some(head.len())).await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
