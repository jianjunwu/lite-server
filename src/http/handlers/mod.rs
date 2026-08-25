use crate::error::{AppError, ProtocolError};
use crate::http::state::AppState;
use crate::protocol::ApiProtocol;
use axum::{extract::Query, response::Json};
use axum::http::header::HeaderMap;
use bytes::Bytes;
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use axum::response::Response;
use std::sync::Arc;
use tracing::warn;

/// Map an HTTP status code to its Prometheus label family.
///
/// ``Status.code`` in the worker protocol describes *execution* state
/// (Ok = pipeline completed normally, Error = exception), while
/// ``SingleResponse.status_code`` carries the HTTP status.  This
/// function bridges the two so early 4xx / 5xx responses are recorded
/// under the correct Prometheus label rather than hardcoded "2xx".
fn status_family(status_code: i32) -> &'static str {
    match status_code / 100 {
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "2xx", // 0 (unset) counts as success
    }
}

/// JSON body extractor that converts axum's plain-text `JsonRejection`
/// into a standardized `AppError::InvalidRequestBody` response.
pub struct ApiJson<T>(pub T);

#[axum::async_trait]
impl<S, T> axum::extract::FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    Json<T>: axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
{
    type Rejection = ProtocolError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        // C9 (P2.1):拒绝按 T1 预筛协议渲染(extractor 期无 T2,header 强信号)。
        let protocol = rejection_protocol(&req);
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(ApiJson(value)),
            Err(rejection) => Err(ProtocolError {
                error: AppError::InvalidRequestBody(rejection.body_text()),
                protocol,
            }),
        }
    }
}

/// Query extractor that converts axum's plain-text `QueryRejection`
/// into a standardized `AppError::InvalidQueryParam` response.
pub struct ApiQuery<T>(pub T);

#[axum::async_trait]
impl<S, T> axum::extract::FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    Query<T>: axum::extract::FromRequestParts<S, Rejection = axum::extract::rejection::QueryRejection>,
{
    type Rejection = ProtocolError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let protocol = parts
            .extensions
            .get::<crate::request_context::RequestContext>()
            .and_then(|cx| cx.api_protocol)
            .unwrap_or(ApiProtocol::Legacy);
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(ApiQuery(value)),
            Err(rejection) => Err(ProtocolError {
                error: AppError::InvalidQueryParam(rejection.body_text()),
                protocol,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// RequestBody + ApiBody: Content-Type-aware body extraction with zero-copy
// JSON validation (D1-D3).
// ---------------------------------------------------------------------------

/// HTTP request body, dispatched on Content-Type.
/// All variants hold the **original request bytes** — no JSON round-trip,
/// no re-serialization anywhere on the wire path.
#[derive(Debug, Clone)]
pub enum RequestBody {
    /// JSON content-type (incl. `application/*+json`): syntax-validated
    /// zero-copy via `&RawValue`, forwarded byte-identical.
    Json(Bytes),
    /// Non-JSON content-type → raw bytes + normalized media type string.
    Raw(Bytes, String),
    /// Triton Binary Tensor Data Extension(KServe V2 dataplane,阶段 1):
    /// body = JSON 头 + 拼接二进制尾;`json_head_len` 为切分点。持有**完整
    /// 原始 body**(不拼接不拷贝),需要哪段就 `Bytes::slice`(O(1) 视图)。
    /// 仅 N>0 时构造;N=0 落回 Raw(v2 C3)。
    TritonBinary { body: Bytes, json_head_len: usize },
}

impl RequestBody {
    /// Metrics / trace label.
    pub fn kind(&self) -> &'static str {
        match self {
            RequestBody::Json(_) => "json",
            RequestBody::Raw(_, _) => "raw",
            RequestBody::TritonBinary { .. } => "triton_binary",
        }
    }

    /// Original body bytes (O(1) refcount clone).
    pub fn bytes(&self) -> Bytes {
        match self {
            RequestBody::Json(b) => b.clone(),
            RequestBody::Raw(b, _) => b.clone(),
            RequestBody::TritonBinary { body, .. } => body.clone(),
        }
    }

    /// Triton Binary 切分点(仅 TritonBinary 变体有)。
    pub fn json_head_len(&self) -> Option<usize> {
        match self {
            RequestBody::TritonBinary { json_head_len, .. } => Some(*json_head_len),
            _ => None,
        }
    }
}

/// Extractor that yields [`RequestBody`] by dispatching on Content-Type.
pub struct ApiBody(pub RequestBody);

/// Trait so [`ApiBody`] can read the configured body limit from router state
/// without coupling to a concrete state type.
pub(crate) trait HasBodyLimit {
    fn max_body_bytes(&self) -> usize;
}

#[axum::async_trait]
impl<S> axum::extract::FromRequest<S> for ApiBody
where
    S: Send + Sync + HasBodyLimit,
{
    type Rejection = ProtocolError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        // C9 (P2.1):拒绝按 T1 预筛协议渲染(extractor 期无 T2,header 强信号)。
        let protocol = rejection_protocol(&req);
        // Materialize header decisions before moving the request into parts.
        let (is_json, content_type, has_content_encoding, content_length, ihcl) = {
            let headers = req.headers();
            let ct_header = headers.get(axum::http::header::CONTENT_TYPE);
            let is_json = match ct_header {
                None => true, // missing → default to JSON (D2)
                Some(v) => is_json_content_type(v), // parse failure → raw (D2)
            };
            let content_type: Option<String> = ct_header
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let has_encoding = headers.contains_key(axum::http::header::CONTENT_ENCODING);
            // D4/D5 (P1): advertised body size for the 413 error body.
            let content_length = headers
                .get(axum::http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            // C9 (阶段 1): `Inference-Header-Content-Length` 存在即 T1 强信号,
            // 优先于 Content-Type 分流(Triton 客户端恒发 octet-stream)。
            let ihcl: Option<String> = headers
                .get(INFERENCE_HEADER_CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            (is_json, content_type, has_encoding, content_length, ihcl)
        };

        // D6: no decompression layer — any Content-Encoding → 415.
        if has_content_encoding {
            return Err(pe(protocol, AppError::UnsupportedMediaType(
                "Content-Encoding (compressed request body) is not supported; \
                 send the request body uncompressed"
                    .into(),
            )));
        }

        let (parts, body) = req.into_parts();
        let max_size = state.max_body_bytes();
        let bytes = axum::body::Bytes::from_request(
            axum::extract::Request::from_parts(parts, body),
            state,
        )
        .await
        .map_err(|rejection| {
            pe(protocol, crate::error::map_body_rejection(rejection, max_size, content_length))
        })?;

        // Triton Binary Tensor Data Extension(D2/C3):header 存在即优先于
        // Content-Type 分流。N>0 → TritonBinary(JSON 头 &RawValue 校验 +
        // Σ binary_data_size 结构校验);N==0 → 落回既有分流(byte-identical)。
        if let Some(raw) = ihcl {
            let n = raw.parse::<usize>().map_err(|_| {
                pe(protocol, AppError::InvalidRequestBody(format!(
                    "invalid inference-header-content-length: {raw}"
                )))
            })?;
            if n > 0 {
                if n > bytes.len() {
                    return Err(pe(protocol, AppError::InvalidRequestBody(format!(
                        "inference-header-content-length {n} exceeds body length {}",
                        bytes.len()
                    ))));
                }
                let total = triton_binary_tail_sum(&bytes[..n])
                    .map_err(|e| pe(protocol, e))?;
                let tail_len = bytes.len() - n;
                if total as usize != tail_len {
                    return Err(pe(protocol, AppError::InvalidRequestBody(format!(
                        "binary_data_size sum {total} does not match binary tail length {tail_len}"
                    ))));
                }
                return Ok(ApiBody(RequestBody::TritonBinary {
                    body: bytes,
                    json_head_len: n,
                }));
            }
            // N == 0 → 落回既有 Content-Type 分流(C3)
        }

        if is_json {
            // D3: zero-allocation syntax validation via `&RawValue` —
            // a full parse (not just first-byte sniffing), no DOM materialized.
            serde_json::from_slice::<&serde_json::value::RawValue>(&bytes)
                .map_err(|e| {
                    pe(protocol, AppError::InvalidRequestBody(format!("invalid JSON body: {e}")))
                })?;
            Ok(ApiBody(RequestBody::Json(bytes)))
        } else {
            let ct = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
            Ok(ApiBody(RequestBody::Raw(bytes, ct)))
        }
    }
}

/// C9 (P2.1):拒绝路径的协议 = T1 预筛值(middleware 填充;无 middleware
/// 的直挂单测 → Legacy,byte-identical)。
pub(crate) fn rejection_protocol(req: &axum::extract::Request) -> ApiProtocol {
    req.extensions()
        .get::<crate::request_context::RequestContext>()
        .and_then(|cx| cx.api_protocol)
        .unwrap_or(ApiProtocol::Legacy)
}

/// 带协议的错误边界构造(C9)。
fn pe(protocol: ApiProtocol, error: AppError) -> ProtocolError {
    ProtocolError { error, protocol }
}

/// Triton Binary Tensor Data Extension 的切分 header(KServe V2 dataplane,
/// constants.py:108 实证,键名小写)。
pub(crate) const INFERENCE_HEADER_CONTENT_LENGTH: &str = "inference-header-content-length";

/// Triton Binary JSON 头(0..N)的部分反序列化:仅取 `inputs[].name /
/// data / parameters.binary_data_size`,借用 head 引用,近零分配(D3/C11)。
#[derive(serde::Deserialize)]
struct TritonBinaryHead<'a> {
    #[serde(borrow, default)]
    inputs: Vec<TritonBinaryInput<'a>>,
}

#[derive(serde::Deserialize)]
struct TritonBinaryInput<'a> {
    #[serde(borrow, default)]
    name: Option<&'a str>,
    /// 是否存在 JSON data(混合输入合法:未声明 size 者走 JSON data)。
    #[serde(default)]
    data: Option<&'a serde_json::value::RawValue>,
    #[serde(default)]
    parameters: Option<TritonBinaryParams>,
}

#[derive(serde::Deserialize)]
struct TritonBinaryParams {
    #[serde(rename = "binary_data_size", default)]
    binary_data_size: Option<u64>,
}

/// Σ `inputs[].parameters.binary_data_size` 的结构校验(D2/G19):
/// - 混合输入合法:只加声明了 size 的 input;tail 切分顺序 = 声明顺序
/// - 负数/浮点/非数字 size → serde u64 解析失败 → 400(不 wrap 不 panic)
/// - `checked_add` 溢出 → 400
/// - 重名 input → 400(Python 侧按 name 建 dict,静默覆盖是隐患)
/// - 声明 size 的 input 缺 name → 400;每个 input 必须有 data 或 size → 400
fn triton_binary_tail_sum(json_head: &[u8]) -> Result<u64, AppError> {
    let head: TritonBinaryHead = serde_json::from_slice(json_head)
        .map_err(|e| AppError::InvalidRequestBody(format!("invalid JSON head: {e}")))?;
    let mut seen = std::collections::HashSet::new();
    let mut total: u64 = 0;
    for input in &head.inputs {
        let size = input.parameters.as_ref().and_then(|p| p.binary_data_size);
        if input.data.is_none() && size.is_none() {
            return Err(AppError::InvalidRequestBody(
                "input must declare data or binary_data_size".to_string(),
            ));
        }
        let Some(size) = size else { continue };
        let Some(name) = input.name else {
            return Err(AppError::InvalidRequestBody(
                "input with binary_data_size must declare name".to_string(),
            ));
        };
        if !seen.insert(name) {
            return Err(AppError::InvalidRequestBody(format!(
                "duplicate input name: {name}"
            )));
        }
        total = total.checked_add(size).ok_or_else(|| {
            AppError::InvalidRequestBody("binary_data_size sum overflow".to_string())
        })?;
    }
    Ok(total)
}

/// axum `Json` extractor's own JSON content-type predicate (D1):
/// `application/json` and `application/*+json`, case-insensitive, ignores
/// parameters. Text subtypes (`text/json`) and unknown suffixes are NOT
/// matched — safer than prefix matching, less risk of misclassification.
pub(super) fn is_json_content_type(value: &axum::http::HeaderValue) -> bool {
    let Ok(value) = value.to_str() else {
        return false; // non-ASCII garbage → raw (D2)
    };
    let Ok(mime) = value.parse::<mime::Mime>() else {
        return false; // parse failure → raw (D2)
    };
    mime.type_() == mime::APPLICATION
        && (mime.subtype() == mime::JSON
            || mime.suffix().is_some_and(|suffix| suffix == mime::JSON))
}

pub mod health;
pub use health::*;
pub mod admin;
pub use admin::*;
pub mod openai_compact;
pub use openai_compact::*;
pub mod inference;
pub use inference::*;
pub mod custom_routes;
pub use custom_routes::*;
pub mod stream;
pub use stream::*;
pub mod bidi;
pub use bidi::*;
pub mod files;
pub use files::*;
pub mod upload_sessions;
pub use upload_sessions::*;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod triton_binary_tests;


/// Pure decision for the peer-IP fallback layer: return the IP string to inject
/// as `x-real-ip`, or `None`. Injection is skipped when a proxy header is
/// already present (never overwrite a client/proxy-supplied value) or when
/// there is no peer (the unix-socket path carries no `ConnectInfo`).
pub(crate) fn peer_ip_fallback_target(
    headers: &HeaderMap,
    peer: Option<std::net::SocketAddr>,
) -> Option<String> {
    let has_proxy = headers.contains_key("x-forwarded-for")
        || headers.contains_key("x-real-ip");
    if has_proxy {
        return None;
    }
    peer.map(|p| p.ip().to_string())
}

/// Middleware: inject the TCP peer IP as a fallback `x-real-ip` for direct
/// (non-proxied) connections so client_ip is never empty for them
/// (the 0.7.x regression). A no-op when `ConnectInfo` is unavailable (unix
/// socket) or when a proxy header is already present. P-MW: the read side
/// converged on `RequestContext::client_ip` (`request_context::http_client_ip`,
/// same header > peer precedence); this layer remains so the injected header
/// also reaches `RequestMeta.headers` verbatim for workers.
pub(crate) async fn peer_ip_fallback(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let peer = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0);
    if let Some(ip) = peer_ip_fallback_target(req.headers(), peer) {
        if let Ok(val) = axum::http::HeaderValue::from_str(&ip) {
            req.headers_mut().insert("x-real-ip", val);
        }
    }
    next.run(req).await
}

/// Acquire a rate-limit token for a model. Shared by unary infer, SSE, and WS.
///
/// API-key check for models declaring `policies.auth`. An empty `keys` list
/// accepts any non-empty value (mirrors the retired Python RequireApiKey).
fn enforce_auth(
    auth: Option<&crate::config::AuthPolicy>,
    headers: &HeaderMap,
) -> Result<(), AppError> {
    let Some(auth) = auth else {
        return Ok(());
    };
    let value = headers
        .get(&auth.header)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if value.is_empty() {
        return Err(AppError::Unauthorized(format!(
            "missing API key (header: {})",
            auth.header
        )));
    }
    if !auth.keys.is_empty() && !crate::access_control::ct_contains(&auth.keys, value) {
        return Err(AppError::Unauthorized(format!(
            "invalid API key (header: {})",
            auth.header
        )));
    }
    Ok(())
}

/// F-03: unknown-model auth gate. When the target model cannot be resolved,
/// an unauthenticated probe must not be able to distinguish "no such model"
/// (404) from "no credentials" (401) — the differential leaks which models
/// exist. If ANY registered model declares `policies.auth`, a request that
/// fails model resolution is gated: credentials valid for at least one
/// declared policy see the original resolution error; anything else gets
/// 401. With no auth configured anywhere the original error is unchanged.
fn auth_gate_for_unknown_model(
    state: &AppState,
    headers: &HeaderMap,
    original: AppError,
) -> AppError {
    let policies = state.registry.all_auth_policies();
    if policies.is_empty() {
        return original;
    }
    let authenticated = policies.iter().any(|p| {
        let value = headers
            .get(&p.header)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        !value.is_empty()
            && (p.keys.is_empty() || crate::access_control::ct_contains(&p.keys, value))
    });
    if authenticated {
        original
    } else {
        AppError::Unauthorized("missing or invalid API key".to_string())
    }
}

/// `scope` derives from the policy key: `"ip"` → client IP from the
/// `RequestContext` (filled once by `context_middleware`), otherwise the
/// constant `"/predict"` route scope so all inference paths for
/// a model share one bucket. Returns `RateLimitExceeded` (429 + Retry-After)
/// when the bucket is empty.
async fn enforce_rate_limit(
    state: &Arc<AppState>,
    rl: Option<&crate::config::RateLimitPolicy>,
    model_name: &str,
    client_ip: &str,
) -> Result<(), AppError> {
    let Some(rl) = rl else {
        return Ok(());
    };
    let scope = match rl.key.as_str() {
        "ip" => client_ip.to_string(),
        _ => "/predict".to_string(),
    };
    // Empty IP scope (unix-socket path with no peer, or a proxy that sent no
    // client headers) collapses every request into one shared bucket — surface
    // it once so per-IP limiting silently degrading to a global cap is visible.
    if rl.key == "ip" && scope.is_empty() {
        warn!(
            model = %model_name,
            "rate-limit key=ip resolved to empty scope; all requests share one bucket"
        );
    }
    let burst = rl.burst.unwrap_or(rl.requests_per_minute * 1.5);
    let key = format!("{}:{}", model_name, scope);
    match state
        .rate_limiter
        .acquire(&key, rl.requests_per_minute, burst)
    {
        crate::rate_limit::AcquireResult::Rejected { retry_after_secs } => {
            Err(AppError::RateLimitExceeded { retry_after_secs })
        }
        crate::rate_limit::AcquireResult::Allowed => Ok(()),
    }
}

// Header injection helpers moved to the protocol seam (P2.0) so error
// rendering lives in one place; re-exported here so existing call sites
// (inference.rs / custom_routes.rs) are unchanged.
pub(crate) use crate::protocol::inject_response_headers;




#[cfg(test)]
mod extractor_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn read_error_body(response: Response) -> Value {
        let body_bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        serde_json::from_slice(&body_bytes).unwrap()
    }

    #[tokio::test]
    async fn test_api_json_valid_body_passes() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input": 1}"#)).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_api_json_malformed_body_standardized_error() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .header("content-type", "application/json")
                .body(Body::from("{not json")).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_request_body");
        assert_eq!(body["error"]["param"], Value::Null);
    }

    #[tokio::test]
    async fn test_api_json_missing_content_type_standardized_error() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .body(Body::from(r#"{"input": 1}"#)).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["code"], "invalid_request_body");
    }

    #[tokio::test]
    async fn test_api_query_invalid_param_standardized_error() {
        #[derive(serde::Deserialize)]
        struct TestQuery { #[allow(dead_code)] flag: bool }

        let app = axum::Router::new().route("/t", axum::routing::get(
            |ApiQuery(q): ApiQuery<TestQuery>| async move { q.flag.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t?flag=notabool").body(Body::empty()).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_query_param");
        assert_eq!(body["error"]["param"], Value::Null);
    }
}

#[cfg(test)]
mod api_body_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use std::sync::Arc;
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

    async fn read_error_body(response: axum::response::Response) -> Value {
        let body_bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        serde_json::from_slice(&body_bytes).unwrap()
    }

    // ------------------------------------------------------------------
    // is_json_content_type unit tests
    // ------------------------------------------------------------------

    #[test]
    fn test_is_json_application_json() {
        let v = axum::http::HeaderValue::from_static("application/json");
        assert!(is_json_content_type(&v));
    }

    #[test]
    fn test_is_json_with_charset() {
        let v = axum::http::HeaderValue::from_static("application/json; charset=utf-8");
        assert!(is_json_content_type(&v));
    }

    #[test]
    fn test_is_json_suffix() {
        let v = axum::http::HeaderValue::from_static("application/problem+json");
        assert!(is_json_content_type(&v));
        let v = axum::http::HeaderValue::from_static("application/vnd.api+json");
        assert!(is_json_content_type(&v));
    }

    #[test]
    fn test_is_json_case_insensitive() {
        let v = axum::http::HeaderValue::from_static("Application/JSON");
        assert!(is_json_content_type(&v));
        let v = axum::http::HeaderValue::from_static("APPLICATION/JSON");
        assert!(is_json_content_type(&v));
    }

    #[test]
    fn test_is_json_text_json_rejected() {
        let v = axum::http::HeaderValue::from_static("text/json");
        assert!(!is_json_content_type(&v));
    }

    #[test]
    fn test_is_json_application_jsonx_rejected() {
        let v = axum::http::HeaderValue::from_static("application/jsonx");
        assert!(!is_json_content_type(&v));
    }

    #[test]
    fn test_is_json_garbage_rejected() {
        let v = axum::http::HeaderValue::from_static("not-even-a-mime");
        assert!(!is_json_content_type(&v));
    }

    #[test]
    fn test_is_json_non_ascii_rejected() {
        // Non-UTF8 bytes in header value → can't parse → raw
        let bytes: [u8; 4] = [0xFF, 0xFE, 0xFD, 0xFC];
        let v = axum::http::HeaderValue::from_bytes(&bytes).unwrap();
        assert!(!is_json_content_type(&v));
    }

    // ------------------------------------------------------------------
    // ApiBody extractor integration tests (via router)
    // ------------------------------------------------------------------

    fn test_app(max_body: usize) -> axum::Router {
        let state = Arc::new(TestState { max_body });
        axum::Router::new()
            .route("/echo-kind", axum::routing::post(
                |ApiBody(body): ApiBody| async move { body.kind().to_string() },
            ))
            .route("/echo-bytes", axum::routing::post(
                |ApiBody(body): ApiBody| async move {
                    axum::body::Body::from(body.bytes())
                },
            ))
            .layer(axum::extract::DefaultBodyLimit::max(max_body))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_api_body_json_content_type() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input": 1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body, "json");
    }

    #[tokio::test]
    async fn test_api_body_missing_content_type_defaults_to_json() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .body(Body::from(r#"{"input": 1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body, "json");
    }

    #[tokio::test]
    async fn test_api_body_octet_stream_is_raw() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(b"\x00\x01\x02\x03".as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body, "raw");
    }

    #[tokio::test]
    async fn test_api_body_invalid_json_returns_400() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from("{not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["code"], "invalid_request_body");
        assert!(body["error"]["message"].as_str().unwrap().contains("invalid JSON"));
    }

    #[tokio::test]
    async fn test_api_body_truncated_json_returns_400() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"a": 1"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["code"], "invalid_request_body");
    }

    #[tokio::test]
    async fn test_api_body_content_encoding_returns_415() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .header("content-encoding", "gzip")
                    .body(Body::from(r#"{"input": 1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["code"], "unsupported_media_type");
        assert!(body["error"]["message"].as_str().unwrap().contains("Content-Encoding"));
    }

    #[tokio::test]
    async fn test_api_body_bytes_preserved_byte_identical() {
        // JSON body bytes must survive byte-identical (no Value normalization).
        let input = r#"{"z":1,"a":2,"b": 3}"#;
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-bytes")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(input))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body.as_ref(), input.as_bytes());
    }

    #[tokio::test]
    async fn test_api_body_413_body_too_large() {
        let app = test_app(8); // 8-byte limit
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from("this body is way longer than 8 bytes"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["code"], "payload_too_large");
        assert_eq!(body["error"]["max_size"], 8);
    }

    // ------------------------------------------------------------------
    // test_body_limit_boundary (P0): == limit OK, limit+1 → 413
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_body_limit_boundary_exact_ok() {
        let limit = 16;
        let app = test_app(limit);
        let body = vec![b'x'; limit];
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK,
            "body exactly at limit ({limit} bytes) must be accepted");
    }

    #[tokio::test]
    async fn test_body_limit_boundary_over_rejected() {
        let limit = 16;
        let app = test_app(limit);
        let body = vec![b'x'; limit + 1];
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE,
            "body at limit+1 ({}) bytes must be rejected 413", limit + 1);
    }

    // ------------------------------------------------------------------
    // test_api_body_scalar_json (P1): scalar JSON values pass validation
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_api_body_scalar_json_number() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from("42"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body, "json", "scalar number 42 must be valid JSON → Json variant");
    }

    #[tokio::test]
    async fn test_api_body_scalar_json_string() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#""hello""#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body, "json", "scalar string must be valid JSON → Json variant");
    }

    #[tokio::test]
    async fn test_api_body_scalar_json_null() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from("null"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body, "json", "scalar null must be valid JSON → Json variant");
    }

    // ------------------------------------------------------------------
    // test_api_body_custom_tensor_type (P0): non-JSON custom types → Raw
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_api_body_custom_tensor_type_is_raw() {
        let app = test_app(64 * 1024 * 1024);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/x-tensor")
                    .body(Body::from(b"\x00\x01\x02\x03".as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body, "raw", "application/x-tensor must dispatch to Raw variant");
    }

    // ------------------------------------------------------------------
    // test_api_body_json_byte_identical_non_canonical (P0):
    // non-canonical JSON (extra whitespace, Unicode escapes, key order)
    // must survive byte-identical — no Value normalisation, no re-serialize.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_api_body_json_byte_identical_whitespace() {
        let app = test_app(64 * 1024 * 1024);
        let input = "  {  \"z\" : 1 , \"a\" : 2 }  "; // extra whitespace everywhere
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-bytes")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(input))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body.as_ref(), input.as_bytes(),
            "non-canonical JSON with extra whitespace must be byte-identical");
    }

    #[tokio::test]
    async fn test_api_body_json_byte_identical_unicode() {
        let app = test_app(64 * 1024 * 1024);
        let input = r#"{"key":"Hello","emoji":"😀"}"#;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-bytes")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(input))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(body.as_ref(), input.as_bytes(),
            "JSON with Unicode escapes and emoji must be byte-identical");
    }

    // ------------------------------------------------------------------
    // test_payload_ptr_identity (P0): Bytes clone shares same buffer
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // /audit fdbd1c9 evidence tests (must FAIL on the audited commit)
    // ------------------------------------------------------------------

    /// D4/D5 (P1 acceptance): when the client sent a Content-Length, the 413
    /// error body must carry `actual_size` so clients can self-correct.
    /// The audited commit hardcodes `actual_size: None` in map_body_rejection.
    #[tokio::test]
    async fn test_audit_413_reports_actual_size_when_content_length_known() {
        let app = test_app(8);
        let payload = "this body is way longer than 8 bytes"; // 36 bytes
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/echo-kind")
                    .method("POST")
                    .header("content-type", "application/octet-stream")
                    .header("content-length", payload.len().to_string())
                    .body(Body::from(payload))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["max_size"], 8);
        assert_eq!(
            body["error"]["actual_size"],
            serde_json::json!(payload.len()),
            "actual_size must be reported when Content-Length is known (plan §7.6 P1)"
        );
    }

    /// D9 parity: pin Rust's authoritative verdicts for the full probed
    /// Content-Type matrix. The Python `_is_json_content_type` must answer
    /// identically — locked by
    /// `TestAuditContentTypeParityMalformedParams` / the parity suite.
    #[test]
    fn test_audit_mime_malformed_params_rust_verdict() {
        // Malformed parameter lists / whitespace in tokens → raw dispatch.
        for v in [
            "application/json;;",
            "application/json; charset",
            "application/json ;charset=utf-8",
            "application/json;foo=\"bar",
            " application/json",
            "application/json ",
            "application/json; charset = utf-8",
            "application/json; charset= utf-8",
            "application/json; charset=utf-8 ",
            "application/json;\tcharset=utf-8",
        ] {
            let hv = axum::http::HeaderValue::from_str(v).unwrap();
            assert!(
                !is_json_content_type(&hv),
                "Rust (authoritative, D9) must dispatch {v:?} to raw"
            );
        }
        // Strict-but-legal forms stay JSON (regression lock for the Python
        // mirror — these must NOT be tightened away).
        for v in [
            "application/json;",
            "application/json;charset=utf-8",
            "application/json; charset=utf-8",
            "application/json;  charset=utf-8",
            "application/json; charset=\"utf-8\"",
            "application/json; charset=utf-8;",
            "application/json; charset=utf-8; ",
            "application/json; charset=UTF-8",
        ] {
            let hv = axum::http::HeaderValue::from_str(v).unwrap();
            assert!(
                is_json_content_type(&hv),
                "Rust must keep dispatching {v:?} to JSON"
            );
        }
    }

    #[test]
    fn test_request_body_bytes_shares_underlying_buffer() {
        // A Bytes clone must share the same underlying memory buffer
        // (refcount increment, O(1), zero extra copy). This is the
        // foundational guarantee for the zero-copy chain D3/D13.
        let data = vec![0u8; 4096]; // large enough for heap allocation
        let original = Bytes::from(data);
        let cloned = original.clone();

        assert_eq!(original.len(), cloned.len(),
            "clone must preserve length");
        assert_eq!(original.as_ptr(), cloned.as_ptr(),
            "clone must share the same underlying buffer pointer");
    }
}

#[cfg(test)]
mod status_family_tests {
    use super::status_family;

    #[test]
    fn test_status_family_mapping() {
        assert_eq!(status_family(0), "2xx");     // unset → success
        assert_eq!(status_family(200), "2xx");
        assert_eq!(status_family(201), "2xx");
        assert_eq!(status_family(302), "3xx");
        assert_eq!(status_family(400), "4xx");
        assert_eq!(status_family(404), "4xx");
        assert_eq!(status_family(500), "5xx");
        assert_eq!(status_family(503), "5xx");
    }
}


#[cfg(test)]
mod client_ip_tests {
    use super::*;
    use std::net::SocketAddr;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn addr(ip: &str) -> Option<SocketAddr> {
        Some(format!("{ip}:1234").parse().unwrap())
    }

    // --- extract_client_ip: header-based resolution moved to
    //     `request_context::http_client_ip` (P-MW); its tests moved with it. ---

    // --- peer_ip_fallback_target: should the layer inject the peer IP? ---

    #[test]
    fn fallback_injects_peer_when_no_proxy_header() {
        // Direct connection (no reverse proxy): the TCP peer IS the client.
        // This is the 0.7.x regression — client_ip was empty for direct HTTP.
        assert_eq!(
            peer_ip_fallback_target(&headers(&[]), addr("203.0.113.7")),
            Some("203.0.113.7".to_string())
        );
    }

    #[test]
    fn fallback_skips_when_xff_present() {
        // A (spoofable) proxy header wins; never overwrite it with the peer IP.
        let h = headers(&[("x-forwarded-for", "10.0.0.99")]);
        assert_eq!(peer_ip_fallback_target(&h, addr("203.0.113.7")), None);
    }

    #[test]
    fn fallback_skips_when_real_ip_present() {
        let h = headers(&[("x-real-ip", "10.0.0.2")]);
        assert_eq!(peer_ip_fallback_target(&h, addr("203.0.113.7")), None);
    }

    #[test]
    fn fallback_skips_when_no_peer() {
        // Unix-socket path: no ConnectInfo → nothing to inject.
        assert_eq!(peer_ip_fallback_target(&headers(&[]), None), None);
    }
}

#[cfg(test)]
mod auth_policy_tests {
    use super::*;
    use crate::config::AuthPolicy;

    fn policy(keys: &[&str]) -> AuthPolicy {
        AuthPolicy {
            header: "X-API-Key".to_string(),
            keys: keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    fn headers_with(key: &str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            axum::http::HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[test]
    fn test_no_policy_passes() {
        assert!(enforce_auth(None, &HeaderMap::new()).is_ok());
    }

    #[test]
    fn test_missing_header_rejected() {
        let err = enforce_auth(Some(&policy(&["sk-a"])), &HeaderMap::new()).unwrap_err();
        assert!(matches!(err, AppError::Unauthorized(_)));
        assert!(err.to_string().contains("missing API key"));
    }

    #[test]
    fn test_wrong_key_rejected() {
        let h = headers_with("x-api-key", "sk-wrong");
        let err = enforce_auth(Some(&policy(&["sk-a"])), &h).unwrap_err();
        assert!(err.to_string().contains("invalid API key"));
    }

    #[test]
    fn test_correct_key_passes() {
        let h = headers_with("X-API-Key", "sk-a");
        assert!(enforce_auth(Some(&policy(&["sk-a", "sk-b"])), &h).is_ok());
    }

    #[test]
    fn test_empty_keys_accepts_any_nonempty() {
        let h = headers_with("x-api-key", "anything");
        assert!(enforce_auth(Some(&policy(&[])), &h).is_ok());
        assert!(enforce_auth(Some(&policy(&[])), &HeaderMap::new()).is_err());
    }

    #[test]
    fn test_custom_header_name_case_insensitive() {
        let p = AuthPolicy {
            header: "Authorization".to_string(),
            keys: vec!["tok".to_string()],
        };
        let h = headers_with("authorization", "tok");
        assert!(enforce_auth(Some(&p), &h).is_ok());
    }
}
