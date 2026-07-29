use crate::error::AppError;
use crate::http::state::AppState;
use crate::http::RequestId;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::streaming;
use crate::worker::protocol::RouteDecl;
use axum::{
    extract::{Multipart, Path, Query, State},
    response::{IntoResponse, Json, Response},
};
use axum::extract::ws::{Message, WebSocket};

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
    type Rejection = AppError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(ApiJson(value)),
            Err(rejection) => Err(AppError::InvalidRequestBody(rejection.body_text())),
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
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(ApiQuery(value)),
            Err(rejection) => Err(AppError::InvalidQueryParam(rejection.body_text())),
        }
    }
}
use axum::http::header::{HeaderMap, CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::response::sse::{Event, Sse};
use serde::Deserialize;
use serde_json::{json, Value};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};
use uuid::Uuid;

pub mod health;
pub use health::*;
pub mod admin;
pub use admin::*;
pub mod inference;
pub use inference::*;


fn extract_client_ip(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or("")
        .to_string()
}

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
/// (non-proxied) connections so `extract_client_ip` is never empty for them
/// (the 0.7.x regression). A no-op when `ConnectInfo` is unavailable (unix
/// socket) or when a proxy header is already present.
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
    if !auth.keys.is_empty() && !auth.keys.iter().any(|k| k == value) {
        return Err(AppError::Unauthorized(format!(
            "invalid API key (header: {})",
            auth.header
        )));
    }
    Ok(())
}

/// `scope` derives from the policy key: `"ip"` → client IP from headers,
/// otherwise the constant `"/predict"` route scope so all inference paths for
/// a model share one bucket. Returns `RateLimitExceeded` (429 + Retry-After)
/// when the bucket is empty.
async fn enforce_rate_limit(
    state: &Arc<AppState>,
    rl: Option<&crate::config::RateLimitPolicy>,
    model_name: &str,
    headers: &HeaderMap,
) -> Result<(), AppError> {
    let Some(rl) = rl else {
        return Ok(());
    };
    let scope = match rl.key.as_str() {
        "ip" => extract_client_ip(headers),
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

/// Headers that must NOT be overridden by user code — managed by the HTTP
/// library or carry hop-by-hop semantics (RFC 7230 §6.1).
const BLOCKED_RESPONSE_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "transfer-encoding",
    "content-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
];

/// Attach CORS headers to an inference response (success or error).
fn attach_cors_headers(
    state: &AppState,
    model: &str,
    result: Result<Response, AppError>,
) -> Response {
    let mut resp = match result {
        Ok(r) => r,
        Err(e) => e.into_response(),
    };
    if let Some(cors) = state.registry.active_cors_headers(model) {
        extend_cors_headers(resp.headers_mut(), &cors);
    }
    resp
}

/// Copy pre-built CORS headers (Arc-shared, cached at policy ingest — B9) into
/// a response in place. Entries are cloned since the source is shared behind an
/// `Arc<HeaderMap>`.
fn extend_cors_headers(target: &mut HeaderMap, src: &axum::http::HeaderMap) {
    for (name, value) in src {
        target.append(name.clone(), value.clone());
    }
}

fn inject_response_headers(
    builder: axum::http::response::Builder,
    headers: &std::collections::HashMap<String, String>,
) -> axum::http::response::Builder {
    let mut builder = builder;
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if !BLOCKED_RESPONSE_HEADERS.contains(&lower.as_str()) {
            builder = builder.header(k.as_str(), v.as_str());
        }
    }
    builder
}

/// Inject response headers into an existing `HeaderMap` in place — the
/// error-response path builds via `into_response()` (no Builder), so it needs
/// this mutating variant. Skips hop-by-hop / library-managed headers and
/// silently drops headers with invalid names/values.
pub(crate) fn inject_response_headers_into(
    map: &mut HeaderMap,
    headers: &std::collections::HashMap<String, String>,
) {
    for (k, v) in headers {
        if BLOCKED_RESPONSE_HEADERS.contains(&k.to_ascii_lowercase().as_str()) {
            continue;
        }
        if let (Ok(name), Ok(val)) = (
            k.parse::<axum::http::HeaderName>(),
            axum::http::HeaderValue::from_str(v),
        ) {
            map.insert(name, val);
        }
    }
}


// ===== Custom Routes (phase 2) =====

/// Result of matching an incoming tail against a version's declared routes.
#[derive(Debug)]
enum RouteMatch {
    /// Pattern + HTTP method both matched; carries the declared pattern
    /// (sent back as `meta.route`) and extracted path params.
    Hit {
        pattern: String,
        path_params: HashMap<String, String>,
    },
    /// A pattern matched but its methods do not include the request method.
    MethodNotAllowed,
    /// No pattern matched the tail.
    NotFound,
}

/// If `seg` is a path-param placeholder (`{name}` or `:name`), return `name`.
fn param_name(seg: &str) -> Option<&str> {
    seg.strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .or_else(|| seg.strip_prefix(':'))
        .filter(|name| !name.is_empty())
}

/// Match `tail` against `routes` (segment-wise, supporting `{p}`/`:p` params)
/// considering the HTTP `method`. First pattern+method hit wins; if a pattern
/// matches but the method does not, returns `MethodNotAllowed` (so the caller
/// can answer 405 rather than 404).
fn match_route(routes: &[RouteDecl], tail: &str, method: &str) -> RouteMatch {
    let req: Vec<&str> = tail.split('/').filter(|s| !s.is_empty()).collect();
    let mut pattern_matched = false;
    for r in routes {
        let pat: Vec<&str> = r.route.split('/').filter(|s| !s.is_empty()).collect();
        if pat.len() != req.len() {
            continue;
        }
        let mut params: HashMap<String, String> = HashMap::new();
        let mut matched = true;
        for (p, q) in pat.iter().zip(req.iter()) {
            if let Some(name) = param_name(p) {
                params.insert(name.to_string(), (*q).to_string());
            } else if p != q {
                matched = false;
                break;
            }
        }
        if matched {
            pattern_matched = true;
            if r.methods.iter().any(|m| m.eq_ignore_ascii_case(method)) {
                return RouteMatch::Hit {
                    pattern: r.route.clone(),
                    path_params: params,
                };
            }
        }
    }
    if pattern_matched {
        RouteMatch::MethodNotAllowed
    } else {
        RouteMatch::NotFound
    }
}

/// Split the catch-all `tail` into an optional explicit version and the
/// remaining route tail: `versions/<v>/<rest...>` → `(Some(v), rest)`;
/// everything else → `(None, tail)`. Leading slashes are normalized away.
fn parse_route_tail(tail: &str) -> (Option<String>, String) {
    let trimmed = tail.trim_start_matches('/');
    if let Some(rest) = trimmed.strip_prefix("versions/") {
        let (v, r) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        return (Some(v.to_string()), r.trim_start_matches('/').to_string());
    }
    (None, trimmed.to_string())
}

/// Dispatch a custom-route request for `/v2/models/<model>/<tail>` to a worker.
/// Called by the fallback handler (`http::route_fallback`) after it parses the
/// path. Resolves the model version, matches the tail against the version's
/// declared `@route`s, and dispatches over ZMQ (bypassing the batch
/// InferenceQueue — route calls are not aggregatable). System leaves
/// (`infer`/`events`/`stream`/...) are exact-registered and matched by axum, so
/// they never reach the fallback.
pub async fn dispatch_custom_route(
    state: &AppState,
    model_name: &str,
    tail: &str,
    method: &axum::http::Method,
    query: HashMap<String, String>,
    headers: &HeaderMap,
    body: bytes::Bytes,
    request_id: String,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(model_name)?;
    let method_str = method.as_str();

    let (version, route_tail) = parse_route_tail(tail);
    if let Some(ref v) = version {
        crate::validation::validate_version(v)?;
    }
    let resolved_version = resolve_version(state, model_name, version, headers).await?;

    if !state.registry.is_ready(model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }

    // Match against this version's declared routes.
    let routes = state.worker_manager.get_routes(model_name, &resolved_version).await;
    let (route_pattern, path_params) = match match_route(&routes, &route_tail, method_str) {
        RouteMatch::Hit { pattern, path_params } => (pattern, path_params),
        RouteMatch::MethodNotAllowed => return Err(AppError::MethodNotAllowed),
        RouteMatch::NotFound => return Err(AppError::RouteNotFound),
    };

    // Select a worker (mirror open_worker_stream: skip ejected, else random).
    let mv = state
        .registry
        .get(model_name, Some(&resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;
    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", model_name)));
    }
    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, &resolved_version)
        .await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", model_name, resolved_version)))?;
    let worker_id = match state
        .worker_manager
        .get_outlier_state(model_name, &resolved_version)
        .await
    {
        Some(outlier) => crate::worker::pick_worker_skip_ejected(num_workers, &outlier),
        None => crate::worker::pick_worker_random(num_workers),
    };
    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }
    let client = clients[worker_id].clone();

    // Fire InferenceRequest callback — model-level callbacks cover custom
    // routes too, same as the inference paths (do_infer & co.).
    let start = Instant::now();
    let req_ctx = crate::callback::InferenceContext {
        model_name: model_name.to_string(),
        version: resolved_version.clone(),
        route: route_pattern.clone(),
        protocol: crate::callback::Protocol::Http,
        request_id: request_id.clone(),
        client_ip: extract_client_ip(headers),
        elapsed_us: None,
    };
    let cb_runner = state.callback_runner.clone();
    let req_ctx_clone = req_ctx.clone();
    tokio::spawn(async move {
        cb_runner.on_inference_request(&req_ctx_clone).await;
    });

    // Build the request: route_call reuses the SingleRequest body type; the
    // route tag discriminates dispatch in the worker. method/query/path_params
    // ride on RequestMeta (route_pattern == meta.route).
    let mut meta = build_request_meta(headers, &serde_json::Value::Null, &route_pattern, request_id);
    meta.method = method_str.to_string();
    meta.query = query;
    meta.path_params = path_params;

    let uid = format!("route_{}_{}-{}", model_name, resolved_version, Uuid::new_v4());
    let request = pb::Request {
        uid,
        meta: Some(meta),
        payload: Some(pb::request::Payload::RouteCall(pb::SingleRequest { data: body })),
        ..Default::default()
    };

    let (mut resp_rx, mut chunk_rx) = client.send_route_or_stream(request).await?;

    // The handler picks the reply shape: a plain/Response result answers with
    // one SingleResponse; a StreamingResponse result answers with a
    // start→chunks→done frame sequence. Race the first arrival, bounded like
    // a unary send so a dead worker can't hang the request.
    enum RouteReply {
        Unary(Option<pb::Response>),
        Stream(pb::StreamResponse),
    }
    let first = tokio::time::timeout(ROUTE_FIRST_FRAME_TIMEOUT, async {
        tokio::select! {
            unary = &mut resp_rx => RouteReply::Unary(unary.ok()),
            frame = chunk_rx.recv() => match frame {
                Some(f) => RouteReply::Stream(f),
                // Chunk sender dropped with no frames sent: the unary reply
                // arrived first (its delivery frees the unused stream route,
                // which is what closed this channel). Wait for the unary
                // reply instead of treating the close as a stream failure —
                // select! would otherwise pick between two ready arms at
                // random.
                None => RouteReply::Unary(resp_rx.await.ok()),
            },
        }
    })
    .await
    .map_err(|_| AppError::InferenceTimeout("route response timeout".to_string()))?;

    let result = match first {
        RouteReply::Unary(Some(response)) => match response.payload {
            // Decode the SingleResponse (routes reuse the inference response shape).
            Some(pb::response::Payload::Single(single)) => build_route_http_response(single),
            _ => Err(AppError::WorkerCrashed("unexpected response type".to_string())),
        },
        RouteReply::Unary(None) => Err(AppError::WorkerCrashed(
            "route reply channel closed".to_string(),
        )),
        RouteReply::Stream(frame) => match frame.payload {
            Some(pb::stream_response::Payload::Start(start)) => {
                build_route_stream_http_response(start, chunk_rx)
            }
            _ => Err(AppError::WorkerCrashed(
                "route stream missing start frame".to_string(),
            )),
        },
    };

    // Fire InferenceResponse callback on success, mirroring do_infer.
    if result.is_ok() {
        let resp_ctx = crate::callback::InferenceContext {
            elapsed_us: Some(start.elapsed().as_micros() as u64),
            ..req_ctx
        };
        let cb_runner = state.callback_runner.clone();
        tokio::spawn(async move { cb_runner.on_inference_response(&resp_ctx).await; });
    }

    result
}

/// Bound on waiting for a route call's first reply frame (unary or stream
/// start). Mirrors the transport's unary response timeout.
const ROUTE_FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(60);

/// Build the HTTP response for a streaming route reply. The `start` frame
/// carries the handler-chosen status/headers/media_type; subsequent `chunk`
/// frames form the body. For `text/event-stream` each chunk is framed as one
/// SSE event; other media types pass chunk bytes through verbatim. A
/// mid-stream `error` frame ends the body (as a final SSE event in SSE
/// mode), since the status line is already on the wire.
fn build_route_stream_http_response(
    start: pb::StreamStart,
    chunk_rx: mpsc::Receiver<pb::StreamResponse>,
) -> Result<Response, AppError> {
    let is_sse = start.media_type.starts_with("text/event-stream");
    let content_type = if start.media_type.is_empty() {
        "text/event-stream"
    } else {
        start.media_type.as_str()
    };
    let body = futures::stream::unfold((chunk_rx, false), move |(mut rx, ended)| async move {
        if ended {
            return None;
        }
        match rx.recv().await.map(|f| f.payload) {
            Some(Some(pb::stream_response::Payload::Chunk(c))) => {
                let data = if is_sse { sse_frame(&c.data) } else { c.data.to_vec() };
                Some((Ok::<_, Infallible>(bytes::Bytes::from(data)), (rx, false)))
            }
            Some(Some(pb::stream_response::Payload::Error(e))) => {
                warn!("route stream error: {}", e.message);
                if is_sse {
                    let event = sse_frame(e.message.as_bytes());
                    Some((Ok(bytes::Bytes::from(event)), (rx, true)))
                } else {
                    None
                }
            }
            // Done, channel closed, or an unexpected frame — end the body.
            _ => None,
        }
    });
    let mut builder = Response::builder().header("content-type", content_type);
    if start.status_code > 0 {
        if let Ok(sc) = axum::http::StatusCode::from_u16(start.status_code as u16) {
            builder = builder.status(sc);
        }
    }
    let builder = inject_response_headers(builder, &start.headers);
    builder
        .body(axum::body::Body::from_stream(body))
        .map_err(|e| AppError::Internal(format!("build response: {}", e)))
}

/// Frame one payload as an SSE event: every line becomes its own `data:`
/// line (embedded newlines would otherwise corrupt the event stream).
fn sse_frame(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 8);
    for line in data.split(|b| *b == b'\n') {
        out.extend_from_slice(b"data: ");
        out.extend_from_slice(line);
        out.push(b'\n');
    }
    out.push(b'\n');
    out
}

/// Map a worker `SingleResponse` (from a route call) onto an HTTP response.
/// Unlike inference, the body is passed through verbatim (the worker already
/// serialized it; route handlers may produce non-JSON bodies). `status.code`
/// is "Ok" on normal completion; "Error" carries an HTTP status in
/// `status.message` (worker exception path).
fn build_route_http_response(single: pb::SingleResponse) -> Result<Response, AppError> {
    let code = single.status.as_ref().map(|s| s.code.as_str()).unwrap_or("Ok");
    if code == "Ok" {
        let content_type = if single.media_type.is_empty() {
            "application/json; charset=utf-8"
        } else {
            &single.media_type
        };
        let mut builder = Response::builder().header("content-type", content_type);
        if single.status_code > 0 {
            if let Ok(sc) = axum::http::StatusCode::from_u16(single.status_code as u16) {
                builder = builder.status(sc);
            }
        }
        let builder = inject_response_headers(builder, &single.headers);
        return builder
            .body(axum::body::Body::from(single.data.to_vec()))
            .map_err(|e| AppError::Internal(format!("build response: {}", e)));
    }

    // Worker exception: status.message holds the HTTP status, data = error JSON.
    let msg = single
        .status
        .as_ref()
        .filter(|s| !s.message.is_empty())
        .map(|s| s.message.clone())
        .unwrap_or_else(|| "500".to_string());
    if let Ok(http_status) = msg.parse::<u16>() {
        let data: serde_json::Value = if single.data.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&single.data).unwrap_or(json!({}))
        };
        let o = data.get("error");
        let pick = |k: &str| o.and_then(|e| e.get(k)).and_then(|v| v.as_str()).map(String::from);
        Err(AppError::ModelError(Box::new(crate::error::ModelErrorData {
            status_code: http_status,
            error_type: pick("type").unwrap_or_else(|| "model_error".to_string()),
            detail: pick("message").unwrap_or_else(|| "model error".to_string()),
            code: pick("code"),
            param: pick("param"),
            headers: if single.headers.is_empty() {
                None
            } else {
                Some(single.headers.clone())
            },
        })))
    } else {
        Err(AppError::WorkerCrashed(msg))
    }
}

async fn open_worker_stream(
    state: &Arc<AppState>,
    model_name: &str,
    resolved_version: &str,
    meta: pb::RequestMeta,
    payload_bytes: bytes::Bytes,
) -> Result<(String, mpsc::Receiver<pb::StreamResponse>), AppError> {
    let mv = state
        .registry
        .get(model_name, Some(resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", model_name)));
    }

    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, resolved_version)
        .await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", model_name, resolved_version)))?;

    // Skip ejected workers for streaming requests
    let worker_id = if let Some(outlier) = state.worker_manager.get_outlier_state(model_name, resolved_version).await {
        crate::worker::pick_worker_skip_ejected(num_workers, &outlier)
    } else {
        crate::worker::pick_worker_random(num_workers)
    };

    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }

    let client = &clients[worker_id];
    let stream_id = format!("stream-{}", Uuid::new_v4());
    let open_req = streaming::build_stream_open(stream_id.clone(), payload_bytes, Some(meta));

    let chunk_rx = client.send_stream(open_req, stream_id.clone()).await?;
    Ok((stream_id, chunk_rx))
}

// ===== SSE Streaming =====

pub async fn sse_infer_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    RequestId(request_id): RequestId,
    ApiJson(payload): ApiJson<Value>,
) -> Response {
    let result =
        sse_infer_entry(&state, &model_name, None, headers, payload, request_id).await;
    attach_cors_headers(&state, &model_name, result)
}

pub async fn sse_infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    RequestId(request_id): RequestId,
    ApiJson(payload): ApiJson<Value>,
) -> Response {
    let result = sse_infer_entry(
        &state, &model_name, Some(version), headers, payload, request_id,
    )
    .await;
    attach_cors_headers(&state, &model_name, result)
}

/// Shared entry for SSE inference: validation, ready check, rate limiting,
/// and stream setup. Returns a `Response` so the caller can uniformly wrap
/// CORS around both the success stream-start and any early error.
async fn sse_infer_entry(
    state: &Arc<AppState>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    payload: Value,
    request_id: String,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(model_name)?;
    if let Some(ref v) = version {
        crate::validation::validate_version(v)?;
    }
    let resolved_version = resolve_version(state, model_name, version, &headers).await?;
    if !state.registry.is_ready(model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }
    // Auth + rate limit (rate limit shares the /predict bucket with unary infer).
    if let Some(mv) = state.registry.get(model_name, Some(&resolved_version)) {
        enforce_auth(mv.policies.auth.as_ref(), &headers)?;
        enforce_rate_limit(state, mv.policies.rate_limit.as_ref(), model_name, &headers).await?;
    }
    let sse = sse_infer_impl(
        state.clone(),
        model_name.to_string(),
        resolved_version,
        headers,
        payload,
        request_id,
    )
    .await?;
    Ok(sse.into_response())
}

async fn sse_infer_impl(
    state: Arc<AppState>,
    model_name: String,
    resolved_version: String,
    headers: HeaderMap,
    payload: Value,
    request_id: String,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {

    let meta = build_request_meta(&headers, &payload, "/predict", request_id);
    let payload_bytes = meta.payload.clone();
    let (stream_id, mut chunk_rx) = open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await?;

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "sse");
    }

    let (event_tx, event_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        while let Some(chunk) = chunk_rx.recv().await {
            let event = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
                    if stream_metrics {
                        if first_chunk {
                            prometheus::record_stream_ttft(&model_name, &resolved_version, "sse", open_time.elapsed().as_secs_f64());
                            first_chunk = false;
                        } else {
                            prometheus::record_stream_tbt(&model_name, &resolved_version, "sse", last_chunk_time.elapsed().as_secs_f64());
                        }
                        last_chunk_time = std::time::Instant::now();
                        prometheus::record_stream_chunk(&model_name, &resolved_version, "sse");
                    }
                    let data = String::from_utf8_lossy(&c.data);
                    Event::default().data(&data)
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    // Try to parse as structured error from HTTPException
                    let event_data = match serde_json::from_str::<serde_json::Value>(&e.message) {
                        Ok(val) if val.get("error").and_then(|err| err.get("type")).is_some() => {
                            json!({"error": val["error"]}).to_string()
                        }
                        _ => json!({"error": e.message}).to_string(),
                    };
                    Event::default().data(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    Event::default().data("[DONE]")
                }
                _ => continue,
            };
            if event_tx.send(Ok(event)).await.is_err() {
                break;
            }
            if matches!(chunk.payload, Some(pb::stream_response::Payload::Done(_))) {
                break;
            }
        }
        if stream_metrics {
            prometheus::record_stream_close(&model_name, &resolved_version, "sse");
        }
        // Ensure stream is cleaned up on worker side
        let cancel_req = streaming::build_stream_cancel(stream_id);
        open_worker_stream_cancel(&state, &model_name, &resolved_version, cancel_req).await;
    });

    Ok(Sse::new(ReceiverStream::new(event_rx)))
}

/// Send a stream cancel request to all workers for a model version.
/// Best-effort: workers that don't own the stream will ignore it.
async fn open_worker_stream_cancel(
    state: &Arc<AppState>,
    model_name: &str,
    version: &str,
    cancel_req: pb::Request,
) {
    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, version)
        .await;
    match clients {
        Some(list) => {
            for client in &list {
                let _ = client.send(cancel_req.clone()).await;
            }
        }
        None => {
            // Worker may already be unloaded; nothing to cancel
        }
    }
}

// ===== WebSocket Streaming =====

pub async fn ws_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    RequestId(request_id): RequestId,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, None, headers, socket, request_id))
}

pub async fn ws_stream_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    RequestId(request_id): RequestId,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if let Err(e) = crate::validation::validate_version(&version) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, Some(version), headers, socket, request_id))
}

async fn handle_ws_stream(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    headers: HeaderMap,
    mut socket: WebSocket,
    request_id: String,
) {
    let resolved_version = match resolve_version(&state, &model_name, version, &headers).await {
        Ok(v) => v,
        Err(_) => {
            let _ = socket.close().await;
            return;
        }
    };

    if !state.registry.is_ready(&model_name, Some(&resolved_version)) {
        let _ = socket.close().await;
        return;
    }

    // Rate limit (same logic as HTTP infer). The original upgrade request's
    // headers are preserved (including the peer_ip_fallback-injected x-real-ip
    // for direct connections), so key="ip" limits per real client.
    if let Some(mv) = state.registry.get(&model_name, Some(&resolved_version)) {
        if enforce_auth(mv.policies.auth.as_ref(), &headers).is_err()
            || enforce_rate_limit(
                &state,
                mv.policies.rate_limit.as_ref(),
                &model_name,
                &headers,
            )
            .await
            .is_err()
        {
            let _ = socket
                .send(Message::Text(
                    json!({"error": "rate limit exceeded"}).to_string(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    }

    // Wait for first message from client (the request payload)
    let first_msg = match socket.recv().await {
        Some(Ok(Message::Text(text))) => text,
        Some(Ok(Message::Binary(bin))) => String::from_utf8_lossy(&bin).to_string(),
        _ => {
            let _ = socket.close().await;
            return;
        }
    };

    let payload: Value = match serde_json::from_str(&first_msg) {
        Ok(v) => v,
        Err(_) => {
            let _ = socket.send(Message::Text(json!({"error": "invalid JSON"}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    // The original upgrade-request headers (with the peer-IP fallback applied
    // by the middleware) flow into meta so WS client_ip/rate-limit are correct.
    let meta = build_request_meta(&headers, &payload, "/predict", request_id);
    let payload_bytes = meta.payload.clone();

    let (stream_id, mut chunk_rx) = match open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await {
        Ok(r) => r,
        Err(e) => {
            let _ = socket.send(Message::Text(json!({"error": e.to_string()}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "websocket");
    }

    // Clone before move so cancel can reference them after the task completes
    let model_name_owned = model_name.clone();
    let version_owned = resolved_version.clone();

    // Spawn task to forward worker chunks -> WebSocket
    let send_task = tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        while let Some(chunk) = chunk_rx.recv().await {
            let msg = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
                    if stream_metrics {
                        if first_chunk {
                            prometheus::record_stream_ttft(&model_name, &resolved_version, "websocket", open_time.elapsed().as_secs_f64());
                            first_chunk = false;
                        } else {
                            prometheus::record_stream_tbt(&model_name, &resolved_version, "websocket", last_chunk_time.elapsed().as_secs_f64());
                        }
                        last_chunk_time = std::time::Instant::now();
                        prometheus::record_stream_chunk(&model_name, &resolved_version, "websocket");
                    }
                    Message::Binary(c.data.to_vec())
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    // Try to parse as structured error from HTTPException
                    let event_data = match serde_json::from_str::<serde_json::Value>(&e.message) {
                        Ok(val) if val.get("error").and_then(|err| err.get("type")).is_some() => {
                            json!({"error": val["error"]}).to_string()
                        }
                        _ => json!({"error": e.message}).to_string(),
                    };
                    Message::Text(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    Message::Text(json!({"done": true}).to_string())
                }
                _ => continue,
            };
            if socket.send(msg).await.is_err() {
                break;
            }
            if matches!(chunk.payload, Some(pb::stream_response::Payload::Done(_))) {
                let _ = socket.close().await;
                break;
            }
        }
        if stream_metrics {
            prometheus::record_stream_close(&model_name, &resolved_version, "websocket");
        }
        stream_id
    });

    // For now, we don't handle bidirectional streaming over WebSocket
    // Just wait for the send task to complete
    let completed_stream_id = match send_task.await {
        Ok(sid) => sid,
        Err(_) => return,
    };

    // Send cancel to clean up
    let cancel_req = streaming::build_stream_cancel(completed_stream_id);
    open_worker_stream_cancel(&state, &model_name_owned, &version_owned, cancel_req).await;
}

// ===== Upload Model =====

#[derive(Deserialize)]
pub struct UploadQuery {
    pub load: Option<bool>,
}

pub async fn upload_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiQuery(query): ApiQuery<UploadQuery>,
    mut multipart: Multipart,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let target_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;
    tokio::fs::create_dir_all(&target_dir)
        .await
        .map_err(AppError::Io)?;

    let mut uploaded_files: Vec<String> = Vec::new();

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        AppError::Validation(format!("multipart error: {}", e))
    })? {
        let filename = field
            .file_name()
            .unwrap_or("unnamed")
            .to_string();

        let data = field.bytes().await.map_err(|e| {
            AppError::Transport(format!("read upload field: {}", e))
        })?;

        if filename.ends_with(".lma") {
            // Save .lma to temp file, then unpack via Python CLI
            let tmp_dir = std::env::temp_dir().join(format!(
                "lite-server-upload-{}",
                uuid::Uuid::new_v4()
            ));
            tokio::fs::create_dir_all(&tmp_dir)
                .await
                .map_err(AppError::Io)?;
            let tmp_file = tmp_dir.join(&filename);
            tokio::fs::write(&tmp_file, &data)
                .await
                .map_err(AppError::Io)?;

            let output = tokio::process::Command::new("python")
                .args([
                    "-m",
                    "lite_server",
                    "unpack",
                    tmp_file.to_str().unwrap_or(""),
                    "--to",
                    target_dir.to_str().unwrap_or(""),
                    "--flat",
                ])
                .output()
                .await
                .map_err(|e| AppError::Internal(format!("failed to run python unpack: {}", e)))?;

            // Clean up temp dir
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(AppError::Validation(format!(
                    "artifact unpack failed: {}",
                    stderr.trim()
                )));
            }
            uploaded_files.push(filename);
        } else {
            // Raw file: write directly to target dir
            // Sanitize filename: strip any path components
            let safe_name = std::path::Path::new(&filename)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if safe_name.is_empty() || safe_name.starts_with('.') {
                continue;
            }
            let file_path = target_dir.join(&safe_name);
            tokio::fs::write(&file_path, &data)
                .await
                .map_err(AppError::Io)?;
            uploaded_files.push(safe_name);
        }
    }

    if uploaded_files.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    // Optionally auto-load after upload
    let auto_load = query.load.unwrap_or(true);
    if auto_load {
        let config_path = target_dir.join("config.yaml");
        let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
        state.config.apply_model_defaults(&mut config);
        if let Err(e) = state.worker_manager.load_model(&model_name, &version, &config).await {
            warn!("Auto-load after upload failed: {}", e);
        }
        let active = state.registry.get_active_version(&model_name);
        if active.is_none() {
            let _ = state.registry.activate_version(&model_name, &version);
        }
    }

    info!(
        model = %model_name,
        version = %version,
        files = ?uploaded_files,
        "Model uploaded"
    );

    Ok(Json(json!({
        "success": true,
        "model": model_name,
        "version": version,
        "files": uploaded_files,
        "loaded": auto_load,
    })))
}

// ===== Download Model =====

#[derive(Deserialize)]
pub struct DownloadQuery {
    pub file: Option<String>,
}

pub async fn download_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiQuery(query): ApiQuery<DownloadQuery>,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;

    if !model_dir.exists() {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    // Single file download
    if let Some(ref file_name) = query.file {
        // Validate file name doesn't contain path separators
        if file_name.contains("..") || file_name.contains('/') || file_name.contains('\\') {
            return Err(AppError::Validation("invalid file name".to_string()));
        }
        let file_path = model_dir.join(file_name);
        // Ensure resolved path is inside model_dir
        let canonical_file = file_path.canonicalize().map_err(AppError::Io)?;
        let canonical_dir = model_dir.canonicalize().map_err(AppError::Io)?;
        if !canonical_file.starts_with(&canonical_dir) {
            return Err(AppError::Validation("path traversal rejected".to_string()));
        }

        let data = tokio::fs::read(&canonical_file)
            .await
            .map_err(AppError::Io)?;
        let content_type = if file_name.ends_with(".py") || file_name.ends_with(".yaml") || file_name.ends_with(".yml") || file_name.ends_with(".json") || file_name.ends_with(".txt") || file_name.ends_with(".md") {
            "text/plain; charset=utf-8"
        } else {
            "application/octet-stream"
        };

        let response = Response::builder()
            .header(CONTENT_TYPE, content_type)
            .header(
                CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", file_name),
            )
            .body(axum::body::Body::from(data))
            .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
        return Ok(response);
    }

    // Full directory download as .lma
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-download-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&tmp_dir)
        .await
        .map_err(AppError::Io)?;

    let output = tokio::process::Command::new("python")
        .args([
            "-m",
            "lite_server",
            "pack",
            model_dir.to_str().unwrap_or(""),
            "--version",
            &version,
            "--output",
            tmp_dir.to_str().unwrap_or(""),
        ])
        .output()
        .await
        .map_err(|e| AppError::Internal(format!("failed to run python pack: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        return Err(AppError::Internal(format!("pack failed: {}", stderr.trim())));
    }

    // Find the generated .lma file
    let mut lma_file = None;
    let mut entries = tokio::fs::read_dir(&tmp_dir)
        .await
        .map_err(AppError::Io)?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.path().extension().map(|e| e == "lma").unwrap_or(false) {
            lma_file = Some(entry.path());
            break;
        }
    }

    let lma_path = lma_file.ok_or_else(|| {
        AppError::Internal("pack produced no .lma file".to_string())
    })?;

    let data = tokio::fs::read(&lma_path)
        .await
        .map_err(AppError::Io)?;
    let artifact_name = lma_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Clean up temp dir
    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;

    let response = Response::builder()
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", artifact_name),
        )
        .body(axum::body::Body::from(data))
        .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
    Ok(response)
}

// ===== List Files =====

pub async fn list_files_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;

    if !model_dir.exists() {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&model_dir)
        .await
        .map_err(AppError::Io)?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let metadata = entry.metadata().await.ok();
        let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let modified = metadata
            .as_ref()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());

        files.push(json!({
            "name": name,
            "size": size,
            "modified": modified,
            "is_dir": path.is_dir(),
        }));
    }

    Ok(Json(json!({
        "model": model_name,
        "version": version,
        "files": files,
    })))
}

#[cfg(test)]
mod upload_download_tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    fn test_app_state(repo_path: std::path::PathBuf) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path.clone(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            Config::default(),
            repo_path,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/v2/repository/models/:model_name/versions/:version/upload",
                axum::routing::post(upload_model_handler),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/download",
                axum::routing::get(download_model_handler),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/files",
                axum::routing::get(list_files_handler),
            )
            .with_state(state)
    }

    // ===== List Files Tests =====

    #[tokio::test]
    async fn test_list_files_returns_directory_contents() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-list-test-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "print('hello')")
            .await
            .unwrap();
        tokio::fs::write(model_dir.join("config.yaml"), "max_batch_size: 1")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["model"], "mymodel");
        assert_eq!(json["version"], "1");
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);

        let names: Vec<&str> = files.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"model.py"));
        assert!(names.contains(&"config.yaml"));

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_list_files_returns_404_for_missing_model() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-list-404-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/nonexistent/versions/1/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== Download Single File Tests =====

    #[tokio::test]
    async fn test_download_single_file() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-test-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=model.py")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let disposition = response
            .headers()
            .get(CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(disposition.contains("model.py"));

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"def predict(x): return x");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_download_rejects_path_traversal() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-traversal-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "test")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=../../../etc/passwd")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== Upload Tests =====

    #[tokio::test]
    async fn test_upload_raw_file() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-test-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        // Build multipart body manually
        let boundary = "----testboundary123";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"model.py\"\r\n\
             Content-Type: text/x-python\r\n\r\n\
             def predict(x): return x\r\n\
             --{boundary}--\r\n"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["model"], "mymodel");
        assert_eq!(json["version"], "1");
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "model.py");

        // Verify file was written
        let content = tokio::fs::read_to_string(tmp.join("mymodel").join("1").join("model.py"))
            .await
            .unwrap();
        assert_eq!(content, "def predict(x): return x");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_upload_rejects_invalid_model_name() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-invalid-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----testboundary123";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"model.py\"\r\n\r\n\
             test\r\n\
             --{boundary}--\r\n"
        );

        // Use a model name with invalid characters (space) that still matches the route
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/bad%20name/versions/1/upload")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}


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
mod route_match_tests {
    use super::*;

    #[test]
    fn test_param_name() {
        assert_eq!(param_name("{id}"), Some("id"));
        assert_eq!(param_name(":id"), Some("id"));
        assert_eq!(param_name("{oid}"), Some("oid"));
        assert_eq!(param_name("pets"), None);
        assert_eq!(param_name("status"), None);
        assert_eq!(param_name("{}"), None); // empty name → literal
        assert_eq!(param_name(":"), None);
    }

    #[test]
    fn test_parse_route_tail_bare() {
        assert_eq!(parse_route_tail("status"), (None, "status".to_string()));
        assert_eq!(parse_route_tail("pets/123"), (None, "pets/123".to_string()));
        assert_eq!(parse_route_tail("/status"), (None, "status".to_string())); // leading slash tolerated
    }

    #[test]
    fn test_parse_route_tail_versioned() {
        assert_eq!(
            parse_route_tail("versions/v2/status"),
            (Some("v2".to_string()), "status".to_string())
        );
        assert_eq!(
            parse_route_tail("versions/v2/pets/123"),
            (Some("v2".to_string()), "pets/123".to_string())
        );
        // versions/<v> with nothing after → empty route tail (will 404)
        assert_eq!(
            parse_route_tail("versions/v2"),
            (Some("v2".to_string()), "".to_string())
        );
    }

    fn decl(route: &str, methods: &[&str]) -> RouteDecl {
        RouteDecl {
            route: route.to_string(),
            methods: methods.iter().map(|m| m.to_string()).collect(),
        }
    }

    #[test]
    fn test_match_route_literal_hit() {
        let routes = vec![decl("/status", &["GET"])];
        match match_route(&routes, "status", "GET") {
            RouteMatch::Hit { pattern, path_params } => {
                assert_eq!(pattern, "/status");
                assert!(path_params.is_empty());
            }
            other => panic!("expected Hit, got {:?}", other),
        }
    }

    #[test]
    fn test_match_route_path_params() {
        let routes = vec![decl("/pets/{id}", &["GET"])];
        match match_route(&routes, "pets/123", "GET") {
            RouteMatch::Hit { pattern, path_params } => {
                assert_eq!(pattern, "/pets/{id}");
                assert_eq!(path_params.get("id"), Some(&"123".to_string()));
            }
            other => panic!("expected Hit, got {:?}", other),
        }
    }

    #[test]
    fn test_match_route_colon_params() {
        // :param syntax is also accepted (lenient)
        let routes = vec![decl("/pets/:id", &["GET"])];
        assert!(matches!(
            match_route(&routes, "pets/9", "GET"),
            RouteMatch::Hit { .. }
        ));
    }

    #[test]
    fn test_match_route_method_not_allowed() {
        let routes = vec![decl("/status", &["GET"])];
        // pattern matches, method does not → 405 (not 404)
        assert!(matches!(
            match_route(&routes, "status", "POST"),
            RouteMatch::MethodNotAllowed
        ));
    }

    #[test]
    fn test_match_route_not_found() {
        let routes = vec![decl("/status", &["GET"])];
        assert!(matches!(match_route(&routes, "nope", "GET"), RouteMatch::NotFound));
        // wrong segment count
        assert!(matches!(
            match_route(&routes, "status/extra", "GET"),
            RouteMatch::NotFound
        ));
    }

    #[test]
    fn test_match_route_multi_params_and_methods() {
        let routes = vec![decl("/a/{x}/b/{y}", &["GET", "POST"])];
        match match_route(&routes, "a/1/b/2", "POST") {
            RouteMatch::Hit { path_params, .. } => {
                assert_eq!(path_params.get("x"), Some(&"1".to_string()));
                assert_eq!(path_params.get("y"), Some(&"2".to_string()));
            }
            other => panic!("expected Hit, got {:?}", other),
        }
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

    // --- extract_client_ip: header-based resolution (peer fallback is the
    //     peer_ip_fallback layer's job, tested below) ---

    #[test]
    fn prefers_x_forwarded_for() {
        let h = headers(&[("x-forwarded-for", "10.0.0.1"), ("x-real-ip", "10.0.0.2")]);
        assert_eq!(extract_client_ip(&h), "10.0.0.1");
    }

    #[test]
    fn uses_x_real_ip_when_no_xff() {
        let h = headers(&[("x-real-ip", "10.0.0.2")]);
        assert_eq!(extract_client_ip(&h), "10.0.0.2");
    }

    #[test]
    fn empty_when_no_headers() {
        assert_eq!(extract_client_ip(&headers(&[])), "");
    }

    #[test]
    fn skips_empty_xff_and_uses_real_ip() {
        // An empty x-forwarded-for must fall through (parity with gRPC),
        // so the layer-injected x-real-ip can engage for direct connections.
        let h = headers(&[("x-forwarded-for", ""), ("x-real-ip", "10.0.0.2")]);
        assert_eq!(extract_client_ip(&h), "10.0.0.2");
    }

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
mod version_routing_tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::registry::types::ModelType;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use axum::http::{HeaderName, HeaderValue, Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            Config::default(),
            std::path::PathBuf::new(),
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    fn register_ready(state: &AppState, model: &str, versions: &[&str]) {
        for v in versions {
            state
                .registry
                .register(model, v, Default::default(), ModelType::LitAPI, std::path::PathBuf::new())
                .unwrap();
            state.registry.mark_ready(model, v).unwrap();
        }
    }

    fn pinned_header(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-lite-version"),
            HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[tokio::test]
    async fn explicit_version_wins_over_header_and_weights() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        let v = resolve_version(&state, "m", Some("1".into()), &pinned_header("2"))
            .await
            .unwrap();
        assert_eq!(v, "1");
    }

    #[tokio::test]
    async fn header_pin_wins_over_weights() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        let v = resolve_version(&state, "m", None, &pinned_header("1"))
            .await
            .unwrap();
        assert_eq!(v, "1");
    }

    #[tokio::test]
    async fn weights_pick_when_no_explicit_or_header() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        let v = resolve_version(&state, "m", None, &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(v, "2");
    }

    #[tokio::test]
    async fn active_fallback_when_no_weights() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();
        let v = resolve_version(&state, "m", None, &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(v, "1");
    }

    #[tokio::test]
    async fn no_active_no_weights_is_not_found() {
        let state = test_state();
        register_ready(&state, "m", &["1"]);
        let err = resolve_version(&state, "m", None, &HeaderMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::ModelNotFound(_)), "got {err:?}");
    }

    // ===== B4: x-lite-version header must pass validate_version =====

    /// Regression for B4: the header pin used to reach downstream lookups
    /// unvalidated, while versioned URL paths are guarded by
    /// `validate_version`. Invalid header values are rejected (400), same
    /// as invalid path versions. (Values with control chars can't appear
    /// in a HeaderValue at all, so only representable cases are tested.)
    #[tokio::test]
    async fn invalid_header_pin_is_rejected() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();

        for bad in ["a/b", "a b", "a..b", ".hidden", "trailing.", &"x".repeat(65)] {
            let err = resolve_version(&state, "m", None, &pinned_header(bad))
                .await
                .unwrap_err();
            assert!(
                matches!(err, AppError::Validation(_)),
                "header pin {bad:?} must be rejected, got {err:?}"
            );
        }

        // Sanity: a valid pin still resolves.
        let v = resolve_version(&state, "m", None, &pinned_header("2"))
            .await
            .unwrap();
        assert_eq!(v, "2");
    }

    // ===== PUT /v2/models/:m/routing (§4.3) =====

    fn routing_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v2/models/:model_name/routing", axum::routing::put(set_routing_handler))
            .with_state(state)
    }

    async fn put_routing(app: Router, model: &str, body: &str) -> axum::response::Response {
        app.oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/models/{}/routing", model))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn put_routing_sets_weights_atomically() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);

        let resp = put_routing(routing_router(state.clone()), "m", r#"{"weights":{"1":90,"2":10}}"#).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 90);
        assert_eq!(state.registry.get("m", Some("2")).unwrap().weight, 10);

        // Atomic full-set: unlisted versions are zeroed.
        let resp = put_routing(routing_router(state.clone()), "m", r#"{"weights":{"2":50}}"#).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 0);
        assert_eq!(state.registry.get("m", Some("2")).unwrap().weight, 50);
    }

    #[tokio::test]
    async fn put_routing_unknown_version_is_400_and_untouched() {
        let state = test_state();
        register_ready(&state, "m", &["1"]);

        let resp = put_routing(routing_router(state.clone()), "m", r#"{"weights":{"nope":100}}"#).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 0);

        // Unknown model → 404.
        let resp = put_routing(routing_router(state.clone()), "nope", r#"{"weights":{}}"#).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn activate_hard_switches_weights() {
        // Explicit activate = hard cutover (§4.3): target gets weight 100,
        // every other version 0.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions/:version/activate",
                axum::routing::post(activate_version_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/models/m/versions/2/activate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.registry.get_active_version("m").as_deref(), Some("2"));
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 0);
        assert_eq!(state.registry.get("m", Some("2")).unwrap().weight, 100);
    }

    // ===== §4.4: bare vs versioned resolution =====

    #[tokio::test]
    async fn bare_unload_targets_active_not_weighted_pick() {
        // Admin ops on the bare path always target the active version (§4.4
        // decision) — never the routing pick, even at 100% weight.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/unload",
                axum::routing::post(unload_model_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/m/unload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(state.registry.get("m", Some("1")).is_none(), "active v1 must be unloaded");
        assert!(state.registry.get("m", Some("2")).is_some(), "weighted v2 must be untouched");
    }

    #[tokio::test]
    async fn versioned_unload_targets_explicit_version() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/versions/:version/unload",
                axum::routing::post(unload_version_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/m/versions/2/unload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(state.registry.get("m", Some("1")).is_some(), "active v1 untouched");
        assert!(state.registry.get("m", Some("2")).is_none(), "explicit v2 unloaded");
    }

    #[tokio::test]
    async fn bare_health_uses_routing_pick() {
        // Traffic-facing bare endpoints resolve via routing (§4.3) — unlike
        // admin ops.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route("/v2/models/:model_name/health", axum::routing::get(model_health_handler))
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/v2/models/m/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["version"], "2", "bare health must follow the routing pick");
    }

    #[tokio::test]
    async fn versioned_ready_reports_explicit_version() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "2").unwrap();

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions/:version/ready",
                axum::routing::get(model_ready_version_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/v2/models/m/versions/1/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["version"], "1");
        assert_eq!(json["ready"], true);
        assert_eq!(json["active_version"], "2");
    }

    #[tokio::test]
    async fn bare_timeline_defaults_to_active_not_1() {
        // The old default was the literal string "1" regardless of what is
        // actually active (§4.0 bug list).
        let state = test_state();
        register_ready(&state, "m", &["2"]);
        state.registry.activate_version("m", "2").unwrap();

        let app = Router::new()
            .route("/metrics/timeline/:model_name", axum::routing::get(timeline_model_handler))
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/metrics/timeline/m").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["version"], "2", "bare timeline must default to the active version");
    }

    // ===== §4.5: multi-version health =====

    #[tokio::test]
    async fn versions_endpoint_returns_multi_version_overview() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();
        state
            .registry
            .set_weights("m", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions",
                axum::routing::get(list_versions_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/v2/models/m/versions").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["name"], "m");
        assert_eq!(json["active_version"], "1");
        let versions = json["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);

        let v1 = versions.iter().find(|v| v["version"] == "1").unwrap();
        assert_eq!(v1["active"], true);
        assert_eq!(v1["status"], "ready");
        assert_eq!(v1["weight"], 90);
        assert_eq!(v1["workers"]["total"], 0);
        assert_eq!(v1["workers"]["ready"], 0);
        assert!(v1["loaded_at"].as_u64().is_some(), "loaded_at must be epoch secs");

        let v2 = versions.iter().find(|v| v["version"] == "2").unwrap();
        assert_eq!(v2["active"], false);
        assert_eq!(v2["weight"], 10);
    }

    #[tokio::test]
    async fn server_health_groups_versions_by_model() {
        // §4.5: /health nests per-version entries under their model with the
        // active_version pointer.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "2").unwrap();

        let app = Router::new()
            .route("/health", axum::routing::get(health_handler))
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "ready");
        let models = json["models"].as_array().unwrap();
        assert_eq!(models.len(), 1, "one model groups both versions");
        let m = &models[0];
        assert_eq!(m["name"], "m");
        assert_eq!(m["active_version"], "2");
        let versions = m["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions.iter().all(|v| v["status"] == "ready"));
        assert!(versions.iter().all(|v| v["loaded_at"].as_u64().is_some()));
        assert!(m.get("version").is_none(), "flat per-version fields must be gone");
    }

    #[tokio::test]
    async fn versioned_options_uses_hit_version_cors_policy() {
        // §4.4: a versioned route's OPTIONS preflight must answer with that
        // version's CORS policy, not the active version's.
        use crate::config::{CorsPolicy, ModelPolicies};
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();
        let policies = |origin: &str| ModelPolicies {
            cors: Some(CorsPolicy {
                allow_origins: vec![origin.to_string()],
                allow_methods: vec!["POST".to_string()],
                allow_headers: vec!["content-type".to_string()],
            }),
            ..Default::default()
        };
        state.registry.set_policies("m", "1", Some(policies("https://v1.example")));
        state.registry.set_policies("m", "2", Some(policies("https://v2.example")));

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions/:version/infer",
                axum::routing::post(infer_version_handler).options(inference_options_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/v2/models/m/versions/2/infer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "https://v2.example",
            "versioned OPTIONS must use the hit version's policy"
        );
    }

    // ===== B1: WS readiness must check the resolved version =====

    /// Regression for B1: `handle_ws_stream` used to gate on the ACTIVE
    /// version's readiness (`is_ready(model, None)`), so a WS pinned via
    /// `x-lite-version` to a Ready non-active version was closed whenever
    /// the active version was not Ready.
    #[tokio::test]
    async fn ws_stream_readiness_uses_resolved_version_not_active() {
        use futures::StreamExt;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let state = test_state();
        // v1 = active but Failed; v2 = Ready (the pin target).
        state
            .registry
            .register("m", "1", Default::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        state
            .registry
            .set_status("m", "1", crate::registry::types::VersionStatus::Failed)
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();
        register_ready(&state, "m", &["2"]);

        let app = Router::new()
            .route(
                "/v2/models/:model_name/stream",
                axum::routing::get(ws_stream_handler),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("ws://{addr}/v2/models/m/stream");
        let mut req = url.into_client_request().unwrap();
        req.headers_mut().insert("x-lite-version", "2".parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req)
            .await
            .expect("WS connect failed");

        // The pinned v2 is Ready, so the handler must be waiting for the
        // first client message — NOT closing. Receiving a close frame here
        // means the readiness gate still checks the active (Failed) version.
        let first = tokio::time::timeout(std::time::Duration::from_millis(500), ws.next()).await;
        assert!(
            first.is_err(),
            "server closed a WS pinned to a Ready version — readiness gate used the active version, got {first:?}"
        );
    }
}

// ===== B3: custom-route callback gap =====

#[cfg(test)]
mod custom_route_callback_tests {
    /// B3 (P1): `dispatch_custom_route` does not fire `on_inference_request`
    /// or `on_inference_response` callbacks, unlike the inference paths
    /// (`do_infer`, `sse_infer_impl`, `handle_ws_stream`).
    ///
    /// The spec states model-level callbacks should cover both inference and
    /// custom routes. Inference paths fire `on_inference_request` before
    /// queueing and `on_inference_response` after; `dispatch_custom_route`
    /// does neither — the callback runner is entirely silent for routes.
    ///
    /// This structural test verifies the source-level gap: the callback
    /// invocations exist in `do_infer` but not in `dispatch_custom_route`.
    #[test]
    fn test_dispatch_custom_route_does_not_fire_inference_callbacks() {
        let source = include_str!("mod.rs");

        // Find the dispatch_custom_route function boundaries.
        let lines: Vec<&str> = source.lines().collect();
        let fn_start = lines
            .iter()
            .position(|l| l.contains("pub async fn dispatch_custom_route("))
            .expect("dispatch_custom_route must exist");

        // Find the matching closing brace (heuristic: next line starting
        // with `^}` at the same indent as `pub async fn`).
        let mut fn_end = fn_start;
        let mut depth = 0i32;
        let mut started = false;
        for (i, line) in lines.iter().enumerate().skip(fn_start) {
            if line.contains('{') {
                depth += line.matches('{').count() as i32;
                started = true;
            }
            if line.contains('}') {
                depth -= line.matches('}').count() as i32;
            }
            if started && depth == 0 {
                fn_end = i;
                break;
            }
        }

        let fn_body: Vec<&&str> = lines[fn_start..=fn_end].iter().collect();

        // The inference path calls on_inference_request (in do_infer).
        let has_inference_request = source.contains("on_inference_request");
        assert!(
            has_inference_request,
            "sanity: handlers.rs must reference on_inference_request somewhere"
        );

        let fn_has_req_cb = fn_body
            .iter()
            .any(|l| l.contains("on_inference_request"));
        let fn_has_resp_cb = fn_body
            .iter()
            .any(|l| l.contains("on_inference_response"));

        // B3: The defect — dispatch_custom_route does NOT fire inference
        // callbacks (the spec says model-level callbacks cover both inference
        // and custom routes). These assertions FAIL against current code.
        // When fixed, they will pass.
        assert!(
            fn_has_req_cb,
            "B3: dispatch_custom_route must fire on_inference_request \
             callback. Currently it does not — only do_infer fires it."
        );

        assert!(
            fn_has_resp_cb,
            "B3: dispatch_custom_route must fire on_inference_response \
             callback. Currently it does not — only do_infer fires it."
        );

        // Counter-check: do_infer DOES call both (verify test methodology).
        let do_infer_start = lines
            .iter()
            .position(|l| l.contains("async fn do_infer("))
            .expect("do_infer must exist");
        let mut do_infer_end = do_infer_start;
        let mut depth = 0i32;
        let mut started = false;
        for (i, line) in lines.iter().enumerate().skip(do_infer_start) {
            if line.contains('{') {
                depth += line.matches('{').count() as i32;
                started = true;
            }
            if line.contains('}') {
                depth -= line.matches('}').count() as i32;
            }
            if started && depth == 0 {
                do_infer_end = i;
                break;
            }
        }
        let do_infer_body: Vec<&&str> =
            lines[do_infer_start..=do_infer_end].iter().collect();
        assert!(
            do_infer_body.iter().any(|l| l.contains("on_inference_request")),
            "do_infer must fire on_inference_request (sanity check)"
        );
        assert!(
            do_infer_body.iter().any(|l| l.contains("on_inference_response")),
            "do_infer must fire on_inference_response (sanity check)"
        );
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
