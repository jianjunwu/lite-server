use super::*;
use super::inference::{build_request_meta, resolve_version};
use crate::error::AppError;
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::request_context::RequestContext;
use crate::worker::protocol::RouteDecl;
use axum::http::HeaderMap;
use axum::response::Response;
use serde_json::json;
use std::collections::HashMap;
use std::convert::Infallible;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::warn;
use uuid::Uuid;

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

/// Phase-1 result of custom-route dispatch: version resolved and tail
/// matched against the version's declared `@route`s.
#[derive(Debug)]
pub struct ResolvedCustomRoute {
    pub resolved_version: String,
    /// The declared route pattern (sent to the worker as `meta.route`).
    pub route_pattern: String,
    pub path_params: HashMap<String, String>,
}

/// Phase 1 of custom-route dispatch (F-06): validate model/tail, resolve the
/// version, and match the tail against the declared `@route`s — everything
/// that must happen BEFORE the request body is read, so an unmatched or
/// oversized request gets 404/405/413 without paying for (or failing on)
/// the body read.
pub async fn resolve_custom_route(
    state: &AppState,
    model_name: &str,
    tail: &str,
    method: &axum::http::Method,
    headers: &HeaderMap,
) -> Result<ResolvedCustomRoute, AppError> {
    crate::validation::validate_identifier(model_name)?;
    let (version, route_tail) = parse_route_tail(tail);
    if let Some(ref v) = version {
        crate::validation::validate_version(v)?;
    }
    let (resolved_version, _) = resolve_version(state, model_name, version, headers).await?;

    if !state.registry.is_ready(model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }

    // Match against this version's declared routes.
    let routes = state.worker_manager.get_routes(model_name, &resolved_version).await;
    match match_route(&routes, &route_tail, method.as_str()) {
        RouteMatch::Hit { pattern, path_params } => Ok(ResolvedCustomRoute {
            resolved_version,
            route_pattern: pattern,
            path_params,
        }),
        RouteMatch::MethodNotAllowed => Err(AppError::MethodNotAllowed),
        RouteMatch::NotFound => Err(AppError::RouteNotFound),
    }
}

/// Dispatch a custom-route request for `/v2/models/<model>/<tail>` to a worker.
/// Called by the fallback handler (`http::route_fallback`) after
/// [`resolve_custom_route`] matched the path (pre-body) and the body was
/// read. Dispatches over ZMQ (bypassing the batch InferenceQueue — route
/// calls are not aggregatable). System leaves (`infer`/`events`/`stream`/...)
/// are exact-registered and matched by axum, so they never reach the fallback.
// allow: 单调用点的 HTTP 请求分解(method/query/headers/body/cx),参数即
// 请求形状本身,无收编语义。
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_custom_route(
    state: &AppState,
    model_name: &str,
    resolved: ResolvedCustomRoute,
    method: &axum::http::Method,
    query: HashMap<String, String>,
    headers: &HeaderMap,
    body: bytes::Bytes,
    cx: &RequestContext,
    admission_slot: crate::admission::AdmissionSlot,
) -> Result<Response, AppError> {
    let method_str = method.as_str();
    let ResolvedCustomRoute {
        resolved_version,
        route_pattern,
        path_params,
    } = resolved;

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
        request_id: cx.request_id.clone(),
        client_ip: cx.client_ip.clone(),
        elapsed_us: None,
    };
    crate::callback::fire_inference_request(&state.callback_runner, &req_ctx);

    // Build the request: route_call reuses the SingleRequest body type; the
    // route tag discriminates dispatch in the worker. method/query/path_params
    // ride on RequestMeta (route_pattern == meta.route).
    let deadline = crate::deadline::resolve_from_http(headers, state.config.server.timeout);
    let mut meta = build_request_meta(headers, bytes::Bytes::from_static(b"null"), &route_pattern, cx, deadline.unix_ns);
    meta.method = method_str.to_string();
    meta.query = query;
    meta.path_params = path_params;
    // P-DEADLINE (方案 C): overall deadline client-specified only; chunk-idle
    // reclaim always on (decoupled parity) for the route response body.
    let stream_deadline = if deadline.client_specified {
        crate::deadline::to_instant(deadline.unix_ns)
    } else {
        None
    };
    let stream_idle = crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs);

    let uid = format!("route_{}_{}-{}", model_name, resolved_version, Uuid::new_v4());
    let request = pb::Request {
        uid,
        meta: Some(meta),
        payload: Some(pb::request::Payload::RouteCall(pb::SingleRequest { data: body })),
    };

    let (resp_rx, mut chunk_rx) = client.send_route_or_stream(request).await?;

    let first = tokio::time::timeout(
        ROUTE_FIRST_FRAME_TIMEOUT,
        first_route_reply(resp_rx, &mut chunk_rx),
    )
    .await
    .map_err(|_| AppError::InferenceTimeout("route response timeout".to_string()))?;

    match first {
        RouteReply::Unary(Some(response)) => match response.payload {
            // Decode the SingleResponse (routes reuse the inference response shape).
            Some(pb::response::Payload::Single(single)) => {
                let r = build_route_http_response(single);
                // Unary route: the response is complete when the Single reply
                // arrives — fire InferenceResponse with the full duration.
                if r.is_ok() {
                    crate::callback::fire_inference_response(
                        &state.callback_runner, &req_ctx, start,
                    );
                }
                r
            }
            _ => Err(AppError::WorkerCrashed("unexpected response type".to_string())),
        },
        RouteReply::Unary(None) => Err(AppError::WorkerCrashed(
            "route reply channel closed".to_string(),
        )),
        RouteReply::Stream(frame) => match frame.payload {
            Some(pb::stream_response::Payload::Start(start_frame)) => {
                // Stream route: InferenceResponse fires on the terminal frame
                // inside the body (aligned with SSE/WS), not here at Start.
                build_route_stream_http_response(
                    start_frame,
                    chunk_rx,
                    stream_deadline,
                    stream_idle,
                    state.callback_runner.clone(),
                    req_ctx.clone(),
                    start,
                    // RN-13 (D9-A): the admission guard rides the response
                    // body — the slot is released when the stream ends.
                    admission_slot.take(),
                )
            }
            _ => Err(AppError::WorkerCrashed(
                "route stream missing start frame".to_string(),
            )),
        },
    }
}

/// Bound on waiting for a route call's first reply frame (unary or stream
/// start). Mirrors the transport's unary response timeout.
const ROUTE_FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(60);

/// The handler picks the reply shape: a plain/Response result answers with
/// one SingleResponse; a StreamingResponse result answers with a
/// start→chunks→done frame sequence. Await the first arrival, bounded by
/// the caller like a unary send so a dead worker can't hang the request.
enum RouteReply {
    Unary(Option<pb::Response>),
    Stream(pb::StreamResponse),
}

/// Race the first arrival of the unary reply vs. the first stream frame.
///
/// `biased` polls the chunk channel FIRST: when the whole frame sequence
/// (start→…→done) lands before this task is scheduled, the actor has
/// already dropped the unary sender (freed on stream done), making both
/// arms ready — an unbiased select! would flip a coin and return
/// Unary(None) → spurious WorkerCrashed 500. With biased, buffered frames
/// always win; the unary arm only fires when no frame ever arrived (the
/// unary reply path, where delivering it frees the unused stream route and
/// closes the chunk channel with nothing sent).
async fn first_route_reply(
    resp_rx: tokio::sync::oneshot::Receiver<pb::Response>,
    chunk_rx: &mut mpsc::Receiver<pb::StreamResponse>,
) -> RouteReply {
    let mut resp_rx = resp_rx;
    tokio::select! {
        biased;
        frame = chunk_rx.recv() => match frame {
            Some(f) => RouteReply::Stream(f),
            None => RouteReply::Unary((&mut resp_rx).await.ok()),
        },
        unary = &mut resp_rx => RouteReply::Unary(unary.ok()),
    }
}

/// Build the HTTP response for a streaming route reply. The `start` frame
/// carries the handler-chosen status/headers/media_type; subsequent `chunk`
/// frames form the body. For `text/event-stream` each chunk is framed as one
/// SSE event; other media types pass chunk bytes through verbatim. A
/// mid-stream `error` frame ends the body (as a final SSE event in SSE
/// mode), since the status line is already on the wire.
fn build_route_stream_http_response(
    start: pb::StreamStart,
    chunk_rx: mpsc::Receiver<pb::StreamResponse>,
    deadline: Option<std::time::Instant>,
    idle: Option<std::time::Duration>,
    callback_runner: std::sync::Arc<crate::callback::CallbackRunner>,
    req_ctx: crate::callback::InferenceContext,
    open_time: std::time::Instant,
    admission_guard: Option<crate::admission::AdmissionGuard>,
) -> Result<Response, AppError> {
    let is_sse = start.media_type.starts_with("text/event-stream");
    let content_type = if start.media_type.is_empty() {
        "text/event-stream"
    } else {
        start.media_type.as_str()
    };
    // Thread the callback context through the unfold state so the terminal
    // frame fires InferenceResponse exactly once with the FULL open→terminal
    // elapsed — aligned with SSE/WS. Firing on the Start frame (the old
    // behavior) reported only cold-start latency and fired before the body
    // had streamed.
    let body = futures::stream::unfold(
        (chunk_rx, false, callback_runner, req_ctx, open_time),
        move |(mut rx, ended, cb, ctx, t0)| {
            // RN-13 (D9-A): force-capture the admission guard into this
            // closure's environment — the unfold stream owns the closure, so
            // the slot is released exactly when the body stream ends/drops.
            let _hold = &admission_guard;
            async move {
            if ended {
                return None;
            }
            // P-DEADLINE (§4.0.4): overall deadline / chunk-idle bound this recv.
            let frame = match crate::streaming::recv_chunk(&mut rx, deadline, idle).await {
                Ok(Some(f)) => Some(f),
                Ok(None) => None, // worker closed the stream
                Err(elapsed) => {
                    tracing::warn!(?elapsed, "route stream closed: deadline/idle elapsed");
                    None
                }
            };
            match frame.map(|f| f.payload) {
                Some(Some(pb::stream_response::Payload::Chunk(c))) => {
                    let data = if is_sse { sse_frame(&c.data) } else { c.data.to_vec() };
                    Some((Ok::<_, Infallible>(bytes::Bytes::from(data)), (rx, false, cb, ctx, t0)))
                }
                Some(Some(pb::stream_response::Payload::Error(e))) => {
                    warn!("route stream error: {}", e.message);
                    // Terminal frame → fire the response callback (full elapsed).
                    crate::callback::fire_inference_response(&cb, &ctx, t0);
                    if is_sse {
                        let event = sse_frame(e.message.as_bytes());
                        Some((Ok(bytes::Bytes::from(event)), (rx, true, cb, ctx, t0)))
                    } else {
                        None
                    }
                }
                // Done, channel closed, or an unexpected frame — terminal.
                _ => {
                    crate::callback::fire_inference_response(&cb, &ctx, t0);
                    None
                }
            }
            }
        },
    );
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
#[cfg(test)]
mod route_match_tests {
    use super::*;

    fn stream_start_frame(sid: &str) -> pb::StreamResponse {
        pb::StreamResponse {
            stream_id: sid.to_string(),
            payload: Some(pb::stream_response::Payload::Start(pb::StreamStart {
                status_code: 200,
                media_type: "text/event-stream".to_string(),
                headers: HashMap::new(),
            })),
        }
    }

    /// Race regression: the whole start→chunks→done sequence lands before
    /// the HTTP task is scheduled. The actor freed the unary slot on stream
    /// done (sender dropped) AND frames are buffered — both select arms are
    /// ready. Buffered frames must always win; Unary(None) here becomes a
    /// spurious WorkerCrashed 500 (seen on ubuntu CI, 0.7.7 tag run).
    #[tokio::test]
    async fn first_route_reply_prefers_buffered_frames_over_closed_unary() {
        for _ in 0..20 {
            let (unary_tx, unary_rx) = tokio::sync::oneshot::channel::<pb::Response>();
            let (chunk_tx, mut chunk_rx) = mpsc::channel(4);
            chunk_tx.send(stream_start_frame("s")).await.unwrap();
            drop(chunk_tx); // whole sequence delivered, channel closed
            drop(unary_tx); // actor freed the unary slot on stream done
            match first_route_reply(unary_rx, &mut chunk_rx).await {
                RouteReply::Stream(_) => {}
                RouteReply::Unary(_) => {
                    panic!("buffered stream frames must win over the closed unary slot")
                }
            }
        }
    }

    /// Unary reply path: delivering it frees the unused stream route,
    /// closing the chunk channel with no frames sent. The unary reply must
    /// be returned, not treated as a stream failure.
    #[tokio::test]
    async fn first_route_reply_falls_back_to_unary_when_no_frames() {
        let (unary_tx, unary_rx) = tokio::sync::oneshot::channel::<pb::Response>();
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<pb::StreamResponse>(1);
        drop(chunk_tx);
        unary_tx.send(pb::Response::default()).unwrap();
        match first_route_reply(unary_rx, &mut chunk_rx).await {
            RouteReply::Unary(Some(_)) => {}
            _ => panic!("expected the unary reply"),
        }
    }

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
