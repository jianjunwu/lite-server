pub mod cors;
pub mod handlers;
pub mod kserve;
pub mod routes;
pub mod state;

use crate::config::{unix_socket_path, Config};
use crate::error::{AppError, ProtocolError};
use crate::http::routes::create_routes;
use crate::http::state::AppState;
use crate::inference_queue::InferenceQueue;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use axum::extract::{FromRequestParts, Query, Request, State};
use axum::http::header::CONNECTION;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, Instrument};
use uuid::Uuid;

async fn disable_keepalive_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    // F-04: never rewrite the Connection header on a 101 upgrade — replacing
    // `Connection: Upgrade` with `close` breaks the WS handshake (RFC 6455
    // §4.2.2 requires the Upgrade token in the 101 response).
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        response.headers_mut().insert(
            CONNECTION,
            axum::http::HeaderValue::from_static("close"),
        );
    }
    response
}

/// L2 (resource-leak-plan): request-body idle timeout (slowloris-body guard).
/// tower-http's TimeoutBody is an IDLE timeout — the timer resets on every
/// body frame, so large uploads are unaffected while bytes flow; a stalled
/// body surfaces TimeoutError to the extractor (4xx-class response). h2
/// `/bidi` routes are exempt: their request body IS the chunk stream and
/// idle gaps between frames are legal there.
async fn request_body_timeout_middleware(
    State(timeout): State<std::time::Duration>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path().ends_with("/bidi") {
        return next.run(request).await;
    }
    let request = request.map(|body| {
        axum::body::Body::new(tower_http::timeout::TimeoutBody::new(timeout, body))
    });
    next.run(request).await
}

/// Newtype for the request ID stash in request extensions, written once by
/// `observability_middleware` (outermost) and read by `context_middleware`
/// when it builds the `RequestContext` (P-MW, D21 single-source).
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Fallback for unmatched routes. A `/v2/models/<model>/<tail>` path that no
/// exact route matched is dispatched as a custom `@route` (phase 2); everything
/// else returns the standardized 404. (A catch-all `/{*tail}` route is rejected
/// by matchit because `:model_name` already has deeper registered children, so
/// custom-route dispatch rides the fallback instead.)
pub(crate) async fn route_fallback(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    // F-17: render errors in the request's protocol — a KServe V2 dataplane
    // URL (/v2/...) must get the flat `{"error": "<message>"}` body, not the
    // nested Legacy shape Triton clients don't parse.
    let protocol = if request.uri().path().starts_with("/v2/") {
        crate::protocol::ApiProtocol::Kserve
    } else {
        crate::protocol::ApiProtocol::Legacy
    };
    let fallback_err = move |e: AppError| {
        ProtocolError { error: e, protocol }.into_response()
    };
    // Only `/v2/models/<model>/<tail>` paths are candidates for custom routes.
    let path = request.uri().path();
    let Some(rest) = path.strip_prefix("/v2/models/") else {
        return fallback_err(AppError::RouteNotFound);
    };
    let (model, tail) = match rest.split_once('/') {
        Some((m, t)) if !m.is_empty() => (m.to_string(), t.to_string()),
        _ => return fallback_err(AppError::RouteNotFound), // bare /v2/models/<model>
    };

    // Split parts/body so the query extractor can run before the body is read.
    let (mut parts, body) = request.into_parts();
    let query = Query::<HashMap<String, String>>::from_request_parts(&mut parts, &state)
        .await
        .map(|q| q.0)
        .unwrap_or_default();
    // RN-13 (D9-A): the admission transfer cell — a streaming custom route
    // moves the guard into its response body so the slot is held for the
    // stream's lifetime.
    let admission_slot = parts
        .extensions
        .get::<crate::admission::AdmissionSlot>()
        .cloned()
        .unwrap_or_default();
    let method = parts.method.clone();
    // P-MW: read the RequestContext filled by context_middleware (the
    // from_http_parts fallback only fires when the middleware is absent).
    let cx = parts
        .extensions
        .get::<crate::request_context::RequestContext>()
        .cloned()
        .unwrap_or_else(|| crate::request_context::RequestContext::from_http_parts(&parts, &[]));
    let headers = parts.headers.clone();

    // F-06: match the custom route BEFORE reading the body — an unmatched
    // tail is a 404 (and a wrong method a 405) regardless of body size, and
    // an oversized body must not be read for a request that will be rejected.
    let resolved = match crate::http::handlers::resolve_custom_route(
        &state, &model, &tail, &method, &headers,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return fallback_err(e),
    };

    let body = match axum::body::to_bytes(body, ROUTE_BODY_LIMIT).await {
        Ok(b) => b,
        // F-06: an over-limit body is 413, not a generic 500.
        Err(e) => return fallback_err(map_route_body_error(e)),
    };

    match crate::http::handlers::dispatch_custom_route(
        &state, &model, resolved, &method, query, &headers, body, &cx, admission_slot,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => fallback_err(e),
    }
}

/// F-06: map a `to_bytes` failure on the custom-route body read — an
/// over-limit body (`LengthLimitError` in the source chain) is
/// `PayloadTooLarge` (413); anything else stays a 500.
fn map_route_body_error(e: axum::Error) -> AppError {
    let mut source: Option<&(dyn std::error::Error + 'static)> =
        std::error::Error::source(&e);
    while let Some(s) = source {
        if s.is::<http_body_util::LengthLimitError>() {
            return AppError::PayloadTooLarge {
                max_size: ROUTE_BODY_LIMIT,
                actual_size: None,
            };
        }
        source = s.source();
    }
    AppError::Internal("failed to read request body".into())
}

/// Max request body size for custom-route dispatch (mirrors a typical JSON body
/// cap). Inference uses its own ApiJson extractor limit.
const ROUTE_BODY_LIMIT: usize = 10 * 1024 * 1024;

/// Fallback for unmatched methods on matched routes — standardized 405 body.
/// F-17: protocol-aware like route_fallback (KServe flat body on /v2/ paths).
pub(crate) async fn method_not_allowed_fallback(uri: axum::http::Uri) -> ProtocolError {
    let protocol = if uri.path().starts_with("/v2/") {
        crate::protocol::ApiProtocol::Kserve
    } else {
        crate::protocol::ApiProtocol::Legacy
    };
    ProtocolError {
        error: AppError::MethodNotAllowed,
        protocol,
    }
}

/// Middleware that injects `x-request-id` and `x-processing-time-ms` into
/// every response. Reads client-supplied `x-client-request-id` (1–512 ASCII
/// chars); falls back to UUID v4.
///
/// The request ID is also stored in request extensions for downstream handlers.
async fn observability_middleware(mut request: Request, next: Next) -> Response {
    let start = Instant::now();

    // Extract or generate request ID
    let request_id = request
        .headers()
        .get("x-client-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| crate::validation::is_valid_request_id(s))
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    request.extensions_mut().insert(RequestId(request_id.clone()));

    // P-TRACE (蓝图 §4.2/§4.3): single-source OTel parent extraction (D21). The
    // extracted context is stashed for `context_middleware` → `trace_cx`, and links
    // the http.server span to the inbound trace so the request becomes a child.
    let parent = crate::telemetry::extract(request.headers());
    request
        .extensions_mut()
        .insert(crate::request_context::OtelParentContext(parent.clone()));

    // http.server span (http.method/route/status). The handler's `inference` span
    // nests under it (ambient); worker RequestMeta injection uses the active span.
    // `endpoint.class` 在创建时 stamp——B4 分类采样器（health/admin 独立比率）
    // 只能看到 span 创建时的属性，后 record 的不可见。
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let span = tracing::info_span!(
        "http.server",
        "http.request.method" = %method,
        "url.path" = %path,
        "endpoint.class" = crate::access_control::classify_http_path(&path).as_str(),
        "http.response.status_code" = tracing::field::Empty,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
    );
    crate::telemetry::link_parent(&span, &parent);
    let span_for_status = span.clone();

    let mut response = next.run(request).instrument(span).await;

    span_for_status.record("http.response.status_code", response.status().as_u16() as i64);

    // Overwrite headers — middleware is the authoritative source for these.
    if let Ok(v) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", v);
    }
    let elapsed_ms = start.elapsed().as_millis() as u64;
    if let Ok(v) = HeaderValue::from_str(&elapsed_ms.to_string()) {
        response.headers_mut().insert("x-processing-time-ms", v);
    }

    response
}

/// P4-2: in-flight accounting for the graceful-shutdown `pending` count — inc on
/// entry, dec when the response is produced. Placed outermost so it spans the
/// full request. For SSE/WebSocket the response is produced when headers are
/// sent, so an active long-lived stream stops counting; the actual stream drain
/// is handled by axum's graceful shutdown + the graceful_timeout backstop, this
/// counter is observability only (it was a no-op for HTTP before P4-2).
async fn inflight_middleware(
    state: State<Arc<crate::server::ShutdownState>>,
    request: Request,
    next: Next,
) -> Response {
    // RN-8: RAII — a handler panic must still decrement (see pending_guard).
    let _guard = state.pending_guard();
    next.run(request).await
}

/// P-FLOW (§4.0.9): global in-flight admission for *inference* requests.
/// Placed inside observability (so the 503 carries x-request-id). Health/admin
/// paths are exempt (probes must stay reachable under load). When `max_inflight`
/// is 0 (unlimited, the default) this is a pass-through.
///
/// RN-13 (D9-A): the guard is parked in an `AdmissionSlot` cell — a clone
/// rides the request extensions, one stays in this scope. Streaming handlers
/// take the guard into their feed/writer task so the slot is held for the
/// stream's lifetime; for unary nobody takes it and the guard drops here,
/// when the response is produced.
async fn admission_middleware(
    State(admission): State<crate::admission::AdmissionCounter>,
    mut request: Request,
    next: Next,
) -> Response {
    if admission.cap() == 0 {
        return next.run(request).await;
    }
    // B4: CORS preflight (OPTIONS) is unauthenticated and must never consume an
    // inference admission slot. Without this, a flood of preflights could
    // saturate max_inflight and 503 real inference; and a 503'd preflight
    // carries no CORS headers, which also blocks the browser's real request.
    // OPTIONS has no handler beyond the CORS short-circuit.
    if request.method() == axum::http::Method::OPTIONS {
        return next.run(request).await;
    }
    if crate::access_control::classify_http_path(request.uri().path())
        != crate::access_control::EndpointClass::Inference
    {
        return next.run(request).await;
    }
    let guard = match admission.try_acquire() {
        Some(g) => g,
        None => {
            tracing::warn!(
                current = admission.current(),
                cap = admission.cap(),
                "admission rejected: inference at max_inflight cap"
            );
            let mut resp = (
                StatusCode::SERVICE_UNAVAILABLE,
                "max_inflight capacity reached",
            )
                .into_response();
            resp.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, HeaderValue::from_static("1"));
            return resp;
        }
    };
    // RN-13 (D9-A): the guard rides in an `AdmissionSlot` cell so streaming
    // handlers can take it into their feed/writer task (stream-lifetime hold).
    // The middleware keeps its own reference to the cell and reclaims the
    // guard after `next.run` — unary handlers never take it out (their
    // extractors drop the request extensions before the inference runs), so
    // reclaiming here is what holds the slot until the response is produced.
    // Streaming handlers MUST take() synchronously, before their response is
    // produced: a take() inside a spawned/upgrade task runs after this
    // reclaim and loses the guard (the 21908c0 SSE/WS regression). `take()`
    // is idempotent, so the guard drops exactly once either way.
    let slot = crate::admission::AdmissionSlot::with_guard(guard);
    request.extensions_mut().insert(slot.clone());
    let response = next.run(request).await;
    let _held = slot.take();
    response
}

/// C3 (P4-2): once draining, reject new non-probe requests with 503 so
/// keep-alive clients (and LBs that miss readyz) add no new work during the
/// drain window. Health probes (/livez, /readyz, /startupz, /health and the
/// /v2/health/* aliases) bypass the
/// gate so they can report draining themselves. Placed inside observability so
/// the 503 is logged with a request-id.
async fn draining_gate(
    state: State<Arc<AtomicBool>>,
    request: Request,
    next: Next,
) -> Response {
    use std::sync::atomic::Ordering;
    if state.load(Ordering::Relaxed) {
        let path = request.uri().path();
        // /v2/health/* 是探针别名路由(批次 3),与本体同豁免(审计修复 B6:
        // 排水期 liveness 别名 503 会诱发 k8s 误杀)。
        let is_probe = matches!(
            path,
            "/livez" | "/readyz" | "/startupz" | "/health" | "/v2/health/live" | "/v2/health/ready"
        );
        if !is_probe {
            return (StatusCode::SERVICE_UNAVAILABLE, "server draining").into_response();
        }
    }
    next.run(request).await
}

/// P7-1 (蓝图 §4.2): endpoint-class access control. Coarse gate inside
/// observability (so a 401 still carries x-request-id) and outside the handler.
/// loopback is taken from the transport peer (ConnectInfo), NEVER from XFF
/// (client-forgable; aligned with P-XFF); UDS / missing ConnectInfo → loopback.
/// Stacks in front of the per-model `policies.auth` gate, which still runs in
/// the handler.
async fn access_control_middleware(
    State(ac): State<Arc<crate::access_control::AccessControl>>,
    request: Request,
    next: Next,
) -> Response {
    let class = crate::access_control::classify_http_path(request.uri().path());
    let is_loopback = request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().is_loopback())
        .unwrap_or(true);
    if !ac.check(class, crate::callback::Protocol::Http, request.headers(), is_loopback) {
        return AppError::Unauthorized("access denied".into()).into_response();
    }
    next.run(request).await
}

/// Compression predicate (P1-4): never compress SSE responses — buffering
/// would break per-event flush semantics.
#[derive(Clone, Copy)]
struct NotEventStream;

impl tower_http::compression::predicate::Predicate for NotEventStream {
    fn should_compress<B>(&self, response: &http::Response<B>) -> bool
    where
        B: axum::body::HttpBody,
    {
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|ct| !ct.starts_with("text/event-stream") && !ct.starts_with("application/x-lite-bidi"))
            .unwrap_or(true)
    }
}

#[cfg(unix)]
use tokio::net::UnixListener;

/// Options for [`start_http_server`] — named fields instead of an
/// 11-positional-arg list (1.96 too_many_arguments batch; gRPC side mirrors
/// this with `GrpcServerOptions`).
pub struct HttpServerOptions {
    pub config: Config,
    pub registry: Arc<ModelRegistry>,
    pub worker_manager: Arc<WorkerManager>,
    pub inference_queue: Arc<InferenceQueue>,
    pub shutdown_state: Arc<crate::server::ShutdownState>,
    pub draining: Arc<AtomicBool>,
    pub callback_runner: Arc<crate::callback::CallbackRunner>,
    pub has_hot_reload: Arc<AtomicBool>,
    pub rate_limiter: Arc<crate::rate_limit::RateLimiter>,
    pub tls: Option<Arc<crate::tls::TlsConfigStore>>,
}

pub async fn start_http_server(
    options: HttpServerOptions,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), AppError> {
    let HttpServerOptions {
        config,
        registry,
        worker_manager,
        inference_queue,
        shutdown_state,
        draining,
        callback_runner,
        has_hot_reload,
        rate_limiter,
        tls,
    } = options;
    let repo_path = PathBuf::from(&config.model_repository.path);
    // P3-1：RateLimiter 构造上移到 server/mod.rs（HTTP/gRPC 共享 + 60s cleanup）。
    let mut state = AppState::new(registry, worker_manager, inference_queue, config.clone(), repo_path, callback_runner, has_hot_reload, rate_limiter);
    state.shutdown_state = shutdown_state.clone();
    state.draining = draining.clone();
    let state_admission = state.admission.clone();

    // P7-1: resolve endpoint-class access control (value_env/value_file read
    // here so a missing source fails fast at startup). Shared shape with gRPC.
    let access_control = std::sync::Arc::new(
        crate::access_control::AccessControl::build(&config.access_control)?,
    );
    // D27: admin handlers 的审计 key 指纹经 AppState 消费（Arc 包装前注入）。
    state.access_control = access_control.clone();

    // openai-compact(/v1) 专属鉴权门:与 access_control 同模式,启动期解析
    // (value_env/value_file 缺源 fail-fast);None = 不启用(/v1 维持公开)。
    // 只被 openai_compact::mount 的 route_layer 消费。
    state.openai_auth = crate::access_control::OpenaiAuthGate::build(
        config.openai_compact.auth.as_ref(),
    )?
    .map(std::sync::Arc::new);

    // P-XFF: parse trusted-proxy CIDRs once (fail-fast on a bad entry). Empty
    // → fail-safe (direct peer used, client proxy headers ignored).
    let trusted = std::sync::Arc::new(config.server.trusted_networks()?);

    // P-CORS / WS Origin check read global cors + per-model policy off the
    // shared AppState; wrap once so the CORS middleware (mounted below) and
    // create_routes share one Arc.
    let shared = std::sync::Arc::new(state);
    let app = create_routes(shared.clone());

    // P-FLOW (§4.0.9): per-request body cap (default 64 MiB). Oversized
    // bodies → 413. DefaultBodyLimit::max is infallible for non-zero values.
    let app = app.layer(axum::extract::DefaultBodyLimit::max(
        config.server.max_request_body_bytes.unwrap_or(64 * 1024 * 1024),
    ));

    // L2 (resource-leak-plan): request-body idle timeout — see
    // request_body_timeout_middleware. Default 0 = off (behavior unchanged).
    let app = if config.server.request_body_timeout_secs > 0.0 {
        app.layer(axum::middleware::from_fn_with_state(
            std::time::Duration::from_secs_f32(config.server.request_body_timeout_secs),
            request_body_timeout_middleware,
        ))
    } else {
        app
    };

    // Peer-IP fallback (innermost): inject the TCP peer IP as a fallback
    // x-real-ip for direct (non-proxied) connections so client_ip is never
    // empty (0.7.x regression). No-op on the unix-socket path (no ConnectInfo)
    // and when a proxy header is already present.
    let app = app.layer(axum::middleware::from_fn(crate::http::handlers::peer_ip_fallback));

    // Keepalive middleware (inner)
    let app = if config.server.keepalive_timeout <= 0.0 {
        app.layer(axum::middleware::from_fn(disable_keepalive_middleware))
    } else {
        app
    };

    // Response compression (P1-4, inside observability so processing-time
    // reflects handling, not the wire): gzip textual responses when the
    // client accepts it. SSE is excluded — buffering would break per-event
    // flush; WS upgrades carry no body and are unaffected.
    let app = if config.server.compression {
        use tower_http::compression::predicate::{DefaultPredicate, Predicate};
        let predicate = DefaultPredicate::new().and(NotEventStream);
        app.layer(tower_http::compression::CompressionLayer::new().compress_when(predicate))
    } else {
        app
    };

    // Context middleware (P-MW, 蓝图 §4.0.2): immediately inside
    // observability, ahead of every RequestContext consumer (D21) — fills
    // RequestContext once from the observability RequestId stash + headers
    // + ConnectInfo; rate-limit / callbacks / RequestMeta all read it.
    // P-XFF: carries trusted_proxies so client-IP cleansing is fail-safe.
    let app = app.layer(axum::middleware::from_fn_with_state(
        trusted.clone(),
        crate::request_context::context_middleware,
    ));

    // P7-1: endpoint-class access control — inside observability (401 carries
    // x-request-id) and inside draining_gate (drain rejects before auth); does
    // not need RequestContext (uses the transport peer for loopback).
    let app = app.layer(axum::middleware::from_fn_with_state(
        access_control.clone(),
        access_control_middleware,
    ));

    // C3 (P4-2): draining gate — inside observability (so 503s carry a
    // request-id) but outside context/compression so new work is rejected
    // before any handler runs. Respects D21: it neither consumes RequestContext
    // nor touches compression/observability semantics.
    let app =
        app.layer(axum::middleware::from_fn_with_state(draining.clone(), draining_gate));

    // P-FLOW (§4.0.9): inference-only global admission cap. Inside
    // observability (503 carries x-request-id); health/admin exempt via
    // classify_http_path. No-op when `max_inflight` is 0.
    let app = app.layer(axum::middleware::from_fn_with_state(
        state_admission.clone(),
        admission_middleware,
    ));

    // P-CORS (蓝图 §4.3): hybrid CORS middleware — per-model > global policy,
    // exact Origin match, preflight short-circuit. Mounted OUTSIDE access_control
    // (preflight must not trigger auth; D21) and inside observability (204
    // carries x-request-id). Admin endpoints are skipped (not browser-facing).
    // 对账修复：移至 admission/draining **外侧**——§4.0.4 不变式「CORS 在错误
    // 响应上也附 ACAO」要求 503 短路响应也过 CORS，否则浏览器读不到错误体。
    // preflight 204 在 CORS 内短路，不再占用 admission 槽位。
    let app = app.layer(axum::middleware::from_fn_with_state(
        shared.clone(),
        crate::http::cors::cors_middleware,
    ));

    // Observability middleware (captures total wall-clock duration and sets
    // x-request-id + x-processing-time-ms on ALL responses including errors and
    // fallbacks).
    let app = app.layer(axum::middleware::from_fn(observability_middleware));

    // P4-2: in-flight accounting — outermost so it spans the whole request,
    // including observability overhead and draining-gate 503s.
    let app = app.layer(axum::middleware::from_fn_with_state(
        shutdown_state.clone(),
        inflight_middleware,
    ));

    if let Some(path) = unix_socket_path(&config.server.host) {
        #[cfg(unix)]
        {
            let _ = std::fs::remove_file(path);
            let listener = UnixListener::bind(path).map_err(AppError::Io)?;
            {
                use std::os::unix::fs::PermissionsExt;
                let metadata = std::fs::metadata(path).map_err(AppError::Io)?;
                let mut permissions = metadata.permissions();
                // B5: configurable via `server.socket_mode` (default 0o666). The
                // HTTP UDS serves admin too — tighten to 0o600 on multi-tenant hosts.
                permissions.set_mode(config.server.socket_mode);
                std::fs::set_permissions(path, permissions).map_err(AppError::Io)?;
            }
            info!("Starting HTTP server on unix:{}", path);
            serve_unix(listener, app, config.server.clone(), shutdown_rx).await?;
        }
        #[cfg(not(unix))]
        {
            return Err(AppError::Config(
                "Unix domain sockets are not supported on this platform".to_string(),
            ));
        }
    } else {
        let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.http_port)
            .parse()
            .map_err(|e| AppError::Config(format!("invalid address: {}", e)))?;

        if let Some(tls_store) = tls {
            // P5-1: TLS/mTLS termination. axum-server drives the accept loop
            // (per-connection handshake concurrency); RotatingTlsAcceptor
            // snapshots the current rustls config per connection so the cert
            // reloader's swap applies to the NEXT handshake. Graceful shutdown
            // parity with the plaintext path: the outer graceful_timeout +
            // task-abort backstop in server/mod.rs still bounds the drain.
            let std_listener = std::net::TcpListener::bind(addr).map_err(AppError::Io)?;
            std_listener.set_nonblocking(true).map_err(AppError::Io)?;
            info!("Starting HTTPS server on {} ({})", addr, tls_store.describe());
            let handle = axum_server::Handle::new();
            tokio::spawn({
                let handle = handle.clone();
                async move {
                    let _ = shutdown_rx.await;
                    handle.graceful_shutdown(None);
                }
            });
            let mut server = axum_server::Server::from_tcp(std_listener);
            {
                // K1/K2/K6: same shared builder wiring as serve_tcp/serve_unix
                // (axum-server's default builder is exactly
                // Builder::new(TokioExecutor::new()) — the replace is a no-op
                // swap).
                let b = server.http_builder();
                let taken = std::mem::replace(
                    b,
                    hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    ),
                );
                *b = configure_conn_builder(taken, &config.server);
            }
            server
                .handle(handle)
                .acceptor(
                    crate::tls::RotatingTlsAcceptor::new(tls_store)
                        .with_connection_limit(config.server.max_connections),
                )
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .map_err(|e| AppError::Internal(format!("server error: {}", e)))?;
        } else {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(AppError::Io)?;

            info!("Starting HTTP server on {}", addr);
            // into_make_service_with_connect_info makes ConnectInfo<SocketAddr>
            // available to extractors/middleware — peer_ip_fallback uses it to
            // populate client_ip for direct connections. serve_tcp preserves it
            // via Connected<SocketAddr> (axum serve.rs parity).
            serve_tcp(listener, app, config.server.clone(), shutdown_rx, None).await?;
        }
    }

    Ok(())
}

#[cfg(unix)]
async fn serve_unix(
    listener: UnixListener,
    app: Router,
    server_config: crate::config::ServerConfig,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), AppError> {
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::service::TowerToHyperService;

    let listener = Arc::new(listener);
    accept_loop(
        move || {
            let listener = listener.clone();
            async move { listener.accept().await.map(|(stream, _)| stream) }
        },
        shutdown_rx,
        move |stream| {
            let app = app.clone();
            let server_config = server_config.clone();
            tokio::spawn(async move {
                crate::metrics::prometheus::record_http_connection_open("uds");
                let io = TokioIo::new(stream);
                let hyper_service = TowerToHyperService::new(app);
                if server_config.keepalive_timeout <= 0.0 {
                    // K2: h1-only — see serve_tcp for why the auto builder's
                    // http1_only() cannot be used here, and why keep_alive
                    // stays on (F-04: 101 upgrades must not get
                    // connection: close).
                    let mut b = hyper::server::conn::http1::Builder::new();
                    b.timer(hyper_util::rt::TokioTimer::new());
                    let conn = b.serve_connection(io, hyper_service).with_upgrades();
                    if let Err(e) = conn.await {
                        tracing::debug!("Connection error: {}", e);
                    }
                } else {
                    // K1/K6: shared builder wiring (idle reaper / h2 keepalive).
                    let builder =
                        configure_conn_builder(Builder::new(TokioExecutor::new()), &server_config);
                    let conn = builder.serve_connection_with_upgrades(io, hyper_service);
                    if let Err(e) = conn.await {
                        tracing::debug!("Connection error: {}", e);
                    }
                }
                crate::metrics::prometheus::record_http_connection_close("uds");
            });
        },
    )
    .await
}

/// K1/K6: shared hyper auto-builder wiring for all three serve paths
/// (serve_tcp / serve_unix / axum-server's http_builder). Only called when
/// keepalive_timeout > 0 — for ka <= 0 see serve_h1_only (K2).
///
/// - K1: arm the h1 idle reaper — hyper applies header_read_timeout to every
///   request-head read, including the idle wait for the next request on a
///   keep-alive connection (hyper 1.10 h1/conn.rs poll_read_head), so an idle
///   connection is closed once the window elapses. Requires an explicit
///   timer; the auto builder has none by default.
/// - K6: optional h2 keepalive PING (dead-peer detection only — hyper h2 has
///   no idle reaper).
fn configure_conn_builder(
    mut builder: hyper_util::server::conn::auto::Builder<hyper_util::rt::TokioExecutor>,
    server: &crate::config::ServerConfig,
) -> hyper_util::server::conn::auto::Builder<hyper_util::rt::TokioExecutor> {
    use hyper_util::rt::TokioTimer;
    let ka = server.keepalive_timeout;
    if ka > 0.0 {
        builder.http1().timer(TokioTimer::new());
        builder
            .http1()
            .header_read_timeout(std::time::Duration::from_secs_f32(ka));
    }
    if let Some(interval) = server.http2_keepalive_interval_secs {
        builder.http2().timer(TokioTimer::new());
        builder
            .http2()
            .keep_alive_interval(std::time::Duration::from_secs_f32(interval));
        if let Some(t) = server.http2_keepalive_timeout_secs {
            builder
                .http2()
                .keep_alive_timeout(std::time::Duration::from_secs_f32(t));
        }
    }
    builder
}

/// L5 (resource-leak-plan): OS-level TCP keepalive on an accepted socket —
/// half-open peers (dead client, dropped NAT state) are reaped by the kernel
/// even when no application frame is in flight. Fixed policy (idle 60s,
/// interval 10s, 3 probes), deliberately not a knob (simple-first).
pub(crate) fn set_tcp_keepalive(stream: &tokio::net::TcpStream) {
    let sock = socket2::SockRef::from(stream);
    let params = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(60))
        .with_interval(std::time::Duration::from_secs(10));
    // Windows SIO_KEEPALIVE_VALS exposes only idle + interval — no retry
    // count (TCP_KEEPCNT) — so the probe count is applied where the OS has it.
    #[cfg(not(windows))]
    let params = params.with_retries(3);
    if let Err(e) = sock.set_tcp_keepalive(&params) {
        tracing::warn!(error = %e, "failed to set TCP keepalive on accepted socket");
    }
}

/// K1: plaintext TCP accept loop. `axum::serve` exposes no hyper builder
/// surface, so the keepalive_timeout idle reaper (timer + h1
/// header_read_timeout) requires a custom loop. Mirrors axum 0.7.9's own
/// serve.rs per-connection pattern:
/// - `ConnectInfo<SocketAddr>` is preserved by calling the make service with
///   the peer addr (`Connected<SocketAddr> for SocketAddr`); peer_ip_fallback
///   must not regress.
/// - graceful shutdown stops accepting and drains in-flight connections via a
///   JoinSet (axum's with_graceful_shutdown parity; the outer
///   graceful_timeout + task-abort backstop in server/mod.rs still bounds the
///   drain).
/// - transient accept errors (fd exhaustion etc.) retry after 50ms, same
///   classifier as the UDS loop (F-07).
async fn serve_tcp(
    listener: tokio::net::TcpListener,
    app: Router,
    server_config: crate::config::ServerConfig,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    // L1 test/observability seam: mirrors `connections.len()` — the number of
    // connection tasks still retained in the JoinSet (in-flight + completed
    // but not yet reaped). `None` = no-op. Lets a test assert that completed
    // connection tasks are reaped instead of accumulating for the server's
    // lifetime.
    connection_tasks: Option<Arc<std::sync::atomic::AtomicUsize>>,
) -> Result<(), AppError> {
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
    use hyper_util::service::TowerToHyperService;
    use std::future::poll_fn;
    use tower::ServiceExt as _;

    let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();
    let mut connections = tokio::task::JoinSet::new();
    // D7: hard connection cap (0 = off). The counter is decremented when the
    // connection task ends.
    let max_connections = server_config.max_connections;
    let open_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) if is_transient_accept_error(&e) => {
                        tracing::warn!(error = %e, "transient accept error; retrying");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(e) => return Err(AppError::Io(e)),
                };
                // D7: over-cap connections are closed at accept — there is no
                // channel to answer on before the connection exists.
                if max_connections > 0
                    && open_connections.load(std::sync::atomic::Ordering::Acquire) >= max_connections
                {
                    tracing::warn!(
                        current = max_connections,
                        "max_connections reached; closing new connection at accept"
                    );
                    drop(stream);
                    continue;
                }
                open_connections.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                let open_connections = Arc::clone(&open_connections);
                // L5: OS-level keepalive on the accepted socket.
                set_tcp_keepalive(&stream);
                poll_fn(|cx| {
                    tower_service::Service::<SocketAddr>::poll_ready(&mut make_service, cx)
                })
                .await
                .unwrap_or_else(|err: std::convert::Infallible| match err {});
                let tower_service = tower_service::Service::<SocketAddr>::call(
                    &mut make_service,
                    peer_addr,
                )
                .await
                .unwrap_or_else(|err: std::convert::Infallible| match err {})
                .map_request(|req: axum::http::Request<hyper::body::Incoming>| {
                    req.map(axum::body::Body::new)
                });
                let hyper_service = TowerToHyperService::new(tower_service);
                let server_config = server_config.clone();
                connections.spawn(async move {
                    // L4: connection-level gauge, held for the task's
                    // lifetime (K1's idle reaper closes the connection → the
                    // task ends → the gauge decrements).
                    crate::metrics::prometheus::record_http_connection_open("tcp");
                    let io = TokioIo::new(stream);
                    if server_config.keepalive_timeout <= 0.0 {
                        // K2: h1-only — hyper-util's auto builder ignores
                        // http1_only() on the with_upgrades path (0.1.20
                        // always starts with the h2 preface sniff), so the
                        // honest h1-only implementation is hyper's own http1
                        // builder. NOTE: no keep_alive(false) here — hyper
                        // would then stamp `connection: close` onto 101
                        // upgrade responses too (F-04 regression); the
                        // disable_keepalive_middleware header (which guards
                        // 101) is what closes the connection, and hyper's h1
                        // encoder honors it.
                        let mut b = hyper::server::conn::http1::Builder::new();
                        b.timer(TokioTimer::new());
                        let conn = b.serve_connection(io, hyper_service).with_upgrades();
                        if let Err(e) = conn.await {
                            tracing::debug!("Connection error: {}", e);
                        }
                    } else {
                        let builder = configure_conn_builder(
                            Builder::new(TokioExecutor::new()),
                            &server_config,
                        );
                        let conn = builder.serve_connection_with_upgrades(io, hyper_service);
                        if let Err(e) = conn.await {
                            tracing::debug!("Connection error: {}", e);
                        }
                    }
                    // D7: release the connection slot FIRST — observers that
                    // poll the L4 gauge for "connection gone" must never see
                    // the gauge drop while the cap counter still holds.
                    open_connections.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    crate::metrics::prometheus::record_http_connection_close("tcp");
                });
                // L1: reap completed connection tasks so their JoinHandles
                // don't accumulate in the JoinSet until shutdown.
                while connections.try_join_next().is_some() {}
                // L1: expose the retained connection-task count (in-flight
                // only, after the reap above) to a test / operator.
                if let Some(c) = &connection_tasks {
                    c.store(connections.len(), std::sync::atomic::Ordering::Relaxed);
                }
            }
            _ = &mut shutdown_rx => {
                info!(
                    "TCP server received shutdown signal; draining {} connection(s)",
                    connections.len()
                );
                while connections.join_next().await.is_some() {}
                break;
            }
        }
    }
    Ok(())
}

/// F-07: classify an accept() error — transient resource errors (fd
/// exhaustion EMFILE/ENFILE, interrupted call EINTR, aborted handshake
/// ECONNABORTED) must be retried, not fatal: fd exhaustion hits exactly
/// when the service is busiest, and axum-server / tonic already tolerate
/// it on the TCP paths (50ms backoff / continue). Anything else is fatal.
fn is_transient_accept_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::Interrupted | std::io::ErrorKind::ConnectionAborted
    ) || matches!(
        e.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOBUFS) | Some(libc::ENOMEM)
    )
}

/// F-07: the UDS accept loop, generic over the accept source and the
/// connection handler so tests can inject error sequences without a
/// failpoint. Transient accept errors sleep 50ms and retry (axum-server
/// parity); fatal errors end the loop.
#[cfg(unix)]
async fn accept_loop<A, Fut, H>(
    mut next_accept: A,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    mut handle_conn: H,
) -> Result<(), AppError>
where
    A: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::io::Result<tokio::net::UnixStream>>,
    H: FnMut(tokio::net::UnixStream),
{
    loop {
        tokio::select! {
            result = next_accept() => {
                match result {
                    Ok(stream) => handle_conn(stream),
                    Err(e) if is_transient_accept_error(&e) => {
                        tracing::warn!(error = %e, "transient accept error; retrying");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(e) => return Err(AppError::Io(e)),
                }
            }
            _ = &mut shutdown_rx => {
                info!("Unix socket server received shutdown signal");
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use tower::ServiceExt;

    /// Minimal AppState for fallback tests (empty registry / no workers).
    fn test_state() -> Arc<AppState> {
        use crate::callback::CallbackRunner;
        use crate::config::Config;
        use crate::inference_queue::InferenceQueue;
        use crate::rate_limit::RateLimiter;
        use crate::registry::ModelRegistry;
        use crate::worker::WorkerManager;
        use std::path::PathBuf;
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            PathBuf::new(),
            inference_queue.clone(),
            "info".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            Config::default(),
            PathBuf::new(),
            callback_runner,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(RateLimiter::new(1024)),
        ))
    }

    #[tokio::test]
    async fn test_disable_keepalive_middleware_adds_connection_close() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(disable_keepalive_middleware));

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let connection = response.headers().get("connection");
        assert_eq!(connection, Some(&axum::http::HeaderValue::from_static("close")));
    }

    // ===== F-07: accept-error classification + injected-loop tests =====

    #[cfg(unix)]
    #[test]
    fn f07_accept_error_classification() {
        use std::io::ErrorKind;
        // Transient: fd exhaustion / interrupted / aborted handshake.
        for raw in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
            assert!(
                is_transient_accept_error(&std::io::Error::from_raw_os_error(raw)),
                "raw {raw} must be transient"
            );
        }
        assert!(is_transient_accept_error(&std::io::Error::from(ErrorKind::Interrupted)));
        assert!(is_transient_accept_error(&std::io::Error::from(ErrorKind::ConnectionAborted)));
        // Fatal: permission / address errors.
        for raw in [libc::EPERM, libc::EACCES, libc::EBADF] {
            assert!(
                !is_transient_accept_error(&std::io::Error::from_raw_os_error(raw)),
                "raw {raw} must be fatal"
            );
        }
    }

    /// F-07: a stream of transient accept errors must not kill the loop;
    /// shutdown still ends it cleanly.
    #[cfg(unix)]
    #[tokio::test]
    async fn f07_transient_accept_errors_are_retried_not_fatal() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(accept_loop(
            || async {
                Err::<tokio::net::UnixStream, _>(std::io::Error::from_raw_os_error(libc::EMFILE))
            },
            rx,
            |_| {},
        ));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !handle.is_finished(),
            "transient accept errors must not kill the accept loop"
        );
        let _ = tx.send(());
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("accept loop must observe shutdown within 2s")
            .expect("accept loop task must not panic");
        assert!(result.is_ok(), "shutdown after transient errors must be clean");
    }

    /// F-07: a non-transient accept error stays fatal.
    #[cfg(unix)]
    #[tokio::test]
    async fn f07_fatal_accept_error_ends_the_loop() {
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let result = accept_loop(
            || async {
                Err::<tokio::net::UnixStream, _>(std::io::Error::from_raw_os_error(libc::EPERM))
            },
            rx,
            |_| {},
        )
        .await;
        assert!(result.is_err(), "non-transient accept error must be fatal");
    }

    #[tokio::test]
    async fn test_without_keepalive_disabled_no_connection_close() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }));

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let connection = response.headers().get("connection");
        assert!(connection.is_none() || connection != Some(&axum::http::HeaderValue::from_static("close")));
    }

    // ===== L1 (memory-leak): connection-task JoinSet must not accumulate =====

    /// L1 reproduction (RED): `serve_tcp`'s `connections` JoinSet retains every
    /// completed connection task until shutdown (`join_next` in the shutdown
    /// branch only). Across the server's lifetime each completed connection
    /// leaves a JoinHandle behind — unbounded growth proportional to the total
    /// number of connections ever accepted.
    ///
    /// The observer seam (`connection_tasks`) mirrors `connections.len()` after
    /// every spawn, so the test asserts the retained count stays bounded after
    /// a batch of short-lived connections instead of growing to their total.
    #[tokio::test]
    async fn l1_completed_connection_tasks_stay_bounded() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
        let connection_tasks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_tcp(
            listener,
            app,
            crate::config::ServerConfig::default(),
            shutdown_rx,
            Some(connection_tasks.clone()),
        ));

        // Open 20 short-lived connections: GET /, read the body, drop.
        // Each server-side connection task completes when the client closes.
        for _ in 0..20 {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut buf = [0u8; 4096];
            let mut body = Vec::new();
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&buf[..n]);
            }
            assert!(
                String::from_utf8_lossy(&body).contains("200 OK"),
                "connection must be served"
            );
            drop(stream);
            // Let the server-side task finish closing before the next accept,
            // so the completed-task accounting is deterministic.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // A probe connection wakes the accept loop so it records the retained
        // count; then poll until the count converges (or timeout on the leak).
        {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut buf = [0u8; 4096];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
            }
        }

        let mut retained = usize::MAX;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            retained = connection_tasks.load(std::sync::atomic::Ordering::Relaxed);
            // Completed connection tasks must be reaped; a small bound for the
            // in-flight probe is acceptable, but never the batch's full count.
            if retained <= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let _ = shutdown_tx.send(());
        server.await.unwrap().unwrap();

        assert!(
            retained <= 2,
            "L1: completed connection tasks not reaped from the JoinSet: \
             retained {retained} (expected bounded) — each completed connection \
             leaks a JoinHandle until shutdown"
        );
    }

    // ===== P-FLOW (§4.0.9) admission + body limit =====

    fn admission_app(cap: usize) -> (axum::Router, crate::admission::AdmissionCounter) {
        let admission = crate::admission::AdmissionCounter::new(cap);
        let app = axum::Router::new()
            .route(
                "/v2/models/foo/infer",
                axum::routing::any(|| async { "ok" }),
            )
            .route("/livez", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                admission.clone(),
                admission_middleware,
            ));
        (app, admission)
    }

    #[tokio::test]
    async fn p_flow_admission_rejects_inference_over_cap_with_retry_after() {
        let (app, admission) = admission_app(1);
        // Saturate the single slot.
        let _fill = admission.try_acquire().expect("cap=1 admits one");
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/models/foo/infer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get("retry-after"),
            Some(&axum::http::HeaderValue::from_static("1")),
            "inference over cap must carry Retry-After"
        );
    }

    #[tokio::test]
    async fn p_flow_admission_exempts_health_under_cap() {
        let (app, admission) = admission_app(1);
        // Saturate the single slot — health must still pass (probes stay live).
        let _fill = admission.try_acquire().expect("cap=1 admits one");
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn p_flow_admission_unlimited_when_cap_zero() {
        let (app, admission) = admission_app(0);
        for _ in 0..5 {
            assert!(admission.try_acquire().is_some());
        }
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/models/foo/infer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn p_flow_admission_holds_slot_across_unary_inference() {
        // Regression (6f032a1, RN-13/D9-A): the middleware parked the guard in
        // the request-extensions cell but kept no reference in scope, and the
        // unary handlers never take it out (unlike streaming handlers). The
        // guard was dropped when the handler's extractors consumed the request
        // — before the inference ran — so max_inflight never bounded unary
        // concurrency. The slot must be held until the response is produced.
        async fn slow() -> &'static str {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            "ok"
        }
        let admission = crate::admission::AdmissionCounter::new(1);
        let app = axum::Router::new()
            .route("/v2/models/foo/infer", axum::routing::any(slow))
            .layer(axum::middleware::from_fn_with_state(
                admission.clone(),
                admission_middleware,
            ));
        let req = || {
            Request::builder()
                .uri("/v2/models/foo/infer")
                .method("POST")
                .body(Body::empty())
                .unwrap()
        };
        // Fire two requests concurrently. The first is admitted and must hold
        // the single slot while the slow handler sleeps; the second must be
        // rejected with 503.
        let (a, b) = tokio::join!(app.clone().oneshot(req()), app.clone().oneshot(req()));
        let statuses = [a.unwrap().status(), b.unwrap().status()];
        assert_eq!(
            statuses.iter().filter(|s| **s == StatusCode::OK).count(),
            1,
            "exactly one admitted, got {statuses:?}"
        );
        assert_eq!(
            statuses
                .iter()
                .filter(|s| **s == StatusCode::SERVICE_UNAVAILABLE)
                .count(),
            1,
            "second request must be 503, got {statuses:?}"
        );
    }

    #[tokio::test]
    async fn p_flow_body_limit_rejects_oversized_with_413() {
        async fn handler(body: axum::body::Bytes) -> String {
            format!("{}", body.len())
        }
        let app = axum::Router::new()
            .route("/v2/models/foo/infer", axum::routing::post(handler))
            .layer(axum::extract::DefaultBodyLimit::max(8));
        // Within limit → ok.
        let ok = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/models/foo/infer")
                    .body(Body::from(&b"short"[..]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        // Over limit → 413.
        let too_big = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/models/foo/infer")
                    .body(Body::from(vec![0u8; 64]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(too_big.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // ===== fallback tests =====

    #[tokio::test]
    async fn test_fallback_route_not_found_standardized_body() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .fallback(route_fallback)
            .with_state(test_state());

        let response = app
            .oneshot(Request::builder().uri("/no-such-route").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "not_found_error");
        assert_eq!(body["error"]["code"], "route_not_found");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_method_not_allowed_standardized_body() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .method_not_allowed_fallback(method_not_allowed_fallback);

        let response = app
            .oneshot(Request::builder().uri("/test").method("POST").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"]["type"], "method_not_allowed");
        assert_eq!(body["error"]["code"], "method_not_allowed");
        assert_eq!(body["error"]["param"], serde_json::Value::Null);
    }

    // ===== observability middleware tests =====

    #[tokio::test]
    async fn test_observability_middleware_sets_headers() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(observability_middleware));

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-request-id").is_some(),
            "x-request-id should be present");
        assert!(response.headers().get("x-processing-time-ms").is_some(),
            "x-processing-time-ms should be present");
    }

    #[tokio::test]
    async fn test_x_client_request_id_propagation() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(observability_middleware));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("x-client-request-id", "my-trace-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.headers().get("x-request-id").unwrap(),
            "my-trace-123"
        );
    }

    #[tokio::test]
    async fn test_x_client_request_id_too_long() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(observability_middleware));

        let long_id = "a".repeat(513);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("x-client-request-id", &long_id)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let returned = response.headers().get("x-request-id").unwrap().to_str().unwrap();
        assert_ne!(returned, &long_id, "over-length client ID should be rejected");
        assert_eq!(returned.len(), 36, "should be UUID v4 (36 chars)");
    }

    #[tokio::test]
    async fn test_x_client_request_id_empty_falls_back_to_uuid() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(observability_middleware));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("x-client-request-id", "")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let returned = response.headers().get("x-request-id").unwrap().to_str().unwrap();
        assert!(!returned.is_empty(), "empty client ID should be rejected");
        assert_eq!(returned.len(), 36, "should fall back to UUID v4 (36 chars)");
    }

    #[tokio::test]
    async fn test_observability_headers_on_error_responses() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async {
                let err: AppError = AppError::ModelNotFound("test".into());
                err.into_response()
            }))
            .layer(axum::middleware::from_fn(observability_middleware));

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        // Headers MUST be present even on error responses
        assert!(response.headers().get("x-request-id").is_some(),
            "x-request-id must be on error responses too");
        assert!(response.headers().get("x-processing-time-ms").is_some(),
            "x-processing-time-ms must be on error responses too");
    }

    // ===== P-MW: production stack order (observability → context_middleware) =====

    #[tokio::test]
    async fn test_context_reads_single_request_id_fill_by_observability() {
        // 蓝图 §4.0.5: one fill, many readers — the handler's RequestContext
        // must carry the SAME request_id observability echoes on the response,
        // and the client_ip from the request headers.
        async fn cx_handler(cx: crate::request_context::RequestContext) -> String {
            format!("{}|{}", cx.request_id, cx.client_ip)
        }
        // Same order as start_http_server: context_middleware applied first
        // (inner), observability last (outermost).
        let app = axum::Router::new()
            .route("/test", axum::routing::get(cx_handler))
            .layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(crate::client_ip::TrustedNetworks::new()),
                crate::request_context::context_middleware,
            ))
            .layer(axum::middleware::from_fn(observability_middleware));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("x-client-request-id", "trace-abc")
                    .header("x-forwarded-for", "10.1.2.3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let echoed = response
            .headers()
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(body, format!("{}|10.1.2.3", echoed),
            "handler context must read the same request_id observability echoes");
    }
}

/// /audit 回归锁（P-MW 面，蓝图 §4.0.2/§4.0.4 顺序不变式）：生产栈挂载顺序
/// （外→内）为 cors → admission → draining_gate（见上方装配），故
/// admission/draining 的 503 短路响应**经过 cors_middleware**——§4.0.4
/// 要求「CORS 在错误响应上也附 ACAO（浏览器需要）」，浏览器在过载/排水的
/// 503 上能读到状态与 Retry-After；preflight 204 在 CORS 内短路，不占
/// admission 槽位。以下测试按生产同序复刻最小栈，验证该不变式。
#[cfg(test)]
mod audit_mw_order_tests {
    use super::*;
    use crate::config::CorsPolicy;

    fn cors_state(max_inflight: usize) -> Arc<AppState> {
        use crate::callback::CallbackRunner;
        use crate::inference_queue::InferenceQueue;
        use crate::rate_limit::RateLimiter;
        use crate::registry::ModelRegistry;
        use crate::worker::WorkerManager;
        let mut config = Config::default();
        config.server.max_inflight = max_inflight;
        config.server.cors = Some(CorsPolicy {
            allow_origins: vec!["https://ok.example".to_string()],
            allow_methods: vec!["*".to_string()],
            ..Default::default()
        });
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            PathBuf::new(),
            inference_queue.clone(),
            "error".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            config,
            PathBuf::new(),
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(RateLimiter::new(1024)),
        ))
    }

    #[tokio::test]
    async fn test_audit_order_admission_503_carries_cors_acao() {
        let state = cors_state(1);
        let admission = state.admission.clone();
        // 生产同序（外→内，修复后）：cors → admission → route。
        let app = Router::new()
            .route("/v2/models/foo/infer", axum::routing::post(|| async { "unreached" }))
            .layer(axum::middleware::from_fn_with_state(
                admission.clone(),
                admission_middleware,
            ))
            .layer(axum::middleware::from_fn_with_state(
                state,
                crate::http::cors::cors_middleware,
            ));
        // 占满唯一 admission 槽位 → 下一个 inference 请求 503。
        let _held = admission.try_acquire().expect("cap=1 admits one");

        let response = tower::ServiceExt::oneshot(
            app,
            Request::builder()
                .method("POST")
                .uri("/v2/models/foo/infer")
                .header("origin", "https://ok.example")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get("access-control-allow-origin"),
            Some(&HeaderValue::from_static("https://ok.example")),
            "§4.0.4: CORS 在错误响应上也附 ACAO——admission 503 在 cors 外侧短路,浏览器读不到"
        );
    }

    #[tokio::test]
    async fn test_audit_order_draining_503_carries_cors_acao() {
        let state = cors_state(0);
        let draining = Arc::new(AtomicBool::new(true));
        // 生产同序（外→内，修复后）：cors → draining_gate → route。
        let app = Router::new()
            .route("/v2/models/foo/infer", axum::routing::post(|| async { "unreached" }))
            .layer(axum::middleware::from_fn_with_state(draining, draining_gate))
            .layer(axum::middleware::from_fn_with_state(
                state,
                crate::http::cors::cors_middleware,
            ));

        let response = tower::ServiceExt::oneshot(
            app,
            Request::builder()
                .method("POST")
                .uri("/v2/models/foo/infer")
                .header("origin", "https://ok.example")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get("access-control-allow-origin"),
            Some(&HeaderValue::from_static("https://ok.example")),
            "§4.0.4: CORS 在错误响应上也附 ACAO——draining 503 在 cors 外侧短路,浏览器读不到"
        );
    }
}
