pub mod handlers;
pub mod routes;
pub mod state;

use crate::config::{unix_socket_path, Config};
use crate::error::AppError;
use crate::http::routes::create_routes;
use crate::http::state::AppState;
use crate::inference_queue::InferenceQueue;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use axum::extract::{FromRequestParts, Query, Request, State};
use axum::http::header::CONNECTION;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;
use uuid::Uuid;

async fn disable_keepalive_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        CONNECTION,
        axum::http::HeaderValue::from_static("close"),
    );
    response
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
    // Only `/v2/models/<model>/<tail>` paths are candidates for custom routes.
    let path = request.uri().path();
    let Some(rest) = path.strip_prefix("/v2/models/") else {
        return AppError::RouteNotFound.into_response();
    };
    let (model, tail) = match rest.split_once('/') {
        Some((m, t)) if !m.is_empty() => (m.to_string(), t.to_string()),
        _ => return AppError::RouteNotFound.into_response(), // bare /v2/models/<model>
    };

    // Split parts/body so the query extractor can run before the body is read.
    let (mut parts, body) = request.into_parts();
    let query = Query::<HashMap<String, String>>::from_request_parts(&mut parts, &state)
        .await
        .map(|q| q.0)
        .unwrap_or_default();
    let method = parts.method.clone();
    // P-MW: read the RequestContext filled by context_middleware (the
    // from_http_parts fallback only fires when the middleware is absent).
    let cx = parts
        .extensions
        .get::<crate::request_context::RequestContext>()
        .cloned()
        .unwrap_or_else(|| crate::request_context::RequestContext::from_http_parts(&parts));
    let headers = parts.headers.clone();
    let body = match axum::body::to_bytes(body, ROUTE_BODY_LIMIT).await {
        Ok(b) => b,
        Err(_) => {
            return AppError::Internal("failed to read request body".into()).into_response()
        }
    };

    match crate::http::handlers::dispatch_custom_route(
        &state, &model, &tail, &method, query, &headers, body, &cx,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

/// Max request body size for custom-route dispatch (mirrors a typical JSON body
/// cap). Inference uses its own ApiJson extractor limit.
const ROUTE_BODY_LIMIT: usize = 10 * 1024 * 1024;

/// Fallback for unmatched methods on matched routes — standardized 405 body.
pub(crate) async fn method_not_allowed_fallback() -> AppError {
    AppError::MethodNotAllowed
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

    let mut response = next.run(request).await;

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
            .map(|ct| !ct.starts_with("text/event-stream"))
            .unwrap_or(true)
    }
}

#[cfg(unix)]
use tokio::net::UnixListener;

pub async fn start_http_server(
    config: Config,
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    inference_queue: Arc<InferenceQueue>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    shutdown_state: Arc<crate::server::ShutdownState>,
    callback_runner: Arc<crate::callback::CallbackRunner>,
    has_hot_reload: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let repo_path = PathBuf::from(&config.model_repository.path);
    let rate_limiter = Arc::new(crate::rate_limit::RateLimiter::new(
        config.rate_limit.max_buckets,
    ));
    let mut state = AppState::new(registry, worker_manager, inference_queue, config.clone(), repo_path, callback_runner, has_hot_reload, rate_limiter.clone());

    // Background cleanup: evict stale rate-limit buckets every 60s
    {
        let limiter = rate_limiter.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                let removed = limiter.cleanup_stale(Duration::from_secs(600));
                if removed > 0 {
                    tracing::debug!(removed, "rate limiter: evicted stale buckets");
                }
            }
        });
    }
    state.shutdown_state = shutdown_state;

    let app = create_routes(state);

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
    let app = app.layer(axum::middleware::from_fn(
        crate::request_context::context_middleware,
    ));

    // Observability middleware (outermost — applied last so it captures total
    // wall-clock duration and sets x-request-id + x-processing-time-ms on ALL
    // responses including errors and fallbacks)
    let app = app.layer(axum::middleware::from_fn(observability_middleware));

    if let Some(path) = unix_socket_path(&config.server.host) {
        #[cfg(unix)]
        {
            let _ = std::fs::remove_file(path);
            let listener = UnixListener::bind(path).map_err(AppError::Io)?;
            {
                use std::os::unix::fs::PermissionsExt;
                let metadata = std::fs::metadata(path).map_err(AppError::Io)?;
                let mut permissions = metadata.permissions();
                permissions.set_mode(0o666);
                std::fs::set_permissions(path, permissions).map_err(AppError::Io)?;
            }
            info!("Starting HTTP server on unix:{}", path);
            serve_unix(listener, app, shutdown_rx).await?;
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

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(AppError::Io)?;

        info!("Starting HTTP server on {}", addr);
        // into_make_service_with_connect_info makes ConnectInfo<SocketAddr>
        // available to extractors/middleware — peer_ip_fallback uses it to
        // populate client_ip for direct connections.
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| AppError::Internal(format!("server error: {}", e)))?;
    }

    Ok(())
}

#[cfg(unix)]
async fn serve_unix(
    listener: UnixListener,
    app: Router,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), AppError> {
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::service::TowerToHyperService;

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result.map_err(AppError::Io)?;
                let app = app.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let builder = Builder::new(TokioExecutor::new());
                    let hyper_service = TowerToHyperService::new(app);
                    let conn = builder.serve_connection_with_upgrades(io, hyper_service);
                    if let Err(e) = conn.await {
                        tracing::debug!("Connection error: {}", e);
                    }
                });
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
            .layer(axum::middleware::from_fn(
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
