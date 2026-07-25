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
use crate::worker::endpoint_manager::EndpointManager;
use axum::extract::Request;
use axum::http::header::CONNECTION;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use axum::Router;
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

/// Newtype for request ID extraction from extensions.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[axum::async_trait]
impl<S: Send + Sync> axum::extract::FromRequestParts<S> for RequestId {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<RequestId>()
            .cloned()
            .unwrap_or_else(|| RequestId(Uuid::new_v4().to_string())))
    }
}

/// Fallback for unmatched routes — standardized 404 error body.
pub(crate) async fn route_fallback() -> AppError {
    AppError::RouteNotFound
}

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
        .filter(|s| !s.is_empty() && s.len() <= 512 && s.is_ascii())
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

#[cfg(unix)]
use tokio::net::UnixListener;

pub async fn start_http_server(
    config: Config,
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    inference_queue: Arc<InferenceQueue>,
    endpoint_manager: Option<Arc<EndpointManager>>,
    endpoint_routes: Vec<crate::worker::protocol::EndpointRoute>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    shutdown_state: Arc<crate::server::ShutdownState>,
    callback_runner: Arc<crate::callback::CallbackRunner>,
    has_hot_reload: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let repo_path = PathBuf::from(&config.model_repository.path);
    let rate_limiter = Arc::new(crate::rate_limit::RateLimiter::new(
        config.rate_limit.max_buckets,
    ));
    let mut state = AppState::new(registry, worker_manager, inference_queue, endpoint_manager, config.clone(), repo_path, callback_runner, has_hot_reload, rate_limiter.clone());

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

    let app = create_routes(state, endpoint_routes);

    // Keepalive middleware (inner)
    let app = if config.server.keepalive_timeout <= 0.0 {
        app.layer(axum::middleware::from_fn(disable_keepalive_middleware))
    } else {
        app
    };

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
        axum::serve(listener, app)
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
            .fallback(route_fallback);

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

    // ===== RequestId extractor tests =====

    #[tokio::test]
    async fn test_request_id_extractor_generates_uuid_without_middleware() {
        // Handlers must work without the observability middleware —
        // the extractor falls back to a fresh UUID instead of failing.
        async fn id_handler(RequestId(id): RequestId) -> String {
            id
        }
        let app = axum::Router::new().route("/test", axum::routing::get(id_handler));

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let id = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(id.len(), 36, "should fall back to UUID v4 (36 chars), got: {}", id);
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
}
