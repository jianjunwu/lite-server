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
use axum::middleware::Next;
use axum::response::Response;
use axum::Router;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

async fn disable_keepalive_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        CONNECTION,
        axum::http::HeaderValue::from_static("close"),
    );
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
) -> Result<(), AppError> {
    let repo_path = PathBuf::from(&config.model_repository.path);
    let state = AppState::new(registry, worker_manager, inference_queue, endpoint_manager, config.clone(), repo_path);

    let app = create_routes(state, endpoint_routes);
    let app = if config.server.keepalive_timeout <= 0.0 {
        app.layer(axum::middleware::from_fn(disable_keepalive_middleware))
    } else {
        app
    };

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
}
