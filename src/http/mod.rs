pub mod handlers;
pub mod routes;
pub mod state;

use crate::config::Config;
use crate::error::AppError;
use crate::http::handlers::*;
use crate::http::routes::create_routes;
use crate::http::state::AppState;
use crate::inference_queue::InferenceQueue;
use crate::metrics::prometheus;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use crate::worker::endpoint_manager::EndpointManager;
use axum::Router;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, error};

pub async fn start_http_server(
    config: Config,
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    inference_queue: Arc<InferenceQueue>,
    endpoint_manager: Option<Arc<EndpointManager>>,
    endpoint_routes: Vec<crate::worker::protocol::EndpointRoute>,
) -> Result<(), AppError> {
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.http_port)
        .parse()
        .map_err(|e| AppError::Config(format!("invalid address: {}", e)))?;

    let repo_path = PathBuf::from(&config.model_repository.path);
    let state = AppState::new(registry, worker_manager, inference_queue, endpoint_manager, config.clone(), repo_path);

    let app = create_routes(state, endpoint_routes);

    info!("Starting HTTP server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| AppError::Io(e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| AppError::Internal(format!("server error: {}", e)))?;

    Ok(())
}
