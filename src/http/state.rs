use crate::callback::CallbackRunner;
use crate::config::Config;
use crate::inference_queue::InferenceQueue;
use crate::metrics::aggregator::{AlertEngine, AlertThresholds};
use crate::registry::ModelRegistry;
use crate::server::ShutdownState;
use crate::worker::WorkerManager;
use crate::worker::endpoint_manager::EndpointManager;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<ModelRegistry>,
    pub worker_manager: Arc<WorkerManager>,
    pub inference_queue: Arc<InferenceQueue>,
    pub endpoint_manager: Option<Arc<EndpointManager>>,
    pub config: Config,
    pub repo_path: PathBuf,
    pub alert_engine: Arc<AlertEngine>,
    pub shutdown_state: Arc<ShutdownState>,
    pub callback_runner: Arc<CallbackRunner>,
}

impl AppState {
    pub fn new(
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        inference_queue: Arc<InferenceQueue>,
        endpoint_manager: Option<Arc<EndpointManager>>,
        config: Config,
        repo_path: PathBuf,
        callback_runner: Arc<CallbackRunner>,
    ) -> Self {
        Self {
            registry,
            worker_manager,
            inference_queue,
            endpoint_manager,
            config,
            repo_path,
            alert_engine: Arc::new(AlertEngine::new(AlertThresholds::default())),
            shutdown_state: Arc::new(ShutdownState::new()),
            callback_runner,
        }
    }
}
