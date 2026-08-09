use crate::admission::AdmissionCounter;
use crate::callback::CallbackRunner;
use crate::config::Config;
use crate::inference_queue::InferenceQueue;
use crate::metrics::aggregator::{AlertEngine, AlertThresholds};
use crate::rate_limit::RateLimiter;
use crate::registry::ModelRegistry;
use crate::server::ShutdownState;
use crate::worker::WorkerManager;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<ModelRegistry>,
    pub worker_manager: Arc<WorkerManager>,
    pub inference_queue: Arc<InferenceQueue>,
    pub config: Config,
    pub repo_path: PathBuf,
    pub alert_engine: Arc<AlertEngine>,
    pub shutdown_state: Arc<ShutdownState>,
    /// C3 (P4-2): set true at the start of graceful shutdown so /livez, /readyz
    /// fail (LB摘流) and new inference is rejected with 503 while in-flight work
    /// drains. Defaults false; overridden by start_http_server.
    pub draining: Arc<AtomicBool>,
    pub callback_runner: Arc<CallbackRunner>,
    pub has_hot_reload: Arc<AtomicBool>,
    pub rate_limiter: Arc<RateLimiter>,
    /// P-FLOW (§4.0.9): global in-flight admission cap for inference
    /// requests. Health/admin traffic is exempt (enforced by the HTTP
    /// middleware / gRPC handler which classify the endpoint).
    pub admission: AdmissionCounter,
    /// D27 审计的 key 指纹来源。默认全 Unconfigured（admin fail-closed），
    /// 启动路径以解析后的实例覆盖（http/mod.rs）——直构 AppState 的单测
    /// 无需注入也能用（指纹字段为 None）。
    pub access_control: Arc<crate::access_control::AccessControl>,
}

impl AppState {
    // allow: 构造器参数与字段一一对应(共享状态组装),收参数对象等于再定义
    // 一个同形 struct,无消歧收益。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        inference_queue: Arc<InferenceQueue>,
        config: Config,
        repo_path: PathBuf,
        callback_runner: Arc<CallbackRunner>,
        has_hot_reload: Arc<AtomicBool>,
        rate_limiter: Arc<RateLimiter>,
    ) -> Self {
        let admission = AdmissionCounter::new(config.server.max_inflight);
        Self {
            registry,
            worker_manager,
            inference_queue,
            config,
            repo_path,
            alert_engine: Arc::new(AlertEngine::new(AlertThresholds::default())),
            shutdown_state: Arc::new(ShutdownState::new()),
            draining: Arc::new(AtomicBool::new(false)),
            callback_runner,
            has_hot_reload,
            rate_limiter,
            admission,
            access_control: Arc::new(crate::access_control::AccessControl::default()),
        }
    }
}

impl crate::http::handlers::HasBodyLimit for std::sync::Arc<AppState> {
    fn max_body_bytes(&self) -> usize {
        self.config
            .server
            .max_request_body_bytes
            .unwrap_or(64 * 1024 * 1024)
    }
}
