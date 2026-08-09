//! /audit 举证测试——P-MW 中间件收敛面（蓝图 §4.0.1/§4.0.2/§4.0.3/§4.0.5）。
//! 命名 test_audit_<维度>_<场景>；在当前代码上 FAIL，证明缺陷存在；修复后转绿
//! 作为回归锁。仅新增测试，不改实现。

use lite_server::access_control::{AccessControl, EndpointClass};
use lite_server::callback::CallbackRunner;
use lite_server::config::{AccessControlConfig, Config};
use lite_server::grpc::interceptor::service_interceptor;
use lite_server::grpc::{GrpcService, GrpcServiceDeps, LiteServer};
use lite_server::http::state::AppState;
use lite_server::inference_queue::InferenceQueue;
use lite_server::proto::liteserver as pb;
use lite_server::rate_limit::RateLimiter;
use lite_server::registry::ModelRegistry;
use lite_server::request_context::RequestContext;
use lite_server::worker::WorkerManager;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

fn build_grpc_service(max_inflight: usize) -> (GrpcService, Arc<AppState>) {
    let registry = Arc::new(ModelRegistry::new());
    let queue = Arc::new(InferenceQueue::new());
    let wm = Arc::new(WorkerManager::new(
        registry.clone(),
        std::env::temp_dir(),
        queue.clone(),
        "error".to_string(),
        Arc::new(CallbackRunner::new()),
    ));
    let mut config = Config::default();
    config.server.max_inflight = max_inflight;
    let app_state = Arc::new(AppState::new(
        registry.clone(),
        wm.clone(),
        queue,
        config,
        std::env::temp_dir(),
        Arc::new(CallbackRunner::new()),
        Arc::new(AtomicBool::new(false)),
        Arc::new(RateLimiter::default()),
    ));
    let service = GrpcService::new(GrpcServiceDeps {
        registry,
        worker_manager: wm,
        streaming_metrics: false,
        canary_override: false,
        grpc_streaming: true,
        callback_runner: Arc::new(CallbackRunner::new()),
        shutdown_state: Arc::new(lite_server::server::ShutdownState::new()),
        server_timeout: Duration::from_secs(5),
        rate_limiter: Arc::new(RateLimiter::default()),
        decoupled_idle_timeout: None,
        app_state: app_state.clone(),
        trusted: Arc::new(Vec::new()),
    });
    (service, app_state)
}

/// 举证 B2（控制流/字段填充遗漏）：gRPC Admin 服务的 interceptor 对缺省的
/// request_id 不做 UUID v4 兜底——inference 路径由 handler 的 finalize_context
/// 兜底、HTTP 由 observability_middleware 兜底，Admin/health 无 finalize 步骤，
/// D27 审计记录（admin.rs `audit()` 读 cx.request_id）恒为空串。
#[test]
fn test_audit_control_grpc_admin_interceptor_generates_request_id_when_absent() {
    let ac = Arc::new(AccessControl::build(&AccessControlConfig::default()).unwrap());
    let mut interceptor = service_interceptor(ac, EndpointClass::Admin, Arc::new(Vec::new()));
    // 无 metadata、无 remote_addr（UDS 形态）：unconfigured admin → loopback
    // fail-closed → 本请求应放行。
    let request =
        interceptor(tonic::Request::new(())).expect("unconfigured admin + no peer is loopback");
    let cx = request
        .extensions()
        .get::<RequestContext>()
        .expect("interceptor must fill RequestContext");
    assert!(
        uuid::Uuid::parse_str(&cx.request_id).is_ok(),
        "蓝图 §4.0.1 + D27：request_id 须 UUID v4 兜底（HTTP observability 与 gRPC \
         inference finalize 均如此）；Admin 审计需要可关联的 request_id，实际为空串 {:?}",
        cx.request_id
    );
}

/// 举证 B4（双栈 parity / 错误路径回显缺失）：gRPC admission 拒绝
/// （max_inflight 超限 → Unavailable）发生在 handler 的 echo 包装之前 → 响应
/// metadata 无 x-request-id / x-processing-time-ms。HTTP 同场景 503 经最外层
/// observability 恒带回显；P2-2 明确「错误路径同样回显」。
#[tokio::test]
async fn test_audit_parity_grpc_admission_rejection_echoes_request_id() {
    let (service, app_state) = build_grpc_service(1);
    // 占满唯一 admission 槽位。
    let _held = app_state.admission.try_acquire().expect("cap=1 admits one");

    let mut request = tonic::Request::new(pb::InferRequest {
        model_name: "any".to_string(),
        version: String::new(),
        data: Vec::new().into(),
        headers: Default::default(),
        sequence_id: None,
    });
    request.metadata_mut().insert(
        tonic::metadata::MetadataKey::from_bytes(b"x-client-request-id").unwrap(),
        "echo-rid-admission".parse().unwrap(),
    );

    let err = LiteServer::infer(&service, request)
        .await
        .expect_err("saturated admission must reject");
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert_eq!(
        err.metadata().get("x-request-id").and_then(|v| v.to_str().ok()),
        Some("echo-rid-admission"),
        "P2-2（蓝图 §4.1）：错误路径须回显 x-request-id（对齐 HTTP observability 错误路径）；\
         admission 拒绝发生在 echo 包装之前，当前无回显"
    );
    assert!(
        err.metadata().contains_key("x-processing-time-ms"),
        "P2-2：错误路径须回显 x-processing-time-ms"
    );
}
