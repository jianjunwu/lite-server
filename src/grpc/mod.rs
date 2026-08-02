use crate::access_control::EndpointClass;
use crate::callback::CallbackRunner;
use crate::error::AppError;
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;
use tracing::Instrument;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

pub mod admin;
pub mod interceptor;

mod auth;
mod canary;
mod error;
mod metadata;
mod reflection;
mod rpc;

pub use pb::lite_server_server::{LiteServer, LiteServerServer};
pub(crate) use error::{app_error_to_grpc_status, err, with_retry_after};
use error::grpc_code_to_status_family;
use metadata::{echo_grpc_response_headers, metadata_request_id, record_grpc_request_end};

/// Shared state for the gRPC service.
#[derive(Clone)]
/// RAII in-flight guard (P4-2): inc on creation, dec on drop. Mirrors the HTTP
/// middleware so gRPC inference counts toward the graceful-shutdown `pending`
/// tally. For unary RPCs the guard spans the whole handler; for streaming RPCs
/// it is dropped when the handler returns the stream (the open phase) — the
/// long-lived stream itself is drained by `serve_with_shutdown` + the
/// `graceful_timeout` backstop, not by this observability counter.
struct InflightGuard(Arc<crate::server::ShutdownState>);
impl InflightGuard {
    fn new(state: Arc<crate::server::ShutdownState>) -> Self {
        state.inc_pending();
        Self(state)
    }
}
impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.dec_pending();
    }
}

pub struct GrpcService {
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    streaming_metrics: bool,
    /// P5-2 (蓝图 §4.4, D16): features.canary_override——false（默认）时
    /// `x-lite-version` pin 被忽略（debug 日志），true 时参与版本解析
    /// （优先级：显式 version > pin > routing_pick > active，与 HTTP 一致）。
    canary_override: bool,
    callback_runner: Arc<CallbackRunner>,
    /// Graceful-shutdown in-flight tracker (P4-2). Held by the service so every
    /// inference handler can inc on entry / dec on exit — mirrors the HTTP
    /// middleware; together they make the drain-time `pending` count accurate.
    shutdown_state: Arc<crate::server::ShutdownState>,
    /// Per-request inference deadline. Mirrors the REST path's
    /// `config.server.timeout` so gRPC and HTTP share one request budget.
    server_timeout: Duration,
    /// P9-1 DecoupledInfer: server-side idle timeout for a decoupled stream.
    /// None = disabled (stream lives until model close / client cancel).
    /// Derived from `config.server.decoupled_idle_timeout_secs` (0 → None).
    decoupled_idle_timeout: Option<Duration>,
    /// Shared per-instance rate limiter（P3-1：构造上移 server/mod.rs，HTTP/gRPC
    /// 共用同一实例 + 60s cleanup task）。进程内 DashMap → per-instance（多副本
    /// 实际限额 = N×配置值；全局限流属上游网关职责，§4.1 P3-1 评审 2.2）。
    rate_limiter: Arc<crate::rate_limit::RateLimiter>,
    /// P-ENSEMBLE-GRPC (蓝图 §4.1, D23): full AppState to dispatch ensemble
    /// models through `execute_ensemble`. Ensemble models have no workers, so
    /// unary infer must route to the DAG executor instead of the worker queue
    /// — mirroring HTTP `do_infer`. Built once in `start_grpc_server` from the
    /// same shared pieces (registry/worker_manager/queue/config/repo_path/…),
    /// overriding `shutdown_state` to the real in-flight tracker.
    app_state: Arc<AppState>,
    /// P-XFF: trusted-proxy CIDRs for client-IP cleansing (parsed once at
    /// startup from `server.trusted_proxies`). Empty → fail-safe (gRPC TCP
    /// peer used, client XFF/X-Real-IP ignored — prevents forged-IP
    /// rate-limit bypass). Consumed by `finalize_context` in every handler.
    trusted: Arc<crate::client_ip::TrustedNetworks>,
}

impl GrpcService {
    pub fn new(
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        streaming_metrics: bool,
        canary_override: bool,
        callback_runner: Arc<CallbackRunner>,
        shutdown_state: Arc<crate::server::ShutdownState>,
        server_timeout: Duration,
        rate_limiter: Arc<crate::rate_limit::RateLimiter>,
        decoupled_idle_timeout: Option<Duration>,
        app_state: Arc<AppState>,
        trusted: Arc<crate::client_ip::TrustedNetworks>,
    ) -> Self {
        Self {
            registry,
            worker_manager,
            streaming_metrics,
            canary_override,
            callback_runner,
            shutdown_state,
            server_timeout,
            rate_limiter,
            decoupled_idle_timeout,
            app_state,
            trusted,
        }
    }

    /// P-FLOW (§4.0.9): admit one inference request against the global cap.
    /// Returns an RAII guard (held for the handler scope → unary spans the
    /// full call; streaming releases on stream-open, the same header-semantic
    /// as the HTTP middleware). Rejects with Unavailable + retry-after at cap;
    /// no-op when `max_inflight` is 0 (unlimited).
    fn acquire_admission(
        &self,
    ) -> Result<crate::admission::AdmissionGuard, Status> {
        match self.app_state.admission.try_acquire() {
            Some(g) => Ok(g),
            None => {
                tracing::warn!(
                    current = self.app_state.admission.current(),
                    cap = self.app_state.admission.cap(),
                    "admission rejected: inference at max_inflight cap"
                );
                Err(with_retry_after(
                    Status::unavailable("max_inflight capacity reached"),
                    1,
                ))
            }
        }
    }
}

#[tonic::async_trait]
impl LiteServer for GrpcService {
    async fn infer(
        &self,
        request: Request<pb::InferRequest>,
    ) -> Result<Response<pb::InferResponse>, Status> {
        let _guard = InflightGuard::new(self.shutdown_state.clone());
        // P-FLOW (§4.0.9): global in-flight admission cap (health/admin RPCs
        // are separate services and never reach here). Held for the handler
        // scope; streaming releases on stream-open (header-semantic).
        let _admission = self.acquire_admission()?;
        // P2-1 请求指标：成功/失败统一在此记一次（version label 取解析后版本，
        // 解析失败保持请求原值；D5 无 protocol label，与 HTTP 共享计数）。
        // P2-2 回显：request_id/processing-time 注入响应或错误 metadata（对齐
        // HTTP observability_middleware 错误路径回显）。
        // P2-3 span：覆盖 handler 全程（字段与 HTTP info_span! 一致）。
        let start = Instant::now();
        let model_label = request.get_ref().model_name.clone();
        let span_version = if request.get_ref().version.is_empty() {
            "auto".to_string()
        } else {
            request.get_ref().version.clone()
        };
        let span_rid = metadata_request_id(request.metadata());
        let span = tracing::info_span!(
            "inference",
            model = %model_label,
            version = %span_version,
            request_id = %span_rid,
            // P5-2: canary pin 命中时由 canary_pin record（蓝图 §4.4）。
            pinned_version = tracing::field::Empty,
        );
        // P-TRACE: link the inference span to the inbound trace (D21 — read the
        // interceptor-stashed RequestContext; no second propagator extract).
        if let Some(rc) = request.extensions().get::<crate::request_context::RequestContext>() {
            crate::telemetry::link_parent(&span, &rc.trace_cx);
        }
        let mut version_label = request.get_ref().version.clone();
        let mut request_id = String::new();
        let result = self
            .infer_impl(request, &mut version_label, &mut request_id)
            .instrument(span)
            .await;
        record_grpc_request_end(&model_label, &version_label, start, &result);
        echo_grpc_response_headers(result, &request_id, start)
    }

    async fn batch_infer(
        &self,
        request: Request<pb::BatchInferRequest>,
    ) -> Result<Response<pb::BatchInferResponse>, Status> {
        let _guard = InflightGuard::new(self.shutdown_state.clone());
        // P-FLOW (§4.0.9): global in-flight admission cap (health/admin RPCs
        // are separate services and never reach here). Held for the handler
        // scope; streaming releases on stream-open (header-semantic).
        let _admission = self.acquire_admission()?;
        // P2-1 请求指标 + P2-2 回显 + P2-3 span（同 infer 包装）。
        let start = Instant::now();
        let model_label = request.get_ref().model_name.clone();
        let span_version = if request.get_ref().version.is_empty() {
            "auto".to_string()
        } else {
            request.get_ref().version.clone()
        };
        let span_rid = metadata_request_id(request.metadata());
        let span = tracing::info_span!(
            "inference",
            model = %model_label,
            version = %span_version,
            request_id = %span_rid,
            // P5-2: canary pin 命中时由 canary_pin record（蓝图 §4.4）。
            pinned_version = tracing::field::Empty,
        );
        // P-TRACE: link the inference span to the inbound trace (D21 — read the
        // interceptor-stashed RequestContext; no second propagator extract).
        if let Some(rc) = request.extensions().get::<crate::request_context::RequestContext>() {
            crate::telemetry::link_parent(&span, &rc.trace_cx);
        }
        let mut version_label = request.get_ref().version.clone();
        let mut request_id = String::new();
        let result = self
            .batch_infer_impl(request, &mut version_label, &mut request_id)
            .instrument(span)
            .await;
        record_grpc_request_end(&model_label, &version_label, start, &result);
        echo_grpc_response_headers(result, &request_id, start)
    }

    type StreamInferStream = ReceiverStream<Result<pb::StreamChunk, Status>>;

    async fn stream_infer(
        &self,
        request: Request<pb::StreamInferRequest>,
    ) -> Result<Response<Self::StreamInferStream>, Status> {
        let _guard = InflightGuard::new(self.shutdown_state.clone());
        // P-FLOW (§4.0.9): global in-flight admission cap (health/admin RPCs
        // are separate services and never reach here). Held for the handler
        // scope; streaming releases on stream-open (header-semantic).
        let _admission = self.acquire_admission()?;
        // P2-1 请求指标：open 失败在此记一次；open 成功后由转发 task 在流
        // 关闭处记一次整体 duration（蓝图 §4.3 P2-1 stream/bidi 语义）。
        // P2-2 回显：注入 stream open 的 initial metadata（processing-time 为
        // 开流耗时，蓝图 §4.0.4）。P2-3 span 同 infer。
        let start = Instant::now();
        let model_label = request.get_ref().model_name.clone();
        let span_version = if request.get_ref().version.is_empty() {
            "auto".to_string()
        } else {
            request.get_ref().version.clone()
        };
        let span_rid = metadata_request_id(request.metadata());
        let span = tracing::info_span!(
            "inference",
            model = %model_label,
            version = %span_version,
            request_id = %span_rid,
            // P5-2: canary pin 命中时由 canary_pin record（蓝图 §4.4）。
            pinned_version = tracing::field::Empty,
        );
        // P-TRACE: link the inference span to the inbound trace (D21 — read the
        // interceptor-stashed RequestContext; no second propagator extract).
        if let Some(rc) = request.extensions().get::<crate::request_context::RequestContext>() {
            crate::telemetry::link_parent(&span, &rc.trace_cx);
        }
        let mut version_label = request.get_ref().version.clone();
        let mut request_id = String::new();
        let result = self
            .stream_infer_impl(request, &mut version_label, &mut request_id, start)
            .instrument(span)
            .await;
        if let Err(s) = &result {
            crate::metrics::prometheus::record_request_end(
                &model_label,
                &version_label,
                grpc_code_to_status_family(s.code()),
                start.elapsed().as_secs_f64(),
            );
        }
        echo_grpc_response_headers(result, &request_id, start)
    }

    type DecoupledInferStream = ReceiverStream<Result<pb::DecoupledResponse, Status>>;

    async fn decoupled_infer(
        &self,
        request: Request<pb::DecoupledInferRequest>,
    ) -> Result<Response<Self::DecoupledInferStream>, Status> {
        // P9-1 DecoupledInfer (蓝图 §4.4): same InflightGuard / span / metric /
        // header-echo wrapper as stream_infer; the lifetime difference (model
        // holds the channel open past predict_decoupled) is in _impl.
        let _guard = InflightGuard::new(self.shutdown_state.clone());
        // P-FLOW (§4.0.9): global in-flight admission cap (health/admin RPCs
        // are separate services and never reach here). Held for the handler
        // scope; streaming releases on stream-open (header-semantic).
        let _admission = self.acquire_admission()?;
        let start = Instant::now();
        let model_label = request.get_ref().model_name.clone();
        let span_version = if request.get_ref().version.is_empty() {
            "auto".to_string()
        } else {
            request.get_ref().version.clone()
        };
        let span_rid = metadata_request_id(request.metadata());
        let span = tracing::info_span!(
            "inference",
            model = %model_label,
            version = %span_version,
            request_id = %span_rid,
            method = "decoupled_infer",
            pinned_version = tracing::field::Empty,
        );
        // P-TRACE: link the inference span to the inbound trace (D21 — read the
        // interceptor-stashed RequestContext; no second propagator extract).
        if let Some(rc) = request.extensions().get::<crate::request_context::RequestContext>() {
            crate::telemetry::link_parent(&span, &rc.trace_cx);
        }
        let mut version_label = request.get_ref().version.clone();
        let mut request_id = String::new();
        let result = self
            .decoupled_infer_impl(request, &mut version_label, &mut request_id, start)
            .instrument(span)
            .await;
        if let Err(s) = &result {
            crate::metrics::prometheus::record_request_end(
                &model_label,
                &version_label,
                grpc_code_to_status_family(s.code()),
                start.elapsed().as_secs_f64(),
            );
        }
        echo_grpc_response_headers(result, &request_id, start)
    }

    type BidiStreamStream = ReceiverStream<Result<pb::BidiChunk, Status>>;

    async fn bidi_stream(
        &self,
        request: Request<Streaming<pb::BidiChunk>>,
    ) -> Result<Response<Self::BidiStreamStream>, Status> {
        let _guard = InflightGuard::new(self.shutdown_state.clone());
        // P-FLOW (§4.0.9): global in-flight admission cap (health/admin RPCs
        // are separate services and never reach here). Held for the handler
        // scope; streaming releases on stream-open (header-semantic).
        let _admission = self.acquire_admission()?;
        // P2-1 请求指标 + P2-2 回显（同 stream_infer；model 在 BidiOpen 前未知，
        // 早期失败以空 label 记录；request_id 来自 transport metadata）。
        let start = Instant::now();
        let mut model_label = String::new();
        let mut version_label = String::new();
        let mut request_id = String::new();
        let result = self
            .bidi_stream_impl(
                request,
                &mut model_label,
                &mut version_label,
                &mut request_id,
                start,
            )
            .await;
        if let Err(s) = &result {
            crate::metrics::prometheus::record_request_end(
                &model_label,
                &version_label,
                grpc_code_to_status_family(s.code()),
                start.elapsed().as_secs_f64(),
            );
        }
        echo_grpc_response_headers(result, &request_id, start)
    }
}

/// Resolve the effective gRPC bind host (P4-1).
///
/// - `grpc.host` set → use it verbatim (`unix:/path` ⇒ UDS, else a TCP host).
/// - `grpc.host` None + `server.host` is a UDS (`unix:/path`) → gRPC cannot
///   share the HTTP socket, fall back to TCP `127.0.0.1`.
/// - `grpc.host` None + `server.host` is TCP → follow `server.host`.
pub(crate) fn resolve_grpc_host(grpc_host: Option<&str>, server_host: &str) -> String {
    match grpc_host {
        Some(h) => h.to_string(),
        None => match crate::config::unix_socket_path(server_host) {
            Some(_) => "127.0.0.1".to_string(),
            None => server_host.to_string(),
        },
    }
}

/// Start the gRPC server.
pub async fn start_grpc_server(
    host: String,
    port: u16,
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    streaming_metrics: bool,
    canary_override: bool,
    callback_runner: Arc<CallbackRunner>,
    shutdown_state: Arc<crate::server::ShutdownState>,
    server_timeout: Duration,
    grpc_config: crate::config::GrpcConfig,
    rate_limiter: Arc<crate::rate_limit::RateLimiter>,
    tls: Option<Arc<crate::tls::TlsConfigStore>>,
    config: crate::config::Config,
    has_hot_reload: Arc<std::sync::atomic::AtomicBool>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), AppError> {
    // P-ENSEMBLE-GRPC (蓝图 §4.1): build an AppState so the unary infer handler
    // can dispatch ensemble models through execute_ensemble (ensemble models have
    // no workers). Built from the same shared pieces as HTTP's AppState; the
    // shutdown_state is overridden to the real in-flight tracker so ensemble
    // sub-step queue submits and the handler's InflightGuard share one tally.
    let repo_path = PathBuf::from(&config.model_repository.path);
    let mut app_state = AppState::new(
        registry.clone(),
        worker_manager.clone(),
        worker_manager.inference_queue().clone(),
        config.clone(),
        repo_path,
        callback_runner.clone(),
        has_hot_reload.clone(),
        rate_limiter.clone(),
    );
    app_state.shutdown_state = shutdown_state.clone();
    let app_state = Arc::new(app_state);

    // P7-1 (蓝图 §4.2): endpoint-class access control — value_env/value_file
    // resolved here so a missing source fails fast at startup. Each service
    // mounts the interceptor with its own class (挂载矩阵 §4.0.3).
    let access_control = Arc::new(
        crate::access_control::AccessControl::build(&config.access_control)?,
    );

    // P-XFF: parse trusted-proxy CIDRs once (fail-fast on a bad entry). Shared
    // by the service interceptors and the handler-side `finalize_context`.
    let trusted = Arc::new(config.server.trusted_networks()?);

    let service = GrpcService::new(
        registry.clone(),
        worker_manager.clone(),
        streaming_metrics,
        canary_override,
        callback_runner.clone(),
        shutdown_state,
        server_timeout,
        rate_limiter,
        // P9-1: decoupled stream idle timeout (0 → disabled / None).
        if config.server.decoupled_idle_timeout_secs > 0.0 {
            Some(Duration::from_secs_f32(config.server.decoupled_idle_timeout_secs))
        } else {
            None
        },
        app_state,
        trusted.clone(),
    );
    let max_request_body_bytes = config.server.max_request_body_bytes;
    let server = LiteServerServer::new(service);
    // P1-3: gzip response compression is opt-in and applies to the
    // LiteServer inference service only (Admin/health stay uncompressed).
    let server = if grpc_config.response_compression {
        server
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
    } else {
        server
    };
    // P-FLOW (§4.0.9): per-request decode cap. Oversized messages decode-fail
    // with ResourceExhausted (tonic's fixed mapping). None = tonic default 4MB.
    let server = if let Some(n) = max_request_body_bytes {
        server.max_decoding_message_size(n)
    } else {
        server
    };
    // P-MW (蓝图 §4.0.3, D20) + P7-1: pre-decode interceptor fills RequestContext
    // into request extensions AND enforces endpoint-class access control. The
    // LiteServer inference service carries the Inference class. Pre-call semantics
    // only: it cannot touch responses/Status, so echo (P2-2) and error logging
    // (P1-1) stay in handlers. Transparent to unary/stream/bidi.
    let server = tonic::codegen::InterceptedService::new(
        server,
        interceptor::service_interceptor(access_control.clone(), EndpointClass::Inference, trusted.clone()),
    );

    // Standard gRPC health checking (grpc.health.v1): the reporter lives in
    // the WorkerManager, which syncs "" and per-model services on every
    // status transition and coordinator tick (phase 3).
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    worker_manager.set_grpc_health_reporter(health_reporter).await;
    worker_manager.sync_grpc_health().await;
    let health_service = tonic::codegen::InterceptedService::new(
        health_service,
        interceptor::service_interceptor(access_control.clone(), EndpointClass::Health, trusted.clone()),
    );

    // P1-2: HTTP/2 keepalive / window / frame tuning — applied to every tonic
    // server below (main, and the admin server when grpc.admin_bind is set: P7-2).
    let make_builder = || {
        let mut builder = tonic::transport::Server::builder();
        if let Some((interval, timeout)) = http2_keepalive_params(&grpc_config) {
            builder = builder
                .http2_keepalive_interval(Some(interval))
                .http2_keepalive_timeout(Some(timeout));
        }
        if grpc_config.http2_adaptive_window {
            builder = builder.http2_adaptive_window(Some(true));
        }
        if let Some(max_frame_size) = grpc_config.http2_max_frame_size {
            builder = builder.max_frame_size(Some(max_frame_size));
        }
        builder
    };

    // P6 Admin service (蓝图 §4.1): mirrors the HTTP admin REST handlers.
    // Built from the same injected state as the inference service.
    let admin_service = crate::grpc::admin::GrpcAdminService::new(
        registry.clone(),
        worker_manager.clone(),
        callback_runner.clone(),
        Arc::new(config),
        has_hot_reload,
    );
    let admin_server = crate::grpc::admin::AdminServer::new(admin_service);
    // P-FLOW (§4.0.9): same per-request decode cap as inference.
    let admin_server = if let Some(n) = max_request_body_bytes {
        admin_server.max_decoding_message_size(n)
    } else {
        admin_server
    };
    // P-MW 挂载矩阵 (§4.0.3) + P7-1: Admin service 挂 service_interceptor——
    // request_id / mTLS principal 供审计日志（D27），admin 类 access_control
    // fail-closed（未配置仅 loopback；D14）。
    let admin_server = tonic::codegen::InterceptedService::new(
        admin_server,
        interceptor::service_interceptor(access_control.clone(), EndpointClass::Admin, trusted.clone()),
    );

    // P7-2 admin_bind (蓝图 §4.2): when unset, a single server serves all three
    // services (unchanged behavior). When set, Admin splits onto a second server
    // (Admin + health) bound to admin_bind — the main port keeps LiteServer +
    // health, so Admin RPCs are reachable ONLY via admin_bind (transport
    // isolation layered on P7-1's class isolation). Both servers share state and
    // observe the same shutdown signal. admin_server is conditionally consumed
    // (into main when no admin_bind, into the admin server otherwise), so it is
    // staged in an Option and taken.
    let admin_bind = grpc_config.admin_bind.clone();
    let mut admin_server_opt = Some(admin_server);
    let main_router = make_builder()
        .add_service(server)
        .add_service(health_service.clone());
    let main_router = if admin_bind.is_none() {
        main_router.add_service(admin_server_opt.take().expect("admin_server staged"))
    } else {
        main_router
    };

    // gRPC reflection（评审低#12, opt-in，见 grpc/reflection.rs）：grpc.reflection
    // =true 时挂载 v1 reflection——注册 liteserver proto + grpc.health.v1 描述符；
    // 挂 Admin 访问类（schema 元数据属 admin 面，fail-closed 仅 loopback）。
    let build_reflection = || -> Result<_, AppError> {
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(crate::proto::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| AppError::Internal(format!("gRPC reflection build: {e}")))
    };
    let main_router = if grpc_config.reflection {
        main_router.add_service(tonic::codegen::InterceptedService::new(
            build_reflection()?,
            interceptor::service_interceptor(
                access_control.clone(),
                EndpointClass::Admin,
                trusted.clone(),
            ),
        ))
    } else {
        main_router
    };

    // P4-2: one shutdown signal shared by every server (main + admin). Shared so
    // both observers fire on the single shutdown_rx; tls_incoming also takes a clone.
    let shutdown = futures::FutureExt::shared(async move {
        let _ = shutdown_rx.await;
    });

    let main_fut = serve_grpc_router(
        main_router,
        host,
        port,
        tls,
        grpc_config.socket_mode,
        false,
        "gRPC",
        shutdown.clone(),
    );

    if let Some(admin_bind) = admin_bind {
        let (admin_host, admin_port, admin_mode) = resolve_admin_bind(&admin_bind)?;
        // admin_bind never uses TLS (it is for local/loopback isolation; TLS is
        // the main port's concern). Admin UDS is forced owner-only 0o600 so a
        // world-writable admin socket cannot let any local user bypass fail-closed.
        let admin_router = make_builder()
            .add_service(admin_server_opt.take().expect("admin_server staged"))
            .add_service(health_service);
        // admin_bind 分离时 admin 面独立成服务组——reflection 同样挂载在此，
        // 使 grpcurl 对 admin_bind 也可发现 Admin/health。
        let admin_router = if grpc_config.reflection {
            admin_router.add_service(tonic::codegen::InterceptedService::new(
                build_reflection()?,
                interceptor::service_interceptor(
                    access_control.clone(),
                    EndpointClass::Admin,
                    trusted.clone(),
                ),
            ))
        } else {
            admin_router
        };
        let admin_fut = serve_grpc_router(
            admin_router,
            admin_host,
            admin_port,
            None,
            admin_mode,
            true,
            "gRPC admin",
            shutdown.clone(),
        );
        // Run both servers concurrently until shutdown; either erroring fails startup.
        let (main_res, admin_res) = tokio::join!(main_fut, admin_fut);
        main_res?;
        admin_res?;
    } else {
        main_fut.await?;
    }

    Ok(())
}

/// Resolve a `grpc.admin_bind` target into (host, port, socket_mode) (P7-2). A
/// `unix:/path` target carries owner-only 0o600 — a world-writable admin socket
/// would let any local user reach admin and bypass P7-1's fail-closed (评审 1.4).
/// A TCP `host:port` is split; socket_mode is unused on TCP (returned 0o600 for
/// uniformity with the UDS branch).
fn resolve_admin_bind(admin_bind: &str) -> Result<(String, u16, u32), AppError> {
    if crate::config::unix_socket_path(admin_bind).is_some() {
        return Ok((admin_bind.to_string(), 0, 0o600));
    }
    let (host, port_s) = admin_bind.rsplit_once(':').ok_or_else(|| {
        AppError::Config(format!(
            "grpc.admin_bind '{}' must be 'host:port' or 'unix:/path'",
            admin_bind
        ))
    })?;
    let port: u16 = port_s.parse().map_err(|_| {
        AppError::Config(format!("grpc.admin_bind '{}' has an invalid port", admin_bind))
    })?;
    Ok((host.to_string(), port, 0o600))
}

/// Serve a tonic router on a bind target (P7-2 factored out of the single-server
/// path so the main and admin servers share it). `host` is `unix:/path` (port
/// ignored) or a TCP host. `owner_only` (admin UDS) additionally requires the
/// bound socket be owned by the current process with no group/other permission
/// bits, so a misconfigured admin socket cannot weaken fail-closed.
async fn serve_grpc_router(
    router: tonic::transport::server::Router,
    host: String,
    port: u16,
    tls: Option<Arc<crate::tls::TlsConfigStore>>,
    socket_mode: u32,
    owner_only: bool,
    label: &str,
    shutdown: impl std::future::Future<Output = ()> + Send + Clone + 'static,
) -> Result<(), AppError> {
    if let Some(path) = crate::config::unix_socket_path(&host) {
        #[cfg(unix)]
        {
            // Defensive (symlink safety): only clear our OWN stale socket from an
            // unclean exit; never remove one owned by another user.
            if std::path::Path::new(path).exists() {
                check_uds_owner(path, label)?;
            }
            let _ = std::fs::remove_file(path);
            let listener = tokio::net::UnixListener::bind(path).map_err(|e| {
                AppError::Config(format!("failed to bind {} UDS {}: {}", label, path, e))
            })?;
            chmod_uds(path, socket_mode, label)?;
            if owner_only {
                enforce_owner_only_uds(path, label)?;
            }
            tracing::info!("Starting {} on unix:{}", label, path);
            // UnixListenerStream yields bare AsyncRead+AsyncWrite streams — exactly
            // what serve_with_incoming wants (tonic provides `Connected` for UnixStream).
            let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
            router
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .map_err(|e| AppError::Internal(format!("{} server error: {}", label, e)))?;
        }
        #[cfg(not(unix))]
        {
            return Err(AppError::Config(format!(
                "{} host '{}' requires Unix domain sockets, which are not supported on this \
                 platform; set it to a TCP host:port instead",
                label, host
            )));
        }
    } else if let Some(tls_store) = tls {
        // P5-1: TLS/mTLS termination over TCP (main port only). Our own incoming
        // (tls.rs) terminates TLS per connection from the rotating store, so the
        // cert reloader's swap applies to the NEXT handshake; tonic's blanket
        // `Connected for TlsStream<TcpStream>` keeps remote_addr() and peer certs.
        let addr: std::net::SocketAddr = format!("{}:{}", host, port)
            .parse()
            .map_err(|e| AppError::Config(format!("invalid {} address: {}", label, e)))?;
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(AppError::Io)?;
        tracing::info!("Starting {} on {} (TLS, {})", label, addr, tls_store.describe());
        let incoming = crate::tls::tls_incoming(listener, tls_store, shutdown.clone());
        router
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
            .map_err(|e| AppError::Internal(format!("{} server error: {}", label, e)))?;
    } else {
        let addr: std::net::SocketAddr = format!("{}:{}", host, port)
            .parse()
            .map_err(|e| AppError::Config(format!("invalid {} address: {}", label, e)))?;
        tracing::info!("Starting {} on {}", label, addr);
        // P4-2: graceful shutdown — tonic stops accepting new connections (sends
        // GOAWAY) and drains in-flight RPCs; bounded by the caller's
        // graceful_timeout + abort backstop.
        router
            .serve_with_shutdown(addr, shutdown)
            .await
            .map_err(|e| AppError::Internal(format!("{} server error: {}", label, e)))?;
    }
    Ok(())
}

#[cfg(unix)]
fn current_uid() -> u32 {
    // getuid never fails (returns the real user ID; no errno), so the FFI is safe.
    unsafe { libc::getuid() }
}

/// chmod a bound UDS to `mode` exactly (independent of the process umask).
#[cfg(unix)]
fn chmod_uds(path: &str, mode: u32, label: &str) -> Result<(), AppError> {
    let path = std::path::Path::new(path);
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = std::fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_mode(mode);
        std::fs::set_permissions(path, permissions).map_err(|e| {
            AppError::Config(format!(
                "failed to chmod {} UDS {}: {}",
                label,
                path.display(),
                e
            ))
        })?;
    }
    Ok(())
}

/// Refuse to remove a pre-existing UDS owned by another user (symlink safety).
#[cfg(unix)]
fn check_uds_owner(path: &str, label: &str) -> Result<(), AppError> {
    let path = std::path::Path::new(path);
    use std::os::unix::fs::MetadataExt;
    if let Ok(metadata) = std::fs::metadata(path) {
        let me = current_uid();
        if metadata.uid() != me {
            return Err(AppError::Config(format!(
                "refusing to remove existing {} UDS {} owned by uid {} (current uid {}); \
                 remove it manually or choose a different path",
                label,
                path.display(),
                metadata.uid(),
                me
            )));
        }
    }
    Ok(())
}

/// Admin UDS hardening (评审 1.4): after bind+chmod, the socket must be owned by
/// the current process AND have NO group/other permission bits — otherwise any
/// local user could connect and bypass admin fail-closed.
#[cfg(unix)]
fn enforce_owner_only_uds(path: &str, label: &str) -> Result<(), AppError> {
    let path = std::path::Path::new(path);
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;
    let metadata = std::fs::metadata(path).map_err(|e| {
        AppError::Config(format!("failed to stat {} UDS {}: {}", label, path.display(), e))
    })?;
    let mode = metadata.permissions().mode();
    let me = current_uid();
    if metadata.uid() != me {
        return Err(AppError::Config(format!(
            "{} UDS {} must be owned by the current process (uid {}); got uid {}",
            label,
            path.display(),
            me,
            metadata.uid()
        )));
    }
    if mode & 0o077 != 0 {
        return Err(AppError::Config(format!(
            "{} UDS {} must be owner-only (0o600); got mode 0o{:o} — group/other access would \
             let any local user bypass admin fail-closed",
            label,
            path.display(),
            mode
        )));
    }
    Ok(())
}

/// Effective HTTP/2 keepalive parameters (P1-2): `(interval, timeout)`.
/// `None` when keepalive is disabled (interval unset). The timeout defaults
/// to 20s when only the interval is configured; a timeout configured without
/// an interval can never fire, so warn at startup.
fn http2_keepalive_params(cfg: &crate::config::GrpcConfig) -> Option<(Duration, Duration)> {
    match cfg.http2_keepalive_interval_secs {
        Some(interval) => Some((
            Duration::from_secs(interval),
            Duration::from_secs(cfg.http2_keepalive_timeout_secs.unwrap_or(20)),
        )),
        None => {
            if cfg.http2_keepalive_timeout_secs.is_some() {
                warn!(
                    "grpc.http2_keepalive_timeout_secs is set but \
                     http2_keepalive_interval_secs is not — the timeout never \
                     takes effect without a ping interval"
                );
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== P7-2 resolve_admin_bind =====

    #[test]
    fn resolve_admin_bind_unix_forces_owner_only_mode() {
        let (host, port, mode) = resolve_admin_bind("unix:/tmp/admin.sock").unwrap();
        assert_eq!(host, "unix:/tmp/admin.sock");
        assert_eq!(port, 0, "port unused for a UDS target");
        assert_eq!(mode, 0o600, "admin UDS must be owner-only 0o600");
    }

    #[test]
    fn resolve_admin_bind_tcp_splits_host_port() {
        let (host, port, mode) = resolve_admin_bind("127.0.0.1:19090").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19090);
        assert_eq!(mode, 0o600, "socket_mode unused on TCP but returned 0o600");
    }

    #[test]
    fn resolve_admin_bind_rejects_missing_port_and_garbage() {
        assert!(resolve_admin_bind("127.0.0.1").is_err(), "bare host without :port is invalid");
        assert!(resolve_admin_bind("127.0.0.1:notaport").is_err(), "non-numeric port is invalid");
    }

    // --- P1-2: HTTP/2 keepalive params ---

    #[test]
    fn test_http2_keepalive_params_none_by_default() {
        let cfg = crate::config::GrpcConfig::default();
        assert_eq!(http2_keepalive_params(&cfg), None);
    }

    #[test]
    fn test_http2_keepalive_params_timeout_without_interval_never_applies() {
        // Timeout alone can never fire (no ping is ever sent) → params stay
        // disabled and startup warns.
        let cfg = crate::config::GrpcConfig {
            http2_keepalive_timeout_secs: Some(5),
            ..Default::default()
        };
        assert_eq!(http2_keepalive_params(&cfg), None);
    }

    #[test]
    fn test_http2_keepalive_params_default_timeout_20s() {
        let cfg = crate::config::GrpcConfig {
            http2_keepalive_interval_secs: Some(30),
            ..Default::default()
        };
        assert_eq!(
            http2_keepalive_params(&cfg),
            Some((Duration::from_secs(30), Duration::from_secs(20)))
        );
    }

    #[test]
    fn test_http2_keepalive_params_custom_timeout() {
        let cfg = crate::config::GrpcConfig {
            http2_keepalive_interval_secs: Some(30),
            http2_keepalive_timeout_secs: Some(5),
            ..Default::default()
        };
        assert_eq!(
            http2_keepalive_params(&cfg),
            Some((Duration::from_secs(30), Duration::from_secs(5)))
        );
    }

    // --- P4-1: gRPC host resolution (TCP follow / UDS fallback / explicit unix:) ---

    #[test]
    fn resolve_grpc_host_follows_server_host_when_unset() {
        // grpc.host None + server.host TCP → follow server.host.
        assert_eq!(
            resolve_grpc_host(None, "0.0.0.0"),
            "0.0.0.0".to_string()
        );
    }

    #[test]
    fn resolve_grpc_host_falls_back_to_loopback_when_server_is_uds() {
        // grpc.host None + server.host is a UDS → gRPC cannot share the socket,
        // fall back to loopback TCP (blueprint §4.1 P4-1).
        assert_eq!(
            resolve_grpc_host(None, "unix:/tmp/x.sock"),
            "127.0.0.1".to_string()
        );
    }

    #[test]
    fn resolve_grpc_host_explicit_unix_takes_uds() {
        // Explicit grpc.host = unix:/path → gRPC listens on that UDS.
        assert_eq!(
            resolve_grpc_host(Some("unix:/run/lite.sock"), "0.0.0.0"),
            "unix:/run/lite.sock".to_string()
        );
    }

    #[test]
    fn resolve_grpc_host_explicit_tcp_takes_that_host() {
        // Explicit grpc.host = a plain host overrides server.host.
        assert_eq!(
            resolve_grpc_host(Some("10.0.0.5"), "0.0.0.0"),
            "10.0.0.5".to_string()
        );
    }

    // ===== request_id / client_ip extraction =====
    // P-MW: the extract_* unit tests moved with the logic to
    // `grpc::interceptor` (metadata side: `RequestContext::from_grpc_metadata`;
    // post-decode fallback: `finalize_context`).

    // ===== B2: gRPC streaming bypasses ejected workers =====

    /// B2 回归守卫: gRPC streaming endpoints (`stream_infer`, `bidi_stream`)
    /// and `batch_infer` use `pick_worker_skip_ejected` for worker selection,
    /// which skips ejected (outlier) workers — 与 HTTP SSE/WS
    /// (`open_worker_stream`) 行为一致。
    ///
    /// This test guards against regression by confirming that
    /// `pick_worker_skip_ejected` is used in production code.
    #[test]
    fn test_grpc_worker_selection_skips_ejected() {
        // 模块拆分后直连 RPC 的生产代码分布在 rpc/*.rs——逐文件按 #[cfg(test)]
        // 边界截断（避免计入测试自身的提及）再拼接检查。
        let prod_source: String = [
            include_str!("rpc/batch.rs"),
            include_str!("rpc/stream.rs"),
            include_str!("rpc/decoupled.rs"),
            include_str!("rpc/bidi.rs"),
            // Task F: stream/decoupled/bidi now delegate worker selection to
            // `pick_streaming_worker`, which is where skip-ejected lives.
            include_str!("../worker/routing.rs"),
        ]
        .into_iter()
        .map(|s| {
            let boundary = s.find("#[cfg(test)]").unwrap_or(s.len());
            &s[..boundary]
        })
        .collect();

        let random_calls: Vec<&str> = prod_source
            .lines()
            .filter(|l| l.contains("pick_worker_random"))
            .collect();
        let ejected_calls: Vec<&str> = prod_source
            .lines()
            .filter(|l| l.contains("pick_worker_skip_ejected"))
            .collect();

        // 生产代码必须调用 pick_worker_skip_ejected —— gRPC 三个直连 RPC
        // (stream_infer / bidi_stream / batch_infer) 均需跳过被驱逐的 worker,
        // 与 HTTP SSE/WS 一致。
        assert!(
            !ejected_calls.is_empty(),
            "B2: gRPC streaming endpoints must skip ejected workers via \
             pick_worker_skip_ejected (parity with HTTP SSE/WS). \
             Found {} pick_worker_random calls, {} pick_worker_skip_ejected \
             calls in production code.",
            random_calls.len(),
            ejected_calls.len()
        );
    }
}

#[cfg(test)]
mod request_metrics_tests {
    //! P2-1: gRPC 4 RPC 记请求指标（与 HTTP 共享 REQUESTS_TOTAL，无 protocol
    //! label，D5）+ §4.0.9 收口（queue-full → Unavailable 落 5xx）。
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::config::ModelConfig;
    use crate::inference_queue::{InferenceQueue, OutlierState};
    use crate::metrics::prometheus::REQUESTS_TOTAL;
    use crate::registry::types::ModelType;
    use crate::transport::zmq::WorkerZmqClient;
    use bytes::Bytes;
    use prost::Message;
    use std::collections::HashMap;
    use tokio::sync::{mpsc, oneshot};

    // --- handler 级指标记录 ---

    fn metric_test_endpoint(name: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("lite-server-grpc-met-{}-{}.sock", name, std::process::id()))
                    .display()
            )
        }
        #[cfg(windows)]
        {
            format!("tcp://127.0.0.1:{}", 36000 + std::process::id() % 1000)
        }
    }

    /// PAIR worker answering every unary request with an Ok Single.
    fn spawn_ok_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resp = pb::Response {
                    uid: req.uid,
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: Bytes::from_static(b"{\"ok\":true}"),
                        status: Some(pb::Status { code: "Ok".to_string(), message: String::new() }),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        })
    }

    /// PAIR worker answering a stream Open with one Chunk + Done; other
    /// requests (e.g. the trailing Cancel) get a bare ack so the client's
    /// request/response exchange completes instead of hanging to timeout.
    fn spawn_stream_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let is_open = matches!(
                    req.payload,
                    Some(pb::request::Payload::Stream(pb::StreamRequest {
                        action: Some(pb::stream_request::Action::Open(_)),
                        ..
                    }))
                );
                if !is_open {
                    // Ack (Cancel etc.) so the awaiting send() completes.
                    let ack = pb::Response {
                        uid: req.uid,
                        ..Default::default()
                    };
                    let _ = s.send(ack.encode_to_vec(), 0);
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let mk = |payload| pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(payload),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let _ = s.send(mk(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                    data: Bytes::from_static(b"{}"),
                    is_final: false,
                })).encode_to_vec(), 0);
                let _ = s.send(mk(pb::stream_response::Payload::Done(pb::StreamDone::default()))
                    .encode_to_vec(), 0);
            }
        })
    }

    fn test_config(max_batch_size: usize, batch_timeout: f32, max_queue_size: usize) -> ModelConfig {
        ModelConfig {
            max_batch_size,
            batch_timeout,
            max_queue_size,
            health_check_interval: 0.0,
            ..Default::default()
        }
    }

    fn build_service(registry: Arc<ModelRegistry>, queue: Arc<InferenceQueue>) -> GrpcService {
        build_service_with_canary(registry, queue, false)
    }

    fn build_service_with_canary(
        registry: Arc<ModelRegistry>,
        queue: Arc<InferenceQueue>,
        canary_override: bool,
    ) -> GrpcService {
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue,
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        // P-ENSEMBLE-GRPC: GrpcService now carries an AppState for ensemble
        // dispatch. These unit tests exercise LitAPI models, so the AppState is
        // never used for ensemble — a minimal default-built one suffices.
        let app_state = Arc::new(AppState::new(
            registry.clone(),
            wm.clone(),
            wm.inference_queue().clone(),
            crate::config::Config::default(),
            std::env::temp_dir(),
            Arc::new(CallbackRunner::new()),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ));
        GrpcService::new(
            registry,
            wm,
            false,
            canary_override,
            Arc::new(CallbackRunner::new()),
            Arc::new(crate::server::ShutdownState::new()),
            Duration::from_secs(5),
            Arc::new(crate::rate_limit::RateLimiter::default()),
            None, // P9-1 decoupled idle timeout — unused in these unit tests.
            app_state,
            Arc::new(Vec::new()), // P-XFF trusted — empty (fail-safe) in unit tests.
        )
    }

    /// Registry (registered + ready) and queue (one worker client) for `model`.
    async fn ready_service_with_worker(model: &str, endpoint: String) -> GrpcService {
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(model, "1", test_config(1, 0.0, 10), ModelType::LitAPI, std::env::temp_dir())
            .unwrap();
        registry.mark_ready(model, "1").unwrap();
        let queue = Arc::new(InferenceQueue::new());
        let client = Arc::new(WorkerZmqClient::new(endpoint));
        let (reload_tx, _rx) = mpsc::channel(8);
        queue.register_model(
            model, "1", &test_config(1, 0.0, 10), vec![],
            vec![client.clone()],
            reload_tx, Arc::new(OutlierState::new(1)), None,
        );
        let service = build_service(registry, queue);
        // stream/bidi 走 worker_manager.get_zmq_clients（不经 queue）——
        // 测试不经 spawn_workers，用 test hook 直接填充。
        service
            .worker_manager
            .insert_zmq_clients_for_test(model, "1", vec![client])
            .await;
        service
    }

    fn infer_request(model: &str, version: &str) -> Request<pb::InferRequest> {
        Request::new(pb::InferRequest {
            model_name: model.to_string(),
            version: version.to_string(),
            data: Bytes::from_static(b"{}"),
            headers: HashMap::new(),
            sequence_id: None,
        })
    }

    #[tokio::test]
    async fn should_record_2xx_after_successful_infer() {
        let model = "met_ok";
        let endpoint = metric_test_endpoint(model);
        let _worker = spawn_ok_worker(endpoint.clone());
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let counter = REQUESTS_TOTAL.with_label_values(&[model, "1", "2xx"]);
        let before = counter.get();

        let resp = service.infer(infer_request(model, "1")).await;
        assert!(resp.is_ok(), "infer must succeed: {:?}", resp.err());
        assert_eq!(counter.get(), before + 1.0, "successful infer must record one 2xx request");
    }

    #[tokio::test]
    async fn should_record_4xx_when_model_not_found() {
        let service = build_service(Arc::new(ModelRegistry::new()), Arc::new(InferenceQueue::new()));

        let counter = REQUESTS_TOTAL.with_label_values(&["met_404", "", "4xx"]);
        let before = counter.get();

        let err = service.infer(infer_request("met_404", "")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(counter.get(), before + 1.0, "model-not-found must record one 4xx request");
    }

    // --- P5-2: handler 级 canary pin（蓝图 §4.4：metadata 优先，fallback proto headers map）---

    /// Two ready versions ("1","2") each backed by an ok-worker; v1 is active
    /// with weights 100/0, so a served "2" can only come from the canary pin.
    async fn two_version_canary_service(model: &str, canary_override: bool) -> GrpcService {
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        for v in ["1", "2"] {
            registry
                .register(model, v, test_config(1, 0.0, 10), ModelType::LitAPI, std::env::temp_dir())
                .unwrap();
            registry.mark_ready(model, v).unwrap();
            let endpoint = metric_test_endpoint(&format!("{}-v{}", model, v));
            spawn_ok_worker(endpoint.clone());
            let client = Arc::new(WorkerZmqClient::new(endpoint));
            let (reload_tx, _rx) = mpsc::channel(8);
            queue.register_model(
                model, v, &test_config(1, 0.0, 10), vec![],
                vec![client], reload_tx, Arc::new(OutlierState::new(1)), None,
            );
        }
        // activate = hard cutover（§4.3）：active=v1 且权重 100/0。
        registry.activate_version(model, "1").unwrap();
        build_service_with_canary(registry, queue, canary_override)
    }

    #[tokio::test]
    async fn should_route_to_pinned_version_when_canary_override_on() {
        let model = "canary_on";
        let service = two_version_canary_service(model, true).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let pinned = REQUESTS_TOTAL.with_label_values(&[model, "2", "2xx"]);
        let before = pinned.get();

        let mut req = infer_request(model, "");
        req.metadata_mut().insert("x-lite-version", "2".parse().unwrap());
        let resp = service.infer(req).await;
        assert!(resp.is_ok(), "pinned infer must succeed: {:?}", resp.err());
        assert_eq!(pinned.get(), before + 1.0, "pin must route to v2 despite weights 100→v1");
    }

    #[tokio::test]
    async fn should_ignore_pin_when_canary_override_off() {
        let model = "canary_off";
        let service = two_version_canary_service(model, false).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let weighted = REQUESTS_TOTAL.with_label_values(&[model, "1", "2xx"]);
        let before = weighted.get();

        let mut req = infer_request(model, "");
        req.metadata_mut().insert("x-lite-version", "2".parse().unwrap());
        let resp = service.infer(req).await;
        assert!(resp.is_ok(), "infer must succeed: {:?}", resp.err());
        assert_eq!(weighted.get(), before + 1.0, "switch off → weights (v1) serve, pin ignored");
    }

    #[tokio::test]
    async fn should_route_to_pinned_version_via_proto_headers() {
        let model = "canary_hdr";
        let service = two_version_canary_service(model, true).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let pinned = REQUESTS_TOTAL.with_label_values(&[model, "2", "2xx"]);
        let before = pinned.get();

        let mut req = infer_request(model, "");
        req.get_mut().headers.insert("x-lite-version".to_string(), "2".to_string());
        let resp = service.infer(req).await;
        assert!(resp.is_ok(), "pinned infer must succeed: {:?}", resp.err());
        assert_eq!(pinned.get(), before + 1.0, "proto headers map pin must route to v2");
    }

    #[tokio::test]
    async fn should_reject_unknown_pin_with_not_found() {
        let model = "canary_nf";
        let service = two_version_canary_service(model, true).await;

        let mut req = infer_request(model, "");
        req.metadata_mut().insert("x-lite-version", "9".parse().unwrap());
        let err = service.infer(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound, "pin 版本不存在 → NotFound（蓝图 §4.4）");
    }

    // --- B1: readiness gate must check the RESOLVED version, not the raw
    //     request version (HTTP inference.rs:76 checks Some(&resolved)).
    //     On the broken code the gate inspects the active Ready v1 and the
    //     request falls through to a dispatch error ("queue not available" /
    //     "no workers available") — Unavailable, but never the gate's
    //     "not ready" message, which is what these tests assert. ---

    /// v1 active+Ready (weight 0); v2 was Ready, now Degraded, holding all
    /// weight — an empty-version request deterministically resolves to the
    /// not-Ready v2. No queue/worker wiring: the gate fires before dispatch.
    fn degraded_weighted_service(model: &str) -> GrpcService {
        use crate::registry::types::VersionStatus;
        let registry = Arc::new(ModelRegistry::new());
        for v in ["1", "2"] {
            registry
                .register(model, v, test_config(1, 0.0, 10), ModelType::LitAPI, std::env::temp_dir())
                .unwrap();
            registry.mark_ready(model, v).unwrap();
        }
        registry.activate_version(model, "1").unwrap();
        registry.set_status(model, "2", VersionStatus::Degraded).unwrap();
        registry
            .set_weights(model, &HashMap::from([("1".into(), 0u32), ("2".into(), 100)]))
            .unwrap();
        build_service_with_canary(registry, Arc::new(InferenceQueue::new()), false)
    }

    /// canary_override=true + pin "2" resolves an empty-version request to the
    /// registered-but-not-Ready v2; the gate must reject it (HTTP parity).
    #[tokio::test]
    async fn grpc_readiness_gate_must_check_resolved_version_not_raw() {
        let model = "gate_pin";
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(model, "1", test_config(1, 0.0, 10), ModelType::LitAPI, std::env::temp_dir())
            .unwrap();
        registry.mark_ready(model, "1").unwrap();
        registry.activate_version(model, "1").unwrap();
        // v2 registered (exists for canary_pin) but deliberately NOT marked ready.
        registry
            .register(model, "2", test_config(1, 0.0, 10), ModelType::LitAPI, std::env::temp_dir())
            .unwrap();
        let service = build_service_with_canary(registry, Arc::new(InferenceQueue::new()), true);

        let mut req = infer_request(model, "");
        req.metadata_mut().insert("x-lite-version", "2".parse().unwrap());
        let err = service.infer(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not ready"),
            "resolved v2 is not Ready → gate must 503; got: {}", err.message());
    }

    /// Same root cause without any opt-in flag: a weighted rollout whose
    /// non-active version degraded — routing_pick (Degraded is a candidate)
    /// resolves empty-version requests to it; the gate must reject.
    #[tokio::test]
    async fn grpc_readiness_gate_bypasses_degraded_weighted_version() {
        let service = degraded_weighted_service("gate_degraded");

        let err = service.infer(infer_request("gate_degraded", "")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not ready"),
            "resolved Degraded v2 → gate must 503; got: {}", err.message());
    }

    #[tokio::test]
    async fn batch_infer_readiness_gate_checks_resolved_version() {
        let service = degraded_weighted_service("gate_batch");

        let err = service
            .batch_infer(Request::new(pb::BatchInferRequest {
                model_name: "gate_batch".to_string(),
                version: String::new(),
                items: vec![Bytes::from_static(b"{}")],
                headers: HashMap::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not ready"),
            "resolved Degraded v2 → gate must 503; got: {}", err.message());
    }

    #[tokio::test]
    async fn stream_infer_readiness_gate_checks_resolved_version() {
        let service = degraded_weighted_service("gate_stream");

        let err = service
            .stream_infer(Request::new(pb::StreamInferRequest {
                model_name: "gate_stream".to_string(),
                version: String::new(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not ready"),
            "resolved Degraded v2 → gate must 503; got: {}", err.message());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_return_unavailable_and_record_5xx_when_queue_full() {
        let model = "met_full";
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(model, "1", test_config(2, 3600.0, 1), ModelType::LitAPI, std::env::temp_dir())
            .unwrap();
        registry.mark_ready(model, "1").unwrap();
        let queue = Arc::new(InferenceQueue::new());
        let (reload_tx, _rx) = mpsc::channel(8);
        queue.register_model(
            model, "1", &test_config(2, 3600.0, 1), vec![], vec![],
            reload_tx, Arc::new(OutlierState::new(0)), None,
        );
        let service = build_service(registry, queue.clone());

        // Pre-fill the queue (capacity 1). current_thread runtime with no
        // intervening yield: the collector task never runs, so the channel
        // stays full and the next submit deterministically gets Full.
        let (filler_tx, _filler_rx) = oneshot::channel();
        queue
            .try_submit(model, "1", crate::inference_queue::QueueItem {
                uid: "filler".to_string(),
                data: Bytes::new(),
                meta: None,
                response_tx: filler_tx,
                inflight_guard: None,
                enqueued_at: Instant::now(),
            })
            .unwrap();

        let counter = REQUESTS_TOTAL.with_label_values(&[model, "1", "5xx"]);
        let before = counter.get();

        let err = service.infer(infer_request(model, "1")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable,
            "§4.0.9: queue-full must be Unavailable (ResourceExhausted 专给限流)");
        assert_eq!(counter.get(), before + 1.0, "queue-full must record one 5xx request");
        assert_eq!(
            err.metadata().get("retry-after").and_then(|v| v.to_str().ok()),
            Some("1"),
            "§4.0.9: queue-full/load-shedding must carry retry-after metadata"
        );
    }

    #[tokio::test]
    async fn p_flow_admission_rejects_over_cap_with_retry_after() {
        // max_inflight=1: saturate the single slot, then the next inference RPC
        // is rejected at the handler top (before any model lookup) with
        // Unavailable + retry-after. Health/admin are separate services and
        // never reach acquire_admission.
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue.clone(),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let mut cfg = crate::config::Config::default();
        cfg.server.max_inflight = 1;
        let app_state = Arc::new(AppState::new(
            registry.clone(),
            wm.clone(),
            queue.clone(),
            cfg,
            std::env::temp_dir(),
            Arc::new(CallbackRunner::new()),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ));
        let service = GrpcService::new(
            registry,
            wm,
            false,
            false,
            Arc::new(CallbackRunner::new()),
            Arc::new(crate::server::ShutdownState::new()),
            Duration::from_secs(5),
            Arc::new(crate::rate_limit::RateLimiter::default()),
            None,
            app_state,
            Arc::new(Vec::new()), // P-XFF trusted — empty (fail-safe) in this test.
        );

        // Saturate the single admission slot.
        let _fill = service
            .app_state
            .admission
            .try_acquire()
            .expect("cap=1 admits one");

        let err = service.infer(infer_request("any", "1")).await.unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::Unavailable,
            "admission over cap must be Unavailable"
        );
        assert_eq!(
            err.metadata().get("retry-after").and_then(|v| v.to_str().ok()),
            Some("1"),
            "admission rejection must carry retry-after metadata"
        );
        assert_eq!(
            service.app_state.admission.current(),
            1,
            "rejected request does not consume a slot"
        );

        // Releasing the slot re-admits.
        drop(_fill);
        assert!(service.app_state.admission.try_acquire().is_some());
    }

    #[tokio::test]
    async fn p_flow_admission_unlimited_when_cap_zero() {
        // Default max_inflight=0 → unlimited: build_service uses Config::default().
        let model = "met_admit0";
        let endpoint = metric_test_endpoint(model);
        let _worker = spawn_ok_worker(endpoint.clone());
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Many acquires never saturate (cap 0).
        for _ in 0..10 {
            assert!(service.app_state.admission.try_acquire().is_some());
        }
        // And a real infer still succeeds (admission is a no-op pass-through).
        let resp = service.infer(infer_request(model, "1")).await;
        assert!(resp.is_ok(), "cap 0 must not reject: {:?}", resp.err());
    }

    #[tokio::test]
    async fn should_record_2xx_once_when_stream_closes() {
        let model = "met_stream";
        let endpoint = metric_test_endpoint(model);
        let _worker = spawn_stream_worker(endpoint.clone());
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let counter = REQUESTS_TOTAL.with_label_values(&[model, "1", "2xx"]);
        let before = counter.get();

        let resp = service
            .stream_infer(Request::new(pb::StreamInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("stream must open");

        // Drain the stream: the worker sends one chunk + Done, so the stream
        // closes and the forwarder records the overall duration exactly once.
        use tokio_stream::StreamExt;
        let mut stream = resp.into_inner();
        while let Some(chunk) = stream.next().await {
            chunk.expect("chunk must be Ok");
        }
        assert_eq!(counter.get(), before + 1.0,
            "stream close must record exactly one 2xx request (overall duration)");
    }

    // ===== P2-2: x-request-id / x-processing-time-ms 回显 =====

    /// 带 `x-client-request-id` metadata 的请求 → 响应 metadata 回显
    /// `x-request-id`（同值）+ `x-processing-time-ms`（蓝图 §4.1 P2-2）。
    #[tokio::test]
    async fn should_echo_request_id_and_processing_time_on_success() {
        let model = "echo_ok";
        let endpoint = metric_test_endpoint(model);
        let _worker = spawn_ok_worker(endpoint.clone());
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut req = infer_request(model, "1");
        req.metadata_mut()
            .insert("x-client-request-id", "echo-foo".parse().unwrap());

        let resp = service.infer(req).await.expect("infer must succeed");
        let md = resp.metadata();
        assert_eq!(
            md.get("x-request-id").and_then(|v| v.to_str().ok()),
            Some("echo-foo"),
            "x-request-id must echo the client-supplied id"
        );
        assert!(
            md.get("x-processing-time-ms").is_some(),
            "x-processing-time-ms must be present on success"
        );
    }

    /// 错误路径同样回显（对齐 HTTP observability 错误路径）。
    #[tokio::test]
    async fn should_echo_request_id_on_error_path() {
        let service = build_service(Arc::new(ModelRegistry::new()), Arc::new(InferenceQueue::new()));

        let mut req = infer_request("echo_404", "");
        req.metadata_mut()
            .insert("x-client-request-id", "echo-bar".parse().unwrap());

        let err = service.infer(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        let md = err.metadata();
        assert_eq!(
            md.get("x-request-id").and_then(|v| v.to_str().ok()),
            Some("echo-bar"),
            "x-request-id must echo on the error path too"
        );
        assert!(
            md.get("x-processing-time-ms").is_some(),
            "x-processing-time-ms must be present on the error path"
        );
    }

    // ===== P2-3: handler tracing span =====

    /// handler 创建 `inference` span，字段与 HTTP info_span! 一致（model/version/
    /// request_id）。在专用线程上 set_default 一个 span-recording Layer + 自有
    /// current_thread runtime——block_on 在持有 guard 的同一线程轮询，span 创建
    /// 必然走该线程的 scoped subscriber。蓝图建议 tracing-test，但其属性宏在
    /// 本工具链未注入 `logs`，故用等价方案。
    ///
    /// 并行套件稳定性：tracing 的 callsite interest 是进程级全局缓存。全进程仅
    /// 注册过 ≤1 个 dispatch 时走 `has_just_one` 快路径——首个执行本 callsite 的
    /// 【无 subscriber】线程（如其他并行测试的 tokio worker）会用 NoSubscriber 把
    /// interest 缓存成 NEVER，之后所有线程的 `info_span!` 宏直接短路返回
    /// `Span::none()`，scoped subscriber 根本不会被询问（曾致本测试 ~50% 假阴性）。
    /// 修复：常驻两个存活 dispatch（锚点 + 录制）使快路径永久失效——此后任何
    /// interest 重建都带上本测试的 dispatch（默认 Layer 投 always，合并不同意见
    /// 至少得 SOMETIMES），且 `Dispatch::new` 触发的全量重建会修复测试开始前已
    /// 被毒化的缓存。锚点须存活到 join 之后（interest 只认存活的 dispatch）。
    #[test]
    fn should_create_inference_span_with_fields() {
        use std::sync::{Arc, Mutex};
        use tracing::field::Visit;
        use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

        #[derive(Default)]
        struct FieldCollector(Vec<(String, String)>);
        impl Visit for FieldCollector {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push((field.name().to_string(), format!("{:?}", value)));
            }
        }

        type Recorded = Arc<Mutex<Vec<(String, Vec<(String, String)>)>>>;
        struct SpanLayer(Recorded);
        impl<S: tracing::Subscriber> Layer<S> for SpanLayer {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _id: &tracing::span::Id,
                _ctx: Context<'_, S>,
            ) {
                let mut collector = FieldCollector::default();
                attrs.record(&mut collector);
                self.0
                    .lock()
                    .unwrap()
                    .push((attrs.metadata().name().to_string(), collector.0));
            }
        }

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let recorded_thread = recorded.clone();
        // 见上方 doc comment：两个 dispatch 都先注册好再执行 handler，保证
        // `has_just_one` 为 false 且缓存已被本测试的 always 票重建过。
        let _anchor = tracing::Dispatch::new(
            tracing_subscriber::registry().with(SpanLayer(Arc::new(Mutex::new(Vec::new())))),
        );
        let recording = tracing::Dispatch::new(
            tracing_subscriber::registry().with(SpanLayer(recorded_thread)),
        );
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&recording);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let service = build_service(
                    Arc::new(ModelRegistry::new()),
                    Arc::new(InferenceQueue::new()),
                );
                let mut req = infer_request("span_404", "");
                req.metadata_mut()
                    .insert("x-client-request-id", "span-rid".parse().unwrap());
                let _ = service.infer(req).await;
            });
        });
        handle.join().expect("span test thread must not panic");

        let spans = recorded.lock().unwrap();
        let inference = spans
            .iter()
            .find(|(name, _)| name == "inference")
            .expect("inference span must be created");
        let field = |key: &str| -> String {
            inference
                .1
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        assert!(field("model").contains("span_404"), "span model field: {:?}", inference.1);
        assert!(field("request_id").contains("span-rid"), "span request_id field: {:?}", inference.1);
    }

    // ===== Task A: gRPC worker metrics (infer / stream / decoupled / batch) =====
    //
    // HTTP already records worker metrics on the SSE/WS Done frames; the gRPC
    // paths carried the field but never recorded. These guard the four new
    // record points (bidi shares the identical Done-arm record — covered by the
    // stream test + structural parity).

    /// PAIR worker answering every unary request with Ok Single + top-level
    /// Response.metrics (proto field 40) — drives the unary `infer` record.
    fn spawn_ok_worker_with_metrics(endpoint: String, m: pb::Metrics) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resp = pb::Response {
                    uid: req.uid,
                    metrics: Some(m.clone()),
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: Bytes::from_static(b"{\"ok\":true}"),
                        status: Some(pb::Status { code: "Ok".to_string(), message: String::new() }),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        })
    }

    /// PAIR worker answering a stream Open with one Chunk + a Done carrying
    /// Metrics (proto StreamDone.metrics = 1) — drives stream/decoupled record.
    fn spawn_stream_worker_with_metrics(endpoint: String, m: pb::Metrics) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let is_open = matches!(
                    req.payload,
                    Some(pb::request::Payload::Stream(pb::StreamRequest {
                        action: Some(pb::stream_request::Action::Open(_)),
                        ..
                    }))
                );
                if !is_open {
                    let ack = pb::Response { uid: req.uid, ..Default::default() };
                    let _ = s.send(ack.encode_to_vec(), 0);
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let mk = |payload| pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(payload),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let _ = s.send(mk(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                    data: Bytes::from_static(b"{}"),
                    is_final: false,
                })).encode_to_vec(), 0);
                let _ = s.send(
                    mk(pb::stream_response::Payload::Done(pb::StreamDone { metrics: Some(m.clone()) }))
                        .encode_to_vec(),
                    0,
                );
            }
        })
    }

    /// PAIR worker answering a Batch request by echoing each item back as a
    /// BatchItemResponse, with top-level Response.metrics — drives batch record.
    fn spawn_batch_worker_with_metrics(endpoint: String, m: pb::Metrics) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let items: Vec<pb::BatchItemResponse> = match req.payload {
                    Some(pb::request::Payload::Batch(b)) => b
                        .items
                        .into_iter()
                        .map(|it| pb::BatchItemResponse {
                            uid: it.uid,
                            data: it.data,
                            status: Some(pb::Status { code: "Ok".to_string(), message: String::new() }),
                            ..Default::default()
                        })
                        .collect(),
                    _ => vec![],
                };
                let resp = pb::Response {
                    uid: req.uid,
                    metrics: Some(m.clone()),
                    payload: Some(pb::response::Payload::Batch(pb::BatchResponse {
                        items,
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        })
    }

    #[tokio::test]
    async fn grpc_infer_records_worker_metrics() {
        let model = "wm_infer";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_ok_worker_with_metrics(
            endpoint.clone(),
            pb::Metrics { prefill_ms: 12.5, tokens_generated: 3, ..Default::default() },
        );
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service.infer(infer_request(model, "1")).await;
        assert!(resp.is_ok(), "infer must succeed: {:?}", resp.err());

        let out = crate::metrics::prometheus::gather_metrics();
        assert!(
            out.contains(r#"lite_server_prefill_ms{model="wm_infer",version="1"} 12.5"#),
            "prefill missing: {out}"
        );
        assert!(
            out.contains(r#"lite_server_tokens_generated_total{model="wm_infer",version="1"} 3"#),
            "tokens missing: {out}"
        );
    }

    #[tokio::test]
    async fn grpc_stream_records_worker_metrics() {
        let model = "wm_stream";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_stream_worker_with_metrics(
            endpoint.clone(),
            pb::Metrics { prefill_ms: 9.5, tokens_generated: 2, ..Default::default() },
        );
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .stream_infer(Request::new(pb::StreamInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("stream must open");
        use tokio_stream::StreamExt;
        let mut s = resp.into_inner();
        while let Some(c) = s.next().await {
            c.expect("chunk must be Ok");
        }

        let out = crate::metrics::prometheus::gather_metrics();
        assert!(
            out.contains(r#"lite_server_prefill_ms{model="wm_stream",version="1"} 9.5"#),
            "prefill missing: {out}"
        );
        assert!(
            out.contains(r#"lite_server_tokens_generated_total{model="wm_stream",version="1"} 2"#),
            "tokens missing: {out}"
        );
    }

    #[tokio::test]
    async fn grpc_decoupled_records_worker_metrics() {
        let model = "wm_dec";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_stream_worker_with_metrics(
            endpoint.clone(),
            pb::Metrics { prefill_ms: 5.5, tokens_generated: 4, ..Default::default() },
        );
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .decoupled_infer(Request::new(pb::DecoupledInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("decoupled must open");
        use tokio_stream::StreamExt;
        let mut s = resp.into_inner();
        while let Some(c) = s.next().await {
            c.expect("decoupled chunk must be Ok");
        }

        let out = crate::metrics::prometheus::gather_metrics();
        assert!(
            out.contains(r#"lite_server_prefill_ms{model="wm_dec",version="1"} 5.5"#),
            "prefill missing: {out}"
        );
        assert!(
            out.contains(r#"lite_server_tokens_generated_total{model="wm_dec",version="1"} 4"#),
            "tokens missing: {out}"
        );
    }

    #[tokio::test]
    async fn grpc_batch_records_worker_metrics() {
        let model = "wm_batch";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_batch_worker_with_metrics(
            endpoint.clone(),
            pb::Metrics { prefill_ms: 7.5, tokens_generated: 1, ..Default::default() },
        );
        let service = ready_service_with_worker(model, endpoint).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .batch_infer(Request::new(pb::BatchInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                items: vec![Bytes::from_static(b"{}")],
                headers: HashMap::new(),
            }))
            .await;
        assert!(resp.is_ok(), "batch must succeed: {:?}", resp.err());

        let out = crate::metrics::prometheus::gather_metrics();
        assert!(
            out.contains(r#"lite_server_prefill_ms{model="wm_batch",version="1"} 7.5"#),
            "prefill missing: {out}"
        );
        assert!(
            out.contains(r#"lite_server_tokens_generated_total{model="wm_batch",version="1"} 1"#),
            "tokens missing: {out}"
        );
    }

    // ===== Task D: streaming callbacks (request on open; response on Done/Error;
    //       cancel/disconnect does not fire) =====
    //
    // bidi shares the identical fire_inference_request/response calls and
    // Done/Error arms — covered by stream/decoupled here + structural parity
    // (constructing a tonic `Streaming<BidiChunk>` in a unit test is impractical).

    /// PAIR worker answering a stream Open with one Error frame — drives the
    /// response-on-error path.
    fn spawn_stream_error_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let is_open = matches!(
                    req.payload,
                    Some(pb::request::Payload::Stream(pb::StreamRequest {
                        action: Some(pb::stream_request::Action::Open(_)),
                        ..
                    }))
                );
                if !is_open {
                    let ack = pb::Response { uid: req.uid, ..Default::default() };
                    let _ = s.send(ack.encode_to_vec(), 0);
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let resp = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                            message: "boom".to_string(),
                        })),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let _ = s.send(resp.encode_to_vec(), 0);
            }
        })
    }

    /// Callback counting request/response fires; stashes the last response ctx
    /// so tests can assert elapsed_us is set.
    struct CountingCallback {
        req: std::sync::atomic::AtomicUsize,
        resp: std::sync::atomic::AtomicUsize,
        last_resp: std::sync::Mutex<Option<crate::callback::InferenceContext>>,
    }

    impl CountingCallback {
        fn new() -> Self {
            Self {
                req: std::sync::atomic::AtomicUsize::new(0),
                resp: std::sync::atomic::AtomicUsize::new(0),
                last_resp: std::sync::Mutex::new(None),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::callback::Callback for CountingCallback {
        async fn on_inference_request(&self, _ctx: &crate::callback::InferenceContext) {
            self.req.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        async fn on_inference_response(&self, ctx: &crate::callback::InferenceContext) {
            self.resp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            *self.last_resp.lock().unwrap() = Some(ctx.clone());
        }
    }

    /// Build a service whose GrpcService.callback_runner is the SHARED runner
    /// (build_service_with_canary uses a fresh unobservable one). The WM/AppState
    /// runners are separate and unused for inference callbacks.
    fn build_service_with_callback(
        registry: Arc<ModelRegistry>,
        queue: Arc<InferenceQueue>,
        cb: Arc<CallbackRunner>,
    ) -> GrpcService {
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue,
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let app_state = Arc::new(AppState::new(
            registry.clone(),
            wm.clone(),
            wm.inference_queue().clone(),
            crate::config::Config::default(),
            std::env::temp_dir(),
            Arc::new(CallbackRunner::new()),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ));
        GrpcService::new(
            registry,
            wm,
            false,
            false,
            cb,
            Arc::new(crate::server::ShutdownState::new()),
            Duration::from_secs(5),
            Arc::new(crate::rate_limit::RateLimiter::default()),
            None,
            app_state,
            Arc::new(Vec::new()),
        )
    }

    async fn ready_service_with_worker_cb(
        model: &str,
        endpoint: String,
        cb: Arc<CallbackRunner>,
    ) -> GrpcService {
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(model, "1", test_config(1, 0.0, 10), ModelType::LitAPI, std::env::temp_dir())
            .unwrap();
        registry.mark_ready(model, "1").unwrap();
        let queue = Arc::new(InferenceQueue::new());
        let client = Arc::new(WorkerZmqClient::new(endpoint));
        let (reload_tx, _rx) = mpsc::channel(8);
        queue.register_model(
            model, "1", &test_config(1, 0.0, 10), vec![],
            vec![client.clone()],
            reload_tx, Arc::new(OutlierState::new(1)), None,
        );
        let service = build_service_with_callback(registry, queue, cb);
        service
            .worker_manager
            .insert_zmq_clients_for_test(model, "1", vec![client])
            .await;
        service
    }

    async fn wait_for<F: Fn() -> bool>(cond: F, label: &str) {
        for _ in 0..60 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("condition never met within ~1.5s: {}", label);
    }

    #[tokio::test]
    async fn stream_callback_fires_on_done() {
        let model = "cb_stream_done";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_stream_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback::new());
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let service = ready_service_with_worker_cb(model, endpoint, runner).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .stream_infer(Request::new(pb::StreamInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("stream must open");
        use tokio_stream::StreamExt;
        let mut s = resp.into_inner();
        while let Some(c) = s.next().await {
            c.expect("chunk must be Ok");
        }

        wait_for(|| cb.req.load(std::sync::atomic::Ordering::Relaxed) >= 1, "req>=1").await;
        wait_for(|| cb.resp.load(std::sync::atomic::Ordering::Relaxed) >= 1, "resp>=1").await;
        assert_eq!(cb.req.load(std::sync::atomic::Ordering::Relaxed), 1, "request fires once");
        assert_eq!(cb.resp.load(std::sync::atomic::Ordering::Relaxed), 1, "response fires once");
        let elapsed_set = cb
            .last_resp
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.elapsed_us.is_some())
            .unwrap_or(false);
        assert!(elapsed_set, "response elapsed_us must be set");
    }

    #[tokio::test]
    async fn stream_callback_fires_on_error() {
        let model = "cb_stream_err";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_stream_error_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback::new());
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let service = ready_service_with_worker_cb(model, endpoint, runner).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .stream_infer(Request::new(pb::StreamInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("stream must open");
        use tokio_stream::StreamExt;
        let mut s = resp.into_inner();
        // Drain ignoring the terminal Err chunk.
        while s.next().await.is_some() {}

        wait_for(|| cb.resp.load(std::sync::atomic::Ordering::Relaxed) >= 1, "resp>=1").await;
        assert_eq!(cb.req.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(cb.resp.load(std::sync::atomic::Ordering::Relaxed), 1, "Error frame fires response");
    }

    #[tokio::test]
    async fn stream_callback_not_fired_on_cancel() {
        let model = "cb_stream_cancel";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_stream_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback::new());
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let service = ready_service_with_worker_cb(model, endpoint, runner).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .stream_infer(Request::new(pb::StreamInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("stream must open");
        // Drop the client rx immediately → the forwarder's tx.send errors on the
        // first chunk, so it breaks before the Done frame: no response callback.
        drop(resp.into_inner());

        wait_for(|| cb.req.load(std::sync::atomic::Ordering::Relaxed) >= 1, "req>=1").await;
        // Give the forwarder a moment to observe the dropped rx and break.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(cb.req.load(std::sync::atomic::Ordering::Relaxed), 1, "request still fires on cancel");
        assert_eq!(cb.resp.load(std::sync::atomic::Ordering::Relaxed), 0, "cancel must NOT fire response");
    }

    #[tokio::test]
    async fn decoupled_callback_fires_on_done() {
        let model = "cb_dec_done";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_stream_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback::new());
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let service = ready_service_with_worker_cb(model, endpoint, runner).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .decoupled_infer(Request::new(pb::DecoupledInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("decoupled must open");
        use tokio_stream::StreamExt;
        let mut s = resp.into_inner();
        while let Some(c) = s.next().await {
            c.expect("decoupled chunk must be Ok");
        }

        wait_for(|| cb.resp.load(std::sync::atomic::Ordering::Relaxed) >= 1, "resp>=1").await;
        assert_eq!(cb.req.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(cb.resp.load(std::sync::atomic::Ordering::Relaxed), 1, "decoupled Done fires response");
    }

    #[tokio::test]
    async fn decoupled_callback_not_fired_on_cancel() {
        let model = "cb_dec_cancel";
        let endpoint = metric_test_endpoint(model);
        let _w = spawn_stream_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback::new());
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let service = ready_service_with_worker_cb(model, endpoint, runner).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let resp = service
            .decoupled_infer(Request::new(pb::DecoupledInferRequest {
                model_name: model.to_string(),
                version: "1".to_string(),
                data: Bytes::from_static(b"{}"),
                headers: HashMap::new(),
                sequence_id: None,
            }))
            .await
            .expect("decoupled must open");
        drop(resp.into_inner());

        wait_for(|| cb.req.load(std::sync::atomic::Ordering::Relaxed) >= 1, "req>=1").await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(cb.req.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(cb.resp.load(std::sync::atomic::Ordering::Relaxed), 0, "cancel must NOT fire response");
    }
}
