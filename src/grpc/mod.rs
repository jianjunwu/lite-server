use crate::access_control::EndpointClass;
use crate::callback::CallbackRunner;
use crate::error::AppError;
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use crate::registry::ModelRegistry;
use crate::request_context::RequestContext;
use crate::streaming;
use crate::worker::WorkerManager;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::warn;
use tracing::Instrument;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use uuid::Uuid;

pub mod admin;
pub mod interceptor;

pub use pb::lite_server_server::{LiteServer, LiteServerServer};

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

    async fn infer_impl(
        &self,
        request: Request<pb::InferRequest>,
        version_label: &mut String,
        request_id_out: &mut String,
    ) -> Result<Response<pb::InferResponse>, Status> {
        let remote_addr = request.remote_addr();
        let (grpc_metadata, extensions, req) = request.into_parts();
        let cx = interceptor::finalize_context(
            extensions.get::<RequestContext>().cloned(),
            &grpc_metadata,
            &req.headers,
            remote_addr,
            &self.trusted,
        );
        let request_id = cx.request_id.clone();
        *request_id_out = request_id.clone();
        let client_ip = cx.client_ip.clone();
        let model_name = &req.model_name;
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        if let Err(e) = crate::validation::validate_identifier(model_name) {
            return Err(err(Status::invalid_argument(e.to_string())));
        }

        // version="" → canary pin (P5-2, 开关开) → weighted routing pick (§4.3),
        // falling back to active. 优先级与 HTTP resolve_version 一致（蓝图 §4.4）。
        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => match canary_pin(
                &self.registry,
                self.canary_override,
                model_name,
                &grpc_metadata,
                &req.headers,
            )? {
                Some(pin) => pin,
                None => self
                    .registry
                    .routing_pick(model_name)
                    .or_else(|| self.registry.get_active_version(model_name))
                    .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
            },
        };
        self.registry.touch_last_used(model_name, &resolved_version);
        *version_label = resolved_version.clone();

        if !self.registry.is_ready(model_name, Some(&resolved_version)) {
            return Err(err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            ))));
        }

        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
            enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), model_name, &client_ip)?;
        }

        // P-ENSEMBLE-GRPC (蓝图 §4.1, D23): ensemble models have no workers —
        // dispatch through the DAG executor instead of the worker queue,
        // mirroring HTTP `do_infer` (inference.rs). Thin translation layer:
        // proto request → execute_ensemble args; orchestration is not duplicated.
        // Only unary `infer` (batch/stream/bidi intentionally excluded, matching
        // HTTP which also only ensembles in do_infer).
        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            if mv.model_type == ModelType::Ensemble {
                let payload = serde_json::from_slice(&req.data).unwrap_or(serde_json::Value::Null);
                // P-DEADLINE: resolve here (grpc_metadata still in scope) and
                // pass to the ensemble cascade.
                let ensemble_deadline = crate::deadline::resolve_from_grpc(
                    &grpc_metadata,
                    self.server_timeout.as_secs_f32(),
                );
                let result = crate::ensemble::execute_ensemble(
                    self.app_state.clone(),
                    model_name,
                    &resolved_version,
                    payload,
                    &request_id,
                    &client_ip,
                    ensemble_deadline.unix_ns,
                )
                .await
                .map_err(|e| err(app_error_to_grpc_status(&e)))?;
                let data = bytes::Bytes::from(serde_json::to_vec(&result).unwrap_or_default());
                return Ok(Response::new(pb::InferResponse {
                    data,
                    status: None,
                    metrics: None,
                }));
            }
        }

        let mut header_map: HashMap<String, String> = req.headers.clone();
        // P-TRACE: inject the active inference span's trace context into the
        // worker RequestMeta.headers (overwrites any client-supplied traceparent
        // so the worker is a child of THIS span; D8 Rust-only).
        crate::telemetry::inject(&mut header_map);
        // P8-1 (B3): envelope hints — parsed and surfaced, NOT yet consumed (define-only).
        let hints = crate::request_context::RequestHints::from_grpc(&req.headers);
        if !hints.is_empty() {
            tracing::debug!(?hints, "envelope hints received (define-only, not consumed)");
        }
        // P-DEADLINE (§4.0.10): resolve the per-request deadline from the
        // client's `grpc-timeout` metadata, falling back to `server.timeout`.
        let deadline =
            crate::deadline::resolve_from_grpc(&grpc_metadata, self.server_timeout.as_secs_f32());
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: header_map,
            client_ip: client_ip.clone(),
            request_id: request_id.clone(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: req.data.clone(),
            sequence_id: req.sequence_id.clone(),
            deadline_unix_ns: deadline.unix_ns,
            ..Default::default()
        };

        let uid = format!("grpc-{}-{}", model_name, Uuid::new_v4());

        // Fire InferenceRequest callback
        let req_ctx = crate::callback::InferenceContext {
            model_name: model_name.to_string(),
            version: resolved_version.clone(),
            route: "/predict".to_string(),
            protocol: crate::callback::Protocol::Grpc,
            request_id,
            client_ip,
            elapsed_us: None,
        };
        let cb_runner = self.callback_runner.clone();
        let req_ctx_clone = req_ctx.clone();
        tokio::spawn(async move {
            cb_runner.on_inference_request(&req_ctx_clone).await;
        });

        // #1: route unary infer through the unified InferenceQueue — the same
        // path as REST — so gRPC inherits batch aggregation, least-loaded
        // worker selection, outlier ejection, retry, and max_requests
        // recycling, all of which the previous direct client.send() bypassed.
        // (batch_infer and streaming keep their direct path; REST streaming
        // bypasses the queue too, so behavior stays consistent.)
        let start = Instant::now();
        let (response_tx, response_rx) = oneshot::channel();
        let item = crate::inference_queue::QueueItem {
            uid,
            data: meta.payload.clone(),
            meta: Some(std::sync::Arc::new(meta)),
            response_tx,
            inflight_guard: None,
            enqueued_at: Instant::now(),
        };
        let resp = match self
            .worker_manager
            .inference_queue()
            .try_submit(model_name, &resolved_version, item)
        {
            Ok(()) => match crate::deadline::remaining(deadline.unix_ns) {
                Some(t) => match tokio::time::timeout(t, response_rx).await {
                    Ok(Ok(resp)) => resp,
                    Ok(Err(_)) => return Err(err(Status::internal("response channel closed"))),
                    Err(_) => {
                        return Err(err(Status::deadline_exceeded(format!(
                            "inference timed out after {:.1}s",
                            t.as_secs_f64()
                        ))));
                    }
                },
                // No deadline (no client spec AND server.timeout<=0): unbounded.
                None => match response_rx.await {
                    Ok(resp) => resp,
                    Err(_) => return Err(err(Status::internal("response channel closed"))),
                },
            },
            Err(crate::inference_queue::QueueError::Full) => {
                // §4.0.9: queue-full/过载 → Unavailable（落 5xx）+
                // retry-after（对齐 HTTP Retry-After）；ResourceExhausted 专给限流（P3-1）。
                return Err(err(with_retry_after(
                    Status::unavailable(format!(
                        "queue full for {} {}",
                        model_name, resolved_version
                    )),
                    1,
                )));
            }
            Err(_) => {
                return Err(err(Status::unavailable(format!(
                    "queue not available for {} {}",
                    model_name, resolved_version
                ))));
            }
        };

        match resp.payload {
            Some(pb::response::Payload::Single(single)) => {
                let grpc_status = single.status.as_ref().map(|s| pb::Status {
                    code: s.code.clone(),
                    message: s.message.clone(),
                });
                let code = single.status.as_ref().map(|s| s.code.as_str()).unwrap_or("Ok");
                match code {
                    "Error" => {
                        let msg = grpc_status
                            .as_ref()
                            .map(|s| s.message.clone())
                            .unwrap_or_default();
                        // If Status.message parses as u16, the worker signalled
                        // a model-level HTTPException with structured error in data.
                        if let Ok(http_status) = msg.parse::<u16>() {
                            let data: serde_json::Value =
                                serde_json::from_slice(&single.data).unwrap_or(serde_json::json!({}));
                            let parsed = try_parse_model_error(&data);
                            return Err(match parsed {
                                Some(p) => err(model_error_status(
                                    http_status_to_grpc_code(http_status), &p)),
                                None => err(Status::new(
                                    http_status_to_grpc_code(http_status),
                                    format!("[model_error] {}", msg),
                                )),
                            });
                        }
                        // Not a numeric status code — internal worker error.
                        return Err(err(Status::internal(msg)));
                    }
                    _ => {
                        let headers = single.headers.clone();
                        let mut response = Response::new(pb::InferResponse {
                            data: single.data,
                            status: grpc_status,
                            metrics: resp.metrics,
                        });
                        inject_grpc_metadata(response.metadata_mut(), &headers);

                        // Fire InferenceResponse callback
                        let duration = start.elapsed().as_secs_f64();
                        let resp_ctx = crate::callback::InferenceContext {
                            elapsed_us: Some((duration * 1_000_000.0) as u64),
                            ..req_ctx.clone()
                        };
                        let cb_runner = self.callback_runner.clone();
                        tokio::spawn(async move { cb_runner.on_inference_response(&resp_ctx).await; });

                        Ok(response)
                    }
                }
            }
            _ => Err(err(Status::internal("unexpected response type"))),
        }
    }

    async fn batch_infer_impl(
        &self,
        request: Request<pb::BatchInferRequest>,
        version_label: &mut String,
        request_id_out: &mut String,
    ) -> Result<Response<pb::BatchInferResponse>, Status> {
        let remote_addr = request.remote_addr();
        let (grpc_metadata, extensions, req) = request.into_parts();
        let cx = interceptor::finalize_context(
            extensions.get::<RequestContext>().cloned(),
            &grpc_metadata,
            &req.headers,
            remote_addr,
            &self.trusted,
        );
        let request_id = cx.request_id;
        *request_id_out = request_id.clone();
        let client_ip = cx.client_ip;
        let model_name = &req.model_name;
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        if let Err(e) = crate::validation::validate_identifier(model_name) {
            return Err(err(Status::invalid_argument(e.to_string())));
        }

        // version="" → canary pin (P5-2, 开关开) → weighted routing pick (§4.3),
        // falling back to active. 优先级与 HTTP resolve_version 一致（蓝图 §4.4）。
        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => match canary_pin(
                &self.registry,
                self.canary_override,
                model_name,
                &grpc_metadata,
                &req.headers,
            )? {
                Some(pin) => pin,
                None => self
                    .registry
                    .routing_pick(model_name)
                    .or_else(|| self.registry.get_active_version(model_name))
                    .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
            },
        };
        self.registry.touch_last_used(model_name, &resolved_version);
        *version_label = resolved_version.clone();

        if !self.registry.is_ready(model_name, Some(&resolved_version)) {
            return Err(err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            ))));
        }

        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
            enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), model_name, &client_ip)?;
        }

        let mut header_map: HashMap<String, String> = req.headers.clone();
        // P-TRACE: inject the active inference span's trace context into the
        // worker RequestMeta.headers (overwrites any client-supplied traceparent
        // so the worker is a child of THIS span; D8 Rust-only).
        crate::telemetry::inject(&mut header_map);
        // P-DEADLINE (§4.0.10): carried to the worker so it can stop; the batch
        // response wait below is also bounded server-side by this deadline.
        let deadline =
            crate::deadline::resolve_from_grpc(&grpc_metadata, self.server_timeout.as_secs_f32());
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: header_map,
            client_ip,
            request_id,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: Default::default(),
            deadline_unix_ns: deadline.unix_ns,
            ..Default::default()
        };

        let batch_item_count = req.items.len();
        let items: Vec<pb::BatchItem> = req
            .items
            .into_iter()
            .enumerate()
            .map(|(i, data)| pb::BatchItem {
                uid: format!("grpc-batch-{}-{}", i, Uuid::new_v4()),
                data,
            })
            .collect();

        let internal_req = pb::Request {
            uid: format!("grpc-batch-{}", Uuid::new_v4()),
            meta: Some(meta),
            payload: Some(pb::request::Payload::Batch(pb::BatchRequest { items })),
        };

        let clients = self
            .worker_manager
            .get_zmq_clients(model_name, &resolved_version)
            .await
            .ok_or_else(|| err(Status::unavailable("no workers available")))?;

        if clients.is_empty() {
            return Err(err(Status::unavailable("no workers available")));
        }

        let worker_id = match self
            .worker_manager
            .get_outlier_state(model_name, &resolved_version)
            .await
        {
            Some(outlier) => crate::worker::pick_worker_skip_ejected(clients.len(), &outlier),
            None => crate::worker::pick_worker_random(clients.len()),
        };
        let client = &clients[worker_id];
        // P6 GetModelStats: count one inference per batch item on this worker.
        crate::metrics::prometheus::record_worker_inference(
            model_name,
            &resolved_version,
            worker_id,
            batch_item_count,
        );

        // B2 audit fix: bound the batch response wait by the resolved deadline,
        // mirroring unary infer_impl (grpc/mod.rs:307-323). Previously this was a
        // bare `client.send(...).await`, so a slow/hung worker made the server
        // wait up to ~ZMQ_RESPONSE_TIMEOUT regardless of server.timeout/grpc-timeout.
        let resp = match crate::deadline::remaining(deadline.unix_ns) {
            Some(t) => match tokio::time::timeout(t, client.send(internal_req)).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    return Err(err(Status::internal(format!("worker error: {}", e))))
                }
                Err(_) => {
                    return Err(err(Status::deadline_exceeded(format!(
                        "inference timed out after {:.1}s",
                        t.as_secs_f32()
                    ))))
                }
            },
            // No deadline (no client spec AND server.timeout<=0): unbounded.
            None => client
                .send(internal_req)
                .await
                .map_err(|e| err(Status::internal(format!("worker error: {}", e))))?,
        };

        match resp.payload {
            Some(pb::response::Payload::Batch(batch_resp)) => {
                let items: Vec<pb::InferResponse> = batch_resp
                    .items
                    .into_iter()
                    .map(|item| pb::InferResponse {
                        data: item.data,
                        status: item.status,
                        metrics: None,
                    })
                    .collect();
                let mut response = Response::new(pb::BatchInferResponse { items });
                inject_grpc_metadata(response.metadata_mut(), &batch_resp.headers);
                Ok(response)
            }
            _ => Err(err(Status::internal("unexpected response type"))),
        }
    }

    async fn stream_infer_impl(
        &self,
        request: Request<pb::StreamInferRequest>,
        version_label: &mut String,
        request_id_out: &mut String,
        start: Instant,
    ) -> Result<Response<ReceiverStream<Result<pb::StreamChunk, Status>>>, Status> {
        let remote_addr = request.remote_addr();
        let (grpc_metadata, extensions, req) = request.into_parts();
        let cx = interceptor::finalize_context(
            extensions.get::<RequestContext>().cloned(),
            &grpc_metadata,
            &req.headers,
            remote_addr,
            &self.trusted,
        );
        let request_id = cx.request_id;
        *request_id_out = request_id.clone();
        let client_ip = cx.client_ip;
        let model_name = &req.model_name;
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        if let Err(e) = crate::validation::validate_identifier(model_name) {
            return Err(err(Status::invalid_argument(e.to_string())));
        }

        // version="" → canary pin (P5-2, 开关开) → weighted routing pick (§4.3),
        // falling back to active. 优先级与 HTTP resolve_version 一致（蓝图 §4.4）。
        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => match canary_pin(
                &self.registry,
                self.canary_override,
                model_name,
                &grpc_metadata,
                &req.headers,
            )? {
                Some(pin) => pin,
                None => self
                    .registry
                    .routing_pick(model_name)
                    .or_else(|| self.registry.get_active_version(model_name))
                    .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
            },
        };
        self.registry.touch_last_used(model_name, &resolved_version);
        *version_label = resolved_version.clone();

        if !self.registry.is_ready(model_name, Some(&resolved_version)) {
            return Err(err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            ))));
        }

        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
            enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), model_name, &client_ip)?;
        }

        let mut header_map: HashMap<String, String> = req.headers.clone();
        // P-TRACE: inject the active inference span's trace context into the
        // worker RequestMeta.headers (overwrites any client-supplied traceparent
        // so the worker is a child of THIS span; D8 Rust-only).
        crate::telemetry::inject(&mut header_map);
        // P-DEADLINE (§4.0.10): resolve + carry to worker; the streaming two-
        // stage bound below activates only when the CLIENT specified a deadline
        // (so the default config leaves streaming behavior unchanged).
        let deadline =
            crate::deadline::resolve_from_grpc(&grpc_metadata, self.server_timeout.as_secs_f32());
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: header_map,
            client_ip,
            request_id,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: req.data.clone(),
            sequence_id: req.sequence_id.clone(),
            deadline_unix_ns: deadline.unix_ns,
            ..Default::default()
        };
        // Capture before `meta` moves into `open_req` (used by the affinity pick below).
        let sequence_id = meta.sequence_id.clone();
        // P-DEADLINE streaming bound, captured before spawn.
        let (stream_deadline, stream_idle) = if deadline.client_specified {
            (
                crate::deadline::to_instant(deadline.unix_ns),
                self.decoupled_idle_timeout,
            )
        } else {
            (None, None)
        };

        let stream_id = format!("grpc-stream-{}", Uuid::new_v4());
        let open_req = streaming::build_stream_open(stream_id.clone(), req.data, Some(meta), false);

        let clients = self
            .worker_manager
            .get_zmq_clients(model_name, &resolved_version)
            .await
            .ok_or_else(|| err(Status::unavailable("no workers available")))?;

        if clients.is_empty() {
            return Err(err(Status::unavailable("no workers available")));
        }

        // P8-1: connect directly to the worker that last served this
        // sequence_id when it is still registered and not ejected; else the
        // normal skip-ejected/random pick, then record the chosen worker.
        let outlier = self
            .worker_manager
            .get_outlier_state(model_name.as_str(), &resolved_version)
            .await;
        let seq_registry = self.app_state.inference_queue.sequence_registry();
        let num_workers = clients.len();
        let preferred = sequence_id.as_deref().and_then(|seq| {
            let w = seq_registry.lookup(seq, model_name, &resolved_version)?;
            let ejected = outlier.as_ref().map(|o| o.is_ejected(w)).unwrap_or(false);
            (w < num_workers && !ejected).then_some(w)
        });
        let worker_id = preferred.unwrap_or_else(|| match &outlier {
            Some(o) => crate::worker::pick_worker_skip_ejected(num_workers, o),
            None => crate::worker::pick_worker_random(num_workers),
        });
        if let Some(seq) = sequence_id.as_deref() {
            seq_registry.record(seq, model_name, &resolved_version, worker_id);
        }
        let client = clients[worker_id].clone();
        // P6 GetModelStats: one streaming inference dispatched to this worker.
        crate::metrics::prometheus::record_worker_inference(
            model_name,
            &resolved_version,
            worker_id,
            1,
        );

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;

        let (tx, rx) = mpsc::channel(64);
        let cancel_client = client.clone();

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.to_string();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc");
        }

        tokio::spawn(async move {
            let open_time = std::time::Instant::now();
            let mut first_chunk = true;
            let mut last_chunk_time = open_time;
            // P2-1：流关闭时记一次整体 duration；中途 worker 错误按其状态族记。
            let mut stream_family = "2xx";

            loop {
                let chunk = match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                    .await
                {
                    Ok(Some(c)) => c,
                    Ok(None) => break, // worker closed the stream
                    Err(elapsed) => {
                        // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                        tracing::warn!(
                            ?elapsed, stream_id = %stream_id,
                            "stream closed: deadline/idle elapsed"
                        );
                        stream_family = "5xx";
                        break;
                    }
                };
                match chunk.payload {
                    Some(pb::stream_response::Payload::Chunk(ref c)) => {
                        if stream_metrics {
                            if first_chunk {
                                crate::metrics::prometheus::record_stream_ttft(&metrics_model, &metrics_version, "grpc", open_time.elapsed().as_secs_f64());
                                first_chunk = false;
                            } else {
                                crate::metrics::prometheus::record_stream_tbt(&metrics_model, &metrics_version, "grpc", last_chunk_time.elapsed().as_secs_f64());
                            }
                            last_chunk_time = std::time::Instant::now();
                            crate::metrics::prometheus::record_stream_chunk(&metrics_model, &metrics_version, "grpc");
                        }
                        let grpc_chunk = pb::StreamChunk {
                            data: c.data.clone(),
                        };
                        if tx.send(Ok(grpc_chunk)).await.is_err() {
                            break;
                        }
                    }
                    Some(pb::stream_response::Payload::Error(ref e)) => {
                        let grpc_err = match serde_json::from_str::<serde_json::Value>(&e.message) {
                            Ok(val) => {
                                if let Some(parsed) = try_parse_model_error(&val) {
                                    err(model_error_status(
                                        error_type_to_grpc_code(&parsed.error_type),
                                        &parsed,
                                    ))
                                } else {
                                    err(Status::internal(e.message.clone()))
                                }
                            }
                            Err(_) => err(Status::internal(e.message.clone())),
                        };
                        stream_family = grpc_code_to_status_family(grpc_err.code());
                        let _ = tx.send(Err(grpc_err)).await;
                        break;
                    }
                    Some(pb::stream_response::Payload::Done(_)) => {
                        break;
                    }
                    _ => {}
                }
            }
            if stream_metrics {
                crate::metrics::prometheus::record_stream_close(&metrics_model, &metrics_version, "grpc");
            }
            crate::metrics::prometheus::record_request_end(
                &metrics_model,
                &metrics_version,
                stream_family,
                start.elapsed().as_secs_f64(),
            );
            // Cleanup: send cancel to worker. `send_raw` (fire-and-forget) —
            // the worker signals the generator to stop and sends NO unary reply
            // to a Cancel, so `.send()` would await the full ZMQ_RESPONSE_TIMEOUT
            // (300s). Aligned with bidi/decoupled/HTTP stream (P-FLOW §4.0.9).
            let cancel_req = streaming::build_stream_cancel(stream_id);
            let _ = cancel_client.send_raw(cancel_req).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn decoupled_infer_impl(
        &self,
        request: Request<pb::DecoupledInferRequest>,
        version_label: &mut String,
        request_id_out: &mut String,
        start: Instant,
    ) -> Result<Response<ReceiverStream<Result<pb::DecoupledResponse, Status>>>, Status> {
        // P9-1 (蓝图 §4.4, D18): DecoupledInfer = a stream whose channel the
        // MODEL controls — predict_decoupled returns before close(), the worker
        // pushes N async chunks, ending with sender.close(). Reuses the ZMQ
        // stream mechanism with StreamOpen.decoupled=true; the only new logic
        // vs stream_infer is the idle-timeout wrapper and the DecoupledResponse
        // mapping. Thin translation layer (D3): resolve → auth → rate-limit →
        // open a decoupled stream → forward chunks.
        let remote_addr = request.remote_addr();
        let (grpc_metadata, extensions, req) = request.into_parts();
        let cx = interceptor::finalize_context(
            extensions.get::<RequestContext>().cloned(),
            &grpc_metadata,
            &req.headers,
            remote_addr,
            &self.trusted,
        );
        let request_id = cx.request_id;
        *request_id_out = request_id.clone();
        let client_ip = cx.client_ip;
        let model_name = &req.model_name;
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        if let Err(e) = crate::validation::validate_identifier(model_name) {
            return Err(err(Status::invalid_argument(e.to_string())));
        }

        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => match canary_pin(
                &self.registry,
                self.canary_override,
                model_name,
                &grpc_metadata,
                &req.headers,
            )? {
                Some(pin) => pin,
                None => self
                    .registry
                    .routing_pick(model_name)
                    .or_else(|| self.registry.get_active_version(model_name))
                    .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
            },
        };
        self.registry.touch_last_used(model_name, &resolved_version);
        *version_label = resolved_version.clone();

        if !self.registry.is_ready(model_name, Some(&resolved_version)) {
            return Err(err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            ))));
        }

        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
            enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), model_name, &client_ip)?;
        }

        let mut header_map: HashMap<String, String> = req.headers.clone();
        // P-TRACE: inject the active inference span's trace context into the
        // worker RequestMeta.headers (overwrites any client-supplied traceparent
        // so the worker is a child of THIS span; D8 Rust-only).
        crate::telemetry::inject(&mut header_map);
        // P-DEADLINE (§4.0.10): carry to worker; the always-on decoupled idle
        // reclaim stays, with an overall deadline layered on when the CLIENT
        // specifies one.
        let deadline =
            crate::deadline::resolve_from_grpc(&grpc_metadata, self.server_timeout.as_secs_f32());
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: header_map,
            client_ip,
            request_id,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: req.data.clone(),
            sequence_id: req.sequence_id.clone(),
            deadline_unix_ns: deadline.unix_ns,
            ..Default::default()
        };
        // Capture before `meta` moves into `open_req` (used by the affinity pick).
        let sequence_id = meta.sequence_id.clone();

        let stream_id = format!("grpc-decoupled-{}", Uuid::new_v4());
        // decoupled=true → the worker keeps the channel open after
        // predict_decoupled returns (model-controlled lifetime).
        let open_req = streaming::build_stream_open(stream_id.clone(), req.data, Some(meta), true);

        let clients = self
            .worker_manager
            .get_zmq_clients(model_name, &resolved_version)
            .await
            .ok_or_else(|| err(Status::unavailable("no workers available")))?;
        if clients.is_empty() {
            return Err(err(Status::unavailable("no workers available")));
        }

        // P8-1 sticky pick (same block as stream_infer — direct connect).
        let outlier = self
            .worker_manager
            .get_outlier_state(model_name.as_str(), &resolved_version)
            .await;
        let seq_registry = self.app_state.inference_queue.sequence_registry();
        let num_workers = clients.len();
        let preferred = sequence_id.as_deref().and_then(|seq| {
            let w = seq_registry.lookup(seq, model_name, &resolved_version)?;
            let ejected = outlier.as_ref().map(|o| o.is_ejected(w)).unwrap_or(false);
            (w < num_workers && !ejected).then_some(w)
        });
        let worker_id = preferred.unwrap_or_else(|| match &outlier {
            Some(o) => crate::worker::pick_worker_skip_ejected(num_workers, o),
            None => crate::worker::pick_worker_random(num_workers),
        });
        if let Some(seq) = sequence_id.as_deref() {
            seq_registry.record(seq, model_name, &resolved_version, worker_id);
        }
        let client = clients[worker_id].clone();
        crate::metrics::prometheus::record_worker_inference(
            model_name,
            &resolved_version,
            worker_id,
            1,
        );

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;

        let (tx, rx) = mpsc::channel(64);
        let cancel_client = client.clone();

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.to_string();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc");
        }

        // P-DEADLINE + P9-1: always-on decoupled idle reclaim, plus an overall
        // deadline layered on only when the CLIENT specified one.
        let stream_idle = self.decoupled_idle_timeout;
        let stream_deadline = if deadline.client_specified {
            crate::deadline::to_instant(deadline.unix_ns)
        } else {
            None
        };
        tokio::spawn(async move {
            let open_time = std::time::Instant::now();
            let mut first_chunk = true;
            let mut last_chunk_time = open_time;
            // P2-1：流关闭时记一次整体 duration；中途 worker 错误按其状态族记。
            let mut stream_family = "2xx";

            loop {
                let chunk = match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                    .await
                {
                    Ok(Some(c)) => c,
                    Ok(None) => break, // actor dropped the route (Done/Error forwarded)
                    Err(elapsed) => {
                        tracing::warn!(
                            ?elapsed, stream_id = %stream_id,
                            "decoupled stream closed: deadline/idle elapsed"
                        );
                        stream_family = "5xx";
                        break;
                    }
                };
                match chunk.payload {
                    Some(pb::stream_response::Payload::Chunk(ref c)) => {
                        if stream_metrics {
                            if first_chunk {
                                crate::metrics::prometheus::record_stream_ttft(&metrics_model, &metrics_version, "grpc", open_time.elapsed().as_secs_f64());
                                first_chunk = false;
                            } else {
                                crate::metrics::prometheus::record_stream_tbt(&metrics_model, &metrics_version, "grpc", last_chunk_time.elapsed().as_secs_f64());
                            }
                            last_chunk_time = std::time::Instant::now();
                            crate::metrics::prometheus::record_stream_chunk(&metrics_model, &metrics_version, "grpc");
                        }
                        let resp = pb::DecoupledResponse { data: c.data.clone(), is_final: false };
                        if tx.send(Ok(resp)).await.is_err() {
                            break; // client disconnect
                        }
                    }
                    Some(pb::stream_response::Payload::Error(ref e)) => {
                        let grpc_err = match serde_json::from_str::<serde_json::Value>(&e.message) {
                            Ok(val) => {
                                if let Some(parsed) = try_parse_model_error(&val) {
                                    err(model_error_status(
                                        error_type_to_grpc_code(&parsed.error_type),
                                        &parsed,
                                    ))
                                } else {
                                    err(Status::internal(e.message.clone()))
                                }
                            }
                            Err(_) => err(Status::internal(e.message.clone())),
                        };
                        stream_family = grpc_code_to_status_family(grpc_err.code());
                        let _ = tx.send(Err(grpc_err)).await;
                        break;
                    }
                    Some(pb::stream_response::Payload::Done(_)) => {
                        // Model called sender.close(): emit the terminal is_final
                        // frame, then end the gRPC stream.
                        let _ = tx
                            .send(Ok(pb::DecoupledResponse { data: Default::default(), is_final: true }))
                            .await;
                        break;
                    }
                    _ => {}
                }
            }
            if stream_metrics {
                crate::metrics::prometheus::record_stream_close(&metrics_model, &metrics_version, "grpc");
            }
            crate::metrics::prometheus::record_request_end(
                &metrics_model,
                &metrics_version,
                stream_family,
                start.elapsed().as_secs_f64(),
            );
            // Cleanup: cancel the worker. send_raw = fire-and-forget: a
            // stream-cancel gets no unary reply, so this avoids the phantom
            // 300s await of send() (对齐 HTTP stream.rs cancel path).
            let cancel_req = streaming::build_stream_cancel(stream_id);
            let _ = cancel_client.send_raw(cancel_req).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn bidi_stream_impl(
        &self,
        request: Request<Streaming<pb::BidiChunk>>,
        model_label: &mut String,
        version_label: &mut String,
        request_id_out: &mut String,
        start: Instant,
    ) -> Result<Response<ReceiverStream<Result<pb::BidiChunk, Status>>>, Status> {
        let remote_addr = request.remote_addr();
        let (grpc_metadata, extensions, mut stream) = request.into_parts();
        // BidiOpen carries no headers map — metadata and the transport peer
        // address are the only client-identity sources on this path.
        let cx = interceptor::finalize_context(
            extensions.get::<RequestContext>().cloned(),
            &grpc_metadata,
            &HashMap::new(),
            remote_addr,
            &self.trusted,
        );
        let request_id = cx.request_id;
        *request_id_out = request_id.clone();
        let client_ip = cx.client_ip;
        // P-DEADLINE (§4.0.10): resolved here (grpc_metadata is in scope) and
        // captured into the body below; bidi streaming two-stage bound activates
        // only when the CLIENT specified a deadline.
        let deadline =
            crate::deadline::resolve_from_grpc(&grpc_metadata, self.server_timeout.as_secs_f32());

        // Wait for first message (must be BidiOpen)
        let first = stream
            .message()
            .await
            .map_err(|e| err(Status::internal(format!("stream error: {}", e))))?;

        let (model_name, resolved_version, stream_id, initial_data, sequence_id, pin) = match first {
            Some(chunk) => match chunk.payload {
                Some(pb::bidi_chunk::Payload::Open(open)) => {
                    let model_name = open.model_name;
                    let version = if open.version.is_empty() {
                        None
                    } else {
                        Some(open.version)
                    };

                    if let Err(e) = crate::validation::validate_identifier(&model_name) {
                        return Err(err(Status::invalid_argument(e.to_string())));
                    }

                    // P5-2: bidi 仅 metadata 携带 pin（BidiOpen 无 headers map）。
                    let pin = canary_pin(
                        &self.registry,
                        self.canary_override,
                        &model_name,
                        &grpc_metadata,
                        &HashMap::new(),
                    )?;

                    // version="" → canary pin (P5-2, 开关开) → weighted routing
                    // pick (§4.3), falling back to active; stamps last_used_at
                    // (P0-2 bidi parity).
                    let resolved_version =
                        resolve_bidi_version(&self.registry, &model_name, version.as_deref(), pin.clone())?;

                    if !self.registry.is_ready(&model_name, Some(&resolved_version)) {
                        return Err(err(Status::unavailable(format!(
                            "{} version {} is not ready",
                            model_name, resolved_version
                        ))));
                    }

                    // BidiOpen has no headers map — transport metadata is the
                    // only credential carrier on this path.
                    if let Some(mv) = self.registry.get(&model_name, Some(&resolved_version)) {
                        enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &HashMap::new())?;
                        // bidi key="ip" 共享 bucket（注释注明：所有 bidi 请求归一）。
                        enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), &model_name, &client_ip)?;
                    }

                    let sid = format!("grpc-bidi-{}", Uuid::new_v4());
                    (model_name, resolved_version, sid, open.initial_data, open.sequence_id, pin)
                }
                _ => return Err(err(Status::invalid_argument("first message must be BidiOpen"))),
            },
            None => return Err(err(Status::invalid_argument("empty stream"))),
        };

        *model_label = model_name.clone();
        *version_label = resolved_version.clone();

        // P2-3 span：覆盖 bidi handler 全程（model/version 在 Open 解码后已知）。
        // P5-2：pin 命中记 pinned_version（bidi span 在解析后才创建，无法走
        // canary_pin 内的 Span::current().record，在此补记）。
        let span = tracing::info_span!(
            "inference",
            model = %model_name,
            version = %resolved_version,
            request_id = %request_id,
            pinned_version = tracing::field::Empty,
        );
        // P-TRACE: link the bidi inference span to the inbound trace (D21 single
        // extract — reuses the interceptor's RequestContext.trace_cx).
        crate::telemetry::link_parent(&span, &cx.trace_cx);
        if let Some(p) = &pin {
            span.record("pinned_version", p.as_str());
        }
        async move {
        // P-TRACE: inject the bidi inference span's trace context (worker child).
        let mut bidi_headers = HashMap::new();
        crate::telemetry::inject(&mut bidi_headers);
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: bidi_headers,
            client_ip,
            request_id,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: initial_data.clone(),
            sequence_id: sequence_id.clone(),
            deadline_unix_ns: deadline.unix_ns,
            ..Default::default()
        };

        let open_req = streaming::build_stream_open(stream_id.clone(), initial_data, Some(meta), false);

        let clients = self
            .worker_manager
            .get_zmq_clients(&model_name, &resolved_version)
            .await
            .ok_or_else(|| err(Status::unavailable("no workers available")))?;

        if clients.is_empty() {
            return Err(err(Status::unavailable("no workers available")));
        }

        // P8-1: connect directly to the worker that last served this
        // sequence_id when it is still registered and not ejected; else the
        // normal pick, then record the chosen worker.
        let outlier = self
            .worker_manager
            .get_outlier_state(model_name.as_str(), &resolved_version)
            .await;
        let seq_registry = self.app_state.inference_queue.sequence_registry();
        let num_workers = clients.len();
        let preferred = sequence_id.as_deref().and_then(|seq| {
            let w = seq_registry.lookup(seq, &model_name, &resolved_version)?;
            let ejected = outlier.as_ref().map(|o| o.is_ejected(w)).unwrap_or(false);
            (w < num_workers && !ejected).then_some(w)
        });
        let worker_id = preferred.unwrap_or_else(|| match &outlier {
            Some(o) => crate::worker::pick_worker_skip_ejected(num_workers, o),
            None => crate::worker::pick_worker_random(num_workers),
        });
        if let Some(seq) = sequence_id.as_deref() {
            seq_registry.record(seq, &model_name, &resolved_version, worker_id);
        }
        let client = clients[worker_id].clone();
        // P6 GetModelStats: one bidi inference dispatched to this worker.
        crate::metrics::prometheus::record_worker_inference(
            &model_name,
            &resolved_version,
            worker_id,
            1,
        );

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;

        let (tx, rx) = mpsc::channel(64);
        let worker_client = client.clone();
        // B3: worker_client is moved into the incoming (client→worker) task below,
        // so keep a separate clone for the cleanup cancel (aligned with stream_infer
        // :722 / decoupled :960 `cancel_client`).
        let cancel_client = client.clone();

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.clone();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc");
        }

        // Spawn forwarder: worker chunks -> gRPC stream
        let stream_id_for_incoming = stream_id.clone();
        // P-DEADLINE streaming bound (client-specified only).
        let (stream_deadline, stream_idle) = if deadline.client_specified {
            (
                crate::deadline::to_instant(deadline.unix_ns),
                self.decoupled_idle_timeout,
            )
        } else {
            (None, None)
        };
        tokio::spawn(async move {
            // Forward incoming bidi chunks to worker as StreamRequest::Chunk.
            // These are fire-and-forget: the worker's response to each chunk
            // comes back as a StreamResponse routed through the stream's
            // channel (registered at open), so we must NOT use send() — that
            // would await a unary reply that never matches and stall for
            // ZMQ_RESPONSE_TIMEOUT between chunks.
            let incoming_task = tokio::spawn(async move {
                while let Some(Ok(chunk)) = stream.message().await.transpose() {
                    match chunk.payload {
                        Some(pb::bidi_chunk::Payload::Data(data)) => {
                            let chunk_req = streaming::build_stream_chunk(
                                stream_id_for_incoming.clone(),
                                data.data,
                            );
                            let _ = worker_client.send_raw(chunk_req).await;
                        }
                        Some(pb::bidi_chunk::Payload::Close(_)) => {
                            let close_req = streaming::build_stream_close(stream_id_for_incoming.clone());
                            let _ = worker_client.send_raw(close_req).await;
                            break;
                        }
                        _ => {}
                    }
                }
            });

            // Forward worker chunks -> gRPC
            let open_time = std::time::Instant::now();
            let mut first_chunk = true;
            let mut last_chunk_time = open_time;
            // P2-1：流关闭时记一次整体 duration；中途 worker 错误按其状态族记。
            let mut stream_family = "2xx";

            loop {
                let chunk = match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                    .await
                {
                    Ok(Some(c)) => c,
                    Ok(None) => break, // worker closed the stream
                    Err(elapsed) => {
                        // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                        tracing::warn!(
                            ?elapsed, stream_id = %stream_id,
                            "bidi stream closed: deadline/idle elapsed"
                        );
                        stream_family = "5xx";
                        break;
                    }
                };
                match chunk.payload {
                    Some(pb::stream_response::Payload::Chunk(ref c)) => {
                        if stream_metrics {
                            if first_chunk {
                                crate::metrics::prometheus::record_stream_ttft(&metrics_model, &metrics_version, "grpc", open_time.elapsed().as_secs_f64());
                                first_chunk = false;
                            } else {
                                crate::metrics::prometheus::record_stream_tbt(&metrics_model, &metrics_version, "grpc", last_chunk_time.elapsed().as_secs_f64());
                            }
                            last_chunk_time = std::time::Instant::now();
                            crate::metrics::prometheus::record_stream_chunk(&metrics_model, &metrics_version, "grpc");
                        }
                        let bidi_chunk = pb::BidiChunk {
                            stream_id: stream_id.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                                data: c.data.clone(),
                            })),
                        };
                        if tx.send(Ok(bidi_chunk)).await.is_err() {
                            break;
                        }
                    }
                    Some(pb::stream_response::Payload::Error(ref e)) => {
                        let bidi_chunk = pb::BidiChunk {
                            stream_id: stream_id.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Close(pb::BidiClose {})),
                        };
                        let grpc_err = match serde_json::from_str::<serde_json::Value>(&e.message) {
                            Ok(val) => {
                                if let Some(parsed) = try_parse_model_error(&val) {
                                    err(model_error_status(
                                        error_type_to_grpc_code(&parsed.error_type),
                                        &parsed,
                                    ))
                                } else {
                                    err(Status::internal(e.message.clone()))
                                }
                            }
                            Err(_) => err(Status::internal(e.message.clone())),
                        };
                        stream_family = grpc_code_to_status_family(grpc_err.code());
                        let _ = tx.send(Err(grpc_err)).await;
                        let _ = tx.send(Ok(bidi_chunk)).await;
                        break;
                    }
                    Some(pb::stream_response::Payload::Done(_)) => {
                        let bidi_chunk = pb::BidiChunk {
                            stream_id: stream_id.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Close(pb::BidiClose {})),
                        };
                        let _ = tx.send(Ok(bidi_chunk)).await;
                        break;
                    }
                    _ => {}
                }
            }

            if stream_metrics {
                crate::metrics::prometheus::record_stream_close(&metrics_model, &metrics_version, "grpc");
            }
            crate::metrics::prometheus::record_request_end(
                &metrics_model,
                &metrics_version,
                stream_family,
                start.elapsed().as_secs_f64(),
            );

            // B3 audit fix: cancel the worker on forwarder exit, aligned with
            // stream_infer (grpc/mod.rs:791) and decoupled (:1038). bidi was
            // previously the only streaming path that never sent a cancel on
            // client disconnect / deadline / idle — the worker kept generating
            // into an undrained ZMQ channel, pinning a worker slot. send_raw =
            // fire-and-forget (a cancel draws no unary reply, so .send() would
            // stall for ZMQ_RESPONSE_TIMEOUT). Harmless when the worker already
            // stopped (Done/Error) — it ignores the cancel.
            let cancel_req = streaming::build_stream_cancel(stream_id.clone());
            let _ = cancel_client.send_raw(cancel_req).await;

            // #8: observe the incoming task so a panic is logged, not silently
            // dropped by a bare abort().
            observe_or_abort(incoming_task).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
        }.instrument(span).await
    }
}

// ---------------------------------------------------------------------------
// Helpers: map worker error signals to gRPC status codes
// ---------------------------------------------------------------------------

/// Map an HTTP status code (from the worker's Status.message) to a gRPC code.
fn http_status_to_grpc_code(http_status: u16) -> tonic::Code {
    match http_status {
        400 | 422 => tonic::Code::InvalidArgument,
        401 => tonic::Code::Unauthenticated,
        403 => tonic::Code::PermissionDenied,
        404 => tonic::Code::NotFound,
        409 => tonic::Code::AlreadyExists,
        429 => tonic::Code::ResourceExhausted,
        503 => tonic::Code::Unavailable,
        504 => tonic::Code::DeadlineExceeded,
        _ => tonic::Code::Internal,
    }
}

/// Map an [`AppError`] to a gRPC [`Status`] for the Admin service (D4:
/// centralized mapping — admin RPCs reuse this instead of inlining). Derives
/// the gRPC code from the same HTTP status the REST path returns
/// ([`AppError::IntoResponse`]) and carries only the sanitized public message
/// (no internal detail / path leak, D4 details 白名单). Used by P6 Admin RPCs.
pub(crate) fn app_error_to_grpc_status(e: &AppError) -> Status {
    // ModelError carries its own status_code + model-author-facing detail.
    if let AppError::ModelError(d) = e {
        let code = http_status_to_grpc_code(d.status_code);
        return Status::new(code, format!("[{}] {}", d.error_type, d.detail));
    }
    let http_status: u16 = match e {
        AppError::ModelNotFound(_) | AppError::VersionNotFound(_, _) | AppError::RouteNotFound => 404,
        AppError::ModelNotReady(_) | AppError::QueueFull(_) => 503,
        AppError::VersionAlreadyLoaded(_, _) => 409,
        AppError::InferenceTimeout(_) => 504,
        AppError::Validation(_)
        | AppError::Config(_)
        | AppError::Serialization(_)
        | AppError::InvalidRequestBody(_)
        | AppError::InvalidQueryParam(_) => 400,
        AppError::FrameTooLarge => 413,
        AppError::RateLimitExceeded { .. } => 429,
        AppError::Unauthorized(_) => 401,
        AppError::MethodNotAllowed => 405,
        _ => 500,
    };
    Status::new(http_status_to_grpc_code(http_status), e.pub_error_message())
}

/// Map an error_type string (from a structured stream error) to a gRPC code.
fn error_type_to_grpc_code(error_type: &str) -> tonic::Code {
    match error_type {
        "invalid_request_error" => tonic::Code::InvalidArgument,
        "authentication_error" => tonic::Code::Unauthenticated,
        "permission_denied_error" => tonic::Code::PermissionDenied,
        "not_found_error" => tonic::Code::NotFound,
        "service_unavailable" | "model_not_ready" => tonic::Code::Unavailable,
        // P9-1: a decoupled stream on a model without predict_decoupled.
        "not_implemented" => tonic::Code::FailedPrecondition,
        _ => tonic::Code::Internal,
    }
}

/// Parsed fields from a structured model error JSON payload.
struct ParsedModelError {
    error_type: String,
    message: String,
    code: Option<String>,
    param: Option<String>,
}

/// Extract structured error fields from a model error JSON payload.
fn try_parse_model_error(data: &serde_json::Value) -> Option<ParsedModelError> {
    let err = data.get("error")?;
    let error_type = err.get("type")?.as_str()?.to_string();
    let message = err.get("message")?.as_str()?.to_string();
    let code = err.get("code").and_then(|c| c.as_str()).map(String::from);
    let param = err.get("param").and_then(|p| p.as_str()).map(String::from);
    Some(ParsedModelError { error_type, message, code, param })
}

/// Build a gRPC Status from a parsed model error. The message keeps the
/// legacy `[error_type] message` format; code/param are attached as standard
/// gRPC ErrorInfo details so clients can read them programmatically.
fn model_error_status(code: tonic::Code, parsed: &ParsedModelError) -> Status {
    use tonic_types::{ErrorDetails, StatusExt};

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("error_type".to_string(), parsed.error_type.clone());
    if let Some(p) = &parsed.param {
        metadata.insert("param".to_string(), p.clone());
    }
    // Same fallback as AppError::error_code() for ModelError
    let reason = parsed.code.as_deref().unwrap_or(&parsed.error_type);
    Status::with_error_details(
        code,
        format!("[{}] {}", parsed.error_type, parsed.message),
        ErrorDetails::with_error_info(reason, "lite-server", metadata),
    )
}

/// P5-2 (蓝图 §4.4, D16): 提取并校验 `x-lite-version` canary pin——metadata 优先，
/// fallback proto headers map（bidi 无 headers map，调用方传空 map → 仅 metadata）。
///
/// - `canary_override=false`（默认）→ `Ok(None)` + debug 日志：pin 完全不参与解析
///   （连非法值也不校验，与 HTTP 侧开关关行为一致）。
/// - 开关开：非法 pin → InvalidArgument（与 HTTP validate_version 同一守卫，B4
///   parity）；pin 版本未注册 → NotFound。
/// - pin 命中在当前 span 记 `pinned_version`（bidi 的 span 在解析后才创建，
///   由调用方自行 record）。
fn canary_pin(
    registry: &ModelRegistry,
    canary_override: bool,
    model_name: &str,
    metadata: &MetadataMap,
    proto_headers: &HashMap<String, String>,
) -> Result<Option<String>, Status> {
    let pin = metadata
        .get("x-lite-version")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            proto_headers
                .get("x-lite-version")
                .filter(|s| !s.is_empty())
                .cloned()
        });
    let Some(pin) = pin else { return Ok(None) };
    if !canary_override {
        tracing::debug!(
            model = %model_name,
            pinned_version = %pin,
            "x-lite-version pin ignored (features.canary_override=false)"
        );
        return Ok(None);
    }
    if let Err(e) = crate::validation::validate_version(&pin) {
        return Err(err(Status::invalid_argument(e.to_string())));
    }
    if registry.get(model_name, Some(&pin)).is_none() {
        return Err(err(Status::not_found(format!(
            "{} version {} not found",
            model_name, pin
        ))));
    }
    tracing::Span::current().record("pinned_version", pin.as_str());
    Ok(Some(pin))
}

/// Resolve the serving version for `bidi_stream` (P0-2 parity with
/// unary/batch/stream): version="" → canary pin (P5-2, 开关开时由
/// [`canary_pin`] 提供) → weighted routing pick (§4.3), falling
/// back to the active version; explicit version passes through. Stamps
/// `last_used_at` for LRU eviction on the resolved version.
///
/// The protocol layer only passes parameters — the actual routing decision
/// is delegated to the registry (`routing_pick` / `get_active_version`).
fn resolve_bidi_version(
    registry: &ModelRegistry,
    model_name: &str,
    version: Option<&str>,
    pin: Option<String>,
) -> Result<String, Status> {
    let resolved = match version {
        Some(v) => v.to_string(),
        None => match pin {
            Some(p) => p,
            None => registry
                .routing_pick(model_name)
                .or_else(|| registry.get_active_version(model_name))
                .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
        },
    };
    registry.touch_last_used(model_name, &resolved);
    Ok(resolved)
}

/// Whether a gRPC status code is a client-class error (P1-1). Mirrors the
/// HTTP 4xx/5xx split in error.rs: client-class codes log at info, server
/// faults at error. ResourceExhausted (429) is client-class so a saturated
/// rate limiter doesn't flood error logs; Cancelled is client-initiated.
fn is_client_class(code: tonic::Code) -> bool {
    use tonic::Code::*;
    matches!(
        code,
        InvalidArgument
            | NotFound
            | OutOfRange
            | Unauthenticated
            | PermissionDenied
            | ResourceExhausted
            | Cancelled
    )
}

/// gRPC 状态码 → 请求指标 status 族（P2-1，蓝图 §4.3 P2-1）：成功 → "2xx"；
/// 客户端类（与 `is_client_class` 同集——InvalidArgument/NotFound/OutOfRange/
/// Unauthenticated/PermissionDenied/ResourceExhausted(限流)/Cancelled）→ "4xx"；
/// 其余服务端故障（Internal/Unavailable/DeadlineExceeded 等）→ "5xx"。
/// queue-full/过载返 Unavailable 天然落 "5xx"（§4.0.9 收口，D5：无 protocol label）。
fn grpc_code_to_status_family(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "2xx",
        c if is_client_class(c) => "4xx",
        _ => "5xx",
    }
}

/// P2-3：从入站 metadata 取 request_id（span 字段用；与 interceptor 同源）。
fn metadata_request_id(metadata: &MetadataMap) -> String {
    metadata
        .get("x-client-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_default()
}

/// P2-2：将 `x-request-id` + `x-processing-time-ms` 注入响应/错误 metadata
///（对齐 HTTP observability_middleware 错误路径回显）。interceptor 是 pre-call
/// 无法改响应（评审 1.12），故回显在 handler 出口统一完成。
fn echo_grpc_response_headers<T>(
    result: Result<Response<T>, Status>,
    request_id: &str,
    start: Instant,
) -> Result<Response<T>, Status> {
    let elapsed = start.elapsed();
    match result {
        Ok(mut response) => {
            inject_echo_headers(response.metadata_mut(), request_id, elapsed);
            Ok(response)
        }
        Err(mut status) => {
            inject_echo_headers(status.metadata_mut(), request_id, elapsed);
            Err(status)
        }
    }
}

/// 注入回显 header（request_id 缺省/非法则跳过该项；processing-time 恒定注入）。
fn inject_echo_headers(metadata: &mut MetadataMap, request_id: &str, elapsed: Duration) {
    if !request_id.is_empty() {
        if let Ok(v) = MetadataValue::try_from(request_id) {
            metadata.insert("x-request-id", v);
        }
    }
    if let Ok(v) = MetadataValue::try_from(elapsed.as_millis().to_string().as_str()) {
        metadata.insert("x-processing-time-ms", v);
    }
}

/// P2-1：unary 请求指标统一记录点（成功 "2xx"；错误按 `grpc_code_to_status_family`）。
fn record_grpc_request_end<T>(
    model: &str,
    version: &str,
    start: Instant,
    result: &Result<T, Status>,
) {
    let family = match result {
        Ok(_) => "2xx",
        Err(s) => grpc_code_to_status_family(s.code()),
    };
    crate::metrics::prometheus::record_request_end(model, version, family, start.elapsed().as_secs_f64());
}

/// Log a gRPC error status with graded severity, then return it (P1-1 parity
/// with HTTP error.rs:256-270 — gRPC handlers previously logged nothing).
pub(crate) fn err(status: Status) -> Status {
    if is_client_class(status.code()) {
        tracing::info!(
            code = ?status.code(),
            message = %status.message(),
            "grpc request error"
        );
    } else {
        tracing::error!(
            code = ?status.code(),
            message = %status.message(),
            "grpc request error"
        );
    }
    status
}

/// Attach a `retry-after` trailing metadata (seconds) to a Status — the gRPC
/// analogue of HTTP's `Retry-After` header for load-shedding / admission
/// rejection (§4.0.9; same metadata key the rate limiter uses).
pub(crate) fn with_retry_after(mut status: Status, secs: u32) -> Status {
    if let Ok(v) = MetadataValue::try_from(secs.to_string().as_str()) {
        status.metadata_mut().insert("retry-after", v);
    }
    status
}

/// Headers that must not be set by user code (RFC 7230 §6.1 hop-by-hop headers
/// and other transport headers managed by the server).
const BLOCKED_RESPONSE_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "transfer-encoding",
    "content-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
];

/// Inject custom response headers into tonic gRPC metadata,
/// blocking hop-by-hop and transport headers.
fn inject_grpc_metadata(
    metadata: &mut MetadataMap,
    headers: &HashMap<String, String>,
) {
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if BLOCKED_RESPONSE_HEADERS.contains(&lower.as_str()) {
            continue;
        }
        if let Ok(mk) = MetadataKey::from_bytes(k.as_bytes()) {
            if let Ok(mv) = MetadataValue::try_from(v.as_str()) {
                metadata.insert(mk, mv);
            }
        }
    }
}

/// API-key enforcement mirroring the HTTP layer's `enforce_auth`: transport
/// metadata first (idiomatic gRPC), then the protobuf `headers` map
/// (REST→gRPC bridges). An empty `keys` list accepts any non-empty value.
fn enforce_auth_grpc(
    auth: Option<&crate::config::AuthPolicy>,
    metadata: &MetadataMap,
    headers: &HashMap<String, String>,
) -> Result<(), Status> {
    let Some(auth) = auth else {
        return Ok(());
    };
    let value = metadata
        .get(&auth.header)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            // Proto headers map is a plain HashMap — match case-insensitively
            // per HTTP header semantics.
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&auth.header))
                .map(|(_, v)| v.clone())
        })
        .unwrap_or_default();
    if value.is_empty() {
        return Err(err(Status::unauthenticated(format!(
            "missing API key (header: {})",
            auth.header
        ))));
    }
    if !auth.keys.is_empty() && !auth.keys.iter().any(|k| k == &value) {
        return Err(err(Status::unauthenticated(format!(
            "invalid API key (header: {})",
            auth.header
        ))));
    }
    Ok(())
}

/// gRPC 限流（P3-1，对齐 HTTP `enforce_rate_limit`）：policy 来自
/// `ModelVersion.policies.rate_limit`；`key=="ip"` 用清洗后 client_ip，否则
/// `/predict` 路由 scope（同模型所有推理共享一桶）。超限 → ResourceExhausted
/// （**专给限流**，落 4xx）+ `retry-after` metadata（§4.0.9 收口；queue-full/
/// 过载用 Unavailable）。无 policy 不限；`rpm<=0` fail-closed（RateLimiter 内置）。
/// 限流放 handler：interceptor 在 decode 前取不到 model 名（D6）。
fn enforce_grpc_rate_limit(
    rate_limiter: &crate::rate_limit::RateLimiter,
    rl: Option<&crate::config::RateLimitPolicy>,
    model_name: &str,
    client_ip: &str,
) -> Result<(), Status> {
    let Some(rl) = rl else {
        return Ok(());
    };
    let scope = match rl.key.as_str() {
        "ip" => client_ip.to_string(),
        _ => "/predict".to_string(),
    };
    if rl.key == "ip" && scope.is_empty() {
        warn!(
            model = %model_name,
            "rate-limit key=ip resolved to empty scope; all requests share one bucket"
        );
    }
    let burst = rl.burst.unwrap_or(rl.requests_per_minute * 1.5);
    let key = format!("{}:{}", model_name, scope);
    match rate_limiter.acquire(&key, rl.requests_per_minute, burst) {
        crate::rate_limit::AcquireResult::Allowed => Ok(()),
        crate::rate_limit::AcquireResult::Rejected { retry_after_secs } => {
            let mut status = Status::resource_exhausted(format!(
                "rate limit exceeded for {} (retry in {}s)",
                model_name, retry_after_secs
            ));
            // retry-after 经 metadata 回传（对齐 HTTP Retry-After header）。
            if let Ok(v) = MetadataValue::try_from(retry_after_secs.to_string().as_str()) {
                status.metadata_mut().insert("retry-after", v);
            }
            Err(err(status))
        }
    }
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

/// Observe a spawned bidi helper task instead of silently dropping its handle
/// (#8). `abort()` alone discards any panic payload; awaiting the handle first
/// — bounded by a short grace — surfaces a panic as a `JoinError` so we can
/// log it. If the task is still running when the grace expires, abort it to
/// release the client stream promptly. Returns `true` if the task panicked
/// (kept as a return value so the panic-detection path is unit-testable).
async fn observe_or_abort(mut task: tokio::task::JoinHandle<()>) -> bool {
    match tokio::time::timeout(Duration::from_millis(500), &mut task).await {
        Ok(Ok(())) => false,
        Ok(Err(join_err)) if join_err.is_panic() => {
            warn!("bidi incoming task panicked during shutdown");
            true
        }
        _ => {
            task.abort();
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic_types::StatusExt;

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

    #[tokio::test]
    async fn observe_or_abort_detects_panicked_task() {
        // #8: a panic inside the bidi incoming task must be observable. abort()
        // alone discards the panic payload; awaiting the handle (bounded by a
        // grace) surfaces it as a JoinError with is_panic() == true.
        let task = tokio::spawn(async {
            panic!("boom");
        });
        let panicked = observe_or_abort(task).await;
        assert!(panicked, "a panicked task must report panicked=true");
    }

    #[tokio::test]
    async fn observe_or_abort_clean_finish_is_not_panic() {
        let task = tokio::spawn(async {});
        let panicked = observe_or_abort(task).await;
        assert!(!panicked);
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

    // --- P1-1: graded error logging (client-class → info, server faults → error) ---

    #[test]
    fn test_is_client_class_codes_log_at_info() {
        use tonic::Code;
        // Client-class codes (HTTP 4xx analogues) — including ResourceExhausted
        // (429): a saturated rate limiter must not flood error logs.
        for code in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::OutOfRange,
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::Cancelled,
        ] {
            assert!(is_client_class(code), "{code:?} should be client-class");
        }
    }

    #[test]
    fn test_server_fault_codes_log_at_error() {
        use tonic::Code;
        for code in [
            Code::Internal,
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::Unknown,
            Code::DataLoss,
            Code::Unimplemented,
        ] {
            assert!(!is_client_class(code), "{code:?} should be a server fault");
        }
    }

    // --- P0-2: bidi version resolution parity (routing_pick + touch_last_used) ---

    fn bidi_test_registry() -> ModelRegistry {
        use crate::config::ModelConfig;
        use crate::registry::types::ModelType;

        let reg = ModelRegistry::new();
        let dir = std::env::temp_dir().join(format!("lite-server-grpc-test-{}", std::process::id()));
        for v in ["1", "2"] {
            reg.register(
                "m1",
                v,
                ModelConfig { max_batch_size: 1, ..Default::default() },
                ModelType::LitAPI,
                dir.clone(),
            )
            .unwrap();
            reg.mark_ready("m1", v).unwrap();
        }
        reg
    }

    #[test]
    fn test_bidi_resolve_version_uses_weighted_routing() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();
        // Weight 100/0 → deterministic pick of the weighted version, even
        // though "1" is the active version.
        reg.set_weights("m1", &HashMap::from([("1".into(), 0u32), ("2".into(), 100)]))
            .unwrap();

        let resolved = resolve_bidi_version(&reg, "m1", None, None).unwrap();
        assert_eq!(resolved, "2");
    }

    #[test]
    fn test_bidi_resolve_version_falls_back_to_active_without_routing() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();

        let resolved = resolve_bidi_version(&reg, "m1", None, None).unwrap();
        assert_eq!(resolved, "1");
    }

    #[test]
    fn test_bidi_resolve_version_touches_last_used() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();
        assert!(reg.get("m1", Some("1")).unwrap().last_used_at.is_none());

        let resolved = resolve_bidi_version(&reg, "m1", None, None).unwrap();
        assert_eq!(
            reg.get("m1", Some(&resolved)).unwrap().last_used_at.is_some(),
            true,
            "bidi version resolution must stamp last_used_at like unary/batch/stream"
        );
    }

    #[test]
    fn test_bidi_resolve_version_explicit_passthrough() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();

        // Explicit version bypasses routing/active resolution entirely.
        let resolved = resolve_bidi_version(&reg, "m1", Some("2"), None).unwrap();
        assert_eq!(resolved, "2");
    }

    // --- P5-2: canary_override 开关 + x-lite-version pin（蓝图 §4.4, D16）---

    fn canary_metadata(pin: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert("x-lite-version", pin.parse().unwrap());
        md
    }

    #[test]
    fn test_canary_pin_absent_is_none() {
        let reg = bidi_test_registry();
        let pin = canary_pin(&reg, true, "m1", &MetadataMap::new(), &HashMap::new()).unwrap();
        assert_eq!(pin, None);
    }

    #[test]
    fn test_canary_pin_prefers_metadata_over_proto_headers() {
        let reg = bidi_test_registry();
        let headers = HashMap::from([("x-lite-version".to_string(), "2".to_string())]);
        let pin = canary_pin(&reg, true, "m1", &canary_metadata("1"), &headers).unwrap();
        assert_eq!(pin.as_deref(), Some("1"), "metadata 优先于 proto headers map");
    }

    #[test]
    fn test_canary_pin_falls_back_to_proto_headers() {
        let reg = bidi_test_registry();
        let headers = HashMap::from([("x-lite-version".to_string(), "2".to_string())]);
        let pin = canary_pin(&reg, true, "m1", &MetadataMap::new(), &headers).unwrap();
        assert_eq!(pin.as_deref(), Some("2"));
    }

    #[test]
    fn test_canary_pin_switch_off_ignores_pin() {
        let reg = bidi_test_registry();
        let pin = canary_pin(&reg, false, "m1", &canary_metadata("1"), &HashMap::new()).unwrap();
        assert_eq!(pin, None, "canary_override=false → pin 被忽略");
        // 非法 pin 在开关关时同样不校验、不报错。
        let pin = canary_pin(&reg, false, "m1", &canary_metadata("a b"), &HashMap::new()).unwrap();
        assert_eq!(pin, None);
    }

    #[test]
    fn test_canary_pin_invalid_is_invalid_argument() {
        let reg = bidi_test_registry();
        let err = canary_pin(&reg, true, "m1", &canary_metadata("a b"), &HashMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_canary_pin_unknown_version_is_not_found() {
        let reg = bidi_test_registry();
        let err = canary_pin(&reg, true, "m1", &canary_metadata("9"), &HashMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_bidi_resolve_version_pin_beats_weights() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();
        reg.set_weights("m1", &HashMap::from([("1".into(), 100u32)])).unwrap();

        let resolved = resolve_bidi_version(&reg, "m1", None, Some("2".to_string())).unwrap();
        assert_eq!(resolved, "2", "pin（开关开）优先级高于 routing_pick");
    }

    #[test]
    fn test_bidi_resolve_version_explicit_beats_pin() {
        let reg = bidi_test_registry();
        let resolved = resolve_bidi_version(&reg, "m1", Some("1"), Some("2".to_string())).unwrap();
        assert_eq!(resolved, "1", "显式 version 优先级高于 pin");
    }

    // B1 readiness-gate tests live in `request_metrics_tests` (below), where
    // the handler-level harness (`build_service_with_canary`, `infer_request`,
    // `test_config`) exists.

    #[test]
    fn test_http_status_to_grpc_code() {
        assert_eq!(http_status_to_grpc_code(400), tonic::Code::InvalidArgument);
        assert_eq!(http_status_to_grpc_code(422), tonic::Code::InvalidArgument);
        assert_eq!(http_status_to_grpc_code(401), tonic::Code::Unauthenticated);
        assert_eq!(http_status_to_grpc_code(403), tonic::Code::PermissionDenied);
        assert_eq!(http_status_to_grpc_code(404), tonic::Code::NotFound);
        assert_eq!(http_status_to_grpc_code(429), tonic::Code::ResourceExhausted);
        assert_eq!(http_status_to_grpc_code(503), tonic::Code::Unavailable);
        assert_eq!(http_status_to_grpc_code(504), tonic::Code::DeadlineExceeded);
        // Unknown HTTP status falls back to Internal
        assert_eq!(http_status_to_grpc_code(418), tonic::Code::Internal);
        assert_eq!(http_status_to_grpc_code(500), tonic::Code::Internal);
    }

    #[test]
    fn test_error_type_to_grpc_code() {
        assert_eq!(error_type_to_grpc_code("invalid_request_error"), tonic::Code::InvalidArgument);
        assert_eq!(error_type_to_grpc_code("authentication_error"), tonic::Code::Unauthenticated);
        assert_eq!(error_type_to_grpc_code("permission_denied_error"), tonic::Code::PermissionDenied);
        assert_eq!(error_type_to_grpc_code("not_found_error"), tonic::Code::NotFound);
        assert_eq!(error_type_to_grpc_code("service_unavailable"), tonic::Code::Unavailable);
        assert_eq!(error_type_to_grpc_code("model_not_ready"), tonic::Code::Unavailable);
        // P9-1: decoupled stream on a model without predict_decoupled.
        assert_eq!(error_type_to_grpc_code("not_implemented"), tonic::Code::FailedPrecondition);
        // Unknown falls back to Internal
        assert_eq!(error_type_to_grpc_code("UNKNOWN_CODE"), tonic::Code::Internal);
    }

    #[test]
    fn test_model_error_status_carries_error_info() {
        let parsed = ParsedModelError {
            error_type: "invalid_request_error".into(),
            message: "bad input".into(),
            code: Some("invalid_input".into()),
            param: Some("temperature".into()),
        };
        let status = model_error_status(tonic::Code::InvalidArgument, &parsed);
        // Message format unchanged for backward compatibility
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "[invalid_request_error] bad input");
        // Structured details: standard gRPC ErrorInfo
        let info = status.get_details_error_info().expect("should carry ErrorInfo");
        assert_eq!(info.reason, "invalid_input");
        assert_eq!(info.domain, "lite-server");
        assert_eq!(info.metadata.get("error_type").map(String::as_str),
            Some("invalid_request_error"));
        assert_eq!(info.metadata.get("param").map(String::as_str), Some("temperature"));
    }

    #[test]
    fn test_model_error_status_reason_falls_back_to_error_type() {
        let parsed = ParsedModelError {
            error_type: "model_error".into(),
            message: "boom".into(),
            code: None,
            param: None,
        };
        let status = model_error_status(tonic::Code::Internal, &parsed);
        let info = status.get_details_error_info().expect("should carry ErrorInfo");
        assert_eq!(info.reason, "model_error");
        assert!(!info.metadata.contains_key("param"));
    }

    #[test]
    fn test_try_parse_model_error_valid() {
        let data = serde_json::json!({
            "error": {
                "type": "INVALID_INPUT",
                "message": "input must be non-negative",
                "code": "invalid_input",
                "param": "temperature",
            }
        });
        let result = try_parse_model_error(&data);
        assert!(result.is_some());
        let p = result.unwrap();
        assert_eq!(p.error_type, "INVALID_INPUT");
        assert_eq!(p.message, "input must be non-negative");
        assert_eq!(p.code.as_deref(), Some("invalid_input"));
        assert_eq!(p.param.as_deref(), Some("temperature"));
    }

    #[test]
    fn test_try_parse_model_error_minimal() {
        // Only required fields (type + message), no code/param — still valid
        let data = serde_json::json!({
            "error": {"type": "X", "message": "Y"}
        });
        let result = try_parse_model_error(&data);
        assert!(result.is_some());
        let p = result.unwrap();
        assert_eq!(p.error_type, "X");
        assert_eq!(p.message, "Y");
        assert_eq!(p.code, None);
        assert_eq!(p.param, None);
    }

    #[test]
    fn test_try_parse_model_error_legacy_format() {
        // Legacy format: {"error": "plain string"} — not parsable as model error
        let data = serde_json::json!({"error": "TypeError: something"});
        let result = try_parse_model_error(&data);
        assert!(result.is_none());
    }

    #[test]
    fn test_try_parse_model_error_missing_fields() {
        let data = serde_json::json!({"error": {"type": "X"}});
        assert!(try_parse_model_error(&data).is_none());

        let data = serde_json::json!({"error": {"message": "X"}});
        assert!(try_parse_model_error(&data).is_none());

        let data = serde_json::json!({"other": "stuff"});
        assert!(try_parse_model_error(&data).is_none());
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
        // Only inspect lines before the #[cfg(test)] boundary to avoid
        // counting the test's own mentions of pick_worker_random.
        let source = include_str!("mod.rs");
        let test_boundary = source.find("#[cfg(test)]").unwrap_or(source.len());
        let prod_source = &source[..test_boundary];

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
mod auth_policy_tests {
    use super::*;
    use crate::config::AuthPolicy;

    fn policy(keys: &[&str]) -> AuthPolicy {
        AuthPolicy {
            header: "x-api-key".to_string(),
            keys: keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    #[test]
    fn test_metadata_key_passes() {
        let mut md = MetadataMap::new();
        md.insert("x-api-key", "sk-a".parse().unwrap());
        assert!(enforce_auth_grpc(Some(&policy(&["sk-a"])), &md, &HashMap::new()).is_ok());
    }

    #[test]
    fn test_proto_headers_fallback_passes() {
        let headers = HashMap::from([("X-API-Key".to_string(), "sk-a".to_string())]);
        assert!(
            enforce_auth_grpc(Some(&policy(&["sk-a"])), &MetadataMap::new(), &headers).is_ok()
        );
    }

    #[test]
    fn test_missing_key_unauthenticated() {
        let err = enforce_auth_grpc(Some(&policy(&["sk-a"])), &MetadataMap::new(), &HashMap::new())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_wrong_key_unauthenticated() {
        let mut md = MetadataMap::new();
        md.insert("x-api-key", "nope".parse().unwrap());
        let err = enforce_auth_grpc(Some(&policy(&["sk-a"])), &md, &HashMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_empty_keys_accepts_any_nonempty() {
        let mut md = MetadataMap::new();
        md.insert("x-api-key", "anything".parse().unwrap());
        assert!(enforce_auth_grpc(Some(&policy(&[])), &md, &HashMap::new()).is_ok());
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

    // --- grpc_code_to_status_family ---

    #[test]
    fn should_map_success_to_2xx() {
        assert_eq!(grpc_code_to_status_family(tonic::Code::Ok), "2xx");
    }

    #[test]
    fn should_map_client_error_codes_to_4xx() {
        // 蓝图映射: InvalidArgument/NotFound/OutOfRange/Unauthenticated/
        // PermissionDenied/ResourceExhausted → "4xx"（ResourceExhausted
        // 专给限流，§4.0.9）。Cancelled 是客户端主动断开，同类。
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::OutOfRange,
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
            tonic::Code::ResourceExhausted,
            tonic::Code::Cancelled,
        ] {
            assert_eq!(grpc_code_to_status_family(code), "4xx", "{code:?}");
        }
    }

    #[test]
    fn should_map_server_fault_codes_to_5xx() {
        // 蓝图映射: Internal/Unavailable/DeadlineExceeded → "5xx"；
        // queue-full/过载返 Unavailable 天然落此族（§4.0.9）。
        for code in [
            tonic::Code::Internal,
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
        ] {
            assert_eq!(grpc_code_to_status_family(code), "5xx", "{code:?}");
        }
    }

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

    // ===== P3-1: gRPC 限流 =====

    use crate::config::RateLimitPolicy;

    fn rl(rpm: f64, key: &str, burst: Option<f64>) -> RateLimitPolicy {
        RateLimitPolicy {
            requests_per_minute: rpm,
            key: key.to_string(),
            burst,
        }
    }

    /// 无 policy 不限（直通 Ok）。
    #[test]
    fn rate_limit_no_policy_is_unlimited() {
        let limiter = crate::rate_limit::RateLimiter::default();
        assert!(enforce_grpc_rate_limit(&limiter, None, "m", "1.2.3.4").is_ok());
    }

    /// 超限返 ResourceExhausted + retry-after metadata（§4.0.9：ResourceExhausted
    /// 专给限流，落 4xx；queue-full/过载才是 Unavailable）。
    #[test]
    fn rate_limit_over_limit_returns_resource_exhausted_with_retry_after() {
        let limiter = crate::rate_limit::RateLimiter::default();
        let policy = rl(1.0, "ip", None); // 极低配额
        // 首个请求耗尽配额，第二个被拒。
        let _ = enforce_grpc_rate_limit(&limiter, Some(&policy), "rlm", "9.9.9.9");
        let err = enforce_grpc_rate_limit(&limiter, Some(&policy), "rlm", "9.9.9.9")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(
            err.metadata().get("retry-after").is_some(),
            "retry-after metadata must be present: {:?}",
            err.metadata()
        );
    }

    /// key="ip"：不同 IP 各自独立桶（互不影响）。
    #[test]
    fn rate_limit_key_ip_separates_buckets_by_ip() {
        let limiter = crate::rate_limit::RateLimiter::default();
        let policy = rl(1.0, "ip", None);
        let _ = enforce_grpc_rate_limit(&limiter, Some(&policy), "m", "10.0.0.1");
        // 不同 IP 不受 10.0.0.1 的桶耗尽影响。
        assert!(enforce_grpc_rate_limit(&limiter, Some(&policy), "m", "10.0.0.2").is_ok());
    }

    /// rpm<=0 fail-closed（RateLimiter 内置：rate=0 直接拒，retry_after 兜底）。
    #[test]
    fn rate_limit_zero_rpm_fails_closed() {
        let limiter = crate::rate_limit::RateLimiter::default();
        let policy = rl(0.0, "route", None);
        let err = enforce_grpc_rate_limit(&limiter, Some(&policy), "m", "1.2.3.4").unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }
}
