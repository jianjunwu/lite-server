//! Decoupled infer RPC (P9-1, 蓝图 §4.4 D18): a stream whose channel the
//! MODEL controls — predict_decoupled returns before close(), the worker
//! pushes N async chunks, ending with sender.close(). Thin translation layer
//! (D3): resolve → auth → rate-limit → open a decoupled stream → forward
//! chunks; only the idle-timeout wrapper and the DecoupledResponse mapping
//! differ from stream_infer.

use crate::grpc::auth::{enforce_auth_grpc, enforce_grpc_rate_limit};
use crate::grpc::canary::canary_pin;
use crate::grpc::error::{
    err, error_type_to_grpc_code, grpc_code_to_status_family, model_error_status,
    try_parse_model_error,
};
use crate::grpc::interceptor;
use crate::grpc::GrpcService;
use crate::proto::liteserver as pb;
use crate::request_context::RequestContext;
use crate::streaming;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::Instrument;
use uuid::Uuid;

impl GrpcService {
    pub(crate) async fn decoupled_infer_impl(
        &self,
        request: Request<pb::DecoupledInferRequest>,
        version_label: &mut String,
        request_id_out: &mut String,
        start: Instant,
        span: tracing::Span,
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
            // §4.4: an ensemble has no workers — DecoupledInfer has no
            // ensemble branch (the §4.5 target matrix does not list it), so
            // fail with the friendly InvalidArgument instead of falling
            // through to the misleading "no workers available".
            if mv.model_type == crate::registry::types::ModelType::Ensemble {
                return Err(err(Status::invalid_argument(format!(
                    "model {model_name} is an ensemble — DecoupledInfer is not \
                     supported for ensembles; use StreamInfer or BidiStream"
                ))));
            }
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
            enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), model_name, &client_ip)?;
        }

        // FD-1: gateway-side JSON validation (HTTP ApiBody/B1 parity) —
        // malformed JSON under a JSON content-type is rejected before a
        // worker stream is opened; raw content-types pass through opaque.
        crate::grpc::payload::validate_json_payload(&req.headers, &req.data).map_err(err)?;

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
        let stream_id = format!("grpc-decoupled-{}", Uuid::new_v4());
        // G3:补记 stream_id,转发 spawn 在 span 内执行(单 span 覆盖全流)。
        span.record("stream_id", stream_id.as_str());

        let clients = self
            .worker_manager
            .get_zmq_clients(model_name, &resolved_version)
            .await
            .ok_or_else(|| err(Status::unavailable("no workers available")))?;
        if clients.is_empty() {
            return Err(err(Status::unavailable("no workers available")));
        }

        // Task F: shared streaming worker pick (B3 hints: x-lite-worker-id pin >
        // sequence_id stickiness > x-lite-affinity-key rendezvous >
        // skip-ejected/random). A bad pin → InvalidArgument.
        let outlier = self
            .worker_manager
            .get_outlier_state(model_name.as_str(), &resolved_version)
            .await;
        let seq_registry = self.app_state.inference_queue.sequence_registry();
        let worker_id = crate::worker::pick_streaming_worker(
            &meta,
            clients.len(),
            outlier.as_deref(),
            seq_registry,
            model_name,
            &resolved_version,
        )
        .map_err(|e| err(Status::invalid_argument(e.0)))?;
        let client = clients[worker_id].clone();
        crate::metrics::prometheus::record_worker_inference(
            model_name,
            &resolved_version,
            worker_id,
            1,
        );

        // decoupled=true → the worker keeps the channel open after
        // predict_decoupled returns (model-controlled lifetime).
        let open_req = streaming::build_stream_open(stream_id.clone(), req.data, Some(meta), true);

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;

        // Task D: fire InferenceRequest once the worker stream opened (streaming
        // bypasses the queue) and arm the response callback. `self` is not
        // captured by the spawn, so clone the runner here (unary parity).
        let cb_runner = self.callback_runner.clone();
        let req_ctx = crate::callback::InferenceContext {
            model_name: model_name.to_string(),
            version: resolved_version.clone(),
            route: "/predict".to_string(),
            protocol: crate::callback::Protocol::Grpc,
            request_id: request_id.clone(),
            client_ip: client_ip.clone(),
            elapsed_us: None,
        };
        crate::callback::fire_inference_request(&cb_runner, &req_ctx);

        // RN-14: per-stream channel depth is operator-tunable (default 64).
        let (tx, rx) =
            mpsc::channel(self.app_state.config.server.stream_channel_size.max(1));
        let cancel_client = client.clone();

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.to_string();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc", &stream_id, true);
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
            // S1/S2:收口枚举——各 break 点只置 reason,尾部 record_stream_terminal
            // 统一消费(family/cancelled 单一来源;Error 帧 family 按 grpc code 覆盖)。
            let reason;
            // S6:per-stream 输出字节(Σ chunk.data.len(),收口统一上报)。
            let mut output_bytes: u64 = 0;
            // G5:per-stream chunk 数(close 日志字段,收口统一上报,非 metric)。
            let mut chunks: u64 = 0;

            loop {
                let chunk = match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                    .await
                {
                    Ok(Some(c)) => c,
                    Ok(None) => {
                        reason = crate::metrics::prometheus::StreamCloseReason::WorkerEof;
                        break; // actor dropped the route (Done/Error forwarded)
                    }
                    Err(elapsed) => {
                        tracing::warn!(
                            ?elapsed, stream_id = %stream_id,
                            "decoupled stream closed: deadline/idle elapsed"
                        );
                        stream_family = "5xx";
                        // F-11/D35 (§4.4): a mid-stream reclaim — deadline OR
                        // idle — is terminal for the client: end with an
                        // Err(Status) item so truncated output is
                        // distinguishable from the worker's normal EOF
                        // (gRPC stream.rs already does this for deadline).
                        let status = match elapsed {
                            streaming::RecvElapsed::Deadline => {
                                reason = crate::metrics::prometheus::StreamCloseReason::Deadline;
                                Status::deadline_exceeded("stream closed: deadline exceeded")
                            }
                            streaming::RecvElapsed::Idle => {
                                reason = crate::metrics::prometheus::StreamCloseReason::Idle;
                                Status::deadline_exceeded("stream closed: idle timeout")
                            }
                        };
                        // L1: bounded terminal send — see TERMINAL_SEND_TIMEOUT.
                        let _ = tokio::time::timeout(
                            streaming::TERMINAL_SEND_TIMEOUT,
                            tx.send(Err(err(status))),
                        )
                        .await;
                        crate::callback::fire_inference_response(&cb_runner, &req_ctx, start);
                        break;
                    }
                };
                match chunk.payload {
                    Some(pb::stream_response::Payload::Chunk(ref c)) => {
                        output_bytes += c.data.len() as u64;
                        chunks += 1;
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
                        // L1 (resource-leak-plan): deadline-bounded send —
                        // a stopped reader must not pin the stream past the
                        // armed deadline (see streaming::send_bounded).
                        match streaming::send_bounded(stream_deadline, tx.send(Ok(resp))).await {
                            streaming::SendOutcome::Sent(Ok(())) => {}
                            streaming::SendOutcome::Sent(Err(_)) => {
                                reason = crate::metrics::prometheus::StreamCloseReason::Cancel;
                                break; // client disconnect
                            }
                            streaming::SendOutcome::Deadline => {
                                reason = crate::metrics::prometheus::StreamCloseReason::Deadline;
                                break;
                            }
                        }
                    }
                    Some(pb::stream_response::Payload::Error(ref e)) => {
                        reason = crate::metrics::prometheus::StreamCloseReason::Error;
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
                        // L1: bounded terminal send — see TERMINAL_SEND_TIMEOUT.
                        let _ = tokio::time::timeout(
                            streaming::TERMINAL_SEND_TIMEOUT,
                            tx.send(Err(grpc_err)),
                        )
                        .await;
                        crate::callback::fire_inference_response(&cb_runner, &req_ctx, start);
                        break;
                    }
                    Some(pb::stream_response::Payload::Done(done)) => {
                        reason = crate::metrics::prometheus::StreamCloseReason::Done;
                        // Task A: record worker-reported metrics (HTTP parity).
                        crate::metrics::prometheus::record_worker_metrics(
                            &metrics_model,
                            &metrics_version,
                            done.metrics.as_ref(),
                        );
                        // Model called sender.close(): emit the terminal is_final
                        // frame, then end the gRPC stream.
                        // L1: bounded terminal send — see TERMINAL_SEND_TIMEOUT.
                        let _ = tokio::time::timeout(
                            streaming::TERMINAL_SEND_TIMEOUT,
                            tx.send(Ok(pb::DecoupledResponse { data: Default::default(), is_final: true })),
                        )
                        .await;
                        // Task D: terminal frame → InferenceResponse.
                        crate::callback::fire_inference_response(&cb_runner, &req_ctx, start);
                        break;
                    }
                    _ => {}
                }
            }
            // S1/S2/S4/S6 收口:无条件 record_request_end + 门控内 cancelled/errors/duration/bytes/close。
            crate::metrics::prometheus::record_stream_terminal(
                &metrics_model,
                &metrics_version,
                "grpc",
                "grpc_decoupled",
                start,
                stream_family,
                reason,
                stream_metrics,
                output_bytes,
                chunks,
            );
            // Cleanup: cancel the worker. send_raw = fire-and-forget: a
            // stream-cancel gets no unary reply, so this avoids the phantom
            // 300s await of send() (对齐 HTTP stream.rs cancel path).
            let cancel_req = streaming::build_stream_cancel(stream_id);
            let _ = cancel_client.send_raw(cancel_req).await;
        }
        .instrument(span));

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::config::ModelConfig;
    use crate::grpc::GrpcServiceDeps;
    use crate::http::state::AppState;
    use crate::inference_queue::InferenceQueue;
    use crate::rate_limit::RateLimiter;
    use crate::registry::types::{ModelType, WorkerInfo, WorkerStatus};
    use crate::registry::ModelRegistry;
    use crate::transport::zmq::WorkerZmqClient;
    use crate::worker::WorkerManager;
    use bytes::Bytes;
    use futures::StreamExt;
    use prost::Message;
    use std::sync::Arc;
    use std::time::Duration;

    fn test_endpoint(tag: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("decoupled-f11-{}-{}.sock", tag, std::process::id()))
                    .display()
            )
        }
        #[cfg(not(unix))]
        {
            format!("tcp://127.0.0.1:{}", 36000 + std::process::id() % 1000)
        }
    }

    /// PAIR worker: Open → ONE chunk, then stall (no Done / close) — the
    /// forwarder then sits in recv_chunk until the client deadline fires.
    fn spawn_stall_chunk_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(8000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let Some(pb::request::Payload::Stream(st)) = req.payload else {
                    let _ = s.send(
                        pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(),
                        0,
                    );
                    continue;
                };
                if !matches!(st.action, Some(pb::stream_request::Action::Open(_))) {
                    continue;
                }
                let chunk = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(pb::stream_response::Payload::Chunk(
                            pb::StreamChunkResponse {
                                data: Bytes::from_static(b"{}"),
                                is_final: false,
                            },
                        )),
                    })),
                    ..Default::default()
                };
                let _ = s.send(chunk.encode_to_vec(), 0);
                // STALL: no Done / Error — only the deadline can end the stream.
            }
        })
    }

    /// GrpcService with one ready model backed by the stall worker.
    async fn make_service(
        model: &str,
        endpoint: String,
    ) -> (GrpcService, std::thread::JoinHandle<()>) {
        let cb = Arc::new(CallbackRunner::new());
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            queue.clone(),
            "warn".to_string(),
            cb.clone(),
        ));
        registry
            .register(
                model,
                "1",
                ModelConfig::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry.mark_ready(model, "1").unwrap();
        registry
            .set_workers(
                model,
                "1",
                vec![WorkerInfo {
                    worker_id: 0,
                    device: "cpu:0".to_string(),
                    endpoint: String::new(),
                    pid: None,
                    status: WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();
        registry.activate_version(model, "1").unwrap();

        let worker = spawn_stall_chunk_worker(endpoint.clone());
        let client = Arc::new(WorkerZmqClient::new(endpoint));
        wm.insert_zmq_clients_for_test(model, "1", vec![client]).await;

        let app_state = Arc::new(AppState::new(
            registry.clone(),
            wm.clone(),
            queue,
            crate::config::Config::default(),
            std::path::PathBuf::new(),
            cb,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(RateLimiter::default()),
        ));
        let svc = GrpcService::new(GrpcServiceDeps {
            registry,
            worker_manager: wm,
            streaming_metrics: false,
            canary_override: false,
            grpc_streaming: true,
            callback_runner: app_state.callback_runner.clone(),
            shutdown_state: app_state.shutdown_state.clone(),
            server_timeout: Duration::ZERO, // no server fallback deadline
            rate_limiter: app_state.rate_limiter.clone(),
            decoupled_idle_timeout: None, // only the client deadline may fire
            app_state,
            trusted: Arc::new(Vec::new()),
        });
        (svc, worker)
    }

    // ===== F-11 (functional-defects-plan, D35) =============================
    // When the client deadline fires mid-stream, the decoupled stream must
    // end with an Err(Status) item — the gRPC stream forwarder already does
    // this (src/grpc/rpc/stream.rs:316-321 sends Err(deadline_exceeded)),
    // the decoupled forwarder breaks silently instead (the Err(elapsed) arm
    // at decoupled.rs:240-255). FAILS on current code: the client observes a
    // clean end-of-stream indistinguishable from the worker's normal EOF.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_f11_decoupled_deadline_must_send_err_frame() {
        let model = "dec_f11";
        let endpoint = test_endpoint(model);
        let (svc, _worker) = make_service(model, endpoint).await;
        // Let the client actor bind and the worker connect (ZMQ handshake).
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut request = Request::new(pb::DecoupledInferRequest {
            model_name: model.to_string(),
            version: String::new(),
            data: Bytes::from_static(b"{}"),
            headers: HashMap::new(),
            sequence_id: None,
        });
        // Client deadline: 500 ms. The worker stalls after one chunk, so the
        // forwarder's recv_chunk must trip the overall deadline.
        request
            .metadata_mut()
            .insert("grpc-timeout", "500m".parse().unwrap());

        let mut version_label = String::new();
        let mut request_id_out = String::new();
        let resp = svc
            .decoupled_infer_impl(
                request,
                &mut version_label,
                &mut request_id_out,
                std::time::Instant::now(),
                tracing::Span::current(),
            )
            .await
            .expect("decoupled stream must open");

        let mut stream = resp.into_inner();
        let mut items: Vec<Result<pb::DecoupledResponse, Status>> = Vec::new();
        while let Ok(Some(item)) = tokio::time::timeout(Duration::from_secs(4), stream.next()).await
        {
            items.push(item);
        }
        assert!(
            items.iter().any(|i| i.is_err()),
            "F-11 (D35): a mid-stream client-deadline expiry must end the \
             decoupled stream with an Err(Status) item; the current \
             Err(elapsed) arm only breaks, so the client sees a clean \
             end-of-stream indistinguishable from the worker's normal EOF \
             (gRPC stream.rs:316-321 already sends Err(deadline_exceeded)). \
             items: {items:?}"
        );
    }
}
