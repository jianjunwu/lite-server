//! Stream infer RPC (P8-1): server-streaming inference over the ZMQ stream
//! mechanism, with sequence-affinity worker pick and a two-stage
//! (overall/idle) deadline bound when the client specifies one.

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
    pub(crate) async fn stream_infer_impl(
        &self,
        request: Request<pb::StreamInferRequest>,
        version_label: &mut String,
        request_id_out: &mut String,
        start: Instant,
        span: tracing::Span,
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

        // FD-1: gateway-side JSON validation (HTTP ApiBody/B1 parity) —
        // malformed JSON under a JSON content-type is rejected before a
        // worker stream is opened; raw content-types pass through opaque.
        crate::grpc::payload::validate_json_payload(&req.headers, &req.data).map_err(err)?;

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
        // P-DEADLINE (方案 C): overall deadline only when the CLIENT specified
        // one; chunk-idle reclaim is ALWAYS on (decoupled parity) so a stuck
        // stream is recovered instead of hanging unbounded. Long streams that
        // keep producing chunks are unaffected. Captured before spawn.
        let stream_deadline = if deadline.client_specified {
            crate::deadline::to_instant(deadline.unix_ns)
        } else {
            None
        };
        let stream_idle = self.decoupled_idle_timeout;

        let stream_id = format!("grpc-stream-{}", Uuid::new_v4());
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
        // P6 GetModelStats: one streaming inference dispatched to this worker.
        crate::metrics::prometheus::record_worker_inference(
            model_name,
            &resolved_version,
            worker_id,
            1,
        );

        let open_req = streaming::build_stream_open(stream_id.clone(), req.data, Some(meta), false);

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;

        // Task D: fire InferenceRequest once the worker stream opened (streaming
        // bypasses the queue, so open-success is the trigger) and arm the
        // response callback for the forwarder below. `self` is not captured by
        // the spawn, so the runner is cloned here and moved in (unary parity).
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

        let (tx, rx) = mpsc::channel(64);
        let cancel_client = client.clone();

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.to_string();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc", &stream_id, false);
        }

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
                        break; // worker closed the stream
                    }
                    Err(elapsed) => {
                        // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                        tracing::warn!(
                            ?elapsed, stream_id = %stream_id,
                            "stream closed: deadline/idle elapsed"
                        );
                        stream_family = "5xx";
                        reason = match elapsed {
                            streaming::RecvElapsed::Deadline => {
                                crate::metrics::prometheus::StreamCloseReason::Deadline
                            }
                            streaming::RecvElapsed::Idle => {
                                crate::metrics::prometheus::StreamCloseReason::Idle
                            }
                        };
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
                        let grpc_chunk = pb::StreamChunk {
                            data: c.data.clone(),
                        };
                        if tx.send(Ok(grpc_chunk)).await.is_err() {
                            reason = crate::metrics::prometheus::StreamCloseReason::Cancel;
                            break;
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
                        let _ = tx.send(Err(grpc_err)).await;
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
                "grpc_stream",
                start,
                stream_family,
                reason,
                stream_metrics,
                output_bytes,
                chunks,
            );
            // Cleanup: send cancel to worker. `send_raw` (fire-and-forget) —
            // the worker signals the generator to stop and sends NO unary reply
            // to a Cancel, so `.send()` would await the full ZMQ_RESPONSE_TIMEOUT
            // (300s). Aligned with bidi/decoupled/HTTP stream (P-FLOW §4.0.9).
            let cancel_req = streaming::build_stream_cancel(stream_id);
            let _ = cancel_client.send_raw(cancel_req).await;
        }
        .instrument(span));

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
