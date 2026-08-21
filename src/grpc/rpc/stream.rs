//! Stream infer RPC (P8-1): server-streaming inference over the ZMQ stream
//! mechanism, with sequence-affinity worker pick and a two-stage
//! (overall/idle) deadline bound when the client specifies one.

use crate::grpc::auth::{enforce_auth_grpc, enforce_grpc_rate_limit};
use crate::grpc::canary::canary_pin;
use crate::grpc::error::{
    app_error_to_grpc_status, err, error_type_to_grpc_code, grpc_code_to_status_family,
    model_error_status, try_parse_model_error,
};
use crate::grpc::interceptor;
use crate::grpc::GrpcService;
use crate::registry::types::ModelType;
use std::sync::Arc;
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
        admission: crate::admission::AdmissionGuard,
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
        let mut stream_deadline = if deadline.client_specified {
            crate::deadline::to_instant(deadline.unix_ns)
        } else {
            None
        };
        let stream_idle = self.decoupled_idle_timeout;

        // §4.1 endpoint adaptation: ensemble models have no workers — dispatch
        // through the DAG executor; the returned Stream plugs the SAME
        // variables the forward loop below consumes (zero changes past this
        // point). Unary outcome here = the DAG has no streaming step (§4.4
        // unsupported-combination row → InvalidArgument).
        let is_ensemble = self
            .registry
            .get(model_name, Some(&resolved_version))
            .map(|mv| mv.model_type == ModelType::Ensemble)
            .unwrap_or(false);

        // P10 (D40): semaphore permit held for the forward task's lifetime.
        let mut ensemble_permit = None;
        // D18: chain handles + chain-tree abort held for the forward task's
        // cancellation (pipeline chains broadcast over every streaming step).
        let mut ensemble_chain: Option<Arc<std::sync::Mutex<Vec<crate::ensemble::StreamHandle>>>> = None;
        let mut ensemble_abort: Option<tokio::task::AbortHandle> = None;
        // §4.1 指标行: streaming-step latency is recorded at stream close.
        let mut ensemble_tail: Option<(String, String, String)> = None;
        let (stream_id, mut chunk_rx, cancel_client, inflight_guard) = if is_ensemble {
            let ensemble_input = crate::ensemble::ensemble_payload_from_bytes(
                &req.data,
                req.headers.get("content-type").cloned(),
            )
            .map_err(|e| crate::grpc::error::err(crate::grpc::error::app_error_to_grpc_status(&e)))?;
            let opts = crate::ensemble::EnsembleExecOpts {
                client_ip: client_ip.clone(),
                deadline_unix_ns: deadline.unix_ns,
                decoupled: false,
                // E8-1 (D38): the dag selector rides the gRPC metadata.
                dag_selector: crate::ensemble::dag_selector_from_grpc(&grpc_metadata)
                    .map_err(|e| crate::grpc::error::err(crate::grpc::error::app_error_to_grpc_status(&e)))?,
            };
            match crate::ensemble::execute_ensemble(
                self.app_state.clone(),
                model_name,
                &resolved_version,
                ensemble_input,
                &request_id,
                opts,
            )
            .await
            .map_err(|e| err(app_error_to_grpc_status(&e)))?
            {
                crate::ensemble::EnsembleOutcome::Stream(mut s) => {
                    // D35 (E5): fold the tail step's timeout cap into the recv
                    // overall bound (min(client overall, step cap)).
                    stream_deadline = crate::deadline::min_instant(stream_deadline, s.step_deadline);
                    ensemble_permit = s.permit.take();
                    ensemble_chain = Some(s.chain.clone());
                    ensemble_abort = Some(s.abort.clone());
                    ensemble_tail = Some((s.tail_step.clone(), s.tail_model.clone(), s.tail_version.clone()));
                    // Ensemble streams are counted per DAG node at the worker
                    // open inside the executor; the guard rides EnsembleStream.
                    (s.stream_id, s.chunk_rx, s.cancel_client, s.inflight_guard)
                }
                crate::ensemble::EnsembleOutcome::Unary(_) => {
                    return Err(err(Status::invalid_argument(
                        "ensemble DAG has no streaming step; use a unary endpoint",
                    )));
                }
            }
        } else {
            let stream_id = format!("grpc-stream-{}", Uuid::new_v4());
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
            .map_err(|e| match e {
                crate::worker::PickError::InvalidPin(msg) => {
                    err(Status::invalid_argument(msg))
                }
                crate::worker::PickError::NoLiveWorkers(msg) => {
                    err(crate::grpc::with_retry_after(Status::unavailable(msg), 1))
                }
                crate::worker::PickError::WorkerRecycling(msg) => {
                    err(crate::grpc::with_retry_after(Status::unavailable(msg), 1))
                }
            })?;
            // G1/G3: count the in-flight stream on its slot; the guard moves
            // into the forward task below (see the guard's doc).
            let inflight_guard = outlier
                .map(|o| crate::streaming::StreamInflightGuard::new(o, worker_id));
            let client = clients[worker_id].clone();
            // P6 GetModelStats: one streaming inference dispatched to this worker.
            crate::metrics::prometheus::record_worker_inference(
                model_name,
                &resolved_version,
                worker_id,
                1,
            );

            let open_req = streaming::build_stream_open(
                stream_id.clone(),
                req.data,
                Some(meta),
                false,
            );

            let chunk_rx = client
                .send_stream(open_req, stream_id.clone())
                .await
                .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;
            // G3: count the stream toward the slot's max_requests budget.
            self.app_state.inference_queue.record_stream_served(model_name, &resolved_version, worker_id);
            (stream_id, chunk_rx, client.clone(), inflight_guard)
        };
        // G3:补记 stream_id,转发 spawn 在 span 内执行(单 span 覆盖全流)。
        span.record("stream_id", stream_id.as_str());

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

        // RN-14: per-stream channel depth is operator-tunable (default 64).
        let (tx, rx) =
            mpsc::channel(self.app_state.config.server.stream_channel_size.max(1));

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.to_string();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc", &stream_id, false);
        }

        // Panic收口 (detached spawn 无人 join):catch_forward_panic 保证 panic
        // 时仍记 Panic 终态指标 + 取消 worker 流(对齐 WS 适配器的 join 臂)。
        let panic_model = metrics_model.clone();
        let panic_version = metrics_version.clone();
        let panic_chain = ensemble_chain.clone();
        let panic_abort = ensemble_abort.clone();
        let panic_stream_id = stream_id.clone();
        let panic_cancel_client = cancel_client.clone();
        let panic_start = start;
        tokio::spawn(async move {
            crate::streaming::catch_forward_panic("grpc", async move {
            // G1/G3: per-slot in-flight stream count, held for the stream's
            // lifetime; dropped when this forward task ends (any exit).
            let _inflight_guard = inflight_guard;
            // P10 (D40): held for the forward task's lifetime — released on
            // drop (terminal frame / idle / disconnect; D18 teardown path).
            let _ensemble_permit = ensemble_permit;
            // RN-13 (O2): the admission slot is held for the stream's
            // lifetime (the wrapper acquired it before open; early open
            // failures dropped it back).
            let _admission_guard = admission;
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
                        // G5: the worker died mid-stream — terminal: the tonic
                        // stream ends with an Err item (Unavailable), so a
                        // killed worker is distinguishable from a clean EOF.
                        reason = crate::metrics::prometheus::StreamCloseReason::WorkerEof;
                        stream_family = "5xx";
                        let _ = tokio::time::timeout(
                            streaming::TERMINAL_SEND_TIMEOUT,
                            tx.send(Err(Status::unavailable("worker exited mid-stream"))),
                        )
                        .await;
                        crate::callback::fire_inference_response(&cb_runner, &req_ctx, start);
                        break;
                    }
                    Err(elapsed) => {
                        // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                        tracing::warn!(
                            ?elapsed, stream_id = %stream_id,
                            "stream closed: deadline/idle elapsed"
                        );
                        stream_family = "5xx";
                        // D35 (§4.4): a mid-stream reclaim — deadline OR idle
                        // — is terminal: the tonic stream must end with an
                        // Err item (the encode layer stops at the first Err;
                        // nothing may follow), so truncated output is
                        // distinguishable from the worker's normal EOF.
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
                            tx.send(Err(status)),
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
                        let grpc_chunk = pb::StreamChunk {
                            data: c.data.clone(),
                        };
                        // L1 (resource-leak-plan): deadline + idle-bounded
                        // send — a stopped reader must not pin the stream
                        // past the armed deadline nor defeat the recv-side
                        // idle reclaim (P0-2; see streaming::send_bounded).
                        match streaming::send_bounded(stream_deadline, stream_idle, tx.send(Ok(grpc_chunk))).await {
                            streaming::SendOutcome::Sent(Ok(())) => {}
                            streaming::SendOutcome::Sent(Err(_)) => {
                                reason = crate::metrics::prometheus::StreamCloseReason::Cancel;
                                break;
                            }
                            streaming::SendOutcome::Deadline => {
                                reason = crate::metrics::prometheus::StreamCloseReason::Deadline;
                                break;
                            }
                            streaming::SendOutcome::Idle => {
                                reason = crate::metrics::prometheus::StreamCloseReason::Idle;
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
            // §4.1 指标行: the streaming step's latency, measured at stream close.
            if let Some((tail_step, tail_model, tail_version)) = ensemble_tail {
                crate::metrics::prometheus::record_ensemble_step_latency(
                    &metrics_model,
                    &tail_step,
                    &tail_model,
                    &tail_version,
                    0, // tail step of a top-level ensemble = depth 0 (nested ensembles cannot stream, D4)
                    start.elapsed().as_secs_f64(),
                );
            }
            // Cleanup: send cancel to worker. `send_raw` (fire-and-forget) —
            // the worker signals the generator to stop and sends NO unary reply
            // to a Cancel, so `.send()` would await the full ZMQ_RESPONSE_TIMEOUT
            // (300s). Aligned with bidi/decoupled/HTTP stream (P-FLOW §4.0.9).
            if is_ensemble {
                crate::ensemble::cancel_chain(
                    ensemble_chain.as_ref(),
                    ensemble_abort.as_ref(),
                    &stream_id,
                    &cancel_client,
                )
                .await;
            } else {
                let cancel_req = streaming::build_stream_cancel(stream_id);
                let _ = cancel_client.send_raw(cancel_req).await;
            }
            }, move || async move {
                // Panic臂 (WS 同款):补记 Panic 终态 + 取消 worker 流。
                crate::metrics::prometheus::record_stream_terminal(
                    &panic_model,
                    &panic_version,
                    "grpc",
                    "grpc_stream",
                    panic_start,
                    crate::metrics::prometheus::StreamCloseReason::Panic.status_family(),
                    crate::metrics::prometheus::StreamCloseReason::Panic,
                    stream_metrics,
                    0,
                    0,
                );
                if is_ensemble {
                    crate::ensemble::cancel_chain(
                        panic_chain.as_ref(),
                        panic_abort.as_ref(),
                        &panic_stream_id,
                        &panic_cancel_client,
                    )
                    .await;
                } else {
                    let cancel_req = streaming::build_stream_cancel(panic_stream_id);
                    let _ = panic_cancel_client.send_raw(cancel_req).await;
                }
            }).await;
        }
        .instrument(span));

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
