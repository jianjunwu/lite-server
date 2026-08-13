//! Bidi stream RPC (P0-2): bidirectional streaming — client chunks forward
//! to the worker as fire-and-forget StreamRequest::Chunk, worker chunks
//! forward back through the stream's registered channel. The inference span
//! covers the whole handler (model/version known only after BidiOpen).

use crate::grpc::auth::{enforce_auth_grpc, enforce_grpc_rate_limit};
use crate::grpc::canary::{canary_pin, resolve_bidi_version};
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
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;
use uuid::Uuid;

impl GrpcService {
    pub(crate) async fn bidi_stream_impl(
        &self,
        request: Request<Streaming<pb::BidiChunk>>,
        model_label: &mut String,
        version_label: &mut String,
        request_id_out: &mut String,
        start: Instant,
    ) -> Result<Response<ReceiverStream<Result<pb::BidiChunk, Status>>>, Status> {
        let remote_addr = request.remote_addr();
        let (grpc_metadata, extensions, mut stream) = request.into_parts();
        // request_id / client_ip come from metadata + the transport peer only:
        // BidiOpen.headers (task C) is decoded later and carries canary / auth
        // / B3 hints — not client identity (structural: headers are unavailable
        // at finalize_context time).
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

        // Wait for first message (must be BidiOpen). Bound the wait by the
        // resolved deadline (server.timeout and/or client grpc-timeout); when
        // no deadline resolves, FD-5 falls back to the always-on decoupled
        // idle budget so a connected-but-silent client cannot pin the handler
        // forever. Only when the operator disabled BOTH budgets does the wait
        // stay unbounded (documented P-DEADLINE semantics).
        let first = match crate::deadline::to_instant(deadline.unix_ns) {
            Some(instant) => match tokio::time::timeout_at(instant.into(), stream.message()).await {
                Ok(res) => {
                    res.map_err(|e| err(Status::internal(format!("stream error: {}", e))))?
                }
                Err(_) => {
                    return Err(err(Status::deadline_exceeded(
                        "bidi stream did not send the opening message before the deadline",
                    )));
                }
            },
            None => match self.decoupled_idle_timeout {
                Some(idle) => match tokio::time::timeout(idle, stream.message()).await {
                    Ok(res) => {
                        res.map_err(|e| err(Status::internal(format!("stream error: {}", e))))?
                    }
                    Err(_) => {
                        return Err(err(Status::deadline_exceeded(
                            "bidi stream did not send the opening message within the idle budget",
                        )));
                    }
                },
                None => stream
                    .message()
                    .await
                    .map_err(|e| err(Status::internal(format!("stream error: {}", e))))?,
            },
        };

        let (model_name, resolved_version, stream_id, initial_data, sequence_id, pin, headers) = match first {
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

                    // P5-2: canary pin — metadata first, then BidiOpen.headers
                    // (task C parity with unary's proto-headers fallback).
                    let pin = canary_pin(
                        &self.registry,
                        self.canary_override,
                        &model_name,
                        &grpc_metadata,
                        &open.headers,
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

                    // Auth: metadata first, then BidiOpen.headers (task C).
                    if let Some(mv) = self.registry.get(&model_name, Some(&resolved_version)) {
                        enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &open.headers)?;
                        // bidi key="ip" 共享 bucket（注释注明：所有 bidi 请求归一）。
                        enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), &model_name, &client_ip)?;
                    }

                    let sid = format!("grpc-bidi-{}", Uuid::new_v4());
                    (model_name, resolved_version, sid, open.initial_data, open.sequence_id, pin, open.headers)
                }
                _ => return Err(err(Status::invalid_argument("first message must be BidiOpen"))),
            },
            None => return Err(err(Status::invalid_argument("empty stream"))),
        };

        // FD-1: gateway-side JSON validation of BidiOpen.initial_data (HTTP
        // h2 bidi B1 parity) — before any worker stream is opened. Dispatch
        // reads BidiOpen.headers, the same map the worker sees.
        crate::grpc::payload::validate_json_payload(&headers, &initial_data).map_err(err)?;

        *model_label = model_name.clone();
        *version_label = resolved_version.clone();

        // P2-3 span：覆盖 bidi handler 全程（model/version 在 Open 解码后已知）。
        // P5-2：pin 命中记 pinned_version（bidi span 在解析后才创建，无法走
        // canary_pin 内的 Span::current().record，在此补记）。
        // FD-2: D11 body fields — initial_data/headers decoded above (HTTP
        // handler parity); bidi has no wrapper-level span to carry them.
        let span = tracing::info_span!(
            "inference",
            stream_id = tracing::field::Empty,
            model = %model_name,
            version = %resolved_version,
            request_id = %request_id,
            pinned_version = tracing::field::Empty,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
            body_bytes = initial_data.len() as i64,
            body_kind = crate::grpc::payload::body_kind_label(&headers),
        );
        // P-TRACE: link the bidi inference span to the inbound trace (D21 single
        // extract — reuses the interceptor's RequestContext.trace_cx).
        crate::telemetry::link_parent(&span, &cx.trace_cx);
        if let Some(p) = &pin {
            span.record("pinned_version", p.as_str());
        }
        // async move 实现块会带走 span(record 在块内使用)——结尾的
        // .instrument 用预克隆,避免 use-after-move。
        let tail_span = span.clone();
        async move {
        // P-TRACE: seed the worker meta with BidiOpen.headers (task C: canary /
        // auth / B3 hints forwarded to the worker) + inject the bidi inference
        // span's trace context (worker child).
        let mut bidi_headers = headers;
        crate::telemetry::inject(&mut bidi_headers);
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: bidi_headers,
            client_ip: client_ip.clone(),
            request_id: request_id.clone(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: initial_data.clone(),
            sequence_id: sequence_id.clone(),
            deadline_unix_ns: deadline.unix_ns,
            ..Default::default()
        };

        // §4.3 endpoint adaptation: ensemble models have no workers — the
        // upstream is AGGREGATED into one root input (D17), the trigger is
        // half-close (BidiClose or transport EOF, D33), and the DAG's tail
        // stream replaces the worker stream below (same forward variables).
        let is_ensemble = self
            .registry
            .get(&model_name, Some(&resolved_version))
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
        // Non-ensemble only: the client→worker chunk forwarder's client
        // (ensemble aggregates upstream instead, D17).
        let mut worker_client: Option<Arc<crate::transport::zmq::WorkerZmqClient>> = None;
        // D35 (E5): the tail step's timeout cap, captured here (the ensemble
        // dispatch precedes the stream_deadline computation) and folded into
        // the recv overall bound below.
        let mut ensemble_step_deadline: Option<std::time::Instant> = None;
        let (stream_id, mut chunk_rx, cancel_client) = if is_ensemble {
            let max_body = self
                .app_state
                .config
                .server
                .max_request_body_bytes
                .unwrap_or(64 * 1024 * 1024);
            let mut aggregator = crate::ensemble::BidiAggregator::new(max_body);
            // First frame kind: content-type declares JSON vs opaque bytes
            // (WS frame-type parity). Mixed kinds → 400 inside push.
            let is_json = crate::grpc::payload::body_kind_label(&meta.headers) == "json";
            let ct = meta.headers.get("content-type").cloned();
            let aggregate_start = std::time::Instant::now();
            aggregator
                .push(initial_data, is_json, ct.as_deref())
                .map_err(|e| err(app_error_to_grpc_status(&e)))?;
            // §4.3 (D17): the aggregation loop reuses the two-stage bound —
            // chunk-idle ALWAYS on (reclaims an abandoned aggregating client),
            // overall deadline since the FIRST frame (aggregation eats the DAG
            // budget — the client is effectively streaming its body).
            let agg_deadline = if deadline.client_specified {
                crate::deadline::to_instant(deadline.unix_ns)
            } else {
                None
            };
            loop {
                // Per-recv bound = min(overall remaining, idle) — recv_chunk's
                // two-stage semantics; a client-specified deadline must NOT
                // disable the always-on idle reclaim (D17).
                let now = std::time::Instant::now();
                let next = match (agg_deadline, self.decoupled_idle_timeout) {
                    (Some(d), idle) => {
                        let remain = d.saturating_duration_since(now);
                        let bound = idle.map(|i| remain.min(i)).unwrap_or(remain);
                        tokio::time::timeout(bound, stream.message()).await
                    }
                    (None, Some(idle)) => tokio::time::timeout(idle, stream.message()).await,
                    (None, None) => {
                        Ok::<_, tokio::time::error::Elapsed>(stream.message().await)
                    }
                };
                match next {
                    Ok(Ok(Some(chunk))) => match chunk.payload {
                        Some(pb::bidi_chunk::Payload::Data(d)) => {
                            aggregator.push(d.data, is_json, ct.as_deref()).map_err(|e| err(app_error_to_grpc_status(&e)))?;
                        }
                        Some(pb::bidi_chunk::Payload::Close(_)) => break,
                        _ => {}
                    },
                    // §4.4 aggregation-disconnect row: transport failed mid-
                    // aggregation — abandon the execution (no response object).
                    Ok(Err(_)) => {
                        return Err(err(Status::cancelled(
                            "bidi stream aborted during aggregation",
                        )));
                    }
                    // Half-close (transport input end) → trigger (D33).
                    Ok(Ok(None)) => break,
                    Err(_) => {
                        return Err(err(Status::deadline_exceeded(
                            "bidi aggregation timed out waiting for close",
                        )));
                    }
                }
            }
            crate::metrics::prometheus::record_ensemble_bidi_aggregate(
                aggregator.total_bytes(),
                aggregate_start.elapsed().as_secs_f64(),
            );
            let value = aggregator.finish().map_err(|e| err(app_error_to_grpc_status(&e)))?;
            let opts = crate::ensemble::EnsembleExecOpts {
                client_ip: client_ip.clone(),
                deadline_unix_ns: deadline.unix_ns,
                decoupled: false,
            };
            match crate::ensemble::execute_ensemble(
                self.app_state.clone(),
                &model_name,
                &resolved_version,
                value,
                &request_id,
                opts,
            )
            .await
            .map_err(|e| err(app_error_to_grpc_status(&e)))?
            {
                crate::ensemble::EnsembleOutcome::Stream(mut s) => {
                    ensemble_step_deadline = s.step_deadline;
                    ensemble_permit = s.permit.take();
                    ensemble_chain = Some(s.chain.clone());
                    ensemble_abort = Some(s.abort.clone());
                    ensemble_tail = Some((s.tail_step.clone(), s.tail_model.clone(), s.tail_version.clone()));
                    (s.stream_id, s.chunk_rx, s.cancel_client)
                }
                crate::ensemble::EnsembleOutcome::Unary(_) => {
                    return Err(err(Status::invalid_argument(
                        "ensemble DAG has no streaming step; use a unary endpoint",
                    )));
                }
            }
        } else {
            let clients = self
                .worker_manager
                .get_zmq_clients(&model_name, &resolved_version)
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
            &model_name,
            &resolved_version,
        )
        .map_err(|e| err(Status::invalid_argument(e.0)))?;
        let client = clients[worker_id].clone();
        // P6 GetModelStats: one bidi inference dispatched to this worker.
        crate::metrics::prometheus::record_worker_inference(
            &model_name,
            &resolved_version,
            worker_id,
            1,
        );
        worker_client = Some(client.clone());

        let open_req = streaming::build_stream_open(stream_id.clone(), initial_data, Some(meta), false);

        let chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;
        (stream_id, chunk_rx, client.clone())
        };

        // Task D: fire InferenceRequest once the worker stream opened and arm
        // the response callback. The inner forwarder spawn below does not
        // capture `self`, so the runner is cloned here and moved in (unary
        // parity). resp_ctx reuses metrics_model/metrics_version below.
        let cb_runner = self.callback_runner.clone();
        let req_ctx = crate::callback::InferenceContext {
            model_name: model_name.clone(),
            version: resolved_version.clone(),
            route: "/predict".to_string(),
            protocol: crate::callback::Protocol::Grpc,
            request_id: request_id.clone(),
            client_ip: client_ip.clone(),
            elapsed_us: None,
        };
        crate::callback::fire_inference_request(&cb_runner, &req_ctx);

        let (tx, rx) = mpsc::channel(64);

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.clone();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc", &stream_id, false);
        }

        // G3:补记 stream_id,转发 spawn 在 span 内执行(单 span 覆盖全流)。
        span.record("stream_id", stream_id.as_str());
        // Spawn forwarder: worker chunks -> gRPC stream
        let stream_id_for_incoming = stream_id.clone();
        // P-DEADLINE (方案 C): overall deadline client-specified only; chunk-idle
        // reclaim always on (decoupled parity). Captured before spawn.
        let stream_deadline = crate::deadline::min_instant(
            if deadline.client_specified {
                crate::deadline::to_instant(deadline.unix_ns)
            } else {
                None
            },
            // D35 (E5): the ensemble tail step's timeout cap.
            ensemble_step_deadline,
        );
        let stream_idle = self.decoupled_idle_timeout;
        tokio::spawn(async move {
            // P10 (D40): held for the forward task's lifetime — released on
            // drop (terminal frame / idle / disconnect; D18 teardown path).
            let _ensemble_permit = ensemble_permit;
            // Non-ensemble: forward incoming bidi chunks to the worker as
            // StreamRequest::Chunk (fire-and-forget — each chunk's response
            // comes back through the stream channel registered at open, so
            // send() would stall for ZMQ_RESPONSE_TIMEOUT).
            // Ensemble (§4.3): multi-round guard — frames after the
            // aggregation trigger are a protocol violation (half-close
            // already ended input); log + ignore.
            let incoming_task = if let Some(worker_client) = &worker_client {
                let worker_client = worker_client.clone();
                tokio::spawn(async move {
                let mut close_sent = false;
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
                            close_sent = true;
                            break;
                        }
                        _ => {}
                    }
                }
                // D4: client half-close (transport-level input end without an
                // explicit BidiClose) → gracefully end worker input. Also covers
                // the stream-error path (Some(Err(_))). Fire-and-forget — harmless
                // when the worker already stopped.
                if !close_sent {
                    let _ = worker_client.send_raw(streaming::build_stream_close(stream_id_for_incoming)).await;
                }
                })
            } else {
                // Ensemble: no worker stream — frames after the aggregation
                // trigger are rejected (multi-round), §4.3.
                tokio::spawn(async move {
                    while let Some(Ok(chunk)) = stream.message().await.transpose() {
                        if matches!(chunk.payload, Some(pb::bidi_chunk::Payload::Data(_))) {
                            tracing::warn!(
                                stream_id = %stream_id_for_incoming,
                                "bidi frame after aggregation trigger ignored (multi-round rejected)"
                            );
                        }
                    }
                })
            };

            // Forward worker chunks -> gRPC
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
                            "bidi stream closed: deadline/idle elapsed"
                        );
                        stream_family = "5xx";
                        let deadline_hit = matches!(elapsed, streaming::RecvElapsed::Deadline);
                        reason = match elapsed {
                            streaming::RecvElapsed::Deadline => {
                                crate::metrics::prometheus::StreamCloseReason::Deadline
                            }
                            streaming::RecvElapsed::Idle => {
                                crate::metrics::prometheus::StreamCloseReason::Idle
                            }
                        };
                        // D35 (§4.4): a mid-stream DEADLINE is terminal — an
                        // Err item ends the tonic stream (nothing may follow).
                        if deadline_hit {
                            let _ = tx
                                .send(Err(Status::deadline_exceeded(
                                    "stream closed: deadline exceeded",
                                )))
                                .await;
                            crate::callback::fire_inference_response(&cb_runner, &req_ctx, start);
                        }
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
                        let bidi_chunk = pb::BidiChunk {
                            stream_id: stream_id.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                                data: c.data.clone(),
                            })),
                        };
                        if tx.send(Ok(bidi_chunk)).await.is_err() {
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
                        // Terminal: tonic's encode layer stops polling at the
                        // first Err item, so nothing may follow it on this
                        // channel (stream_infer / decoupled parity, FD-4).
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
                        let bidi_chunk = pb::BidiChunk {
                            stream_id: stream_id.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Close(pb::BidiClose {})),
                        };
                        let _ = tx.send(Ok(bidi_chunk)).await;
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
                "grpc_bidi",
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

            // B3 audit fix: cancel the worker on forwarder exit, aligned with
            // stream_infer (grpc/mod.rs:791) and decoupled (:1038). bidi was
            // previously the only streaming path that never sent a cancel on
            // client disconnect / deadline / idle — the worker kept generating
            // into an undrained ZMQ channel, pinning a worker slot. send_raw =
            // fire-and-forget (a cancel draws no unary reply, so .send() would
            // stall for ZMQ_RESPONSE_TIMEOUT). Harmless when the worker already
            // stopped (Done/Error) — it ignores the cancel.
            if is_ensemble {
                crate::ensemble::cancel_chain(
                    ensemble_chain.as_ref(),
                    ensemble_abort.as_ref(),
                    &stream_id,
                    &cancel_client,
                )
                .await;
            } else {
                let cancel_req = streaming::build_stream_cancel(stream_id.clone());
                let _ = cancel_client.send_raw(cancel_req).await;
            }

            // #8: observe the incoming task so a panic is logged, not silently
            // dropped by a bare abort().
            streaming::observe_or_abort(incoming_task).await;
        }
        .instrument(span.clone()));

        Ok(Response::new(ReceiverStream::new(rx)))
        }.instrument(tail_span).await
    }
}
