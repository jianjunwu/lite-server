//! Unary infer RPC: routed through the unified InferenceQueue (the same path
//! as REST, so gRPC inherits batching / least-loaded selection / outlier
//! ejection / retry / max_requests recycling), plus the P-ENSEMBLE-GRPC
//! DAG-executor dispatch for ensemble models.

use crate::error::AppError;
use crate::grpc::auth::{enforce_auth_grpc, enforce_grpc_rate_limit};
use crate::grpc::canary::canary_pin;
use crate::grpc::error::{
    app_error_to_grpc_status, err, http_status_to_grpc_code, model_error_status,
    try_parse_model_error, with_retry_after,
};
use crate::grpc::interceptor;
use crate::grpc::metadata::inject_grpc_metadata;
use crate::grpc::GrpcService;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use crate::request_context::RequestContext;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use uuid::Uuid;

impl GrpcService {
    pub(crate) async fn infer_impl(
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
                // B3 (E6): gRPC ensemble — parse JSON into Json variant,
                // fall back to Binary so malformed JSON is no longer silently
                // swallowed as Value::Null (the old unwrap_or behaviour).
                // MIMO (D32): the LSBE-1 container splits into the envelope
                // (declared-inputs ensembles); transport de-framing only.
                let ensemble_input = crate::ensemble::ensemble_payload_from_bytes(
                    &req.data,
                    // 审计 E-B1 修复:保留 proto headers 声明的 content-type
                    // (HTTP parity:Raw(bytes, ct) 保留声明值);未声明才回退。
                    req.headers.get("content-type").cloned(),
                )
                .map_err(|e| err(app_error_to_grpc_status(&e)))?;
                // P-DEADLINE: resolve here (grpc_metadata still in scope) and
                // pass to the ensemble cascade.
                let ensemble_deadline = crate::deadline::resolve_from_grpc(
                    &grpc_metadata,
                    self.server_timeout.as_secs_f32(),
                );
                // D37 (batch 0): signature converged — execution-face opts.
                let opts = crate::ensemble::EnsembleExecOpts {
                    client_ip: client_ip.clone(),
                    deadline_unix_ns: ensemble_deadline.unix_ns,
                    decoupled: false,
                };
                let result = crate::ensemble::execute_ensemble(
                    self.app_state.clone(),
                    model_name,
                    &resolved_version,
                    ensemble_input,
                    &request_id,
                    opts,
                )
                .await
                .map_err(|e| err(app_error_to_grpc_status(&e)))?;
                // B3 (E6) egress: Json → serialized bytes (historical path);
                // Binary → raw bytes (CT dropped, gRPC has no media_type field;
                // client must know the model contract, consistent with gRPC
                // unary semantics).
                let data = match result {
                    crate::ensemble::EnsembleOutcome::Unary(crate::ensemble::EnsembleValue::Json(v)) => {
                        bytes::Bytes::from(serde_json::to_vec(&v).unwrap_or_default())
                    }
                    crate::ensemble::EnsembleOutcome::Unary(crate::ensemble::EnsembleValue::Binary(b, _ct, ..)) => b,
                    // E7 (D32): the multi-sink response — InferResponse has
                    // no headers map, so the LSBE-1 container is the
                    // self-describing carrier (head + tail).
                    crate::ensemble::EnsembleOutcome::Unary(crate::ensemble::EnsembleValue::Envelope { head, tail }) => {
                        crate::ensemble::encode_lsbe1(&head, &tail)
                    }
                    crate::ensemble::EnsembleOutcome::Stream(_) => {
                        // D1: a unary endpoint calling a streaming DAG is a
                        // client contract violation — InvalidArgument (parity
                        // with the HTTP 400).
                        return Err(err(app_error_to_grpc_status(
                            &AppError::InvalidRequestBody(
                                "DAG contains a streaming step; use a streaming endpoint".to_string(),
                            ),
                        )));
                    }
                };
                return Ok(Response::new(pb::InferResponse {
                    data,
                    status: None,
                    metrics: None,
                }));
            }
        }

        // FD-1: gateway-side JSON validation (HTTP ApiBody D3 parity). The
        // ensemble branch above is excluded on purpose — gRPC ensemble treats
        // `data` as opaque bytes (B3/E6) and keeps that semantic.
        crate::grpc::payload::validate_json_payload(&req.headers, &req.data).map_err(err)?;

        let mut header_map: HashMap<String, String> = req.headers.clone();
        // P-TRACE: inject the active inference span's trace context into the
        // worker RequestMeta.headers (overwrites any client-supplied traceparent
        // so the worker is a child of THIS span; D8 Rust-only).
        crate::telemetry::inject(&mut header_map);
        // P8-1 (B3): envelope hints — debug 记录；消费点在队列（priority/affinity_key/direct_worker_id）。
        let hints = crate::request_context::RequestHints::from_grpc(&req.headers);
        if !hints.is_empty() {
            tracing::debug!(?hints, "envelope hints received");
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
        crate::callback::fire_inference_request(&self.callback_runner, &req_ctx);

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
            Err(crate::inference_queue::QueueError::InvalidWorker(msg)) => {
                // B3 direct-mode: x-lite-worker-id 不存在/已剔除 → InvalidArgument
                // （对齐 HTTP 400，客户端错误不落 5xx）。
                return Err(err(Status::invalid_argument(msg)));
            }
            Err(_) => {
                return Err(err(Status::unavailable(format!(
                    "queue not available for {} {}",
                    model_name, resolved_version
                ))));
            }
        };

        // Task A: record worker-reported metrics (HTTP parity). The metrics field
        // rides on the top-level Response (proto field 40); infer carries it on
        // the InferResponse but never recorded it before.
        crate::metrics::prometheus::record_worker_metrics(
            model_name,
            &resolved_version,
            resp.metrics.as_ref(),
        );

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
                        Err(err(Status::internal(msg)))
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
                        crate::callback::fire_inference_response(
                            &self.callback_runner,
                            &req_ctx,
                            start,
                        );

                        Ok(response)
                    }
                }
            }
            _ => Err(err(Status::internal("unexpected response type"))),
        }
    }
}
