//! Batch infer RPC: direct worker dispatch (not via the queue — batching is
//! already explicit in the request), with the response wait bounded by the
//! resolved deadline (B2 audit fix).

use crate::error::AppError;
use crate::grpc::auth::{enforce_auth_grpc, enforce_grpc_rate_limit};
use crate::grpc::canary::canary_pin;
use crate::grpc::error::err;
use crate::grpc::interceptor;
use crate::grpc::metadata::inject_grpc_metadata;
use crate::grpc::GrpcService;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use crate::request_context::RequestContext;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// D30 (batch 3): element payload classification — the unary rules
/// (`grpc_payload_is_json`: JSON content-type, or missing = JSON default)
/// applied to a single batch element. JSON → parsed Json; anything else →
/// opaque Binary bytes with the request's content-type.
fn d30_element_payload(
    data: &bytes::Bytes,
    headers: &HashMap<String, String>,
) -> Result<crate::ensemble::EnsembleValue, String> {
    if crate::grpc::payload::grpc_payload_is_json(headers) {
        let v: serde_json::Value = serde_json::from_slice(data)
            .map_err(|e| format!("element is not valid JSON: {e}"))?;
        Ok(crate::ensemble::EnsembleValue::Json(v))
    } else {
        let ct = headers
            .get("content-type")
            .cloned()
            .unwrap_or_else(|| "application/octet-stream".to_string());
        Ok(crate::ensemble::EnsembleValue::Binary(data.clone(), ct))
    }
}

/// D30: element-level error — the mapped §4.4 unary-row HTTP status rides
/// the status message (numeric-in-message convention, worker parity: 4xx
/// passthrough / 5xx 500 / queue 503 / deadline 504 / load 503 / validation
/// 400, via `AppError::http_status`).
fn d30_element_error(e: &AppError) -> pb::InferResponse {
    pb::InferResponse {
        data: bytes::Bytes::new(),
        status: Some(pb::Status {
            code: "Error".to_string(),
            message: format!("{} {}", e.http_status().as_u16(), e),
        }),
        metrics: None,
    }
}

/// D30: success element — Json serialized back to bytes; Binary passed
/// through verbatim (InferResponse carries no per-item content-type field).
fn d30_element_ok(value: crate::ensemble::EnsembleValue) -> pb::InferResponse {
    let data = match value {
        crate::ensemble::EnsembleValue::Json(v) => {
            bytes::Bytes::from(serde_json::to_vec(&v).unwrap_or_default())
        }
        crate::ensemble::EnsembleValue::Binary(data, _ct) => data,
    };
    pb::InferResponse {
        data,
        status: Some(pb::Status {
            code: "Ok".to_string(),
            message: String::new(),
        }),
        metrics: None,
    }
}

impl GrpcService {
    pub(crate) async fn batch_infer_impl(
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

        let mv = self.registry.get(model_name, Some(&resolved_version));
        if let Some(mv) = &mv {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
            enforce_grpc_rate_limit(&self.rate_limiter, mv.policies.rate_limit.as_ref(), model_name, &client_ip)?;
        }

        // Fire InferenceRequest callback — batch_infer now mirrors unary
        // infer_impl (previously it fired no callback at all). Built before
        // `meta` below moves request_id / client_ip.
        let req_ctx = crate::callback::InferenceContext {
            model_name: model_name.to_string(),
            version: resolved_version.clone(),
            route: "/predict".to_string(),
            protocol: crate::callback::Protocol::Grpc,
            request_id: request_id.clone(),
            client_ip: client_ip.clone(),
            elapsed_us: None,
        };
        crate::callback::fire_inference_request(&self.callback_runner, &req_ctx);

        // P-DEADLINE (§4.0.10): carried to the worker so it can stop; the batch
        // response wait below is also bounded server-side by this deadline.
        // Moved before the D30 ensemble branch — elements share the same bound.
        let deadline =
            crate::deadline::resolve_from_grpc(&grpc_metadata, self.server_timeout.as_secs_f32());

        // D30 (batch 3): batch × ensemble — element-wise DAG execution reusing
        // execute_ensemble (batch = the unary shell). Elements run in parallel,
        // response order preserved; element errors ride the element's status
        // (§4.4 unary rows, B3 direct propagation) and never fail the whole
        // batch. A streaming DAG is an unsupported combination → whole-RPC
        // InvalidArgument.
        if mv.as_ref().map(|m| m.model_type == ModelType::Ensemble).unwrap_or(false) {
            return self
                .batch_infer_ensemble(
                    model_name.to_string(),
                    resolved_version.clone(),
                    req,
                    client_ip.clone(),
                    request_id.clone(),
                    &deadline,
                    &req_ctx,
                    Instant::now(),
                )
                .await;
        }

        let mut header_map: HashMap<String, String> = req.headers.clone();
        // P-TRACE: inject the active inference span's trace context into the
        // worker RequestMeta.headers (overwrites any client-supplied traceparent
        // so the worker is a child of THIS span; D8 Rust-only).
        crate::telemetry::inject(&mut header_map);
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
        let start = Instant::now();
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

        // Task A: record worker-reported metrics. The top-level Response.metrics
        // (proto field 40) is the only metrics carrier for batch —
        // BatchItemResponse has no per-item metrics field, so per-item
        // InferResponse.metrics stays None (record the response-level metrics
        // once, matching unary / streaming parity).
        crate::metrics::prometheus::record_worker_metrics(
            model_name,
            &resolved_version,
            resp.metrics.as_ref(),
        );

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
                // Fire InferenceResponse callback (aligned with unary infer_impl).
                crate::callback::fire_inference_response(&self.callback_runner, &req_ctx, start);
                Ok(response)
            }
            _ => Err(err(Status::internal("unexpected response type"))),
        }
    }

    /// D30 (batch 3): ensemble branch of batch_infer_impl — load the plan
    /// once, reject streaming DAGs (no element-level streaming), then run
    /// every element through `execute_ensemble` in parallel and return the
    /// ordered per-element responses. Element-level failures map per §4.4
    /// unary rows (B3 direct propagation) into the element's status; only a
    /// plan-level failure (broken config) fails the whole RPC.
    #[allow(clippy::too_many_arguments)] // batch plumbing: ids+deadline+ctx ride together by design
    async fn batch_infer_ensemble(
        &self,
        model_name: String,
        resolved_version: String,
        req: pb::BatchInferRequest,
        client_ip: String,
        request_id: String,
        deadline: &crate::deadline::ResolvedDeadline,
        req_ctx: &crate::callback::InferenceContext,
        start: Instant,
    ) -> Result<Response<pb::BatchInferResponse>, Status> {
        let plan = crate::ensemble::get_ensemble_plan(&self.app_state, &model_name, &resolved_version)
            .await
            .map_err(|e| err(crate::grpc::error::app_error_to_grpc_status(&e)))?;
        if plan.steps[plan.output_step].stream || !plan.chains.is_empty() {
            return Err(err(Status::invalid_argument(
                "ensemble DAG contains a streaming step; batch has no element-level streaming (D30)",
            )));
        }

        let opts = crate::ensemble::EnsembleExecOpts {
            client_ip,
            deadline_unix_ns: deadline.unix_ns,
            decoupled: false,
        };
        // D15: ONE request-scope version snapshot shared by every element —
        // a batch is one logical request, so elements resolve sub-model
        // versions consistently.
        let snapshot = Arc::new(crate::ensemble::VersionSnapshot::default());

        let item_count = req.items.len();
        let mut set: tokio::task::JoinSet<(usize, pb::InferResponse)> =
            tokio::task::JoinSet::new();
        for (i, item) in req.items.into_iter().enumerate() {
            let payload = match d30_element_payload(&item, &req.headers) {
                Ok(p) => p,
                Err(msg) => {
                    // Parse failure is an element-level error (order kept via
                    // the index slot).
                    let resp = d30_element_error(&AppError::InvalidRequestBody(msg));
                    set.spawn(async move { (i, resp) });
                    continue;
                }
            };
            let state = self.app_state.clone();
            let model = model_name.clone();
            let version = resolved_version.clone();
            let request_id_elem = format!("{}:{}", request_id, i);
            let opts = opts.clone();
            let snapshot = Arc::clone(&snapshot);
            set.spawn(async move {
                let resp = match crate::ensemble::execute_ensemble_inner(
                    state, &model, &version, payload, &request_id_elem, opts, &snapshot, 0, &[],
                )
                .await
                {
                    Ok(crate::ensemble::EnsembleOutcome::Unary(v)) => d30_element_ok(v),
                    Ok(crate::ensemble::EnsembleOutcome::Stream(_)) => d30_element_error(
                        &AppError::Internal(
                            "ensemble returned a stream from batch (D30 violation)".to_string(),
                        ),
                    ),
                    Err(e) => d30_element_error(&e),
                };
                (i, resp)
            });
        }

        let mut items: Vec<Option<pb::InferResponse>> = vec![None; item_count];
        while let Some(joined) = set.join_next().await {
            let (i, resp) = joined.map_err(|e| {
                err(Status::internal(format!("batch element task join error: {e}")))
            })?;
            items[i] = Some(resp);
        }
        let items = items
            .into_iter()
            .map(|r| r.expect("every spawned element task reports its slot"))
            .collect();

        crate::callback::fire_inference_response(&self.callback_runner, req_ctx, start);
        Ok(Response::new(pb::BatchInferResponse { items }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// D30: JSON content-type (and the missing-ct default) parses elements
    /// as Json; any other content-type treats them as opaque Binary.
    #[test]
    fn d30_element_payload_classification() {
        let json_headers: HashMap<String, String> =
            [("content-type".to_string(), "application/json".to_string())].into();
        let v = d30_element_payload(&bytes::Bytes::from_static(br#"{"a": 1}"#), &json_headers).unwrap();
        assert_eq!(
            v,
            crate::ensemble::EnsembleValue::Json(serde_json::json!({"a": 1}))
        );

        // Missing content-type → JSON default (unary parity).
        let v = d30_element_payload(&bytes::Bytes::from_static(br#"{"a": 1}"#), &HashMap::new()).unwrap();
        assert!(matches!(v, crate::ensemble::EnsembleValue::Json(_)));

        // Non-JSON content-type → Binary with the declared content-type.
        let bin_headers: HashMap<String, String> =
            [("content-type".to_string(), "image/png".to_string())].into();
        let v = d30_element_payload(&bytes::Bytes::from_static(&[0x00, 0x01]), &bin_headers).unwrap();
        match v {
            crate::ensemble::EnsembleValue::Binary(data, ct) => {
                assert_eq!(data.as_ref(), &[0x00, 0x01]);
                assert_eq!(ct, "image/png");
            }
            other => panic!("expected Binary, got {other:?}"),
        }

        // JSON content-type + invalid bytes → element error.
        let res = d30_element_payload(&bytes::Bytes::from_static(b"not json"), &json_headers);
        assert!(res.is_err(), "invalid JSON element must error");
    }

    /// D30: element errors carry the mapped §4.4 unary-row HTTP status in
    /// the status message (numeric-in-message, worker parity).
    #[test]
    fn d30_element_error_maps_http_status() {
        let e = crate::error::AppError::QueueFull("queue full".to_string());
        let resp = d30_element_error(&e);
        let status = resp.status.as_ref().unwrap();
        assert_eq!(status.code, "Error");
        assert!(
            status.message.starts_with("503"),
            "queue full → 503, got {}",
            status.message
        );

        let e = crate::error::AppError::InferenceTimeout("t".to_string());
        assert!(
            d30_element_error(&e).status.unwrap().message.starts_with("504"),
            "step timeout → 504"
        );
    }

    /// D30: success elements — Json serialized back to bytes, Binary passed
    /// through verbatim (InferResponse carries no per-item content-type).
    #[test]
    fn d30_element_ok_shapes() {
        let resp = d30_element_ok(crate::ensemble::EnsembleValue::Json(serde_json::json!({"x": 1})));
        assert_eq!(resp.status.as_ref().unwrap().code, "Ok");
        assert_eq!(resp.data.as_ref(), &br#"{"x":1}"#[..]);

        let resp = d30_element_ok(crate::ensemble::EnsembleValue::Binary(
            bytes::Bytes::from_static(b"raw"),
            "image/png".to_string(),
        ));
        assert_eq!(resp.status.as_ref().unwrap().code, "Ok");
        assert_eq!(resp.data.as_ref(), b"raw");
    }
}
