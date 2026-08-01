//! Batch infer RPC: direct worker dispatch (not via the queue — batching is
//! already explicit in the request), with the response wait bounded by the
//! resolved deadline (B2 audit fix).

use crate::grpc::auth::{enforce_auth_grpc, enforce_grpc_rate_limit};
use crate::grpc::canary::canary_pin;
use crate::grpc::error::err;
use crate::grpc::interceptor;
use crate::grpc::metadata::inject_grpc_metadata;
use crate::grpc::GrpcService;
use crate::proto::liteserver as pb;
use crate::request_context::RequestContext;
use std::collections::HashMap;
use tonic::{Request, Response, Status};
use uuid::Uuid;

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
}
