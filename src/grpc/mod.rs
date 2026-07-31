use crate::callback::CallbackRunner;
use crate::error::AppError;
use crate::proto::liteserver as pb;
use crate::registry::ModelRegistry;
use crate::request_context::RequestContext;
use crate::streaming;
use crate::worker::WorkerManager;
use std::collections::HashMap;
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

pub mod interceptor;

pub use pb::lite_server_server::{LiteServer, LiteServerServer};

/// Shared state for the gRPC service.
#[derive(Clone)]
pub struct GrpcService {
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    streaming_metrics: bool,
    callback_runner: Arc<CallbackRunner>,
    /// Per-request inference deadline. Mirrors the REST path's
    /// `config.server.timeout` so gRPC and HTTP share one request budget.
    server_timeout: Duration,
}

impl GrpcService {
    pub fn new(
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        streaming_metrics: bool,
        callback_runner: Arc<CallbackRunner>,
        server_timeout: Duration,
    ) -> Self {
        Self {
            registry,
            worker_manager,
            streaming_metrics,
            callback_runner,
            server_timeout,
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

        // version="" → weighted routing pick (§4.3), falling back to active.
        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => self
                .registry
                .routing_pick(model_name)
                .or_else(|| self.registry.get_active_version(model_name))
                .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
        };
        self.registry.touch_last_used(model_name, &resolved_version);
        *version_label = resolved_version.clone();

        if !self.registry.is_ready(model_name, version) {
            return Err(err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            ))));
        }

        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
        }

        let header_map: HashMap<String, String> = req.headers.clone();
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
            Ok(()) => match tokio::time::timeout(self.server_timeout, response_rx).await {
                Ok(Ok(resp)) => resp,
                Ok(Err(_)) => return Err(err(Status::internal("response channel closed"))),
                Err(_) => {
                    return Err(err(Status::deadline_exceeded(format!(
                        "inference timed out after {:.1}s",
                        self.server_timeout.as_secs_f64()
                    ))));
                }
            },
            Err(crate::inference_queue::QueueError::Full) => {
                // §4.0.9: queue-full/过载 → Unavailable（落 5xx）；
                // ResourceExhausted 专给限流（P3-1）。
                return Err(err(Status::unavailable(format!(
                    "queue full for {} {}",
                    model_name, resolved_version
                ))));
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

        // version="" → weighted routing pick (§4.3), falling back to active.
        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => self
                .registry
                .routing_pick(model_name)
                .or_else(|| self.registry.get_active_version(model_name))
                .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
        };
        self.registry.touch_last_used(model_name, &resolved_version);
        *version_label = resolved_version.clone();

        if !self.registry.is_ready(model_name, version) {
            return Err(err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            ))));
        }

        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
        }

        let header_map: HashMap<String, String> = req.headers.clone();
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
            ..Default::default()
        };

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

        let resp = client
            .send(internal_req)
            .await
            .map_err(|e| err(Status::internal(format!("worker error: {}", e))))?;

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

        // version="" → weighted routing pick (§4.3), falling back to active.
        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => self
                .registry
                .routing_pick(model_name)
                .or_else(|| self.registry.get_active_version(model_name))
                .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
        };
        self.registry.touch_last_used(model_name, &resolved_version);
        *version_label = resolved_version.clone();

        if !self.registry.is_ready(model_name, version) {
            return Err(err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            ))));
        }

        if let Some(mv) = self.registry.get(model_name, Some(&resolved_version)) {
            enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &req.headers)?;
        }

        let header_map: HashMap<String, String> = req.headers.clone();
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
            ..Default::default()
        };

        let stream_id = format!("grpc-stream-{}", Uuid::new_v4());
        let open_req = streaming::build_stream_open(stream_id.clone(), req.data, Some(meta));

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
            .get_outlier_state(model_name.as_str(), &resolved_version)
            .await
        {
            Some(outlier) => crate::worker::pick_worker_skip_ejected(clients.len(), &outlier),
            None => crate::worker::pick_worker_random(clients.len()),
        };
        let client = clients[worker_id].clone();

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

            while let Some(chunk) = chunk_rx.recv().await {
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
                                    model_error_status(
                                        error_type_to_grpc_code(&parsed.error_type),
                                        &parsed,
                                    )
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
            // Cleanup: send cancel to worker
            let cancel_req = streaming::build_stream_cancel(stream_id);
            let _ = cancel_client.send(cancel_req).await;
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
        );
        let request_id = cx.request_id;
        *request_id_out = request_id.clone();
        let client_ip = cx.client_ip;

        // Wait for first message (must be BidiOpen)
        let first = stream
            .message()
            .await
            .map_err(|e| err(Status::internal(format!("stream error: {}", e))))?;

        let (model_name, resolved_version, stream_id, initial_data) = match first {
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

                    // version="" → weighted routing pick (§4.3), falling back
                    // to active; stamps last_used_at (P0-2 bidi parity).
                    let resolved_version =
                        resolve_bidi_version(&self.registry, &model_name, version.as_deref())?;

                    if !self.registry.is_ready(&model_name, version.as_deref()) {
                        return Err(err(Status::unavailable(format!(
                            "{} version {} is not ready",
                            model_name, resolved_version
                        ))));
                    }

                    // BidiOpen has no headers map — transport metadata is the
                    // only credential carrier on this path.
                    if let Some(mv) = self.registry.get(&model_name, Some(&resolved_version)) {
                        enforce_auth_grpc(mv.policies.auth.as_ref(), &grpc_metadata, &HashMap::new())?;
                    }

                    let sid = format!("grpc-bidi-{}", Uuid::new_v4());
                    (model_name, resolved_version, sid, open.initial_data)
                }
                _ => return Err(err(Status::invalid_argument("first message must be BidiOpen"))),
            },
            None => return Err(err(Status::invalid_argument("empty stream"))),
        };

        *model_label = model_name.clone();
        *version_label = resolved_version.clone();

        // P2-3 span：覆盖 bidi handler 全程（model/version 在 Open 解码后已知）。
        let span = tracing::info_span!(
            "inference",
            model = %model_name,
            version = %resolved_version,
            request_id = %request_id,
        );
        async move {
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: HashMap::new(),
            client_ip,
            request_id,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: initial_data.clone(),
            ..Default::default()
        };

        let open_req = streaming::build_stream_open(stream_id.clone(), initial_data, Some(meta));

        let clients = self
            .worker_manager
            .get_zmq_clients(&model_name, &resolved_version)
            .await
            .ok_or_else(|| err(Status::unavailable("no workers available")))?;

        if clients.is_empty() {
            return Err(err(Status::unavailable("no workers available")));
        }

        let worker_id = match self
            .worker_manager
            .get_outlier_state(model_name.as_str(), &resolved_version)
            .await
        {
            Some(outlier) => crate::worker::pick_worker_skip_ejected(clients.len(), &outlier),
            None => crate::worker::pick_worker_random(clients.len()),
        };
        let client = clients[worker_id].clone();

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| err(Status::internal(format!("worker stream error: {}", e))))?;

        let (tx, rx) = mpsc::channel(64);
        let worker_client = client.clone();

        let stream_metrics = self.streaming_metrics;
        let metrics_model = model_name.clone();
        let metrics_version = resolved_version.clone();
        if stream_metrics {
            crate::metrics::prometheus::record_stream_open(&metrics_model, &metrics_version, "grpc");
        }

        // Spawn forwarder: worker chunks -> gRPC stream
        let stream_id_for_incoming = stream_id.clone();
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

            while let Some(chunk) = chunk_rx.recv().await {
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
                                    model_error_status(
                                        error_type_to_grpc_code(&parsed.error_type),
                                        &parsed,
                                    )
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
        429 => tonic::Code::ResourceExhausted,
        503 => tonic::Code::Unavailable,
        504 => tonic::Code::DeadlineExceeded,
        _ => tonic::Code::Internal,
    }
}

/// Map an error_type string (from a structured stream error) to a gRPC code.
fn error_type_to_grpc_code(error_type: &str) -> tonic::Code {
    match error_type {
        "invalid_request_error" => tonic::Code::InvalidArgument,
        "authentication_error" => tonic::Code::Unauthenticated,
        "permission_denied_error" => tonic::Code::PermissionDenied,
        "not_found_error" => tonic::Code::NotFound,
        "service_unavailable" | "model_not_ready" => tonic::Code::Unavailable,
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

/// Resolve the serving version for `bidi_stream` (P0-2 parity with
/// unary/batch/stream): version="" → weighted routing pick (§4.3), falling
/// back to the active version; explicit version passes through. Stamps
/// `last_used_at` for LRU eviction on the resolved version.
///
/// The protocol layer only passes parameters — the actual routing decision
/// is delegated to the registry (`routing_pick` / `get_active_version`).
fn resolve_bidi_version(
    registry: &ModelRegistry,
    model_name: &str,
    version: Option<&str>,
) -> Result<String, Status> {
    let resolved = match version {
        Some(v) => v.to_string(),
        None => registry
            .routing_pick(model_name)
            .or_else(|| registry.get_active_version(model_name))
            .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
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
fn err(status: Status) -> Status {
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
        );
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
        );
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
        );
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

    type BidiStreamStream = ReceiverStream<Result<pb::BidiChunk, Status>>;

    async fn bidi_stream(
        &self,
        request: Request<Streaming<pb::BidiChunk>>,
    ) -> Result<Response<Self::BidiStreamStream>, Status> {
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

/// Start the gRPC server.
pub async fn start_grpc_server(
    host: String,
    port: u16,
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    streaming_metrics: bool,
    callback_runner: Arc<CallbackRunner>,
    server_timeout: Duration,
    grpc_config: crate::config::GrpcConfig,
) -> Result<(), AppError> {
    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| AppError::Config(format!("invalid gRPC address: {}", e)))?;

    let service = GrpcService::new(
        registry,
        worker_manager.clone(),
        streaming_metrics,
        callback_runner,
        server_timeout,
    );
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
    // P-MW (蓝图 §4.0.3, D20): pre-decode interceptor fills RequestContext
    // into request extensions — mounted on LiteServer AND health (挂载矩阵).
    // Pre-call semantics only: it cannot touch responses/Status, so echo
    // (P2-2) and error logging (P1-1) stay in handlers. Transparent to
    // unary/stream/bidi (runs once per RPC, never touches the message flow).
    let server = tonic::codegen::InterceptedService::new(server, interceptor::context_interceptor);

    // Standard gRPC health checking (grpc.health.v1): the reporter lives in
    // the WorkerManager, which syncs "" and per-model services on every
    // status transition and coordinator tick (phase 3).
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    worker_manager.set_grpc_health_reporter(health_reporter).await;
    worker_manager.sync_grpc_health().await;
    let health_service =
        tonic::codegen::InterceptedService::new(health_service, interceptor::context_interceptor);

    tracing::info!("Starting gRPC server on {}", addr);

    let mut builder = tonic::transport::Server::builder();
    // P1-2: HTTP/2 keepalive + flow-control window / frame size tuning.
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
        .add_service(server)
        .add_service(health_service)
        .serve(addr)
        .await
        .map_err(|e| AppError::Internal(format!("gRPC server error: {}", e)))?;

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

        let resolved = resolve_bidi_version(&reg, "m1", None).unwrap();
        assert_eq!(resolved, "2");
    }

    #[test]
    fn test_bidi_resolve_version_falls_back_to_active_without_routing() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();

        let resolved = resolve_bidi_version(&reg, "m1", None).unwrap();
        assert_eq!(resolved, "1");
    }

    #[test]
    fn test_bidi_resolve_version_touches_last_used() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();
        assert!(reg.get("m1", Some("1")).unwrap().last_used_at.is_none());

        let resolved = resolve_bidi_version(&reg, "m1", None).unwrap();
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
        let resolved = resolve_bidi_version(&reg, "m1", Some("2")).unwrap();
        assert_eq!(resolved, "2");
    }

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
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue,
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        GrpcService::new(
            registry,
            wm,
            false,
            Arc::new(CallbackRunner::new()),
            Duration::from_secs(5),
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
    /// request_id）。错误路径在 span 内打日志（err 助手），故 span 上下文可见。
    /// handler 创建 `inference` span，字段与 HTTP info_span! 一致（model/version/
    /// request_id）。用一个 in-memory capturing subscriber（thread-local
    /// set_default）捕获 err() 在 span 内打的 info 事件——其输出含 span 名 +
    /// 字段，证明 span 被创建并覆盖 handler。（蓝图建议 tracing-test，但其
    /// 属性宏在本工具链未注入 `logs`，故用等价的手动 capturing subscriber。）
    #[tokio::test]
    async fn should_create_inference_span_with_fields() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::fmt::format::FmtSpan;
        use tracing_subscriber::fmt::{MakeWriter};

        #[derive(Clone)]
        struct CaptureMaker(Arc<Mutex<Vec<u8>>>);
        impl<'a> MakeWriter<'a> for CaptureMaker {
            type Writer = CaptureWriter;
            fn make_writer(&'a self) -> Self::Writer {
                CaptureWriter(self.0.clone())
            }
        }
        struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().write(b)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(CaptureMaker(buf.clone()))
            .with_target(false)
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_span_events(FmtSpan::ACTIVE)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let service = build_service(Arc::new(ModelRegistry::new()), Arc::new(InferenceQueue::new()));
        let mut req = infer_request("span_404", "");
        req.metadata_mut()
            .insert("x-client-request-id", "span-rid".parse().unwrap());
        let err = service.infer(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);

        let logs = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("inference"), "inference span must be created: {}", logs);
        assert!(logs.contains("span_404"), "span must carry model field: {}", logs);
        assert!(logs.contains("span-rid"), "span must carry request_id: {}", logs);
    }
}
