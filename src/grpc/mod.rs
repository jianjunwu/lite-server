use crate::callback::CallbackRunner;
use crate::error::AppError;
use crate::proto::liteserver as pb;
use crate::registry::ModelRegistry;
use crate::streaming;
use crate::worker::WorkerManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use uuid::Uuid;

pub use pb::lite_server_server::{LiteServer, LiteServerServer};

/// Shared state for the gRPC service.
#[derive(Clone)]
pub struct GrpcService {
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    streaming_metrics: bool,
    callback_runner: Arc<CallbackRunner>,
}

impl GrpcService {
    pub fn new(
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        streaming_metrics: bool,
        callback_runner: Arc<CallbackRunner>,
    ) -> Self {
        Self {
            registry,
            worker_manager,
            streaming_metrics,
            callback_runner,
        }
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

/// Map an error_code string (from a structured stream error) to a gRPC code.
fn error_code_to_grpc_code(code: &str) -> tonic::Code {
    match code {
        "BAD_REQUEST" | "INVALID_INPUT" | "VALIDATION_ERROR" => tonic::Code::InvalidArgument,
        "UNAUTHORIZED" => tonic::Code::Unauthenticated,
        "FORBIDDEN" => tonic::Code::PermissionDenied,
        "NOT_FOUND" => tonic::Code::NotFound,
        "SERVICE_UNAVAILABLE" | "MODEL_NOT_READY" => tonic::Code::Unavailable,
        _ => tonic::Code::Internal,
    }
}

/// Extract (error_code, message) from a structured model error JSON payload.
fn try_parse_model_error(
    data: &serde_json::Value,
) -> Option<(String, String)> {
    let err = data.get("error")?;
    let code = err.get("code")?.as_str()?.to_string();
    let message = err.get("message")?.as_str()?.to_string();
    Some((code, message))
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

#[tonic::async_trait]
impl LiteServer for GrpcService {
    async fn infer(
        &self,
        request: Request<pb::InferRequest>,
    ) -> Result<Response<pb::InferResponse>, Status> {
        let req = request.into_inner();
        let model_name = &req.model_name;
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        if let Err(e) = crate::validation::validate_identifier(model_name) {
            return Err(Status::invalid_argument(e.to_string()));
        }

        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => self
                .registry
                .get_active_version(model_name)
                .ok_or_else(|| Status::not_found(format!("{} has no active version", model_name)))?,
        };

        if !self.registry.is_ready(model_name, version) {
            return Err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            )));
        }

        let header_map: HashMap<String, String> = req.headers.clone();
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: header_map,
            client_ip: "".to_string(),
            request_id: Uuid::new_v4().to_string(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: req.data.clone(),
        };

        let uid = format!("grpc-{}-{}", model_name, Uuid::new_v4());
        let internal_req = pb::Request {
            uid,
            meta: Some(meta),
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: req.data,
            })),
        };

        let clients = self
            .worker_manager
            .get_zmq_clients(model_name, &resolved_version)
            .await
            .ok_or_else(|| Status::unavailable("no workers available"))?;

        if clients.is_empty() {
            return Err(Status::unavailable("no workers available"));
        }

        let worker_id = crate::worker::pick_worker_random(clients.len());
        let client = &clients[worker_id];

        // Fire InferenceRequest callback
        let req_ctx = crate::callback::InferenceContext {
            model_name: model_name.to_string(),
            version: resolved_version.clone(),
            route: "/predict".to_string(),
            protocol: crate::callback::Protocol::Grpc,
            request_id: Uuid::new_v4().to_string(),
            client_ip: String::new(),
            elapsed_us: None,
        };
        let cb_runner = self.callback_runner.clone();
        let req_ctx_clone = req_ctx.clone();
        tokio::spawn(async move {
            cb_runner.on_inference_request(&req_ctx_clone).await;
        });

        let start = Instant::now();
        let resp = client
            .send(internal_req)
            .await
            .map_err(|e| Status::internal(format!("worker error: {}", e)))?;

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
                            let (error_code, error_message) =
                                try_parse_model_error(&data)
                                    .unwrap_or_else(|| ("MODEL_ERROR".into(), msg));
                            return Err(Status::new(
                                http_status_to_grpc_code(http_status),
                                format!("[{}] {}", error_code, error_message),
                            ));
                        }
                        // Not a numeric status code — internal worker error.
                        return Err(Status::internal(msg));
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
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn batch_infer(
        &self,
        request: Request<pb::BatchInferRequest>,
    ) -> Result<Response<pb::BatchInferResponse>, Status> {
        let req = request.into_inner();
        let model_name = &req.model_name;
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        if let Err(e) = crate::validation::validate_identifier(model_name) {
            return Err(Status::invalid_argument(e.to_string()));
        }

        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => self
                .registry
                .get_active_version(model_name)
                .ok_or_else(|| Status::not_found(format!("{} has no active version", model_name)))?,
        };

        if !self.registry.is_ready(model_name, version) {
            return Err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            )));
        }

        let header_map: HashMap<String, String> = req.headers.clone();
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: header_map,
            client_ip: "".to_string(),
            request_id: Uuid::new_v4().to_string(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: Default::default(),
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
            .ok_or_else(|| Status::unavailable("no workers available"))?;

        if clients.is_empty() {
            return Err(Status::unavailable("no workers available"));
        }

        let worker_id = crate::worker::pick_worker_random(clients.len());
        let client = &clients[worker_id];

        let resp = client
            .send(internal_req)
            .await
            .map_err(|e| Status::internal(format!("worker error: {}", e)))?;

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
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    type StreamInferStream = ReceiverStream<Result<pb::StreamChunk, Status>>;

    async fn stream_infer(
        &self,
        request: Request<pb::StreamInferRequest>,
    ) -> Result<Response<Self::StreamInferStream>, Status> {
        let req = request.into_inner();
        let model_name = &req.model_name;
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        if let Err(e) = crate::validation::validate_identifier(model_name) {
            return Err(Status::invalid_argument(e.to_string()));
        }

        let resolved_version = match version {
            Some(v) => v.to_string(),
            None => self
                .registry
                .get_active_version(model_name)
                .ok_or_else(|| Status::not_found(format!("{} has no active version", model_name)))?,
        };

        if !self.registry.is_ready(model_name, version) {
            return Err(Status::unavailable(format!(
                "{} version {} is not ready",
                model_name, resolved_version
            )));
        }

        let header_map: HashMap<String, String> = req.headers.clone();
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: header_map,
            client_ip: "".to_string(),
            request_id: Uuid::new_v4().to_string(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: req.data.clone(),
        };

        let stream_id = format!("grpc-stream-{}", Uuid::new_v4());
        let open_req = streaming::build_stream_open(stream_id.clone(), req.data, Some(meta));

        let clients = self
            .worker_manager
            .get_zmq_clients(model_name, &resolved_version)
            .await
            .ok_or_else(|| Status::unavailable("no workers available"))?;

        if clients.is_empty() {
            return Err(Status::unavailable("no workers available"));
        }

        let worker_id = crate::worker::pick_worker_random(clients.len());
        let client = clients[worker_id].clone();

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| Status::internal(format!("worker stream error: {}", e)))?;

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
                                if let Some((error_code, error_message)) = try_parse_model_error(&val) {
                                    Status::new(
                                        error_code_to_grpc_code(&error_code),
                                        format!("[{}] {}", error_code, error_message),
                                    )
                                } else {
                                    Status::internal(e.message.clone())
                                }
                            }
                            Err(_) => Status::internal(e.message.clone()),
                        };
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
            // Cleanup: send cancel to worker
            let cancel_req = streaming::build_stream_cancel(stream_id);
            let _ = cancel_client.send(cancel_req).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type BidiStreamStream = ReceiverStream<Result<pb::BidiChunk, Status>>;

    async fn bidi_stream(
        &self,
        request: Request<Streaming<pb::BidiChunk>>,
    ) -> Result<Response<Self::BidiStreamStream>, Status> {
        let mut stream = request.into_inner();

        // Wait for first message (must be BidiOpen)
        let first = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("stream error: {}", e)))?;

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
                        return Err(Status::invalid_argument(e.to_string()));
                    }

                    let resolved_version = match &version {
                        Some(v) => v.clone(),
                        None => self
                            .registry
                            .get_active_version(&model_name)
                            .ok_or_else(|| {
                                Status::not_found(format!("{} has no active version", model_name))
                            })?,
                    };

                    if !self.registry.is_ready(&model_name, version.as_deref()) {
                        return Err(Status::unavailable(format!(
                            "{} version {} is not ready",
                            model_name, resolved_version
                        )));
                    }

                    let sid = format!("grpc-bidi-{}", Uuid::new_v4());
                    (model_name, resolved_version, sid, open.initial_data)
                }
                _ => return Err(Status::invalid_argument("first message must be BidiOpen")),
            },
            None => return Err(Status::invalid_argument("empty stream")),
        };

        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: HashMap::new(),
            client_ip: "".to_string(),
            request_id: Uuid::new_v4().to_string(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            payload: initial_data.clone(),
        };

        let open_req = streaming::build_stream_open(stream_id.clone(), initial_data, Some(meta));

        let clients = self
            .worker_manager
            .get_zmq_clients(&model_name, &resolved_version)
            .await
            .ok_or_else(|| Status::unavailable("no workers available"))?;

        if clients.is_empty() {
            return Err(Status::unavailable("no workers available"));
        }

        let worker_id = crate::worker::pick_worker_random(clients.len());
        let client = clients[worker_id].clone();

        let mut chunk_rx = client
            .send_stream(open_req, stream_id.clone())
            .await
            .map_err(|e| Status::internal(format!("worker stream error: {}", e)))?;

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
            // Forward incoming bidi chunks to worker as StreamRequest::Chunk
            let incoming_task = tokio::spawn(async move {
                while let Some(Ok(chunk)) = stream.message().await.transpose() {
                    match chunk.payload {
                        Some(pb::bidi_chunk::Payload::Data(data)) => {
                            let chunk_req = streaming::build_stream_chunk(
                                stream_id_for_incoming.clone(),
                                data.data,
                            );
                            let _ = worker_client.send(chunk_req).await;
                        }
                        Some(pb::bidi_chunk::Payload::Close(_)) => {
                            let close_req = streaming::build_stream_close(stream_id_for_incoming.clone());
                            let _ = worker_client.send(close_req).await;
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
                                if let Some((error_code, error_message)) = try_parse_model_error(&val) {
                                    Status::new(
                                        error_code_to_grpc_code(&error_code),
                                        format!("[{}] {}", error_code, error_message),
                                    )
                                } else {
                                    Status::internal(e.message.clone())
                                }
                            }
                            Err(_) => Status::internal(e.message.clone()),
                        };
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

            incoming_task.abort();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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
) -> Result<(), AppError> {
    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| AppError::Config(format!("invalid gRPC address: {}", e)))?;

    let service = GrpcService::new(registry, worker_manager, streaming_metrics, callback_runner);
    let server = LiteServerServer::new(service);

    tracing::info!("Starting gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await
        .map_err(|e| AppError::Internal(format!("gRPC server error: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_error_code_to_grpc_code() {
        assert_eq!(error_code_to_grpc_code("BAD_REQUEST"), tonic::Code::InvalidArgument);
        assert_eq!(error_code_to_grpc_code("INVALID_INPUT"), tonic::Code::InvalidArgument);
        assert_eq!(error_code_to_grpc_code("VALIDATION_ERROR"), tonic::Code::InvalidArgument);
        assert_eq!(error_code_to_grpc_code("UNAUTHORIZED"), tonic::Code::Unauthenticated);
        assert_eq!(error_code_to_grpc_code("FORBIDDEN"), tonic::Code::PermissionDenied);
        assert_eq!(error_code_to_grpc_code("NOT_FOUND"), tonic::Code::NotFound);
        assert_eq!(error_code_to_grpc_code("SERVICE_UNAVAILABLE"), tonic::Code::Unavailable);
        assert_eq!(error_code_to_grpc_code("MODEL_NOT_READY"), tonic::Code::Unavailable);
        // Unknown falls back to Internal
        assert_eq!(error_code_to_grpc_code("UNKNOWN_CODE"), tonic::Code::Internal);
    }

    #[test]
    fn test_try_parse_model_error_valid() {
        let data = serde_json::json!({
            "error": {
                "code": "INVALID_INPUT",
                "message": "input must be non-negative"
            }
        });
        let result = try_parse_model_error(&data);
        assert!(result.is_some());
        let (code, message) = result.unwrap();
        assert_eq!(code, "INVALID_INPUT");
        assert_eq!(message, "input must be non-negative");
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
        let data = serde_json::json!({"error": {"code": "X"}});
        assert!(try_parse_model_error(&data).is_none());

        let data = serde_json::json!({"error": {"message": "X"}});
        assert!(try_parse_model_error(&data).is_none());

        let data = serde_json::json!({"other": "stuff"});
        assert!(try_parse_model_error(&data).is_none());
    }
}
