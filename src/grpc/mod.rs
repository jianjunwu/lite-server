use crate::error::AppError;
use crate::proto::liteserver as pb;
use crate::registry::ModelRegistry;
use crate::streaming;
use crate::worker::WorkerManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, warn};
use uuid::Uuid;

pub use pb::lite_server_server::{LiteServer, LiteServerServer};

/// Shared state for the gRPC service.
#[derive(Clone)]
pub struct GrpcService {
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    streaming_metrics: bool,
}

impl GrpcService {
    pub fn new(registry: Arc<ModelRegistry>, worker_manager: Arc<WorkerManager>, streaming_metrics: bool) -> Self {
        Self {
            registry,
            worker_manager,
            streaming_metrics,
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
                .await
                .ok_or_else(|| Status::not_found(format!("{} has no active version", model_name)))?,
        };

        if !self.registry.is_ready(model_name, version).await {
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

        let start = Instant::now();
        let resp = client
            .send(internal_req)
            .await
            .map_err(|e| Status::internal(format!("worker error: {}", e)))?;

        match resp.payload {
            Some(pb::response::Payload::Single(single)) => {
                let status = single.status.as_ref().map(|s| pb::Status {
                    code: s.code.clone(),
                    message: s.message.clone(),
                });
                if status.as_ref().map(|s| s.code.as_str()) == Some("Error") {
                    return Err(Status::internal(
                        status.map(|s| s.message).unwrap_or_default(),
                    ));
                }
                Ok(Response::new(pb::InferResponse {
                    data: single.data,
                    status,
                    metrics: resp.metrics,
                }))
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
                .await
                .ok_or_else(|| Status::not_found(format!("{} has no active version", model_name)))?,
        };

        if !self.registry.is_ready(model_name, version).await {
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
            payload: vec![],
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
                Ok(Response::new(pb::BatchInferResponse { items }))
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
                .await
                .ok_or_else(|| Status::not_found(format!("{} has no active version", model_name)))?,
        };

        if !self.registry.is_ready(model_name, version).await {
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
                        let _ = tx.send(Err(Status::internal(e.message.clone()))).await;
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
                            .await
                            .ok_or_else(|| {
                                Status::not_found(format!("{} has no active version", model_name))
                            })?,
                    };

                    if !self.registry.is_ready(&model_name, version.as_deref()).await {
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
                        let _ = tx.send(Err(Status::internal(e.message.clone()))).await;
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
) -> Result<(), AppError> {
    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| AppError::Config(format!("invalid gRPC address: {}", e)))?;

    let service = GrpcService::new(registry, worker_manager, streaming_metrics);
    let server = LiteServerServer::new(service);

    tracing::info!("Starting gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await
        .map_err(|e| AppError::Internal(format!("gRPC server error: {}", e)))?;

    Ok(())
}
