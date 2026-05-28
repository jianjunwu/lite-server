use crate::error::AppError;
use crate::worker::protocol::{BatchInferenceResponse, InferenceRequest, InferenceResponse, ResponseStatus};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(windows)]
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{error, info, warn};

#[cfg(unix)]
type Stream = UnixStream;
#[cfg(windows)]
type Stream = TcpStream;

#[cfg(unix)]
async fn connect_stream(path: &std::path::Path) -> Result<Stream, std::io::Error> {
    UnixStream::connect(path).await
}

#[cfg(windows)]
async fn connect_stream(path: &std::path::Path) -> Result<Stream, std::io::Error> {
    let path_str = path.to_string_lossy();
    let port = derive_port_from_path(&path_str);
    TcpStream::connect(format!("127.0.0.1:{}", port)).await
}

#[cfg(windows)]
fn derive_port_from_path(path: &str) -> u16 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    30000 + (hasher.finish() % 35535) as u16
}

// ===== Global connection pool =====

lazy_static::lazy_static! {
    static ref CONNECTION_POOL: RwLock<HashMap<PathBuf, Arc<WorkerConnection>>> = RwLock::new(HashMap::new());
}

/// Remove a connection from the pool (called on worker unload).
pub async fn remove_connection(uds_path: &Path) {
    let mut guard = CONNECTION_POOL.write().await;
    if let Some(conn) = guard.remove(uds_path) {
        conn.close();
    }
}

/// Clear all connections from the pool.
pub async fn clear_connections() {
    let mut guard = CONNECTION_POOL.write().await;
    for (_, conn) in guard.drain() {
        conn.close();
    }
}

// ===== WorkerConnection =====

enum InferRequest {
    Single(InferenceRequest, oneshot::Sender<InferenceResponse>),
    Batch(InferenceRequest, oneshot::Sender<BatchInferenceResponse>),
}

/// A persistent UDS connection to a Python worker with request multiplexing.
pub struct WorkerConnection {
    request_tx: mpsc::UnboundedSender<InferRequest>,
    closed: std::sync::atomic::AtomicBool,
}

impl WorkerConnection {
    pub async fn new(uds_path: &Path) -> Result<Self, AppError> {
        let stream = connect_stream(uds_path)
            .await
            .map_err(|e| AppError::Transport(format!("failed to connect to UDS {}: {}", uds_path.display(), e)))?;

        let (read_half, write_half) = split(stream);
        let (request_tx, request_rx) = mpsc::unbounded_channel::<InferRequest>();

        let pending_single: Arc<RwLock<HashMap<String, oneshot::Sender<InferenceResponse>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let pending_batch: Arc<RwLock<HashMap<String, oneshot::Sender<BatchInferenceResponse>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Start writer task
        let pending_writer = pending_single.clone();
        let pending_writer_batch = pending_batch.clone();
        tokio::spawn(writer_task(
            write_half,
            request_rx,
            pending_writer,
            pending_writer_batch,
        ));

        // Start reader task
        let pending_reader = pending_single.clone();
        let pending_reader_batch = pending_batch.clone();
        tokio::spawn(reader_task(
            read_half,
            pending_reader,
            pending_reader_batch,
        ));

        Ok(Self {
            request_tx,
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub async fn send_single(&self, request: InferenceRequest) -> Result<InferenceResponse, AppError> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(AppError::Transport("worker connection closed".to_string()));
        }
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(InferRequest::Single(request, tx))
            .map_err(|_| AppError::Transport("worker connection closed".to_string()))?;
        rx.await
            .map_err(|_| AppError::Transport("response channel closed".to_string()))
    }

    pub async fn send_batch(&self, request: InferenceRequest) -> Result<BatchInferenceResponse, AppError> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(AppError::Transport("worker connection closed".to_string()));
        }
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(InferRequest::Batch(request, tx))
            .map_err(|_| AppError::Transport("worker connection closed".to_string()))?;
        rx.await
            .map_err(|_| AppError::Transport("response channel closed".to_string()))
    }

    pub fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn writer_task(
    mut write_half: WriteHalf<Stream>,
    mut request_rx: mpsc::UnboundedReceiver<InferRequest>,
    pending_single: Arc<RwLock<HashMap<String, oneshot::Sender<InferenceResponse>>>>,
    pending_batch: Arc<RwLock<HashMap<String, oneshot::Sender<BatchInferenceResponse>>>>,
) {
    while let Some(req) = request_rx.recv().await {
        let uid = match &req {
            InferRequest::Single(r, _) => r.uid.clone(),
            InferRequest::Batch(r, _) => r.uid.clone(),
        };

        let encoded = match encode_request(match &req {
            InferRequest::Single(r, _) => r,
            InferRequest::Batch(r, _) => r,
        }) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to encode request: {}", e);
                match req {
                    InferRequest::Single(_, tx) => {
                        let _ = tx.send(InferenceResponse {
                            uid: uid.clone(),
                            data: None,
                            status: ResponseStatus::error(format!("encode failed: {}", e)),
                            worker_id: 0,
                            metrics: None,
                        });
                    }
                    InferRequest::Batch(_, tx) => {
                        let _ = tx.send(BatchInferenceResponse {
                            response_type: "BATCH_RESPONSE".to_string(),
                            items: vec![],
                            metrics: None,
                        });
                    }
                }
                continue;
            }
        };

        // Register pending before writing (take ownership of sender)
        match req {
            InferRequest::Single(_, tx) => {
                let mut guard = pending_single.write().await;
                guard.insert(uid.clone(), tx);
            }
            InferRequest::Batch(_, tx) => {
                let mut guard = pending_batch.write().await;
                guard.insert(uid.clone(), tx);
            }
        }

        if let Err(e) = write_frame(&mut write_half, &encoded).await {
            error!("UDS write error: {}", e);
            // Remove from pending and notify
            let err_msg = format!("UDS write failed: {}", e);
            if let Some(tx) = pending_single.write().await.remove(&uid) {
                let _ = tx.send(InferenceResponse {
                    uid: uid.clone(),
                    data: None,
                    status: ResponseStatus::error(&err_msg),
                    worker_id: 0,
                    metrics: None,
                });
            }
            if let Some(tx) = pending_batch.write().await.remove(&uid) {
                let _ = tx.send(BatchInferenceResponse {
                    response_type: "BATCH_RESPONSE".to_string(),
                    items: vec![],
                    metrics: None,
                });
            }
            break;
        }
    }
    // Connection broken: clear remaining pending requests
    clear_pending(&pending_single, &pending_batch, "UDS connection closed").await;
}

async fn reader_task(
    mut read_half: ReadHalf<Stream>,
    pending_single: Arc<RwLock<HashMap<String, oneshot::Sender<InferenceResponse>>>>,
    pending_batch: Arc<RwLock<HashMap<String, oneshot::Sender<BatchInferenceResponse>>>>,
) {
    loop {
        let frame = match read_frame(&mut read_half).await {
            Ok(v) => v,
            Err(e) => {
                error!("UDS read error: {}", e);
                break;
            }
        };

        // Try to parse as JSON Value first to detect response type
        let value: serde_json::Value = match serde_json::from_slice(&frame) {
            Ok(v) => v,
            Err(e) => {
                error!("UDS response JSON parse error: {}", e);
                continue;
            }
        };

        // Check if it's a batch response
        let is_batch = value
            .get("type")
            .and_then(|v| v.as_str())
            == Some("BATCH_RESPONSE");

        if is_batch {
            match serde_json::from_value::<BatchInferenceResponse>(value) {
                Ok(batch_resp) => {
                    let mut guard = pending_batch.write().await;
                    if let Some(tx) = guard.remove(&batch_resp.items.first().map(|i| i.uid.clone()).unwrap_or_default()) {
                        let _ = tx.send(batch_resp);
                    } else {
                        // Fallback: try to find by any uid in the batch
                        for item in &batch_resp.items {
                            if let Some(tx) = guard.remove(&item.uid) {
                                let _ = tx.send(batch_resp);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("UDS batch response deserialize error: {}", e);
                }
            }
        } else {
            match serde_json::from_value::<InferenceResponse>(value) {
                Ok(response) => {
                    let mut guard = pending_single.write().await;
                    if let Some(tx) = guard.remove(&response.uid) {
                        let _ = tx.send(response);
                    }
                }
                Err(e) => {
                    error!("UDS response deserialize error: {}", e);
                }
            }
        }
    }
    // Connection broken: clear remaining pending requests
    clear_pending(&pending_single, &pending_batch, "UDS connection closed").await;
}

// ===== Helpers =====

fn encode_request(request: &InferenceRequest) -> Result<Vec<u8>, AppError> {
    let encoded = serde_json::to_vec(request)
        .map_err(|e| AppError::Transport(format!("json serialize: {}", e)))?;
    let len = encoded.len() as u32;
    let mut buf = Vec::with_capacity(4 + encoded.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&encoded);
    Ok(buf)
}

async fn write_frame(writer: &mut WriteHalf<Stream>, data: &[u8]) -> Result<(), AppError> {
    writer
        .write_all(data)
        .await
        .map_err(|e| AppError::Transport(format!("write to UDS: {}", e)))?;
    Ok(())
}

async fn read_frame(reader: &mut ReadHalf<Stream>) -> Result<Vec<u8>, AppError> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| AppError::Transport(format!("read len from UDS: {}", e)))?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    let mut resp_buf = vec![0u8; resp_len];
    reader
        .read_exact(&mut resp_buf)
        .await
        .map_err(|e| AppError::Transport(format!("read body from UDS: {}", e)))?;
    Ok(resp_buf)
}

async fn clear_pending(
    pending_single: &Arc<RwLock<HashMap<String, oneshot::Sender<InferenceResponse>>>>,
    pending_batch: &Arc<RwLock<HashMap<String, oneshot::Sender<BatchInferenceResponse>>>>,
    message: &str,
) {
    {
        let mut guard = pending_single.write().await;
        for (_, tx) in guard.drain() {
            let _ = tx.send(InferenceResponse {
                uid: "".to_string(),
                data: None,
                status: ResponseStatus::error(message),
                worker_id: 0,
                metrics: None,
            });
        }
    }
    {
        let mut guard = pending_batch.write().await;
        for (_, tx) in guard.drain() {
            let _ = tx.send(BatchInferenceResponse {
                response_type: "BATCH_RESPONSE".to_string(),
                items: vec![],
                metrics: None,
            });
        }
    }
}

// ===== Public API =====

async fn get_or_create_connection(uds_path: &Path) -> Result<Arc<WorkerConnection>, AppError> {
    // Fast path: check pool
    {
        let guard = CONNECTION_POOL.read().await;
        if let Some(conn) = guard.get(uds_path) {
            return Ok(conn.clone());
        }
    }

    // Slow path: create new connection
    let mut guard = CONNECTION_POOL.write().await;
    if let Some(conn) = guard.get(uds_path) {
        return Ok(conn.clone());
    }

    let conn = Arc::new(WorkerConnection::new(uds_path).await?);
    guard.insert(uds_path.to_path_buf(), conn.clone());
    info!("Established persistent UDS connection to {}", uds_path.display());
    Ok(conn)
}

/// Send a single inference request using a persistent UDS connection.
pub async fn send_to_worker(
    uds_path: &Path,
    request: InferenceRequest,
) -> Result<InferenceResponse, AppError> {
    let conn = get_or_create_connection(uds_path).await?;
    conn.send_single(request).await
}

/// Send a batch inference request using a persistent UDS connection.
pub async fn send_batch_to_worker(
    uds_path: &Path,
    request: InferenceRequest,
) -> Result<BatchInferenceResponse, AppError> {
    let conn = get_or_create_connection(uds_path).await?;
    conn.send_batch(request).await
}

// ===== Legacy response consumer (kept for compatibility with existing worker startup code) =====

/// Start a background task to consume responses from a worker stream.
/// NOTE: This is legacy code for direct stream consumers. New code should use WorkerConnection.
pub fn start_response_consumer(
    stream: Stream,
    response_tx: mpsc::UnboundedSender<InferenceResponse>,
) {
    tokio::spawn(async move {
        let (mut read_half, _) = split(stream);
        loop {
            let frame = match read_frame(&mut read_half).await {
                Ok(v) => v,
                Err(e) => {
                    error!("UDS response consumer read error: {}", e);
                    break;
                }
            };
            match serde_json::from_slice::<InferenceResponse>(&frame) {
                Ok(response) => {
                    if response_tx.send(response).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    error!("UDS response deserialize error: {}", e);
                }
            }
        }
    });
}
