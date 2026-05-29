use crate::error::AppError;
use crate::proto::liteserver as pb;
use prost::Message;
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

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
const STREAM_CHANNEL_SIZE: usize = 64;

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

lazy_static::lazy_static! {
    static ref CONNECTION_POOL: RwLock<HashMap<PathBuf, Arc<WorkerConnection>>> = RwLock::new(HashMap::new());
}

pub async fn remove_connection(uds_path: &Path) {
    let mut guard = CONNECTION_POOL.write().await;
    if let Some(conn) = guard.remove(uds_path) {
        conn.close();
    }
}

pub async fn clear_connections() {
    let mut guard = CONNECTION_POOL.write().await;
    for (_, conn) in guard.drain() {
        conn.close();
    }
}

enum InferRequest {
    Single(pb::Request, oneshot::Sender<pb::Response>),
    Batch(pb::Request, oneshot::Sender<pb::BatchResponse>),
    Stream(pb::Request, String, mpsc::Sender<pb::StreamResponse>),
}

pub struct WorkerConnection {
    request_tx: mpsc::UnboundedSender<InferRequest>,
    closed: std::sync::atomic::AtomicBool,
    writer_handle: tokio::task::JoinHandle<()>,
    reader_handle: tokio::task::JoinHandle<()>,
}

impl WorkerConnection {
    pub async fn new(uds_path: &Path) -> Result<Self, AppError> {
        let stream = connect_stream(uds_path)
            .await
            .map_err(|e| AppError::Transport(format!("failed to connect to UDS {}: {}", uds_path.display(), e)))?;

        let (read_half, write_half) = split(stream);
        let (request_tx, request_rx) = mpsc::unbounded_channel::<InferRequest>();

        let pending_single: Arc<RwLock<HashMap<String, oneshot::Sender<pb::Response>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let pending_batch: Arc<RwLock<HashMap<String, oneshot::Sender<pb::BatchResponse>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let stream_routes: Arc<RwLock<HashMap<String, mpsc::Sender<pb::StreamResponse>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let writer_handle = tokio::spawn(writer_task(
            write_half,
            request_rx,
            pending_single.clone(),
            pending_batch.clone(),
            stream_routes.clone(),
        ));

        let reader_handle = tokio::spawn(reader_task(
            read_half,
            pending_single.clone(),
            pending_batch.clone(),
            stream_routes.clone(),
        ));

        Ok(Self {
            request_tx,
            closed: std::sync::atomic::AtomicBool::new(false),
            writer_handle,
            reader_handle,
        })
    }

    pub async fn send_single(&self, request: pb::Request) -> Result<pb::Response, AppError> {
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

    pub async fn send_stream(
        &self,
        request: pb::Request,
        stream_id: String,
    ) -> Result<mpsc::Receiver<pb::StreamResponse>, AppError> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(AppError::Transport("worker connection closed".to_string()));
        }
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_SIZE);
        self.request_tx
            .send(InferRequest::Stream(request, stream_id, tx))
            .map_err(|_| AppError::Transport("worker connection closed".to_string()))?;
        Ok(rx)
    }

    pub fn close(&self) {
        if !self.closed.swap(true, std::sync::atomic::Ordering::Relaxed) {
            self.writer_handle.abort();
            self.reader_handle.abort();
        }
    }
}

async fn writer_task(
    mut write_half: WriteHalf<Stream>,
    mut request_rx: mpsc::UnboundedReceiver<InferRequest>,
    pending_single: Arc<RwLock<HashMap<String, oneshot::Sender<pb::Response>>>>,
    pending_batch: Arc<RwLock<HashMap<String, oneshot::Sender<pb::BatchResponse>>>>,
    stream_routes: Arc<RwLock<HashMap<String, mpsc::Sender<pb::StreamResponse>>>>,
) {
    while let Some(req) = request_rx.recv().await {
        let uid = match &req {
            InferRequest::Single(r, _) => r.uid.clone(),
            InferRequest::Batch(r, _) => r.uid.clone(),
            InferRequest::Stream(r, sid, _) => {
                let _ = stream_routes.write().await.insert(sid.clone(), mpsc::channel(1).0);
                r.uid.clone()
            }
        };

        let encoded = match encode_request(match &req {
            InferRequest::Single(r, _) => r,
            InferRequest::Batch(r, _) => r,
            InferRequest::Stream(r, _, _) => r,
        }) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to encode request: {}", e);
                match req {
                    InferRequest::Single(_, tx) => {
                        let _ = tx.send(error_response(&uid, &format!("encode failed: {}", e)));
                    }
                    InferRequest::Batch(_, tx) => {
                        let _ = tx.send(pb::BatchResponse {
                            items: vec![],
                        });
                    }
                    InferRequest::Stream(_, sid, tx) => {
                        let _ = tx.try_send(pb::StreamResponse {
                            stream_id: sid,
                            payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                                message: format!("encode failed: {}", e),
                            })),
                        });
                    }
                }
                continue;
            }
        };

        match req {
            InferRequest::Single(_, tx) => {
                let mut guard = pending_single.write().await;
                guard.insert(uid.clone(), tx);
            }
            InferRequest::Batch(_, tx) => {
                let mut guard = pending_batch.write().await;
                guard.insert(uid.clone(), tx);
            }
            InferRequest::Stream(_, sid, tx) => {
                let mut guard = stream_routes.write().await;
                guard.insert(sid, tx);
            }
        }

        if let Err(e) = write_frame(&mut write_half, &encoded).await {
            error!("UDS write error: {}", e);
            let err_msg = format!("UDS write failed: {}", e);
            if let Some(tx) = pending_single.write().await.remove(&uid) {
                let _ = tx.send(error_response(&uid, &err_msg));
            }
            if let Some(tx) = pending_batch.write().await.remove(&uid) {
                let _ = tx.send(pb::BatchResponse {
                    items: vec![],
                });
            }
            break;
        }
    }
    clear_pending(&pending_single, &pending_batch, "UDS connection closed").await;
}

async fn reader_task(
    mut read_half: ReadHalf<Stream>,
    pending_single: Arc<RwLock<HashMap<String, oneshot::Sender<pb::Response>>>>,
    pending_batch: Arc<RwLock<HashMap<String, oneshot::Sender<pb::BatchResponse>>>>,
    stream_routes: Arc<RwLock<HashMap<String, mpsc::Sender<pb::StreamResponse>>>>,
) {
    loop {
        let frame = match read_frame(&mut read_half).await {
            Ok(v) => v,
            Err(e) => {
                error!("UDS read error: {}", e);
                break;
            }
        };

        let value: pb::Response = match pb::Response::decode(frame.as_slice()) {
            Ok(v) => v,
            Err(e) => {
                error!("UDS response protobuf decode error: {}", e);
                continue;
            }
        };

        if let Some(pb::response::Payload::Stream(ref stream_resp)) = value.payload {
            let sid = &stream_resp.stream_id;
            if let Some(tx) = stream_routes.write().await.remove(sid) {
                let is_done = matches!(stream_resp.payload, Some(pb::stream_response::Payload::Done(_)));
                let is_error = matches!(stream_resp.payload, Some(pb::stream_response::Payload::Error(_)));
                if tx.try_send(stream_resp.clone()).is_err() {
                    warn!("Stream channel closed for {}", sid);
                }
                if !is_done && !is_error {
                    let mut guard = stream_routes.write().await;
                    guard.insert(sid.clone(), tx);
                }
            }
            continue;
        }

        let is_batch = value
            .payload
            .as_ref()
            .and_then(|p| match p {
                pb::response::Payload::Batch(_) => Some(true),
                _ => None,
            })
            .is_some();

        if is_batch {
            match value.payload {
                Some(pb::response::Payload::Batch(batch_resp)) => {
                    let mut guard = pending_batch.write().await;
                    if let Some(tx) = guard.remove(&batch_resp.items.first().map(|i| i.uid.clone()).unwrap_or_default()) {
                        let _ = tx.send(pb::BatchResponse {
                            items: batch_resp.items,
                        });
                    }
                }
                _ => {}
            }
        } else {
            let mut guard = pending_single.write().await;
            if let Some(tx) = guard.remove(&value.uid) {
                let _ = tx.send(value);
            }
        }
    }
    clear_pending(&pending_single, &pending_batch, "UDS connection closed").await;
}

fn encode_request(request: &pb::Request) -> Result<Vec<u8>, AppError> {
    let encoded = request.encode_to_vec();
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

    if resp_len > MAX_FRAME_SIZE {
        return Err(AppError::FrameTooLarge);
    }

    let mut resp_buf = vec![0u8; resp_len];
    reader
        .read_exact(&mut resp_buf)
        .await
        .map_err(|e| AppError::Transport(format!("read body from UDS: {}", e)))?;
    Ok(resp_buf)
}

fn error_response(uid: &str, message: &str) -> pb::Response {
    pb::Response {
        uid: uid.to_string(),
        payload: Some(pb::response::Payload::Single(pb::SingleResponse {
            data: vec![],
            status: Some(pb::Status {
                code: "Error".to_string(),
                message: message.to_string(),
            }),
        })),
        metrics: None,
    }
}

async fn clear_pending(
    pending_single: &Arc<RwLock<HashMap<String, oneshot::Sender<pb::Response>>>>,
    pending_batch: &Arc<RwLock<HashMap<String, oneshot::Sender<pb::BatchResponse>>>>,
    message: &str,
) {
    {
        let mut guard = pending_single.write().await;
        for (_, tx) in guard.drain() {
            let _ = tx.send(error_response("", message));
        }
    }
    {
        let mut guard = pending_batch.write().await;
        for (_, tx) in guard.drain() {
            let _ = tx.send(pb::BatchResponse {
                items: vec![],
            });
        }
    }
}

pub async fn send_to_worker(
    uds_path: &Path,
    request: pb::Request,
) -> Result<pb::Response, AppError> {
    let conn = get_or_create_connection(uds_path).await?;
    conn.send_single(request).await
}

async fn get_or_create_connection(uds_path: &Path) -> Result<Arc<WorkerConnection>, AppError> {
    {
        let guard = CONNECTION_POOL.read().await;
        if let Some(conn) = guard.get(uds_path) {
            return Ok(conn.clone());
        }
    }
    let mut guard = CONNECTION_POOL.write().await;
    if let Some(conn) = guard.get(uds_path) {
        return Ok(conn.clone());
    }
    let conn = Arc::new(WorkerConnection::new(uds_path).await?);
    guard.insert(uds_path.to_path_buf(), conn.clone());
    info!("Established persistent UDS connection to {}", uds_path.display());
    Ok(conn)
}
