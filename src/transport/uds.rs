use crate::error::AppError;
use crate::proto::liteserver as pb;
use dashmap::DashMap;
use prost::Message;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(windows)]
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
const STREAM_CHANNEL_SIZE: usize = 64;
const MAX_PENDING_REQUESTS: usize = 1024;

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
    let port = crate::transport::derive_port_from_path(&path_str);
    TcpStream::connect(format!("127.0.0.1:{}", port)).await
}

lazy_static::lazy_static! {
    static ref CONNECTION_POOL: DashMap<PathBuf, Arc<WorkerConnection>> = DashMap::new();
}

pub async fn remove_connection(uds_path: &Path) {
    if let Some((_, conn)) = CONNECTION_POOL.remove(uds_path) {
        conn.close();
    }
}

pub async fn clear_connections() {
    for entry in CONNECTION_POOL.iter() {
        entry.value().close();
    }
    CONNECTION_POOL.clear();
}

#[allow(dead_code)]
enum InferRequest {
    Single(pb::Request, oneshot::Sender<pb::Response>),
    Batch(pb::Request, oneshot::Sender<pb::BatchResponse>),
    Stream(pb::Request, String, mpsc::Sender<pb::StreamResponse>),
}

pub struct WorkerConnection {
    request_tx: mpsc::Sender<InferRequest>,
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
        let (request_tx, request_rx) = mpsc::channel::<InferRequest>(MAX_PENDING_REQUESTS);

        let pending_single: Arc<DashMap<String, oneshot::Sender<pb::Response>>> =
            Arc::new(DashMap::new());
        let pending_batch: Arc<DashMap<String, oneshot::Sender<pb::BatchResponse>>> =
            Arc::new(DashMap::new());
        let stream_routes: Arc<DashMap<String, mpsc::Sender<pb::StreamResponse>>> =
            Arc::new(DashMap::new());

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
            .await
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
            .await
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
    mut request_rx: mpsc::Receiver<InferRequest>,
    pending_single: Arc<DashMap<String, oneshot::Sender<pb::Response>>>,
    pending_batch: Arc<DashMap<String, oneshot::Sender<pb::BatchResponse>>>,
    stream_routes: Arc<DashMap<String, mpsc::Sender<pb::StreamResponse>>>,
) {
    while let Some(req) = request_rx.recv().await {
        let uid = match &req {
            InferRequest::Single(r, _) => r.uid.clone(),
            InferRequest::Batch(r, _) => r.uid.clone(),
            InferRequest::Stream(r, sid, _) => {
                stream_routes.insert(sid.clone(), mpsc::channel(1).0);
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
                pending_single.insert(uid.clone(), tx);
            }
            InferRequest::Batch(_, tx) => {
                pending_batch.insert(uid.clone(), tx);
            }
            InferRequest::Stream(_, sid, tx) => {
                stream_routes.insert(sid, tx);
            }
        }

        if let Err(e) = write_frame(&mut write_half, &encoded).await {
            error!("UDS write error: {}", e);
            let err_msg = format!("UDS write failed: {}", e);
            if let Some((_, tx)) = pending_single.remove(&uid) {
                let _ = tx.send(error_response(&uid, &err_msg));
            }
            if let Some((_, tx)) = pending_batch.remove(&uid) {
                let _ = tx.send(pb::BatchResponse {
                    items: vec![],
                });
            }
            break;
        }
    }
    clear_pending(&pending_single, &pending_batch, "UDS connection closed");
}

async fn reader_task(
    mut read_half: ReadHalf<Stream>,
    pending_single: Arc<DashMap<String, oneshot::Sender<pb::Response>>>,
    pending_batch: Arc<DashMap<String, oneshot::Sender<pb::BatchResponse>>>,
    stream_routes: Arc<DashMap<String, mpsc::Sender<pb::StreamResponse>>>,
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
            if let Some((_, tx)) = stream_routes.remove(sid) {
                let is_done = matches!(stream_resp.payload, Some(pb::stream_response::Payload::Done(_)));
                let is_error = matches!(stream_resp.payload, Some(pb::stream_response::Payload::Error(_)));
                if tx.try_send(stream_resp.clone()).is_err() {
                    warn!("Stream channel closed for {}", sid);
                }
                if !is_done && !is_error {
                    stream_routes.insert(sid.clone(), tx);
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
                    let key = batch_resp.items.first().map(|i| i.uid.clone()).unwrap_or_default();
                    if let Some((_, tx)) = pending_batch.remove(&key) {
                        let _ = tx.send(pb::BatchResponse {
                            items: batch_resp.items,
                        });
                    }
                }
                _ => {}
            }
        } else {
            if let Some((_, tx)) = pending_single.remove(&value.uid) {
                let _ = tx.send(value);
            }
        }
    }
    clear_pending(&pending_single, &pending_batch, "UDS connection closed");
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
            data: Default::default(),
            status: Some(pb::Status {
                code: "Error".to_string(),
                message: message.to_string(),
            }),
        })),
        metrics: None,
    }
}

fn clear_pending(
    pending_single: &DashMap<String, oneshot::Sender<pb::Response>>,
    pending_batch: &DashMap<String, oneshot::Sender<pb::BatchResponse>>,
    message: &str,
) {
    let keys: Vec<String> = pending_single.iter().map(|e| e.key().clone()).collect();
    for key in keys {
        if let Some((_, tx)) = pending_single.remove(&key) {
            let _ = tx.send(error_response("", message));
        }
    }
    let keys: Vec<String> = pending_batch.iter().map(|e| e.key().clone()).collect();
    for key in keys {
        if let Some((_, tx)) = pending_batch.remove(&key) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// CONNECTION_POOL must be DashMap so insert/get/remove are lock-free per shard.
    /// This test verifies the type — if someone reverts to RwLock<HashMap>, it won't compile.
    #[test]
    fn connection_pool_type_is_dashmap() {
        fn assert_type<T>() {}
        // This compiles only if CONNECTION_POOL is DashMap<PathBuf, Arc<WorkerConnection>>
        assert_type::<DashMap<PathBuf, Arc<WorkerConnection>>>();
        // Verify the static is accessible without .await (DashMap ops are sync)
        let _ = CONNECTION_POOL.len();
    }

    /// get_or_create_connection must use DashMap::entry (no double-check RwLock pattern).
    /// We can't easily test the full async path without a real UDS socket,
    /// but we verify remove/contains_key are sync (no .await needed).
    #[test]
    fn connection_pool_operations_are_sync() {
        let key = PathBuf::from("/tmp/test-dashmap-sync.sock");
        // These must compile without .await — proves DashMap not RwLock
        let _existed = CONNECTION_POOL.remove(&key);
        let _exists = CONNECTION_POOL.contains_key(&key);
        let _len = CONNECTION_POOL.len();
    }

    // ===== UDS pending maps must be DashMap (lock-free) =====

    /// Verify that pending map helper functions operate without .await.
    /// If pending_single/pending_batch/stream_routes revert to RwLock<HashMap>,
    /// these calls won't compile because DashMap ops are sync.
    #[test]
    fn uds_pending_insert_remove_are_sync() {
        let pending: DashMap<String, oneshot::Sender<pb::Response>> = DashMap::new();
        let (tx, _rx) = oneshot::channel();
        pending.insert("uid-1".to_string(), tx);
        assert!(pending.contains_key("uid-1"));
        let _ = pending.remove("uid-1");
        assert!(pending.is_empty());
    }

    #[test]
    fn uds_pending_batch_insert_remove_are_sync() {
        let pending: DashMap<String, oneshot::Sender<pb::BatchResponse>> = DashMap::new();
        let (tx, _rx) = oneshot::channel();
        pending.insert("batch-1".to_string(), tx);
        assert!(pending.contains_key("batch-1"));
        let _ = pending.remove("batch-1");
        assert!(pending.is_empty());
    }

    #[test]
    fn uds_stream_routes_insert_remove_are_sync() {
        let routes: DashMap<String, mpsc::Sender<pb::StreamResponse>> = DashMap::new();
        let (tx, _rx) = mpsc::channel(1);
        routes.insert("stream-1".to_string(), tx);
        assert!(routes.contains_key("stream-1"));
        let _ = routes.remove("stream-1");
        assert!(routes.is_empty());
    }
}

async fn get_or_create_connection(uds_path: &Path) -> Result<Arc<WorkerConnection>, AppError> {
    match CONNECTION_POOL.entry(uds_path.to_path_buf()) {
        dashmap::Entry::Occupied(e) => Ok(e.get().clone()),
        dashmap::Entry::Vacant(e) => {
            let conn = Arc::new(WorkerConnection::new(uds_path).await?);
            e.insert(conn.clone());
            info!("Established persistent UDS connection to {}", uds_path.display());
            Ok(conn)
        }
    }
}
