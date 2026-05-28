use crate::error::AppError;
use crate::proto::liteserver as pb;
use prost::Message;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

/// Internal command sent to the ZMQ blocking thread.
struct ZmqCommand {
    request: pb::Request,
    response_tx: oneshot::Sender<pb::Response>,
}

/// A ZMQ PAIR client for a single Python worker.
///
/// Each worker gets one `WorkerZmqClient` with a dedicated PAIR socket.
/// The socket runs in a `spawn_blocking` task; the async side communicates
/// via channels.
pub struct WorkerZmqClient {
    cmd_tx: mpsc::Sender<ZmqCommand>,
}

impl WorkerZmqClient {
    /// Create a new ZMQ client and spawn the background blocking task.
    pub fn new(endpoint: String) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ZmqCommand>(128);

        tokio::task::spawn_blocking(move || {
            let ctx = zmq::Context::new();
            let socket = match ctx.socket(zmq::PAIR) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create ZMQ PAIR socket: {}", e);
                    return;
                }
            };

            if let Err(e) = socket.set_rcvtimeo(100) {
                error!("Failed to set ZMQ rcvtimeout: {}", e);
                return;
            }
            if let Err(e) = socket.set_sndtimeo(1000) {
                error!("Failed to set ZMQ sndtimeout: {}", e);
                return;
            }
            if let Err(e) = socket.set_linger(0) {
                error!("Failed to set ZMQ linger: {}", e);
                return;
            }

            if let Err(e) = socket.bind(&endpoint) {
                error!("Failed to bind ZMQ PAIR to {}: {}", endpoint, e);
                return;
            }

            info!("ZMQ PAIR worker socket bound to {}", endpoint);

            // Local pending map inside the blocking thread
            let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();

            loop {
                // ---- 1. Drain async command channel (non-blocking) ----
                loop {
                    match cmd_rx.try_recv() {
                        Ok(cmd) => {
                            let uid = cmd.request.uid.clone();
                            let bytes = cmd.request.encode_to_vec();
                            match socket.send(&bytes, 0) {
                                Ok(_) => {
                                    pending.insert(uid, cmd.response_tx);
                                }
                                Err(e) => {
                                    error!("ZMQ send error: {}", e);
                                    let _ = cmd.response_tx.send(pb::Response {
                                        uid: uid.clone(),
                                        payload: Some(pb::response::Payload::Single(
                                            pb::SingleResponse {
                                                data: vec![],
                                                status: Some(pb::Status {
                                                    code: "Error".to_string(),
                                                    message: format!("ZMQ send: {}", e),
                                                }),
                                            },
                                        )),
                                        metrics: None,
                                    });
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            info!("ZMQ command channel closed, draining pending and shutting down");
                            for (uid, tx) in pending.drain() {
                                let _ = tx.send(pb::Response {
                                    uid,
                                    payload: Some(pb::response::Payload::Single(
                                        pb::SingleResponse {
                                            data: vec![],
                                            status: Some(pb::Status {
                                                code: "Error".to_string(),
                                                message: "Worker shutting down".to_string(),
                                            }),
                                        },
                                    )),
                                    metrics: None,
                                });
                            }
                            return;
                        }
                    }
                }

                // ---- 2. Receive responses (non-blocking) ----
                match socket.recv_bytes(zmq::DONTWAIT) {
                    Ok(bytes) => {
                        match pb::Response::decode(bytes.as_slice()) {
                            Ok(resp) => {
                                // Handle CB completed: multiple uids in one response
                                if let Some(pb::response::Payload::CbCompleted(ref cb)) = resp.payload {
                                    for seq in &cb.sequences {
                                        if let Some(tx) = pending.remove(&seq.uid) {
                                            let single_resp = pb::Response {
                                                uid: seq.uid.clone(),
                                                payload: Some(pb::response::Payload::Single(
                                                    pb::SingleResponse {
                                                        data: seq.data.clone(),
                                                        status: Some(pb::Status {
                                                            code: "Ok".to_string(),
                                                            message: "".to_string(),
                                                        }),
                                                    },
                                                )),
                                                metrics: resp.metrics.clone(),
                                            };
                                            let _ = tx.send(single_resp);
                                        }
                                    }
                                } else if let Some(tx) = pending.remove(&resp.uid) {
                                    let _ = tx.send(resp);
                                } else {
                                    warn!("Received response for unknown uid: {}", resp.uid);
                                }
                            }
                            Err(e) => {
                                error!("Protobuf decode error: {}", e);
                            }
                        }
                    }
                    Err(zmq::Error::EAGAIN) => {
                        // No message available, yield to reduce latency
                        std::thread::yield_now();
                    }
                    Err(e) => {
                        error!("ZMQ recv error: {}", e);
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        });

        Self { cmd_tx }
    }

    /// Send a request and wait for the response.
    pub async fn send(&self, request: pb::Request) -> Result<pb::Response, AppError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ZmqCommand {
            request,
            response_tx,
        };

        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| AppError::Transport("ZMQ command channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| AppError::Transport("ZMQ response channel closed".to_string()))
    }
}
