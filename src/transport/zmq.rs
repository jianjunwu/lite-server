use crate::error::AppError;
use crate::proto::liteserver as pb;
use prost::Message;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

const ZMQ_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
const STREAM_CHANNEL_SIZE: usize = 64;

enum ZmqCommand {
    Unary {
        request: pb::Request,
        response_tx: oneshot::Sender<pb::Response>,
    },
    Stream {
        request: pb::Request,
        chunk_tx: mpsc::Sender<pb::StreamResponse>,
        stream_id: String,
    },
}

pub struct WorkerZmqClient {
    cmd_tx: mpsc::Sender<ZmqCommand>,
}

impl WorkerZmqClient {
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

            let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
            let mut stream_routes: HashMap<String, mpsc::Sender<pb::StreamResponse>> = HashMap::new();

            loop {
                loop {
                    match cmd_rx.try_recv() {
                        Ok(ZmqCommand::Unary { request, response_tx }) => {
                            let uid = request.uid.clone();
                            let bytes = request.encode_to_vec();
                            match socket.send(&bytes, 0) {
                                Ok(_) => {
                                    pending.insert(uid, response_tx);
                                }
                                Err(e) => {
                                    error!("ZMQ send error: {}", e);
                                    let _ = response_tx.send(error_response(&uid, &format!("ZMQ send: {}", e)));
                                }
                            }
                        }
                        Ok(ZmqCommand::Stream { request, chunk_tx, stream_id }) => {
                            let bytes = request.encode_to_vec();
                            match socket.send(&bytes, 0) {
                                Ok(_) => {
                                    stream_routes.insert(stream_id, chunk_tx);
                                }
                                Err(e) => {
                                    error!("ZMQ stream send error: {}", e);
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            info!("ZMQ command channel closed, draining pending and shutting down");
                            for (uid, tx) in pending.drain() {
                                let _ = tx.send(error_response(&uid, "Worker shutting down"));
                            }
                            stream_routes.clear();
                            drop(socket);
                            drop(ctx);
                            return;
                        }
                    }
                }

                // Use zmq::poll with 100ms timeout instead of DONTWAIT busy-loop.
                // Cross-platform: zmq::poll works on all supported ZMQ backends.
                match socket.poll(zmq::POLLIN, 100) {
                    Ok(_) => {
                        match socket.recv_bytes(0) {
                            Ok(bytes) => {
                                match pb::Response::decode(bytes.as_slice()) {
                                    Ok(resp) => {
                                        if let Some(pb::response::Payload::CbCompleted(ref cb)) = resp.payload {
                                            for seq in &cb.sequences {
                                                if let Some(tx) = pending.remove(&seq.uid) {
                                                    let single_resp = pb::Response {
                                                        uid: seq.uid.clone(),
                                                        payload: Some(pb::response::Payload::Single(
                                                            pb::SingleResponse {
                                                                data: seq.data.clone(),
                                                                headers: HashMap::new(),
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
                                        } else if let Some(pb::response::Payload::Stream(ref stream_resp)) = resp.payload {
                                            let sid = &stream_resp.stream_id;
                                            if let Some(tx) = stream_routes.get(sid) {
                                                let is_done = matches!(stream_resp.payload, Some(pb::stream_response::Payload::Done(_)));
                                                let is_error = matches!(stream_resp.payload, Some(pb::stream_response::Payload::Error(_)));
                                                if tx.try_send(stream_resp.clone()).is_err() {
                                                    warn!("Stream channel full or closed for {}", sid);
                                                    stream_routes.remove(sid);
                                                } else if is_done || is_error {
                                                    stream_routes.remove(sid);
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
                                // poll signaled readable but recv got EAGAIN — rare, retry
                                continue;
                            }
                            Err(e) => {
                                error!("ZMQ recv error: {}", e);
                                std::thread::sleep(Duration::from_millis(10));
                            }
                        }
                    }
                    Err(e) => {
                        error!("ZMQ poll error: {}", e);
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });

        Self { cmd_tx }
    }

    pub async fn send(&self, request: pb::Request) -> Result<pb::Response, AppError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ZmqCommand::Unary {
            request,
            response_tx,
        };

        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| AppError::Transport("ZMQ command channel closed".to_string()))?;

        tokio::time::timeout(ZMQ_RESPONSE_TIMEOUT, response_rx)
            .await
            .map_err(|_| AppError::InferenceTimeout("ZMQ response timeout".to_string()))?
            .map_err(|_| AppError::Transport("ZMQ response channel closed".to_string()))
    }

    pub async fn send_stream(
        &self,
        request: pb::Request,
        stream_id: String,
    ) -> Result<mpsc::Receiver<pb::StreamResponse>, AppError> {
        let (chunk_tx, chunk_rx) = mpsc::channel(STREAM_CHANNEL_SIZE);
        let cmd = ZmqCommand::Stream {
            request,
            chunk_tx,
            stream_id,
        };

        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| AppError::Transport("ZMQ command channel closed".to_string()))?;

        Ok(chunk_rx)
    }
}

fn error_response(uid: &str, message: &str) -> pb::Response {
    pb::Response {
        uid: uid.to_string(),
        payload: Some(pb::response::Payload::Single(pb::SingleResponse {
            data: Default::default(),
            headers: HashMap::new(),
            status: Some(pb::Status {
                code: "Error".to_string(),
                message: message.to_string(),
            }),
        })),
        metrics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_zmq_client_cleanup_does_not_panic() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!("lite-server-zmq-test-{}.sock", std::process::id()));
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 31000 + std::process::id() % 1000);
        let client = WorkerZmqClient::new(endpoint.clone());
        drop(client);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn test_zmq_client_send_fails_when_no_peer() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!("lite-server-zmq-test-timeout-{}.sock", std::process::id()));
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 32000 + std::process::id() % 1000);
        let client = WorkerZmqClient::new(endpoint.clone());

        let request = pb::Request {
            uid: "test-uid".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: bytes::Bytes::from(vec![1, 2, 3]),
            })),
        };

        let result = client.send(request).await;
        assert!(result.is_ok(), "send() should return Ok with error payload");
        let resp = result.unwrap();
        if let Some(pb::response::Payload::Single(single)) = resp.payload {
            assert_eq!(single.status.unwrap().code, "Error");
        } else {
            panic!("Expected Single response with error status");
        }
    }
}
