use crate::error::AppError;
use crate::proto::liteserver as pb;
use prost::Message;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

/// Backstop response timeout applied by [`WorkerZmqClient::send`] when the
/// caller has no timeout of its own (e.g. request_timeout disabled). Must be
/// large enough to never silently cap a user-configured request_timeout —
/// callers with a configured timeout use [`WorkerZmqClient::send_with_timeout`]
/// instead. Health checks wrap `send` with their own shorter
/// outer timeouts and are unaffected by this value.
pub(crate) const ZMQ_RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);
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
    /// Fire-and-forget send: emit `request` with no pending reply slot.
    ///
    /// Used for bidirectional stream chunks/closes, whose responses come back
    /// later as `StreamResponse`s routed through the stream's registered
    /// channel (set up at open).  A plain `send()` here would deadlock for
    /// `ZMQ_RESPONSE_TIMEOUT` waiting for a unary reply that never matches.
    Raw {
        request: pb::Request,
    },
    /// Custom-route call whose reply shape is not known up front: a plain
    /// handler result answers with one `SingleResponse`, a `StreamingResponse`
    /// result answers with a start→chunks→done `StreamResponse` sequence.
    /// Registers both a pending unary slot and a stream route under the
    /// request uid; whichever shape arrives uses its channel, the other is
    /// cleaned up on arrival (or by the pending sweep if never used).
    RouteOrStream {
        request: pb::Request,
        response_tx: oneshot::Sender<pb::Response>,
        chunk_tx: mpsc::Sender<pb::StreamResponse>,
    },
}

pub struct WorkerZmqClient {
    cmd_tx: mpsc::Sender<ZmqCommand>,
    /// Wake-signal socket paired with the actor's poll set: a single byte
    /// interrupts the actor's blocking poll so newly queued commands are
    /// drained immediately instead of at the next backstop tick. `None`
    /// only if inproc pair creation failed — the actor then relies on the
    /// backstop tick (correct, just slower).
    wake_tx: Option<std::sync::Arc<std::sync::Mutex<zmq::Socket>>>,
}

impl WorkerZmqClient {
    pub fn new(endpoint: String) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ZmqCommand>(128);

        // inproc wake pair sharing the actor's context: senders poke it
        // after queueing a command so the actor's poll wakes immediately
        // instead of sleeping out its tick (the c=1 ~200ms/req idle-cycle
        // bug: try_recv + poll(100ms) + rcvtimeo(100ms) made a 200ms cycle
        // that serial requests always paid in full).
        static WAKE_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let ctx = zmq::Context::new();
        let wake_endpoint = format!(
            "inproc://lite-zmq-wake-{}-{}",
            std::process::id(),
            WAKE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let wake_pair = (|| {
            let rx = ctx.socket(zmq::PAIR).ok()?;
            rx.bind(&wake_endpoint).ok()?;
            rx.set_linger(0).ok()?;
            let tx = ctx.socket(zmq::PAIR).ok()?;
            tx.connect(&wake_endpoint).ok()?;
            tx.set_linger(0).ok()?;
            Some((rx, tx))
        })();
        let (wake_rx, wake_tx) = match wake_pair {
            Some((rx, tx)) => (Some(rx), Some(std::sync::Arc::new(std::sync::Mutex::new(tx)))),
            None => {
                error!("Failed to create ZMQ wake pair; commands will wait for the backstop poll tick");
                (None, None)
            }
        };

        tokio::task::spawn_blocking(move || {
            let socket = match ctx.socket(zmq::PAIR) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create ZMQ PAIR socket: {}", e);
                    return;
                }
            };

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
            let mut last_pending_sweep = Instant::now();

            loop {
                if matches!(
                    drain_commands(&mut cmd_rx, &socket, &mut pending, &mut stream_routes),
                    DrainOutcome::Shutdown
                ) {
                    info!("ZMQ command channel closed, draining pending and shutting down");
                    for (uid, tx) in pending.drain() {
                        let _ = tx.send(error_response(&uid, "Worker shutting down"));
                    }
                    stream_routes.clear();
                    drop(socket);
                    drop(ctx);
                    return;
                }

                // Block until the worker has data or a sender wakes us for
                // newly queued commands. The timeout is only a backstop for
                // a lost wake byte and for shutdown detection — the wake
                // socket provides the fast path.
                let (poll_result, worker_fired) = match &wake_rx {
                    Some(wrx) => {
                        let mut items = [
                            socket.as_poll_item(zmq::POLLIN),
                            wrx.as_poll_item(zmq::POLLIN),
                        ];
                        let r = zmq::poll(&mut items, 200);
                        if r.is_ok() && items[1].get_revents().contains(zmq::POLLIN) {
                            // Drain wake bytes; the commands they signaled
                            // are drained at the top of the loop.
                            while wrx.recv_bytes(zmq::DONTWAIT).is_ok() {}
                        }
                        (r, items[0].get_revents().contains(zmq::POLLIN))
                    }
                    None => {
                        let mut items = [socket.as_poll_item(zmq::POLLIN)];
                        let r = zmq::poll(&mut items, 200);
                        (r, items[0].get_revents().contains(zmq::POLLIN))
                    }
                };

                match poll_result {
                    Ok(_) => {
                        if worker_fired {
                            match socket.recv_bytes(zmq::DONTWAIT) {
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

                                                                ..Default::default()
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
                                            // A streaming route reply leaves its
                                            // unused unary slot behind — free it
                                            // once the stream terminates.
                                            if matches!(stream_resp.payload,
                                                Some(pb::stream_response::Payload::Done(_))
                                                | Some(pb::stream_response::Payload::Error(_)))
                                            {
                                                pending.remove(sid);
                                            }
                                        } else if let Some(tx) = pending.remove(&resp.uid) {
                                            // A unary route reply leaves its
                                            // unused stream route behind.
                                            stream_routes.remove(&resp.uid);
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
                                    // poll signaled readable but recv got EAGAIN — rare
                                }
                                Err(e) => {
                                    error!("ZMQ recv error: {}", e);
                                    std::thread::sleep(Duration::from_millis(10));
                                }
                            }
                        }
                        // Ok(0) backstop timeout: fall through to the sweep.
                    }
                    Err(e) => {
                        error!("ZMQ poll error: {}", e);
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }

                // #5: periodically evict pending senders whose caller already
                // timed out and dropped the receiver, so a worker that never
                // replies can't leak entries until disconnect. The backstop
                // tick keeps this running when idle; gating on elapsed time
                // bounds sweep cost.
                if last_pending_sweep.elapsed() >= Duration::from_secs(5) {
                    let removed = sweep_dead_pending(&mut pending);
                    if removed > 0 {
                        warn!("Swept {} orphaned pending ZMQ response(s)", removed);
                    }
                    last_pending_sweep = Instant::now();
                }
            }
        });

        Self { cmd_tx, wake_tx }
    }

    pub async fn send(&self, request: pb::Request) -> Result<pb::Response, AppError> {
        self.send_with_timeout(request, ZMQ_RESPONSE_TIMEOUT).await
    }

    /// Like [`send`], but the response wait is bounded by `timeout` instead of
    /// the [`ZMQ_RESPONSE_TIMEOUT`] backstop.
    pub async fn send_with_timeout(
        &self,
        request: pb::Request,
        timeout: Duration,
    ) -> Result<pb::Response, AppError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ZmqCommand::Unary {
            request,
            response_tx,
        };

        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| AppError::Transport("ZMQ command channel closed".to_string()))?;
        self.wake_actor();

        tokio::time::timeout(timeout, response_rx)
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
        self.wake_actor();

        Ok(chunk_rx)
    }

    /// Send a custom-route call whose reply shape is decided by the handler:
    /// a plain result comes back on the oneshot (one `SingleResponse`), a
    /// `StreamingResponse` result comes back as stream frames on the channel.
    /// Exactly one of the two produces output.
    pub async fn send_route_or_stream(
        &self,
        request: pb::Request,
    ) -> Result<(oneshot::Receiver<pb::Response>, mpsc::Receiver<pb::StreamResponse>), AppError> {
        let (response_tx, response_rx) = oneshot::channel();
        let (chunk_tx, chunk_rx) = mpsc::channel(STREAM_CHANNEL_SIZE);
        self.cmd_tx
            .send(ZmqCommand::RouteOrStream { request, response_tx, chunk_tx })
            .await
            .map_err(|_| AppError::Transport("ZMQ command channel closed".to_string()))?;
        self.wake_actor();
        Ok((response_rx, chunk_rx))
    }

    /// Fire-and-forget send: deliver `request` without awaiting a reply.
    ///
    /// For bidirectional stream chunks/closes — their responses arrive as
    /// `StreamResponse`s on the stream's registered channel, so there is no
    /// unary reply to wait for.  See `ZmqCommand::Raw`.
    pub async fn send_raw(&self, request: pb::Request) -> Result<(), AppError> {
        self.cmd_tx
            .send(ZmqCommand::Raw { request })
            .await
            .map_err(|_| AppError::Transport("ZMQ command channel closed".to_string()))?;
        self.wake_actor();
        Ok(())
    }

    /// Interrupt the actor's blocking poll so it drains the command channel
    /// now instead of at the next backstop tick. Best-effort: a full wake
    /// pipe means the actor is already awake.
    fn wake_actor(&self) {
        if let Some(wake) = &self.wake_tx {
            // Poison recovery: a panic while holding the lock must not
            // disable waking — matches ShutdownState's precedent.
            let guard = wake.lock().unwrap_or_else(|e| e.into_inner());
            let _ = guard.send(b"\0".as_ref(), zmq::DONTWAIT);
        }
    }
}

impl Drop for WorkerZmqClient {
    fn drop(&mut self) {
        // Close the command channel BEFORE waking the actor. The actor
        // shuts down when its drain observes Disconnected; waking while the
        // channel is still open (field-drop order) races — an actor that
        // drains in that window re-enters poll and only notices the close
        // at the next 200ms backstop tick.
        let (dummy_tx, dummy_rx) = mpsc::channel::<ZmqCommand>(1);
        drop(dummy_rx);
        drop(std::mem::replace(&mut self.cmd_tx, dummy_tx));
        self.wake_actor();
    }
}

enum DrainOutcome {
    Continue,
    Shutdown,
}

/// Drain every queued command, sending each on the worker socket.
/// Returns [`DrainOutcome::Shutdown`] when the command channel is closed.
fn drain_commands(
    cmd_rx: &mut mpsc::Receiver<ZmqCommand>,
    socket: &zmq::Socket,
    pending: &mut HashMap<String, oneshot::Sender<pb::Response>>,
    stream_routes: &mut HashMap<String, mpsc::Sender<pb::StreamResponse>>,
) -> DrainOutcome {
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
            Ok(ZmqCommand::Raw { request }) => {
                let bytes = request.encode_to_vec();
                if let Err(e) = socket.send(&bytes, 0) {
                    error!("ZMQ raw send error: {}", e);
                }
            }
            Ok(ZmqCommand::RouteOrStream { request, response_tx, chunk_tx }) => {
                let uid = request.uid.clone();
                let bytes = request.encode_to_vec();
                match socket.send(&bytes, 0) {
                    Ok(_) => {
                        pending.insert(uid.clone(), response_tx);
                        stream_routes.insert(uid, chunk_tx);
                    }
                    Err(e) => {
                        error!("ZMQ send error: {}", e);
                        let _ = response_tx.send(error_response(&uid, &format!("ZMQ send: {}", e)));
                    }
                }
            }
            Err(mpsc::error::TryRecvError::Empty) => return DrainOutcome::Continue,
            Err(mpsc::error::TryRecvError::Disconnected) => return DrainOutcome::Shutdown,
        }
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
        
            ..Default::default()
        })),
        metrics: None,
    }
}

/// Drop pending-response senders whose receiver has gone away — i.e. the
/// caller already timed out and dropped `response_rx`. Such entries otherwise
/// linger in the map until the worker finally replies or the whole client is
/// dropped (#5). Returns the number evicted.
fn sweep_dead_pending(pending: &mut HashMap<String, oneshot::Sender<pb::Response>>) -> usize {
    let before = pending.len();
    pending.retain(|_, tx| !tx.is_closed());
    before - pending.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_dead_pending_removes_senders_whose_receiver_dropped() {
        // #5: a request whose caller timed out (response_rx dropped) leaves a
        // dead oneshot::Sender in the pending map. Its is_closed() is true, so
        // a periodic sweep must evict it while keeping live senders intact.
        let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
        let (live_tx, _live_rx) = oneshot::channel();
        let (dead_tx, dead_rx) = oneshot::channel();
        drop(dead_rx);
        pending.insert("live".to_string(), live_tx);
        pending.insert("dead".to_string(), dead_tx);

        let removed = sweep_dead_pending(&mut pending);
        assert_eq!(removed, 1);
        assert!(pending.contains_key("live"));
        assert!(!pending.contains_key("dead"));
    }

    #[test]
    fn sweep_dead_pending_keeps_all_when_all_live() {
        let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        pending.insert("a".to_string(), tx1);
        pending.insert("b".to_string(), tx2);

        let removed = sweep_dead_pending(&mut pending);
        assert_eq!(removed, 0);
        assert_eq!(pending.len(), 2);
    }

    #[tokio::test]
    async fn drop_client_releases_endpoint_before_backstop_tick() {
        // Regression: Drop used to wake the actor BEFORE the command
        // channel closed (field-drop order), so the actor drained nothing,
        // re-entered its 200ms poll, and only noticed the close at the next
        // backstop tick — holding the bound endpoint ~200ms after drop.
        // Drop must close the channel first, then wake: the endpoint is
        // released within a few milliseconds.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let endpoint = format!("tcp://127.0.0.1:{port}");

        let client = WorkerZmqClient::new(endpoint.clone());
        // Let the actor reach its blocking poll.
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(client);

        let start = std::time::Instant::now();
        loop {
            let probe_ctx = zmq::Context::new();
            let probe = probe_ctx.socket(zmq::PAIR).unwrap();
            if probe.bind(&endpoint).is_ok() {
                drop(probe);
                break;
            }
            drop(probe);
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "actor never released {endpoint}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "endpoint released in {:?}; drop must shut the actor down promptly, \
             not at the 200ms backstop tick",
            start.elapsed()
        );
    }

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

    // 回归防护: actor 曾以 try_recv + poll(100ms)/rcvtimeo(100ms) 交替轮询,
    // 空闲周期 200ms —— 低并发(串行)请求每个恒定 +~200ms(0.7.7 基线实测
    // c=1 p50=203ms)。命令通道接入 inproc wake socket 后,空闲往返必须远
    // 低于一个 poll 周期。
    #[tokio::test]
    async fn idle_unary_round_trip_not_gated_by_actor_poll_tick() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-latency-{}.sock",
                std::process::id()
            ));
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 34000 + std::process::id() % 1000);

        let ep_for_worker = endpoint.clone();
        let worker = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&ep_for_worker).expect("worker connect");
            let _ = s.set_rcvtimeo(1000);
            loop {
                let bytes = match s.recv_bytes(0) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resp = pb::Response {
                    uid: req.uid.clone(),
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: bytes::Bytes::from_static(b"{}"),
                        headers: HashMap::new(),
                        status: Some(pb::Status {
                            code: "Ok".to_string(),
                            message: "".to_string(),
                        }),
                        ..Default::default()
                    })),
                    metrics: None,
                };
                let _ = s.send(resp.encode_to_vec(), 0);
            }
        });

        let client = WorkerZmqClient::new(endpoint);
        // Let the bind + worker connect establish the PAIR.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Sequential (concurrency=1) round trips against an idle actor.
        let mut worst = Duration::ZERO;
        for i in 0..5 {
            let request = pb::Request {
                uid: format!("lat-{i}"),
                meta: None,
                payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                    data: bytes::Bytes::from_static(b"{}"),
                })),
            };
            let t0 = Instant::now();
            client.send(request).await.expect("send");
            worst = worst.max(t0.elapsed());
        }

        drop(client);
        let _ = worker.join();

        assert!(
            worst < Duration::from_millis(150),
            "idle round trip took {worst:?} — actor poll tick is gating requests"
        );
    }

    // Bidirectional stream chunks must be sent fire-and-forget: the worker's
    // reply to a chunk is a StreamResponse routed through the stream channel
    // (registered at open), NOT a unary Response matched by uid.  Using send()
    // for chunks therefore deadlocks for ZMQ_RESPONSE_TIMEOUT waiting for a
    // reply that never matches — which is the bidi multi-chunk stall bug.
    // send_raw emits the request with no pending reply slot.
    #[tokio::test]
    async fn send_raw_is_fire_and_forget_for_bidi_chunks() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-bidi-{}.sock",
                std::process::id()
            ));
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 33000 + std::process::id() % 1000);

        let ep_for_worker = endpoint.clone();
        let worker = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&ep_for_worker).expect("worker connect");
            let _ = s.set_rcvtimeo(3000);
            loop {
                let bytes = match s.recv_bytes(0) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let stream = match req.payload {
                    Some(pb::request::Payload::Stream(st)) => st,
                    _ => continue,
                };
                // Reply on the stream channel (as a bidi worker would for
                // on_open / on_chunk) — never as a unary Response by uid.
                let resp = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: stream.stream_id.clone(),
                        payload: Some(pb::stream_response::Payload::Chunk(
                            pb::StreamChunkResponse {
                                data: bytes::Bytes::from_static(b"{\"ok\":true}"),
                                is_final: false,
                            },
                        )),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let _ = s.send(resp.encode_to_vec(), 0);
            }
        });

        let client = WorkerZmqClient::new(endpoint);
        // Let the bind + worker connect establish the PAIR.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let sid = "bidi-test".to_string();
        let open_req = pb::Request {
            uid: "open".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
                stream_id: sid.clone(),
                action: Some(pb::stream_request::Action::Open(pb::StreamOpen {
                    data: bytes::Bytes::from_static(b"{}"),
                    meta: None,
                })),
            })),
        };
        let mut chunk_rx = client
            .send_stream(open_req, sid.clone())
            .await
            .expect("open stream");

        // on_open reply arrives via the stream channel.
        tokio::time::timeout(Duration::from_secs(3), chunk_rx.recv())
            .await
            .expect("on_open response timed out")
            .expect("stream channel closed");

        // Two bidi chunks via send_raw: each must return promptly and its
        // reply must arrive on the stream channel within a couple seconds.
        for i in 0..2u8 {
            let chunk_req = pb::Request {
                uid: format!("chunk-{i}"),
                meta: None,
                payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
                    stream_id: sid.clone(),
                    action: Some(pb::stream_request::Action::Chunk(pb::StreamChunk {
                        data: bytes::Bytes::from(vec![i]),
                    })),
                })),
            };
            let t0 = std::time::Instant::now();
            client.send_raw(chunk_req).await.expect("send_raw");
            assert!(
                t0.elapsed() < Duration::from_secs(3),
                "send_raw blocked for {:?} on chunk {i}; bidi chunk must be fire-and-forget",
                t0.elapsed()
            );
            tokio::time::timeout(Duration::from_secs(3), chunk_rx.recv())
                .await
                .expect("chunk response timed out")
                .expect("stream channel closed");
        }

        drop(client);
        drop(chunk_rx);
        let _ = worker.join();
    }
}
