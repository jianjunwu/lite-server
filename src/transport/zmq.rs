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
    /// Fail every in-flight request: the worker process is gone (crash or
    /// supervisor kill). Pending unary slots get an error response, stream
    /// routes get a terminal Error frame. The transport itself stays up —
    /// the bound socket outlives the worker and a respawned peer reconnects.
    FailAll { reason: String },
}

pub struct WorkerZmqClient {
    cmd_tx: mpsc::Sender<ZmqCommand>,
    /// Wake-signal socket paired with the actor's poll set: a single byte
    /// interrupts the actor's blocking poll so newly queued commands are
    /// drained immediately instead of at the next backstop tick. `None`
    /// only if inproc pair creation failed — the actor then relies on the
    /// backstop tick (correct, just slower).
    wake_tx: Option<std::sync::Arc<std::sync::Mutex<zmq::Socket>>>,
    /// True while the actor is parked in its blocking poll. Senders read it
    /// to skip the wake lock+syscall when the actor is busy draining (the
    /// command is seen on the current pass); the actor re-checks the command
    /// channel right after setting it, closing the lost-wakeup window.
    actor_asleep: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Test instrumentation: how many wake bytes were actually sent (the
    /// gating regression test asserts this stays near zero under load).
    wake_send_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl WorkerZmqClient {
    pub fn new(endpoint: String) -> Self {
        Self::new_with_cb(endpoint, false)
    }

    /// `is_cb` marks the peer as a continuous-batching worker: the pending
    /// sweep then notifies it via `CbRemove` when a reply slot dies (B2).
    pub fn new_with_cb(endpoint: String, is_cb: bool) -> Self {
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
        let actor_asleep = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let actor_asleep_in_loop = actor_asleep.clone();
        let wake_send_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

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

                // Advertise "going to sleep", then re-check the command
                // channel: a producer that queued between the drain above and
                // this store sees asleep==false and skips its wake — this
                // second drain keeps its command from waiting out the
                // backstop tick (lost wakeup).
                actor_asleep_in_loop.store(true, std::sync::atomic::Ordering::SeqCst);
                if !cmd_rx.is_empty() {
                    actor_asleep_in_loop.store(false, std::sync::atomic::Ordering::SeqCst);
                    continue;
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
                actor_asleep_in_loop.store(false, std::sync::atomic::Ordering::SeqCst);

                match poll_result {
                    Ok(_) => {
                        if worker_fired {
                            match socket.recv_bytes(zmq::DONTWAIT) {
                                Ok(bytes) => {
                                match pb::Response::decode(bytes.as_slice()) {
                                    Ok(resp) => {
                                        if let Some(pb::response::Payload::Stream(stream_resp)) = resp.payload {
                                            // Move the frame into the channel
                                            // (try_send returns the value on
                                            // Full) instead of cloning it per
                                            // chunk. sid / is_terminal are
                                            // computed up front for post-move
                                            // cleanup.
                                            let sid = stream_resp.stream_id.clone();
                                            let is_terminal = matches!(stream_resp.payload,
                                                Some(pb::stream_response::Payload::Done(_))
                                                | Some(pb::stream_response::Payload::Error(_)));
                                            let mut overflowed = false;
                                            if let Some(tx) = stream_routes.get(&sid) {
                                                match tx.try_send(stream_resp) {
                                                    Ok(()) => {
                                                        if is_terminal {
                                                            stream_routes.remove(&sid);
                                                        }
                                                    }
                                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                        overflowed = true;
                                                        // B2(审计 P9-1):背压截断必须对 client 可见——
                                                        // channel 满意味着后续数据帧(含终态 Done)被丢弃,
                                                        // 若安静移除路由,consumer 观察到干净 EOF 误当正常
                                                        // 完成。补一个合成 Error 终态帧:克隆 sender +
                                                        // spawn 异步投递(不阻塞 actor 主循环),随后移除
                                                        // 路由。投递不设超时(#9):恢复的 consumer 总能
                                                        // 收到截断帧;卡死的 consumer 由 chunk-idle 回收
                                                        // (recv_chunk 常开)或 rx drop 终结——send 随之
                                                        // 以 Closed 完成,任务退出,无泄漏。
                                                        warn!("Stream channel full for {} — delivering truncation error", sid);
                                                        let sid_t = sid.clone();
                                                        let tx2 = tx.clone();
                                                        tokio::spawn(async move {
                                                            let term = pb::StreamResponse {
                                                                stream_id: sid_t,
                                                                payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                                                                    message: "stream truncated: consumer too slow (channel overflow)".to_string(),
                                                                })),
                                                            };
                                                            let _ = tx2.send(term).await;
                                                        });
                                                    }
                                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                        stream_routes.remove(&sid);
                                                    }
                                                }
                                            }
                                            if overflowed {
                                                stream_routes.remove(&sid);
                                            }
                                            // A streaming route reply leaves its
                                            // unused unary slot behind — free it
                                            // once the stream terminates.
                                            if is_terminal {
                                                pending.remove(&sid);
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
                    let evicted = sweep_dead_pending(&mut pending, &socket, is_cb);
                    if !evicted.is_empty() {
                        warn!("Swept {} orphaned pending ZMQ response(s)", evicted.len());
                    }
                    last_pending_sweep = Instant::now();
                }
            }
        });

        Self { cmd_tx, wake_tx, actor_asleep, wake_send_count }
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

    /// Fail every in-flight request immediately (worker process gone).
    ///
    /// ZMQ PAIR has no peer-disconnect event, so without this the entries
    /// linger until their caller-side timeouts (request_timeout, else the
    /// [`ZMQ_RESPONSE_TIMEOUT`] backstop). The transport stays usable: the
    /// bound socket outlives the worker and a respawned peer reconnects to
    /// it. Best-effort: a full/closed command channel (flood or teardown)
    /// falls back to the timeout path with a warn.
    pub fn fail_all(&self, reason: &str) {
        match self.cmd_tx.try_send(ZmqCommand::FailAll { reason: reason.to_string() }) {
            Ok(()) => self.wake_actor(),
            Err(e) => warn!(
                "ZMQ fail_all dropped ({}); in-flight requests fall back to timeouts",
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => "command channel full",
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => "command channel closed",
                }
            ),
        }
    }

    /// Interrupt the actor's blocking poll so it drains the command channel
    /// now instead of at the next backstop tick. Best-effort: a full wake
    /// pipe means the actor is already awake. Skipped entirely when the
    /// actor is busy draining (its current pass sees the command) — that
    /// saves a mutex acquisition + syscall per command under load.
    fn wake_actor(&self) {
        if !self.actor_asleep.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        if let Some(wake) = &self.wake_tx {
            // Poison recovery: a panic while holding the lock must not
            // disable waking — matches ShutdownState's precedent.
            let guard = wake.lock().unwrap_or_else(|e| e.into_inner());
            if guard.send(b"\0".as_ref(), zmq::DONTWAIT).is_ok() {
                self.wake_send_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
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
                        // Dropping chunk_tx silently would read as a clean EOF
                        // (WorkerEof) to consumers; deliver a terminal Error
                        // frame instead (parity with the Unary/RouteOrStream
                        // error paths). Fresh channel — try_send can't fill.
                        let term = pb::StreamResponse {
                            stream_id,
                            payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                                message: format!("ZMQ send: {}", e),
                            })),
                        };
                        let _ = chunk_tx.try_send(term);
                    }
                }
            }
            Ok(ZmqCommand::Raw { request }) => {
                // B1(审计 P9-1):cancel 控制消息到达即回收流路由——worker 对
                // cancel 语义上不回任何终态帧(python _ResponseSender.cancel:
                // "No StreamDone is sent"),不在此回收则该流的路由
                // (mpsc::Sender+至多 64 条缓冲 chunk)泄漏到 worker 重启。
                if let Some(pb::request::Payload::Stream(ref st)) = request.payload {
                    if matches!(st.action, Some(pb::stream_request::Action::Cancel(_))) {
                        stream_routes.remove(&st.stream_id);
                        pending.remove(&st.stream_id);
                    }
                }
                let bytes = request.encode_to_vec();
                if let Err(e) = socket.send(&bytes, 0) {
                    // B2: stop messages are sent during unload/shutdown; if the
                    // worker has already exited (e.g. SIGINT from terminal),
                    // the PAIR socket peer is gone and send fails with EAGAIN.
                    // Log at WARN instead of ERROR — this is expected during
                    // graceful shutdown, not a transport fault.
                    let is_stop = request.uid == "stop";
                    if is_stop {
                        warn!("ZMQ stop send failed (worker already gone): {}", e);
                    } else {
                        error!("ZMQ raw send error: {}", e);
                    }
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
            Ok(ZmqCommand::FailAll { reason }) => {
                // ZMQ PAIR has no peer-disconnect event: without this the
                // entries would hang until their caller-side timeouts.
                let (n_pending, n_streams) = (pending.len(), stream_routes.len());
                for (uid, tx) in pending.drain() {
                    let _ = tx.send(error_response(&uid, &reason));
                }
                for (sid, tx) in stream_routes.drain() {
                    let term = pb::StreamResponse {
                        stream_id: sid,
                        payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                            message: reason.clone(),
                        })),
                    };
                    // S2: a full stream channel must not silently swallow the
                    // crash frame — a dropped terminal reads as a clean EOF
                    // and the crash gets counted as a successful stream. Same
                    // delivery contract as the overflow path above: spawn an
                    // unbounded send; a live consumer always observes the
                    // frame, a dead one ends the send with Closed.
                    tokio::spawn(async move {
                        let _ = tx.send(term).await;
                    });
                }
                if n_pending > 0 || n_streams > 0 {
                    warn!(
                        "ZMQ fail_all ({}): released {} pending + {} stream(s)",
                        reason, n_pending, n_streams
                    );
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
/// dropped (#5). Returns the evicted uids.
///
/// B2: when the peer is a continuous-batching worker (`is_cb`), eviction
/// also sends a fire-and-forget `CbRemove` per uid — a CB sequence keeps
/// generating after its reply slot dies (client disconnect / timeout), and
/// the worker-side cb_remove branch stops that wasted compute. Non-CB
/// workers have no generation to cancel, so eviction stays silent for them.
fn sweep_dead_pending(
    pending: &mut HashMap<String, oneshot::Sender<pb::Response>>,
    socket: &zmq::Socket,
    is_cb: bool,
) -> Vec<String> {
    let mut evicted = Vec::new();
    pending.retain(|uid, tx| {
        if tx.is_closed() {
            evicted.push(uid.clone());
            false
        } else {
            true
        }
    });
    if is_cb {
        for uid in &evicted {
            let req = pb::Request {
                uid: uid.clone(),
                meta: None,
                payload: Some(pb::request::Payload::CbRemove(pb::CbRemoveRequest {
                    uid: uid.clone(),
                })),
            };
            if let Err(e) = socket.send(req.encode_to_vec(), 0) {
                warn!("cb_remove send failed for {}: {}", uid, e);
            }
        }
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_dead_pending_removes_senders_whose_receiver_dropped() {
        // #5: a request whose caller timed out (response_rx dropped) leaves a
        // dead oneshot::Sender in the pending map. Its is_closed() is true, so
        // a periodic sweep must evict it while keeping live senders intact.
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PAIR).unwrap();
        let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
        let (live_tx, _live_rx) = oneshot::channel();
        let (dead_tx, dead_rx) = oneshot::channel();
        drop(dead_rx);
        pending.insert("live".to_string(), live_tx);
        pending.insert("dead".to_string(), dead_tx);

        let evicted = sweep_dead_pending(&mut pending, &socket, false);
        assert_eq!(evicted, vec!["dead".to_string()]);
        assert!(pending.contains_key("live"));
        assert!(!pending.contains_key("dead"));
    }

    #[test]
    fn sweep_dead_pending_keeps_all_when_all_live() {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PAIR).unwrap();
        let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        pending.insert("a".to_string(), tx1);
        pending.insert("b".to_string(), tx2);

        let evicted = sweep_dead_pending(&mut pending, &socket, false);
        assert!(evicted.is_empty());
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn sweep_dead_pending_cb_worker_receives_cb_remove_for_evicted() {
        // B2: evicting a dead pending entry on a continuous-batching worker
        // must tell the worker to stop generating that uid — the reply slot
        // is gone (client disconnect / timeout), so the response would be
        // dropped on arrival while generation keeps burning compute.
        let ctx = zmq::Context::new();
        let endpoint = format!("inproc://sweep-cb-{}", std::process::id());
        let actor = ctx.socket(zmq::PAIR).unwrap();
        actor.bind(&endpoint).unwrap();
        let peer = ctx.socket(zmq::PAIR).unwrap();
        peer.connect(&endpoint).unwrap();
        peer.set_rcvtimeo(2000).unwrap();

        let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
        let (live_tx, _live_rx) = oneshot::channel();
        let (dead_tx, dead_rx) = oneshot::channel();
        drop(dead_rx);
        pending.insert("live".to_string(), live_tx);
        pending.insert("cb-dead".to_string(), dead_tx);

        let evicted = sweep_dead_pending(&mut pending, &actor, true);
        assert_eq!(evicted, vec!["cb-dead".to_string()]);

        let bytes = peer.recv_bytes(0).expect("cb_remove frame for evicted uid");
        let req = pb::Request::decode(bytes.as_slice()).unwrap();
        assert_eq!(req.uid, "cb-dead");
        match req.payload {
            Some(pb::request::Payload::CbRemove(rm)) => assert_eq!(rm.uid, "cb-dead"),
            other => panic!("expected CbRemove payload, got {:?}", other),
        }
    }

    #[test]
    fn sweep_dead_pending_non_cb_worker_receives_nothing() {
        // Non-CB workers have no generation to cancel: eviction must stay
        // silent (a spurious cb_remove would be noise on the dispatch loop).
        let ctx = zmq::Context::new();
        let endpoint = format!("inproc://sweep-noncb-{}", std::process::id());
        let actor = ctx.socket(zmq::PAIR).unwrap();
        actor.bind(&endpoint).unwrap();
        let peer = ctx.socket(zmq::PAIR).unwrap();
        peer.connect(&endpoint).unwrap();

        let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
        let (dead_tx, dead_rx) = oneshot::channel();
        drop(dead_rx);
        pending.insert("dead".to_string(), dead_tx);

        let evicted = sweep_dead_pending(&mut pending, &actor, false);
        assert_eq!(evicted, vec!["dead".to_string()]);
        assert!(
            peer.recv_bytes(zmq::DONTWAIT).is_err(),
            "non-CB worker must not receive cb_remove"
        );
    }

    #[tokio::test]
    async fn fail_all_full_stream_channel_still_delivers_terminal_error() {
        // S2: worker crash while a stream's channel is FULL must not silently
        // drop the terminal error frame (try_send fails → client sees a clean
        // EOF → the crash is recorded as a successful stream). Delivery must
        // follow the overflow path's contract (0dd5fde): pend until the
        // consumer drains, so the truncation/crash is always visible.
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PAIR).unwrap(); // FailAll never sends
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ZmqCommand>(8);
        let mut pending: HashMap<String, oneshot::Sender<pb::Response>> = HashMap::new();
        let mut stream_routes: HashMap<String, mpsc::Sender<pb::StreamResponse>> = HashMap::new();

        let (chunk_tx, mut chunk_rx) = mpsc::channel::<pb::StreamResponse>(STREAM_CHANNEL_SIZE);
        for _ in 0..STREAM_CHANNEL_SIZE {
            chunk_tx
                .try_send(pb::StreamResponse {
                    stream_id: "s-full".to_string(),
                    payload: Some(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                        data: bytes::Bytes::from_static(b"x"),
                        is_final: false,
                    })),
                })
                .unwrap();
        }
        stream_routes.insert("s-full".to_string(), chunk_tx);

        cmd_tx
            .send(ZmqCommand::FailAll { reason: "worker exited unexpectedly".to_string() })
            .await
            .unwrap();
        drain_commands(&mut cmd_rx, &socket, &mut pending, &mut stream_routes);

        // Drain the buffered chunks; the terminal Error must follow — a
        // silent EOF (dropped frame) is the bug.
        for _ in 0..STREAM_CHANNEL_SIZE {
            chunk_rx.recv().await.expect("buffered chunk");
        }
        let term = tokio::time::timeout(Duration::from_secs(2), chunk_rx.recv())
            .await
            .expect("terminal error frame never arrived: crash looks like clean EOF")
            .expect("channel closed without terminal frame");
        match term.payload {
            Some(pb::stream_response::Payload::Error(e)) => {
                assert!(e.message.contains("worker exited unexpectedly"), "reason lost: {}", e.message);
            }
            other => panic!("expected terminal Error frame, got {:?}", other),
        }
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

    // Regression: a Stream command whose socket send fails used to drop
    // `chunk_tx` silently — the consumer observed a clean EOF (WorkerEof)
    // with no terminal frame, indistinguishable from normal completion.
    // The send-failure path must deliver a terminal Error frame, matching
    // the Unary/RouteOrStream error paths.
    #[tokio::test]
    async fn stream_send_failure_delivers_error_frame_not_clean_eof() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-stream-sendfail-{}.sock",
                std::process::id()
            ));
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 35000 + std::process::id() % 1000);

        let client = WorkerZmqClient::new(endpoint);

        let open_req = pb::Request {
            uid: "s-open".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
                stream_id: "sid-1".to_string(),
                action: Some(pb::stream_request::Action::Open(pb::StreamOpen {
                    data: bytes::Bytes::from_static(b"{}"),
                    meta: None,
                    decoupled: None,
                })),
            })),
        };
        let mut chunk_rx = client
            .send_stream(open_req, "sid-1".to_string())
            .await
            .expect("send_stream");

        // No peer: the actor's blocking send errors after sndtimeo (1s).
        // The consumer must receive a terminal Error frame, not a silent EOF.
        let frame = tokio::time::timeout(Duration::from_secs(5), chunk_rx.recv())
            .await
            .expect("no frame within 5s")
            .expect("channel closed without any frame (silent EOF)");
        assert!(
            matches!(frame.payload, Some(pb::stream_response::Payload::Error(_))),
            "expected terminal Error frame, got {:?}",
            frame.payload
        );
        drop(client);
    }

    // Regression (audit 2026-08-10 #8): ZMQ PAIR has no peer-disconnect
    // event — when the worker process died, in-flight requests hung until
    // their caller-side timeouts (request_timeout, else the 300s backstop).
    // fail_all must release every pending waiter and stream route
    // immediately, while keeping the transport usable for a respawned peer.
    #[tokio::test]
    async fn fail_all_releases_pending_and_stream_routes_fast() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-failall-{}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&sock);
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 36000 + std::process::id() % 1000);

        // Silent peer: receives everything, never replies — in-flight
        // entries stay registered until fail_all. rcvtimeo-bounded so the
        // thread exits after the test drops the client.
        let ep_for_worker = endpoint.clone();
        let worker = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&ep_for_worker).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            loop {
                match s.recv_bytes(0) {
                    Ok(_) => {}
                    Err(_) => return,
                }
            }
        });

        let client = std::sync::Arc::new(WorkerZmqClient::new(endpoint));
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 1. Pending unary with a 30s timeout — fail_all, not the timeout,
        //    must be what resolves it.
        let unary_req = pb::Request {
            uid: "inflight-unary".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: bytes::Bytes::from_static(b"{}"),
            })),
        };
        let unary_task = tokio::spawn({
            let c = client.clone();
            async move { c.send_with_timeout(unary_req, Duration::from_secs(30)).await }
        });

        // 2. Stream route with no terminal frame in sight.
        let open_req = pb::Request {
            uid: "s-open".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
                stream_id: "inflight-stream".to_string(),
                action: Some(pb::stream_request::Action::Open(pb::StreamOpen {
                    data: bytes::Bytes::from_static(b"{}"),
                    meta: None,
                    decoupled: None,
                })),
            })),
        };
        let mut chunk_rx = client
            .send_stream(open_req, "inflight-stream".to_string())
            .await
            .expect("send_stream");

        // Let the actor register both entries.
        tokio::time::sleep(Duration::from_millis(150)).await;

        client.fail_all("worker process exited unexpectedly");

        // The pending unary resolves immediately with an Error payload.
        let resp = tokio::time::timeout(Duration::from_secs(3), unary_task)
            .await
            .expect("pending unary not released by fail_all")
            .expect("unary task panicked")
            .expect("unary send failed");
        match resp.payload {
            Some(pb::response::Payload::Single(single)) => {
                let st = single.status.expect("status");
                assert_eq!(st.code, "Error");
                assert!(
                    st.message.contains("exited unexpectedly"),
                    "fail reason lost: {}",
                    st.message
                );
            }
            other => panic!("expected Single error, got {:?}", other),
        }

        // The stream route gets a terminal Error frame (not a silent EOF).
        let frame = tokio::time::timeout(Duration::from_secs(3), chunk_rx.recv())
            .await
            .expect("stream route not released by fail_all")
            .expect("stream closed without a frame");
        match frame.payload {
            Some(pb::stream_response::Payload::Error(e)) => {
                assert!(
                    e.message.contains("exited unexpectedly"),
                    "fail reason lost: {}",
                    e.message
                );
            }
            other => panic!("expected Error frame, got {:?}", other),
        }

        // The transport stays usable for a respawned peer: a fresh send
        // reaches the actor and times out normally — it must NOT fail with
        // "command channel closed".
        let probe_req = pb::Request {
            uid: "post-failall-probe".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: bytes::Bytes::from_static(b"{}"),
            })),
        };
        let err = client
            .send_with_timeout(probe_req, Duration::from_millis(300))
            .await
            .expect_err("silent peer must time out");
        assert!(
            matches!(err, AppError::InferenceTimeout(_)),
            "actor died after fail_all: {err}"
        );

        drop(client);
        let _ = worker.join();
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

    // The wake socket must fire ONLY when the actor is actually parked in its
    // poll (audit 2026-08-10 #7): while commands flow faster than the actor
    // drains, it never reaches the poll — and per-command wakes are pure
    // overhead (a mutex + a syscall each). Under a sustained flood the wake
    // count must stay far below the command count (ungated, it equalled it).
    // The warm-up phase is excluded from the measurement: startup scheduling
    // (actor bind, first drain passes) dominates the wake count there and
    // would make a tight bound flaky under xdist load.
    #[tokio::test]
    async fn wake_is_skipped_while_actor_is_busy() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-wakegate-{}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&sock);
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 38000 + std::process::id() % 1000);

        // Peer that connects but never reads: buffered by ZMQ/kernel, sends
        // don't error — the actor drains as fast as commands arrive.
        let ep_for_worker = endpoint.clone();
        let worker = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&ep_for_worker).expect("worker connect");
            std::thread::sleep(Duration::from_secs(3));
        });

        let client = WorkerZmqClient::new(endpoint);
        tokio::time::sleep(Duration::from_millis(200)).await;

        let req = pb::Request {
            uid: "flood".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: bytes::Bytes::from_static(b"{}"),
            })),
        };

        // Warm-up: let the actor reach its steady drain loop.
        for _ in 0..200 {
            client.send_raw(req.clone()).await.expect("send_raw");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        const N: usize = 1000;
        let base = client.wake_send_count.load(std::sync::atomic::Ordering::SeqCst);
        for _ in 0..N {
            client.send_raw(req.clone()).await.expect("send_raw");
        }
        let wakes = client.wake_send_count.load(std::sync::atomic::Ordering::SeqCst) - base;
        assert!(
            wakes < N / 2,
            "wake gate barely engaged under a sustained flood: {wakes}/{N} commands paid a wake \
             (ungated = every command)"
        );

        drop(client);
        let _ = worker.join();
    }

    // The gate must never lose a wakeup: with the actor idling between
    // bursts, concurrent round trips must all complete promptly (a lost wake
    // would strand a command until the 200ms backstop tick).
    #[tokio::test]
    async fn wake_gate_loses_no_wakeup_under_concurrency() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-wakeconc-{}.sock",
                std::process::id()
            ));
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 39000 + std::process::id() % 1000);

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

        let client = std::sync::Arc::new(WorkerZmqClient::new(endpoint));
        tokio::time::sleep(Duration::from_millis(200)).await;

        let start = Instant::now();
        let mut handles = Vec::new();
        for i in 0..32 {
            let c = client.clone();
            handles.push(tokio::spawn(async move {
                let req = pb::Request {
                    uid: format!("conc-{i}"),
                    meta: None,
                    payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                        data: bytes::Bytes::from_static(b"{}"),
                    })),
                };
                c.send(req).await.expect("send")
            }));
        }
        for h in handles {
            h.await.expect("task panicked");
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "32 concurrent round trips took {:?} — wakeups are being lost",
            start.elapsed()
        );

        drop(client);
        let _ = worker.join();
    }

    // Regression (audit 2026-08-10 #9): the synthetic truncation frame used
    // to have a 2s delivery window — a consumer stalled longer saw a clean
    // EOF (WorkerEof) instead of the truncation error, reading data loss as
    // normal completion. Delivery must wait as long as the channel lives:
    // a recovering consumer always gets the terminal Error frame.
    #[tokio::test]
    async fn overflow_truncation_frame_survives_slow_consumer() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-overflow-{}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&sock);
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 40000 + std::process::id() % 1000);

        let ep_for_worker = endpoint.clone();
        let worker = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&ep_for_worker).expect("worker connect");
            let _ = s.set_rcvtimeo(3000);
            // Wait for the open, then blast more chunks than the channel
            // holds at once — forcing the overflow path immediately.
            let bytes = match s.recv_bytes(0) {
                Ok(b) => b,
                Err(_) => return,
            };
            let req = match pb::Request::decode(bytes.as_slice()) {
                Ok(r) => r,
                Err(_) => return,
            };
            let sid = match req.payload {
                Some(pb::request::Payload::Stream(st)) => st.stream_id,
                _ => return,
            };
            for _ in 0..(STREAM_CHANNEL_SIZE + 6) {
                let resp = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: sid.clone(),
                        payload: Some(pb::stream_response::Payload::Chunk(
                            pb::StreamChunkResponse {
                                data: bytes::Bytes::from_static(b"{}"),
                                is_final: false,
                            },
                        )),
                    })),
                    ..Default::default()
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        });

        let client = WorkerZmqClient::new(endpoint);
        tokio::time::sleep(Duration::from_millis(200)).await;

        let sid = "sid-overflow".to_string();
        let open_req = pb::Request {
            uid: "open-overflow".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
                stream_id: sid.clone(),
                action: Some(pb::stream_request::Action::Open(pb::StreamOpen {
                    data: bytes::Bytes::from_static(b"{}"),
                    meta: None,
                    decoupled: None,
                })),
            })),
        };
        let mut chunk_rx = client
            .send_stream(open_req, sid)
            .await
            .expect("send_stream");

        // Stall longer than the old 2s delivery window (the overflow spawn
        // fires within milliseconds of the blast).
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Drain: the buffered chunks must be followed by the terminal
        // truncation Error — not a clean EOF.
        let mut chunks = 0usize;
        let terminal = loop {
            match tokio::time::timeout(Duration::from_secs(3), chunk_rx.recv()).await {
                Ok(Some(frame)) => match &frame.payload {
                    Some(pb::stream_response::Payload::Chunk(_)) => chunks += 1,
                    _ => break frame,
                },
                Ok(None) => panic!("clean EOF before terminal frame — truncation was lost"),
                Err(_) => panic!("channel stalled"),
            }
        };
        assert_eq!(chunks, STREAM_CHANNEL_SIZE, "channel must drain its full capacity first");
        match terminal.payload {
            Some(pb::stream_response::Payload::Error(e)) => {
                assert!(
                    e.message.contains("truncated"),
                    "unexpected terminal message: {}",
                    e.message
                );
            }
            other => panic!("expected truncation Error frame, got {:?}", other),
        }

        drop(client);
        let _ = worker.join();
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
                    decoupled: None,
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

    // ── F4/F6 闸门测试 ──────────────────────────────────────────────────
    // 决定走 Strategy 1(复用 bound PAIR socket)还是 Strategy 2(可换 slot + 代际 endpoint)。
    // 声称: `WorkerZmqClient` 的 bound PAIR socket 在对端(worker 进程)死后,
    //   不重 bind 即可接受新对端连接、流量恢复。
    // 绿 → Strategy 1 可行且最简(respawn 不新建 client);红 → 走 Strategy 2。
    #[tokio::test]
    async fn bound_pair_survives_peer_death_and_reconnect() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-reconnect-{}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&sock);
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 32000 + std::process::id() % 1000);

        // 回一个 echo(uid)+ 标记 data 的 Single 响应后退出(= worker 进程死亡:
        // socket 与 ctx 逆序释放)。注意 ctx 必须与 s 同为函数局部、不可放入子作用域,
        // 否则 ctx 先于 socket 释放会 use-after-free。
        fn run_worker(endpoint: String, marker: &'static [u8]) {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(2000);
            let bytes = match s.recv_bytes(0) {
                Ok(b) => b,
                Err(_) => return,
            };
            let req = match pb::Request::decode(bytes.as_slice()) {
                Ok(r) => r,
                Err(_) => return,
            };
            let resp = pb::Response {
                uid: req.uid.clone(),
                payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                    data: bytes::Bytes::from_static(marker),
                    headers: HashMap::new(),
                    status: Some(pb::Status {
                        code: "Ok".to_string(),
                        message: String::new(),
                    }),
                    ..Default::default()
                })),
                metrics: None,
            };
            let _ = s.send(resp.encode_to_vec(), 0);
        }

        // Strategy 1 要复用的 bound 端(真实 actor,带 wake socket + poll 循环)。
        let client = WorkerZmqClient::new(endpoint.clone());

        // ── peer-1:连接、答一个请求后死亡 ──
        let ep = endpoint.clone();
        let w1 = std::thread::spawn(move || run_worker(ep, b"{\"peer\":1}"));
        tokio::time::sleep(Duration::from_millis(200)).await; // 让 bind + connect 建立

        let r1 = client
            .send_with_timeout(
                pb::Request {
                    uid: "peer-1".to_string(),
                    meta: None,
                    payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                        data: bytes::Bytes::from_static(b"{}"),
                    })),
                },
                Duration::from_secs(3),
            )
            .await
            .expect("peer-1 send");
        assert_eq!(r1.uid, "peer-1");
        let _ = w1.join(); // peer-1 彻底死亡(对端 socket 关闭)

        // ── peer-2 连同一 endpoint:若 bound PAIR 接受新对端,Strategy 1 成立 ──
        let ep = endpoint.clone();
        let w2 = std::thread::spawn(move || run_worker(ep, b"{\"peer\":2}"));
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 决定性 recv:若 bound actor 没接受 peer-2,send 在此超时返回 Err。
        let r2 = client
            .send_with_timeout(
                pb::Request {
                    uid: "peer-2".to_string(),
                    meta: None,
                    payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                        data: bytes::Bytes::from_static(b"{}"),
                    })),
                },
                Duration::from_secs(3),
            )
            .await
            .expect("bound PAIR 未在 peer-1 死亡后接受 peer-2 —— Strategy 1 失败,改走 Strategy 2");
        assert_eq!(r2.uid, "peer-2");

        drop(client);
        let _ = w2.join();
    }

    /// B2: stop request (used during unload/shutdown) should NOT panic or
    /// produce unexpected errors when the worker is gone. If the transport
    /// thread is still alive but the peer disconnected, the send fails with a
    /// warning — not an error — because this is expected during graceful
    /// shutdown (not a transport fault).
    #[tokio::test]
    async fn stop_request_send_raw_tolerates_missing_peer() {
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-zmq-stop-{}.sock", std::process::id()
            ));
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 34000 + std::process::id() % 1000);

        let client = WorkerZmqClient::new(endpoint.clone());

        // Send a stop request — the worker peer never existed, so this may fail
        // at the transport level, but it must not crash.
        let stop_req = crate::streaming::build_stop_request();
        assert_eq!(stop_req.uid, "stop", "stop request uid must be 'stop' for B2 detection");

        // send_raw is fire-and-forget; it should not panic even with no peer.
        // The transport thread may or may not still be alive — either result
        // is acceptable. The key assertion: no panic, no deadlock.
        let _ = tokio::time::timeout(Duration::from_secs(3), client.send_raw(stop_req)).await;
        // Clean up (will shut down the transport thread)
        drop(client);
    }
}
