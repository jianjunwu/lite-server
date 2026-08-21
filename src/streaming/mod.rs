pub mod lpm;

use crate::proto::liteserver as pb;
use std::time::Duration;
use tracing::warn;

/// Build a protobuf StreamRequest::Open.
///
/// `decoupled` (P9-1): when true, sets the additive `StreamOpen.decoupled`
/// flag so the worker keeps the channel open after `predict_decoupled`
/// returns (model-controlled lifetime). False/None preserves the existing
/// stream_predict semantics on the wire.
pub fn build_stream_open(
    stream_id: String,
    data: bytes::Bytes,
    meta: Option<pb::RequestMeta>,
    decoupled: bool,
) -> pb::Request {
    pb::Request {
        uid: format!("stream-open-{}", stream_id),
        meta: meta.clone(),
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Open(pb::StreamOpen {
                data,
                meta,
                decoupled: if decoupled { Some(true) } else { None },
            })),
        })),
    }
}

/// Build a protobuf StreamRequest::Chunk.
pub fn build_stream_chunk(stream_id: String, data: bytes::Bytes) -> pb::Request {
    pb::Request {
        uid: format!("stream-chunk-{}", stream_id),
        meta: None,
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Chunk(pb::StreamChunk { data })),
        })),
    }
}

/// Build a protobuf StreamRequest::Close.
pub fn build_stream_close(stream_id: String) -> pb::Request {
    pb::Request {
        uid: format!("stream-close-{}", stream_id),
        meta: None,
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Close(pb::StreamClose {})),
        })),
    }
}

/// Build a graceful-stop control message (server unload / shutdown).
///
/// The worker breaks its recv loop, runs the Python teardown path
/// (_run_teardown: LitAPI.teardown + lifecycle callbacks) and exits cleanly;
/// the server SIGKILLs it only if it never reads this message (hung worker).
/// The worker does not reply — the server observes the natural process exit.
pub fn build_stop_request() -> pb::Request {
    pb::Request {
        uid: "stop".to_string(),
        meta: None,
        payload: Some(pb::request::Payload::Stop(pb::StopRequest {})),
    }
}

/// Build a protobuf StreamRequest::Cancel.
pub fn build_stream_cancel(stream_id: String) -> pb::Request {
    pb::Request {
        uid: format!("stream-cancel-{}", stream_id),
        meta: None,
        payload: Some(pb::request::Payload::Stream(pb::StreamRequest {
            stream_id,
            action: Some(pb::stream_request::Action::Cancel(pb::StreamCancel {})),
        })),
    }
}

/// Why a bounded stream recv was terminated by P-DEADLINE (蓝图 §4.0.4 两段式).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvElapsed {
    /// The overall per-request deadline fired.
    Deadline,
    /// The per-chunk idle budget fired.
    Idle,
}

/// L1 (resource-leak-plan): outcome of a deadline-bounded stream send.
pub enum SendOutcome<T> {
    /// The send completed (the underlying send's own Ok/Err).
    Sent(T),
    /// The overall deadline fired mid-send — reclaim the stream (the client
    /// is stopped or gone; a terminal error frame would not flush anyway).
    Deadline,
    /// P0-2: the chunk-idle budget fired mid-send — the client stopped
    /// draining (backpressure) for longer than a chunk gap; reclaim the
    /// stream (the recv-side idle bound can never fire while the send
    /// blocks, so the send must carry the same bound).
    Idle,
}

/// L1: bound a stream send by the overall stream deadline AND the chunk-idle
/// budget — the effective cap is `min(remaining-to-deadline, idle)`. A
/// stopped reader backpressures the bounded channel/socket; an unbounded
/// send would pin the connection + worker stream + admission slot past the
/// armed deadline and defeat the recv-side idle reclaim entirely (P0-2: the
/// send sits outside the select! that polls recv_chunk). Both `None` =
/// unbounded send (D6 contract; `decoupled_idle_timeout_secs = 0` disables
/// the idle leg), the keepalive ticker (K3/K4) is then the dead-peer
/// detector.
pub async fn send_bounded<F, T>(
    deadline: Option<std::time::Instant>,
    idle: Option<std::time::Duration>,
    send: F,
) -> SendOutcome<T>
where
    F: std::future::Future<Output = T>,
{
    let now = std::time::Instant::now();
    // Overall deadline already expired → stop immediately (recv_chunk parity).
    if let Some(d) = deadline {
        if d <= now {
            return SendOutcome::Deadline;
        }
    }
    let to_deadline = deadline.map(|d| d - now);
    let cap = match (to_deadline, idle) {
        (None, None) => return SendOutcome::Sent(send.await),
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (Some(a), Some(b)) => if a < b { a } else { b },
    };
    match tokio::time::timeout(cap, send).await {
        Ok(r) => SendOutcome::Sent(r),
        Err(_) => match deadline {
            // Same attribution as recv_chunk: if the overall deadline has now
            // passed, it fired; otherwise the (shorter) idle bound did.
            Some(d) if d <= std::time::Instant::now() => SendOutcome::Deadline,
            _ => SendOutcome::Idle,
        },
    }
}

/// L1: bound for terminal-frame sends on a reclaim path. On deadline/idle
/// reclaim the client may be stopped; an unbounded send into a backlogged
/// channel would hang the reclaim itself. 2s lets a slow (still draining)
/// client receive the terminal frame (D35 delivery preserved), a dead one is
/// dropped.
pub const TERMINAL_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Receive the next worker stream chunk under a P-DEADLINE two-stage bound.
///
/// `deadline`: absolute overall deadline (None = no overall bound).
/// `idle`: per-recv idle budget (None = no idle bound). When both are `None`
/// this is a plain `recv()`.
///
/// 方案 C: ALL streaming modes (regular stream/bidi/SSE/WS/custom-route +
/// decoupled) keep chunk-idle reclaim ALWAYS on (decoupled parity) — a stuck
/// stream is recovered instead of hanging unbounded, while long streams that
/// keep producing chunks are unaffected. The OVERALL deadline activates only
/// when the client specified one (default config leaves long streams unbounded
/// by overall deadline). Escape hatch: `decoupled_idle_timeout_secs = 0`
/// disables idle reclaim.
///
/// Each call bounds THIS recv by `min(remaining-to-deadline, idle)`, so a
/// stalled stream trips the idle bound even before the overall deadline.
/// Returns `Ok(None)` when the worker closed the stream, `Ok(Some(chunk))`
/// for the next chunk, or `Err(RecvElapsed)` when a bound fired (caller logs
/// + breaks to end the stream, triggering the existing cancel/close cleanup).
pub async fn recv_chunk(
    rx: &mut tokio::sync::mpsc::Receiver<pb::StreamResponse>,
    deadline: Option<std::time::Instant>,
    idle: Option<std::time::Duration>,
) -> Result<Option<pb::StreamResponse>, RecvElapsed> {
    let now = std::time::Instant::now();
    // Overall deadline: already-expired → stop immediately; else cap this recv.
    if let Some(d) = deadline {
        if d <= now {
            return Err(RecvElapsed::Deadline);
        }
    }
    let to_deadline = deadline.map(|d| d - now);
    let per_recv = match (to_deadline, idle) {
        (None, None) => return Ok(rx.recv().await),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => Some(if a < b { a } else { b }),
    };
    match tokio::time::timeout(per_recv.unwrap(), rx.recv()).await {
        Ok(v) => Ok(v),
        Err(_) => Err(match deadline {
            // If the overall deadline has now passed, attribute it to Deadline;
            // otherwise the (shorter) idle bound fired.
            Some(d) if d <= std::time::Instant::now() => RecvElapsed::Deadline,
            _ => RecvElapsed::Idle,
        }),
    }
}

/// F1 (warmup-gaps audit, promoted from worker/lifecycle for B1): RAII
/// cancel for a worker-side stream. Any exit that did not observe a terminal
/// frame (Done/Error) or a channel close drops the guard armed → a
/// StreamCancel goes out so the worker-side generator stops instead of
/// running to its natural end (unbounded waste for a long LLM stream or an
/// infinite route generator). Covers every abort shape: per-iteration
/// timeout, unexpected frames, a sibling's failure dropping the consumer
/// future mid-poll, client disconnect dropping the HTTP body, and total
/// budgets cutting the whole run. `send_raw` is async, so the cancel rides a
/// spawned task (Drop is sync); a vanished runtime (process teardown) skips
/// it — the workers die with the runtime there anyway.
pub(crate) struct StreamCancelGuard {
    pub(crate) client: std::sync::Arc<crate::transport::zmq::WorkerZmqClient>,
    pub(crate) stream_id: String,
    armed: bool,
}

impl StreamCancelGuard {
    pub(crate) fn armed(
        client: std::sync::Arc<crate::transport::zmq::WorkerZmqClient>,
        stream_id: String,
    ) -> Self {
        Self {
            client,
            stream_id,
            armed: true,
        }
    }

    /// Terminal frame observed or the channel already closed — no cancel.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StreamCancelGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let client = self.client.clone();
                let stream_id = std::mem::take(&mut self.stream_id);
                handle.spawn(async move {
                    let _ = client.send_raw(crate::streaming::build_stream_cancel(stream_id)).await;
                });
            }
        }
    }
}

/// G1/G3 地基: RAII per-slot in-flight stream count. Constructing increments
/// the slot's `stream_inflight`; dropping decrements — every exit shape
/// (terminal frame, client disconnect, idle/deadline reclaim, panic unwind
/// inside `catch_forward_panic`, worker EOF) releases the count with no
/// call-site bookkeeping. The recycle drain waits on this counter reaching
/// zero (bounded by the drain timeout), so a rolling recycle stops killing
/// in-flight streams. Dec saturates at 0 (see
/// [`crate::inference_queue::OutlierState::stream_inflight_dec`]), so a
/// double-drop can never wrap the counter and block the drain forever.
///
/// The guard must live in the task/future that consumes the stream's chunks
/// (the detached forward task for SSE/gRPC, the writer task for WS) — NOT in
/// the handler scope that returns right after open, or the count would
/// release while the stream is still running.
pub(crate) struct StreamInflightGuard {
    outlier: std::sync::Arc<crate::inference_queue::OutlierState>,
    worker_idx: usize,
}

impl StreamInflightGuard {
    pub(crate) fn new(
        outlier: std::sync::Arc<crate::inference_queue::OutlierState>,
        worker_idx: usize,
    ) -> Self {
        outlier.stream_inflight_inc(worker_idx);
        Self { outlier, worker_idx }
    }
}

impl Drop for StreamInflightGuard {
    fn drop(&mut self) {
        self.outlier.stream_inflight_dec(self.worker_idx);
    }
}

/// Run a streaming forward-task body with a uniform panic收口. The SSE /
/// gRPC-server-stream / h2-bidi forward tasks are DETACHED spawns — nobody
/// joins their handle, so a panicking body would silently lose its terminal
/// metric (`record_stream_terminal` never runs) and never cancel the worker
/// stream (the generator runs to its natural end). The unwind itself
/// releases the body's RAII locals (P10 permit, admission guard); `on_panic`
/// then records the Panic terminal and cancels the worker stream, mirroring
/// the WS adapter's join-based panic arm. Normal completion never touches
/// `on_panic`.
pub(crate) async fn catch_forward_panic<B, P, PFut>(transport: &'static str, body: B, on_panic: P)
where
    B: std::future::Future<Output = ()>,
    P: FnOnce() -> PFut,
    PFut: std::future::Future<Output = ()>,
{
    if futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body))
        .await
        .is_err()
    {
        warn!("{transport} forward task panicked; recording Panic terminal and cancelling the worker stream");
        on_panic().await;
    }
}

/// Observe a spawned bidi helper task instead of silently dropping its handle
/// (#8). `abort()` alone discards any panic payload; awaiting the handle first
/// — bounded by a short grace — surfaces a panic as a `JoinError` so we can
/// log it. If the task is still running when the grace expires, abort it to
/// release the client stream promptly. Returns `true` if the task panicked
/// (kept as a return value so the panic-detection path is unit-testable).
pub(crate) async fn observe_or_abort(mut task: tokio::task::JoinHandle<()>) -> bool {
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

    // --- StreamInflightGuard (T7: 收口穷尽) ---

    #[test]
    fn should_increment_on_construct_and_decrement_on_drop() {
        let outlier = std::sync::Arc::new(crate::inference_queue::OutlierState::new(2));
        {
            let _g = StreamInflightGuard::new(outlier.clone(), 1);
            assert_eq!(outlier.stream_inflight(1), 1);
            {
                let _g2 = StreamInflightGuard::new(outlier.clone(), 1);
                assert_eq!(outlier.stream_inflight(1), 2);
            }
            assert_eq!(outlier.stream_inflight(1), 1);
        }
        assert_eq!(outlier.stream_inflight(1), 0, "all guards dropped → slot drains to zero");
    }

    #[tokio::test]
    async fn should_decrement_when_forward_task_panics() {
        // The forward tasks are detached spawns; a panic unwinds the body and
        // must still release the count, or the recycle drain blocks forever.
        let outlier = std::sync::Arc::new(crate::inference_queue::OutlierState::new(1));
        let o = outlier.clone();
        let task = tokio::spawn(async move {
            catch_forward_panic(
                "test",
                async move {
                    let _g = StreamInflightGuard::new(o, 0);
                    panic!("boom");
                },
                || async {},
            )
            .await;
        });
        task.await.unwrap();
        assert_eq!(outlier.stream_inflight(0), 0, "panic unwind must release the count");
    }

    #[tokio::test]
    async fn should_decrement_when_consumer_future_is_dropped_mid_poll() {
        // Client disconnect drops the HTTP body → the consumer future is
        // dropped mid-poll; Drop must run without a terminal frame.
        let outlier = std::sync::Arc::new(crate::inference_queue::OutlierState::new(1));
        let o = outlier.clone();
        let task = tokio::spawn(async move {
            let _g = StreamInflightGuard::new(o, 0);
            std::future::pending::<()>().await;
        });
        // Sync point: wait until the task has actually been polled (spawn
        // does not guarantee the body has started when we continue).
        for _ in 0..100 {
            if outlier.stream_inflight(0) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(outlier.stream_inflight(0), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(outlier.stream_inflight(0), 0, "abort/drop mid-poll must release the count");
    }

    // --- observe_or_abort ---

    #[tokio::test]
    async fn observe_or_abort_detects_panicked_task() {
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

    // --- catch_forward_panic ---

    #[tokio::test]
    async fn catch_forward_panic_runs_cleanup_on_panic() {
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran_c = ran.clone();
        catch_forward_panic(
            "test",
            async { panic!("boom") },
            move || {
                let ran_c = ran_c.clone();
                async move {
                    ran_c.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            },
        )
        .await;
        assert!(
            ran.load(std::sync::atomic::Ordering::Relaxed),
            "on_panic must run when the body panics"
        );
    }

    #[tokio::test]
    async fn catch_forward_panic_clean_body_skips_cleanup() {
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran_c = ran.clone();
        catch_forward_panic(
            "test",
            async {},
            move || {
                let ran_c = ran_c.clone();
                async move {
                    ran_c.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            },
        )
        .await;
        assert!(
            !ran.load(std::sync::atomic::Ordering::Relaxed),
            "on_panic must NOT run on normal completion"
        );
    }

    #[tokio::test]
    async fn catch_forward_panic_unwind_releases_body_guards_first() {
        struct Guard(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let released = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let released_body = released.clone();
        let released_check = released.clone();
        catch_forward_panic(
            "test",
            async move {
                let _guard = Guard(released_body);
                panic!("boom");
            },
            move || {
                let released_check = released_check.clone();
                async move {
                    assert!(
                        released_check.load(std::sync::atomic::Ordering::Relaxed),
                        "RAII locals must be released by the unwind before on_panic runs"
                    );
                }
            },
        )
        .await;
    }

    // --- build_stream_* helpers ---

    #[test]
    fn test_build_stream_open() {
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"data"), None, false);
        assert_eq!(req.uid, "stream-open-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert_eq!(s.stream_id, "s1");
                match s.action {
                    Some(pb::stream_request::Action::Open(o)) => {
                        assert_eq!(o.data, &b"data"[..]);
                        // decoupled=false → field absent (wire backward-compat).
                        assert_eq!(o.decoupled, None);
                    }
                    _ => panic!("expected open action"),
                }
            }
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_open_decoupled() {
        // P9-1: decoupled=true sets the additive flag the worker reads to
        // keep the channel open after predict_decoupled returns.
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"data"), None, true);
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => match s.action {
                Some(pb::stream_request::Action::Open(o)) => {
                    assert_eq!(o.decoupled, Some(true));
                }
                _ => panic!("expected open action"),
            },
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_chunk() {
        let req = build_stream_chunk("s1".to_string(), bytes::Bytes::from_static(b"chunk"));
        assert_eq!(req.uid, "stream-chunk-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => match s.action {
                Some(pb::stream_request::Action::Chunk(c)) => {
                    assert_eq!(c.data, &b"chunk"[..]);
                }
                _ => panic!("expected chunk action"),
            },
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_close() {
        let req = build_stream_close("s1".to_string());
        assert_eq!(req.uid, "stream-close-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert!(matches!(s.action, Some(pb::stream_request::Action::Close(_))));
            }
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_cancel() {
        let req = build_stream_cancel("s1".to_string());
        assert_eq!(req.uid, "stream-cancel-s1");
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => {
                assert!(matches!(s.action, Some(pb::stream_request::Action::Cancel(_))));
            }
            _ => panic!("expected stream payload"),
        }
    }

    #[test]
    fn test_build_stream_open_with_meta() {
        let meta = pb::RequestMeta {
            route: "/predict".to_string(),
            headers: Default::default(),
            client_ip: "1.2.3.4".to_string(),
            request_id: "r1".to_string(),
            timestamp_ns: 100,
            payload: Default::default(),
            ..Default::default()
        };
        let req = build_stream_open("s1".to_string(), bytes::Bytes::from_static(b"d"), Some(meta), false);
        match req.payload {
            Some(pb::request::Payload::Stream(s)) => match s.action {
                Some(pb::stream_request::Action::Open(o)) => {
                    assert!(o.meta.is_some());
                    assert_eq!(o.meta.unwrap().client_ip, "1.2.3.4");
                }
                _ => panic!("expected open"),
            },
            _ => panic!("expected stream"),
        }
    }

    // --- recv_chunk (P-DEADLINE two-stage) ---

    fn rx_pair() -> (
        tokio::sync::mpsc::Sender<pb::StreamResponse>,
        tokio::sync::mpsc::Receiver<pb::StreamResponse>,
    ) {
        tokio::sync::mpsc::channel(4)
    }
    fn chunk() -> pb::StreamResponse {
        pb::StreamResponse {
            stream_id: "s".to_string(),
            payload: Some(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                data: bytes::Bytes::from_static(b"x"),
                is_final: false,
            })),
        }
    }

    #[tokio::test]
    async fn recv_chunk_no_bounds_is_plain_recv() {
        let (tx, mut rx) = rx_pair();
        tx.send(chunk()).await.unwrap();
        drop(tx); // close → next recv is None
        let first = recv_chunk(&mut rx, None, None).await.unwrap();
        assert!(first.is_some());
        let second = recv_chunk(&mut rx, None, None).await.unwrap();
        assert!(second.is_none(), "channel closed → None");
    }

    #[tokio::test]
    async fn recv_chunk_expired_deadline_is_immediate() {
        let (_tx, mut rx) = rx_pair();
        let past = std::time::Instant::now() - std::time::Duration::from_millis(1);
        let elapsed = recv_chunk(&mut rx, Some(past), None).await.unwrap_err();
        assert_eq!(elapsed, RecvElapsed::Deadline);
    }

    #[tokio::test]
    async fn recv_chunk_idle_fires_on_stall() {
        let (_tx, mut rx) = rx_pair();
        // No sender, tiny idle → must trip Idle promptly.
        let elapsed = recv_chunk(
            &mut rx,
            None,
            Some(std::time::Duration::from_millis(20)),
        )
        .await
        .unwrap_err();
        assert_eq!(elapsed, RecvElapsed::Idle);
    }

    #[tokio::test]
    async fn recv_chunk_chunk_arrives_in_time() {
        let (tx, mut rx) = rx_pair();
        tx.send(chunk()).await.unwrap();
        let got = recv_chunk(
            &mut rx,
            Some(std::time::Instant::now() + std::time::Duration::from_secs(1)),
            Some(std::time::Duration::from_secs(1)),
        )
        .await
        .unwrap();
        assert!(got.is_some());
    }

    #[tokio::test]
    async fn recv_chunk_deadline_fires_when_shorter_than_idle() {
        let (_tx, mut rx) = rx_pair();
        // overall deadline is the tighter bound and fires first.
        let elapsed = recv_chunk(
            &mut rx,
            Some(std::time::Instant::now() + std::time::Duration::from_millis(20)),
            Some(std::time::Duration::from_secs(60)),
        )
        .await
        .unwrap_err();
        assert_eq!(elapsed, RecvElapsed::Deadline);
    }

    // --- P0-2 evidence (resource-leak sweep 2026-08-16) ---

    /// The SSE/WS/gRPC forward loops send with `send_bounded(stream_deadline,
    /// stream_idle, ...)`. The send sits OUTSIDE the select! that polls
    /// recv_chunk, so a stopped reader that fills the bounded event channel
    /// makes the send block; without the idle leg the always-on chunk-idle
    /// bound is never polled again, and the keepalive ticker is also in the
    /// select and equally starved. N half-open clients can pin N admission
    /// slots + worker streams + streaming permits.
    ///
    /// Fixed code bounds the send by the idle budget (`send_bounded`'s idle
    /// leg); pre-fix code blocked unbounded.
    #[tokio::test]
    async fn send_backpressure_defeats_recv_idle_reclaim() {
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<pb::StreamResponse>(64);
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel::<pb::StreamResponse>(64);
        // A stopped reader: `_event_rx` is never polled. Fill the bounded
        // event channel so any send into it blocks.
        for _ in 0..64 {
            event_tx.send(chunk()).await.unwrap();
        }
        // Worker emits one chunk, then closes its stream.
        chunk_tx.send(chunk()).await.unwrap();
        drop(chunk_tx);

        let idle = Duration::from_millis(100);
        let task = tokio::spawn(async move {
            loop {
                let c = tokio::select! {
                    c = recv_chunk(&mut chunk_rx, None, Some(idle)) => c,
                    _ = std::future::pending::<()>() => break, // keepalive off
                };
                match c {
                    Ok(Some(chunk)) => {
                        // The idle leg bounds the send even with no armed
                        // overall deadline — a stopped client can no longer
                        // pin the forward task past one chunk-idle budget.
                        let _ = send_bounded(None, Some(idle), event_tx.send(chunk)).await;
                    }
                    _ => break,
                }
            }
        });

        // Contract: chunk-idle must reclaim the forward task even under send
        // backpressure. 2s is 20x the 100ms idle — a bounded send would have
        // tripped long before. Current code: the task is still alive.
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("forward task must be reclaimed by the idle bound despite send backpressure")
            .expect("forward task must not panic");
    }
}
