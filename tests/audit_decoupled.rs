//! Audit evidence tests — P9-1 DecoupledInfer 1:N (蓝图 §4.4).
//!
//! Each test FAILS on current code, demonstrating a confirmed defect found by
//! the targeted audit. They exercise the shared ZMQ stream machinery that
//! DecoupledInfer is built on (`build_stream_open(decoupled=true)` →
//! `WorkerZmqClient::send_stream` → forward → `build_stream_cancel`).
//!
//! Defect summary:
//! - B1 (resource): a stream cancelled via `build_stream_cancel` (the normal
//!   client-disconnect / idle-timeout / deadline path — the worker's cancel
//!   semantics send NO terminal frame) never gets its `stream_routes` entry
//!   removed from the ZMQ actor. The route (an mpsc::Sender plus up to 64
//!   buffered chunks) leaks until the worker process restarts or unloads.
//! - B2 (backpressure): a slow consumer lets the actor→forwarder mpsc(64)
//!   overflow; the actor drops the route and every remaining worker frame —
//!   including the terminal Done — is silently discarded. The receiver
//!   observes a clean end-of-stream (None) with no terminal frame and no
//!   error, and the close is recorded as 2xx (WorkerEof→2xx, D7 mapping).

use lite_server::proto::liteserver as pb;
use lite_server::streaming;
use lite_server::transport::zmq::WorkerZmqClient;
use prost::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Shared log capture (B1 evidence oracle: the actor's "Stream channel full or
// closed" warn fires only when a route is still present for a dead receiver —
// i.e. it PROVES the route survived the cancel).
// ---------------------------------------------------------------------------

struct BufWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for BufWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Install a process-global fmt subscriber writing into a shared buffer.
/// Global default is set once, before any actor activity — no scoped-default
/// callsite-interest flakiness.
fn log_buffer() -> Arc<Mutex<Vec<u8>>> {
    static BUF: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    BUF.get_or_init(|| {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer_buf = buf.clone();
        let _ = tracing_subscriber::fmt()
            .with_writer(move || BufWriter(writer_buf.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .try_init();
        buf
    })
    .clone()
}

fn captured(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

// ---------------------------------------------------------------------------
// Fake worker helpers
// ---------------------------------------------------------------------------

fn test_endpoint(tag: &str) -> String {
    #[cfg(unix)]
    {
        let sock = std::env::temp_dir().join(format!(
            "lite-server-audit-decoupled-{}-{}.sock",
            tag,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&sock);
        format!("ipc://{}", sock.display())
    }
    #[cfg(windows)]
    {
        format!("tcp://127.0.0.1:{}", 35000 + (std::process::id() % 1000))
    }
}

fn stream_chunk_resp(stream_id: &str, data: &[u8]) -> pb::Response {
    pb::Response {
        uid: format!("stream-chunk-{stream_id}"),
        payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
            stream_id: stream_id.to_string(),
            payload: Some(pb::stream_response::Payload::Chunk(
                pb::StreamChunkResponse {
                    data: bytes::Bytes::copy_from_slice(data),
                    is_final: false,
                },
            )),
        })),
        metrics: None,
    }
}

fn unary_ok(uid: &str) -> pb::Response {
    pb::Response {
        uid: uid.to_string(),
        payload: Some(pb::response::Payload::Single(pb::SingleResponse {
            data: bytes::Bytes::from_static(b"{}"),
            headers: HashMap::new(),
            status: Some(pb::Status {
                code: "Ok".to_string(),
                message: String::new(),
            }),
            ..Default::default()
        })),
        metrics: None,
    }
}

fn open_req(stream_id: &str) -> pb::Request {
    streaming::build_stream_open(stream_id.to_string(), bytes::Bytes::from_static(b"{}"), None, true)
}

fn ping_req(uid: &str) -> pb::Request {
    pb::Request {
        uid: uid.to_string(),
        meta: None,
        payload: Some(pb::request::Payload::Single(pb::SingleRequest {
            data: bytes::Bytes::from_static(b"{}"),
        })),
    }
}

// ---------------------------------------------------------------------------
// B1: cancel does not reclaim the actor's stream route
// ---------------------------------------------------------------------------

/// P9-1 lifecycle: client disconnect / idle timeout / deadline all end the
/// decoupled forwarder with `build_stream_cancel` (send_raw, fire-and-forget),
/// and the worker's cancel semantics deliberately send NO terminal frame back
/// (python `_ResponseSender.cancel`: "No StreamDone is sent"). The ZMQ actor
/// only removes a stream route when a Done/Error frame arrives or a try_send
/// fails — so a cancelled stream that never produces another frame leaks its
/// `stream_routes` entry (mpsc::Sender + buffered chunks) until the worker
/// process restarts.
///
/// Oracle: after the cancel has been processed, make the worker emit one late
/// frame for the cancelled stream. If the route was reclaimed at cancel time
/// the late frame is dropped silently; if the route LEAKED, the actor's
/// try_send hits the dead channel and logs
/// `Stream channel full or closed for <sid>`. The warn's presence proves the
/// route survived the cancel.
#[tokio::test]
async fn test_audit_resource_cancel_leaves_stream_route_orphaned() {
    let buf = log_buffer();
    let sid = format!("audit-route-leak-{}", uuid::Uuid::new_v4());
    let endpoint = test_endpoint("routeleak");

    let (ep, sid_w) = (endpoint.clone(), sid.clone());
    let worker = std::thread::spawn(move || {
        let ctx = zmq::Context::new();
        let s = ctx.socket(zmq::PAIR).expect("worker socket");
        s.connect(&ep).expect("worker connect");
        let _ = s.set_rcvtimeo(8000);
        loop {
            let bytes = match s.recv_bytes(0) {
                Ok(b) => b,
                Err(_) => return, // rcvtimeo elapsed — test over
            };
            let req = match pb::Request::decode(bytes.as_slice()) {
                Ok(r) => r,
                Err(_) => continue,
            };
            match req.payload {
                Some(pb::request::Payload::Stream(st)) => match st.action {
                    Some(pb::stream_request::Action::Open(_)) => {
                        // One chunk so the test can confirm the route is live.
                        let _ = s.send(stream_chunk_resp(&sid_w, b"first").encode_to_vec(), 0);
                    }
                    Some(pb::stream_request::Action::Cancel(_)) => {
                        // P9-1 cancel semantics: flag closed, send NOTHING back.
                    }
                    _ => {}
                },
                Some(pb::request::Payload::Single(_)) => {
                    if req.uid == "trigger-late" {
                        // Late frame for the cancelled stream. PAIR is FIFO per
                        // direction, so the actor processes this frame before
                        // the unary reply below reaches the test — by the time
                        // send() returns, the warn (if any) has been logged.
                        let _ = s.send(stream_chunk_resp(&sid_w, b"late").encode_to_vec(), 0);
                    }
                    let _ = s.send(unary_ok(&req.uid).encode_to_vec(), 0);
                }
                _ => {}
            }
        }
    });

    let client = WorkerZmqClient::new(endpoint);
    tokio::time::sleep(Duration::from_millis(200)).await; // bind + connect

    // 1. Open the decoupled stream; the route is registered on send success.
    let mut chunk_rx = client
        .send_stream(open_req(&sid), sid.clone())
        .await
        .expect("open stream");
    // Route live: the worker's first chunk arrives.
    tokio::time::timeout(Duration::from_secs(3), chunk_rx.recv())
        .await
        .expect("first chunk timed out")
        .expect("stream channel closed early");

    // 2. Client disconnect: the forwarder breaks and drops chunk_rx...
    drop(chunk_rx);
    // ...then the cleanup path sends the cancel (fire-and-forget).
    client
        .send_raw(streaming::build_stream_cancel(sid.clone()))
        .await
        .expect("send cancel");

    // 3. Barrier: unary round-trip guarantees the actor drained the cancel
    //    command (commands are FIFO on one channel).
    client
        .send_with_timeout(ping_req("ping-1"), Duration::from_secs(3))
        .await
        .expect("ping-1");

    // 4. Late frame for the cancelled stream + FIFO barrier (see worker).
    client
        .send_with_timeout(ping_req("trigger-late"), Duration::from_secs(3))
        .await
        .expect("trigger-late");

    let logs = captured(&buf);
    drop(client);
    let _ = worker.join();

    assert!(
        !logs.contains(&format!("Stream channel full or closed for {sid}")),
        "B1: the actor still held the route for cancelled stream {sid} — the\n\
         cancel command never reclaims stream_routes, so a cancelled stream\n\
         that produces no further frames leaks the route (mpsc::Sender +\n\
         buffered chunks) until worker restart. Actor log:\n{logs}"
    );
}

// ---------------------------------------------------------------------------
// B2: slow consumer → silent chunk loss + clean (non-terminal) stream end
// ---------------------------------------------------------------------------

/// Backpressure: the actor forwards worker frames into a per-stream mpsc(64)
/// with try_send. When the consumer is slow (decoupled forwarder blocked on a
/// stalled gRPC client), the channel overflows: the actor drops the route and
/// silently discards every remaining frame — including the terminal Done.
/// The receiver then observes `None` (clean end-of-stream) with no Done/Error
/// terminal frame; the gRPC decoupled forwarder maps this to WorkerEof → 2xx
/// and ends the client stream with Status OK and no is_final frame.
///
/// Expected: a terminated stream must be distinguishable from normal
/// completion — either all frames are delivered (backpressure propagated) or
/// a terminal (Done/Error) frame is observed. Neither holds today.
#[tokio::test]
async fn test_audit_backpressure_slow_consumer_silent_chunk_loss() {
    log_buffer(); // keep actor warns out of the global subscriber-less void
    let sid = format!("audit-trunc-{}", uuid::Uuid::new_v4());
    let endpoint = test_endpoint("trunc");
    const CHUNKS: u32 = 200;

    let (ep, sid_w) = (endpoint.clone(), sid.clone());
    let worker = std::thread::spawn(move || {
        let ctx = zmq::Context::new();
        let s = ctx.socket(zmq::PAIR).expect("worker socket");
        s.connect(&ep).expect("worker connect");
        let _ = s.set_rcvtimeo(8000);
        loop {
            let bytes = match s.recv_bytes(0) {
                Ok(b) => b,
                Err(_) => return,
            };
            let req = match pb::Request::decode(bytes.as_slice()) {
                Ok(r) => r,
                Err(_) => continue,
            };
            if matches!(
                req.payload,
                Some(pb::request::Payload::Stream(pb::StreamRequest {
                    action: Some(pb::stream_request::Action::Open(_)),
                    ..
                }))
            ) {
                // Model pushes 200 chunks then closes (Done).
                for i in 0..CHUNKS {
                    let _ = s.send(
                        stream_chunk_resp(&sid_w, &i.to_le_bytes()).encode_to_vec(),
                        0,
                    );
                }
                let done = pb::Response {
                    uid: format!("stream-done-{sid_w}"),
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: sid_w.clone(),
                        payload: Some(pb::stream_response::Payload::Done(pb::StreamDone {
                            metrics: None,
                        })),
                    })),
                    metrics: None,
                };
                let _ = s.send(done.encode_to_vec(), 0);
            }
        }
    });

    let client = WorkerZmqClient::new(endpoint);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut chunk_rx = client
        .send_stream(open_req(&sid), sid.clone())
        .await
        .expect("open stream");

    // Stall the consumer: let the actor overflow the 64-deep channel.
    tokio::time::sleep(Duration::from_millis(700)).await;

    let mut frames = 0u32;
    let mut saw_terminal = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), chunk_rx.recv()).await {
            Ok(Some(resp)) => {
                frames += 1;
                if matches!(
                    resp.payload,
                    Some(pb::stream_response::Payload::Done(_))
                        | Some(pb::stream_response::Payload::Error(_))
                ) {
                    saw_terminal = true;
                }
            }
            Ok(None) => break, // channel closed (route dropped)
            Err(_) => break,   // no more frames
        }
    }

    drop(client);
    let _ = worker.join();

    assert!(
        frames == CHUNKS + 1 || saw_terminal,
        "B2: silent truncation — worker sent {CHUNKS} chunks + Done; the \
         stalled consumer received {frames} frame(s) and then a clean \
         end-of-stream with NO terminal frame (saw_terminal={saw_terminal}). \
         The remaining frames were silently discarded and the close is \
         recorded as WorkerEof→2xx: data loss indistinguishable from normal \
         completion (no is_final, gRPC Status OK)."
    );
}
