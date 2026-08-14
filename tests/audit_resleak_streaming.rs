//! /audit evidence tests for resource-leak-plan.md streaming-side defects.
//! All six tests MUST FAIL on the current code (each asserts behaviour the
//! implementation does not provide):
//!
//! | ID   | Defect                                                            | Evidence anchors                                            |
//! |------|-------------------------------------------------------------------|-------------------------------------------------------------|
//! | K3   | WS never emits server Ping keepalive frames                       | src/http/handlers/stream.rs:1737 ws_sink.send sends Text/Binary only, no keepalive ticker |
//! | K4   | SSE never emits `: keepalive` comment frames                      | src/http/handlers/stream.rs:520-560,639 Event::default().data(...) only |
//! | L1   | Stream send side has no deadline; a stopped reader pins the conn  | src/http/handlers/stream.rs:1737 bare ws_sink.send await; recv loop unpolled while send blocks |
//! | RN-6 | decoupled_idle_timeout_secs=0 -> zero-traffic stream never reaped | src/deadline.rs:171-177 idle_budget returns None -> recv_chunk is a pure recv (stream.rs:510/1641) |
//! | RN-13| Admission slot released when headers are produced; streaming bypasses max_inflight | src/http/mod.rs:250-283 _guard spans only next.run |
//! | RN-14| 64-slot channel truncates bursts with a synthetic Error frame    | src/transport/zmq.rs:17,397,425 STREAM_CHANNEL_SIZE=64; zmq.rs:237-254 overflow -> truncate + Error + route removal |
//!
//! Real server subprocess + Python LitAPI fixtures (same harness shape as
//! tests/audit_stream_endpoints.rs); port range 23100-23199.

use serde_json::json;
use serial_test::serial;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

// ---------------------------------------------------------------------------
// Harness (same shape as tests/audit_stream_endpoints.rs; test targets do not
// share code)
// ---------------------------------------------------------------------------

fn project_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn lite_server_bin() -> std::path::PathBuf {
    project_root()
        .join("target")
        .join("debug")
        .join("lite-server-core")
}

/// Audit port segment 23100-23199.
fn next_test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(23100);
    let p = NEXT.fetch_add(1, Ordering::Relaxed);
    assert!(p < 23200, "audit port range 23100-23199 exhausted");
    p
}

fn start_server(args: &[&str]) -> Child {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .env("LITESERVER_DIE_WITH_PARENT", "1");
    for arg in args {
        cmd.arg(arg);
    }
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    cmd.spawn().expect("Failed to start server")
}

fn stop_server(mut child: Child) {
    #[cfg(unix)]
    {
        let pid = child.id() as i32;
        let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

struct ServerGuard(Option<Child>);

impl ServerGuard {
    fn start(args: &[&str]) -> Self {
        ServerGuard(Some(start_server(args)))
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            stop_server(child);
        }
    }
}

fn kill_stale_on_port(port: u16) {
    let output = Command::new("lsof")
        .args(["-ti", &format!(":{}", port)])
        .output();
    if let Ok(out) = output {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid_str in pids.lines() {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                let ps = Command::new("ps")
                    .args(["-p", &pid.to_string(), "-o", "comm="])
                    .output();
                if let Ok(ps_out) = ps {
                    let comm = String::from_utf8_lossy(&ps_out.stdout);
                    if comm.contains("lite-server") {
                        unsafe { libc::kill(pid, libc::SIGKILL); }
                    }
                }
            }
        }
    }
}

fn http_client() -> reqwest::Client {
    // macOS system proxy would forward loopback into the proxy — disable it.
    reqwest::Client::builder().no_proxy().build().unwrap()
}

async fn wait_for_server(port: u16, timeout_secs: u64) {
    let client = http_client();
    let url = format!("http://127.0.0.1:{}/health", port);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).timeout(Duration::from_secs(1)).send().await {
            if resp.status() == 200 {
                return;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("Server did not start within {} seconds", timeout_secs);
}

async fn wait_model_ready(base: &str, model: &str, timeout_secs: u64) -> bool {
    let client = http_client();
    let url = format!("{}/v2/models/{}/ready", base, model);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).timeout(Duration::from_secs(1)).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if body["ready"].as_bool() == Some(true) {
                    return true;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn wait_ready_all(base: &str, models: &[&str]) {
    for m in models {
        assert!(
            wait_model_ready(base, m, 30).await,
            "model {} did not become ready",
            m
        );
    }
}

fn write_model_py(repo: &std::path::Path, name: &str, py: &str) {
    let dir = repo.join(name).join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("model.py"), py).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// Stall: the stream opens but never produces a chunk (the generator sleeps
/// 60s per iteration) — keeps the downstream stream open and silent.
fn write_stall(repo: &std::path::Path) {
    write_model_py(
        repo,
        "stall",
        r#"import time
from lite_server import LitAPI


class StallAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        return {"token": "stall"}

    def stream_predict(self, request, ctx=None):
        while True:
            time.sleep(60)
            yield {"token": "stall"}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Flood: 3000 chunks of 64 KiB produced flat-out — enough to fill the zmq
/// socket HWM, the 64-slot channels and the TCP buffers, so the server's send
/// must block once the client stops reading.
fn write_flood(repo: &std::path::Path) {
    write_model_py(
        repo,
        "flood",
        r#"from lite_server import LitAPI


class FloodAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        return {"token": "flood"}

    def stream_predict(self, request, ctx=None):
        for _ in range(3000):
            yield {"token": "x" * 65536}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Burst: exactly N chunks of 64 KiB back-to-back, then the stream ends —
/// exercises the 64-slot channel truncation under a high-throughput burst.
fn write_burst(repo: &std::path::Path, n: usize) {
    write_model_py(
        repo,
        "burst500",
        &format!(
            r#"from lite_server import LitAPI


class BurstAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        return {{"token": "burst"}}

    def stream_predict(self, request, ctx=None):
        for i in range({n}):
            yield {{"i": i, "t": "x" * 65536}}

    async def encode_response(self, output, ctx=None):
        return output
"#,
        ),
    );
}

/// Write a per-test server YAML; `extra` is injected verbatim into the
/// `server:` section (e.g. "  timeout: 2.0\n").
fn write_server_yaml(
    repo: &std::path::Path,
    http_port: u16,
    extra: &str,
    models: &[&str],
) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lite-server-audit-rl-yaml-{}-{}",
        std::process::id(),
        http_port
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("server.yaml");
    let load_list = models
        .iter()
        .map(|m| format!("    - {m}\n"))
        .collect::<String>();
    std::fs::write(
        &path,
        format!(
            "server:\n  http_port: {http_port}\n{extra}\n\n\
             model_repository:\n  path: {}\n\n\
             orchestration:\n  control_mode: explicit\n  load_models:\n{load_list}",
            repo.display()
        ),
    )
    .unwrap();
    path
}

/// Boot a fresh server + fixture repo; returns (http base, guard, repo).
async fn boot(extra: &str, models: &[&str]) -> (String, ServerGuard, std::path::PathBuf) {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir().join(format!(
        "lite-server-audit-rl-{}-{}",
        std::process::id(),
        http_port
    ));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();
    for m in models {
        match *m {
            "stall" => write_stall(&repo),
            "flood" => write_flood(&repo),
            "burst500" => write_burst(&repo, 500),
            other => panic!("unknown fixture {other}"),
        }
    }
    let server_yaml = write_server_yaml(&repo, http_port, extra, models);
    let guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    (
        format!("http://127.0.0.1:{}", http_port),
        guard,
        repo,
    )
}

fn port_of(base: &str) -> u16 {
    base.trim_start_matches("http://127.0.0.1:")
        .parse()
        .unwrap()
}

/// Current value of `liteserver_streaming_connections{model,protocol}` (labels
/// model/version/protocol; only model+protocol filtered, version may vary).
async fn streaming_gauge(base: &str, model: &str, protocol: &str) -> Option<f64> {
    let text = http_client()
        .get(format!("{base}/metrics"))
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    for line in text.lines() {
        if line.starts_with(&format!("liteserver_streaming_connections{{model=\"{model}\""))
            && line.contains(&format!("protocol=\"{protocol}\""))
        {
            return line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok());
        }
    }
    None
}

async fn wait_gauge(base: &str, model: &str, protocol: &str, want: f64, timeout_secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if let Some(v) = streaming_gauge(base, model, protocol).await {
            if (v - want).abs() < f64::EPSILON {
                return;
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    panic!(
        "gauge liteserver_streaming_connections[{model},{protocol}] never reached {want}"
    );
}

// ---------------------------------------------------------------------------
// K3: WS keepalive — the server must emit Ping frames on an idle stream.
// Current behaviour: the writer loop (stream.rs:1737) only sends Text/Binary,
// no keepalive ticker exists, so no Ping ever arrives. FAIL.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_k3_ws_no_server_ping_keepalive() {
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;

    let (base, _guard, _repo) = boot("  timeout: 30.0", &["stall"]).await;
    wait_ready_all(&base, &["stall"]).await;
    let http_port = port_of(&base);
    let ws_url = format!("ws://127.0.0.1:{http_port}/v2/models/stall/stream");
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");

    // Send NO client message. The writer loop only emits Text/Binary frames —
    // there is no keepalive ticker, so no Ping can ever reach the client.
    let mut saw_ping = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(Message::Ping(_)))) => {
                saw_ping = true;
                break;
            }
            Ok(Some(Ok(Message::Pong(_)))) => {} // our own auto-pong echo; ignore
            Ok(Some(Ok(_))) => {}                // data/close — not a keepalive
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {} // read timeout — keep waiting for a Ping
        }
    }
    assert!(
        saw_ping,
        "K3: the WS server must emit a Ping keepalive within 5s of an idle stream; \
         none arrived (stream.rs:1737 sends only Text/Binary — no keepalive ticker)"
    );
}

// ---------------------------------------------------------------------------
// K4: SSE keepalive — the feed must emit `: keepalive` comment frames.
// Current behaviour: the feed loop (stream.rs:520-560,639) emits only
// `data:` events, so the stalled stream is silent. FAIL.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_k4_sse_no_keepalive_comment() {
    use futures::StreamExt;

    let (base, _guard, _repo) = boot("  timeout: 30.0", &["stall"]).await;
    wait_ready_all(&base, &["stall"]).await;
    let resp = http_client()
        .post(format!("{base}/v2/models/stall/events"))
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream")
        .json(&json!({"text": "hi"}))
        .send()
        .await
        .expect("SSE stream must open");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "SSE stream opens");
    let mut body = resp.bytes_stream();

    let mut saw_keepalive = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), body.next()).await {
            Ok(Some(Ok(bytes))) => {
                if String::from_utf8_lossy(&bytes).contains(": keepalive") {
                    saw_keepalive = true;
                    break;
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {} // no bytes — keep waiting for the keepalive
        }
    }
    assert!(
        saw_keepalive,
        "K4: the SSE feed must emit `: keepalive` comment frames within 5s of an idle \
         stream; none arrived (stream.rs:520-560,639 emits only `data:` events)"
    );
}

// ---------------------------------------------------------------------------
// L1: WS send-side deadline — with server.timeout=2.0 AND a client-armed
// `x-lite-timeout: 2` overall deadline, a client that stops reading must be
// reaped. Current behaviour: ws_sink.send (stream.rs:1737) blocks on
// backpressure and the recv loop is never polled again, so even an armed
// deadline can never fire — the stream stays open (observed via the
// liteserver_streaming_connections gauge, which is only decremented at stream
// terminal). NOTE: a read-based probe would drain the TCP buffers and unblock
// the server, so the assertion must observe the server side, not the socket.
// FAIL.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_l1_ws_send_no_deadline_holds_connection() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    let (base, _guard, _repo) = boot("  timeout: 2.0", &["flood"]).await;
    wait_ready_all(&base, &["flood"]).await;
    let http_port = port_of(&base);
    // Arm the overall stream deadline (2s) from the client side so the recv
    // loop HAS a deadline to enforce — the claim is that it cannot fire while
    // the send is blocked. Build the handshake request first (which generates
    // Sec-WebSocket-Key & co.), then add the timeout header on top.
    let mut request = format!("ws://127.0.0.1:{http_port}/v2/models/flood/stream")
        .into_client_request()
        .expect("build ws request");
    request.headers_mut().insert(
        "x-lite-timeout",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static("2"),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.expect("WS connect");
    ws.send(Message::Text(r#"{"text":"go"}"#.into()))
        .await
        .unwrap();

    // Confirm the flood flows, then STOP reading entirely (no further poll).
    let _f1 = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("first chunk")
        .unwrap()
        .unwrap();
    let _f2 = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("second chunk")
        .unwrap()
        .unwrap();
    // The stream must be registered in the gauge before the hold starts.
    wait_gauge(&base, "flood", "websocket", 1.0, 5).await;

    // T=4.5s: the 2.0s deadline must already have reclaimed the stream — but
    // the send is blocked on the full TCP window and the recv loop (which
    // owns the deadline) is never polled, so the stream is still alive.
    sleep(Duration::from_millis(4500)).await;
    let reclaimed = match streaming_gauge(&base, "flood", "websocket").await {
        Some(v) if v <= 0.0 => true,
        None => true, // metric absent == gauge 0
        _ => false,
    };
    // Cleanup: dropping the client unblocks the server's send (its channel
    // overflowed) and lets it terminate.
    drop(ws);
    assert!(
        reclaimed,
        "L1: with a 2.0s armed deadline a stopped-reader flood must be reaped by \
         ~4.5s; liteserver_streaming_connections[flood,websocket] is still 1 — \
         ws_sink.send (stream.rs:1737) has no deadline and blocks the recv loop"
    );
}

// ---------------------------------------------------------------------------
// RN-6: decoupled_idle_timeout_secs=0 — a zero-traffic stream is never
// reclaimed after the client disconnects. idle_budget returns None
// (deadline.rs:171-177), recv_chunk becomes a pure recv, the forward task
// hangs, and liteserver_streaming_connections never returns to 0. FAIL.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_rn6_idle_zero_never_reclaims_stalled_stream() {
    let (base, _guard, _repo) = boot("  decoupled_idle_timeout_secs: 0\n  timeout: 30.0", &["stall"]).await;
    wait_ready_all(&base, &["stall"]).await;
    let resp = http_client()
        .post(format!("{base}/v2/models/stall/events"))
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream")
        .json(&json!({"text": "hi"}))
        .send()
        .await
        .expect("SSE stream must open");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "SSE stream opens");

    // The gauge must first show the stream open.
    wait_gauge(&base, "stall", "sse", 1.0, 5).await;
    // Client disconnects after 1s (drops the response body).
    sleep(Duration::from_secs(1)).await;
    drop(resp);

    // With idle=0 nothing reclaims the stream: the forward task stays blocked
    // in recv_chunk, so the gauge must drop back to 0 within 5s — it does not.
    let mut reclaimed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match streaming_gauge(&base, "stall", "sse").await {
            Some(v) if v <= 0.0 => {
                reclaimed = true;
                break;
            }
            None => {
                // Metric absent == gauge 0 (never re-inc'd).
                reclaimed = true;
                break;
            }
            _ => {}
        }
        sleep(Duration::from_millis(250)).await;
    }
    assert!(
        reclaimed,
        "RN-6: with decoupled_idle_timeout_secs=0 a zero-traffic stream must be \
         reclaimed within 5s of the client disconnect; \
         liteserver_streaming_connections stays at 1 (stream.rs:510 recv_chunk \
         is a pure recv when idle_budget is None)"
    );
}

// ---------------------------------------------------------------------------
// RN-13: max_inflight admission — the slot must be held for the stream's
// lifetime. Current behaviour: the guard (http/mod.rs:250-283) only spans
// next.run, so the slot is released as soon as the SSE headers are produced
// and every sequential stream slips past the cap. FAIL.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_rn13_admission_slot_released_at_headers() {
    let (base, _guard, _repo) = boot("  max_inflight: 1\n  timeout: 30.0", &["stall"]).await;
    wait_ready_all(&base, &["stall"]).await;
    let mut statuses = Vec::new();
    for i in 0..5 {
        let resp = http_client()
            .post(format!("{base}/v2/models/stall/events"))
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .json(&json!({"text": "hi"}))
            .send()
            .await
            .expect("SSE stream must open");
        statuses.push(resp.status().as_u16());
        drop(resp); // leave the stalled body undrained
        if i < 4 {
            sleep(Duration::from_millis(500)).await;
        }
    }
    assert!(
        statuses.iter().any(|s| *s == 503),
        "RN-13: with max_inflight=1 the admission slot must be held for the stream's \
         lifetime — all 5 sequential SSE streams returned 200; the slot was released \
         when the headers were produced: {statuses:?}"
    );
}

// ---------------------------------------------------------------------------
// RN-14: burst throughput — a burst of 200 chunks must arrive intact. Current
// behaviour: the 64-slot worker channel (zmq.rs STREAM_CHANNEL_SIZE=64,
// overflow at zmq.rs:237-254) truncates the burst and delivers a synthetic
// Error frame. FAIL.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_rn14_burst_truncated_by_64_slot_channel() {
    use futures::StreamExt;

    let (base, _guard, _repo) = boot("  timeout: 30.0", &["burst500"]).await;
    wait_ready_all(&base, &["burst500"]).await;
    let resp = http_client()
        .post(format!("{base}/v2/models/burst500/events"))
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream")
        .json(&json!({"text": "go"}))
        .send()
        .await
        .expect("SSE stream must open");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "SSE stream opens");
    let mut body = resp.bytes_stream();

    // Accumulate the whole body, then count chunk markers ("i":N — one per
    // yield) and look for the synthetic truncation error frame.
    //
    // Pacing: the overflow (zmq.rs:237-254) fires only when the consumer lags
    // the producer by more than the 64-slot channel. A localhost client that
    // reads at full speed keeps up with the Python producer, so the race wins
    // the other way and no truncation is observed (measured: racy at 500
    // chunks). A 1ms/chunk read pace (~1000 chunks/s vs the producer's ~50k/s)
    // is still a fast consumer in real terms yet lags far more than 64 chunks
    // — making the truncation reproducible.
    let mut all = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), body.next()).await {
            Ok(Some(Ok(bytes))) => {
                all.push_str(&String::from_utf8_lossy(&bytes));
                sleep(Duration::from_millis(1)).await;
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    let chunks = all.matches("\"i\":").count();
    let saw_truncation = all.contains("\"error\"");
    assert_eq!(
        chunks, 500,
        "RN-14: a burst of 500 chunks must all arrive; the 64-slot worker channel \
         (zmq.rs STREAM_CHANNEL_SIZE=64) truncated the burst at {chunks} chunks"
    );
    assert!(
        !saw_truncation,
        "RN-14: no synthetic truncation Error frame may be delivered; one was \
         received (zmq.rs:237-254 overflow path)"
    );
}
