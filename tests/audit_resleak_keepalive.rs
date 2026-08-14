//! Audit evidence tests — .claude/resource-leak-plan.md K1/K2 (server
//! keepalive family) + L3 (request_timeout) probe (2026-08-15). K1 and K2
//! FAIL on current code; L3 is a passing probe documenting the default.
//!
//! Defect summary (plan IDs; in-crate locations):
//! - K1  a positive `server.keepalive_timeout` never takes effect. The
//!   keepalive middleware (src/http/mod.rs:442) only consumes the `<= 0.0`
//!   branch (adds `Connection: close`); none of the three serve paths
//!   configures a hyper idle timer — `serve_unix` (src/http/mod.rs:590-625)
//!   builds a bare `hyper_util::server::conn::auto::Builder` with no
//!   `http1().timer()`, the TCP path uses `axum::serve` (no builder
//!   surface), and the TLS path uses axum-server without `http_builder()`.
//!   The default `keepalive_timeout: 5.0` (src/config.rs:238) therefore
//!   never reaps idle connections.
//! - K2  with `keepalive_timeout = 0` the server only appends `Connection:
//!   close` to responses — an h1-only semantic — but no serve path sets
//!   `http1_only()`. hyper-util's auto builder starts as H2 and only falls
//!   back to H1 when the first bytes fail to match the h2 preface
//!   (hyper-util-0.1.20 auto/mod.rs `read_version`), so the UDS path accepts
//!   h2c prior-knowledge connections and answers with an HTTP/2 SETTINGS
//!   frame.
//! - L3  (passing probe) `request_timeout: 0.0` is the model-level default
//!   (src/config.rs:1113) and `WarmupPolicy::effective_timeout(0.0)` returns
//!   None — an unbounded wait is the DEFAULT, which is the premise of the L3
//!   defect (an explicit 0 cannot be distinguished from unset).
//!
//! Harness mirrors tests/audit_fd.rs `fd_c`: an in-process
//! `start_http_server` on a unix socket, driven with raw socket bytes.

/// K1 / K2: drive the real server in-process over a unix socket
/// (lite_server::http::start_http_server).
#[cfg(unix)]
mod keepalive_server {
    use lite_server::callback::CallbackRunner;
    use lite_server::config::Config;
    use lite_server::http::{start_http_server, HttpServerOptions};
    use lite_server::inference_queue::InferenceQueue;
    use lite_server::rate_limit::RateLimiter;
    use lite_server::registry::ModelRegistry;
    use lite_server::server::ShutdownState;
    use lite_server::worker::WorkerManager;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    /// K1: keepalive_timeout is 1.0 s, so 1.6 s of idle must already have
    /// reaped the connection.
    const K1_IDLE: Duration = Duration::from_millis(1600);
    /// K1: how long the post-idle read waits for EOF. 2.5 s clears the idle
    /// budget with margin, so only a missing server-side timer can exhaust it.
    const K1_READ_TIMEOUT: Duration = Duration::from_millis(2500);
    /// K2: how long the server may stay silent after the h2c preface before
    /// the "rejected" verdict.
    const K2_SILENCE_BUDGET: Duration = Duration::from_secs(2);

    struct TestDeps {
        config: Config,
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        inference_queue: Arc<InferenceQueue>,
    }

    fn sock_path(tag: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lite-audit-resleak-{tag}-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn build_deps(sock: &Path, ka: f32) -> TestDeps {
        let mut config = Config::default();
        // K1 exercises the positive branch (ka = 1.0), K2 the ka = 0 branch
        // (disable_keepalive_middleware, src/http/mod.rs:442).
        config.server.keepalive_timeout = ka;
        config.server.host = format!("unix:{}", sock.display());
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            inference_queue.clone(),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        TestDeps {
            config,
            registry,
            worker_manager,
            inference_queue,
        }
    }

    async fn spawn_test_server(
        sock: &Path,
        deps: TestDeps,
    ) -> (tokio::sync::oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let options = HttpServerOptions {
            config: deps.config,
            registry: deps.registry,
            worker_manager: deps.worker_manager,
            inference_queue: deps.inference_queue,
            shutdown_state: Arc::new(ShutdownState::new()),
            draining: Arc::new(AtomicBool::new(false)),
            callback_runner: Arc::new(CallbackRunner::new()),
            has_hot_reload: Arc::new(AtomicBool::new(false)),
            rate_limiter: Arc::new(RateLimiter::default()),
            tls: None,
        };
        let handle = tokio::spawn(async move {
            let _ = start_http_server(options, shutdown_rx).await;
        });
        // bind() creates the socket file synchronously; the accept loop follows
        // immediately, so a queued connect is safe once the file exists.
        for _ in 0..200 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(sock.exists(), "test server did not bind {}", sock.display());
        (shutdown_tx, handle)
    }

    /// Read one HTTP/1.1 response (head + body) on an existing connection and
    /// return (status, body). `rx_buf` accumulates raw stream bytes so a
    /// coalesced/pipelined segment of a following response survives across
    /// calls.
    async fn read_response(stream: &mut UnixStream, rx_buf: &mut Vec<u8>) -> (u16, Vec<u8>) {
        let mut chunk = [0u8; 8192];
        while !rx_buf.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut chunk).await.expect("read response head");
            assert!(n > 0, "connection closed before the response head completed");
            rx_buf.extend_from_slice(&chunk[..n]);
        }
        let head_end = rx_buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let head = String::from_utf8_lossy(&rx_buf[..head_end]).to_string();
        let status = head
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let content_length: usize = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.trim().eq_ignore_ascii_case("content-length") {
                    value.trim().parse().ok()
                } else {
                    None
                }
            })
            .expect("response must carry a Content-Length header");
        while rx_buf.len() < head_end + content_length {
            let n = stream.read(&mut chunk).await.expect("read response body");
            assert!(n > 0, "connection closed before the response body completed");
            rx_buf.extend_from_slice(&chunk[..n]);
        }
        let body = rx_buf[head_end..head_end + content_length].to_vec();
        rx_buf.drain(..head_end + content_length);
        (status, body)
    }

    /// K1 (resource-leak-plan.md): a positive `server.keepalive_timeout` must
    /// arm a server-side idle reaper — an idle connection is closed (read
    /// returns EOF) once the window elapses. The keepalive middleware only
    /// consumes the `<= 0.0` branch and no serve path configures a hyper
    /// timer, so today the connection is never reaped.
    /// Currently FAILS: the post-idle read times out instead of returning EOF.
    #[tokio::test]
    async fn test_k1_idle_keepalive_connection_must_be_reaped() {
        let sock = sock_path("k1");
        // K1: a positive keepalive window must be honored.
        let (shutdown_tx, server) = spawn_test_server(&sock, build_deps(&sock, 1.0)).await;

        let mut stream =
            UnixStream::connect(&sock).await.expect("connect to test server");
        let mut rx_buf: Vec<u8> = Vec::new();
        let request = b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";

        // Positive control: two back-to-back requests on one connection must
        // both succeed inside the keepalive window (the connection is not
        // killed while it is being used).
        stream.write_all(request).await.expect("write request 1");
        let (status1, _body1) = read_response(&mut stream, &mut rx_buf).await;
        assert_eq!(status1, 200, "first /health request must be 200");

        stream.write_all(request).await.expect("write request 2");
        let (status2, _body2) = read_response(&mut stream, &mut rx_buf).await;
        assert_eq!(
            status2, 200,
            "second /health request on the same connection must be 200 (keep-alive reuse)"
        );

        // Idle past the 1.0 s keepalive window, then the next read must hit
        // EOF: the server has reaped the connection.
        tokio::time::sleep(K1_IDLE).await;
        let mut probe = [0u8; 16];
        let outcome = tokio::time::timeout(K1_READ_TIMEOUT, stream.read(&mut probe)).await;

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
        let _ = std::fs::remove_file(&sock);

        let n = match outcome {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => panic!("K1: post-idle read failed: {e}"),
            Err(_) => {
                panic!(
                    "K1 (resource-leak-plan.md): after {K1_IDLE:?} idle the read did not \
                     reach EOF within {K1_READ_TIMEOUT:?} — keepalive_timeout = 1.0 s must \
                     arm a hyper idle timer, but no serve path configures one (the \
                     keepalive middleware consumes only the <=0.0 branch; serve_unix has no \
                     builder timer); the idle connection is never reaped"
                );
            }
        };
        assert_eq!(
            n, 0,
            "K1 (resource-leak-plan.md): after {K1_IDLE:?} idle (keepalive_timeout = 1.0 s) \
             the server must close the connection — the post-idle read must return EOF (0 \
             bytes), got {n} bytes; no hyper idle timer arms on the UDS path"
        );
    }

    /// K2 (resource-leak-plan.md): with `keepalive_timeout = 0` the server
    /// only expresses its keep-alive policy through `Connection: close`
    /// response headers (h1 semantics) — but no serve path calls
    /// `http1_only()`, and hyper-util's auto builder treats an h2c
    /// prior-knowledge preface as HTTP/2 (it starts as H2 and only falls back
    /// to H1 on a mismatch). The UDS path therefore answers the preface with
    /// an HTTP/2 SETTINGS frame.
    /// Currently FAILS: a SETTINGS frame (first byte 0x04) arrives within 2 s.
    #[tokio::test]
    async fn test_k2_keepalive_zero_must_reject_h2_prior_knowledge() {
        let sock = sock_path("k2");
        // K2: ka = 0 is the `Connection: close` middleware branch.
        let (shutdown_tx, server) = spawn_test_server(&sock, build_deps(&sock, 0.0)).await;

        let mut stream =
            UnixStream::connect(&sock).await.expect("connect to test server");

        // HTTP/2 connection preface + an empty SETTINGS frame (client preface).
        // SETTINGS frame header: 3-byte length 0, type 0x04, flags 0, stream 0.
        let mut h2_preface = Vec::new();
        h2_preface.extend_from_slice(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        h2_preface.extend_from_slice(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0]);
        stream
            .write_all(&h2_preface)
            .await
            .expect("write h2c prior-knowledge preface");

        // The server must reject the h2c prior-knowledge connection: no HTTP/2
        // frames may arrive. The first frame of any h2 session is the server's
        // SETTINGS; an h2 frame header is [3-byte length][1-byte type][1-byte
        // flags][4-byte stream id], so the SETTINGS type byte 0x04 lands at
        // offset 3. Read until 4 bytes or EOF so the type byte is always
        // available when any data at all was sent.
        let outcome = tokio::time::timeout(K2_SILENCE_BUDGET, async {
            let mut got = Vec::new();
            let mut buf = [0u8; 16];
            while got.len() < 4 {
                match stream.read(&mut buf).await {
                    Ok(0) => break, // EOF
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e) => return Err(e),
                }
            }
            Ok::<Vec<u8>, std::io::Error>(got)
        })
        .await;

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
        let _ = std::fs::remove_file(&sock);

        match outcome {
            // No bytes within the budget, or EOF without a single frame: the
            // server never spoke HTTP/2 — the required behavior.
            Err(_) => {}
            Ok(Err(e)) => panic!("K2: read from the test connection failed: {e}"),
            Ok(Ok(got)) => {
                assert!(
                    !(got.len() >= 4 && got[3] == 0x04),
                    "K2 (resource-leak-plan.md): with keepalive_timeout = 0 the server must \
                     not accept h2c prior-knowledge (its keep-alive handling is h1-only via \
                     the `Connection: close` middleware and no serve path sets http1_only()); \
                     the hyper auto builder answered with an HTTP/2 SETTINGS frame — frame \
                     type byte at offset 3 is 0x04 ({:02x?})",
                    got
                );
            }
        }
    }
}

/// L3 (resource-leak-plan.md): the model-level `request_timeout` defaults to
/// 0.0 and `WarmupPolicy::effective_timeout(0.0)` resolves to None — an
/// unbounded wait. PASSES on current code: this probe documents that the
/// default request timeout is unbounded (the premise of the L3 defect: an
/// explicit 0.0 is indistinguishable from unset).
mod l3_probe {
    use lite_server::config::{ModelConfig, WarmupPolicy};

    #[test]
    fn test_l3_request_timeout_default_zero_is_unbounded() {
        let model = ModelConfig::default();
        assert_eq!(
            model.request_timeout, 0.0,
            "L3: the default model request_timeout must be 0.0 (unset = unbounded)"
        );

        let warmup = WarmupPolicy::default();
        assert_eq!(
            warmup.effective_timeout(0.0),
            None,
            "L3: effective_timeout(0.0) must be None — a zero request_timeout means an \
             unbounded wait"
        );
    }
}
