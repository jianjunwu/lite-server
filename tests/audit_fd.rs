//! Audit evidence tests — .claude/functional-defects-plan.md P1/P2 deep audit
//! (2026-08-14). Each test was a failing repro at audit time; all confirmed
//! defects are now fixed and the tests pin the fixed behavior. The passing
//! F-10(a)/F-15 tests document NOT-REPRODUCED verdicts (evidence or
//! discriminative probe).
//!
//! Defect summary (plan IDs; in-crate repros live next to the code):
//! - F-01 (lib, src/grpc/mod.rs)  keepalive interval/timeout 0 reach hyper
//! - F-02 (fd_b)                  invalid JSON worker response silently 200 + {}
//! - F-03 (fd_b)                  auth/rate-limit after model existence check
//! - F-04 (fd_c)                  ka=0 middleware rewrites the 101 WS upgrade
//! - F-06 (fd_c)                  over-limit body -> 500, not 413 / 404
//! - F-09 (lib, src/http/handlers/bidi.rs)  bad frame skips D4 Close
//! - F-10(b) (fd_d)               per-chunk lossy UTF-8 -> U+FFFD across chunks
//! - F-10(a) (fd_d)               NOT-REPRODUCED: axum 0.7.9 Event::data splits \n
//! - B16 (fd_d, adjacent)         chunk with bare \r panics the SSE forward task
//! - F-11 (lib, src/grpc/rpc/decoupled.rs)  deadline/idle reclaim silent EOF
//! - F-12 (fd_a)                  http2_max_frame_size out of range unvalidated
//! - F-13 (lib, src/grpc/admin.rs) download open failure -> OK + zero chunks
//! - F-14 (fd_e)                  ZMQ bind failure not surfaced to the caller
//! - F-15 (fd_e)                  NOT-REPRODUCED: libzmq discards stale PAIR frames
//! - F-17 (fd_e)                  KServe fallback always renders Legacy error body
//! - F-18 (fd_b)                  channel closed: HTTP 504 vs gRPC Internal
//!
//! The in-crate repros (F-01/F-09/F-11/F-13) need private access and live in
//! the corresponding `#[cfg(test)] mod tests` blocks.

mod fd_a {
    //! F-12: `grpc.http2_max_frame_size` is forwarded to the h2 builder without
    //! any range check (src/grpc/mod.rs make_builder). h2 0.4.14 hard-asserts
    //! `16_384 <= val <= 16_777_215` (`Settings::set_max_frame_size`,
    //! frame/settings.rs:91 — a plain `assert!`, active in release builds) when
    //! the first connection initializes, so an out-of-range config value (e.g.
    //! 20000000, or 0) panics per new connection instead of failing startup.
    //! `Config::validate` accepts every value today.

    use lite_server::config::Config;

    #[test]
    fn test_f12_http2_max_frame_size_out_of_range_must_fail_validate() {
        // F-12: h2's legal range is [16384, 16777215]; anything outside panics on
        // the first connection (h2 assert). Config must reject it at startup.
        for bad in ["20000000", "0"] {
            let cfg: Config =
                serde_yaml::from_str(&format!("grpc:\n  http2_max_frame_size: {bad}\n"))
                    .expect("config parses");
            assert!(
                cfg.validate().is_err(),
                "http2_max_frame_size {bad} is outside [16384, 16777215] and must fail startup validation"
            );
        }
    }
}

mod fd_b {
    //! F-02 / F-03 / F-18 (inference handler surface). FAIL on current code.
    //! Harness mirrors tests/audit_middleware.rs and the handler tests.

    use bytes::Bytes;
    use lite_server::callback::CallbackRunner;
    use lite_server::config::{AuthPolicy, Config, ModelConfig, ModelPolicies};
    use lite_server::grpc::{GrpcService, GrpcServiceDeps, LiteServer};
    use lite_server::http::handlers::inference::run_infer;
    use lite_server::http::handlers::RequestBody;
    use lite_server::http::state::AppState;
    use lite_server::inference_queue::{InferenceQueue, OutlierState};
    use lite_server::proto::liteserver as pb;
    use lite_server::rate_limit::RateLimiter;
    use lite_server::registry::types::{ModelType, WorkerInfo, WorkerStatus};
    use lite_server::registry::ModelRegistry;
    use lite_server::request_context::RequestContext;
    use lite_server::transport::zmq::WorkerZmqClient;
    use lite_server::worker::WorkerManager;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;

    fn test_endpoint(name: &str) -> String {
        format!(
            "ipc://{}",
            std::env::temp_dir()
                .join(format!("lite-server-audit-fd-b-{}-{}.sock", name, std::process::id()))
                .display()
        )
    }

    fn test_cx(request_id: &str, client_ip: &str) -> RequestContext {
        RequestContext {
            request_id: request_id.to_string(),
            client_ip: client_ip.to_string(),
            trace_cx: opentelemetry::Context::new(),
            protocol: lite_server::callback::Protocol::Http,
            principal: None,
            api_protocol: None,
        }
    }

    fn build_state() -> (Arc<AppState>, Arc<InferenceQueue>) {
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue.clone(),
            "error".to_string(),
            callback_runner.clone(),
        ));
        let state = Arc::new(AppState::new(
            registry,
            wm,
            queue.clone(),
            Config::default(),
            PathBuf::new(),
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(RateLimiter::default()),
        ));
        (state, queue)
    }

    fn build_grpc_service() -> (GrpcService, Arc<InferenceQueue>, Arc<ModelRegistry>) {
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue.clone(),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let app_state = Arc::new(AppState::new(
            registry.clone(),
            wm.clone(),
            queue.clone(),
            Config::default(),
            PathBuf::new(),
            Arc::new(CallbackRunner::new()),
            Arc::new(AtomicBool::new(false)),
            Arc::new(RateLimiter::default()),
        ));
        let service = GrpcService::new(GrpcServiceDeps {
            registry: registry.clone(),
            worker_manager: wm,
            streaming_metrics: false,
            canary_override: false,
            grpc_streaming: true,
            callback_runner: Arc::new(CallbackRunner::new()),
            shutdown_state: Arc::new(lite_server::server::ShutdownState::new()),
            server_timeout: Duration::from_secs(5),
            rate_limiter: Arc::new(RateLimiter::default()),
            decoupled_idle_timeout: None,
            app_state: app_state.clone(),
            trusted: Arc::new(Vec::new()),
        });
        (service, queue, registry)
    }

    fn register_ready(registry: &ModelRegistry, model: &str, version: &str) {
        registry
            .register(model, version, ModelConfig::default(), ModelType::LitAPI, PathBuf::new())
            .unwrap();
        registry.mark_ready(model, version).unwrap();
    }

    /// Register a ready model carrying an API-key auth policy (used to prove the
    /// 401-vs-404 / Unauthenticated-vs-NotFound differential for unknown models).
    fn register_with_auth(registry: &ModelRegistry, model: &str, version: &str) {
        register_ready(registry, model, version);
        registry.set_policies(
            model,
            version,
            Some(ModelPolicies {
                auth: Some(AuthPolicy {
                    header: "X-API-Key".to_string(),
                    keys: vec!["sk-audit".to_string()],
                }),
                ..Default::default()
            }),
        );
    }

    /// Queue config for the audit tests: single-request batches, no health
    /// checker, no queue-timeout rejection (deterministic collector behavior).
    fn audit_queue_config() -> ModelConfig {
        ModelConfig {
            max_batch_size: 1,
            health_check_interval: 0.0,
            ..Default::default()
        }
    }

    // ===== F-02: invalid JSON from a worker declaring application/json is
    // silently replaced with 200 + {} =====

    /// Fake worker: replies to every request with invalid JSON bytes while
    /// declaring `media_type = "application/json"`.
    fn spawn_bad_json_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        use prost::Message;
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("fake worker socket");
            let _ = s.set_linger(0);
            s.connect(&endpoint).expect("fake worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resp = pb::Response {
                    uid: req.uid,
                    payload: Some(pb::response::Payload::Single(pb::SingleResponse {
                        data: Bytes::from_static(b"this is NOT valid json"),
                        media_type: "application/json".to_string(),
                        status: Some(pb::Status {
                            code: "Ok".to_string(),
                            message: String::new(),
                        }),
                        ..Default::default()
                    })),
                    metrics: None,
                };
                if s.send(resp.encode_to_vec(), 0).is_err() {
                    return;
                }
            }
        })
    }

    /// F-02 (functional-defects-plan.md:24): a parse failure is not an error
    /// path but a "tolerant" one — `serde_json::from_slice(&single.data)
    /// .unwrap_or(json!({}))` (inference.rs:431) fabricates 200 + empty object
    /// from corrupt worker output. A response declaring application/json must
    /// be parsed strictly; failure should surface as 502. Expect 502, got 200.
    #[tokio::test]
    async fn test_f02_bad_worker_json_declared_json_is_502_not_200() {
        let (state, queue) = build_state();
        let endpoint = test_endpoint("f02");
        let _worker = spawn_bad_json_worker(endpoint.clone());
        register_ready(&state.registry, "m", "1");
        state
            .registry
            .set_workers(
                "m",
                "1",
                vec![WorkerInfo {
                    worker_id: 0,
                    device: "cpu".to_string(),
                    endpoint: endpoint.clone(),
                    pid: None,
                    status: WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();
        let (reload_tx, _reload_rx) = tokio::sync::mpsc::channel(8);
        queue.register_model(
            "m",
            "1",
            &audit_queue_config(),
            vec![],
            vec![Arc::new(WorkerZmqClient::new(endpoint))],
            reload_tx,
            Arc::new(OutlierState::new(1)),
            None,
        );
        // Let the fake worker's connect complete before the first request.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // run_infer surfaces handler errors via Err(ProtocolError) — the
        // wire status is what matters (same Result handling as the F-03 test).
        let result = run_infer(
            state,
            "m".to_string(),
            Some("1".to_string()),
            "/predict".to_string(),
            axum::http::HeaderMap::new(),
            RequestBody::Json(Bytes::from_static(b"{\"input\": 1}")),
            test_cx("f02", "10.0.0.1"),
        )
        .await;
        let status = match result {
            Ok(resp) => resp.status(),
            Err(e) => axum::response::IntoResponse::into_response(e).status(),
        };

        assert_eq!(
            status,
            axum::http::StatusCode::BAD_GATEWAY,
            "F-02: a worker response declaring application/json but containing invalid JSON \
             must surface as 502 Bad Gateway, not a silent 200 + empty object"
        );
    }

    // ===== F-03: auth/rate-limit run after the model-existence check
    // (existence probe + rate-limit bypass) =====

    /// F-03 (functional-defects-plan.md:30): on the HTTP side the registry
    /// check (inference.rs:201-210) runs BEFORE enforce_auth /
    /// enforce_rate_limit (:213-214), and the middleware layer has no auth — an
    /// unauthenticated request for a nonexistent model gets 404 (ModelNotFound)
    /// instead of 401, exposing whether a model exists / is loaded. Expect 401,
    /// got 404.
    #[tokio::test]
    async fn test_f03_http_unauth_unknown_model_must_be_401_not_404() {
        let (state, _queue) = build_state();
        // Auth is configured on an existing model — the server is "secured" —
        // but an unauthenticated probe of an UNKNOWN model must still get 401.
        register_with_auth(&state.registry, "m", "1");

        let result = run_infer(
            state,
            "ghost-model".to_string(),
            None,
            "/predict".to_string(),
            axum::http::HeaderMap::new(),
            RequestBody::Json(Bytes::from_static(b"{}")),
            test_cx("f03", "10.0.0.1"),
        )
        .await;

        let status = match result {
            Ok(resp) => resp.status(),
            Err(e) => axum::response::IntoResponse::into_response(e).status(),
        };
        assert_eq!(
            status,
            axum::http::StatusCode::UNAUTHORIZED,
            "F-03: unauthenticated request for an unknown model must be 401 (auth must \
             precede model existence lookup), not 404 — the 404-vs-401 differential \
             leaks model existence"
        );
    }

    /// F-03 (functional-defects-plan.md:30): the gRPC side also passes
    /// is_ready/get (infer.rs:77-84) before enforce_auth_grpc (:85); an unknown
    /// model is rejected with NotFound at version resolution (:71), so the
    /// NotFound-vs-Unauthenticated differential exposes model existence. Expect
    /// Unauthenticated, got NotFound.
    #[tokio::test]
    async fn test_f03_grpc_unauth_unknown_model_must_be_unauthenticated() {
        let (service, _queue, registry) = build_grpc_service();
        register_with_auth(&registry, "m", "1");

        let request = tonic::Request::new(pb::InferRequest {
            model_name: "ghost-model".to_string(),
            version: String::new(),
            data: Vec::new().into(),
            headers: Default::default(),
            sequence_id: None,
        });
        let err = LiteServer::infer(&service, request)
            .await
            .expect_err("unknown model without credentials must be rejected");

        assert_eq!(
            err.code(),
            tonic::Code::Unauthenticated,
            "F-03: gRPC auth must precede model resolution — expect Unauthenticated for an \
             unknown model, not NotFound"
        );
    }

    // ===== F-18: the same "response channel closed" event is HTTP 504 vs
    // gRPC Internal =====

    /// F-18 (functional-defects-plan.md:106): when a worker crash/recycle
    /// closes the response channel, HTTP mapped 504 InferenceTimeout while
    /// gRPC mapped Status::internal — the same upstream-death event drove
    /// opposite client retry policies. Fixed mapping: HTTP 502 Bad Gateway +
    /// Retry-After, gRPC Unavailable + retry-after metadata. Expect
    /// Unavailable, got Internal on the pre-fix code.
    ///
    /// Repro mechanism: register a queue with 0 workers — the collector picks
    /// the item, has no dispatch target; pre-fix it index-panicked and the
    /// dropped QueueItem closed the oneshot send side. Post-fix the 0-worker
    /// guard (inference_queue.rs do_send_batch) fails the item with a
    /// structured 503, which the gRPC path maps to the same Unavailable.
    #[tokio::test]
    async fn test_f18_grpc_closed_response_channel_must_be_unavailable() {
        let (service, queue, registry) = build_grpc_service();
        register_ready(&registry, "m", "1");

        let (reload_tx, _reload_rx) = tokio::sync::mpsc::channel(8);
        queue.register_model(
            "m",
            "1",
            &audit_queue_config(),
            vec![],
            vec![],
            reload_tx,
            Arc::new(OutlierState::new(0)),
            None,
        );

        let request = tonic::Request::new(pb::InferRequest {
            model_name: "m".to_string(),
            version: "1".to_string(),
            data: b"{}".to_vec().into(),
            headers: Default::default(),
            sequence_id: None,
        });
        let err = LiteServer::infer(&service, request)
            .await
            .expect_err("closed response channel must surface as an error");

        assert_eq!(
            err.code(),
            tonic::Code::Unavailable,
            "F-18: response channel closed (worker crash/recycle) must map to Unavailable \
             (upstream unavailable, HTTP 502/504 family parity), not Internal"
        );
    }
}

/// F-04 / F-06: drive the real server in-process over a unix socket
/// (lite_server::http::start_http_server) with keepalive_timeout = 0.
///
/// F-07 (accept-error tolerance) has no test seam: serve_unix's accept error
/// cannot be injected without a failpoint and no error-classification logic
/// exists to unit-test — see the audit report.
#[cfg(unix)]
mod fd_c {
    use lite_server::callback::CallbackRunner;
    use lite_server::config::{Config, ModelConfig};
    use lite_server::http::{start_http_server, HttpServerOptions};
    use lite_server::inference_queue::InferenceQueue;
    use lite_server::rate_limit::RateLimiter;
    use lite_server::registry::types::ModelType;
    use lite_server::registry::ModelRegistry;
    use lite_server::server::ShutdownState;
    use lite_server::worker::protocol::RouteDecl;
    use lite_server::worker::WorkerManager;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    /// route_fallback's body cap (ROUTE_BODY_LIMIT, src/http/mod.rs) + 1 byte.
    const OVER_LIMIT_BODY: usize = 10 * 1024 * 1024 + 1;

    struct TestDeps {
        config: Config,
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        inference_queue: Arc<InferenceQueue>,
    }

    fn sock_path(tag: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lite-audit-fd-c-{tag}-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn build_deps(sock: &Path, registry: Option<Arc<ModelRegistry>>) -> TestDeps {
        let mut config = Config::default();
        // ka=0 enables disable_keepalive_middleware (src/http/mod.rs:389) — F-04.
        config.server.keepalive_timeout = 0.0;
        config.server.host = format!("unix:{}", sock.display());
        let registry = registry.unwrap_or_else(|| Arc::new(ModelRegistry::new()));
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

    /// Send one raw HTTP/1.1 request over the unix socket and return
    /// (status code, response head). Read stops at the end of the head.
    ///
    /// The server may legitimately reject early (F-06: 404/413 decided before
    /// the body is read) and close while the client is still writing a large
    /// body — a broken pipe on write is the expected signature of that
    /// correct behavior, and the already-sent response head stays readable.
    async fn raw_request(sock: &Path, request: &[u8]) -> (u16, String) {
        let mut stream = tokio::time::timeout(Duration::from_secs(5), UnixStream::connect(sock))
            .await
            .expect("timed out connecting to test server")
            .expect("connect to test server failed");
        if let Err(e) = stream.write_all(request).await {
            assert_eq!(
                e.kind(),
                std::io::ErrorKind::BrokenPipe,
                "write request to test server: {e}"
            );
        }
        let head = tokio::time::timeout(Duration::from_secs(5), async {
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                head.extend_from_slice(&buf[..n]);
            }
            Ok::<Vec<u8>, std::io::Error>(head)
        })
        .await
        .expect("timed out reading response head")
        .expect("read response head failed");
        let head = String::from_utf8_lossy(&head).to_string();
        let status = head
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        (status, head)
    }

    /// F-04: with keepalive_timeout=0 the keepalive-disable middleware rewrites
    /// `Connection: close` onto EVERY response — including the 101 WS upgrade
    /// handshake. RFC 6455 §4.2.2 requires the Upgrade token in the 101
    /// response's Connection header; `Connection: close` fails the handshake.
    /// Currently FAILS: the 101 carries `Connection: close`.
    #[tokio::test]
    async fn test_f04_ws_upgrade_101_must_keep_connection_upgrade_when_keepalive_disabled() {
        let sock = sock_path("f04");
        let (shutdown_tx, server) = spawn_test_server(&sock, build_deps(&sock, None)).await;

        let request = concat!(
            "GET /v2/models/foo/stream HTTP/1.1\r\n",
            "Host: localhost\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "\r\n",
        );
        let (status, head) = raw_request(&sock, request.as_bytes()).await;

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
        let _ = std::fs::remove_file(&sock);

        assert_eq!(status, 101_u16, "WS handshake must be 101, got {status}\n{head}");
        let connection = head
            .lines()
            .map(str::trim)
            .find(|l| l.to_ascii_lowercase().starts_with("connection:"))
            .expect("response head must carry a Connection header");
        assert!(
            connection.to_ascii_lowercase().contains("upgrade"),
            "101 upgrade response must keep `Connection: Upgrade` (RFC 6455 §4.2.2); \
             got `{connection}`\n{head}"
        );
    }

    /// F-06: a body over ROUTE_BODY_LIMIT (10 MiB) on a MATCHED custom @route is
    /// mapped to `AppError::Internal` -> 500 in route_fallback (src/http/mod.rs
    /// lines 87-92) instead of 413 Payload Too Large.
    /// Currently FAILS: the request returns 500.
    #[tokio::test]
    async fn test_f06_oversized_body_on_matched_custom_route_should_be_413() {
        let sock = sock_path("f06a");
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(
                "foo",
                "1",
                ModelConfig::default(),
                ModelType::LitAPI,
                std::env::temp_dir(),
            )
            .expect("register fake model");
        registry.mark_ready("foo", "1").expect("mark model ready");
        let deps = build_deps(&sock, Some(registry));
        deps.worker_manager
            .upsert_routes(
                "foo",
                "1",
                vec![RouteDecl {
                    route: "/custom".into(),
                    methods: vec!["POST".into()],
                }],
            )
            .await;
        let (shutdown_tx, server) = spawn_test_server(&sock, deps).await;

        let mut request = format!(
            "POST /v2/models/foo/versions/1/custom HTTP/1.1\r\n\
             Host: localhost\r\n\
             Content-Length: {OVER_LIMIT_BODY}\r\n\r\n"
        )
        .into_bytes();
        request.extend(std::iter::repeat_n(0u8, OVER_LIMIT_BODY));
        let (status, head) = raw_request(&sock, &request).await;

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
        let _ = std::fs::remove_file(&sock);

        assert_eq!(
            status, 413_u16,
            "oversized body on a matched custom @route must be 413, got {status}\n{head}"
        );
    }

    /// F-06: route_fallback reads the body (src/http/mod.rs line 87) BEFORE
    /// checking whether a custom route exists (dispatch_custom_route, line 94),
    /// so an oversized body on an UNMATCHED @route path returns 500 instead of
    /// 404 (the route check must precede the body read).
    /// Currently FAILS: the request returns 500.
    #[tokio::test]
    async fn test_f06_oversized_body_on_unmatched_route_should_be_404() {
        let sock = sock_path("f06b");
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(
                "bar",
                "1",
                ModelConfig::default(),
                ModelType::LitAPI,
                std::env::temp_dir(),
            )
            .expect("register fake model");
        registry.mark_ready("bar", "1").expect("mark model ready");
        let deps = build_deps(&sock, Some(registry));
        let (shutdown_tx, server) = spawn_test_server(&sock, deps).await;

        let mut request = format!(
            "POST /v2/models/bar/versions/1/does-not-exist HTTP/1.1\r\n\
             Host: localhost\r\n\
             Content-Length: {OVER_LIMIT_BODY}\r\n\r\n"
        )
        .into_bytes();
        request.extend(std::iter::repeat_n(0u8, OVER_LIMIT_BODY));
        let (status, head) = raw_request(&sock, &request).await;

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
        let _ = std::fs::remove_file(&sock);

        assert_eq!(
            status, 404_u16,
            "unmatched custom-route path with an oversized body must be 404, got {status}\n{head}"
        );
    }
}

mod fd_d {
    //! F-10 (SSE encoding surface).
    //!
    //! - F-10(a) NOT-REPRODUCED — the plan claims a worker chunk with an
    //!   embedded `\n` is written raw into `Event::data` and axum emits
    //!   `data: {text}\n\n`, splitting the event into an unprefixed line. On the
    //!   locked axum 0.7.9, `Event::data` itself splits `\n` into per-line
    //!   `data:` fields (axum-0.7.9/src/response/sse.rs:180-191,
    //!   `memchr_split(b'\n', ...)` + `field("data", line)`), so the wire stays
    //!   well-formed — `test_f10_embedded_newline_does_not_corrupt_sse_wire`
    //!   below documents this and PASSES on current code.
    //! - B16 ADJACENT (not in the plan): a chunk containing a bare `\r` PANICS
    //!   the SSE forward task — axum's `field()` asserts no CR/LF in a field
    //!   value (sse.rs:361-367) while `Event::data` only splits on `\n`.
    //! - F-10(b) CONFIRMED — the non-ensemble SSE assembly
    //!   (src/http/handlers/stream.rs) ran `String::from_utf8_lossy` per
    //!   chunk, so a multi-byte codepoint split across chunk boundaries became
    //!   U+FFFD twice; the ensemble path (`ensemble_chunk_utf8`) buffers the
    //!   incomplete tail and decodes one logical sequence. The fix extracted
    //!   the public seam `direct_chunk_utf8` (incremental lossy decode + CR
    //!   normalization); the tests below pin that seam.

    use axum::response::sse::{Event, Sse};
    use axum::response::IntoResponse;
    use futures::stream::once;
    use std::convert::Infallible;

    /// F-10(b): a multi-byte UTF-8 codepoint split across two worker chunks must
    /// survive the chunk→SSE assembly. The old assembly
    /// (src/http/handlers/stream.rs:591) ran `String::from_utf8_lossy` per
    /// chunk, so each half of a split codepoint was replaced with U+FFFD —
    /// proven by the pre-fix version of this test, which pinned that exact
    /// transform inline. The fix extracted a public seam
    /// (`direct_chunk_utf8`): incremental decode with the incomplete tail
    /// held across chunks (ensemble parity). This test now pins the seam.
    #[test]
    fn test_f10_multibyte_utf8_split_across_chunks_not_replaced() {
        // 'é' (U+00E9) = 0xC3 0xA9, split by a byte-oriented chunk boundary.
        let chunk1: &[u8] = b"\xC3";
        let chunk2: &[u8] = b"\xA9";

        let mut pending = Vec::new();
        let first = lite_server::http::handlers::stream::direct_chunk_utf8(&mut pending, chunk1);
        let second = lite_server::http::handlers::stream::direct_chunk_utf8(&mut pending, chunk2);
        let assembled = format!(
            "{}{}",
            first.unwrap_or_default(),
            second.unwrap_or_default()
        );

        assert!(
            assembled.contains('\u{00E9}'),
            "F-10(b): per-chunk from_utf8_lossy replaced the split multi-byte \
             codepoint with U+FFFD (got {assembled:?}); the assembly must buffer \
             the incomplete tail and decode one logical UTF-8 sequence (ensemble \
             parity, stream.rs ensemble_chunk_utf8)"
        );
    }

    /// B16 (F-10(a) adjacent, not in the plan): axum 0.7.9's `Event::data`
    /// splits on `\n` (sse.rs:180-191) but NOT on `\r`, and `field()`
    /// hard-asserts no CR/LF in a field value (sse.rs:361-367). A worker chunk
    /// containing a bare `\r` therefore PANICKED the SSE forward task (the
    /// pre-fix version of this test fed the raw chunk straight to
    /// `Event::data` and the drain panicked), killing the stream
    /// mid-response. The fix normalizes CR inside `direct_chunk_utf8`; this
    /// test pins the seam → axum path end to end.
    #[tokio::test]
    async fn test_f10_cr_in_chunk_must_not_panic_forward_task() {
        let mut pending = Vec::new();
        let data =
            lite_server::http::handlers::stream::direct_chunk_utf8(&mut pending, b"line1\rline2")
                .expect("chunk must decode");
        let event = Event::default().data(&data);
        let body = Sse::new(once(async { Ok::<Event, Infallible>(event) })).into_response();
        let wire = axum::body::to_bytes(body.into_body(), 1 << 16)
            .await
            .expect("drain SSE body");
        let wire = String::from_utf8_lossy(&wire);
        assert!(
            wire.contains("line1") && wire.contains("line2"),
            "both lines must survive CR normalization on the wire: {wire:?}"
        );
    }

    /// F-10(a) evidence: axum 0.7.9 `Event::data` splits embedded `\n` into
    /// per-line `data:` fields itself, so a chunk containing `line1\nline2`
    /// still produces a well-formed SSE wire — the plan's claim of a bare
    /// unprefixed line does NOT reproduce. PASSES on current code (documents the
    /// verdict; the wire contract is pinned either way).
    #[tokio::test]
    async fn test_f10_embedded_newline_does_not_corrupt_sse_wire() {
        let data = String::from_utf8_lossy(b"line1\nline2");
        let event = Event::default().data(&data);

        let body = Sse::new(once(async { Ok::<Event, Infallible>(event) })).into_response();
        let wire = axum::body::to_bytes(body.into_body(), 1 << 16)
            .await
            .expect("drain SSE body");
        let wire = String::from_utf8_lossy(&wire);

        for line in wire.lines() {
            if line.is_empty() {
                continue;
            }
            assert!(
                line.starts_with("data: "),
                "SSE wire must not contain bare lines: {wire:?}"
            );
        }
    }
}

mod fd_e {
    //! F-14 / F-15 / F-17 (F-13 lives in src/grpc/admin.rs mod tests — it
    //! needs the private gRPC admin harness).

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use lite_server::callback::CallbackRunner;
    use lite_server::config::Config;
    use lite_server::http::state::AppState;
    use lite_server::inference_queue::InferenceQueue;
    use lite_server::proto::liteserver as pb;
    use lite_server::rate_limit::RateLimiter;
    use lite_server::registry::ModelRegistry;
    use lite_server::worker::WorkerManager;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_app_state(repo_path: std::path::PathBuf, config: Config) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path.clone(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            config,
            repo_path,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(RateLimiter::default()),
        ))
    }

    fn unique_tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lite-server-audit-fd-{}-{}-{}",
            tag,
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    /// F-14 (functional-defects-plan.md): a ZMQ bind failure (occupied
    /// endpoint / path) only logs inside the actor thread —
    /// `WorkerZmqClient::new` still returns Ok, so the caller can never learn
    /// the worker socket is dead. Requests fail with a generic "ZMQ send:
    /// Resource temporarily unavailable" error response instead of surfacing
    /// the bind failure. The fix must propagate the bind error to the caller.
    /// Red: the surfaced error never mentions the bind.
    #[tokio::test]
    async fn test_f14_bind_failure_surfaced_to_caller() {
        // Occupy an IPC endpoint with a live PAIR socket, then build a
        // WorkerZmqClient on the same endpoint: its actor bind fails.
        let ctx = zmq::Context::new();
        let occupier = ctx.socket(zmq::PAIR).unwrap();
        let endpoint = format!(
            "ipc:///tmp/lite-server-f14-{}-{}.sock",
            std::process::id(),
            uuid::Uuid::new_v4()
        );
        occupier.bind(&endpoint).unwrap();

        let client = lite_server::transport::zmq::WorkerZmqClient::new(endpoint.clone());
        // Give the actor time to attempt (and fail) its bind and exit.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let result = client
            .send_with_timeout(
                pb::Request {
                    uid: "f14".to_string(),
                    ..Default::default()
                },
                std::time::Duration::from_secs(5),
            )
            .await;
        // The caller may observe the failure either as a transport Err or as an
        // error Status inside an Ok(Response) — either way it must name the bind
        // failure, not a generic "ZMQ send: Resource temporarily unavailable"
        // (EAGAIN) that hides the root cause.
        let msg = match &result {
            Ok(resp) => format!("{resp:?}"),
            Err(e) => format!("{e:?}"),
        };
        let lower = msg.to_lowercase();
        assert!(
            lower.contains("bind") || lower.contains("address in use") || msg.contains("EADDRINUSE"),
            "a bind failure must be surfaced to the caller (not a generic \
             transport error that hides the root cause); got: {msg}"
        );
    }

    /// F-15 (functional-defects-plan.md): the plan claims ZMQ PAIR retains
    /// frames queued on the bound socket after the peer dies and delivers
    /// them to a reconnecting peer (a stale-request replay hazard for
    /// respawned workers, currently documented in worker/process.rs:441-449).
    /// Per libzmq 4.3.4 source and RFC 31 the bound PAIR socket destroys its
    /// queue when the peer disconnects and a fresh peer receives only new
    /// frames. This probe pins that contract at the socket seam. If it FAILS
    /// at runtime, the F-15 replay hazard is real and this test is its repro.
    #[test]
    fn test_f15_pair_stale_frames_not_replayed_to_replacing_peer() {
        let ctx = zmq::Context::new();
        let endpoint = format!(
            "ipc:///tmp/lite-server-f15-{}-{}.sock",
            std::process::id(),
            uuid::Uuid::new_v4()
        );
        let bound = ctx.socket(zmq::PAIR).unwrap();
        bound.set_linger(0).unwrap();
        bound.bind(&endpoint).unwrap();

        // Peer 1 connects and never reads — a hung worker.
        let peer1 = ctx.socket(zmq::PAIR).unwrap();
        peer1.set_linger(0).unwrap();
        peer1.connect(&endpoint).unwrap();
        // Let the ZMQ handshake complete so the bound pipe exists before queueing.
        std::thread::sleep(std::time::Duration::from_millis(300));

        // Queue frames on the bound side until the transport backpressures:
        // frames that cannot reach the peer stay in the bound socket's pipe.
        let frame = vec![b'x'; 64 * 1024];
        let mut queued = 0usize;
        while bound.send(&frame, zmq::DONTWAIT).is_ok() && queued < 2048 {
            queued += 1;
        }
        assert!(queued > 0, "test premise: at least one frame must be queued");

        // Peer dies without reading a single byte.
        drop(peer1);
        std::thread::sleep(std::time::Duration::from_millis(300));

        // Model the actor's recv loop: drain the bound socket's input queue
        // (responses + the pipe-termination delimiter) so the pipe teardown
        // completes.
        while bound.recv_bytes(zmq::DONTWAIT).is_ok() {}

        // A replacement peer attaches to the same bound socket.
        let peer2 = ctx.socket(zmq::PAIR).unwrap();
        peer2.set_rcvtimeo(1500).unwrap();
        peer2.connect(&endpoint).unwrap();

        // RFC 31: the bound socket discarded its queue with the dead peer —
        // the replacement must receive no stale frame.
        assert!(
            peer2.recv_bytes(zmq::DONTWAIT).is_err(),
            "bound PAIR must discard frames queued for the dead peer; the \
             replacement received a stale frame (F-15 replay confirmed)"
        );
    }

    /// F-17 (functional-defects-plan.md): `route_fallback` /
    /// `method_not_allowed_fallback` have no request context, so every 404/405
    /// renders the Legacy error body `{"error": {"type":..., "message":...}}`
    /// — even for KServe V2 dataplane URLs, whose contract is the flat
    /// `{"error": "<message>"}` body (src/protocol/kserve.rs). Red: the
    /// fallbacks emit the nested Legacy shape on /v2/ paths.
    #[tokio::test]
    async fn test_f17_kserve_prefix_fallback_uses_flat_error_body() {
        let tmp = unique_tmp("f17-fallback");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let state = test_app_state(tmp.clone(), Config::default());
        let app = lite_server::http::routes::create_routes(state);

        // A KServe V2 dataplane URL that matches no registered route (and no
        // custom @route) → route_fallback → 404.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v2/models/nonexistent/unknown_tail")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body).to_string();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        assert!(
            json["error"].is_string(),
            "KServe V2 404 must render the flat error body \
             `{{\"error\": \"<message>\"}}`; got: {body_str}"
        );

        // Wrong method on a matched KServe inference path → 405.
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v2/models/nonexistent/infer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body).to_string();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        assert!(
            json["error"].is_string(),
            "KServe V2 405 must render the flat error body \
             `{{\"error\": \"<message>\"}}`; got: {body_str}"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}
