use reqwest;
use serde_json::{json, Value};
use serial_test::serial;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

// ---------------------------------------------------------------------------
// Helpers
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

fn start_server(args: &[&str]) -> std::process::Child {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
        // Nobody reads the child's stdout: a pipe would fill its 64KB buffer
        // and block the server on write. null also keeps the orphaned-server
        // footprint minimal when cleanup never runs.
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());

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

fn stop_server(mut child: std::process::Child) {
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

/// RAII guard that kills a dedicated server on drop — including when the test
/// panics before an explicit `stop_server()`. Without it, a failing
/// dedicated-server test orphans the server, which inherits the test's stderr
/// pipe (the `2>&1 | tail` of the runner) so the pipe never sees EOF and the
/// whole test command hangs.
struct ServerGuard(Option<std::process::Child>);

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

/// Create a self-contained model_repo in a temp directory with test_model and status_endpoint.
fn create_test_model_repo() -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!("lite-server-test-{}", std::process::id()));
    let model_dir = tmp.join("test_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();

    std::fs::write(
        model_dir.join("model.py"),
        r#"from lite_server import LitAPI

from lite_server.exceptions import BadRequestError


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        value = request.get("input", 0)
        if value is None:
            raise BadRequestError(
                "input must not be null", code="invalid_input", param="input")
        return value

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();

    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    let endpoints_dir = tmp.join("endpoints");
    std::fs::create_dir_all(&endpoints_dir).unwrap();
    std::fs::write(
        endpoints_dir.join("status.py"),
        "'''Custom endpoint example - GET /status returns server overview.'''\n\nmethods = [\"GET\"]\n\n\ndef handler(request, server):\n    '''Return a quick status overview of the server.'''\n    models = server.registry.list_loaded()\n    return {\n        \"server\": \"lite-server\",\n        \"loaded_models_count\": len(models),\n        \"loaded_models\": models,\n    }\n",
    )
    .unwrap();

    // status_ep: decorator endpoint with RateLimit + Cors (non-stream).
    std::fs::write(
        endpoints_dir.join("status_ep.py"),
        r#""""Decorator endpoint with RateLimit + Cors callbacks (non-stream)."""

from lite_server import Cors, RateLimit
from lite_server.endpoint import endpoint


@endpoint.get(
    "/status_ep",
    callbacks=[
        RateLimit(requests_per_minute=2, burst=2),
        Cors(allow_origins=["https://endpoint.example.com"]),
    ],
)
def status_ep_handler(ctx):
    return {"server": "lite-server", "endpoint": "status_ep"}
"#,
    )
    .unwrap();

    // batch_model: max_batch_size=2 so concurrent requests aggregate into
    // one BatchRequest.  predict() records the aggregated batch size in each
    // item's body; encode_response() gives the "bad" item its own 400 status
    // and X-Item header.
    let batch_dir = tmp.join("batch_model/1");
    std::fs::create_dir_all(&batch_dir).unwrap();
    std::fs::write(
        batch_dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.response import Response


class BatchAPI(LitAPI):
    def setup(self, device):
        pass

    def batch(self, inputs):
        return inputs

    def predict(self, batched):
        n = len(batched)
        return [dict(item, batch_size=n) for item in batched]

    def unbatch(self, output):
        return output

    def encode_response(self, output):
        if output.get("kind") == "bad":
            return Response(
                content={"error": "bad item"},
                status_code=400,
                headers={"X-Item": "bad"},
            )
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        batch_dir.join("config.yaml"),
        "max_batch_size: 2\nbatch_timeout: 0.1\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // policy_model: declares RateLimit + Cors callbacks so the Rust HTTP
    // layer executes rate limiting and attaches CORS headers / answers
    // preflight (LITE_POLICY_MANAGED=1 makes the Python side a declaration).
    let policy_dir = tmp.join("policy_model/1");
    std::fs::create_dir_all(&policy_dir).unwrap();
    std::fs::write(
        policy_dir.join("model.py"),
        r#"from lite_server import LitAPI, RateLimit, Cors


class PolicyAPI(LitAPI):
    callbacks = (
        RateLimit(requests_per_minute=3, burst=3),
        Cors(allow_origins=["https://app.example.com"]),
    )

    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        policy_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    tmp
}

/// Return a cached model_repo path (created once, reused across tests).
fn test_model_repo() -> &'static std::path::PathBuf {
    static REPO: OnceLock<std::path::PathBuf> = OnceLock::new();
    REPO.get_or_init(create_test_model_repo)
}

async fn wait_for_server(port: u16, timeout_secs: u64) {
    let client = reqwest::Client::new();
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

/// Poll until a model is ready. Returns false on timeout.
async fn wait_model_ready(base: &str, model: &str, timeout_secs: u64) -> bool {
    let client = reqwest::Client::new();
    let url = format!("{}/v2/models/{}/ready", base, model);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).timeout(Duration::from_secs(1)).send().await {
            if let Ok(body) = resp.json::<Value>().await {
                if body["ready"].as_bool() == Some(true) {
                    return true;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Load a model and wait until ready.
async fn load_model(base: &str, model: &str, version: &str) {
    let client = reqwest::Client::new();
    let resp = client
        .post(&format!("{}/v2/repository/models/{}/load?version={}", base, model, version))
        .send()
        .await
        .expect("load request failed");
    assert_eq!(resp.status(), 200, "load failed: {:?}", resp.text().await);
    assert!(
        wait_model_ready(base, model, 30).await,
        "model {} did not become ready",
        model
    );
}

/// Unload a model.
async fn unload_model(base: &str, model: &str, version: &str) {
    let client = reqwest::Client::new();
    let _ = client
        .post(&format!("{}/v2/repository/models/{}/unload?version={}", base, model, version))
        .send()
        .await;
}

// ---------------------------------------------------------------------------
// Shared server fixture — one server for most tests
// ---------------------------------------------------------------------------

static SHARED_SERVER: Mutex<Option<Child>> = Mutex::new(None);

const SHARED_PORT: u16 = 18010;

/// Kill any stale lite-server-core process listening on the given port.
fn kill_stale_on_port(port: u16) {
    let output = Command::new("lsof")
        .args(["-ti", &format!(":{}", port)])
        .output();
    if let Ok(out) = output {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid_str in pids.lines() {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                // Verify it's our binary before killing
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

/// Start the shared server once. Safe to call from async context.
async fn ensure_shared_server() {
    let mut guard = SHARED_SERVER.lock().unwrap();
    if guard.is_some() {
        // Already started — wait for it to be ready
        drop(guard);
        wait_for_server(SHARED_PORT, 10).await;
        return;
    }
    kill_stale_on_port(SHARED_PORT);
    let repo = test_model_repo();
    let child = start_server(&[
        "--port", &SHARED_PORT.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level", "warn",
    ]);
    *guard = Some(child);
    register_shared_cleanup();
    drop(guard);
    wait_for_server(SHARED_PORT, 15).await;
}

/// Kill the shared server (and its whole process group, Python workers
/// included) when the test binary exits. Statics never drop, so without this
/// hook the server is orphaned (PPID=1) and keeps holding the inherited
/// stderr pipe — background/CI runners then never see EOF and look hung.
/// kill_stale_on_port remains the fallback for SIGKILLed test binaries.
fn register_shared_cleanup() {
    #[cfg(unix)]
    {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            extern "C" fn cleanup() {
                // A poisoned mutex means a panic raced with us; skip and let
                // the next run's kill_stale_on_port handle leftovers.
                if let Ok(guard) = SHARED_SERVER.lock() {
                    if let Some(ref child) = *guard {
                        unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
                    }
                }
            }
            unsafe { libc::atexit(cleanup) };
        });
    }
    // Non-unix: no atexit/process-group kill — acceptable leak, these tests
    // already depend on unix tooling (lsof in kill_stale_on_port).
}

async fn shared_base() -> String {
    ensure_shared_server().await;
    format!("http://127.0.0.1:{}", SHARED_PORT)
}

// ---------------------------------------------------------------------------
// Health & Info (shared server, no state mutation)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_health_endpoint() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client.get(&format!("{}/health", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
#[serial]
async fn test_info_endpoint() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client.get(&format!("{}/info", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["server"], "lite-server");
}

// ---------------------------------------------------------------------------
// Model lifecycle (sequential — each test loads/unloads test_model)
// ---------------------------------------------------------------------------

const MODEL: &str = "test_model";

#[tokio::test]
#[serial]
async fn test_model_load_ready_infer_unload() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Load
    load_model(&base, MODEL, "1").await;

    // Ready check
    let resp = client
        .get(&format!("{}/v2/models/{}/ready", base, MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], true);

    // List models
    let resp = client.get(&format!("{}/v2/models", base)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    let models: Vec<&str> = body["models"].as_array().unwrap()
        .iter().filter_map(|m| m["name"].as_str()).collect();
    assert!(models.contains(&MODEL), "model not in list: {:?}", models);

    // Infer
    let resp = client
        .post(&format!("{}/v2/models/{}/infer", base, MODEL))
        .json(&json!({"input": 5}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);

    // Unload
    unload_model(&base, MODEL, "1").await;

    // Verify gone
    let resp = client
        .get(&format!("{}/v2/models/{}/ready", base, MODEL))
        .send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], false);
}

#[tokio::test]
#[serial]
async fn test_model_list_versions() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, MODEL, "1").await;

    let resp = client
        .get(&format!("{}/v2/models/{}/versions", base, MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let versions = body["versions"].as_array().unwrap();
    assert!(versions.iter().any(|v| v["version"] == "1"));

    unload_model(&base, MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_model_infer_versioned() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, MODEL, "1").await;

    let resp = client
        .post(&format!("{}/v2/models/{}/versions/1/infer", base, MODEL))
        .json(&json!({"input": 7}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 14);

    unload_model(&base, MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_model_repository_index() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Load a model first so the index is non-empty
    load_model(&base, MODEL, "1").await;

    let resp = client
        .post(&format!("{}/v2/repository/index", base))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    // Response can be array or object with models key
    let models = body.as_array()
        .or_else(|| body["models"].as_array())
        .expect("expected array or {models: [...]}");
    assert!(!models.is_empty(), "repository index should not be empty");

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// Batch aggregation: per-item status codes / headers must stay independent
// ---------------------------------------------------------------------------

const BATCH_MODEL: &str = "batch_model";

#[tokio::test]
#[serial]
async fn test_batch_aggregation_per_item_status_and_headers() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, BATCH_MODEL, "1").await;

    // Fire two concurrent requests — with max_batch_size=2 and
    // batch_timeout=0.1 they aggregate into ONE BatchRequest.
    let bad = client
        .post(&format!("{}/v2/models/{}/infer", base, BATCH_MODEL))
        .json(&json!({"kind": "bad"}))
        .send();
    let ok = client
        .post(&format!("{}/v2/models/{}/infer", base, BATCH_MODEL))
        .json(&json!({"kind": "ok"}))
        .send();
    let (bad_resp, ok_resp) = tokio::join!(bad, ok);
    let bad_resp = bad_resp.unwrap();
    let ok_resp = ok_resp.unwrap();

    // Per-item status codes travel independently: the bad item is a 400,
    // the ok item stays a 200 (both would have been 200 before the fix).
    assert_eq!(bad_resp.status(), 400);
    assert_eq!(ok_resp.status(), 200);

    // Per-item headers likewise: only the bad item carries X-Item.
    assert_eq!(bad_resp.headers().get("x-item").unwrap(), "bad");
    assert!(ok_resp.headers().get("x-item").is_none());

    // batch_size=2 proves the two requests really did aggregate into one
    // BatchRequest rather than two Single requests.
    let ok_body: Value = ok_resp.json().await.unwrap();
    assert_eq!(ok_body["batch_size"], 2);
    assert_eq!(ok_body["kind"], "ok");

    let bad_body: Value = bad_resp.json().await.unwrap();
    assert_eq!(bad_body["error"], "bad item");

    unload_model(&base, BATCH_MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// SSE streaming
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_sse_streaming() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, MODEL, "1").await;

    // SSE endpoint expects POST with streaming response
    let resp = client
        .post(&format!("{}/v2/models/{}/events", base, MODEL))
        .json(&json!({"input": 3}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(content_type.contains("text/event-stream"), "expected SSE content-type, got: {}", content_type);

    let body = resp.text().await.unwrap();
    // SSE format: "data: {...}\n\n"
    assert!(body.contains("data:"), "SSE response should contain data frames: {}", body);

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// WebSocket streaming
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_websocket_streaming() {
    use tokio_tungstenite::connect_async;
    use futures::SinkExt;
    use futures::StreamExt;

    let base = shared_base().await;

    load_model(&base, MODEL, "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/{}/stream", SHARED_PORT, MODEL);
    let (mut ws, _) = connect_async(&ws_url).await.expect("WS connect failed");

    // Send inference request as text (JSON)
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&json!({"input": 4})).unwrap(),
    )).await.expect("WS send failed");

    // Collect messages until done or timeout
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got_response = false;

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                match msg {
                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                        let body: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                        if body.get("done").is_some() || body.get("error").is_some() {
                            got_response = true;
                            break;
                        }
                        if body.get("output").is_some() {
                            got_response = true;
                        }
                    }
                    tokio_tungstenite::tungstenite::Message::Binary(_) => {
                        got_response = true;
                    }
                    _ => {}
                }
            }
            _ => break,
        }
    }

    assert!(got_response, "WS did not receive a response");
    let _ = ws.close(None).await;

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_metrics_endpoint() {
    let port = 18020;
    let metrics_port = 18021;
    kill_stale_on_port(port);
    kill_stale_on_port(metrics_port);
    let repo = test_model_repo();
    let server = start_server(&[
        "--port", &port.to_string(),
        "--metrics-port", &metrics_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc",
        "--log-level", "warn",
    ]);

    wait_for_server(port, 15).await;

    let client = reqwest::Client::new();

    // Load a model so metrics have content
    load_model(&format!("http://127.0.0.1:{}", port), "test_model", "1").await;

    let resp = client
        .get(&format!("http://127.0.0.1:{}/metrics", metrics_port))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("liteserver"), "metrics body: {}", body);

    stop_server(server);
}

// ---------------------------------------------------------------------------
// Custom endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_custom_endpoint_status() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // test_model is already loaded by the shared server or another test
    // Load it here to be safe
    load_model(&base, "test_model", "1").await;

    let resp = client
        .get(&format!("{}/status", base))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["server"], "lite-server");
    assert!(body["loaded_models_count"].as_u64().unwrap_or(0) >= 1);

    unload_model(&base, "test_model", "1").await;
}

// ---------------------------------------------------------------------------
// API Response Standardization — error body + observability headers
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_error_response_format() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // POST to a nonexistent model — should get structured error
    let resp = client
        .post(format!("{}/v2/models/nonexistent_model/infer", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);

    // Verify response headers
    assert!(resp.headers().get("x-request-id").is_some(),
        "x-request-id must be present on error responses");
    assert!(resp.headers().get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present on error responses");

    // Verify error body: type, message, code, param
    let body: Value = resp.json().await.unwrap();
    let err = &body["error"];
    assert!(err.is_object(), "error should be an object, got: {:?}", err);
    assert_eq!(err["type"], "not_found_error");
    assert!(!err["message"].as_str().unwrap().is_empty());
    assert_eq!(err["code"], "model_not_found");
    assert!(err["param"].is_null(),
        "param should be null but was {}", err["param"]);
}

#[tokio::test]
#[serial]
async fn test_observability_headers_on_success() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/health", base))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("x-request-id").is_some(),
        "x-request-id must be present on success responses");
    assert!(resp.headers().get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present on success responses");
    // x-processing-time-ms should be a non-negative integer
    let ms = resp.headers().get("x-processing-time-ms").unwrap().to_str().unwrap();
    assert!(ms.parse::<u64>().is_ok(),
        "x-processing-time-ms should be a u64, got: {}", ms);
}

#[tokio::test]
#[serial]
async fn test_x_client_request_id_propagation() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    let trace_id = "integration-test-001";

    let resp = client
        .get(format!("{}/health", base))
        .header("x-client-request-id", trace_id)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("x-request-id").unwrap().to_str().unwrap(),
        trace_id,
        "x-request-id should echo x-client-request-id"
    );
}

#[tokio::test]
#[serial]
async fn test_infer_error_response_has_code_and_param() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Load model first so the model route is active
    load_model(&base, MODEL, "1").await;

    // test_model raises BadRequestError(code="invalid_input", param="input")
    // for null input — verifies the full Python → Rust → client chain.
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, MODEL))
        .json(&json!({"input": null}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert!(resp.headers().get("x-request-id").is_some(),
        "x-request-id must be present");
    assert!(resp.headers().get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present");

    let body: Value = resp.json().await.unwrap();
    let err = &body["error"];
    assert_eq!(err["type"], "invalid_request_error");
    assert_eq!(err["message"], "input must not be null");
    assert_eq!(err["code"], "invalid_input");
    assert_eq!(err["param"], "input");

    unload_model(&base, MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_unknown_route_standardized_404() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/no-such-route", base))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    assert!(resp.headers().get("x-request-id").is_some());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "not_found_error");
    assert_eq!(body["error"]["code"], "route_not_found");
    assert!(body["error"]["param"].is_null());
}

#[tokio::test]
#[serial]
async fn test_method_not_allowed_standardized_405() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // /health only supports GET
    let resp = client
        .post(format!("{}/health", base))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 405);
    assert!(resp.headers().get("x-request-id").is_some());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "method_not_allowed");
    assert_eq!(body["error"]["code"], "method_not_allowed");
    assert!(body["error"]["param"].is_null());
}

#[tokio::test]
#[serial]
async fn test_malformed_json_standardized_400() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Route exists; the ApiJson extractor rejects before any model lookup.
    let resp = client
        .post(format!("{}/v2/models/any_model/infer", base))
        .header("content-type", "application/json")
        .body("{not valid json")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert!(resp.headers().get("x-request-id").is_some());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["code"], "invalid_request_body");
    assert!(body["error"]["param"].is_null());
}

// ---------------------------------------------------------------------------
// Custom endpoint responses carry CORS on every path — covers B2
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_endpoint_responses_carry_cors() {
    // Dedicated server: status_ep's RateLimit bucket (burst=2) is shared
    // across tests on the shared server, so a fresh process gives a clean
    // starting bucket.
    let port = 18041;
    kill_stale_on_port(port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level",
        "warn",
    ]);
    wait_for_server(port, 15).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // status_ep: RateLimit(burst=2) + Cors. Success response carries ACAO.
    let resp = client
        .get(format!("{}/status_ep", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "https://endpoint.example.com"
    );
    let _ = resp.text().await.unwrap();

    // 2nd request still within burst.
    let resp = client
        .get(format!("{}/status_ep", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.text().await.unwrap();

    // 3rd request: rate-limited → 429. Before B2 the early Err return (rate
    // limit) propagated without CORS; the outer wrapper now attaches ACAO to
    // the 429 as well.
    let resp = client
        .get(format!("{}/status_ep", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "https://endpoint.example.com"
    );
}

// ---------------------------------------------------------------------------
// SSE streaming must carry CORS headers — covers A2
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_sse_rate_limit_returns_429() {
    // Dedicated server: the rate-limit bucket is shared across tests on the
    // shared server (key model:/predict, burst=3), so a fresh process gives a
    // deterministic starting bucket.
    let port = 18040;
    kill_stale_on_port(port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level",
        "warn",
    ]);
    wait_for_server(port, 15).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();
    load_model(&base, POLICY_MODEL, "1").await;

    // policy_model: RateLimit(rpm=3, burst=3). First 3 SSE requests allowed,
    // 4th rejected with 429 + sane Retry-After.
    for i in 0..3 {
        let resp = client
            .post(format!("{}/v2/models/{}/events", base, POLICY_MODEL))
            .json(&json!({"input": i}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "SSE request {} should be allowed", i);
    }
    let resp = client
        .post(format!("{}/v2/models/{}/events", base, POLICY_MODEL))
        .json(&json!({"input": 9}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    let retry = resp
        .headers()
        .get("retry-after")
        .expect("429 must carry Retry-After");
    let secs: u64 = retry.to_str().unwrap().parse().unwrap();
    assert!(
        (1..=60).contains(&secs),
        "Retry-After {} out of sane range",
        secs
    );

    // _server guard kills the process group on drop (incl. panic path).
}

// ---------------------------------------------------------------------------
// SSE streaming must carry CORS headers — covers A2
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_sse_response_carries_cors_headers() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, POLICY_MODEL, "1").await;

    // SSE success path must carry CORS headers (attach_cors_headers wraps the
    // whole entry, including the stream-start response).
    let resp = client
        .post(format!("{}/v2/models/{}/events", base, POLICY_MODEL))
        .json(&json!({"input": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
    // Drain the SSE body
    let _ = resp.text().await.unwrap();

    unload_model(&base, POLICY_MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// OPTIONS preflight (CORS) on inference routes — covers A1
// ---------------------------------------------------------------------------

const POLICY_MODEL: &str = "policy_model";

#[tokio::test]
#[serial]
async fn test_inference_options_preflight_all_routes() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // policy_model declares Cors(allow_origins=["https://app.example.com"]).
    load_model(&base, POLICY_MODEL, "1").await;

    // All four inference routes (incl. the two versioned ones) must answer
    // OPTIONS preflight with 204 + ACAO. The versioned routes used to 500
    // because inference_options_handler took Path<String> on a 2-param route.
    let routes = [
        format!("/v2/models/{}/infer", POLICY_MODEL),
        format!("/v2/models/{}/versions/1/infer", POLICY_MODEL),
        format!("/v2/models/{}/events", POLICY_MODEL),
        format!("/v2/models/{}/versions/1/events", POLICY_MODEL),
    ];
    for path in &routes {
        let resp = client
            .request(reqwest::Method::OPTIONS, format!("{}{}", base, path))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            204,
            "OPTIONS {} should be 204, got {}",
            path,
            resp.status()
        );
        let acao = resp
            .headers()
            .get("access-control-allow-origin")
            .unwrap_or_else(|| panic!("ACAO missing on OPTIONS {}", path));
        assert_eq!(acao.to_str().unwrap(), "https://app.example.com");
    }

    // A model WITHOUT a Cors declaration answers OPTIONS with 405.
    load_model(&base, MODEL, "1").await;
    let resp = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/v2/models/{}/infer", base, MODEL),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    unload_model(&base, POLICY_MODEL, "1").await;
    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// Hot reload (separate server with temp dir)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
#[ignore] // flaky on CI: filesystem watcher timeout on macOS runners
async fn test_hot_reload() {
    let original = r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-hotreload-{}", std::process::id()));
    let repo_src = test_model_repo().clone();
    let repo_dst = tmp_dir.join("model_repo");

    tokio::fs::create_dir_all(&repo_dst).await.unwrap();
    copy_dir_recursive(&repo_src, &repo_dst).await.unwrap();

    // Enable hot reload in config
    let config_yaml = repo_dst.join("test_model/1/config.yaml");
    tokio::fs::write(&config_yaml, "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\nhot_reload: true\nhot_reload_interval: 1.0\n").await.unwrap();

    let model_py = repo_dst.join("test_model/1/model.py");
    tokio::fs::write(&model_py, original).await.unwrap();

    let port = 18030;
    kill_stale_on_port(port);
    let server = start_server(&[
        "--port", &port.to_string(),
        "--model-repo", &repo_dst.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level", "warn",
    ]);

    wait_for_server(port, 15).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // Load and verify initial behavior (x * 2)
    load_model(&base, "test_model", "1").await;
    let resp = client
        .post(&format!("{}/v2/models/test_model/infer", base))
        .json(&json!({"input": 5}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);

    // Modify the model (x * 2 → x * 3)
    let modified = original.replace("x * 2", "x * 3");
    tokio::fs::write(&model_py, modified).await.unwrap();

    // Poll until hot reload picks up the change (no fixed sleep)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut reloaded = false;
    while tokio::time::Instant::now() < deadline {
        let resp = client
            .post(&format!("{}/v2/models/test_model/infer", base))
            .json(&json!({"input": 5}))
            .send().await.unwrap();
        if resp.status() == 200 {
            let body: Value = resp.json().await.unwrap();
            if body["output"] == 15 {
                reloaded = true;
                break;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(reloaded, "hot reload did not pick up model change within 30s");

    stop_server(server);
    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(dst).await?;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            Box::pin(copy_dir_recursive(&src_path, &dst_path)).await?;
        } else {
            tokio::fs::copy(&src_path, &dst_path).await?;
        }
    }
    Ok(())
}
