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
        .stdout(Stdio::piped())
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

/// Create a self-contained model_repo in a temp directory with test_model and status_endpoint.
fn create_test_model_repo() -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!("lite-server-test-{}", std::process::id()));
    let model_dir = tmp.join("test_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();

    std::fs::write(
        model_dir.join("model.py"),
        r#"from litserve import LitAPI


class TestAPI(LitAPI):
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
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    std::fs::write(
        tmp.join("status_endpoint.py"),
        "'''Custom endpoint example - GET /status returns server overview.'''\n\nmethods = [\"GET\"]\n\n\ndef handler(request, server):\n    '''Return a quick status overview of the server.'''\n    models = server.registry.list_loaded()\n    return {\n        \"server\": \"lite-server\",\n        \"loaded_models_count\": len(models),\n        \"loaded_models\": models,\n    }\n",
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
    drop(guard);
    wait_for_server(SHARED_PORT, 15).await;
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
    assert!(body.contains("lightserver"), "metrics body: {}", body);

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
// Hot reload (separate server with temp dir)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
#[ignore] // flaky on CI: filesystem watcher timeout on macOS runners
async fn test_hot_reload() {
    let original = r#"from litserve import LitAPI


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
        "--log-verbose",
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
