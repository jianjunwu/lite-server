//! Decisive test: `LitAPI.teardown()` (and the `before_teardown` /
//! `after_teardown` callbacks) must run on graceful server shutdown.
//!
//! Regression guard for the teardown dead zone: workers were terminated with
//! SIGKILL on every stop path, so the Python teardown (fired from the worker's
//! `finally`) never ran in production. The fix sends the worker an explicit
//! ZMQ stop message first, so the worker exits cleanly and runs teardown;
//! SIGKILL remains only as the hung-worker fallback.
//!
//! RED marker: before the fix, the marker file is never created.

use serial_test::serial;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;

fn project_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn lite_server_bin() -> std::path::PathBuf {
    project_root().join("target").join("debug").join("lite-server-core")
}

fn next_test_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(20000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A model whose `teardown()` + callbacks append ordered lines to the marker
/// file (path injected via env so the spawned worker inherits it).
fn create_teardown_marker_repo(marker_path: &std::path::Path) -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-teardown-repo-{}",
        std::process::id()
    ));
    let model_dir = tmp.join("marker_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        format!(
            r#"import os
from lite_server import LitAPI, Callback

MARKER = {marker:?}

class TeardownOrderCB(Callback):
    def before_teardown(self, lit_api):
        with open(MARKER, "a") as f:
            f.write("before_teardown\n")

    def after_teardown(self, lit_api):
        with open(MARKER, "a") as f:
            f.write("after_teardown\n")

class MarkerModel(LitAPI):
    callbacks = (TeardownOrderCB(),)

    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x * 2}}

    def encode_response(self, output):
        return output

    def teardown(self):
        with open(MARKER, "a") as f:
            f.write("teardown\n")
"#,
            marker = marker_path.to_str().unwrap()
        ),
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    tmp
}

/// Poll until the HTTP server accepts connections (any response counts).
async fn wait_server_up(base: &str, timeout_secs: u64) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(format!("{}/v2/health/ready", base)).timeout(Duration::from_secs(1)).send().await {
            let _ = resp;
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn wait_model_ready(base: &str, model: &str, timeout_secs: u64) -> bool {
    let client = reqwest::Client::new();
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

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_teardown_runs_on_graceful_shutdown() {
    let marker = std::env::temp_dir().join(format!(
        "lite-server-teardown-marker-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&marker);

    let port = next_test_port();
    let repo = create_teardown_marker_repo(&marker);

    let mut child = Command::new(lite_server_bin())
        .arg("serve")
        .arg("--port")
        .arg(port.to_string())
        .arg("--model-repo")
        .arg(&repo)
        .arg("--no-metrics")
        .arg("--no-grpc")
        .current_dir(project_root())
        // Test-only watchdog: the server self-terminates (graceful shutdown)
        // once reparented to init, so a panicking test run leaves no orphan
        // holding the port (mirrors integration_test.rs).
        .env("LITESERVER_DIE_WITH_PARENT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start server");

    let base = format!("http://localhost:{port}");

    // Load the model explicitly (repo-scan alone does not load), then wait
    // until it is fully ready — the worker's recv loop is live, so the
    // teardown under test happens against a serving worker.
    assert!(
        wait_server_up(&base, 20).await,
        "server did not come up within 20s"
    );
    let client = reqwest::Client::new();
    let load_resp = client
        .post(format!(
            "{}/v2/repository/models/marker_model/versions/1/load",
            base
        ))
        .send()
        .await
        .expect("load request failed");
    assert_eq!(load_resp.status(), 200, "load failed");
    assert!(
        wait_model_ready(&base, "marker_model", 30).await,
        "marker_model did not become ready"
    );

    // Graceful shutdown: SIGTERM to the server.
    let pid = child.id() as i32;
    unsafe { libc::kill(pid, libc::SIGTERM) };

    // The server must exit promptly (drain → stop workers → teardown).
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let _ = child.wait();
    })
    .await;
    assert!(result.is_ok(), "server did not exit within 10s");

    // Decisive assertion: the marker must exist with the exact hook order.
    let content = std::fs::read_to_string(&marker)
        .unwrap_or_else(|_| panic!("teardown marker file missing: {}", marker.display()));
    assert_eq!(
        content,
        "before_teardown\nteardown\nafter_teardown\n",
        "teardown did not run (or ran out of order) on graceful shutdown"
    );

    let _ = std::fs::remove_file(&marker);
    let _ = std::fs::remove_dir_all(&repo);
}
