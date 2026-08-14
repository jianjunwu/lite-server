// Port block for this file: 18241-18245 (fixed — kill_stale_servers needs
// stable ports). Must not overlap integration_test.rs (18092-18099/18212-18232),
// audit_ensemble_stream.rs (19700段) or model_repository_test.rs (20000段).

use serde_json::json;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

fn project_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn lite_server_bin() -> std::path::PathBuf {
    project_root()
        .join("target")
        .join("debug")
        .join("lite-server-core")
}

fn kill_stale_servers(port: u16) {
    #[cfg(unix)]
    {
        let output = std::process::Command::new("lsof")
            .args(["-t", "-i", &format!("tcp:{}", port)])
            .output();
        if let Ok(out) = output {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                if let Ok(pid) = line.trim().parse::<i32>() {
                    let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
                }
            }
        }
    }
}

struct ServerGuard(std::process::Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            let pid = self.0.id() as i32;
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
        #[cfg(not(unix))]
        {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn start_server(project_dir: &std::path::Path, args: &[&str]) -> ServerGuard {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

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

    ServerGuard(cmd.spawn().expect("Failed to start server"))
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
        sleep(Duration::from_millis(200)).await;
    }
    panic!("Server did not start within {} seconds", timeout_secs);
}

/// Test that .lma files placed in the model repository are auto-discovered
/// and loaded on server startup.
#[tokio::test]
async fn test_lma_autodiscovery_on_startup() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-lma-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    std::fs::create_dir_all(&tmp_dir).unwrap();

    // 1. Create a model directory to pack
    let model_dir = tmp_dir.join("my_model");
    let v1_dir = model_dir.join("1");
    std::fs::create_dir_all(&v1_dir).unwrap();

    std::fs::write(
        v1_dir.join("model.py"),
        r#"
from lite_server import LitAPI

class TestAPI(LitAPI):
    def setup(self, device):
        self.logger.info(f"device: {device}")
        self.model = lambda x: x * 2

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x, **kwargs):
        if isinstance(x, list):
            return [self.model(item) for item in x]
        return self.model(x)

    def encode_response(self, output):
        return {"result": output}
"#,
    )
    .unwrap();

    std::fs::write(
        v1_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\n",
    )
    .unwrap();

    // 2. Pack into .lma via Python CLI
    let output = Command::new("python")
        .args([
            "-m",
            "lite_server.cli",
            "pack",
            model_dir.to_str().unwrap(),
            "--version",
            "1",
            "--output",
            tmp_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run lite-server pack");

    assert!(
        output.status.success(),
        "pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let lma_file = tmp_dir.join("my_model_v1.lma");
    assert!(lma_file.exists(), "lma file should exist after pack");

    // 3. Set up model_repo with the .lma file
    let repo_dir = tmp_dir.join("model_repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::copy(&lma_file, repo_dir.join("my_model_v1.lma")).unwrap();

    // 4. Write server.yaml (orchestration section auto-loads all models)
    let server_yaml_content = format!(
        r#"
server:
  host: 0.0.0.0
  http_port: 18241
  grpc_port: 18242
  metrics_port: 18243
  log_level: warn
metrics:
  enabled: false
grpc:
  enabled: false
model_repository:
  path: {}
orchestration:
  control_mode: all
"#,
        repo_dir.to_string_lossy()
    );
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(&server_yaml, server_yaml_content).unwrap();

    // 6. Start server
    kill_stale_servers(18241);

    let _server = start_server(&tmp_dir, &["--config", &server_yaml.to_string_lossy()]);

    wait_for_server(18241, 30).await;

    let client = reqwest::Client::new();
    let base = "http://127.0.0.1:18241";

    // 7. Model from .lma should be auto-discovered and ready
    let mut ready = false;
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < ready_deadline {
        if let Ok(resp) = client
            .get(format!("{}/v2/models/my_model/ready", base))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            if resp.status() == 200 {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if body["ready"] == true {
                        ready = true;
                        break;
                    }
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(
        ready,
        "model from .lma should be auto-discovered and ready"
    );

    // 8. Inference should work
    let resp = client
        .post(format!("{}/v2/models/my_model/infer", base))
        .json(&json!({"input": 5}))
        .send()
        .await
        .expect("Failed to connect to infer endpoint");
    assert_eq!(
        resp.status(),
        200,
        "inference should succeed: {:?}",
        resp.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["result"], 10, "5 * 2 should be 10: {:?}", body);

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

fn pack_model_to(tmp_dir: &std::path::Path, name: &str, model_py: &str) -> std::path::PathBuf {
    let model_dir = tmp_dir.join(name);
    let v1_dir = model_dir.join("1");
    std::fs::create_dir_all(&v1_dir).unwrap();
    std::fs::write(v1_dir.join("model.py"), model_py).unwrap();
    std::fs::write(v1_dir.join("config.yaml"), "max_batch_size: 1\nbatch_timeout: 0.0\n").unwrap();

    let output = Command::new("python")
        .args([
            "-m",
            "lite_server.cli",
            "pack",
            model_dir.to_str().unwrap(),
            "--version",
            "1",
            "--output",
            tmp_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run lite-server pack");
    assert!(
        output.status.success(),
        "pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    tmp_dir.join(format!("{}_v1.lma", name))
}

fn write_server_yaml_all(tmp_dir: &std::path::Path, port: u16, repo_dir: &std::path::Path) -> std::path::PathBuf {
    let content = format!(
        r#"
server:
  host: 0.0.0.0
  http_port: {}
  log_level: warn
metrics:
  enabled: false
grpc:
  enabled: false
model_repository:
  path: {}
orchestration:
  control_mode: all
"#,
        port,
        repo_dir.to_string_lossy()
    );
    let yaml = tmp_dir.join(format!("server-{}.yaml", port));
    std::fs::write(&yaml, content).unwrap();
    yaml
}

async fn wait_ready(base: &str, model: &str) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client
            .get(format!("{}/v2/models/{}/ready", base, model))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            if resp.status() == 200 {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if body["ready"] == true {
                        return true;
                    }
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    false
}

/// H6: after a restart, an up-to-date extracted directory must NOT be
/// re-unpacked (extractall would clobber local edits) — the artifact's
/// mtime is older than the directory it produced.
#[tokio::test]
async fn test_lma_restart_preserves_local_edits_when_artifact_older() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-lma-h6-edit-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    std::fs::create_dir_all(&tmp_dir).unwrap();

    let model_py = "from lite_server import LitAPI\n\nclass TestAPI(LitAPI):\n    def predict(self, x, **kwargs):\n        return x * 2\n";
    let lma = pack_model_to(&tmp_dir, "my_model", model_py);
    let repo_dir = tmp_dir.join("model_repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::copy(&lma, repo_dir.join("my_model_v1.lma")).unwrap();

    let port = 18244u16;
    kill_stale_servers(port);
    let yaml = write_server_yaml_all(&tmp_dir, port, &repo_dir);
    let server_a = start_server(&tmp_dir, &["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;
    let base = format!("http://127.0.0.1:{}", port);
    assert!(wait_ready(&base, "my_model").await, "model must be ready after first boot");
    drop(server_a);

    // Local edit on the extracted tree.
    let extracted = repo_dir.join("my_model").join("1").join("model.py");
    let mut content = std::fs::read_to_string(&extracted).unwrap();
    content.push_str("\n# local edit\n");
    std::fs::write(&extracted, &content).unwrap();

    // Restart against the same repo.
    let server_b = start_server(&tmp_dir, &["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;
    assert!(wait_ready(&base, "my_model").await, "model must be ready after restart");

    let after = std::fs::read_to_string(&extracted).unwrap();
    assert!(
        after.contains("# local edit"),
        "restart must not re-unpack over a fresh directory; local edit lost"
    );
    drop(server_b);

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// H6: a replaced artifact (newer mtime) must re-unpack normally —
/// the updated content lands via staging + swap.
#[tokio::test]
async fn test_lma_restart_reunpacks_when_artifact_newer() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-lma-h6-newer-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    std::fs::create_dir_all(&tmp_dir).unwrap();

    let v1_py = "from lite_server import LitAPI\n\nclass TestAPI(LitAPI):\n    def predict(self, x, **kwargs):\n        return x * 2\n";
    let lma = pack_model_to(&tmp_dir, "my_model", v1_py);
    let repo_dir = tmp_dir.join("model_repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::copy(&lma, repo_dir.join("my_model_v1.lma")).unwrap();

    let port = 18245u16;
    kill_stale_servers(port);
    let yaml = write_server_yaml_all(&tmp_dir, port, &repo_dir);
    let server_a = start_server(&tmp_dir, &["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;
    let base = format!("http://127.0.0.1:{}", port);
    assert!(wait_ready(&base, "my_model").await, "model must be ready after first boot");
    drop(server_a);

    // Replace the artifact with a NEW pack (newer mtime; sleep past the
    // filesystem mtime granularity of the extracted dir).
    sleep(Duration::from_millis(1100)).await;
    let v2_py = "from lite_server import LitAPI\n\nclass TestAPI(LitAPI):\n    def predict(self, x, **kwargs):\n        return x * 3\n";
    let _ = std::fs::remove_dir_all(tmp_dir.join("my_model"));
    let lma2 = pack_model_to(&tmp_dir, "my_model", v2_py);
    std::fs::copy(&lma2, repo_dir.join("my_model_v1.lma")).unwrap();

    let server_b = start_server(&tmp_dir, &["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;
    assert!(wait_ready(&base, "my_model").await, "model must be ready after restart");

    let extracted = repo_dir.join("my_model").join("1").join("model.py");
    let after = std::fs::read_to_string(&extracted).unwrap();
    assert!(
        after.contains("x * 3"),
        "newer artifact must be re-unpacked; old content still on disk"
    );
    drop(server_b);

    let _ = std::fs::remove_dir_all(&tmp_dir);
}
