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
  http_port: 18095
  grpc_port: 18096
  metrics_port: 18097
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
    kill_stale_servers(18095);

    let _server = start_server(&tmp_dir, &["--config", &server_yaml.to_string_lossy()]);

    wait_for_server(18095, 30).await;

    let client = reqwest::Client::new();
    let base = "http://127.0.0.1:18095";

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
