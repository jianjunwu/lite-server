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

/// Kill any stale lite-server processes that may be holding the test port.
fn kill_stale_servers(port: u16) {
    #[cfg(unix)]
    {
        // Best-effort: lsof the port and SIGKILL the PIDs
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

/// Spawn the Python init generator to create a temporary project,
/// then start lite-server against it and assert the model is
/// auto-loaded, active, and inference works.
#[tokio::test]
async fn test_init_project_runs_end_to_end() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-init-test-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();

    // Generate project via Python CLI
    let output = Command::new("python")
        .arg("-m")
        .arg("lite_server.cli")
        .arg("init")
        .arg("test_proj")
        .arg("--template")
        .arg("empty")
        .current_dir(&tmp_dir)
        .output()
        .expect("failed to run lite-server init");

    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_dir = tmp_dir.join("test_proj");
    assert!(project_dir.exists(), "project directory not created");

    // Clean up stale processes before binding the port
    kill_stale_servers(18090);

    let _server = start_server(
        &project_dir,
        &[
            "--config",
            &project_dir.join("server.yaml").to_string_lossy(),
            "--port",
            "18090",
            "--metrics-port",
            "18091",
            "--no-metrics",
            "--no-grpc",
            "--log-level",
            "warn",
        ],
    );

    wait_for_server(18090, 30).await;

    let client = reqwest::Client::new();
    let base = "http://127.0.0.1:18090";

    // 1. Poll until model is ready (auto-loaded + active)
    let mut ready = false;
    let mut active_version: Option<String> = None;
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
                        active_version = body["active_version"].as_str().map(|s| s.to_string());
                        break;
                    }
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(ready, "model should be ready after init project startup");
    assert!(
        active_version.is_some(),
        "active_version should be set"
    );

    // 2. Inference should succeed
    let resp = client
        .post(format!("{}/v2/models/my_model/infer", base))
        .json(&json!({"input": 5}))
        .send()
        .await
        .expect("Failed to connect to infer endpoint");
    assert_eq!(
        resp.status(), 200,
        "inference should succeed: {:?}",
        resp.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["result"], 10, "5 * 2 should be 10: {:?}", body);

    let _ = std::fs::remove_dir_all(&tmp_dir);
}
