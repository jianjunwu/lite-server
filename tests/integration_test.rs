use reqwest;
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

fn start_server(args: &[&str]) -> std::process::Child {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
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

    cmd.spawn().expect("Failed to start server")
}

fn stop_server(mut child: std::process::Child) {
    #[cfg(unix)]
    {
        if let Ok(Some(pid)) = child.try_wait().map(|s| s.map(|_| child.id() as i32)) {
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
        } else if child.id() > 0 {
            let pid = child.id() as i32;
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
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

#[tokio::test]
async fn test_server_health() {
    let server = start_server(
        &["--port", "18000", "--model-repo", "./examples/model_repo", "--no-metrics", "--log-level", "warn"],
    );

    wait_for_server(18000, 15).await;

    let client = reqwest::Client::new();

    let resp = client
        .get("http://127.0.0.1:18000/health")
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "ok");

    let resp = client
        .get("http://127.0.0.1:18000/info")
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["server"], "lite-server");

    stop_server(server);
}

#[tokio::test]
async fn test_model_lifecycle() {
    let server = start_server(
        &["--port", "18001", "--model-repo", "./examples/model_repo", "--no-metrics", "--log-level", "warn"],
    );

    wait_for_server(18001, 15).await;

    let client = reqwest::Client::new();
    let base = "http://127.0.0.1:18001";

    let resp = client
        .post(&format!("{}/v2/repository/models/test_model/load?version=1", base))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(&format!("{}/v2/models/test_model/ready", base))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], true);

    let resp = client
        .get(&format!("{}/v2/models", base))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let models = body["models"].as_array().unwrap();
    assert!(!models.is_empty());

    let resp = client
        .post(&format!("{}/v2/repository/models/test_model/unload?version=1", base))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);

    stop_server(server);
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let server = start_server(&[
        "--port", "18002",
        "--metrics-port", "18003",
        "--model-repo", "./examples/model_repo",
        "--log-level", "warn",
    ]);

    wait_for_server(18002, 15).await;

    let client = reqwest::Client::new();

    let _ = client
        .post("http://127.0.0.1:18002/v2/repository/models/test_model/load?version=1")
        .send()
        .await;
    sleep(Duration::from_secs(2)).await;

    let resp = client
        .get("http://127.0.0.1:18003/metrics")
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("lightserver"));

    stop_server(server);
}

#[tokio::test]
async fn test_custom_endpoint() {
    let server = start_server(
        &["--port", "18004", "--model-repo", "./examples/model_repo", "--no-metrics", "--log-level", "warn"],
    );

    wait_for_server(18004, 15).await;

    let client = reqwest::Client::new();
    let base = "http://127.0.0.1:18004";

    let _ = client
        .post(&format!("{}/v2/repository/models/test_model/load?version=1", base))
        .send()
        .await;
    sleep(Duration::from_secs(2)).await;

    let resp = client
        .get(&format!("{}/status", base))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["server"], "lite-server");
    assert!(body["loaded_models_count"].as_u64().unwrap_or(0) >= 1);

    stop_server(server);
}

#[tokio::test]
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

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-test-{}", std::process::id()));
    let repo_src = project_root().join("examples/model_repo");
    let repo_dst = tmp_dir.join("model_repo");

    tokio::fs::create_dir_all(&repo_dst).await.unwrap();
    copy_dir_recursive(&repo_src, &repo_dst).await.unwrap();

    let model_py = repo_dst.join("test_model/1/model.py");
    tokio::fs::write(&model_py, original).await.unwrap();

    let server = start_server(&[
        "--port", "18005",
        "--model-repo",
        &repo_dst.to_string_lossy(),
        "--no-metrics",
        "--log-level", "warn",
    ]);

    wait_for_server(18005, 15).await;

    let client = reqwest::Client::new();
    let base = "http://127.0.0.1:18005";

    let resp = client
        .post(&format!("{}/v2/repository/models/test_model/load?version=1", base))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    sleep(Duration::from_secs(2)).await;

    let resp = client
        .post(&format!("{}/v2/models/test_model/infer", base))
        .json(&json!({"input": 5}))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);

    let modified = original.replace("x * 2", "x * 3");
    tokio::fs::write(&model_py, modified).await.unwrap();

    sleep(Duration::from_secs(6)).await;

    let resp = client
        .post(&format!("{}/v2/models/test_model/infer", base))
        .json(&json!({"input": 5}))
        .send()
        .await
        .expect("Failed to connect");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 15);

    stop_server(server);

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

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
