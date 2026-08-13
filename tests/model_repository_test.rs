//! End-to-end coverage for the model repository upload/download surface
//! (.claude/model-upload-and-retire-plan.md, batches 0-2).
//!
//! `test_upload_lma_places_files_and_loads_model` and
//! `test_upload_lma_reports_load_failure` guard the `.lma` upload fix
//! (plan part 1); the raw round-trips guard F7.1/F7.2.

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

/// Per-process monotonic port allocator (same scheme as integration_test.rs):
/// no two concurrent tests share a port, so `kill_stale_servers` can only
/// ever hit this test's own stale server from a previously crashed run.
fn next_test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(20000);
    NEXT.fetch_add(1, Ordering::Relaxed)
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

fn start_server(args: &[&str]) -> ServerGuard {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
        // Nobody reads the child's stdout: a pipe would fill its 64KB buffer
        // and block the server on write (same rationale as integration_test.rs).
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

fn write_server_yaml(
    tmp: &std::path::Path,
    port: u16,
    repo: &std::path::Path,
) -> std::path::PathBuf {
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
  control_mode: explicit
"#,
        port,
        repo.to_string_lossy()
    );
    let yaml = tmp.join("server.yaml");
    std::fs::write(&yaml, content).unwrap();
    yaml
}

/// Build a multipart body carrying the given file parts as raw bytes
/// (the reqwest multipart feature is not enabled in dev-deps).
fn multipart_body(boundary: &str, parts: &[(&str, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    for (filename, data) in parts {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\n\
                 Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
                 Content-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(data);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

async fn upload_files(
    base: &str,
    model: &str,
    version: &str,
    load: bool,
    parts: &[(&str, &[u8])],
) -> reqwest::Response {
    let boundary = "----repo-upload-boundary";
    let client = reqwest::Client::new();
    client
        .post(format!(
            "{}/v2/repository/models/{}/versions/{}/upload?load={}",
            base, model, version, load
        ))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(multipart_body(boundary, parts))
        .send()
        .await
        .expect("upload request failed")
}

/// Pack a versioned model directory into a `.lma` artifact via the Python CLI.
fn pack_lma(
    model_dir: &std::path::Path,
    version: &str,
    output_dir: &std::path::Path,
) -> std::path::PathBuf {
    let output = Command::new("python")
        .args([
            "-m",
            "lite_server.cli",
            "pack",
            model_dir.to_str().unwrap(),
            "--version",
            version,
            "--output",
            output_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run lite-server pack");
    assert!(
        output.status.success(),
        "pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let name = model_dir
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    output_dir.join(format!("{}_v{}.lma", name, version))
}

/// Minimal loadable model: doubles its input (same shape as the
/// autodiscovery fixture in lma_autodiscovery_test.rs).
const MODEL_PY: &str = r#"
from lite_server import LitAPI

class TestAPI(LitAPI):
    def setup(self, device):
        self.model = lambda x: x * 2

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x, **kwargs):
        if isinstance(x, list):
            return [self.model(item) for item in x]
        return self.model(x)

    def encode_response(self, output):
        return {"result": output}
"#;

/// Plan part 1: uploading a `.lma` must place files under {name}/{v}
/// (no nested version directory), auto-load on ?load=true, and report the
/// real load outcome in `loaded`.
#[tokio::test]
async fn test_upload_lma_places_files_and_loads_model() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-upload-lma-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    // Pack a real (loadable) model.
    let src = tmp.join("src").join("mymodel");
    let v1 = src.join("1");
    std::fs::create_dir_all(&v1).unwrap();
    std::fs::write(v1.join("model.py"), MODEL_PY).unwrap();
    std::fs::write(v1.join("config.yaml"), "max_batch_size: 1\nbatch_timeout: 0.0\n").unwrap();
    let lma = pack_lma(&src, "1", &tmp);
    let lma_bytes = std::fs::read(&lma).unwrap();

    let port = next_test_port();
    kill_stale_servers(port);
    let yaml = write_server_yaml(&tmp, port, &repo);
    let _server = start_server(&["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;

    let base = format!("http://127.0.0.1:{}", port);
    let resp = upload_files(&base, "mymodel", "1", true, &[("mymodel_v1.lma", &lma_bytes)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "upload failed: {}", body);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        json["loaded"], true,
        "load=true must report the real load outcome"
    );

    // No nested duplicate version directory.
    assert!(repo.join("mymodel").join("1").join("model.py").exists());
    assert!(!repo.join("mymodel").join("1").join("1").exists());

    // The model must actually be ready and serve inference.
    let client = reqwest::Client::new();
    let mut ready = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if let Ok(r) = client
            .get(format!("{}/v2/models/mymodel/versions/1/ready", base))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if v["ready"] == true {
                    ready = true;
                    break;
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(ready, "uploaded model must become ready");

    let infer = client
        .post(format!("{}/v2/models/mymodel/infer", base))
        .json(&json!({"input": 5}))
        .send()
        .await
        .expect("infer request failed");
    assert_eq!(infer.status(), 200);
    let infer_body: serde_json::Value = infer.json().await.unwrap();
    assert_eq!(infer_body["result"], 10);

    let _ = std::fs::remove_dir_all(&tmp);
}

/// Plan part 1: when auto-load after upload fails, the response must say so
/// (loaded=false + load_error) instead of echoing the ?load= query param.
#[tokio::test]
async fn test_upload_lma_reports_load_failure() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-upload-loadfail-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    // Version directory without model.py or ensemble config — the load must
    // fail immediately with "neither model.py nor ensemble config found".
    let src = tmp.join("src").join("mymodel");
    let v1 = src.join("1");
    std::fs::create_dir_all(&v1).unwrap();
    std::fs::write(v1.join("data.txt"), "not a model").unwrap();
    let lma = pack_lma(&src, "1", &tmp);
    let lma_bytes = std::fs::read(&lma).unwrap();

    let port = next_test_port();
    kill_stale_servers(port);
    let yaml = write_server_yaml(&tmp, port, &repo);
    let _server = start_server(&["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;

    let base = format!("http://127.0.0.1:{}", port);
    let resp = upload_files(&base, "mymodel", "1", true, &[("mymodel_v1.lma", &lma_bytes)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "upload failed: {}", body);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        json["loaded"], false,
        "failed auto-load must report loaded=false"
    );
    let load_error = json["load_error"]
        .as_str()
        .expect("load_error must be present and a string");
    assert!(!load_error.is_empty());

    let _ = std::fs::remove_dir_all(&tmp);
}

/// F7.1: raw upload → single-file download must return byte-identical content.
#[tokio::test]
async fn test_raw_upload_single_file_download_byte_identical() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-raw-roundtrip-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    let port = next_test_port();
    kill_stale_servers(port);
    let yaml = write_server_yaml(&tmp, port, &repo);
    let _server = start_server(&["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;

    let base = format!("http://127.0.0.1:{}", port);
    let content: &[u8] = b"def predict(x): return x * 3\n";
    let resp = upload_files(&base, "mymodel", "1", false, &[("model.py", content)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "upload failed: {}", body);

    let client = reqwest::Client::new();
    let dl = client
        .get(format!(
            "{}/v2/repository/models/mymodel/versions/1/download?file=model.py",
            base
        ))
        .send()
        .await
        .expect("download request failed");
    assert_eq!(dl.status(), 200);
    let disposition = dl
        .headers()
        .get("content-disposition")
        .expect("content-disposition header required")
        .to_str()
        .unwrap()
        .to_string();
    assert!(disposition.contains("attachment"));

    let dl_body = dl.bytes().await.unwrap();
    assert_eq!(
        dl_body.as_ref(),
        content,
        "downloaded bytes must match the uploaded file"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// F7.2: raw upload → whole-directory .lma download → CLI unpack must
/// validate and reproduce the uploaded files.
#[tokio::test]
async fn test_raw_upload_lma_download_unpack_matches_manifest() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-lma-roundtrip-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    let port = next_test_port();
    kill_stale_servers(port);
    let yaml = write_server_yaml(&tmp, port, &repo);
    let _server = start_server(&["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;

    let base = format!("http://127.0.0.1:{}", port);
    let model_py: &[u8] = b"def predict(x): return x\n";
    let config_yaml: &[u8] = b"max_batch_size: 1\n";
    let resp = upload_files(
        &base,
        "mymodel",
        "1",
        false,
        &[("model.py", model_py), ("config.yaml", config_yaml)],
    )
    .await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "upload failed: {}", body);

    let client = reqwest::Client::new();
    let dl = client
        .get(format!(
            "{}/v2/repository/models/mymodel/versions/1/download",
            base
        ))
        .send()
        .await
        .expect("download request failed");
    assert_eq!(dl.status(), 200);
    let lma_bytes = dl.bytes().await.unwrap();

    // Save the artifact and unpack it via the CLI — _cmd_unpack runs
    // validate() before extracting, so exit 0 proves checksum integrity.
    let down_dir = tmp.join("down");
    std::fs::create_dir_all(&down_dir).unwrap();
    let lma_path = down_dir.join("mymodel_v1.lma");
    std::fs::write(&lma_path, &lma_bytes).unwrap();

    let unpacked_dir = down_dir.join("unpacked");
    let output = Command::new("python")
        .args([
            "-m",
            "lite_server.cli",
            "unpack",
            lma_path.to_str().unwrap(),
            "--to",
            unpacked_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run lite-server unpack");
    assert!(
        output.status.success(),
        "unpack must validate and succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(
        std::fs::read(unpacked_dir.join("mymodel").join("1").join("model.py")).unwrap(),
        model_py
    );
    assert_eq!(
        std::fs::read(unpacked_dir.join("mymodel").join("1").join("config.yaml")).unwrap(),
        config_yaml
    );

    let manifest_raw = std::fs::read(unpacked_dir.join("mymodel").join("manifest.json")).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_raw).unwrap();
    assert_eq!(manifest["name"], "mymodel");
    assert_eq!(manifest["version"], "1");
    let files = manifest["files"].as_object().unwrap();
    assert!(files.contains_key("1/model.py"));
    assert!(files.contains_key("1/config.yaml"));

    let _ = std::fs::remove_dir_all(&tmp);
}
