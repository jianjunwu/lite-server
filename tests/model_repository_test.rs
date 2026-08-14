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
    write_server_yaml_mode(tmp, port, repo, "explicit", 30)
}

/// control_mode/poll_interval parameterized variant: auto-mode tests need
/// a short resync interval and an explicit load_models entry.
fn write_server_yaml_mode(
    tmp: &std::path::Path,
    port: u16,
    repo: &std::path::Path,
    control_mode: &str,
    poll_interval: u64,
) -> std::path::PathBuf {
    // Auto mode reconciles only models listed in load_models; explicit mode
    // is manual-control (no load list).
    let load_block = if control_mode == "auto" {
        "  load_models:\n    - mymodel\n"
    } else {
        ""
    };
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
  control_mode: {}
  poll_interval: {}
{}"#,
        port,
        repo.to_string_lossy(),
        control_mode,
        poll_interval,
        load_block,
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

// ===== G1: delete-version coverage (plan part 2, batch 1) =====

/// Poll the versioned ready endpoint until `ready: true` or the deadline.
async fn wait_until_ready(base: &str, model: &str, version: &str, timeout_secs: u64) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if let Ok(r) = client
            .get(format!("{}/v2/models/{}/versions/{}/ready", base, model, version))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if v["ready"] == true {
                    return true;
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    false
}

async fn delete_version(base: &str, model: &str, version: &str, force: bool) -> reqwest::Response {
    let client = reqwest::Client::new();
    let mut url = format!("{}/v2/models/{}/versions/{}", base, model, version);
    if force {
        url.push_str("?force=true");
    }
    client
        .delete(&url)
        .send()
        .await
        .expect("delete request failed")
}

async fn activate_version(base: &str, model: &str, version: &str) -> reqwest::Response {
    let client = reqwest::Client::new();
    client
        .post(format!(
            "{}/v2/models/{}/versions/{}/activate",
            base, model, version
        ))
        .send()
        .await
        .expect("activate request failed")
}

/// True if any live process command line contains `needle`. Unix-only
/// (pgrep); returns false elsewhere, so callers must unix-gate assertions.
fn any_process_matches(needle: &str) -> bool {
    #[cfg(unix)]
    {
        let out = Command::new("pgrep").args(["-f", needle]).output();
        matches!(out, Ok(o) if !o.stdout.is_empty())
    }
    #[cfg(not(unix))]
    {
        let _ = needle;
        false
    }
}

/// G1: deleting a loaded version must stop the worker, remove the version
/// directory, and drop it from the registry (list_versions 404) and the
/// repository index. Re-uploading the same version afterwards must load
/// cleanly — proving no orphan worker still holds the ZMQ socket.
#[tokio::test]
async fn test_delete_loaded_version_stops_worker_and_removes_files() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-loaded-{}",
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
    let config_yaml: &[u8] = b"max_batch_size: 1\nbatch_timeout: 0.0\n";
    let resp = upload_files(&base, "mymodel", "1", true, &[("model.py", MODEL_PY.as_bytes()), ("config.yaml", config_yaml)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(status.is_success(), "upload failed: {}", body);
    assert!(wait_until_ready(&base, "mymodel", "1", 30).await, "model must be ready before delete");

    // The worker process carries the model.py path on its command line.
    let model_py_path = repo.join("mymodel").join("1").join("model.py");
    #[cfg(unix)]
    assert!(
        any_process_matches(model_py_path.to_str().unwrap()),
        "worker process must be running before delete"
    );

    // v1 is the only (hence active) version — E3 requires force.
    let resp = delete_version(&base, "mymodel", "1", true).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "delete failed: {}", body);

    // Version directory removed from disk.
    assert!(!repo.join("mymodel").join("1").exists(), "version dir must be deleted");

    // Registry no longer lists the version (404 once the model is empty).
    let client = reqwest::Client::new();
    let lv = client
        .get(format!("{}/v2/models/mymodel/versions", base))
        .send()
        .await
        .unwrap();
    assert_eq!(lv.status(), 404, "list_versions must 404 after the only version is deleted");

    // Repository index no longer lists it.
    let idx = client
        .post(format!("{}/v2/repository/index", base))
        .send()
        .await
        .unwrap();
    let idx_json: serde_json::Value = idx.json().await.unwrap();
    assert!(
        idx_json["models"].as_array().unwrap().is_empty(),
        "index must not list the deleted version: {idx_json}"
    );

    // Readiness drops to false (worker gone).
    let ready = client
        .get(format!("{}/v2/models/mymodel/versions/1/ready", base))
        .send()
        .await
        .unwrap();
    let ready_json: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(ready_json["ready"], false, "ready must be false after delete");

    // Worker process is gone (poll: graceful stop takes a moment).
    #[cfg(unix)]
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline
            && any_process_matches(model_py_path.to_str().unwrap())
        {
            sleep(Duration::from_millis(300)).await;
        }
        assert!(
            !any_process_matches(model_py_path.to_str().unwrap()),
            "worker process must be stopped after delete"
        );
    }

    // Re-upload + load the same version: must come back cleanly (an orphan
    // worker would have stolen the re-bound ZMQ socket and poisoned load).
    let resp = upload_files(&base, "mymodel", "1", true, &[("model.py", MODEL_PY.as_bytes()), ("config.yaml", config_yaml)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(status.is_success(), "re-upload failed: {}", body);
    assert!(wait_until_ready(&base, "mymodel", "1", 30).await, "re-uploaded version must become ready");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// G1/E3: deleting the active version with ?force=true must fall back to
/// another ready version (activation is a requirement the plan fixes in a
/// later step; force is accepted here because it just deletes today).
#[tokio::test]
async fn test_delete_active_version_with_force_falls_back_to_other_ready() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-active-{}",
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
    let config_yaml: &[u8] = b"max_batch_size: 1\nbatch_timeout: 0.0\n";
    for version in ["1", "2"] {
        let resp = upload_files(&base, "mymodel", version, true, &[("model.py", MODEL_PY.as_bytes()), ("config.yaml", config_yaml)]).await;
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert!(status.is_success(), "upload {} failed: {}", version, body);
        assert!(wait_until_ready(&base, "mymodel", version, 30).await, "version {} must be ready", version);
    }

    // Make v1 the active version, then delete it with force.
    let act = activate_version(&base, "mymodel", "1").await;
    let status = act.status();
    let body = act.text().await.unwrap();
    assert!(status.is_success(), "activate failed: {}", body);

    let resp = delete_version(&base, "mymodel", "1", true).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "forced delete of active version failed: {}", body);

    assert!(!repo.join("mymodel").join("1").exists(), "deleted version dir must be gone");
    assert!(repo.join("mymodel").join("2").exists(), "surviving version dir must stay");

    // Active pointer must fall back to the remaining ready version.
    let client = reqwest::Client::new();
    let ready = client
        .get(format!("{}/v2/models/mymodel/ready", base))
        .send()
        .await
        .unwrap();
    let ready_json: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(ready_json["ready"], true, "model must stay ready after fallback");
    assert_eq!(
        ready_json["active_version"], "2",
        "active version must fall back to the other ready version: {ready_json}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// E3: deleting the active version without ?force=true must be refused
/// with 409 (accident protection); ?force=true proceeds.
#[tokio::test]
async fn test_delete_active_version_requires_force() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-force-{}",
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
    let config_yaml: &[u8] = b"max_batch_size: 1\nbatch_timeout: 0.0\n";
    let resp = upload_files(&base, "mymodel", "1", true, &[("model.py", MODEL_PY.as_bytes()), ("config.yaml", config_yaml)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(status.is_success(), "upload failed: {}", body);
    assert!(wait_until_ready(&base, "mymodel", "1", 30).await, "model must be ready");
    let act = activate_version(&base, "mymodel", "1").await;
    let status = act.status();
    let body = act.text().await.unwrap();
    assert!(status.is_success(), "activate failed: {}", body);

    // Without force → 409 with an explanation; directory untouched.
    let resp = delete_version(&base, "mymodel", "1", false).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 409, "delete of active version without force must 409: {}", body);
    assert!(
        body.contains("force"),
        "409 body must explain the force override: {body}"
    );
    assert!(repo.join("mymodel").join("1").exists(), "409 must not delete anything");

    // With force → 200; directory gone.
    let resp = delete_version(&base, "mymodel", "1", true).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "forced delete failed: {}", body);
    assert!(!repo.join("mymodel").join("1").exists(), "forced delete must remove the dir");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// E2: batch retire with `{"keep": N}` deletes the lowest versions,
/// semver-lenient (10 > 2 > 1), keeping the N highest.
#[tokio::test]
async fn test_delete_versions_keep_preserves_latest() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-keep-{}",
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
    for version in ["1", "2", "10"] {
        let resp = upload_files(&base, "mymodel", version, false, &[("model.py", model_py)]).await;
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert!(status.is_success(), "upload {} failed: {}", version, body);
    }

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/v2/models/mymodel/versions", base))
        .json(&json!({"keep": 2}))
        .send()
        .await
        .expect("batch delete request failed");
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "batch delete failed: {}", body);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["deleted"], json!(["1"]), "keep=2 must delete only the lowest: {v}");
    assert!(v["failed"].as_array().unwrap().is_empty(), "no failures expected: {v}");

    assert!(!repo.join("mymodel").join("1").exists(), "lowest version must be gone");
    assert!(repo.join("mymodel").join("2").exists(), "v2 must be kept");
    assert!(repo.join("mymodel").join("10").exists(), "v10 must be kept");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// E2+E3: a batch containing the active version without force must report
/// it in `failed` (with the force explanation) while deleting the rest.
#[tokio::test]
async fn test_delete_versions_list_reports_partial_failure_for_active() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-batch-active-{}",
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
    let config_yaml: &[u8] = b"max_batch_size: 1\nbatch_timeout: 0.0\n";
    for version in ["1", "2"] {
        let resp = upload_files(&base, "mymodel", version, true, &[("model.py", MODEL_PY.as_bytes()), ("config.yaml", config_yaml)]).await;
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert!(status.is_success(), "upload {} failed: {}", version, body);
        assert!(wait_until_ready(&base, "mymodel", version, 30).await, "version {} must be ready", version);
    }
    let act = activate_version(&base, "mymodel", "1").await;
    let status = act.status();
    let body = act.text().await.unwrap();
    assert!(status.is_success(), "activate failed: {}", body);

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/v2/models/mymodel/versions", base))
        .json(&json!({"versions": ["1", "2"]}))
        .send()
        .await
        .expect("batch delete request failed");
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "partial failure must still be 200: {}", body);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["deleted"], json!(["2"]), "non-active version must be deleted: {v}");
    let failed = v["failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1, "active version must be reported in failed: {v}");
    assert_eq!(failed[0]["version"], "1");
    assert!(
        failed[0]["error"].as_str().unwrap().contains("force"),
        "failed entry must explain the force override: {v}"
    );
    assert!(repo.join("mymodel").join("1").exists(), "active version must remain on disk");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// E2: invalid batch requests are 400 (keep must be >= 1; each listed
/// version must pass validate_version; one of keep/versions is required).
#[tokio::test]
async fn test_delete_versions_invalid_request_is_400() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-batch-400-{}",
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
    let client = reqwest::Client::new();
    for body in [json!({"keep": 0}), json!({"versions": ["bad version!"]}), json!({})] {
        let resp = client
            .delete(format!("{}/v2/models/mymodel/versions", base))
            .json(&body)
            .send()
            .await
            .expect("batch delete request failed");
        assert_eq!(resp.status(), 400, "request {body} must be 400");
    }

    let _ = std::fs::remove_dir_all(&tmp);
}

/// E1: model-level delete — force required while an active version exists;
/// force removes the whole model directory and the linked artifacts
/// (.artifacts/<name>_v*.lma + root <name>_v*.lma).
#[tokio::test]
async fn test_delete_model_removes_directory_and_linked_artifacts() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-model-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    // v1 via .lma (F10a keeps it in .artifacts/), v2 raw.
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
    assert!(status.is_success(), "upload v1 failed: {}", body);
    assert!(wait_until_ready(&base, "mymodel", "1", 30).await, "v1 must be ready");
    let resp = upload_files(&base, "mymodel", "2", true, &[("model.py", MODEL_PY.as_bytes())]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(status.is_success(), "upload v2 failed: {}", body);
    assert!(wait_until_ready(&base, "mymodel", "2", 30).await, "v2 must be ready");

    let act = activate_version(&base, "mymodel", "1").await;
    let status = act.status();
    let body = act.text().await.unwrap();
    assert!(status.is_success(), "activate failed: {}", body);

    // Simulate an ops-placed artifact at the repo root.
    std::fs::write(repo.join("mymodel_v1.lma"), &lma_bytes).unwrap();
    assert!(repo.join(".artifacts").join("mymodel_v1.lma").exists(), "F10a must keep the artifact");

    // Without force → 409 (an active version exists).
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/v2/models/mymodel", base))
        .send()
        .await
        .expect("model delete request failed");
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 409, "model delete with active version must 409: {}", body);

    // With force → 200; everything gone.
    let resp = client
        .delete(format!("{}/v2/models/mymodel?force=true", base))
        .send()
        .await
        .expect("model delete request failed");
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "forced model delete failed: {}", body);

    assert!(!repo.join("mymodel").exists(), "model directory must be gone");
    assert!(
        !repo.join(".artifacts").join("mymodel_v1.lma").exists(),
        "linked .artifacts copy must be gone"
    );
    assert!(!repo.join("mymodel_v1.lma").exists(), "linked root .lma must be gone");

    let lv = client
        .get(format!("{}/v2/models/mymodel/versions", base))
        .send()
        .await
        .unwrap();
    assert_eq!(lv.status(), 404, "list_versions must 404 after model delete");

    let idx = client
        .post(format!("{}/v2/repository/index", base))
        .send()
        .await
        .unwrap();
    let idx_json: serde_json::Value = idx.json().await.unwrap();
    assert!(
        idx_json["models"].as_array().unwrap().is_empty(),
        "index must not list the deleted model: {idx_json}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// F7.3: full loop — upload .lma → download .lma → delete → re-upload the
/// downloaded artifact → files land without nesting and match the originals.
#[tokio::test]
async fn test_lma_upload_download_reupload_round_trip() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-lma-loop-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

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
    let resp = upload_files(&base, "mymodel", "1", false, &[("mymodel_v1.lma", &lma_bytes)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "first upload failed: {}", body);
    assert!(repo.join("mymodel").join("1").join("model.py").exists());
    assert!(!repo.join("mymodel").join("1").join("1").exists(), "no nesting after upload");

    // Download the whole version as .lma (repacked — F10b serves the
    // original only in a later step).
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
    let downloaded = dl.bytes().await.unwrap();

    // Delete the version, then re-upload the downloaded artifact.
    let resp = delete_version(&base, "mymodel", "1", false).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "delete failed: {}", body);

    let resp = upload_files(&base, "mymodel", "1", false, &[("mymodel_v1.lma", downloaded.as_ref())]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "re-upload of downloaded artifact failed: {}", body);

    // Still no nesting, and the content survived the full loop.
    assert!(repo.join("mymodel").join("1").join("model.py").exists());
    assert!(!repo.join("mymodel").join("1").join("1").exists(), "no nesting after re-upload");
    let re_uploaded = std::fs::read_to_string(repo.join("mymodel").join("1").join("model.py")).unwrap();
    assert_eq!(re_uploaded, MODEL_PY, "content must survive the round trip");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// F8: model-level upload — the version comes from the package manifest,
/// and ?load=true must bring the model up ready.
#[tokio::test]
async fn test_model_level_upload_lma_loads_model() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-ml-upload-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    let src = tmp.join("src").join("mymodel");
    let v2 = src.join("2");
    std::fs::create_dir_all(&v2).unwrap();
    std::fs::write(v2.join("model.py"), MODEL_PY).unwrap();
    std::fs::write(v2.join("config.yaml"), "max_batch_size: 1\nbatch_timeout: 0.0\n").unwrap();
    let lma = pack_lma(&src, "2", &tmp);
    let lma_bytes = std::fs::read(&lma).unwrap();

    let port = next_test_port();
    kill_stale_servers(port);
    let yaml = write_server_yaml(&tmp, port, &repo);
    let _server = start_server(&["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;

    let base = format!("http://127.0.0.1:{}", port);
    let boundary = "----ml-upload-boundary";
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/repository/models/mymodel/upload?load=true", base))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(multipart_body(boundary, &[("mymodel_v2.lma", &lma_bytes)]))
        .send()
        .await
        .expect("model-level upload failed");
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "model-level upload failed: {}", body);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["version"], "2", "version must come from the manifest");
    assert_eq!(json["loaded"], true, "load=true must report the real outcome");

    assert!(repo.join("mymodel").join("2").join("model.py").exists());
    assert!(!repo.join("mymodel").join("2").join("2").exists(), "no nesting");
    assert!(wait_until_ready(&base, "mymodel", "2", 30).await, "uploaded model must become ready");

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

/// E4: the drift endpoint exists and reports empty groups on a fresh repo
/// (read-only; detailed semantics covered by mod tests).
#[tokio::test]
async fn test_drift_endpoint_returns_empty_groups_on_fresh_repo() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-drift-{}",
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
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/v2/repository/drift", base))
        .send()
        .await
        .expect("drift request failed");
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["configured_missing"], json!([]), "{v}");
    assert_eq!(v["on_disk_unconfigured"], json!([]), "{v}");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// E1: deleting a model that never existed is an idempotent success.
#[tokio::test]
async fn test_delete_model_nonexistent_is_idempotent() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-model-idem-{}",
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
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/v2/models/ghost", base))
        .send()
        .await
        .expect("model delete request failed");
    assert_eq!(resp.status(), 200, "delete of nonexistent model must succeed");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// G1: deleting a version that never existed is an idempotent success.
#[tokio::test]
async fn test_delete_nonexistent_version_is_idempotent() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-idempotent-{}",
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
    for _ in 0..2 {
        let resp = delete_version(&base, "ghost", "9", false).await;
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert_eq!(status, 200, "delete of nonexistent version must succeed: {}", body);
    }

    let _ = std::fs::remove_dir_all(&tmp);
}

/// G1: in auto mode the watcher must not resurrect a version deleted via
/// the endpoint — the version directory is gone, so reconcile has nothing
/// to load.
#[tokio::test]
async fn test_delete_in_auto_mode_does_not_resurrect() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-auto-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    // Place the model on disk BEFORE the server starts so the watcher
    // loads it.
    let v1 = repo.join("mymodel").join("1");
    std::fs::create_dir_all(&v1).unwrap();
    std::fs::write(v1.join("model.py"), MODEL_PY).unwrap();
    std::fs::write(v1.join("config.yaml"), "max_batch_size: 1\nbatch_timeout: 0.0\n").unwrap();

    let port = next_test_port();
    kill_stale_servers(port);
    let yaml = write_server_yaml_mode(&tmp, port, &repo, "auto", 1);
    let _server = start_server(&["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;

    let base = format!("http://127.0.0.1:{}", port);
    assert!(wait_until_ready(&base, "mymodel", "1", 30).await, "watcher must load the model");

    // The watcher auto-activates the first loaded version — E3 needs force.
    let resp = delete_version(&base, "mymodel", "1", true).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "delete failed: {}", body);
    assert!(!repo.join("mymodel").join("1").exists(), "version dir must be deleted");

    // Wait several poll ticks (interval = 1s) — the watcher must not
    // resurrect the deleted version.
    sleep(Duration::from_secs(4)).await;
    let client = reqwest::Client::new();
    let ready = client
        .get(format!("{}/v2/models/mymodel/versions/1/ready", base))
        .send()
        .await
        .unwrap();
    let ready_json: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(ready_json["ready"], false, "watcher must not resurrect the deleted version");
    assert!(!repo.join("mymodel").join("1").exists(), "dir must not reappear");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// G5: deleting a version must remove the linked artifacts — the
/// scanner-placed root `.lma` and the F10a `.artifacts/` copy — so a
/// restart with a fresh `seen` set cannot resurrect the version via
/// auto-unpack.
#[tokio::test]
async fn test_delete_version_removes_linked_artifacts_and_survives_restart() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-repo-del-linked-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    // Pack a real model and upload the .lma (F10a keeps it in .artifacts/).
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
    let server_a = start_server(&["--config", yaml.to_str().unwrap()]);
    wait_for_server(port, 30).await;

    let base = format!("http://127.0.0.1:{}", port);
    let resp = upload_files(&base, "mymodel", "1", true, &[("mymodel_v1.lma", &lma_bytes)]).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(status.is_success(), "upload failed: {}", body);
    assert!(wait_until_ready(&base, "mymodel", "1", 30).await, "model must be ready");
    assert!(repo.join(".artifacts").join("mymodel_v1.lma").exists(), "F10a must keep the original artifact");

    // Simulate an ops-placed artifact at the repo root (explicit mode has
    // no reconcile task, so it stays put while this server runs).
    std::fs::write(repo.join("mymodel_v1.lma"), &lma_bytes).unwrap();

    // v1 is the only (hence active) version — E3 requires force.
    let resp = delete_version(&base, "mymodel", "1", true).await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "delete failed: {}", body);

    assert!(!repo.join("mymodel").join("1").exists(), "version dir must be deleted");
    // G5: linked cleanup — RED until implemented.
    assert!(
        !repo.join("mymodel_v1.lma").exists(),
        "root .lma must be deleted with the version (G5)"
    );
    assert!(
        !repo.join(".artifacts").join("mymodel_v1.lma").exists(),
        ".artifacts copy must be deleted with the version (G5)"
    );

    // Restart against the same repo in auto mode: with the linked
    // artifacts gone, the fresh `seen` set must find nothing to unpack —
    // no resurrection.
    drop(server_a);
    let port_b = next_test_port();
    kill_stale_servers(port_b);
    let yaml_b = write_server_yaml_mode(&tmp, port_b, &repo, "auto", 1);
    let _server_b = start_server(&["--config", yaml_b.to_str().unwrap()]);
    wait_for_server(port_b, 30).await;
    sleep(Duration::from_secs(3)).await;

    let base_b = format!("http://127.0.0.1:{}", port_b);
    let client = reqwest::Client::new();
    let ready = client
        .get(format!("{}/v2/models/mymodel/versions/1/ready", base_b))
        .send()
        .await
        .unwrap();
    let ready_json: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(ready_json["ready"], false, "deleted version must not resurrect after restart");
    assert!(!repo.join("mymodel").join("1").exists(), "version dir must not reappear after restart");

    let _ = std::fs::remove_dir_all(&tmp);
}
