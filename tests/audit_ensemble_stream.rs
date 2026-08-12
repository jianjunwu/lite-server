//! /audit 举证测试——ensemble 批次 0 末步流式(ensemble-streaming-plan
//! §4.1/§4.4、D1/D7/D16/D18/D21、m7、P0 缓存、P10 信号量)。
//! 真 server 子进程 + 模型夹具;端口段 19700(开工时核对全仓:在用段
//! 180xx-183xx / 18992 / 19000 / 19600,19700 段无冲突)。
//! 命名 test_audit_<维度>_<场景>;每测试独立 repo + server,防跨测试状态泄漏。

use serde_json::{json, Value};
use serial_test::serial;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

// ---------------------------------------------------------------------------
// Helpers(与 tests/audit_ensemble_grpc.rs 同构;test target 间不共享代码)
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

/// 单调端口分配(19700 起,避开 integration_test 的 19000 段与
/// audit_ensemble_grpc 的 19600 段)。
fn next_test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(19700);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn start_server(args: &[&str]) -> Child {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
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
    cmd.spawn().expect("Failed to start server")
}

fn stop_server(mut child: Child) {
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

struct ServerGuard(Option<Child>);

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

fn kill_stale_on_port(port: u16) {
    let output = Command::new("lsof")
        .args(["-ti", &format!(":{}", port)])
        .output();
    if let Ok(out) = output {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid_str in pids.lines() {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
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

/// POST an SSE endpoint and return the full body text (all `data:` lines).
async fn sse_post(base: &str, path: &str, body: Value) -> Result<String, reqwest::StatusCode> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}{}", base, path))
        .header("Content-Type", "application/json")
        .json(&body)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .expect("SSE request failed");
    let status = resp.status();
    let text = resp.text().await.expect("SSE body read failed");
    if status != 200 {
        return Err(status);
    }
    Ok(text)
}

// ---------------------------------------------------------------------------
// Fixtures(统一 repo,每测试独立目录)
// ---------------------------------------------------------------------------

fn write_model_py(repo: &std::path::Path, name: &str, py: &str) {
    let dir = repo.join(name).join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("model.py"), py).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

fn write_ensemble(repo: &std::path::Path, name: &str, yaml: &str) {
    let dir = repo.join(name).join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("config.yaml"), yaml).unwrap();
}

/// Unary pre-layer: {"pre": <text>} (dict output — safe worker encoding).
fn write_pre(repo: &std::path::Path) {
    write_model_py(
        repo,
        "pre",
        r#"from lite_server import LitAPI


class PreAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        return {"pre": x}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Real streaming tail: one chunk per word of the pre output.
fn write_tail(repo: &std::path::Path, name: &str, suffix: &str) {
    let py = format!(
        r#"import time
from lite_server import LitAPI


class TailAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {{"tokens": x.split()}}

    def stream_predict(self, request, ctx=None):
        for w in request.split():
            time.sleep(0.02)
            yield {{"token": w + "{suffix}"}}

    async def encode_response(self, output, ctx=None):
        return output
"#
    );
    write_model_py(repo, name, &py);
}

/// Slow streaming tail (0.5s per chunk) — keeps the stream open for the P10
/// capacity test.
fn write_tail_slow(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_slow",
        r#"import time
from lite_server import LitAPI


class TailSlowAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        for w in request.split():
            time.sleep(0.5)
            yield {"token": w}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Mid-stream failure: raises after the 3rd chunk → worker Error frame.
fn write_tail_fail(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_fail",
        r#"import time
from lite_server import LitAPI


class TailFailAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        for i, w in enumerate(request.split()):
            time.sleep(0.02)
            if i == 2:
                raise RuntimeError("mid-stream boom")
            yield {"token": w}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Binary streaming tail: yields raw non-UTF-8 bytes (m7 type-mismatch path).
fn write_tail_binary(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_binary",
        r#"from lite_server import LitAPI


class TailBinaryAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        yield b"\xff\xfe\x00binary-chunk"

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Unary step raising a client 4xx (B3 passthrough).
fn write_pre_bad(repo: &std::path::Path) {
    write_model_py(
        repo,
        "pre_bad",
        r#"from lite_server.exceptions import BadRequestError
from lite_server import LitAPI


class PreBadAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        raise BadRequestError("bad input from sub-model")

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Unary step raising a server 5xx.
fn write_pre_5xx(repo: &std::path::Path) {
    write_model_py(
        repo,
        "pre_5xx",
        r#"from lite_server.exceptions import InternalServerError
from lite_server import LitAPI


class Pre5xxAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        raise InternalServerError("boom from sub-model")

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Corrupt-config sub-model: exists but load_model fails → ModelNotReady 503.
fn write_missing_model(repo: &std::path::Path) {
    let dir = repo.join("ghost").join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("model.py"), "not a real model\n").unwrap();
    std::fs::write(dir.join("config.yaml"), "not: [valid yaml\n").unwrap();
}

/// echo unary tail (ens_unary: full-unary DAG hit via a streaming endpoint).
fn write_echo(repo: &std::path::Path) {
    write_model_py(
        repo,
        "echo",
        r#"from lite_server import LitAPI


class EchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("data", "")

    async def predict(self, x, ctx=None):
        return {"echo": x}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

const ENS_STREAM_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

const ENS_BINARY_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_binary
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

const ENS_UNARY_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: echo
      model: echo
      version: "1"
      inputs:
        data: "$pre.pre"
"#;

/// Pipeline form: the streaming step's output is consumed downstream →
/// rejected at load time (D16 batch-0 boundary), never silently accepted.
const ENS_PIPELINE_YAML: &str = r#"ensemble:
  steps:
    - name: s1
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$request.text"
    - name: s2
      model: pre
      version: "1"
      inputs:
        text: "$s1"
"#;

/// Streaming tail whose sub-model cannot load (corrupt config) → 503.
const ENS_BAD_SUB_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: ghost
      model: ghost
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

const ENS_4XX_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre_bad
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

const ENS_5XX_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre_5xx
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

const ENS_FAIL_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_fail
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

const ENS_SLOW_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_slow
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

fn write_all_fixtures(repo: &std::path::Path) {
    write_pre(repo);
    write_tail(repo, "tail", "");
    write_tail(repo, "tail_v2", "_v2");
    write_tail_slow(repo);
    write_tail_fail(repo);
    write_tail_binary(repo);
    write_pre_bad(repo);
    write_pre_5xx(repo);
    write_missing_model(repo);
    write_echo(repo);
    write_ensemble(repo, "ens_stream", ENS_STREAM_YAML);
    write_ensemble(repo, "ens_binary", ENS_BINARY_YAML);
    write_ensemble(repo, "ens_unary", ENS_UNARY_YAML);
    write_ensemble(repo, "ens_pipeline", ENS_PIPELINE_YAML);
    write_ensemble(repo, "ens_bad_sub", ENS_BAD_SUB_YAML);
    write_ensemble(repo, "ens_4xx", ENS_4XX_YAML);
    write_ensemble(repo, "ens_5xx", ENS_5XX_YAML);
    write_ensemble(repo, "ens_fail", ENS_FAIL_YAML);
    write_ensemble(repo, "ens_slow", ENS_SLOW_YAML);
}

fn write_server_yaml(repo: &std::path::Path, http_port: u16, extra: &str) -> std::path::PathBuf {
    // Port-suffixed dir: tests run concurrently and must not share files.
    let dir = std::env::temp_dir().join(format!(
        "lite-server-ens-stream-yaml-{}-{}",
        std::process::id(),
        http_port
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("server.yaml");
    std::fs::write(
        &path,
        format!(
            "server:\n  http_port: {http_port}\n  timeout: 30.0\n{extra}\n\n\
             model_repository:\n  path: {}\n\n\
             orchestration:\n  control_mode: explicit\n  load_models:\n    - pre\n    - tail\n    - tail_v2\n    - tail_slow\n    - tail_fail\n    - tail_binary\n    - pre_bad\n    - pre_5xx\n    - ghost\n    - echo\n    - ens_stream\n    - ens_binary\n    - ens_unary\n    - ens_pipeline\n    - ens_bad_sub\n    - ens_4xx\n    - ens_5xx\n    - ens_fail\n    - ens_slow\n",
            repo.display()
        ),
    )
    .unwrap();
    path
}

/// Boot a server with the fixture repo; returns (base, guard, repo).
async fn boot_server(extra: &str) -> (String, ServerGuard, std::path::PathBuf) {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    // Port-suffixed repo: tests run concurrently and must not share files.
    let repo = std::env::temp_dir()
        .join(format!("lite-server-ens-stream-{}-{}", std::process::id(), http_port));
    let _ = std::fs::remove_dir_all(&repo);
    write_all_fixtures(&repo);
    let server_yaml = write_server_yaml(&repo, http_port, extra);
    let guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    (
        format!("http://127.0.0.1:{}", http_port),
        guard,
        repo,
    )
}

/// Wait for all named models ready (ensemble models are loadable instantly).
async fn wait_ready_all(base: &str, models: &[&str]) {
    for m in models {
        assert!(
            wait_model_ready(base, m, 30).await,
            "model {} did not become ready",
            m
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// SSE happy path: unary pre-layer + streaming tail → per-chunk tokens + [DONE].
#[serial]
#[tokio::test]
async fn test_audit_stream_ensemble_sse_happy() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;

    let body = sse_post(&base, "/v2/models/ens_stream/events", json!({"text": "hello world"}))
        .await
        .expect("SSE happy path must return 200");
    assert!(body.contains(r#""token":"hello""#), "missing first chunk: {body}");
    assert!(body.contains(r#""token":"world""#), "missing second chunk: {body}");
    assert!(body.contains("[DONE]"), "missing [DONE] terminator: {body}");
    assert!(!body.contains("error"), "no error expected: {body}");
}

/// A full-unary DAG hit via a streaming endpoint → 400 (§4.4 unsupported
/// combination — the endpoint would otherwise report "has no workers").
#[serial]
#[tokio::test]
async fn test_audit_stream_unary_dag_on_stream_endpoint_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_unary"]).await;

    let err = sse_post(&base, "/v2/models/ens_unary/events", json!({"text": "x"}))
        .await
        .expect_err("unary DAG on a streaming endpoint must NOT be 200");
    assert_eq!(err, reqwest::StatusCode::BAD_REQUEST, "must be 400");
}

/// D1: a streaming DAG hit via the unary endpoint → 400 with an explicit
/// message (aggregating chunks would fake unary semantics).
#[serial]
#[tokio::test]
async fn test_audit_stream_unary_endpoint_on_stream_dag_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_stream/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "D1 must be 400");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("streaming step"),
        "D1 error must name the streaming step: {body}"
    );
}

/// D16: pipeline form (streaming output consumed downstream) is NOT silently
/// accepted in batch 0 — the config fails validation, the model never
/// becomes ready, and requests see not-ready (never a fake 200 stream).
#[serial]
#[tokio::test]
async fn test_audit_stream_pipeline_form_rejected_batch0() {
    let (base, _guard, _repo) = boot_server("").await;
    // ens_pipeline must NOT be ready (config validation failed at load).
    assert!(
        !wait_model_ready(&base, "ens_pipeline", 5).await,
        "pipeline form must be rejected at load time in batch 0 (D16)"
    );
    let err = sse_post(&base, "/v2/models/ens_pipeline/events", json!({"text": "x"}))
        .await
        .expect_err("rejected pipeline DAG must not stream");
    assert_ne!(
        err,
        reqwest::StatusCode::OK,
        "rejected pipeline DAG must not produce a 200 stream"
    );
}

/// D7: SSE + binary_data_output request flag → 400 (SSE is a text channel).
#[serial]
#[tokio::test]
async fn test_audit_stream_binary_output_flag_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_stream/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({
            "inputs": [{"name": "text", "data": "x"}],
            "parameters": {"binary_data_output": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "D7 must be 400");
}

/// m7: a binary chunk from the tail worker (model flag unset) on a text
/// endpoint → Error frame + close, terminal reason type_mismatch (the stream
/// is already open; no status-code change is possible).
#[serial]
#[tokio::test]
async fn test_audit_stream_binary_chunk_type_mismatch() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_binary"]).await;

    let body = sse_post(&base, "/v2/models/ens_binary/events", json!({"text": "x"}))
        .await
        .expect("binary-chunk stream must open (200)");
    assert!(
        body.contains("error") && !body.contains("[DONE]"),
        "binary chunk on SSE must close with an Error frame, no [DONE]: {body}"
    );
}

/// §4.4 autoload-failure row: sub-model with a corrupt config → 503.
#[serial]
#[tokio::test]
async fn test_audit_stream_autoload_failure_503() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_bad_sub"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_bad_sub/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "autoload failure must be 503 (§4.4)"
    );
}

/// B3 passthrough: a sub-model 4xx reaches the client as 400 (not wrapped 500).
#[serial]
#[tokio::test]
async fn test_audit_stream_step_4xx_passthrough() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_4xx"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_4xx/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "sub-model 4xx must pass through (B3)"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("bad input from sub-model"),
        "4xx detail must be preserved: {body}"
    );
}

/// B3 passthrough: a sub-model 5xx → 500.
#[serial]
#[tokio::test]
async fn test_audit_stream_step_5xx_500() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_5xx"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_5xx/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "sub-model 5xx must be 500 (§4.4)"
    );
}

/// Mid-stream worker failure → Error frame + close (no fake [DONE]).
#[serial]
#[tokio::test]
async fn test_audit_stream_midstream_failure_error_frame() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_fail"]).await;

    let body = sse_post(&base, "/v2/models/ens_fail/events", json!({"text": "a b c d"}))
        .await
        .expect("stream must open (200)");
    assert!(body.contains(r#""token":"a""#), "first chunks must stream: {body}");
    assert!(
        body.contains("error") && !body.contains("[DONE]"),
        "mid-stream failure must close with an Error frame, no [DONE]: {body}"
    );
}

/// P10: max_concurrent_streaming_dags = 1 — the second concurrent streaming
/// DAG is rejected with 429 (immediate, no queueing); the first completes.
#[tokio::test]
#[serial]
async fn test_audit_stream_p10_capacity_429() {
    let (base, _guard, _repo) = boot_server("  max_concurrent_streaming_dags: 1").await;
    wait_ready_all(&base, &["ens_slow"]).await;

    let client = reqwest::Client::new();
    // First stream holds the only slot (0.5s/chunk).
    let first = client
        .post(format!("{}/v2/models/ens_slow/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "one two"}))
        .send()
        .await
        .expect("first stream must open");
    assert_eq!(first.status(), reqwest::StatusCode::OK, "first stream opens");

    // Give the first stream time to acquire the semaphore slot.
    sleep(Duration::from_millis(700)).await;

    // Second concurrent streaming DAG → 429 (StreamingCapacityExceeded).
    let second = client
        .post(format!("{}/v2/models/ens_slow/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "three"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        second.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "P10 exhausted capacity must reject with 429"
    );
    let body = second.text().await.unwrap();
    assert!(
        body.contains("streaming"),
        "429 body must name streaming capacity: {body}"
    );

    // First stream still completes with its chunks.
    let first_body = first.text().await.expect("first stream body");
    assert!(
        first_body.contains(r#""token":"one""#) && first_body.contains("[DONE]"),
        "first stream must complete: {first_body}"
    );
}

/// P0: config edit → reload → the next request uses the NEW plan
/// (validation: reload invalidates the plan cache, D23).
#[tokio::test]
#[serial]
async fn test_audit_stream_p0_reload_uses_new_plan() {
    let (base, _guard, repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;

    // Baseline: tail (no suffix).
    let before = sse_post(&base, "/v2/models/ens_stream/events", json!({"text": "hello world"}))
        .await
        .expect("baseline stream must open");
    assert!(
        before.contains(r#""token":"hello""#) && !before.contains("_v2"),
        "baseline must use tail (no suffix): {before}"
    );

    // Point the streaming step at tail_v2 (different chunk content) and
    // reload through the admin API (validate-then-swap reads disk).
    let ens_dir = repo.join("ens_stream").join("1");
    std::fs::write(
        ens_dir.join("config.yaml"),
        ENS_STREAM_YAML.replace("model: tail\n", "model: tail_v2\n"),
    )
    .unwrap();
    let client = reqwest::Client::new();
    let reload = client
        .post(format!("{}/v2/models/ens_stream/reload", base))
        .send()
        .await
        .expect("reload request");
    assert_eq!(reload.status(), reqwest::StatusCode::OK, "reload must succeed");
    assert!(wait_model_ready(&base, "ens_stream", 30).await, "ens_stream ready after reload");

    // New request must use the NEW plan (tail_v2 chunks).
    let after = sse_post(&base, "/v2/models/ens_stream/events", json!({"text": "hello world"}))
        .await
        .expect("post-reload stream must open");
    assert!(
        after.contains(r#""token":"hello_v2""#),
        "post-reload request must use the new plan (tail_v2): {after}"
    );
}
