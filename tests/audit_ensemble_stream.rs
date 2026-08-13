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

/// E6 (batch 4): flaky unary step — 500 on the FIRST call only, then
/// succeeds (module counter; one worker per process keeps it deterministic).
fn write_pre_flaky(repo: &std::path::Path) {
    write_model_py(
        repo,
        "pre_flaky",
        r#"from lite_server.exceptions import InternalServerError
from lite_server import LitAPI


_ATTEMPTS = 0


class PreFlakyAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        global _ATTEMPTS
        _ATTEMPTS += 1
        if _ATTEMPTS == 1:
            raise InternalServerError("flaky first call")
        return {"pre": x}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// E6 (batch 4): flaky streaming tail — the FIRST stream raises (first-frame
/// Error), the second streams normally (build-window retry fixture, D35).
fn write_tail_flaky(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_flaky",
        r#"import time
from lite_server import LitAPI


_ATTEMPTS = 0


class TailFlakyAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        global _ATTEMPTS
        _ATTEMPTS += 1
        if _ATTEMPTS == 1:
            raise RuntimeError("first-frame boom")
        for w in request.split():
            time.sleep(0.02)
            yield {"token": w}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// MIMO (batch 4①, D8): vision encoder — binary in, JSON out carrying a
/// `$binary_b64` marker field (the step.outputs binary-alias source).
fn write_vis_enc(repo: &std::path::Path) {
    write_model_py(
        repo,
        "vis_enc",
        r#"import base64
from lite_server import LitAPI


class VisEncAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request  # raw bytes (binary passthrough)

    async def predict(self, x, ctx=None):
        return {
            "thumb": {"$binary_b64": base64.b64encode(x).decode(), "content_type": "image/jpeg"},
            "emb": {"v": 1.0},
        }

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// MIMO (D8): cropper — binary in, binary marker out (chain hop 2).
fn write_cropper(repo: &std::path::Path) {
    write_model_py(
        repo,
        "cropper",
        r#"import base64
from lite_server import LitAPI


class CropperAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request  # raw bytes

    async def predict(self, x, ctx=None):
        return {"crop": {"$binary_b64": base64.b64encode(x).decode(), "content_type": "image/png"}}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// MIMO (D8): classifier — binary in, plain JSON out (chain tail).
fn write_classifier(repo: &std::path::Path) {
    write_model_py(
        repo,
        "classifier",
        r#"from lite_server import LitAPI


class ClassifierAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request  # raw bytes

    async def predict(self, x, ctx=None):
        return {"label": "cat", "bytes_in": len(x)}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// E6 (batch 4): streaming tail that ALWAYS raises on the first frame
/// (build-window retry exhaustion fixture, D35).
fn write_tail_fail_always(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_fail_always",
        r#"from lite_server import LitAPI


class TailFailAlwaysAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        raise RuntimeError("first-frame boom")

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

/// Array-aware pre-layer: joins aggregated JSON arrays back into a string
/// (batch-1 bidi aggregation tests send multi-frame arrays).
fn write_pre_agg(repo: &std::path::Path) {
    write_model_py(
        repo,
        "pre_agg",
        r#"from lite_server import LitAPI


class PreAggAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        t = request.get("data", request)
        if isinstance(t, list):
            parts = []
            for x in t:
                if isinstance(x, dict):
                    parts.append(str(x.get("text", x)))
                else:
                    parts.append(str(x))
            return " ".join(parts)
        if isinstance(t, dict):
            return t.get("text", "")
        return t

    async def predict(self, x, ctx=None):
        return {"pre": x}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Decoupled-capable slow streaming tail (0.5s/chunk via the decoupled
/// sender contract) — ensemble decoupled cancel tests need a worker that
/// implements predict_decoupled (stream_predict-only workers report
/// not_implemented on a decoupled open).
fn write_tail_dslow(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_dslow",
        r#"import asyncio
from lite_server import LitAPI


class TailDecoupledAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    async def predict_decoupled(self, data, sender, ctx=None):
        for w in data.split():
            await asyncio.sleep(0.5)
            await sender.send({"token": w})
        await sender.close()

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Split-UTF-8 streaming tail: a single multi-byte codepoint ("你" = E4 BD A0)
/// split across two chunk yields — byte-oriented chunking is legal for a text
/// stream (the direct SSE path tolerates it via from_utf8_lossy).
fn write_tail_split(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_split",
        r#"from lite_server import LitAPI


class TailSplitAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": [x]}

    def stream_predict(self, request, ctx=None):
        b = "你".encode("utf-8")
        yield b[:1]
        yield b[1:]

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
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

// ===== E6 (batch 4) fixtures =====

/// Skip step (fails 5xx) + independent sibling — the DAG must still produce
/// the sibling's output (the skip is parse-guaranteed unreferenced, D5).
const ENS_SKIP_YAML: &str = r#"ensemble:
  steps:
    - name: may_skip
      model: pre_5xx
      version: "1"
      on_error: skip
      inputs:
        text: "$request.text"
    - name: main
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
"#;

/// Retry recovery: flaky unary step (500 on first call) with retries: 1.
const ENS_RETRY_YAML: &str = r#"ensemble:
  steps:
    - name: flaky
      model: pre_flaky
      version: "1"
      retries: 1
      inputs:
        text: "$request.text"
"#;

/// Retry default-off pin: same flaky model with retries: 0 must fail 500.
const ENS_RETRY_OFF_YAML: &str = r#"ensemble:
  steps:
    - name: flaky
      model: pre_flaky
      version: "1"
      inputs:
        text: "$request.text"
"#;

/// Retry exhaustion: always-500 model with retries: 2 → 500 after 3 attempts.
const ENS_RETRY_EXHAUST_YAML: &str = r#"ensemble:
  steps:
    - name: broken
      model: pre_5xx
      version: "1"
      retries: 2
      inputs:
        text: "$request.text"
"#;

/// Streaming build-window retry recovery (D35): the tail's first stream
/// opens with a first-frame Error; retries rebuild it and chunks stream.
const ENS_STREAM_RETRY_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_flaky
      version: "1"
      stream: true
      retries: 2
      inputs:
        pre: "$pre.pre"
"#;

/// Streaming build-window retry exhaustion: the tail always fails on the
/// first frame → Error frame + close (no [DONE]).
const ENS_STREAM_RETRY_EXHAUST_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_fail_always
      version: "1"
      stream: true
      retries: 1
      inputs:
        pre: "$pre.pre"
"#;

/// Committed-stream pin (D35): a mid-stream failure (3rd chunk) with
/// retries: 2 must NOT replay — chunks before the Error frame appear once.
const ENS_STREAM_COMMITTED_YAML: &str = r#"ensemble:
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
      retries: 2
      inputs:
        pre: "$pre.pre"
"#;

// ===== MIMO (batch 4①) fixtures =====

/// Declared multi-input DAG: text + default-carrying sys.
const ENS_MIMO_YAML: &str = r#"ensemble:
  inputs:
    text:
      type: json
    sys:
      type: json
      required: false
      default: "be terse"
  steps:
    - name: tok
      model: pre
      version: "1"
      inputs:
        text: "$inputs.text"
    - name: out
      model: echo
      version: "1"
      inputs:
        data: "$tok.pre"
"#;

/// D8 binary passthrough chain: image → vis_enc (marker output) → cropper
/// (marker output) → classifier (plain JSON tail). Each binary hop is a
/// declared alias consumed whole as the step's sole input.
const ENS_MIMO_BIN_YAML: &str = r#"ensemble:
  inputs:
    image:
      type: binary
      content_type: image/png
  steps:
    - name: enc
      model: vis_enc
      version: "1"
      outputs:
        thumb:
          type: binary
          path: "$.thumb"
      inputs:
        img: "$inputs.image"
    - name: crop
      model: cropper
      version: "1"
      outputs:
        crop:
          type: binary
          path: "$.crop"
      inputs:
        img: "$enc.thumb"
    - name: cls
      model: classifier
      version: "1"
      inputs:
        img: "$crop.crop"
"#;

/// MIMO② (D10): json alias projection — pre's `{"pre": ...}` response
/// projects through the declared alias (default path `$.pre`).
const ENS_MIMO_JSON_ALIAS_YAML: &str = r#"ensemble:
  inputs:
    text:
      type: json
  steps:
    - name: tok
      model: pre
      version: "1"
      outputs:
        pre:
          type: json
      inputs:
        text: "$inputs.text"
    - name: out
      model: echo
      version: "1"
      inputs:
        data: "$tok.pre"
"#;

/// E7 (D31/D5): multi-sink — json alias + binary alias (marker output) +
/// skip alias (pre_5xx with on_error: skip → null).
const ENS_MULTISINK_YAML: &str = r#"ensemble:
  inputs:
    text:
      type: json
    image:
      type: binary
  outputs:
    answer: "$tok"
    thumb: "$enc.crop"
    score: "$may"
  steps:
    - name: tok
      model: pre
      version: "1"
      inputs:
        text: "$inputs.text"
    - name: enc
      model: vis_enc
      version: "1"
      outputs:
        crop:
          type: binary
          path: "$.thumb"
      inputs:
        img: "$inputs.image"
    - name: may
      model: pre_5xx
      version: "1"
      on_error: skip
      inputs:
        text: "$inputs.text"
"#;

/// E8-1: named DAG sets — default runs `pre`, fast runs `echo` (distinct
/// outputs make the selection observable).
const ENS_DAGS_YAML: &str = r#"ensemble:
  dags:
    default:
      steps:
        - name: main
          model: pre
          version: "1"
          inputs:
            text: "$request.text"
    fast:
      steps:
        - name: main
          model: echo
          version: "1"
          inputs:
            data: "$request.text"
"#;

/// E8-2: when conditions — `dag_path` runs only when the DEFAULT set is
/// selected ($request.dag mirrors the selected name), `maybe` runs only
/// when the optional input is present (pre_5xx → observable 500), and its
/// outputs alias is null when skipped (D5). The fast set answers with pre
/// (distinct output).
const ENS_WHEN_YAML: &str = r#"ensemble:
  dags:
    default:
      inputs:
        mode:
          type: json
        opt:
          type: json
          required: false
      outputs:
        answer: "$main"
        maybe_score: "$maybe"
      steps:
        - name: dag_path
          model: pre
          version: "1"
          when: "$request.dag == 'default'"
          inputs:
            text: "$inputs.mode"
        - name: maybe
          model: pre_5xx
          version: "1"
          when: "$inputs.opt != null"
          inputs:
            text: "$inputs.mode"
        - name: main
          model: echo
          version: "1"
          inputs:
            data: "$inputs.mode"
    fast:
      inputs:
        mode:
          type: json
      steps:
        - name: main
          model: pre
          version: "1"
          inputs:
            text: "$inputs.mode"
"#;

/// R4 conditional skip: the optional input absent → `cond` (which would 500
/// if it ran) is skipped, `main` still produces the output.
const ENS_MIMO_COND_YAML: &str = r#"ensemble:
  inputs:
    text:
      type: json
    opt:
      type: json
      required: false
  steps:
    - name: cond
      model: pre_5xx
      version: "1"
      inputs:
        text: "$inputs.opt"
    - name: main
      model: pre
      version: "1"
      inputs:
        text: "$inputs.text"
"#;

/// Declared streaming DAG — the D33 bidi fixture (envelope frame triggers
/// immediately, no close frame).
const ENS_MIMO_STREAM_YAML: &str = r#"ensemble:
  inputs:
    text:
      type: json
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$inputs.text"
    - name: tail
      model: tail
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

/// Array-aware DAG for bidi multi-frame aggregation tests (batch 1).
const ENS_AGG_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre_agg
      version: "1"
      inputs:
        data: "$request"
    - name: tail
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

/// Decoupled slow DAG (WS decoupled cancel contract tests).
const ENS_DSLOW_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_dslow
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

/// Split-UTF-8 DAG (m7 boundary: byte-split text chunks must not kill the stream).
const ENS_SPLIT_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_split
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
    write_tail_dslow(repo);
    write_tail_fail(repo);
    write_tail_binary(repo);
    write_tail_split(repo);
    write_pre_bad(repo);
    write_pre_5xx(repo);
    write_missing_model(repo);
    write_echo(repo);
    write_pre_agg(repo);
    write_tail_upper(repo);
    write_tail_fail_chain(repo);
    write_chain_slow_head(repo);
    write_chain_head_fail(repo);
    write_ensemble(repo, "ens_stream", ENS_STREAM_YAML);
    write_ensemble(repo, "ens_agg", ENS_AGG_YAML);
    write_ensemble(repo, "ens_chain", ENS_CHAIN_YAML);
    write_ensemble(repo, "ens_chain_fail", ENS_CHAIN_FAIL_YAML);
    write_ensemble(repo, "ens_chain_slow", ENS_CHAIN_SLOW_YAML);
    write_ensemble(repo, "ens_chain_head_fail", ENS_CHAIN_HEAD_FAIL_YAML);
    write_ensemble(repo, "ens_chain_sibling", ENS_CHAIN_SIBLING_YAML);
    write_ensemble(repo, "ens_split", ENS_SPLIT_YAML);
    write_ensemble(repo, "ens_dslow", ENS_DSLOW_YAML);
    write_ensemble(repo, "ens_binary", ENS_BINARY_YAML);
    write_ensemble(repo, "ens_unary", ENS_UNARY_YAML);
    write_ensemble(repo, "ens_pipeline", ENS_PIPELINE_YAML);
    write_ensemble(repo, "ens_bad_sub", ENS_BAD_SUB_YAML);
    write_ensemble(repo, "ens_4xx", ENS_4XX_YAML);
    write_ensemble(repo, "ens_5xx", ENS_5XX_YAML);
    write_ensemble(repo, "ens_fail", ENS_FAIL_YAML);
    write_ensemble(repo, "ens_slow", ENS_SLOW_YAML);
    // ===== batch 3 (E1/E2/E3/E4/D35) fixtures =====
    write_reflect(repo);
    write_drift(repo);
    write_tail(repo, "tail_late", "");
    write_ensemble(repo, "ens_nested_child", ENS_NESTED_CHILD_YAML);
    write_ensemble(repo, "ens_nested_parent", ENS_NESTED_PARENT_YAML);
    write_ensemble(repo, "ens_self", ENS_SELF_YAML);
    write_ensemble(repo, "ens_mut_a", ENS_MUT_A_YAML);
    write_ensemble(repo, "ens_mut_b", ENS_MUT_B_YAML);
    write_ensemble(repo, "ens_child_stream", ENS_CHILD_STREAM_YAML);
    write_ensemble(repo, "ens_out_mid", ENS_OUT_MID_YAML);
    write_ensemble(repo, "ens_out_field", ENS_OUT_FIELD_YAML);
    write_ensemble(repo, "ens_out_missing", ENS_OUT_MISSING_YAML);
    write_ensemble(repo, "ens_params", ENS_PARAMS_YAML);
    write_ensemble(repo, "ens_drift", ENS_DRIFT_YAML);
    write_ensemble(repo, "ens_drift_child", ENS_DRIFT_CHILD_YAML);
    write_ensemble(repo, "ens_drift_parent", ENS_DRIFT_PARENT_YAML);
    write_ensemble(repo, "ens_e5_stream", ENS_E5_STREAM_YAML);
    write_ensemble(repo, "ens_e5_autoload", ENS_E5_AUTOLOAD_YAML);
    // ===== audit (batch 3) defect-reproduction fixtures =====
    write_ensemble(repo, "ens_sibling_race", ENS_SIBLING_RACE_YAML);
    write_ensemble(repo, "ens_e5_slow_child", ENS_E5_SLOW_CHILD_YAML);
    write_ensemble(repo, "ens_e5_nested_timeout", ENS_E5_NESTED_TIMEOUT_YAML);
    // ===== batch 4 (E6) fixtures =====
    write_pre_flaky(repo);
    write_tail_flaky(repo);
    write_tail_fail_always(repo);
    write_ensemble(repo, "ens_skip", ENS_SKIP_YAML);
    write_ensemble(repo, "ens_retry", ENS_RETRY_YAML);
    write_ensemble(repo, "ens_retry_off", ENS_RETRY_OFF_YAML);
    write_ensemble(repo, "ens_retry_exhaust", ENS_RETRY_EXHAUST_YAML);
    write_ensemble(repo, "ens_stream_retry", ENS_STREAM_RETRY_YAML);
    write_ensemble(repo, "ens_stream_retry_exhaust", ENS_STREAM_RETRY_EXHAUST_YAML);
    write_ensemble(repo, "ens_stream_committed", ENS_STREAM_COMMITTED_YAML);
    // ===== batch 4 (MIMO①) fixtures =====
    write_vis_enc(repo);
    write_cropper(repo);
    write_classifier(repo);
    write_ensemble(repo, "ens_mimo", ENS_MIMO_YAML);
    write_ensemble(repo, "ens_mimo_json_alias", ENS_MIMO_JSON_ALIAS_YAML);
    write_ensemble(repo, "ens_multisink", ENS_MULTISINK_YAML);
    write_ensemble(repo, "ens_dags", ENS_DAGS_YAML);
    write_ensemble(repo, "ens_when", ENS_WHEN_YAML);
    write_ensemble(repo, "ens_mimo_bin", ENS_MIMO_BIN_YAML);
    write_ensemble(repo, "ens_mimo_cond", ENS_MIMO_COND_YAML);
    write_ensemble(repo, "ens_mimo_stream", ENS_MIMO_STREAM_YAML);
}

fn write_server_yaml(repo: &std::path::Path, http_port: u16, extra: &str, orch_extra: &str) -> std::path::PathBuf {
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
             orchestration:\n  control_mode: explicit\n  load_models:\n    - pre\n    - pre_agg\n    - tail\n    - tail_upper\n    - tail_fail_chain\n    - tail_v2\n    - tail_slow\n    - tail_fail\n    - tail_binary\n    - tail_split\n    - tail_dslow\n    - pre_bad\n    - pre_5xx\n    - ghost\n    - echo\n    - ens_stream\n    - ens_agg\n    - ens_chain\n    - ens_chain_fail\n    - chain_slow_head\n    - chain_head_fail\n    - ens_chain_slow\n    - ens_chain_head_fail\n    - ens_chain_sibling\n    - ens_split\n    - ens_dslow\n    - ens_binary\n    - ens_unary\n    - ens_pipeline\n    - ens_bad_sub\n    - ens_4xx\n    - ens_5xx\n    - ens_fail\n    - ens_slow\n    - ens_nested_child\n    - ens_nested_parent\n    - ens_self\n    - ens_mut_a\n    - ens_mut_b\n    - ens_child_stream\n    - ens_out_mid\n    - ens_out_field\n    - ens_out_missing\n    - ens_params\n    - drift\n    - ens_drift\n    - ens_drift_child\n    - ens_drift_parent\n    - ens_e5_stream\n    - ens_e5_autoload\n    - ens_sibling_race\n    - ens_e5_slow_child\n    - ens_e5_nested_timeout\n    - pre_flaky\n    - tail_flaky\n    - tail_fail_always\n    - ens_skip\n    - ens_retry\n    - ens_retry_off\n    - ens_retry_exhaust\n    - ens_stream_retry\n    - ens_stream_retry_exhaust\n    - ens_stream_committed\n    - vis_enc\n    - cropper\n    - classifier\n    - ens_mimo\n    - ens_mimo_json_alias\n    - ens_multisink\n    - ens_dags\n    - ens_when\n    - ens_mimo_bin\n    - ens_mimo_cond\n    - ens_mimo_stream\n",
            repo.display()
        ),
    )
    .unwrap();
    // Orchestration-scope extras (e.g. per-model strategies) land inside the
    // orchestration block — injected between the key and control_mode, so the
    // anchored replacement keeps the rest of the block intact.
    if !orch_extra.is_empty() {
        let content = std::fs::read_to_string(&path).unwrap();
        let marked = content.replacen(
            "orchestration:\n  control_mode: explicit",
            &format!("orchestration:{orch_extra}\n  control_mode: explicit"),
            1,
        );
        std::fs::write(&path, marked).unwrap();
    }
    path
}

/// Boot a server with the fixture repo; returns (base, guard, repo).
async fn boot_server(extra: &str) -> (String, ServerGuard, std::path::PathBuf) {
    boot_server_orch(extra, "").await
}

/// boot_server + orchestration-scope extras (per-model strategies).
async fn boot_server_orch(extra: &str, orch_extra: &str) -> (String, ServerGuard, std::path::PathBuf) {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    // Port-suffixed repo: tests run concurrently and must not share files.
    let repo = std::env::temp_dir()
        .join(format!("lite-server-ens-stream-{}-{}", std::process::id(), http_port));
    let _ = std::fs::remove_dir_all(&repo);
    write_all_fixtures(&repo);
    let server_yaml = write_server_yaml(&repo, http_port, extra, orch_extra);
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

/// D16/D26: the pipeline form is OPEN in batch 2, but this fixture's chain
/// has a UNARY consumer (s2 = pre consumes s1's streaming output) — the
/// chunk→unary→chunk shape is rejected at load (D26), so the model never
/// becomes ready and requests see not-ready (never a fake 200 stream).
#[serial]
#[tokio::test]
async fn test_audit_stream_pipeline_form_rejected_batch0() {
    let (base, _guard, _repo) = boot_server("").await;
    // ens_pipeline must NOT be ready (config validation failed at load).
    assert!(
        !wait_model_ready(&base, "ens_pipeline", 5).await,
        "unary-consumer chain must be rejected at load time (D26)"
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

// ---------------------------------------------------------------------------
// Batch 1: gRPC server-streaming + bidi aggregation (§4.3/D17/D33/D3)
// ---------------------------------------------------------------------------

/// Boot a server with gRPC enabled (batch-1 gRPC tests).
async fn boot_server_grpc(extra: &str) -> (String, u16, ServerGuard, std::path::PathBuf) {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-ens-stream-{}-{}", std::process::id(), http_port));
    let _ = std::fs::remove_dir_all(&repo);
    write_all_fixtures(&repo);
    let dir = std::env::temp_dir().join(format!(
        "lite-server-ens-stream-yaml-{}-{}",
        std::process::id(),
        http_port
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let server_yaml = dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  timeout: 30.0\n{extra}\n\n\
             grpc:\n  enabled: true\n\n\
             model_repository:\n  path: {}\n\n\
             orchestration:\n  control_mode: explicit\n  load_models:\n    - pre\n    - pre_agg\n    - tail\n    - tail_upper\n    - tail_fail_chain\n    - tail_v2\n    - tail_slow\n    - tail_fail\n    - tail_binary\n    - tail_split\n    - tail_dslow\n    - pre_bad\n    - pre_5xx\n    - ghost\n    - echo\n    - ens_stream\n    - ens_agg\n    - ens_chain\n    - ens_chain_fail\n    - chain_slow_head\n    - chain_head_fail\n    - ens_chain_slow\n    - ens_chain_head_fail\n    - ens_chain_sibling\n    - ens_split\n    - ens_dslow\n    - ens_binary\n    - ens_unary\n    - ens_pipeline\n    - ens_bad_sub\n    - ens_4xx\n    - ens_5xx\n    - ens_fail\n    - ens_slow\n    - ens_nested_child\n    - ens_nested_parent\n    - ens_self\n    - ens_mut_a\n    - ens_mut_b\n    - ens_child_stream\n    - ens_out_mid\n    - ens_out_field\n    - ens_out_missing\n    - ens_params\n    - drift\n    - ens_drift\n    - ens_drift_child\n    - ens_drift_parent\n    - ens_e5_stream\n    - ens_e5_autoload\n    - ens_sibling_race\n    - ens_e5_slow_child\n    - ens_e5_nested_timeout\n    - pre_flaky\n    - tail_flaky\n    - tail_fail_always\n    - ens_skip\n    - ens_retry\n    - ens_retry_off\n    - ens_retry_exhaust\n    - ens_stream_retry\n    - ens_stream_retry_exhaust\n    - ens_stream_committed\n    - vis_enc\n    - cropper\n    - classifier\n    - ens_mimo\n    - ens_mimo_json_alias\n    - ens_multisink\n    - ens_dags\n    - ens_when\n    - ens_mimo_bin\n    - ens_mimo_cond\n    - ens_mimo_stream\n",
            repo.display()
        ),
    )
    .unwrap();
    let guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    (
        format!("http://127.0.0.1:{}", http_port),
        grpc_port,
        guard,
        repo,
    )
}

#[cfg(unix)]
async fn grpc_tcp_channel(grpc_port: u16) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", grpc_port))
        .expect("grpc endpoint")
        .connect()
        .await
        .expect("grpc connect")
}

/// gRPC server-streaming parity (e8430dd precedent): the SAME ensemble DAG
/// produces the SAME chunk sequence over gRPC StreamInfer and HTTP SSE.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_stream_grpc_server_streaming_parity() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::StreamInferRequest;
    use std::collections::HashMap;

    let (base, grpc_port, _guard, _repo) = boot_server_grpc("").await;
    wait_ready_all(&base, &["ens_stream"]).await;

    // SSE reference sequence.
    let sse = sse_post(&base, "/v2/models/ens_stream/events", json!({"text": "parity check"}))
        .await
        .expect("SSE must open");
    let sse_tokens: Vec<String> = sse
        .lines()
        .filter_map(|l| {
            let l = l.strip_prefix("data: ")?;
            let v: Value = serde_json::from_str(l).ok()?;
            v.get("token")?.as_str().map(|s| s.to_string())
        })
        .collect();
    assert_eq!(sse_tokens, vec!["parity", "check"], "SSE reference: {sse}");

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let resp = client
        .stream_infer(StreamInferRequest {
            model_name: "ens_stream".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(r#"{"text":"parity check"}"#),
            headers: HashMap::new(),
            sequence_id: None,
        })
        .await
        .expect("StreamInfer must open");
    let mut stream = resp.into_inner();
    let mut grpc_tokens: Vec<String> = Vec::new();
    while let Ok(Some(chunk)) = stream.message().await {
        if let Ok(v) = serde_json::from_slice::<Value>(&chunk.data) {
            if let Some(t) = v.get("token").and_then(|t| t.as_str()) {
                grpc_tokens.push(t.to_string());
            }
        }
    }
    assert_eq!(
        sse_tokens, grpc_tokens,
        "gRPC server-streaming must produce the same chunk sequence as SSE (parity)"
    );
}

/// gRPC bidi aggregation: Open + multiple JSON Data frames → half-close →
/// the DAG runs on the aggregated JSON array → tail stream flows down.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_stream_grpc_bidi_json_aggregation() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::{bidi_chunk, BidiChunk, BidiData, BidiOpen};

    let (base, grpc_port, _guard, _repo) = boot_server_grpc("").await;
    wait_ready_all(&base, &["ens_agg"]).await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel::<BidiChunk>(16);
    tx.send(BidiChunk {
        stream_id: "t".into(),
        payload: Some(bidi_chunk::Payload::Open(BidiOpen {
            model_name: "ens_agg".into(),
            version: "1".into(),
            initial_data: bytes::Bytes::from(r#"{"text":"hello"}"#),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    tx.send(BidiChunk {
        stream_id: "t".into(),
        payload: Some(bidi_chunk::Payload::Data(BidiData {
            data: bytes::Bytes::from(r#"{"text":"world"}"#),
        })),
    })
    .await
    .unwrap();
    // Half-close → trigger (D33).
    drop(tx);

    let resp = client
        .bidi_stream(tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
        .await
        .expect("bidi must open");
    let mut out = resp.into_inner();
    let mut tokens: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), out.message()).await {
            Ok(Ok(Some(chunk))) => {
                match chunk.payload {
                    Some(bidi_chunk::Payload::Data(d)) => {
                        if let Ok(v) = serde_json::from_slice::<Value>(&d.data) {
                            if let Some(t) = v.get("token").and_then(|t| t.as_str()) {
                                tokens.push(t.to_string());
                            }
                        }
                    }
                    Some(bidi_chunk::Payload::Close(_)) => break,
                    _ => {}
                }
            }
            Ok(Ok(None)) | Ok(Err(_)) => break,
            Err(_) => break,
        }
    }
    assert_eq!(
        tokens,
        vec!["hello", "world"],
        "gRPC bidi must aggregate JSON frames into an array and stream the DAG output: {tokens:?}"
    );
}

/// WS bidi aggregation: multi-frame JSON + app-level close frame → trigger →
/// tail stream flows down. (D33: the close frame is the WS trigger.)
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_bidi_aggregation() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_agg"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_agg/stream", http_port);
    use futures::{SinkExt, StreamExt};
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"via"}"#.into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"ws"}"#.into(),
    ))
    .await
    .unwrap();
    // App-level close frame → aggregation trigger (D33).
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"close"}"#.into(),
    ))
    .await
    .unwrap();

    let mut tokens: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            // WS writer emits chunks as Binary frames (existing behaviour).
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if let Some(tok) = v.get("token").and_then(|x| x.as_str()) {
                        tokens.push(tok.to_string());
                    }
                    if v.get("done").and_then(|x| x.as_bool()) == Some(true) {
                        break;
                    }
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b)))) => {
                if let Ok(v) = serde_json::from_slice::<Value>(&b) {
                    if let Some(tok) = v.get("token").and_then(|x| x.as_str()) {
                        tokens.push(tok.to_string());
                    }
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) => break,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
            _ => {}
        }
    }
    assert_eq!(
        tokens,
        vec!["via", "ws"],
        "WS bidi must aggregate JSON frames and stream the DAG output: {tokens:?}"
    );
}

/// h2 bidi aggregation: LPM Open + Data frames → body EOF (half-close) →
/// trigger → tail stream flows down in LPM frames.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_stream_h2_bidi_aggregation() {
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;
    use futures::StreamExt;

    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_agg"]).await;

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, reqwest::Error>>(16);
    tx.send(Ok(lpm::encode_frame(&pb::BidiChunk {
        stream_id: "t".into(),
        payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
            initial_data: bytes::Bytes::from(r#"{"text":"h2"}"#),
            ..Default::default()
        })),
    })))
    .await
    .unwrap();
    tx.send(Ok(lpm::encode_frame(&pb::BidiChunk {
        stream_id: "t".into(),
        payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
            data: bytes::Bytes::from(r#"{"text":"works"}"#),
        })),
    })))
    .await
    .unwrap();
    drop(tx); // body EOF → half-close → trigger (D33)
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = reqwest::Body::wrap_stream(body_stream);

    let resp = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap()
        .post(format!("{}/v2/models/ens_agg/bidi", base))
        .header("content-type", "application/x-lite-bidi")
        .body(body)
        .send()
        .await
        .expect("h2 bidi POST");
    assert_eq!(resp.status(), 200, "h2 bidi must open");

    let mut tokens: Vec<String> = Vec::new();
    let mut buf = bytes::BytesMut::new();
    let mut body = resp.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    'outer: while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), body.next()).await {
            Ok(Some(Ok(bytes))) => {
                buf.extend_from_slice(&bytes);
                while let Ok(Some(chunk)) = lpm::try_decode_frame(&mut buf) {
                    match chunk.payload {
                        Some(pb::bidi_chunk::Payload::Data(d)) => {
                            if let Ok(v) = serde_json::from_slice::<Value>(&d.data) {
                                if let Some(t) = v.get("token").and_then(|x| x.as_str()) {
                                    tokens.push(t.to_string());
                                }
                            }
                        }
                        Some(pb::bidi_chunk::Payload::Close(_)) => break 'outer,
                        _ => {}
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    assert_eq!(
        tokens,
        vec!["h2", "works"],
        "h2 bidi must aggregate LPM frames and stream the DAG output: {tokens:?}"
    );
}

/// WS multi-round rejection: frames after the aggregation trigger are a
/// session violation → error frame + close (§4.3).
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_bidi_multi_round_rejected() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_stream/stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"hello"}"#.into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"close"}"#.into(),
    ))
    .await
    .unwrap();

    // Wait for the first downstream chunk (aggregation triggered + DAG ran).
    // WS writer emits chunks as Binary frames (existing behaviour).
    let mut got_chunk = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b)))) => {
                if b.windows(5).any(|w| w == b"token") {
                    got_chunk = true;
                    break;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if t.contains("token") {
                    got_chunk = true;
                    break;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) => break,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
            _ => {}
        }
    }
    assert!(got_chunk, "must receive a downstream chunk before multi-round check");

    // A data frame after the trigger → error + close (multi-round rejected).
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"late"}"#.into(),
    ))
    .await
    .unwrap();
    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if t.contains("multi-round") {
                    saw_error = true;
                    break;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) => {
                saw_error = true;
                break;
            }
            _ => break,
        }
    }
    assert!(saw_error, "multi-round frame must be rejected with an error/close");
}

/// WS mixed frames: JSON + Binary → 400 semantics (error frame + close, D17).
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_bidi_mixed_frames_rejected() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_stream/stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"hello"}"#.into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![0u8, 1, 2]))
        .await
        .unwrap();

    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if t.contains("error") {
                    saw_error = true;
                    break;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) => {
                saw_error = true;
                break;
            }
            _ => break,
        }
    }
    assert!(saw_error, "mixed JSON/Binary frames must be rejected (D17)");
}

/// WS aggregation idle timeout: no close frame within the idle budget →
/// error + close (§4.3 D17: aggregation reuses the two-stage bound).
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_bidi_aggregation_idle_timeout() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("  decoupled_idle_timeout_secs: 1.0").await;
    wait_ready_all(&base, &["ens_stream"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_stream/stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    // Send a first frame but never the close trigger.
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"stuck"}"#.into(),
    ))
    .await
    .unwrap();

    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if t.contains("idle") || t.contains("error") {
                    saw_error = true;
                    break;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) => {
                saw_error = true;
                break;
            }
            _ => break,
        }
    }
    assert!(
        saw_error,
        "aggregation without a close trigger must hit the idle timeout"
    );
}

// ---------------------------------------------------------------------------
// /audit 2026-08-12: batch-0/1 defect repros (read-only; no impl changes)
// ---------------------------------------------------------------------------

/// D33/现有 decoupled 契约:WS decoupled 的 close/cancel 帧是 cancel 别名
/// (is_cancel_or_close_frame)。ensemble 分支把 cancel 帧误判为「多轮违规」,
/// 客户端收到子虚乌有的协议错误。
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_decoupled_cancel_frame_is_not_multi_round() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_dslow"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_dslow/decoupled-stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"one two three four"}"#.into(),
    ))
    .await
    .unwrap();

    // Wait for the first downstream chunk (DAG is streaming).
    let mut got_chunk = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !got_chunk {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b)))) =
            tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        {
            if b.windows(5).any(|w| w == b"token") {
                got_chunk = true;
            }
        }
    }
    assert!(got_chunk, "must receive a downstream chunk before cancel");

    // decoupled contract: a cancel frame cancels the worker stream + closes.
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"cancel"}"#.into(),
    ))
    .await
    .unwrap();
    let mut multi_round_error = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if t.contains("multi-round") {
                    multi_round_error = true;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b)))) => {
                if b.windows(11).any(|w| w == b"multi-round") {
                    multi_round_error = true;
                }
            }
            _ => break,
        }
    }
    assert!(
        !multi_round_error,
        "decoupled cancel frame must cancel the stream (existing contract), \
         not be rejected as a multi-round violation"
    );
}

/// D33:decoupled 的 close 帧同为 cancel 别名。ensemble 分支静默忽略它——
/// worker 继续生成、客户端继续收 chunk,cancel 语义丢失。
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_decoupled_close_frame_cancels_stream() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_dslow"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_dslow/decoupled-stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"one two three four five six"}"#.into(),
    ))
    .await
    .unwrap();

    // Wait for the first downstream chunk.
    let mut got_chunk = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !got_chunk {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b)))) =
            tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        {
            if b.windows(5).any(|w| w == b"token") {
                got_chunk = true;
            }
        }
    }
    assert!(got_chunk, "must receive a downstream chunk before close");

    // decoupled contract: close frame = cancel alias → stream stops promptly.
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"close"}"#.into(),
    ))
    .await
    .unwrap();
    let mut further_chunks = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(1500), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b)))) => {
                if b.windows(5).any(|w| w == b"token") {
                    further_chunks += 1;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if t.contains("token") {
                    further_chunks += 1;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) | Ok(None) => break,
            _ => break,
        }
    }
    assert_eq!(
        further_chunks, 0,
        "decoupled close frame must cancel the ensemble stream; chunks kept flowing"
    );
}

/// D17 硬要求(方案原文「禁止新写不接 idle 超时的聚合循环」):h2 聚合循环在
/// body_stream.next() 上无超时阻塞,静默客户端可无限期钉住聚合缓冲。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_stream_h2_bidi_aggregation_stall_reclaimed() {
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;

    let (base, _guard, _repo) = boot_server("  decoupled_idle_timeout_secs: 1.0").await;
    wait_ready_all(&base, &["ens_agg"]).await;

    // Body yields the Open frame, then never yields and never EOFs.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, reqwest::Error>>(16);
    tx.send(Ok(lpm::encode_frame(&pb::BidiChunk {
        stream_id: "t".into(),
        payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
            initial_data: bytes::Bytes::from(r#"{"text":"h2"}"#),
            ..Default::default()
        })),
    })))
    .await
    .unwrap();
    let _hold_tx = tx; // held open: no more frames, no EOF
    let body = reqwest::Body::wrap_stream(tokio_stream::wrappers::ReceiverStream::new(rx));

    let send = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap()
        .post(format!("{}/v2/models/ens_agg/bidi", base))
        .header("content-type", "application/x-lite-bidi")
        .body(body)
        .send();
    // Fixed behavior: the always-on idle budget (~1s) reclaims the stalled
    // aggregation and the server responds/closes. Today the recv is not
    // timeout-wrapped, so the handler hangs forever.
    let answered = tokio::time::timeout(Duration::from_secs(6), send).await;
    assert!(
        answered.is_ok(),
        "h2 aggregation of a stalled client must be reclaimed by the idle budget (D17), \
         not hang forever holding the aggregation buffer"
    );
}

/// §4.4 (D21):WS 错误一律落 close frame + 契约 close code(组合不支持 =
/// 1003)。当前 ws_send_error 发的是无 code 的普通 close。
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_bidi_error_close_code_is_contractual() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_stream/stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"hello"}"#.into(),
    ))
    .await
    .unwrap();
    // Mixed frames (JSON then Binary) → 400 semantics: close 1003 (D17/§4.4).
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![0u8, 1, 2]))
        .await
        .unwrap();

    let mut close_code = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(frame)))) => {
                close_code = frame.map(|f| f.code);
                break;
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            _ => {}
        }
    }
    assert_eq!(
        close_code,
        Some(tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Unsupported),
        "mixed-frame rejection must close with the contractual 1003 (§4.4)"
    );
}

/// D17 两段式:per-chunk idle 常开。客户端指定了 overall deadline 时,
/// 聚合循环丢掉 idle 约束((Some(d), _) 分支),静默连接可挂到 deadline
/// 才被回收——recv_chunk 的语义是 min(overall, idle)。
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_bidi_aggregation_idle_despite_client_deadline() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("  decoupled_idle_timeout_secs: 1.0").await;
    wait_ready_all(&base, &["ens_stream"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_stream/stream", http_port);
    let req = tokio_tungstenite::tungstenite::client::ClientRequestBuilder::new(
        ws_url.parse::<tokio_tungstenite::tungstenite::http::Uri>().unwrap(),
    )
    .with_header("x-lite-timeout", "30");
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"stuck"}"#.into(),
    ))
    .await
    .unwrap();
    // Never send the close trigger. idle=1s must fire well before the 30s
    // overall deadline.
    let mut saw_error = false;
    let answered = tokio::time::timeout(Duration::from_secs(6), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => {
                    if t.contains("idle") || t.contains("error") {
                        saw_error = true;
                        break;
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                    saw_error = true;
                    break;
                }
                _ => {}
            }
        }
    })
    .await;
    assert!(
        answered.is_ok() && saw_error,
        "aggregation idle timeout must fire even when the client set an overall deadline (D17 two-stage)"
    );
}

/// m7 边界:字节级切分的文本 chunk(多字节 UTF-8 字符跨 chunk)不是 Binary。
/// 直连 SSE 容忍(from_utf8_lossy),ensemble 不得误判 type_mismatch 杀流。
#[tokio::test]
#[serial]
async fn test_audit_stream_split_utf8_chunks_not_type_mismatch() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_split"]).await;

    let body = sse_post(&base, "/v2/models/ens_split/events", json!({"text": "x"}))
        .await
        .expect("split-UTF8 text stream must open");
    assert!(
        body.contains("[DONE]") && !body.contains("type_mismatch") && !body.contains("binary chunk"),
        "byte-split UTF-8 text chunks must stream to completion (direct-path parity), \
         not be killed as type_mismatch: {body}"
    );
}

/// §4.1 指标行:record_ensemble_step_latency 对流式 step 以流 close 时刻计
/// (EnsembleStream.tail_model/tail_version 即为此携带)。当前没有任何记录点。
#[tokio::test]
#[serial]
async fn test_audit_stream_step_latency_covers_streaming_step() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream"]).await;

    let _ = sse_post(&base, "/v2/models/ens_stream/events", json!({"text": "a b"}))
        .await
        .expect("SSE happy path");
    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics", base))
        .send()
        .await
        .expect("/metrics")
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains(r#"step="tail""#),
        "ensemble_step_latency must include the streaming step (recorded at stream close, §4.1); \
         only pre-layer steps are recorded today"
    );
}

// ---------------------------------------------------------------------------
// Batch 2: pipeline chain (§4.2/D2/D18/D20/D26)
// ---------------------------------------------------------------------------

/// Second chain hop: consumes the previous step's chunk (per-chunk sub-call),
/// yields the token uppercased.
fn write_tail_upper(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_upper",
        r#"import time
from lite_server import LitAPI


class TailUpperAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("prev", {})

    async def predict(self, x, ctx=None):
        return {"final": str(x)}

    def stream_predict(self, request, ctx=None):
        w = request.get("token", "?") if isinstance(request, dict) else str(request)
        time.sleep(0.02)
        yield {"final": w.upper()}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Second chain hop that fails on the 2nd chunk (mid-chain failure, m8).
fn write_tail_fail_chain(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail_fail_chain",
        r#"import time
from lite_server import LitAPI


class TailFailChainAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("prev", {})

    async def predict(self, x, ctx=None):
        return {"final": str(x)}

    def stream_predict(self, request, ctx=None):
        w = request.get("token", "?") if isinstance(request, dict) else str(request)
        time.sleep(0.02)
        if w == "chain":
            raise RuntimeError("mid-chain boom")
        yield {"final": w.upper()}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Chain head that streams slowly and marks generator cancellation — the
/// D18 fixture: chain teardown must cancel EVERY streaming step's worker
/// (GeneratorExit fires iff the worker receives a StreamCancel with the
/// correct stream id).
fn write_chain_slow_head(repo: &std::path::Path) {
    write_model_py(
        repo,
        "chain_slow_head",
        r#"import os
import time
from lite_server import LitAPI


class ChainSlowHeadAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": [x]}

    def stream_predict(self, request, ctx=None):
        marker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_cancelled")
        try:
            for i in range(15):
                time.sleep(0.3)
                yield {"token": f"t{i}"}
        except GeneratorExit:
            with open(marker, "w") as f:
                f.write("cancelled")
            raise

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Chain head that fails on its 2nd chunk (head mid-stream failure — §4.4
/// requires the failure to surface as an Error frame, not a silent EOF).
fn write_chain_head_fail(repo: &std::path::Path) {
    write_model_py(
        repo,
        "chain_head_fail",
        r#"import time
from lite_server import LitAPI


class ChainHeadFailAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": [x]}

    def stream_predict(self, request, ctx=None):
        words = (request.get("pre", "") if isinstance(request, dict) else str(request)).split()
        for i, w in enumerate(words):
            time.sleep(0.02)
            if i == 1:
                raise RuntimeError("head boom")
            yield {"token": w}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Two-level streaming chain: t1 (stream) → t2 (stream, whole $t1).
const ENS_CHAIN_YAML: &str = r#"ensemble:
  steps:
    - name: t1
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$request.text"
    - name: t2
      model: tail_upper
      version: "1"
      stream: true
      inputs:
        prev: "$t1"
"#;

const ENS_CHAIN_FAIL_YAML: &str = r#"ensemble:
  steps:
    - name: t1
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$request.text"
    - name: t2
      model: tail_fail_chain
      version: "1"
      stream: true
      inputs:
        prev: "$t1"
"#;

/// Slow-head chain (WS D18 cancel-contract fixture).
const ENS_CHAIN_SLOW_YAML: &str = r#"ensemble:
  steps:
    - name: t1
      model: chain_slow_head
      version: "1"
      stream: true
      inputs:
        pre: "$request.text"
    - name: t2
      model: tail_upper
      version: "1"
      stream: true
      inputs:
        prev: "$t1"
"#;

/// Head-fail chain: the HEAD worker errors on its 2nd chunk.
const ENS_CHAIN_HEAD_FAIL_YAML: &str = r#"ensemble:
  steps:
    - name: t1
      model: chain_head_fail
      version: "1"
      stream: true
      inputs:
        pre: "$request.text"
    - name: t2
      model: tail_upper
      version: "1"
      stream: true
      inputs:
        prev: "$t1"
"#;

/// Chain + a unary sibling in the head's layer (§4.2: non-chain steps still
/// run by layer — the sibling must not be silently skipped).
const ENS_CHAIN_SIBLING_YAML: &str = r#"ensemble:
  steps:
    - name: t1
      model: tail
      version: "1"
      stream: true
      inputs:
        pre: "$request.text"
    - name: u
      model: echo
      version: "1"
      inputs:
        data: "$request.text"
    - name: t2
      model: tail_upper
      version: "1"
      stream: true
      inputs:
        prev: "$t1"
"#;

/// §4.2 e2e: a two-level streaming chain drives the tail per upstream chunk
/// — each t1 token opens a t2 sub-stream (D20), all chunks flow, [DONE].
#[serial]
#[tokio::test]
async fn test_audit_stream_pipeline_chain_sse() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_chain"]).await;

    let body = sse_post(&base, "/v2/models/ens_chain/events", json!({"text": "hello chain world"}))
        .await
        .expect("chain SSE must open");
    assert!(
        body.contains(r#""final":"HELLO""#)
            && body.contains(r#""final":"CHAIN""#)
            && body.contains(r#""final":"WORLD""#),
        "chain must drive the tail per upstream chunk: {body}"
    );
    assert!(body.contains("[DONE]"), "chain must terminate with [DONE]: {body}");
    assert!(!body.contains("error"), "no error expected: {body}");
}

/// §4.2/m8: a mid-chain sub-stream failure delivers the partial output, then
/// an Error frame (no fake [DONE]) — the failure propagates downstream.
#[serial]
#[tokio::test]
async fn test_audit_stream_pipeline_midchain_failure() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_chain_fail"]).await;

    let body = sse_post(&base, "/v2/models/ens_chain_fail/events", json!({"text": "hello chain world"}))
        .await
        .expect("chain must open (200)");
    assert!(
        body.contains(r#""final":"HELLO""#),
        "chunks before the failure must stream: {body}"
    );
    assert!(
        body.contains("error") && !body.contains("[DONE]"),
        "mid-chain failure must close with an Error frame, no [DONE]: {body}"
    );
}

// ---------------------------------------------------------------------------
// /audit 2026-08-13: batch-2 defect repros (read-only; no impl changes)
// ---------------------------------------------------------------------------

/// §4.2/D18:WS 适配器未接线 cancel_chain(ensemble_chain/ensemble_abort
/// 赋值后从未读取,clippy unused_variables 已告警)。客户端断连后主任务只
/// 向 head client 发携带合成 "chain-*" id 的 StreamCancel,head worker 的
/// 真实流 id 收不到 cancel → 生成器跑完全部 token,无 GeneratorExit。
#[tokio::test]
#[serial]
async fn test_audit_stream_ws_chain_disconnect_cancels_head_worker() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (base, _guard, repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_chain_slow"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_chain_slow/stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(Message::Text(r#"{"text":"a b c"}"#.into())).await.unwrap();
    ws.send(Message::Text(r#"{"type":"close"}"#.into())).await.unwrap();

    // Two downstream chunks, then abrupt disconnect (no close frame).
    let mut chunks = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline && chunks < 2 {
        if let Ok(Some(Ok(Message::Binary(b)))) =
            tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        {
            if b.windows(5).any(|w| w == b"final") {
                chunks += 1;
            }
        }
    }
    assert_eq!(chunks, 2, "chain must deliver downstream chunks before disconnect");
    drop(ws);

    // D18: chain teardown must cancel EVERY streaming step's worker — the
    // head receives StreamCancel → generator.close() → GeneratorExit marker.
    let marker = repo.join("chain_slow_head").join("1").join("gen_cancelled");
    let mut cancelled = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < deadline && !cancelled {
        if marker.exists() {
            cancelled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        cancelled,
        "client disconnect must cancel the chain head worker (D18 broadcast); \
         the head generator ran without receiving a StreamCancel"
    );
}

/// §4.4:开流后的一切失败 → Error 帧 + close 收口。链头 worker 中途失败
/// 时,首个消费者的 Error 分支吞帧(注释声称「已由前跳转发」,但链头没有
/// 前跳),下游静默 EOF——客户端看到 200 流干净结束,无 Error 帧无 [DONE]。
#[tokio::test]
#[serial]
async fn test_audit_stream_pipeline_head_failure_emits_error_frame() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_chain_head_fail"]).await;

    let body = sse_post(
        &base,
        "/v2/models/ens_chain_head_fail/events",
        json!({"text": "hello chain world"}),
    )
    .await
    .expect("chain must open (200)");
    assert!(
        body.contains(r#""final":"HELLO""#),
        "chunks before the head failure must stream: {body}"
    );
    assert!(
        body.contains("error"),
        "head mid-stream failure must close with an Error frame (§4.4), \
         got a silent EOF: {body}"
    );
}

/// §4.2:非链部分仍按层 JoinSet 推进。与链头同层的 unary step(无依赖)
/// 落在 layers[..head_layer] 之外 → 永远不执行且无任何报错。对照 §4.1
/// 规则 3(末步流式的同层兄弟并行执行)与规则 1 的拒止精神,属静默截断。
#[tokio::test]
#[serial]
async fn test_audit_stream_pipeline_same_layer_unary_sibling_runs() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_chain_sibling"]).await;

    let _ = sse_post(&base, "/v2/models/ens_chain_sibling/events", json!({"text": "hello"}))
        .await
        .expect("chain SSE must open");
    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics", base))
        .send()
        .await
        .expect("/metrics")
        .text()
        .await
        .unwrap();
    let count = metrics
        .lines()
        .find(|l| l.starts_with(r#"liteserver_worker_inference_total{model="echo""#))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        count > 0,
        "non-chain unary step in the head's layer must still execute (§4.2); \
         the sibling worker saw {count} inferences"
    );
}

// ---------------------------------------------------------------------------
// Batch 3 fixtures (E1/E2/E3/E4/D35)
// ---------------------------------------------------------------------------

/// Nested ensembles: parent pre → ens_nested_child (echo inside).
// NOTE: the child only consumes single-level request fields ($request.x) —
// multi-segment paths ($request.x.pre) are MIMO/R17 territory (batch 4), so
// the parent flattens the payload it hands down.
const ENS_NESTED_CHILD_YAML: &str = r#"ensemble:
  steps:
    - name: a
      model: pre
      version: "1"
      inputs:
        text: "$request.x"
    - name: b
      model: echo
      version: "1"
      inputs:
        data: "$a.pre"
"#;

const ENS_NESTED_PARENT_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: child
      model: ens_nested_child
      inputs:
        x: "$pre.pre"
"#;

/// Self-referencing DAG (step → its own model) — the ancestor guard rejects
/// it at request time.
const ENS_SELF_YAML: &str = r#"ensemble:
  steps:
    - name: only
      model: ens_self
      inputs:
        x: "$request"
"#;

/// Cross-model mutual recursion (ens_mut_a → ens_mut_b → ens_mut_a) — the
/// runtime ancestor chain catches what parse-time Kahn cannot.
const ENS_MUT_A_YAML: &str = r#"ensemble:
  steps:
    - name: only
      model: ens_mut_b
      inputs:
        x: "$request"
"#;

const ENS_MUT_B_YAML: &str = r#"ensemble:
  steps:
    - name: only
      model: ens_mut_a
      inputs:
        x: "$request"
"#;

/// Parent step pointing at a STREAMING child DAG — rejected (D4).
const ENS_CHILD_STREAM_YAML: &str = r#"ensemble:
  steps:
    - name: child
      model: ens_stream
      inputs:
        x: "$request"
"#;

/// E2: explicit output pointing at a mid-DAG step (s2 still runs).
const ENS_OUT_MID_YAML: &str = r#"ensemble:
  output: "$s1"
  steps:
    - name: s1
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: s2
      model: echo
      version: "1"
      inputs:
        data: "$s1.pre"
"#;

/// E2: field projection on the output step.
const ENS_OUT_FIELD_YAML: &str = r#"ensemble:
  output: "$s1.pre"
  steps:
    - name: s1
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: s2
      model: echo
      version: "1"
      inputs:
        data: "$s1.pre"
"#;

/// E2: output field missing from the model's actual output → 400.
const ENS_OUT_MISSING_YAML: &str = r#"ensemble:
  output: "$s1.nope"
  steps:
    - name: s1
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
"#;

/// E3: params merge into the assembled payload (reflect echoes the whole
/// request back, so the merged params are observable).
const ENS_PARAMS_YAML: &str = r#"ensemble:
  steps:
    - name: s1
      model: reflect
      version: "1"
      inputs:
        data: "$request.text"
      params:
        bias: 2
"#;

/// E4/D15: two steps of the SAME model (drift, version omitted) must share
/// the first resolution even when the active version changes mid-request.
/// `fin` (reflect) surfaces BOTH steps' versions in the response.
const ENS_DRIFT_YAML: &str = r#"ensemble:
  steps:
    - name: a
      model: drift
      inputs:
        x: "$request.text"
    - name: b
      model: drift
      inputs:
        x: "$a.ver"
    - name: fin
      model: reflect
      version: "1"
      inputs:
        a_ver: "$a.ver"
        b_ver: "$b.ver"
"#;

/// E4/D36 nested variant: the CHILD DAG resolves the same model through the
/// parent's snapshot — a child building its own table would see the drift.
const ENS_DRIFT_CHILD_YAML: &str = r#"ensemble:
  steps:
    - name: c
      model: drift
      inputs:
        x: "$request.x"
"#;

const ENS_DRIFT_PARENT_YAML: &str = r#"ensemble:
  steps:
    - name: a
      model: drift
      inputs:
        x: "$request.text"
    - name: child
      model: ens_drift_child
      inputs:
        x: "$a.ver"
    - name: fin
      model: reflect
      version: "1"
      inputs:
        a_ver: "$a.ver"
        c_ver: "$child.ver"
"#;

/// D35: streaming step with timeout_secs 1.0 over a 0.5s/chunk tail — the
/// recv overall bound fires mid-stream → Error frame + close.
const ENS_E5_STREAM_YAML: &str = r#"ensemble:
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
      timeout_secs: 1.0
      inputs:
        pre: "$pre.pre"
"#;

/// D35 pre-open exhaustion: the tail model is NOT preloaded (autoload at
/// request) and its step budget (0.01s) cannot survive the autoload → 504.
const ENS_E5_AUTOLOAD_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_late
      version: "1"
      stream: true
      timeout_secs: 0.01
      inputs:
        pre: "$pre.pre"
"#;

/// Audit: two SAME-LAYER steps both calling the same child ensemble is legal
/// fan-out (E3 params make the shape natural) — not recursion. The child
/// (ens_drift_child → drift v1) runs ~2s, so the first sibling's child run
/// stays in flight while the second checks the shared ancestor chain.
const ENS_SIBLING_RACE_YAML: &str = r#"ensemble:
  steps:
    - name: s1
      model: ens_drift_child
      inputs:
        x: "$request"
    - name: s2
      model: ens_drift_child
      inputs:
        x: "$request"
"#;

/// Audit: child of the nested-timeout parent — a 2s unary sub-model, so the
/// parent step's 0.3s cap is the only bound that could stop it early.
const ENS_E5_SLOW_CHILD_YAML: &str = r#"ensemble:
  steps:
    - name: c
      model: drift
      version: "1"
      inputs:
        x: "$request.x"
"#;

/// Audit: parent step → child ensemble with timeout_secs 0.3 — E5's local
/// timeout wrap must bound the CHILD run (nested execution is the step's
/// execution), not just direct worker calls.
const ENS_E5_NESTED_TIMEOUT_YAML: &str = r#"ensemble:
  steps:
    - name: child
      model: ens_e5_slow_child
      timeout_secs: 0.3
      inputs:
        x: "$request"
"#;

/// Reflect worker: returns the WHOLE decoded request (params observable).
fn write_reflect(repo: &std::path::Path) {
    write_model_py(
        repo,
        "reflect",
        r#"from lite_server import LitAPI


class ReflectAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request

    async def predict(self, x, ctx=None):
        return {"got": x}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Two drift versions: v1 (slow 2s, answers "v1"), v2 (fast, answers "v2").
fn write_drift(repo: &std::path::Path) {
    for (version, ver, sleep_s) in [("1", "v1", "        time.sleep(2.0)\n"), ("2", "v2", "")] {
        let dir = repo.join("drift").join(version);
        std::fs::create_dir_all(&dir).unwrap();
        let py = format!(
            r#"import time
from lite_server import LitAPI


class DriftAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("x", "")

    async def predict(self, x, ctx=None):
{sleep_s}        return {{"ver": "{ver}"}}

    async def encode_response(self, output, ctx=None):
        return output
"#
        );
        std::fs::write(dir.join("model.py"), py).unwrap();
        std::fs::write(
            dir.join("config.yaml"),
            "max_batch_size: 1\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();
    }
}

// ---------------------------------------------------------------------------
// Batch 3: E1 nesting / E2 output / E3 params / E4 snapshot / D35 timeout
// ---------------------------------------------------------------------------

/// E1: two-level nesting — parent pre → child ensemble (echo) → the child's
/// output becomes the parent step's value.
#[serial]
#[tokio::test]
async fn test_audit_stream_e1_nested_happy() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_nested_parent"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_nested_parent/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "nested ensemble must succeed");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"echo": "hello"}), "nested output mismatch: {body}");
}

/// E1: a DAG whose step references ITSELF — the runtime ancestor guard
/// rejects it (parse-time Kahn cannot see across configs).
#[serial]
#[tokio::test]
async fn test_audit_stream_e1_self_loop_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_self"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_self/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "self-loop must be 400");
    let body = resp.text().await.unwrap();
    assert!(body.contains("recursion"), "error must name the recursion: {body}");
}

/// E1: cross-model mutual recursion (A→B→A) — the ancestor chain catches it
/// instead of running to the depth limit.
#[serial]
#[tokio::test]
async fn test_audit_stream_e1_mutual_recursion_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mut_a"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_mut_a/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "mutual recursion must be 400");
    let body = resp.text().await.unwrap();
    assert!(body.contains("recursion"), "error must name the recursion: {body}");
}

/// E1 × D4: a parent step pointing at a STREAMING child DAG is rejected
/// (nested ensembles are unary-only).
#[serial]
#[tokio::test]
async fn test_audit_stream_e1_child_streaming_d4_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_child_stream"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_child_stream/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "D4 must be 400");
    let body = resp.text().await.unwrap();
    assert!(body.contains("streaming"), "error must name the streaming child: {body}");
}

/// E2: explicit output — mid-DAG step selection and field projection.
#[serial]
#[tokio::test]
async fn test_audit_stream_e2_output_mid_and_field() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_out_mid", "ens_out_field"]).await;

    let client = reqwest::Client::new();
    // output: "$s1" — s1's WHOLE value, even though s2 is the config-last step.
    let resp = client
        .post(format!("{}/v2/models/ens_out_mid/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"pre": "hello"}), "output must select s1: {body}");

    // output: "$s1.pre" — field projection.
    let resp = client
        .post(format!("{}/v2/models/ens_out_field/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!("hello"), "field projection mismatch: {body}");
}

/// E2: output field missing from the model's actual output → 400 (the DAG
/// contract does not match the model's shape).
#[serial]
#[tokio::test]
async fn test_audit_stream_e2_output_missing_field_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_out_missing"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_out_missing/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "missing field must be 400");
}

/// E3: params merge into the assembled payload (the reflect worker echoes
/// the whole request, so both the input and the param are observable).
#[serial]
#[tokio::test]
async fn test_audit_stream_e3_params_merged() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_params"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_params/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["got"]["data"], json!("hello"), "input must reach the worker: {body}");
    assert_eq!(body["got"]["bias"], json!(2), "params must merge into the payload: {body}");
}

/// E3: params × Binary input → 400 (assembly-time rejection).
#[serial]
#[tokio::test]
async fn test_audit_stream_e3_params_binary_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_params"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_params/infer", base))
        .header("Content-Type", "application/octet-stream")
        .body(vec![0x00u8, 0x01, 0x02])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "params × Binary must be 400");
}

/// E4/D15: two steps of the same model (version omitted) share the FIRST
/// resolution — activating v2 mid-request must not split the DAG.
#[serial]
#[tokio::test]
async fn test_audit_stream_e4_snapshot_active_drift() {
    // drift must hold TWO loaded versions (v1 active, v2 pre-loaded for the
    // mid-request activation) — default max_loaded_versions=1 would 409.
    let (base, _guard, _repo) = boot_server_orch("", "\n  models:\n    - name: drift\n      max_loaded_versions: 2\n      versions_to_load:\n        - \"1\"").await;
    wait_ready_all(&base, &["ens_drift"]).await;

    let client = reqwest::Client::new();
    // Pre-load drift v2 so the mid-request activation is instant.
    let resp = client
        .post(format!("{}/v2/repository/models/drift/versions/2/load", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "drift v2 load must succeed");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let ready = client
            .get(format!("{}/v2/models/drift/versions/2/ready", base))
            .send()
            .await
            .unwrap();
        let v: Value = ready.json().await.unwrap();
        if v["ready"].as_bool() == Some(true) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "drift v2 never became ready");
        sleep(Duration::from_millis(100)).await;
    }

    // Loading v2 AUTO-ACTIVATES it (auto_activated=true) — restore v1 as
    // active so the request starts against v1.
    let restore = client
        .post(format!("{}/v2/models/drift/versions/1/activate", base))
        .send()
        .await
        .unwrap();
    assert_eq!(restore.status(), 200, "restore v1 active must succeed");

    // Fire the request (drift v1 predicts for ~2s), activate v2 mid-flight.
    // The request must be POLLED concurrently (tokio::spawn) — an unpolled
    // reqwest future never sends.
    let req_client = client.clone();
    let req_base = base.clone();
    let req_task = tokio::spawn(async move {
        req_client
            .post(format!("{}/v2/models/ens_drift/infer", req_base))
            .header("Content-Type", "application/json")
            .json(&json!({"text": "hello"}))
            .send()
            .await
    });
    sleep(Duration::from_millis(400)).await;
    let activate = client
        .post(format!("{}/v2/models/drift/versions/2/activate", base))
        .send()
        .await
        .unwrap();
    assert_eq!(activate.status(), 200, "activate v2 must succeed");

    let start = std::time::Instant::now();
    let resp = req_task.await.expect("request task must not panic").unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let got = &body["got"];
    assert_eq!(got["a_ver"], json!("v1"), "step a must use the pre-drift v1: {body}");
    assert_eq!(
        got["b_ver"], json!("v1"),
        "step b must share step a's snapshot (same-request consistency, D15): {body}"
    );
    // Both steps ran drift v1 (2s predict each) — a wall-clock check proves
    // step a really used the slow v1, not the fast v2.
    assert!(
        start.elapsed() >= Duration::from_secs(3),
        "both steps must have run the slow v1 (elapsed {:?})",
        start.elapsed()
    );

    // A FRESH request re-resolves — the snapshot is per-request.
    let resp = client
        .post(format!("{}/v2/models/ens_drift/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    let body2: Value = resp.json().await.unwrap();
    assert_eq!(
        body2["got"]["a_ver"], json!("v2"),
        "fresh request must see the new active v2: {body2}"
    );
}

/// E4/D36 nested variant: the CHILD DAG resolves the same model through the
/// PARENT's snapshot — a child building its own table would see the drift.
#[serial]
#[tokio::test]
async fn test_audit_stream_e4_snapshot_nested_d36() {
    let (base, _guard, _repo) = boot_server_orch("", "\n  models:\n    - name: drift\n      max_loaded_versions: 2\n      versions_to_load:\n        - \"1\"").await;
    wait_ready_all(&base, &["ens_drift_parent"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/repository/models/drift/versions/2/load", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let ready = client
            .get(format!("{}/v2/models/drift/versions/2/ready", base))
            .send()
            .await
            .unwrap();
        let v: Value = ready.json().await.unwrap();
        if v["ready"].as_bool() == Some(true) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "drift v2 never became ready");
        sleep(Duration::from_millis(100)).await;
    }

    // Loading v2 AUTO-ACTIVATES it (auto_activated=true) — restore v1 as
    // active so the request starts against v1.
    let restore = client
        .post(format!("{}/v2/models/drift/versions/1/activate", base))
        .send()
        .await
        .unwrap();
    assert_eq!(restore.status(), 200, "restore v1 active must succeed");

    let req_client = client.clone();
    let req_base = base.clone();
    let req_task = tokio::spawn(async move {
        req_client
            .post(format!("{}/v2/models/ens_drift_parent/infer", req_base))
            .header("Content-Type", "application/json")
            .json(&json!({"text": "hello"}))
            .send()
            .await
    });
    sleep(Duration::from_millis(400)).await;
    let activate = client
        .post(format!("{}/v2/models/drift/versions/2/activate", base))
        .send()
        .await
        .unwrap();
    assert_eq!(activate.status(), 200);

    let resp = req_task.await.expect("request task must not panic").unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let got = &body["got"];
    assert_eq!(got["a_ver"], json!("v1"), "parent step a must use v1: {body}");
    assert_eq!(
        got["c_ver"], json!("v1"),
        "the nested child must share the parent's snapshot (D36): {body}"
    );
}

/// D35: a streaming step's timeout_secs cap fires mid-stream → Error frame +
/// close (reason=deadline), chunks before the cap still flow.
#[serial]
#[tokio::test]
async fn test_audit_stream_e5_stream_timeout_error_frame() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_e5_stream"]).await;

    let body = sse_post(&base, "/v2/models/ens_e5_stream/events", json!({"text": "a b c d e f g h"}))
        .await
        .expect("stream must open (200) before the step cap fires");
    assert!(body.contains("token"), "chunks before the cap must flow: {body}");
    assert!(
        body.contains("error") && !body.contains("[DONE]"),
        "step timeout must close with an Error frame, no [DONE]: {body}"
    );
}

/// D35 pre-open exhaustion: the tail model is not preloaded (autoload at
/// request) and its 0.01s step budget cannot survive the autoload → 504
/// (§4.4 deadline row, the status-code window is still open).
#[serial]
#[tokio::test]
async fn test_audit_stream_e5_autoload_timeout_504() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_e5_autoload"]).await;

    let err = sse_post(&base, "/v2/models/ens_e5_autoload/events", json!({"text": "hello"}))
        .await
        .expect_err("a step budget that cannot cover the autoload must fail");
    assert_eq!(err, reqwest::StatusCode::GATEWAY_TIMEOUT, "must be 504 (deadline row)");
}

/// D35 transport parity (gRPC server-streaming): the tail step's timeout
/// fires mid-stream — chunks flow first, then the tonic stream ends with
/// DEADLINE_EXCEEDED (the same terminal shape as a client overall deadline).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_stream_e5_stream_timeout_grpc_deadline_exceeded() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::StreamInferRequest;
    use std::collections::HashMap;

    let (base, grpc_port, _guard, _repo) = boot_server_grpc("").await;
    wait_ready_all(&base, &["ens_e5_stream"]).await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let resp = client
        .stream_infer(StreamInferRequest {
            model_name: "ens_e5_stream".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(r#"{"text":"a b c d e f g h"}"#),
            headers: HashMap::new(),
            sequence_id: None,
        })
        .await
        .expect("StreamInfer must open");
    let mut stream = resp.into_inner();
    let mut saw_token = false;
    loop {
        match stream.message().await {
            Ok(Some(chunk)) => {
                if let Ok(v) = serde_json::from_slice::<Value>(&chunk.data) {
                    if v.get("token").is_some() {
                        saw_token = true;
                    }
                }
            }
            Ok(None) => panic!("stream must end with a deadline error, not clean EOF"),
            Err(status) => {
                assert_eq!(
                    status.code(),
                    tonic::Code::DeadlineExceeded,
                    "mid-stream step timeout must surface as DEADLINE_EXCEEDED: {status}"
                );
                break;
            }
        }
    }
    assert!(saw_token, "chunks must flow before the step cap fires");
}

/// D35 transport parity (WS): the tail step's timeout fires mid-stream —
/// chunks flow first, then the error message + close (§4.4: 开流后失败 →
/// Error 收口; the ws sink drop ends the session).
#[tokio::test]
#[serial]
async fn test_audit_stream_e5_stream_timeout_ws_error_close() {
    use futures::{SinkExt, StreamExt};
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_e5_stream"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ens_e5_stream/stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"text":"a b c d e f g h"}"#.into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"close"}"#.into(),
    ))
    .await
    .unwrap();

    let mut saw_token = false;
    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b)))) => {
                if b.windows(5).any(|w| w == b"token") {
                    saw_token = true;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if t.contains("deadline exceeded") {
                    saw_error = true;
                    break;
                }
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))))
            | Ok(Some(Err(_)))
            | Ok(None)
            | Err(_) => break,
            _ => {}
        }
    }
    assert!(saw_token, "chunks must flow before the step cap fires");
    assert!(saw_error, "mid-stream step timeout must close with an error message");
}

/// D35 transport parity (h2 bidi): the tail step's timeout fires mid-stream
/// — LPM Data chunks flow first, then an Error frame + close (§4.4).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_stream_e5_stream_timeout_h2_bidi_error_frame() {
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;
    use futures::StreamExt;

    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_e5_stream"]).await;

    // Single Open frame (single frame = the original value, D17) + body EOF
    // half-close → trigger (D33) → the tail stream flows down.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, reqwest::Error>>(16);
    tx.send(Ok(lpm::encode_frame(&pb::BidiChunk {
        stream_id: "t".into(),
        payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
            initial_data: bytes::Bytes::from(r#"{"text":"a b c d e f g h"}"#),
            ..Default::default()
        })),
    })))
    .await
    .unwrap();
    drop(tx); // body EOF → half-close → trigger (D33)
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = reqwest::Body::wrap_stream(body_stream);

    let resp = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap()
        .post(format!("{}/v2/models/ens_e5_stream/bidi", base))
        .header("content-type", "application/x-lite-bidi")
        .body(body)
        .send()
        .await
        .expect("h2 bidi POST");
    assert_eq!(resp.status(), 200, "h2 bidi must open");

    let mut saw_token = false;
    let mut saw_error = false;
    let mut buf = bytes::BytesMut::new();
    let mut body = resp.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    'outer: while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), body.next()).await {
            Ok(Some(Ok(bytes))) => {
                buf.extend_from_slice(&bytes);
                while let Ok(Some(chunk)) = lpm::try_decode_frame(&mut buf) {
                    match chunk.payload {
                        Some(pb::bidi_chunk::Payload::Data(d)) => {
                            if let Ok(v) = serde_json::from_slice::<Value>(&d.data) {
                                if v.get("token").is_some() {
                                    saw_token = true;
                                }
                            }
                        }
                        Some(pb::bidi_chunk::Payload::Error(e)) => {
                            assert!(
                                e.message.contains("deadline exceeded"),
                                "Error frame must name the deadline: {}",
                                e.message
                            );
                            saw_error = true;
                            break 'outer;
                        }
                        Some(pb::bidi_chunk::Payload::Close(_)) => break 'outer,
                        _ => {}
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(saw_token, "chunks must flow before the step cap fires");
    assert!(saw_error, "mid-stream step timeout must send an Error frame");
}

// ---------------------------------------------------------------------------
// Audit (batch 3) defect reproductions — these FAIL on the current code and
// pin the fixes.
// ---------------------------------------------------------------------------

/// E1: same-layer sibling steps calling the SAME child ensemble is legal
/// fan-out, not recursion. The shared flat ancestor chain flags the second
/// sibling while the first's child run (~2s) is still in flight — a spurious
/// "recursion detected" 400 on a valid DAG (same shape as D30 batch elements
/// sharing one snapshot).
#[serial]
#[tokio::test]
async fn test_audit_stream_e1_sibling_same_child_not_recursion() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_sibling_race", "ens_drift_child"]).await;

    let client = reqwest::Client::new();
    // Several rounds: round 0 cold-loads the child plan (both siblings check
    // before either pushes); warm rounds race check-vs-push with the child
    // run held open ~2s — on current code they 400 with "recursion".
    for round in 0..4 {
        let resp = client
            .post(format!("{}/v2/models/ens_sibling_race/infer", base))
            .header("Content-Type", "application/json")
            .json(&json!({"text": "hello"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "round {round}: sibling fan-out to the same child must succeed, got {:?}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
}

/// E5 × E1: a nested-ensemble step's timeout_secs must bound the CHILD run
/// (the local timeout wrap, §5-E5 — nested execution IS the step's
/// execution). The child's sub-model sleeps 2s; the 0.3s step cap must fire
/// 504. Running the full 2s and returning 200 silently drops the cap.
#[serial]
#[tokio::test]
async fn test_audit_stream_e5_nested_step_timeout_enforced() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_e5_nested_timeout"]).await;

    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    let resp = client
        .post(format!("{}/v2/models/ens_e5_nested_timeout/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::GATEWAY_TIMEOUT,
        "step timeout must bound the nested child run (504), got {:?}",
        resp.status()
    );
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "the step cap must fire at ~0.3s, not after the 2s child run: {:?}",
        start.elapsed()
    );
}

/// m4: the step-latency metric's version label must normalize UNRESOLVED
/// versions ("latest"/omitted) to "latest" — recording the resolved active
/// value makes the label set grow with every active drift (model × step ×
/// version cardinality explosion, the exact risk m4 exists to prevent).
#[serial]
#[tokio::test]
async fn test_audit_stream_e4_metric_version_label_normalized_to_latest() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_drift"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_drift/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let metrics = client
        .get(format!("{}/metrics", base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // Scope to ens_drift's version-OMITTED steps (model="drift"; the fin
    // step pins version "1" explicitly and may legitimately carry "1").
    let drift_series: Vec<&str> = metrics
        .lines()
        .filter(|l| {
            l.starts_with("liteserver_ensemble_step_latency_seconds")
                && l.contains(r#"ensemble="ens_drift""#)
                && l.contains(r#"model="drift""#)
        })
        .collect();
    assert!(!drift_series.is_empty(), "ens_drift drift-step series must exist");
    for line in &drift_series {
        assert!(
            line.contains(r#"version="latest""#),
            "unresolved step versions must normalize to \"latest\" (m4): {line}"
        );
    }
}

// ---------------------------------------------------------------------------
// Batch 4 (E6): on_error skip + retries
// ---------------------------------------------------------------------------

/// E6 (D5): a skip step that fails leaves the DAG intact — the sibling
/// still produces the output (the skip step is absent from the context).
#[serial]
#[tokio::test]
async fn test_audit_e6_skip_step_sibling_continues() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_skip"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_skip/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "still works"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "skip must not fail the DAG");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("still works"),
        "sibling step must produce the output despite the skip: {body}"
    );
}

/// E6: a flaky unary step (500 on first call) recovers with retries.
#[serial]
#[tokio::test]
async fn test_audit_e6_unary_retry_recovers() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_retry"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_retry/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "recovered"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "retry must recover from the first 500"
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("recovered"), "second attempt output: {body}");
}

/// E6: retries default off — the same flaky model without retries is 500.
#[serial]
#[tokio::test]
async fn test_audit_e6_retry_default_off_is_500() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_retry_off"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_retry_off/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "retries: 0 (default) must keep the historical single-attempt 500"
    );
}

/// E6: retry exhaustion — an always-500 model with retries: 2 still ends 500.
#[serial]
#[tokio::test]
async fn test_audit_e6_retry_exhausted_is_500() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_retry_exhaust"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_retry_exhaust/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "exhausted retries must surface the final 500"
    );
}

/// E6 (D35): a streaming step whose first build attempt errors on the first
/// frame rebuilds the stream — chunks arrive on the second attempt.
#[serial]
#[tokio::test]
async fn test_audit_e6_stream_build_retry_recovers() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream_retry"]).await;

    let body = sse_post(&base, "/v2/models/ens_stream_retry/events", json!({"text": "hello world"}))
        .await
        .expect("stream must open (200)");
    assert!(body.contains(r#""token":"hello""#), "chunks must arrive after rebuild: {body}");
    assert!(body.contains(r#""token":"world""#), "second chunk: {body}");
    assert!(body.contains("[DONE]"), "clean [DONE]: {body}");
    assert!(!body.contains("error"), "no error frame expected: {body}");
}

/// E6 (D35): build-window retry exhaustion — an always-failing first frame
/// surfaces as an Error frame + close (no [DONE]).
#[serial]
#[tokio::test]
async fn test_audit_e6_stream_retry_exhausted_error_frame() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream_retry_exhaust"]).await;

    let body = sse_post(&base, "/v2/models/ens_stream_retry_exhaust/events", json!({"text": "hello"}))
        .await
        .expect("stream must open (200) — first-frame failure is in-stream");
    assert!(
        body.contains("error") && !body.contains("[DONE]"),
        "exhausted build retries must close with an Error frame, no [DONE]: {body}"
    );
}

/// E6 (D35): once a chunk commits the stream, retries close — a mid-stream
/// failure is NOT replayed (no doubled chunks, Error frame after the
/// committed prefix).
#[serial]
#[tokio::test]
async fn test_audit_e6_stream_committed_no_replay() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_stream_committed"]).await;

    let body = sse_post(&base, "/v2/models/ens_stream_committed/events", json!({"text": "a b c d"}))
        .await
        .expect("stream must open (200)");
    assert_eq!(
        body.matches(r#""token":"a""#).count(),
        1,
        "committed chunks must not replay after a mid-stream failure: {body}"
    );
    assert!(
        body.contains("error") && !body.contains("[DONE]"),
        "mid-stream failure must close with an Error frame: {body}"
    );
}

// ---------------------------------------------------------------------------
// Batch 4 (MIMO①): KServe envelope wire (D31), LSBE-1 (D32), D8 binary
// passthrough chain, R18/R19, D33 bidi single-envelope-frame trigger
// ---------------------------------------------------------------------------

/// MIMO happy path: a KServe envelope with named inputs (defaults filled)
/// runs the DAG — HTTP unary.
#[serial]
#[tokio::test]
async fn test_audit_mimo_envelope_happy_http_unary() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mimo"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_mimo/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"inputs": [{"name": "text", "data": {"text": "hello envelope"}}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "envelope request must run the DAG");
    let body = resp.text().await.unwrap();
    assert!(body.contains("hello envelope"), "DAG output must reflect the named input: {body}");
}

/// R18: a missing required input → 400 (named-input contract).
#[serial]
#[tokio::test]
async fn test_audit_mimo_missing_required_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mimo"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_mimo/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"inputs": [{"name": "sys", "data": "x"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "missing required input must be 400 (R18)");
}

/// R18: an unknown input name → 400.
#[serial]
#[tokio::test]
async fn test_audit_mimo_unknown_input_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mimo"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_mimo/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"inputs": [
            {"name": "text", "data": {"text": "x"}},
            {"name": "nope", "data": 1}
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "unknown input must be 400 (R18)");
}

/// D8: the binary passthrough chain — a TritonBinary envelope (JSON head +
/// binary tail) flows image → vis_enc → cropper → classifier whole-value
/// along declared binary aliases.
#[serial]
#[tokio::test]
async fn test_audit_mimo_binary_passthrough_chain() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mimo_bin"]).await;

    let head = json!({"id": "r1", "inputs": [
        {"name": "image", "parameters": {"binary_data_size": 3}}
    ]});
    let head_bytes = serde_json::to_vec(&head).unwrap();
    let mut body = head_bytes.clone();
    body.extend_from_slice(b"\x00\x01\x02");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_mimo_bin/infer", base))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", head_bytes.len().to_string())
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "binary passthrough chain must run: {:?}",
        resp.text().await
    );
    // Re-send to read the body (consumed above on failure path only — use a
    // fresh request to assert the payload).
    let mut body2 = head_bytes.clone();
    body2.extend_from_slice(b"\x00\x01\x02");
    let resp2 = client
        .post(format!("{}/v2/models/ens_mimo_bin/infer", base))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", head_bytes.len().to_string())
        .body(body2)
        .send()
        .await
        .unwrap();
    let v: Value = resp2.json().await.unwrap();
    assert_eq!(v["label"], json!("cat"), "classifier output: {v}");
    assert_eq!(v["bytes_in"], json!(3), "the original 3 bytes must reach the tail: {v}");
}

/// R4/D13: the optional input absent → the conditional step (which would
/// 500) is skipped; the sibling still produces the output.
#[serial]
#[tokio::test]
async fn test_audit_mimo_conditional_skip() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mimo_cond"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_mimo_cond/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"inputs": [{"name": "text", "data": {"text": "no opt today"}}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "conditional skip must not fail the DAG");
    let body = resp.text().await.unwrap();
    assert!(body.contains("no opt today"), "sibling output: {body}");
}

/// R19/D14: a legacy ensemble receiving a top-level `$inputs` key → 400
/// (reserved namespace).
#[serial]
#[tokio::test]
async fn test_audit_mimo_r19_reserved_namespace_400() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_unary"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_unary/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"$inputs": [{"name": "a", "data": 1}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "'$inputs' on a legacy ensemble must be 400 (R19/D14)"
    );
}

/// D32: gRPC unary with an LSBE-1 container (binary envelope) runs the DAG;
/// a malformed container is InvalidArgument.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_mimo_grpc_lsbe1_container() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let (base, grpc_port, _guard, _repo) = boot_server_grpc("").await;
    wait_ready_all(&base, &["ens_mimo"]).await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);

    // Happy: LSBE-1 container with a json-only envelope (no tail).
    let head = serde_json::to_vec(&json!({"inputs": [{"name": "text", "data": "grpc envelope"}]})).unwrap();
    let mut blob = Vec::new();
    blob.extend_from_slice(b"LSB1");
    blob.extend_from_slice(&(head.len() as u64).to_le_bytes());
    blob.extend_from_slice(&head);
    let resp = client
        .infer(tonic::Request::new(InferRequest {
            model_name: "ens_mimo".into(),
            version: "1".into(),
            data: bytes::Bytes::from(blob),
            ..Default::default()
        }))
        .await
        .expect("LSBE-1 gRPC unary must succeed");
    let v: Value = serde_json::from_slice(resp.into_inner().data.as_ref()).unwrap();
    assert_eq!(v["echo"], json!("grpc envelope"), "gRPC envelope output: {v}");

    // Malformed container (magic mismatch) → InvalidArgument.
    let resp = client
        .infer(tonic::Request::new(InferRequest {
            model_name: "ens_mimo".into(),
            version: "1".into(),
            data: bytes::Bytes::from_static(b"XXXX-not-a-container"),
            ..Default::default()
        }))
        .await;
    match resp {
        Err(status) => assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "malformed LSBE-1 must be InvalidArgument, got {status:?}"
        ),
        Ok(_) => panic!("malformed container must fail"),
    }
}

/// D33: a declared-inputs ensemble executes on the FIRST WS envelope frame
/// — no close frame needed; chunks stream back immediately.
#[tokio::test]
#[serial]
async fn test_audit_mimo_ws_bidi_envelope_frame_immediate() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mimo_stream"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{http_port}/v2/models/ens_mimo_stream/stream");
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");

    // Single envelope text frame — D33: executes immediately, no close frame.
    let envelope = json!({"inputs": [{"name": "text", "data": "hi ws"}]});
    ws.send(Message::Text(envelope.to_string())).await.unwrap();

    let mut tokens: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Value = serde_json::from_str(&t).ok().unwrap_or(Value::Null);
                if let Some(tok) = v.get("token").and_then(|t| t.as_str()) {
                    tokens.push(tok.to_string());
                }
            }
            // The WS writer emits chunks as Binary frames (existing
            // behaviour — parity with the aggregation tests).
            Ok(Some(Ok(Message::Binary(b)))) => {
                if let Ok(v) = serde_json::from_slice::<Value>(&b) {
                    if let Some(tok) = v.get("token").and_then(|t| t.as_str()) {
                        tokens.push(tok.to_string());
                    }
                }
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => break,
            Ok(Some(Ok(_))) => {}
            Err(_) => break,
        }
    }
    assert_eq!(
        tokens,
        vec!["hi", "ws"],
        "the envelope frame must trigger execution without a close frame: {tokens:?}"
    );
}

/// MIMO② (D10): json alias projection — the declared alias projects
/// `$.pre` out of the step response and feeds the downstream step.
#[serial]
#[tokio::test]
async fn test_audit_mimo2_json_alias_projection() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_mimo_json_alias"]).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_mimo_json_alias/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"inputs": [{"name": "text", "data": "via alias"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "json alias DAG must run");
    let body = resp.text().await.unwrap();
    assert!(body.contains("via alias"), "projected alias must feed the tail: {body}");
}

/// E7 (D31/D5): multi-sink response — the KServe envelope: JSON head
/// outputs[] (answer json, thumb binary with binary_data_size, score null
/// after the skip) + binary tail split via
/// Inference-Header-Content-Length.
#[serial]
#[tokio::test]
async fn test_audit_e7_multisink_envelope_response() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_multisink"]).await;

    let image_bytes: &[u8] = b"\x00\x01\x02";
    let head = json!({"id": "r1", "inputs": [
        {"name": "text", "data": "multi sink"},
        {"name": "image", "parameters": {"binary_data_size": 3}}
    ]});
    let head_bytes = serde_json::to_vec(&head).unwrap();
    let mut body = head_bytes.clone();
    body.extend_from_slice(image_bytes);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ens_multisink/infer", base))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", head_bytes.len().to_string())
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "multi-sink must succeed");
    let head_len: usize = resp
        .headers()
        .get("inference-header-content-length")
        .expect("envelope response must carry the head-length header")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let full = resp.bytes().await.unwrap();
    let resp_head: Value = serde_json::from_slice(&full[..head_len]).unwrap();
    let tail = &full[head_len..];
    assert_eq!(tail, image_bytes, "binary alias must land in the tail verbatim");
    assert_eq!(resp_head["model_name"], json!("ens_multisink"));
    let outs = resp_head["outputs"].as_array().unwrap();
    assert_eq!(outs.len(), 3, "{outs:?}");
    assert_eq!(outs[0], json!({"name": "answer", "data": {"pre": "multi sink"}}));
    assert_eq!(
        outs[1],
        json!({"name": "thumb", "parameters": {"binary_data_size": 3}}),
        "thumb size = the input bytes (marker decode round-trip)"
    );
    assert_eq!(outs[2], json!({"name": "score", "data": null}), "skip alias → null (D5)");
}

/// E7 (D32): gRPC unary multi-sink — the LSBE-1 container carries the
/// envelope head + binary tail (InferResponse has no headers map).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_e7_multisink_grpc_lsbe1_response() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let (base, grpc_port, _guard, _repo) = boot_server_grpc("").await;
    wait_ready_all(&base, &["ens_multisink"]).await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);

    // Request: LSBE-1 envelope — text (json) + image (binary tail).
    let head = serde_json::to_vec(&json!({"inputs": [
        {"name": "text", "data": "grpc sink"},
        {"name": "image", "parameters": {"binary_data_size": 3}}
    ]})).unwrap();
    let mut blob = Vec::new();
    blob.extend_from_slice(b"LSB1");
    blob.extend_from_slice(&(head.len() as u64).to_le_bytes());
    blob.extend_from_slice(&head);
    blob.extend_from_slice(b"\x01\x02\x03");
    let resp = client
        .infer(tonic::Request::new(InferRequest {
            model_name: "ens_multisink".into(),
            version: "1".into(),
            data: bytes::Bytes::from(blob),
            ..Default::default()
        }))
        .await
        .expect("gRPC multi-sink must succeed");
    let data = resp.into_inner().data;
    let (resp_head, tail) = lite_server::ensemble::split_envelope(&data)
        .expect("response must be an LSBE-1 container");
    assert_eq!(
        tail.as_deref(),
        Some(&b"\x01\x02\x03"[..]),
        "binary alias must land in the container tail verbatim"
    );
    let outs = resp_head["outputs"].as_array().unwrap();
    assert_eq!(outs[0], json!({"name": "answer", "data": {"pre": "grpc sink"}}));
    assert_eq!(outs[2], json!({"name": "score", "data": null}), "skip alias → null (D5)");
}

/// E8-1 (D38/D22): x-lite-dag selects the DAG set over HTTP — no header =
/// "default", a named set switches the output, an unknown name or malformed
/// value is 400 (no silent default fallback).
#[serial]
#[tokio::test]
async fn test_audit_e8_dag_selection_http_header() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_dags"]).await;

    let client = reqwest::Client::new();
    // No header → the default set.
    let resp = client
        .post(format!("{}/v2/models/ens_dags/infer", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains(r#""pre""#), "default set must run pre: {body}");

    // x-lite-dag: fast → the fast set.
    let resp = client
        .post(format!("{}/v2/models/ens_dags/infer", base))
        .header("Content-Type", "application/json")
        .header("x-lite-dag", "fast")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains(r#""echo""#), "fast set must run echo: {body}");

    // Unknown dag → 400 (D22: never a silent default fallback).
    let resp = client
        .post(format!("{}/v2/models/ens_dags/infer", base))
        .header("Content-Type", "application/json")
        .header("x-lite-dag", "nope")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "unknown dag must be 400");

    // Malformed value → 400.
    let resp = client
        .post(format!("{}/v2/models/ens_dags/infer", base))
        .header("Content-Type", "application/json")
        .header("x-lite-dag", "bad!")
        .json(&json!({"text": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "malformed selector must be 400 (D22)");
}

/// E8-1 (D38): the gRPC metadata channel carries the same `x-lite-dag` key.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_e8_dag_selection_grpc_metadata() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let (base, grpc_port, _guard, _repo) = boot_server_grpc("").await;
    wait_ready_all(&base, &["ens_dags"]).await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);

    let mut req = tonic::Request::new(InferRequest {
        model_name: "ens_dags".into(),
        version: "1".into(),
        data: bytes::Bytes::from_static(br#"{"text": "x"}"#),
        ..Default::default()
    });
    req.metadata_mut().insert("x-lite-dag", "fast".parse().unwrap());
    let resp = client.infer(req).await.expect("dag-selected gRPC unary must succeed");
    let v: Value = serde_json::from_slice(resp.into_inner().data.as_ref()).unwrap();
    assert!(v.get("echo").is_some(), "fast set must run echo over gRPC: {v}");
}

/// E8-2 (R16): when conditions — a when-false step is skipped (its outputs
/// alias → null, D5); a when-true step RUNS (observable as the sub-model's
/// 500); $request.dag mirrors the selected set name.
#[serial]
#[tokio::test]
async fn test_audit_e8_when_skip_and_dag_condition() {
    let (base, _guard, _repo) = boot_server("").await;
    wait_ready_all(&base, &["ens_when"]).await;

    let client = reqwest::Client::new();
    let envelope = json!({"inputs": [{"name": "mode", "data": "hello when"}]});

    // Default set, no opt: dag_path RUNS (dag == 'default'), maybe skipped
    // (absent != null → false) → answer present, alias null.
    let resp = client
        .post(format!("{}/v2/models/ens_when/infer", base))
        .header("Content-Type", "application/json")
        .json(&envelope)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "when-false steps must skip cleanly");
    let v: Value = resp.json().await.unwrap();
    let outs = v["outputs"].as_array().unwrap();
    assert_eq!(outs[0], json!({"name": "answer", "data": {"echo": "hello when"}}));
    assert_eq!(outs[1], json!({"name": "maybe_score", "data": null}), "when-false alias → null (D5)");

    // x-lite-dag: fast → the fast SET answers (pre, a distinct output).
    let resp = client
        .post(format!("{}/v2/models/ens_when/infer", base))
        .header("Content-Type", "application/json")
        .header("x-lite-dag", "fast")
        .json(&json!({"inputs": [{"name": "mode", "data": "hello when"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "fast set must run");
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v, json!({"pre": "hello when"}), "fast set answers with pre: {v}");

    // opt present → `maybe` RUNS → pre_5xx 500 (observable run proof).
    let with_opt = json!({"inputs": [
        {"name": "mode", "data": "hello when"},
        {"name": "opt", "data": 1}
    ]});
    let resp = client
        .post(format!("{}/v2/models/ens_when/infer", base))
        .header("Content-Type", "application/json")
        .json(&with_opt)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "a when-true step must RUN (observable 500)"
    );
}
