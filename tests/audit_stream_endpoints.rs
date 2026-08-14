//! /audit 举证测试——ensemble 流式端点集成层(ensemble-streaming-plan
//! §4.3/§4.4、D17/D18/D21/D33、P10/D40、D35)。
//! 覆盖对象:
//!   S1  P10 信号量在 open_tail_stream 之后才 acquire → 429 时 worker 流已开、
//!       无 cancel,孤儿流泄漏(exec.rs:467-473 / 426-435);
//!   S2  WS 聚合 idle/deadline 超时的 close 契约(stream.rs:1328-1341 vs §4.4
//!       「close 1011 + {error:{code,message}}」);
//!   S3  WS 多轮违规的 close 契约(stream.rs:1481-1487/1594-1599/1713 vs §4.4
//!       「close 1003 + 错误 JSON」);
//!   S4  gRPC bidi 声明式(envelope)模型多轮帧只 warn 不 InvalidArgument
//!       (grpc/rpc/bidi.rs:477-486 vs §4.3/§4.4);
//!   S5  h2 bidi 声明式模型多轮帧静默丢弃、无错误帧
//!       (http/handlers/bidi.rs:229-241 vs §4.4「h2 同语义错误帧」)。
//! 真 server 子进程 + 模型夹具(与 tests/audit_ensemble_stream.rs 同构);
//! 端口段 23100-23199。

use serde_json::{json, Value};
use serial_test::serial;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

// ---------------------------------------------------------------------------
// Harness(与 tests/audit_ensemble_stream.rs 同构;test target 间不共享代码)
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

/// 端口段 23100-23199(审计任务分配段)。
fn next_test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(23100);
    let p = NEXT.fetch_add(1, Ordering::Relaxed);
    assert!(p < 23200, "audit port range 23100-23199 exhausted");
    p
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

fn http_client() -> reqwest::Client {
    // macOS 系统代理会把 loopback 送进代理 —— 显式禁用。
    reqwest::Client::builder().no_proxy().build().unwrap()
}

async fn wait_for_server(port: u16, timeout_secs: u64) {
    let client = http_client();
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
    let client = http_client();
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
// Fixtures(最小集,每测试独立 repo)
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

/// Unary pre-layer: {"pre": <text>}.
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

/// Fast streaming tail: one chunk per word.
fn write_tail(repo: &std::path::Path) {
    write_model_py(
        repo,
        "tail",
        r#"import time
from lite_server import LitAPI


class TailAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        for w in request.split():
            time.sleep(0.02)
            yield {"token": w}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    );
}

/// Slow streaming tail (0.5s/chunk) — keeps the downstream stream open while
/// the test injects protocol violations.
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

/// Slow streaming tail that LOGS every stream invocation (one line per
/// StreamOpen) — the S1 orphan-stream witness.
fn write_tail_mark(repo: &std::path::Path, log_path: &std::path::Path) {
    let py = r#"import time
from lite_server import LitAPI


class TailMarkAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("pre", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        with open("__LOG__", "a") as f:
            f.write("open\n")
        for w in request.split():
            time.sleep(0.4)
            yield {"token": w}

    async def encode_response(self, output, ctx=None):
        return output
"#
    .replace("__LOG__", &log_path.display().to_string());
    write_model_py(repo, "tail_mark", &py);
}

/// S1 fixture: pre → tail_mark (stream).
const ENS_MARK_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: tail_mark
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

/// S2/S3 fixture (undeclared inputs → D17 aggregation on WS): pre → tail_slow.
const ENS_WS_SLOW_YAML: &str = r#"ensemble:
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

/// S4/S5 fixture (declared inputs → D33 envelope trigger): pre → tail_slow.
const ENS_MIMO_SLOW_YAML: &str = r#"ensemble:
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
      model: tail_slow
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

fn write_server_yaml(
    repo: &std::path::Path,
    http_port: u16,
    grpc: Option<u16>,
    extra: &str,
    models: &[&str],
) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lite-server-audit-sep-yaml-{}-{}",
        std::process::id(),
        http_port
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("server.yaml");
    let (grpc_port_line, grpc_block) = match grpc {
        Some(p) => (format!("  grpc_port: {p}\n"), "grpc:\n  enabled: true\n\n".to_string()),
        None => (String::new(), String::new()),
    };
    let load_list = models
        .iter()
        .map(|m| format!("    - {m}\n"))
        .collect::<String>();
    std::fs::write(
        &path,
        format!(
            "server:\n  http_port: {http_port}\n{grpc_port_line}  timeout: 30.0\n{extra}\n\n\
             {grpc_block}\
             model_repository:\n  path: {}\n\n\
             orchestration:\n  control_mode: explicit\n  load_models:\n{load_list}",
            repo.display()
        ),
    )
    .unwrap();
    path
}

/// Boot a server with a minimal fixture repo; returns (base, grpc_port, guard, repo).
async fn boot_minimal(
    extra: &str,
    grpc: bool,
    models: &[&str],
) -> (String, Option<u16>, ServerGuard, std::path::PathBuf) {
    let http_port = next_test_port();
    let grpc_port = if grpc { Some(next_test_port()) } else { None };
    kill_stale_on_port(http_port);
    if let Some(p) = grpc_port {
        kill_stale_on_port(p);
    }
    let repo = std::env::temp_dir().join(format!(
        "lite-server-audit-sep-{}-{}",
        std::process::id(),
        http_port
    ));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();
    for m in models {
        match *m {
            "pre" => write_pre(&repo),
            "tail" => write_tail(&repo),
            "tail_slow" => write_tail_slow(&repo),
            "tail_mark" => write_tail_mark(&repo, &repo.join("tail_mark.log")),
            "ens_mark" => write_ensemble(&repo, "ens_mark", ENS_MARK_YAML),
            "ens_ws_slow" => write_ensemble(&repo, "ens_ws_slow", ENS_WS_SLOW_YAML),
            "ens_mimo_slow" => write_ensemble(&repo, "ens_mimo_slow", ENS_MIMO_SLOW_YAML),
            other => panic!("unknown fixture {other}"),
        }
    }
    let server_yaml = write_server_yaml(&repo, http_port, grpc_port, extra, models);
    let guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    (
        format!("http://127.0.0.1:{}", http_port),
        grpc_port,
        guard,
        repo,
    )
}

// ---------------------------------------------------------------------------
// S1: P10 permit acquired AFTER the tail worker stream opens → a 429-rejected
// request has already opened (and never cancels) a sub-model worker stream.
// ---------------------------------------------------------------------------

/// P10 (D40/§6-P10 + exec.rs:463-473): with max_concurrent_streaming_dags = 1,
/// request A holds the only slot; request B is rejected 429 — but B's
/// StreamOpen already reached the tail worker (the permit is acquired after
/// open_tail_stream), and no StreamCancel follows (the EnsembleStream is
/// dropped on the `?` path). Witness: tail_mark logs one line per worker
/// stream invocation; a correct "reject BEFORE open" never touches the worker.
#[tokio::test]
#[serial]
async fn test_audit_p10_reject_after_open_leaks_worker_stream() {
    let (base, _gp, _guard, repo) =
        boot_minimal("  max_concurrent_streaming_dags: 1", false, &["pre", "tail_mark", "ens_mark"])
            .await;
    wait_ready_all(&base, &["pre", "tail_mark", "ens_mark"]).await;
    let log_path = repo.join("tail_mark.log");

    let client = http_client();
    // A: 4 chunks × 0.4s ≈ 1.6s — holds the only P10 slot.
    let first = client
        .post(format!("{}/v2/models/ens_mark/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "one two three four"}))
        .send()
        .await
        .expect("first stream must open");
    assert_eq!(first.status(), reqwest::StatusCode::OK, "first stream opens");

    // Let A acquire the slot and start streaming.
    sleep(Duration::from_millis(700)).await;

    // B: must be rejected 429 — and must NOT open a worker stream.
    let second = client
        .post(format!("{}/v2/models/ens_mark/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "five six"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        second.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "P10 exhausted capacity must reject with 429"
    );

    // Let any leaked open settle, then count worker-side stream invocations.
    sleep(Duration::from_millis(500)).await;
    let opens = std::fs::read_to_string(&log_path)
        .unwrap_or_default()
        .lines()
        .filter(|l| *l == "open")
        .count();
    assert_eq!(
        opens, 1,
        "a 429-rejected streaming DAG must never open a sub-model worker stream \
         (permit must be acquired BEFORE open_tail_stream; the opened stream is \
         orphaned — no StreamCancel is sent on the error path)"
    );

    // Drain A so the server tears down cleanly.
    let _ = first.text().await;
}

// ---------------------------------------------------------------------------
// S2: WS aggregation idle timeout close contract (§4.4「聚合期 idle 超时」行:
// close 1011 + {error:{code,message}})。
// ---------------------------------------------------------------------------

/// §4.4 + 注②(close code 为契约): an aggregating client that goes silent is
/// reclaimed after decoupled_idle_timeout_secs — the server must send the
/// contractual error JSON `{error:{code,message}}` and close with 1011.
/// Current code sends `{"error": "<string>"}` (no code) and a default
/// (code-less) close frame (stream.rs:1328-1341).
#[tokio::test]
#[serial]
async fn test_audit_ws_aggregation_idle_timeout_close_contract() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (base, _gp, _guard, _repo) = boot_minimal(
        "  decoupled_idle_timeout_secs: 1.0",
        false,
        &["pre", "tail_slow", "ens_ws_slow"],
    )
    .await;
    wait_ready_all(&base, &["pre", "tail_slow", "ens_ws_slow"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{http_port}/v2/models/ens_ws_slow/stream");
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");

    // One data frame, then silence — the aggregation never gets its close
    // trigger, so the always-on idle budget must reclaim it (~1s).
    ws.send(Message::Text(r#"{"text":"hi"}"#.into())).await.unwrap();

    let mut error_json: Option<Value> = None;
    let mut close_code: Option<u16> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(6), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if v.get("error").is_some() {
                        error_json = Some(v);
                    }
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                close_code = frame.map(|f| u16::from(f.code));
                break;
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Ok(Some(Ok(_))) => {}
            Err(_) => break,
        }
    }

    // Close-code deviation proven first (the error-shape deviation was proven
    // in the audit run with the opposite order — both fire independently).
    assert_eq!(
        close_code,
        Some(1011),
        "§4.4 contract: aggregation idle timeout must close with 1011, got {close_code:?}"
    );
    let err = error_json.expect("idle reclaim must send an error JSON before closing");
    assert!(
        err.get("error").and_then(|e| e.get("code")).is_some(),
        "§4.4 contract: the error JSON must be {{error:{{code,message}}}}, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// S3: WS multi-round violation close contract (§4.4「组合不支持/会话式多轮」行:
// close 1003 + {error:{code,message}})。
// ---------------------------------------------------------------------------

/// §4.3/D33 + §4.4: data frames after the aggregation trigger are a
/// session-multi-round violation — contractual close = 1003 with
/// `{error:{code,message}}`. Current code sends `{"error": "<string>"}` and a
/// default close (stream.rs:1481-1487 reader → 1594-1599 writer → 1713 close).
#[tokio::test]
#[serial]
async fn test_audit_ws_multi_round_close_contract() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (base, _gp, _guard, _repo) =
        boot_minimal("", false, &["pre", "tail_slow", "ens_ws_slow"]).await;
    wait_ready_all(&base, &["pre", "tail_slow", "ens_ws_slow"]).await;
    let http_port = base.trim_start_matches("http://127.0.0.1:").parse::<u16>().unwrap();
    let ws_url = format!("ws://127.0.0.1:{http_port}/v2/models/ens_ws_slow/stream");
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect");

    // Data frame → close frame (aggregation trigger, D33) → extra data frame
    // (multi-round violation). The slow tail keeps the downstream open while
    // the violation is processed.
    ws.send(Message::Text(r#"{"text":"one two three four"}"#.into()))
        .await
        .unwrap();
    ws.send(Message::Text(r#"{"type":"close"}"#.into()))
        .await
        .unwrap();
    ws.send(Message::Text(r#"{"text":"extra"}"#.into()))
        .await
        .unwrap();

    let mut error_json: Option<Value> = None;
    let mut close_code: Option<u16> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(8), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if v.get("error").is_some() {
                        error_json = Some(v);
                    }
                }
            }
            Ok(Some(Ok(Message::Binary(_)))) => {} // stream chunks
            Ok(Some(Ok(Message::Close(frame)))) => {
                close_code = frame.map(|f| u16::from(f.code));
                break;
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Ok(Some(Ok(_))) => {}
            Err(_) => break,
        }
    }

    assert_eq!(
        close_code,
        Some(1003),
        "§4.4 contract: session-multi-round must close with 1003, got {close_code:?}"
    );
    let err = error_json.expect("multi-round violation must send an error JSON before closing");
    assert!(
        err.get("error").and_then(|e| e.get("code")).is_some(),
        "§4.4 contract: the error JSON must be {{error:{{code,message}}}}, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// S4: gRPC bidi — frames after the envelope trigger (declared-inputs model)
// must fail with InvalidArgument (§4.3「会话式多轮 → gRPC InvalidArgument」,
// §4.4「组合不支持」行), not be silently dropped with a warn log
// (grpc/rpc/bidi.rs:477-486)。
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_grpc_bidi_multi_round_invalid_argument() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::{bidi_chunk, BidiChunk, BidiData, BidiOpen};

    let (base, grpc_port, _guard, _repo) =
        boot_minimal("", true, &["pre", "tail_slow", "ens_mimo_slow"]).await;
    wait_ready_all(&base, &["pre", "tail_slow", "ens_mimo_slow"]).await;
    let grpc_port = grpc_port.unwrap();

    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("grpc endpoint")
        .connect()
        .await
        .expect("grpc connect");
    let mut client = LiteServerClient::new(channel);

    let (tx, rx) = tokio::sync::mpsc::channel::<BidiChunk>(16);
    // Envelope frame (declared inputs) — D33: executes immediately.
    tx.send(BidiChunk {
        stream_id: "t".into(),
        payload: Some(bidi_chunk::Payload::Open(BidiOpen {
            model_name: "ens_mimo_slow".into(),
            version: "1".into(),
            initial_data: bytes::Bytes::from(
                r#"{"inputs":[{"name":"text","data":"one two three four"}]}"#,
            ),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let resp = client
        .bidi_stream(tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
        .await
        .expect("bidi must open");
    // Multi-round violation: another data frame after the trigger. The slow
    // tail (4 chunks × 0.5s) keeps the response stream open so the server has
    // every chance to reject it in-band.
    sleep(Duration::from_millis(300)).await;
    tx.send(BidiChunk {
        stream_id: "t".into(),
        payload: Some(bidi_chunk::Payload::Data(BidiData {
            data: bytes::Bytes::from(r#"{"text":"extra"}"#),
        })),
    })
    .await
    .unwrap();
    // Keep the request stream alive a moment so the violation is processed
    // mid-stream (not after completion), then half-close.
    sleep(Duration::from_millis(500)).await;
    drop(tx);

    let mut out = resp.into_inner();
    let mut terminal_err: Option<tonic::Code> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(8), out.message()).await {
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => break,
            Ok(Err(status)) => {
                terminal_err = Some(status.code());
                break;
            }
            Err(_) => break,
        }
    }
    assert_eq!(
        terminal_err,
        Some(tonic::Code::InvalidArgument),
        "§4.3/§4.4: frames after the envelope trigger must fail the stream with \
         InvalidArgument (multi-round rejected), got {terminal_err:?}"
    );
}

// ---------------------------------------------------------------------------
// S5: h2 bidi — frames after the envelope trigger (declared-inputs model)
// must produce an error frame (§4.4「组合不支持」行:「h2 同语义错误帧」),
// not die silently with the dropped body stream
// (http/handlers/bidi.rs:229-241)。
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_h2_bidi_multi_round_error_frame() {
    use futures::StreamExt;
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;

    let (base, _gp, _guard, _repo) =
        boot_minimal("", false, &["pre", "tail_slow", "ens_mimo_slow"]).await;
    wait_ready_all(&base, &["pre", "tail_slow", "ens_mimo_slow"]).await;

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, reqwest::Error>>(16);
    // Envelope frame (declared inputs) — D33: executes immediately.
    tx.send(Ok(lpm::encode_frame(&pb::BidiChunk {
        stream_id: "t".into(),
        payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
            initial_data: bytes::Bytes::from(
                r#"{"inputs":[{"name":"text","data":"one two three four"}]}"#,
            ),
            ..Default::default()
        })),
    })))
    .await
    .unwrap();
    let body = reqwest::Body::wrap_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    let resp = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .build()
        .unwrap()
        .post(format!("{}/v2/models/ens_mimo_slow/bidi", base))
        .header("content-type", "application/x-lite-bidi")
        .body(body)
        .send()
        .await
        .expect("h2 bidi POST");
    assert_eq!(resp.status(), 200, "h2 bidi must open");

    // Multi-round violation while the slow tail keeps the response open.
    sleep(Duration::from_millis(300)).await;
    let _ = tx
        .send(Ok(lpm::encode_frame(&pb::BidiChunk {
            stream_id: "t".into(),
            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                data: bytes::Bytes::from(r#"{"text":"extra"}"#),
            })),
        })))
        .await;
    sleep(Duration::from_millis(500)).await;
    drop(tx);

    let mut saw_error_frame = false;
    let mut buf = bytes::BytesMut::new();
    let mut body = resp.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(8), body.next()).await {
            Ok(Some(Ok(bytes))) => {
                buf.extend_from_slice(&bytes);
                while let Ok(Some(chunk)) = lpm::try_decode_frame(&mut buf) {
                    if matches!(chunk.payload, Some(pb::bidi_chunk::Payload::Error(_))) {
                        saw_error_frame = true;
                    }
                    if matches!(chunk.payload, Some(pb::bidi_chunk::Payload::Close(_))) {
                        break;
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(
        saw_error_frame,
        "§4.4 (组合不支持/会话式多轮): h2 bidi must emit an error frame for \
         frames after the envelope trigger — currently they die silently with \
         the dropped body stream"
    );
}
