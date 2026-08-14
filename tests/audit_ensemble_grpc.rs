//! /audit 举证测试——P-ENSEMBLE-GRPC gRPC ensemble 派发面（蓝图 §4.1 + D23，
//! parity 基准 = HTTP do_infer ensemble 分支 src/http/handlers/inference.rs）。
//! 命名 test_audit_<维度>_<场景>；在当前代码上 FAIL，证明缺陷存在；修复后转绿
//! 作为回归锁。仅新增测试，不改实现。

use serde_json::{json, Value};
use serial_test::serial;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

// ---------------------------------------------------------------------------
// Helpers（与 tests/integration_test.rs 同构；test target 间不共享代码）
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

/// 单调端口分配（19600 起，避开 integration_test.rs 的 19000 段）。
fn next_test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(19600);
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

async fn load_model(base: &str, model: &str, version: &str) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/v2/repository/models/{}/versions/{}/load",
            base, model, version
        ))
        .send()
        .await
        .expect("load request failed");
    assert_eq!(resp.status(), 200, "load failed: {:?}", resp.text().await);
    assert!(
        wait_model_ready(base, model, 30).await,
        "model {} did not become ready",
        model
    );
}

#[cfg(unix)]
async fn grpc_tcp_channel(grpc_port: u16) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", grpc_port))
        .expect("grpc endpoint")
        .connect()
        .await
        .expect("grpc connect")
}

// ---------------------------------------------------------------------------
// B1（数据维度）:gRPC ensemble 二进制入口丢弃客户端声明的 content-type
// ---------------------------------------------------------------------------
//
// 缺陷:src/grpc/rpc/infer.rs:100-107 —— JSON 解析失败落 Binary 时硬编码
// "application/octet-stream",完全不读 `req.headers["content-type"]`。parity
// 基准(HTTP do_infer ensemble 分支,inference.rs:187-189):Raw(bytes, ct) 保留
// 请求的真实 Content-Type 并随 step_headers 传给子模型 worker(worker 据此分流
// JSON/裸字节,且用户 hook 可读 ctx.meta.headers["content-type"])。gRPC 的 proto
// headers map 是其声明 CT 的唯一信道(非 ensemble 路径会原样转发给 worker),
// ensemble 分支却把它丢了。

/// 回显子模型:把 worker 实际看到的 content-type 放进 {"out": ...} 返回。
fn write_ct_echo_model(repo: &std::path::Path) {
    let dir = repo.join("ct_echo/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class CtEchoAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return ctx.meta.headers.get("content-type", "")

    def predict(self, x):
        return {"out": x}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// 单步 ensemble:$request 整体引用 → 二进制原样透传 + content-type 进 step_headers。
fn write_ct_ensemble(repo: &std::path::Path) {
    let dir = repo.join("ens_ct/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: only
      model: ct_echo
      version: "1"
      inputs:
        x: "$request"
"#,
    )
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_data_grpc_ensemble_binary_input_drops_declared_content_type() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-audit-ensct-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_ct_echo_model(&repo);
    write_ct_ensemble(&repo);

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-audit-ensct-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            metrics_port = metrics_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "ct_echo", "1").await;
    load_model(&base, "ens_ct", "1").await;

    // 非 JSON 二进制负载 + 显式声明的 content-type。
    let payload: &[u8] = b"\x89PNG\r\n\x1a\nnot-json";

    // HTTP parity 基准(应先通过):Raw(bytes, ct) 保留声明的 CT。
    let http_resp = reqwest::Client::new()
        .post(format!("{}/v2/models/ens_ct/infer", base))
        .header("content-type", "image/png")
        .body(payload.to_vec())
        .send()
        .await
        .expect("http ensemble infer failed");
    assert_eq!(http_resp.status(), 200, "HTTP ensemble must accept binary root input");
    let http_body: Value = http_resp.json().await.expect("http response must be JSON");
    assert_eq!(
        http_body["out"].as_str(),
        Some("image/png"),
        "HTTP baseline: declared content-type must reach the sub-model worker; got: {}",
        http_body
    );

    // gRPC:proto headers map 声明同样的 content-type。
    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let resp = client
        .infer(InferRequest {
            model_name: "ens_ct".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(payload.to_vec()),
            headers: HashMap::from([(
                "content-type".to_string(),
                "image/png".to_string(),
            )]),
            ..Default::default()
        })
        .await
        .expect("gRPC ensemble infer must walk the DAG")
        .into_inner();
    let got: Value = serde_json::from_slice(&resp.data).expect("ensemble response must be JSON");
    assert_eq!(
        got["out"].as_str(),
        Some("image/png"),
        "parity 缺陷(infer.rs:100-107):gRPC ensemble 二进制入口硬编码 \
         application/octet-stream,丢弃 proto headers 声明的 content-type;HTTP 侧 \
         Raw(bytes, ct) 保留声明值。worker 实际看到: {}",
        got
    );

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// 对账 C（覆盖测试，非举证）：跨协议 parity + DAG 错误映射
// ---------------------------------------------------------------------------

/// 引用不存在子模型的 ensemble（DAG 错误在 infer 期暴露）。
fn write_missing_sub_ensemble(repo: &std::path::Path) {
    let dir = repo.join("ens_missing/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: bad
      model: no_such_model
      version: "1"
      inputs:
        x: "$request"
"#,
    )
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_http_ensemble_parity_and_dag_not_found_mapping() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-cgap-ens-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_ct_echo_model(&repo);
    write_ct_ensemble(&repo);
    write_missing_sub_ensemble(&repo);

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-cgap-ens-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            metrics_port = metrics_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "ct_echo", "1").await;
    load_model(&base, "ens_ct", "1").await;
    load_model(&base, "ens_missing", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let json_payload = serde_json::to_vec(&json!({"x": 1})).unwrap();

    // 1) 跨协议 parity：同一 ensemble 模型、同一 JSON 输入，HTTP 与 gRPC
    //    输出逐字节等价。
    let http_resp = reqwest::Client::new()
        .post(format!("{}/v2/models/ens_ct/infer", base))
        .header("content-type", "application/json")
        .body(json_payload.clone())
        .send()
        .await
        .expect("http infer");
    assert_eq!(http_resp.status(), 200);
    let http_json: Value = http_resp.json().await.unwrap();

    let grpc_resp = client
        .infer(InferRequest {
            model_name: "ens_ct".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(json_payload.clone()),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            ..Default::default()
        })
        .await
        .expect("grpc infer")
        .into_inner();
    let grpc_json: Value = serde_json::from_slice(&grpc_resp.data).unwrap();

    assert_eq!(
        http_json, grpc_json,
        "HTTP 与 gRPC 对同一 ensemble 模型产出必须一致"
    );

    // 2) DAG 错误映射：子模型不存在 → gRPC NotFound / HTTP 404。
    // 语义注记:子模型「不存在」在 ensemble 里先被就绪检查拦下——
    // not loaded ⇒ not ready(ModelNotReady → 503/Unavailable),双侧一致
    // 即为「映射正确」(蓝图测试条的真意是双协议同错同码,而非特定 404)。
    let err = client
        .infer(InferRequest {
            model_name: "ens_missing".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(json_payload),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            ..Default::default()
        })
        .await
        .expect_err("missing sub-model must fail");
    assert_eq!(
        err.code(),
        tonic::Code::Unavailable,
        "DAG 子模型未加载 → ModelNotReady → Unavailable(须与 HTTP 503 parity); got: {err:?}"
    );

    let http_resp = reqwest::Client::new()
        .post(format!("{}/v2/models/ens_missing/infer", base))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&json!({"x": 1})).unwrap())
        .send()
        .await
        .expect("http infer missing");
    assert_eq!(http_resp.status(), 503, "HTTP 侧 503 parity 基准");

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// D30 (batch 3): gRPC batch × ensemble — element-wise DAG execution
// ---------------------------------------------------------------------------

/// Unary echo sub-model for the batch fixtures.
fn write_batch_echo(repo: &std::path::Path) {
    let dir = repo.join("echo/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class EchoAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return request.get("data", "")

    def predict(self, x):
        return {"echo": x}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// Minimal streaming tail (never executed — the D30 reject fires on the
/// plan's stream marker before any element runs).
fn write_batch_stream_tail(repo: &std::path::Path) {
    let dir = repo.join("stream_tail/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class TailAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return request.get("data", "")

    def stream_predict(self, request, ctx=None):
        yield {"token": request}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: true\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

fn write_batch_ensembles(repo: &std::path::Path) {
    let dir = repo.join("ens_batch/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        r#"ensemble:
  steps:
    - name: s1
      model: echo
      version: "1"
      inputs:
        data: "$request.data"
"#,
    )
    .unwrap();
    let dir = repo.join("ens_batch_stream/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        r#"ensemble:
  steps:
    - name: tail
      model: stream_tail
      version: "1"
      stream: true
      inputs:
        data: "$request.data"
"#,
    )
    .unwrap();
}

/// Boot a server with the D30 fixtures (explicit loads via the API).
#[cfg(unix)]
async fn boot_batch_server() -> (String, u16, ServerGuard, std::path::PathBuf) {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-audit-ensbatch-{}-{}", std::process::id(), http_port));
    let _ = std::fs::remove_dir_all(&repo);
    write_batch_echo(&repo);
    write_batch_stream_tail(&repo);
    write_batch_ensembles(&repo);

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-audit-ensbatch-yaml-{}-{}", std::process::id(), http_port));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            metrics_port = metrics_port,
            repo = repo.to_string_lossy()
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

/// D30: batch × ensemble happy — elements run in parallel and the response
/// preserves request order; each element = the DAG's unary output.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_d30_batch_ensemble_elements_ordered() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::BatchInferRequest;
    use std::collections::HashMap;

    let (base, grpc_port, _guard, _repo) = boot_batch_server().await;
    load_model(&base, "ens_batch", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let req = tonic::Request::new(BatchInferRequest {
        model_name: "ens_batch".to_string(),
        version: "1".to_string(),
        items: vec![
            br#"{"data":"a"}"#.to_vec().into(),
            br#"{"data":"b"}"#.to_vec().into(),
            br#"{"data":"c"}"#.to_vec().into(),
        ],
        headers: HashMap::new(),
    });
    let resp = client
        .batch_infer(req)
        .await
        .expect("batch ensemble must succeed");
    let items = resp.into_inner().items;
    assert_eq!(items.len(), 3, "one response per element");
    let expected = [r#"{"echo":"a"}"#, r#"{"echo":"b"}"#, r#"{"echo":"c"}"#];
    for (i, item) in items.iter().enumerate() {
        assert_eq!(
            item.status.as_ref().unwrap().code,
            "Ok",
            "element {i} must be Ok: {}",
            item.status.as_ref().unwrap().message
        );
        assert_eq!(
            String::from_utf8_lossy(&item.data),
            expected[i],
            "element {i} must keep its request order"
        );
    }
}

/// D30: element-level failures ride the element's status (the RPC stays Ok —
/// batch semantics); the mapped §4.4 unary-row code rides the message.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_d30_batch_ensemble_element_error_mapped() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::BatchInferRequest;
    use std::collections::HashMap;

    let (base, grpc_port, _guard, _repo) = boot_batch_server().await;
    load_model(&base, "ens_batch", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let req = tonic::Request::new(BatchInferRequest {
        model_name: "ens_batch".to_string(),
        version: "1".to_string(),
        items: vec![
            br#"{"data":"a"}"#.to_vec().into(),
            br#"not json"#.to_vec().into(),
            br#"{"data":"c"}"#.to_vec().into(),
        ],
        headers: HashMap::new(),
    });
    let resp = client
        .batch_infer(req)
        .await
        .expect("element errors must not fail the batch RPC");
    let items = resp.into_inner().items;
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].status.as_ref().unwrap().code, "Ok");
    let err_status = items[1].status.as_ref().unwrap();
    assert_eq!(err_status.code, "Error", "invalid JSON element must error");
    assert!(
        err_status.message.starts_with("400"),
        "element error must carry the mapped 400 (§4.4): {}",
        err_status.message
    );
    assert_eq!(items[2].status.as_ref().unwrap().code, "Ok");
    assert_eq!(String::from_utf8_lossy(&items[2].data), r#"{"echo":"c"}"#);
}

/// D30: a streaming DAG via batch → whole-RPC InvalidArgument (no
/// element-level streaming).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_d30_batch_ensemble_streaming_dag_invalid_argument() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::BatchInferRequest;
    use std::collections::HashMap;

    let (base, grpc_port, _guard, _repo) = boot_batch_server().await;
    load_model(&base, "ens_batch_stream", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let req = tonic::Request::new(BatchInferRequest {
        model_name: "ens_batch_stream".to_string(),
        version: "1".to_string(),
        items: vec![br#"{"data":"a"}"#.to_vec().into()],
        headers: HashMap::new(),
    });
    let status = client
        .batch_infer(req)
        .await
        .expect_err("streaming DAG via batch must be rejected");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "streaming DAG via batch → InvalidArgument, got: {status}"
    );
}

/// §4.4 friendly error: an ensemble model on gRPC DecoupledInfer must get a
/// clear InvalidArgument (the §4.5 matrix has no gRPC-decoupled ensemble
/// row), not the misleading "no workers available" Unavailable.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_grpc_decoupled_ensemble_friendly_error() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::DecoupledInferRequest;
    use std::collections::HashMap;

    let (base, grpc_port, _guard, _repo) = boot_batch_server().await;
    load_model(&base, "ens_batch", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let req = tonic::Request::new(DecoupledInferRequest {
        model_name: "ens_batch".to_string(),
        version: "1".to_string(),
        data: br#"{"data":"a"}"#.to_vec().into(),
        headers: HashMap::new(),
        sequence_id: None,
    });
    let status = client
        .decoupled_infer(req)
        .await
        .expect_err("ensemble on DecoupledInfer must be rejected");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "ensemble DecoupledInfer → InvalidArgument (not 'no workers'), got: {status}"
    );
    assert!(
        status.message().contains("ensemble"),
        "the error must name the real reason, got: {}",
        status.message()
    );
}
