//! /audit 举证测试——流式场景 server.timeout 回退泄漏(deadline.rs 方案 C
//! 契约 vs 实现)。
//!
//! 契约(deadline.rs:10-14 模块文档 + stream.rs:464-470 注释):流式的
//! OVERALL deadline 仅在客户端显式指定(x-lite-timeout / grpc-timeout)时
//! 激活,「默认配置下长流不受 overall deadline 约束」「long streams keep
//! flowing untouched」。但所有流式端点把 `deadline.unix_ns` —— 含
//! `server.timeout`(默认 30s)的回退值,client_specified=false —— 无条件
//! 写入 `StreamOpen.meta.deadline_unix_ns`(stream.rs:462→inference.rs:758、
//! rpc/stream.rs:118、rpc/bidi.rs:197、rpc/decoupled.rs:135、
//! handlers/bidi.rs:398、custom_routes.rs:217-227),而 worker 在每两个
//! chunk 之间协作式检查该字段并截断流(python worker/streaming.py
//! `_deadline_passed`,898/917 行)。结果:运维只要设置了 server.timeout
//! (文档默认 30s,docs/configuration.md:19),所有超过该值的长流都会被
//! worker 侧静默截断,与 gateway 的策略意图直接矛盾。
//!
//! 真 server 子进程 + 模型夹具(与 tests/audit_ensemble_stream.rs 同构);
//! 端口段 23300-23399(开工核对:在用段 180xx-183xx / 18992 / 19000 /
//! 19600 / 19700 / 23100-23199,本段无冲突)。

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

/// 端口段 23300-23399。
fn next_test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(23300);
    let p = NEXT.fetch_add(1, Ordering::Relaxed);
    assert!(p < 23400, "audit port range 23300-23399 exhausted");
    p
}

fn start_server(args: &[&str], log_path: &std::path::Path) -> Child {
    let mut cmd = Command::new(lite_server_bin());
    let log = std::fs::File::create(log_path).expect("create server log");
    cmd.arg("serve")
        .current_dir(project_root())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
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

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            stop_server(child);
        }
    }
}

fn kill_stale_on_port(port: u16) {
    let output = Command::new("lsof").args(["-ti", &format!(":{}", port)]).output();
    if let Ok(out) = output {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid_str in pids.lines() {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                let ps = Command::new("ps").args(["-p", &pid.to_string(), "-o", "comm="]).output();
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

async fn wait_for_server(port: u16, timeout_secs: u64, log_path: &std::path::Path) {
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
    let tail = std::fs::read_to_string(log_path)
        .map(|s| {
            s.lines()
                .rev()
                .take(50)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_else(|e| format!("<log unreadable: {e}>"));
    panic!(
        "Server did not start within {} seconds\n--- server log tail ({}) ---\n{}",
        timeout_secs,
        log_path.display(),
        tail
    );
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

// ---------------------------------------------------------------------------
// Fixture: non-ensemble streaming model, 0.5s per chunk, one chunk per word.
// ---------------------------------------------------------------------------

fn write_slowgen(repo: &std::path::Path) {
    let dir = repo.join("slowgen").join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"import time
from lite_server import LitAPI


class SlowGenAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request.get("text", "")

    async def predict(self, x, ctx=None):
        return {"tokens": x.split()}

    def stream_predict(self, request, ctx=None):
        for w in request.split():
            time.sleep(0.5)
            yield {"token": w}

    async def encode_response(self, output, ctx=None):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

fn write_server_yaml(repo: &std::path::Path, http_port: u16, extra: &str) -> std::path::PathBuf {
    let path = repo.join("server.yaml");
    std::fs::write(
        &path,
        format!(
            "server:\n  http_port: {http_port}\n{extra}\n\
             model_repository:\n  path: {}\n\n\
             orchestration:\n  control_mode: explicit\n  load_models:\n    - slowgen\n",
            repo.display()
        ),
    )
    .unwrap();
    path
}

// ---------------------------------------------------------------------------
// 举证:server.timeout 回退值经 StreamOpen.meta 下传,worker 侧截断长流,
// 而 gateway 侧(方案 C)刻意不为非客户端指定的 deadline 布防 overall 上限。
// ---------------------------------------------------------------------------

/// 数据/范围假设维度:server.timeout = 2s,6 个 chunk × 0.5s ≈ 3s 的流,
/// 客户端未指定 x-lite-timeout。契约:流不受 overall deadline 约束
/// (client_specified=false),应收齐 6 个 chunk 正常结束。当前:worker 在
/// 2s 处按 meta.deadline_unix_ns 截断,客户端在第 ~4 个 chunk 后收到
/// error 帧,流被静默截断。
#[tokio::test]
#[serial]
async fn test_audit_data_server_timeout_fallback_truncates_long_stream() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir().join(format!(
        "lite-server-audit-deadline-{}-{}",
        std::process::id(),
        http_port
    ));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();
    write_slowgen(&repo);
    let server_yaml = write_server_yaml(&repo, http_port, "  timeout: 2.0\n");
    let log_path = repo.join("server.log");
    let _guard = ServerGuard(Some(start_server(
        &["--config", &server_yaml.to_string_lossy()],
        &log_path,
    )));
    wait_for_server(http_port, 30, &log_path).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    assert!(wait_model_ready(&base, "slowgen", 30).await, "slowgen must load");

    // No x-lite-timeout header: the client specified NO deadline, so per
    // 方案 C the stream must not be overall-deadline-bounded at all.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/slowgen/events", base))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "w1 w2 w3 w4 w5 w6"}))
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .expect("SSE request failed");
    assert_eq!(resp.status(), 200, "stream must open");
    let body = resp.text().await.expect("SSE body read failed");

    let chunks = body.matches("\"token\"").count();
    assert_eq!(
        chunks, 6,
        "all 6 chunks must arrive — the client specified no deadline, so \
         server.timeout must not truncate the stream; body:\n{body}"
    );
    assert!(
        !body.contains("deadline"),
        "the stream must not be cut by the worker-side deadline fallback; body:\n{body}"
    );
}

// ---------------------------------------------------------------------------
// 防过矫 guard + ensemble 流式 tail e2e(B1 修复的两侧边界)
// ---------------------------------------------------------------------------

fn write_ensemble(repo: &std::path::Path, name: &str, yaml: &str) {
    let dir = repo.join(name).join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("config.yaml"), yaml).unwrap();
}

/// Single streaming step: the request text flows straight into slowgen.
const ENS_SLOW_YAML: &str = r#"ensemble:
  steps:
    - name: tail
      model: slowgen
      version: "1"
      stream: true
      inputs:
        text: "$request.text"
"#;

fn write_server_yaml_multi(repo: &std::path::Path, http_port: u16, extra: &str, models: &str) -> std::path::PathBuf {
    let path = repo.join("server.yaml");
    std::fs::write(
        &path,
        format!(
            "server:\n  http_port: {http_port}\n{extra}\n\
             model_repository:\n  path: {}\n\n\
             orchestration:\n  control_mode: explicit\n  load_models:\n{models}",
            repo.display()
        ),
    )
    .unwrap();
    path
}

async fn read_sse_body(base: &str, model: &str, extra_headers: &[(&str, &str)]) -> String {
    let client = reqwest::Client::new();
    let mut req = client
        .post(format!("{}/v2/models/{}/events", base, model))
        .header("Content-Type", "application/json")
        .json(&json!({"text": "w1 w2 w3 w4 w5 w6"}))
        .timeout(Duration::from_secs(20));
    for (k, v) in extra_headers {
        req = req.header(*k, *v);
    }
    let resp = req.send().await.expect("SSE request failed");
    assert_eq!(resp.status(), 200, "stream must open");
    resp.text().await.expect("SSE body read failed")
}

/// 防过矫 guard:修复「fallback 不下传」后,客户端显式 deadline 必须仍然
/// 生效——x-lite-timeout: 1 时 ~1s 处被截断(error 帧含 deadline),证明
/// client_specified 的值依旧经 meta 下传并被 worker/gateway 执行。
#[tokio::test]
#[serial]
async fn test_audit_guard_client_timeout_still_truncates_stream() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir().join(format!(
        "lite-server-audit-deadline-guard-{}-{}",
        std::process::id(),
        http_port
    ));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();
    write_slowgen(&repo);
    let server_yaml = write_server_yaml(&repo, http_port, "  timeout: 2.0\n");
    let log_path = repo.join("server.log");
    let _guard = ServerGuard(Some(start_server(
        &["--config", &server_yaml.to_string_lossy()],
        &log_path,
    )));
    wait_for_server(http_port, 30, &log_path).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    assert!(wait_model_ready(&base, "slowgen", 30).await, "slowgen must load");

    let start = std::time::Instant::now();
    let body = read_sse_body(&base, "slowgen", &[("x-lite-timeout", "1")]).await;
    let elapsed = start.elapsed();

    let chunks = body.matches("\"token\"").count();
    assert!(
        chunks < 6,
        "a client-specified 1s deadline must truncate the 3s stream; got {chunks} chunks; body:\n{body}"
    );
    assert!(
        body.contains("deadline"),
        "the terminal error frame must carry the deadline signal (reason=deadline); body:\n{body}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the 1s client deadline must fire promptly, not at the 2s fallback or later; elapsed {elapsed:?}"
    );
}

/// ensemble 流式 tail 同场景 e2e:server.timeout=2s、客户端未指定 deadline,
/// 经 ensemble 预层(保留 fallback,unary 语义)+ 流式 tail(client 门控)
/// 的 DAG,6 个 chunk 必须全部到达——tail 的 step meta 不带 fallback 值。
#[tokio::test]
#[serial]
async fn test_audit_data_ensemble_tail_stream_not_truncated_by_fallback() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir().join(format!(
        "lite-server-audit-deadline-ens-{}-{}",
        std::process::id(),
        http_port
    ));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();
    write_slowgen(&repo);
    write_ensemble(&repo, "ensslow", ENS_SLOW_YAML);
    let server_yaml = write_server_yaml_multi(
        &repo,
        http_port,
        "  timeout: 2.0\n",
        "    - slowgen\n    - ensslow\n",
    );
    let log_path = repo.join("server.log");
    let _guard = ServerGuard(Some(start_server(
        &["--config", &server_yaml.to_string_lossy()],
        &log_path,
    )));
    wait_for_server(http_port, 30, &log_path).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    assert!(wait_model_ready(&base, "ensslow", 30).await, "ensslow must load");

    let body = read_sse_body(&base, "ensslow", &[]).await;
    let chunks = body.matches("\"token\"").count();
    assert_eq!(
        chunks, 6,
        "ensemble tail: all 6 chunks must arrive — the streaming step's deadline \
         base is client-gated, so server.timeout must not truncate it; body:\n{body}"
    );
    assert!(
        !body.contains("deadline"),
        "ensemble tail must not be cut by the fallback; body:\n{body}"
    );
}

/// ensemble 侧的防过矫 guard:客户端显式 x-lite-timeout: 1 经
/// EnsembleExecOpts.deadline_client_specified 门控后仍须成为流式 step 的
/// E5 基值,~1s 截断。
#[tokio::test]
#[serial]
async fn test_audit_guard_ensemble_tail_client_timeout_still_truncates() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir().join(format!(
        "lite-server-audit-deadline-ensg-{}-{}",
        std::process::id(),
        http_port
    ));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();
    write_slowgen(&repo);
    write_ensemble(&repo, "ensslow", ENS_SLOW_YAML);
    let server_yaml = write_server_yaml_multi(
        &repo,
        http_port,
        "  timeout: 30.0\n",
        "    - slowgen\n    - ensslow\n",
    );
    let log_path = repo.join("server.log");
    let _guard = ServerGuard(Some(start_server(
        &["--config", &server_yaml.to_string_lossy()],
        &log_path,
    )));
    wait_for_server(http_port, 30, &log_path).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    assert!(wait_model_ready(&base, "ensslow", 30).await, "ensslow must load");

    let body = read_sse_body(&base, "ensslow", &[("x-lite-timeout", "1")]).await;
    let chunks = body.matches("\"token\"").count();
    assert!(
        chunks < 6,
        "ensemble tail: a client-specified 1s deadline must truncate the stream; \
         got {chunks} chunks; body:\n{body}"
    );
    assert!(
        body.contains("deadline"),
        "the terminal error frame must carry the deadline signal; body:\n{body}"
    );
}
