//! P-PERF-a（蓝图 §4.0.8）：perf-smoke——关键路径性能烟测（自含，零外部工具）。
//!
//! 对常驻模型做三类测量：HTTP unary infer / gRPC unary infer / SSE 流式，
//! 输出 p50/p99/吞吐 JSON（stdout + target/perf-smoke.json）。模型为零计算
//! echo（`benchmarks/models/echo_model` / `echo_stream_model`），故读数逼近
//! **server 侧可控开销**（协议层+队列+横切），即 §4.0.8 SLO 的锚定对象。
//!
//! 用法：
//!   cargo build --release --example perf_smoke   # 需先 cargo build --release
//!   cargo run  --release --example perf_smoke
//!
//! 当前为 informational（P-PERF-a）：永不以非零码退出（报告异常以 0 退出但
//! 打印 ERROR）。阈值门控（p99 回归 +X% fail）属 P-PERF-b——需 CI runner 数据
//! 校准后锁定（共享 runner 方差 >30%，勿用本机数直接当门槛）。

use futures::stream::{self, StreamExt};
use lite_server::proto::liteserver as pb;
use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
use std::process::Stdio;
use std::time::{Duration, Instant};

const HTTP_REQUESTS: usize = 2000;
const GRPC_REQUESTS: usize = 2000;
const UNARY_CONCURRENCY: usize = 32;
const SSE_STREAMS: usize = 16;
const SSE_CHUNKS: u32 = 20;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(e) = run().await {
        // informational：报告失败不以非零码退出（CI 不因此变红），但必须可见。
        eprintln!("perf-smoke ERROR: {e}");
    }
}

async fn run() -> Result<(), String> {
    let bin = std::env::var("LITESERVER_BIN")
        .unwrap_or_else(|_| "target/release/lite-server-core".to_string());
    if !std::path::Path::new(&bin).exists() {
        return Err(format!(
            "server binary {bin} not found — run `cargo build --release` first \
             (or set LITESERVER_BIN)"
        ));
    }
    let http_port = free_port()?;
    let grpc_port = free_port()?;

    // 起服务（配方同 tests/integration_test.rs：LITESERVER_DIE_WITH_PARENT 让
    // 服务在父进程退出时自尽——含 panic/SIGKILL，无需 RAII guard）。
    let mut cmd = std::process::Command::new(&bin);
    cmd.arg("serve")
        .arg("--port")
        .arg(http_port.to_string())
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .arg("--model-repo")
        .arg("benchmarks/models")
        .arg("--no-metrics")
        .arg("--log-level")
        .arg("warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .env("LITESERVER_DIE_WITH_PARENT", "1");
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    let child = cmd.spawn().map_err(|e| format!("spawn server: {e}"))?;
    let mut child = Some(child);

    let client = reqwest::Client::new();
    wait_healthy(&client, http_port, 30).await?;
    load_model(&client, http_port, "echo_model").await?;
    load_model(&client, http_port, "echo_stream_model").await?;

    // ---- 测量 ----
    let http = bench_http_unary(&client, http_port).await?;
    eprintln!("[perf-smoke] http_unary done");
    let grpc = bench_grpc_unary(grpc_port).await?;
    eprintln!("[perf-smoke] grpc_unary done");
    let sse = bench_sse(&client, http_port).await?;
    eprintln!("[perf-smoke] sse_stream done");

    // ---- 停机 ----
    if let Some(c) = child.take() {
        kill_server(c);
    }

    let report = serde_json::json!({
        "meta": {
            "git": git_sha(),
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "cores": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
            "rustc": rustc_version(),
            "mode": "informational (P-PERF-a; thresholds locked in P-PERF-b)",
            "note": "zero-compute echo models — numbers approximate SERVER-side overhead (protocol+queue+middleware), not model time",
        },
        "paths": {
            "http_unary": http,
            "grpc_unary": grpc,
            "sse_stream": sse,
        }
    });
    let pretty = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
    println!("{pretty}");
    std::fs::write("target/perf-smoke.json", &pretty)
        .map_err(|e| format!("write target/perf-smoke.json: {e}"))?;
    eprintln!("[perf-smoke] report written to target/perf-smoke.json");
    Ok(())
}

fn free_port() -> Result<u16, String> {
    let l = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    Ok(l.local_addr().map_err(|e| e.to_string())?.port())
}

fn kill_server(mut child: std::process::Child) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

async fn wait_healthy(client: &reqwest::Client, port: u16, secs: u64) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if let Ok(resp) = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .timeout(Duration::from_secs(1))
            .send()
            .await
        {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!("server not healthy within {secs}s"))
}

async fn load_model(client: &reqwest::Client, port: u16, model: &str) -> Result<(), String> {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{port}/v2/repository/models/{model}/versions/1/load"
        ))
        .send()
        .await
        .map_err(|e| format!("load {model}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("load {model}: HTTP {}", resp.status()));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(resp) = client
            .get(format!("http://127.0.0.1:{port}/v2/models/{model}/ready"))
            .send()
            .await
        {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if body["ready"].as_bool() == Some(true) {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!("{model} not ready within 30s"))
}

/// 延迟样本 → {n, rps, p50_ms, p99_ms, mean_ms}。
fn summarize(samples: &[f64], wall: Duration) -> serde_json::Value {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| -> f64 {
        if s.is_empty() {
            return 0.0;
        }
        let idx = ((s.len() as f64) * p).ceil() as usize;
        s[idx.saturating_sub(1).min(s.len() - 1)] * 1000.0
    };
    let mean = if s.is_empty() { 0.0 } else { s.iter().sum::<f64>() / s.len() as f64 * 1000.0 };
    serde_json::json!({
        "n": s.len(),
        "rps": (s.len() as f64 / wall.as_secs_f64()).round(),
        "p50_ms": (pct(0.50) * 100.0).round() / 100.0,
        "p99_ms": (pct(0.99) * 100.0).round() / 100.0,
        "mean_ms": (mean * 100.0).round() / 100.0,
    })
}

/// HTTP unary infer：POST /v2/models/echo_model/infer，{"input":1}。
async fn bench_http_unary(client: &reqwest::Client, port: u16) -> Result<serde_json::Value, String> {
    let url = format!("http://127.0.0.1:{port}/v2/models/echo_model/infer");
    let wall_start = Instant::now();
    let latencies: Vec<f64> = stream::iter(0..HTTP_REQUESTS)
        .map(|_| {
            let client = client.clone();
            let url = url.clone();
            async move {
                let t = Instant::now();
                let resp = client
                    .post(&url)
                    .json(&serde_json::json!({"input": 1}))
                    .send()
                    .await
                    .ok()?;
                if !resp.status().is_success() {
                    return None;
                }
                let _ = resp.bytes().await.ok()?;
                Some(t.elapsed().as_secs_f64())
            }
        })
        .buffer_unordered(UNARY_CONCURRENCY)
        .filter_map(|x| async move { x })
        .collect()
        .await;
    let wall = wall_start.elapsed();
    if latencies.len() < HTTP_REQUESTS / 2 {
        return Err(format!(
            "http_unary: only {}/{} requests succeeded",
            latencies.len(),
            HTTP_REQUESTS
        ));
    }
    Ok(serde_json::json!({
        "requests": HTTP_REQUESTS,
        "concurrency": UNARY_CONCURRENCY,
        "result": summarize(&latencies, wall),
    }))
}

/// gRPC unary infer：LiteServer.Infer(echo_model)。
async fn bench_grpc_unary(port: u16) -> Result<serde_json::Value, String> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| format!("grpc connect: {e}"))?;
    let wall_start = Instant::now();
    let latencies: Vec<f64> = stream::iter(0..GRPC_REQUESTS)
        .map(|_| {
            let mut client = LiteServerClient::new(channel.clone());
            async move {
                let t = Instant::now();
                let req = pb::InferRequest {
                    model_name: "echo_model".to_string(),
                    version: "1".to_string(),
                    data: bytes::Bytes::from_static(b"{\"input\":1}"),
                    headers: Default::default(),
                    sequence_id: None,
                };
                client.infer(req).await.ok()?;
                Some(t.elapsed().as_secs_f64())
            }
        })
        .buffer_unordered(UNARY_CONCURRENCY)
        .filter_map(|x| async move { x })
        .collect()
        .await;
    let wall = wall_start.elapsed();
    if latencies.len() < GRPC_REQUESTS / 2 {
        return Err(format!(
            "grpc_unary: only {}/{} requests succeeded",
            latencies.len(),
            GRPC_REQUESTS
        ));
    }
    Ok(serde_json::json!({
        "requests": GRPC_REQUESTS,
        "concurrency": UNARY_CONCURRENCY,
        "result": summarize(&latencies, wall),
    }))
}

/// SSE 流式：POST /v2/models/echo_stream_model/events，{"n":20}。
/// 测开流延迟（首字节）、逐 chunk 间隔、整流时长。
async fn bench_sse(client: &reqwest::Client, port: u16) -> Result<serde_json::Value, String> {
    let url = format!("http://127.0.0.1:{port}/v2/models/echo_stream_model/events");
    let wall_start = Instant::now();
    let results: Vec<Option<(f64, f64, Vec<f64>)>> = stream::iter(0..SSE_STREAMS)
        .map(|_| {
            let client = client.clone();
            let url = url.clone();
            async move {
                let start = Instant::now();
                let resp = client
                    .post(&url)
                    .json(&serde_json::json!({"n": SSE_CHUNKS}))
                    .send()
                    .await
                    .ok()?;
                if !resp.status().is_success() {
                    return None;
                }
                let mut open: Option<f64> = None;
                let mut last = start;
                let mut intervals = Vec::new();
                let mut buf = String::new();
                let mut stream = resp.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let bytes = chunk.ok()?;
                    let now = Instant::now();
                    if open.is_none() {
                        open = Some(now.duration_since(start).as_secs_f64());
                    }
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    // SSE 事件以空行分隔——逐完整事件记录间隔。
                    while let Some(pos) = buf.find("\n\n") {
                        buf.drain(..pos + 2);
                        intervals.push(now.duration_since(last).as_secs_f64());
                        last = now;
                    }
                }
                let total = start.elapsed().as_secs_f64();
                Some((open?, total, intervals))
            }
        })
        .buffer_unordered(SSE_STREAMS)
        .collect()
        .await;
    let wall = wall_start.elapsed();

    let mut opens = Vec::new();
    let mut totals = Vec::new();
    let mut intervals = Vec::new();
    let mut ok = 0usize;
    for r in results.into_iter().flatten() {
        opens.push(r.0);
        totals.push(r.1);
        intervals.extend(r.2);
        ok += 1;
    }
    if ok < SSE_STREAMS / 2 {
        return Err(format!("sse_stream: only {ok}/{SSE_STREAMS} streams succeeded"));
    }
    let chunks_seen = intervals.len();
    Ok(serde_json::json!({
        "streams": SSE_STREAMS,
        "chunks_per_stream": SSE_CHUNKS,
        "chunks_total": chunks_seen,
        "open": summarize(&opens, wall),
        "stream_total": summarize(&totals, wall),
        "chunk_interval": summarize(&intervals, wall),
    }))
}

fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn rustc_version() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
