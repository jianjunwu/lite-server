//! stream-chunk-overhead: 流式路径 per-chunk 固定开销测量(自含,零外部工具)。
//!
//! 回答「流式 ASR 这类逐 chunk 负载,管道每 chunk 的固定开销是多少」:
//!   1. gRPC bidi 逐 chunk RTT(open 后 ping-pong:发一个 chunk 收一个回显)——ASR
//!      形态的核心指标。两档:爆发 ping-pong(管道延迟下限)与 40ms 节奏(真实 ASR
//!      帧节奏,顺带报「RTT ≥ 节奏」的违约数——实时性是否成立)。
//!   2. SSE 逐 chunk 到达间隔(POST /events,stream_predict 输出)——前向循环。
//!   3. WS 逐 chunk 到达间隔(GET /stream,同一前向循环)。
//!
//! 模型零计算(`bidi_echo_model` / `echo_stream_model`),读数逼近 **server 侧可控
//! 开销**(gRPC/ZMQ/队列/前向循环),不含模型时间。
//!
//! 用法:
//!   cargo build --release --example stream_chunk_overhead   # 需先 cargo build --release
//!   cargo run  --release --example stream_chunk_overhead
//!
//! 报告写 target/stream-chunk-overhead.json。

use futures::stream::{self, StreamExt};
use futures::SinkExt;
use lite_server::proto::liteserver as pb;
use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::{Duration, Instant};

const BIDI_BURST_CHUNKS: usize = 1000;
const BIDI_PACED_CHUNKS: usize = 300;
const BIDI_PACE_MS: u64 = 40; // 真实 ASR 帧节奏(25 fps)
const SSE_STREAMS: usize = 8;
const SSE_CHUNKS: u32 = 200;
const WS_STREAMS: usize = 4;
const WS_CHUNKS: u32 = 200;
const CHUNK_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(e) = run().await {
        // 测量工具:报告失败不以非零码退出(不挡 shell),但必须可见。
        eprintln!("stream-chunk-overhead ERROR: {e}");
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

    // 起服务(配方同 benchmarks/perf_smoke.rs:LITESERVER_DIE_WITH_PARENT 让
    // 服务在父进程退出时自尽——含 panic/SIGKILL,无需 RAII guard)。
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
    load_model(&client, http_port, "bidi_echo_model").await?;
    load_model(&client, http_port, "echo_stream_model").await?;

    // ---- 测量 ----
    let bidi = bench_bidi_rtt(grpc_port).await?;
    eprintln!("[stream-chunk-overhead] bidi_rtt done");
    let sse = bench_sse(&client, http_port).await?;
    eprintln!("[stream-chunk-overhead] sse done");
    let ws = bench_ws(http_port).await?;
    eprintln!("[stream-chunk-overhead] ws done");

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
            "mode": "local/manual measurement — per-chunk plumbing overhead",
            "note": "zero-compute models — numbers approximate per-chunk pipe overhead (gRPC+ZMQ+worker dispatch / SSE+WS forward loop), not model time",
        },
        "paths": {
            "grpc_bidi": bidi,
            "sse_stream": sse,
            "ws_stream": ws,
        }
    });
    let pretty = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
    println!("{pretty}");
    std::fs::write("target/stream-chunk-overhead.json", &pretty)
        .map_err(|e| format!("write target/stream-chunk-overhead.json: {e}"))?;
    eprintln!("[stream-chunk-overhead] report written to target/stream-chunk-overhead.json");
    Ok(())
}

/// gRPC bidi ping-pong:open 后每发一个 chunk 等一个回显,测逐 chunk RTT。
/// 两档:爆发(1000 个连续 ping-pong)与 40ms 节奏(300 个,ASR 帧节奏)。
async fn bench_bidi_rtt(port: u16) -> Result<serde_json::Value, String> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| format!("grpc connect: {e}"))?;
    let mut client = LiteServerClient::new(channel);

    // ping-pong 走 mpsc 请求流(tonic bidi 标准姿势:响应流与请求流并发驱动)。
    // BidiOpen 必须先入队再发起 RPC(仓库测试注释:服务器的 handler 在返回
    // Response 前就要等首个消息,否则两边互等死锁)。
    let (tx, rx) = tokio::sync::mpsc::channel::<pb::BidiChunk>(64);
    tx.send(pb::BidiChunk {
        stream_id: String::new(),
        payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
            model_name: "bidi_echo_model".to_string(),
            version: String::new(),
            initial_data: bytes::Bytes::from_static(b"{}"),
            sequence_id: None,
            headers: HashMap::new(),
        })),
    })
    .await
    .map_err(|e| format!("queue open: {e}"))?;
    let req_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let t = Instant::now();
    let mut resp = client
        .bidi_stream(tonic::Request::new(req_stream))
        .await
        .map_err(|e| format!("bidi open: {e}"))?
        .into_inner();

    // open → worker on_open 回显(第一个 Data 帧)
    let open_ack = recv_echo(&mut resp).await?;
    let open_rtt_ms = t.elapsed().as_secs_f64() * 1000.0;

    // 爆发档:1000 个严格 ping-pong(每个 chunk 等回显后再发下一个)。
    // 注意:elapsed 必须在循环后立刻求值(报告在 json 组装时才算 rps,
    // 若求值点拖到后面阶段,会污染本档的 wall)。
    let burst_wall = Instant::now();
    let mut burst = Vec::with_capacity(BIDI_BURST_CHUNKS);
    for i in 0..BIDI_BURST_CHUNKS {
        let t = Instant::now();
        tx.send(chunk_msg(i)).await.map_err(|e| format!("send burst {i}: {e}"))?;
        let _ = recv_echo(&mut resp).await?;
        burst.push(t.elapsed().as_secs_f64());
    }
    let burst_wall_elapsed = burst_wall.elapsed();

    // 40ms 节奏档:每 40ms 发一个 chunk;RTT 超过 40ms 说明管道跟不上 ASR 实时。
    let pace = Duration::from_millis(BIDI_PACE_MS);
    let paced_wall = Instant::now();
    let mut tick = Instant::now();
    let mut paced = Vec::with_capacity(BIDI_PACED_CHUNKS);
    let mut over_40ms = 0usize;
    for i in 0..BIDI_PACED_CHUNKS {
        let now = Instant::now();
        if now < tick {
            tokio::time::sleep(tick - now).await;
        }
        let t = Instant::now();
        tx.send(chunk_msg(i)).await.map_err(|e| format!("send paced {i}: {e}"))?;
        let _ = recv_echo(&mut resp).await?;
        let rtt = t.elapsed();
        paced.push(rtt.as_secs_f64());
        if rtt >= pace {
            over_40ms += 1;
        }
        tick += pace;
    }
    let paced_wall_elapsed = paced_wall.elapsed();

    // close → on_close 收尾,不测时。
    let _ = tx
        .send(pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Close(pb::BidiClose {})),
        })
        .await;

    Ok(serde_json::json!({
        "model": "bidi_echo_model",
        "open_ack_ms": (open_rtt_ms * 100.0).round() / 100.0,
        "burst": {
            "chunks": BIDI_BURST_CHUNKS,
            "result": summarize(&burst, burst_wall_elapsed),
        },
        "paced_40ms": {
            "chunks": BIDI_PACED_CHUNKS,
            "pace_ms": BIDI_PACE_MS,
            "over_pace": over_40ms,
            "cadence_ms": (paced_wall_elapsed.as_secs_f64() / BIDI_PACED_CHUNKS as f64 * 1000.0 * 100.0).round() / 100.0,
            "result": summarize(&paced, paced_wall_elapsed),
        },
        "open_ack_payload": open_ack,
    }))
}

fn chunk_msg(i: usize) -> pb::BidiChunk {
    pb::BidiChunk {
        stream_id: String::new(),
        payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
            data: bytes::Bytes::from(format!("{{\"frame\": {i}}}")),
        })),
    }
}

/// 等一个回显帧;Close/超时视为异常。
async fn recv_echo(resp: &mut tonic::Streaming<pb::BidiChunk>) -> Result<serde_json::Value, String> {
    match tokio::time::timeout(CHUNK_TIMEOUT, resp.message()).await {
        Ok(Ok(Some(chunk))) => match chunk.payload {
            Some(pb::bidi_chunk::Payload::Data(d)) => {
                serde_json::from_slice(&d.data).map_err(|e| format!("echo parse: {e}"))
            }
            Some(pb::bidi_chunk::Payload::Close(_)) => {
                Err("stream closed early (Close before expected echo)".to_string())
            }
            Some(pb::bidi_chunk::Payload::Open(_)) => {
                Err("unexpected Open frame".to_string())
            }
            Some(pb::bidi_chunk::Payload::Error(e)) => {
                Err(format!("bidi error frame: {}", e.message))
            }
            None => Err("empty payload frame".to_string()),
        },
        Ok(Ok(None)) => Err("stream ended".to_string()),
        Ok(Err(e)) => Err(format!("grpc stream error: {e}")),
        Err(_) => Err("chunk echo timed out".to_string()),
    }
}

/// SSE 流式:POST /v2/models/echo_stream_model/events,{"n":200}。
/// 测开流延迟(首字节)、逐 chunk 间隔、整流时长。
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
    Ok(serde_json::json!({
        "streams": SSE_STREAMS,
        "chunks_per_stream": SSE_CHUNKS,
        "chunks_total": intervals.len(),
        "open": summarize(&opens, wall),
        "stream_total": summarize(&totals, wall),
        "chunk_interval": summarize(&intervals, wall),
    }))
}

/// WS 流式:GET /v2/models/echo_stream_model/stream,首消息 {"n":200}。
/// chunk 以 Binary 帧到达(server 前向循环),{"done":true} 收尾。
async fn bench_ws(port: u16) -> Result<serde_json::Value, String> {
    let url = format!("ws://127.0.0.1:{port}/v2/models/echo_stream_model/stream");
    let wall_start = Instant::now();
    let results: Vec<Option<(f64, Vec<f64>)>> = stream::iter(0..WS_STREAMS)
        .map(|_| {
            let url = url.clone();
            async move {
                let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.ok()?;
                ws.send(tokio_tungstenite::tungstenite::Message::Text(
                    format!("{{\"n\": {WS_CHUNKS}}}").into(),
                ))
                .await
                .ok()?;
                let start = Instant::now();
                let mut first: Option<f64> = None;
                let mut last = start;
                let mut intervals = Vec::new();
                while intervals.len() < WS_CHUNKS as usize {
                    let next = tokio::time::timeout(Duration::from_secs(10), ws.next())
                        .await
                        .ok();
                    let msg = match next {
                        Some(Some(Ok(m))) => m,
                        _ => return None,
                    };
                    let now = Instant::now();
                    match msg {
                        tokio_tungstenite::tungstenite::Message::Binary(_) => {
                            if first.is_none() {
                                first = Some(now.duration_since(start).as_secs_f64());
                            }
                            intervals.push(now.duration_since(last).as_secs_f64());
                            last = now;
                        }
                        tokio_tungstenite::tungstenite::Message::Text(_) => break, // {"done":true}
                        tokio_tungstenite::tungstenite::Message::Close(_) => break,
                        _ => {}
                    }
                }
                if intervals.len() != WS_CHUNKS as usize {
                    return None; // 没凑齐 chunks → 弃流
                }
                Some((first?, intervals))
            }
        })
        .buffer_unordered(WS_STREAMS)
        .collect()
        .await;
    let wall = wall_start.elapsed();

    let mut ttfb = Vec::new();
    let mut intervals = Vec::new();
    let mut ok = 0usize;
    for r in results.into_iter().flatten() {
        ttfb.push(r.0);
        intervals.extend(r.1);
        ok += 1;
    }
    if ok < WS_STREAMS / 2 {
        return Err(format!("ws_stream: only {ok}/{WS_STREAMS} streams succeeded"));
    }
    Ok(serde_json::json!({
        "streams": WS_STREAMS,
        "chunks_per_stream": WS_CHUNKS,
        "chunks_total": intervals.len(),
        "ttfb": summarize(&ttfb, wall),
        "chunk_interval": summarize(&intervals, wall),
    }))
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

/// 延迟样本 → {n, rps, p50_ms, p99_ms, max_ms, mean_ms}。
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
        "max_ms": (pct(1.0) * 100.0).round() / 100.0,
        "mean_ms": (mean * 100.0).round() / 100.0,
    })
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
