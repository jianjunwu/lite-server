# 性能基准

[English](../benchmark.md)

## P-PERF-a 基线与 perf-smoke（2026-08-02）

首轮实测基线（蓝图 §4.0.8）+ 可复现的自含 **perf-smoke** 测量设施。当前为
**本地/手动测量工具，未接入 CI**——GitHub 共享 runner 方差 >30%，在其上设 CI
性能门要么抖动（紧阈值）要么抓不到东西（松阈值）。性能门推迟到有了低方差
（自建/dedicated）runner 再议；在此之前回归靠 review（async 路径改动按 §6.5
贴 perf 数据）+ 手动跑本设施兜底。

### 测量口径（含什么、不含什么）

perf-smoke 对**零计算 echo 模型**（`benchmarks/models/echo_model`、
`echo_stream_model`）压三条关键路径：

| 路径 | 形态 | 逼近什么 |
|---|---|---|
| `http_unary` | POST `/v2/models/echo_model/infer`，2000 请求 @ 32 并发 | 全链路客户端可见延迟（零推理模型）：server 开销（协议+横切+队列）+ ZMQ IPC + Python worker 往返 |
| `grpc_unary` | `LiteServer.Infer`，2000 请求 @ 32 并发 | 同管线，gRPC/tonic 面 |
| `sse_stream` | POST `/v2/models/echo_stream_model/events`，16 流 × 20 chunk | 开流延迟、逐 chunk 转发间隔、整流时长 |

**不含**：模型计算（模型立即返回/回显）、TLS、CORS、限流、OTel 导出（全部
默认关——与 §4.0.8 预算口径"默认配置"一致）。读数因此是*服务端开销的上界
代理*，更重要的是一份**同管线、同负载形态、跨 commit/跨机器可比的回归基线**。

### 运行

```bash
cargo build --release                     # 服务端二进制（lite-server-core）
cargo run --release --example perf_smoke  # 报告 → stdout + target/perf-smoke.json
```

要求 PATH 上的 `python` 可 import `lite_server`（worker 是 Python 进程；仓库
根目录 `uv sync` 即可配好）。

### 首轮基线

机器：macOS x86_64，16 核，rustc 1.97.1，release（LTO），git `d0d6992`。
工作站数据——为回归方法论存档，**不作 CI 门槛**。

| 路径 | 指标 | p50 | p99 | 吞吐 |
|---|---|---|---|---|
| http_unary | 请求延迟 | 10.25 ms | 14.03 ms | ~3050 rps |
| grpc_unary | 请求延迟 | 9.42 ms | 12.46 ms | ~3350 rps |
| sse_stream | 开流 | 4.87 ms | 5.68 ms | — |
| sse_stream | chunk 间隔 | 2.07 ms | 5.33 ms | ~6900 chunks/s |

对照 §4.0.8 SLO（"协议层+队列开销 p99 < 5 ms"）读 unary 数：实测 p99 含
Python worker 往返（ZMQ IPC + 进程调度）且占大头——server 独占份额按设计
不可从本设施分离，分解靠下方 profiling runbook。回归用途看的是两次运行
之间的*差值*。

### SLO 对照 §4.0.8 状态

- **基线**：已建立（本节 + 可复现设施）。✅
- **CI 门槛**：**未接入 CI。** 共享 runner 方差（>30%）使任何绝对阈值要么抖动
  要么抓不到东西；门槛推迟到有低方差 runner 再议。§4.0.8 的 `+10%` 占位删去——
  它只会是噪音。
- **0.7.2 教训**：改动 async 路径须附本设施的 perf 数据（见 §6.5 验收）。

### Profiling runbook（测量可信化）

回归出现时的分解手段：

- **tokio-console**（task 级，找两次 `.await` 之间 >1ms 的阻塞——p99 尾
  延迟红线）：以 cargo feature 引入 `console-subscriber`，
  `RUSTFLAGS="--cfg tokio_unstable"` 编译，用 `tokio-console` 连服务端。
  （尚未接入二进制——P-PERF-b 跟踪。）
- **pprof-rs**（CPU 火焰图）：加 debug-only `/debug/pprof` 端点（admin 门控、
  仅 loopback），配 `[profile.profiling] inherits = "release", debug = true`，
  然后 `go tool pprof -http :8080 <dump>`。（后续项，蓝图 P-PERF 子项①。）
- **代码评审红线**（常备，蓝图）：两次 `.await` 之间无 >1ms 阻塞任务；横切
  逻辑单请求 <100µs。

## wrk 对比（lite-server vs LitServe）

> **注意：** 数据实测于 2026-08-02，单机（Intel i9-9980HK）——单机型负载不足以作为定论，请视作方向性参考而非规格。

lite-server 与 LitServe 的 `wrk` 性能对比。报告两种负载：**1ms sleep mock**
（本节——衡量 worker 侧行为，框架开销被淹没）和**零计算 echo 模型**（见
[下方](#框架开销对比零计算-echo对齐)——衡量纯框架开销，真实差异在此显现）。

## 测试环境

- **模型**：矩阵用 1ms `time.sleep()` CPU mock；框架开销节用零计算 echo
- **工具**：`wrk` 4.2.0 + POST 请求（`{"input":"hello"}`），每配置 30s，4 线程
- **系统**：macOS x86_64，Intel Core i9-9980HK（8P/16L），32 GB
- **版本**：lite-server 0.7.8（git `82b0535`）vs LitServe 0.2.17
- **HTTP 层线程模型**（对齐前提）：LitServe 是单 uvicorn 进程 + 单 asyncio
  事件循环；lite-server / lite-server-core 是 tokio N 线程（`--threads N`，
  默认 = CPU 核数）。框架开销节用 `--threads 1` 钉住单事件循环做同构对比。

## 测试结果（1ms sleep 模型）

### 吞吐量（req/s）

| Worker 数 | 并发数 | lite-server | LitServe | lite-server-core | 加速比（ls/lit） |
|-----------|--------|-------------|----------|------------------|-----------------|
| 1 | 1 | 353 | 379 | 443 | 0.93x |
| 1 | 4 | 634 | 656 | 612 | 0.97x |
| 1 | 16 | 658 | 656 | 695 | 1.00x |
| 1 | 64 | 666 | 658 | 694 | 1.01x |
| 2 | 1 | 335 | 346 | 395 | 0.97x |
| 2 | 4 | 1,203 | 1,333 | 1,218 | 0.90x |
| 2 | 16 | 1,333 | 1,341 | 1,369 | 0.99x |
| 2 | 64 | 1,357 | 1,335 | 1,394 | 1.02x |
| 4 | 1 | 306 | 345 | 383 | 0.89x |
| 4 | 4 | 1,464 | 1,495 | 1,526 | 0.98x |
| 4 | 16 | 2,574 | 2,607 | 2,311 | 0.99x |
| 4 | 64 | 2,617 | 2,601 | 2,425 | 1.01x |

### p99 延迟（ms）

| Worker 数 | 并发数 | lite-server | LitServe | lite-server-core |
|-----------|--------|-------------|----------|------------------|
| 1 | 1 | 4.08 | 5.47 | 2.80 |
| 1 | 4 | 8.54 | 7.23 | 7.46 |
| 1 | 16 | 29.59 | 28.04 | 31.98 |
| 1 | 64 | 144.68 | 149.18 | 141.82 |
| 2 | 1 | 4.07 | 3.59 | 4.59 |
| 2 | 4 | 7.04 | 6.23 | 11.71 |
| 2 | 16 | 21.18 | 25.99 | 21.32 |
| 2 | 64 | 57.37 | 60.57 | 67.80 |
| 4 | 1 | 4.41 | 3.86 | 3.20 |
| 4 | 4 | 6.17 | 3.41 | 3.19 |
| 4 | 16 | 8.17 | 27.35 | 9.01 |
| 4 | 64 | 50.34 | 52.38 | 66.78 |

### 分析

**三个服务器在整个矩阵内互差约 ±10% 以内**（lite-server 对 LitServe 0.89x–1.02x，`lite-server-core` 0.89x–1.17x）。这与早期占位数据（w=2/c=4：1,583 vs 531 rps，"3.0x"）大不相同——主要原因是 LitServe 自身：0.2.17 相比占位数据所用的旧版本吞吐提升约 2.5×（w=2/c=4 从 531 → 1,333 rps），而 lite-server 与早前结果相差仅几个百分点。

**这个持平是测量假象，不是结论。** 1ms sleep 本身就是瓶颈：1 worker 触顶 ~660 rps、4 worker ~2,600 rps，三个服务器完全一致——矩阵反映的是"三方都在等同一个 1ms"，不是"三方性能等价"。框架差异在这种负载下不可见，需要用零计算 echo 模型（下一节）才能暴露：**同 worker 数、同单线程 HTTP 层下，lite-server 是 LitServe 的 2.5×**。

**关键结论**：不要用 sleep 模型给服务框架做基准。框架开销对比用下面的 echo 负载；真实模型负载拍板前另行验证。

## 复现步骤

```bash
# 前置依赖
pip install litserve
brew install wrk  # 或 apt-get install wrk

# 快速测试（单配置，约 30 秒）
python benchmarks/scripts/compare.py --lite

# 完整对比
python benchmarks/scripts/compare.py \
  --workers 1 2 4 \
  --concurrency 1 4 16 64 \
  --plot

# 自定义模型
python benchmarks/scripts/compare.py \
  --model-repo /path/to/model_repo \
  --model your_model \
  --workers 1 2 4 \
  --concurrency 1 4 16 64
```

结果保存到 `benchmarks/results/benchmark.csv`。使用 `--plot` 时图表保存到 `benchmarks/results/comparison.png`。

## 指标说明

- **lite-server** = Python CLI 包装启动 Rust 二进制（`lite-server-core`）+ Python workers
- **lite-server-core** = 直接运行 Rust 二进制（无 Python 包装开销）
- **LitServe** = Lightning AI 的推理服务器（FastAPI + uvicorn）

"加速比"列对比 `lite-server` vs `LitServe`。`lite-server-core` 列展示无 Python 桥接开销的原始 Rust 二进制性能。

## 框架开销对比（零计算 echo，对齐）

echo 模型立即返回（`benchmarks/models/echo_model`）——无 sleep，线上的每一
微秒都是框架成本。三个服务器均跑 **2 workers**（`workers_per_device: 2`），
HTTP 层按"测试环境"的线程注记对齐。每配置 25-30s，同一 `wrk` 负载。

> **注意（负载不一致）：** LitServe 无法直接加载 `echo_model`。自 0.7.0 起
> `lite_server.LitAPI` 自包含、**不是** `litserve.LitAPI` 子类，故
> `benchmarks/scripts/run_litserve.py` 对其替换为 **1ms sleep mock**
>（`_BUILTIN_SLEEP_MAP`）。因此本节 LitServe 列是 *零计算 echo（lite-server）
> 对 1ms sleep（LitServe）*——非同构：1ms sleep 主导了 LitServe 的每请求成本，
> 拉大了差距。请按"lite-server echo 对 LitServe 1ms-sleep 基线"读，而非
> 零计算对零计算。同构的框架开销对比见上方 1ms-sleep 矩阵（双方都 sleep 1ms）。

### 默认形态（lite-server tokio 线程 = auto/16，LitServe 单进程）

| Server | c=16 | c=64 |
|---|---|---|
| lite-server | 4,531 rps / p99 8.30 ms | 4,383 rps / p99 39.05 ms |
| lite-server-core | 4,927 rps / p99 28.56 ms | 5,808 rps / p99 23.09 ms |
| LitServe | 2,644 rps / p99 15.23 ms | 2,725 rps / p99 53.36 ms |

### 对齐（双方单事件循环线程：`--threads 1`）

| Server | c=16 | c=64 |
|---|---|---|
| lite-server | 6,606 rps / p99 4.94 ms | 6,574 rps / p99 36.65 ms |
| lite-server-core | 4,920 rps / p99 29.21 ms | 6,376 rps / p99 27.34 ms |
| LitServe | 2,568 rps / p99 31.03 ms | 2,679 rps / p99 70.78 ms |

**lite-server 在 HTTP 层资源完全一致下是 LitServe 的 ~2.5×**（c=16：6,606 vs
2,568 rps），c=16 的 p99 尾延迟更紧（4.94 vs 31.03 ms）。`lite-server-core`
在此负载下与包装层相当（双方每请求固定开销均 ~0.14 ms——包装层的 tokio
事件循环跑在独立 OS 线程上，不在 CPython 主线程）。

**关键结论**：HTTP 层资源与 workers 对齐后，lite-server 的框架开销优于
LitServe 约 2.5×，Python 包装层与原生二进制持平。

## 注意事项

- 这些基准测试衡量的是 HTTP + IPC 开销，非模型计算时间。真实 GPU 推理模型的相对差异会更小。
- 1ms sleep 模型刻意轻量化，用于隔离服务框架开销。
- 结果可能因硬件、操作系统和系统配置而异。
