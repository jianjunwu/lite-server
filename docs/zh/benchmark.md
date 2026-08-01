# 性能基准

[English](../benchmark.md)

## P-PERF-a 基线与 perf-smoke（2026-08-02）

首轮实测基线（蓝图 §4.0.8）+ 可复现的自含 **perf-smoke** 测量设施。当前为
**informational**——只报告、不让构建失败；回归阈值在 P-PERF-b 用 CI runner
数据锁定（GitHub 共享 runner 方差 >30%，拿工作站数据定 p99 门槛必然抖动）。

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
根目录 `uv sync` 即可配好）。CI 跑同一命令（`checks.yml` 的 `perf-smoke`
job，`continue-on-error: true`），报告以 artifact 上传。

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
- **CI 门槛**：informational 运行中；阈值数值（§4.0.8 的 `+10%` 占位）在
  **P-PERF-b** 随 runner 数据锁定，mimalloc A/B 决策同批。⏳
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

> **注意：** 以下基准数据为初步占位数据，仅包含有限的数据点（2 种 worker 配置 × 1 种并发级别）。涵盖更多配置、硬件平台和真实模型负载的全面基准测试正在计划中。仅供参考。

lite-server 与 LitServe 的 `wrk` 性能对比（初步数据）。

## 测试环境

- **模型**：1ms `time.sleep()` CPU mock（衡量 IPC 和 HTTP 开销，非 GPU 计算）
- **工具**：`wrk` + POST 请求（`{"input":"hello"}`）
- **系统**：macOS（Apple Silicon）

## 测试结果

### 吞吐量（req/s）

| Worker 数 | 并发数 | lite-server | LitServe | lite-server-core | 加速比（ls/lit） |
|-----------|--------|-------------|----------|------------------|-----------------|
| 1 | 4 | 171 | 330 | 444 | 0.5x |
| 2 | 4 | 1,583 | 531 | 1,364 | 3.0x |

### p99 延迟（ms）

| Worker 数 | 并发数 | lite-server | LitServe | lite-server-core |
|-----------|--------|-------------|----------|------------------|
| 1 | 4 | 72.1 | 139.6 | 139.2 |
| 2 | 4 | 11.5 | 162.6 | 11.6 |

### 分析

**单 worker（w=1）**：LitServe 原始吞吐更高，因为 lite-server 的 Rust HTTP 层在低并发下无法摊薄开销。Python 包装路径（`lite-server`）比直接运行 Rust 二进制（`lite-server-core`）慢，因为有 PyO3 桥接开销。

**双 worker（w=2）**：lite-server 的架构优势显现。多 worker 场景下，Rust 内核通过 ZMQ 高效分发请求，自适应 batching 和最小负载调度保持 worker 忙碌。LitServe 吞吐下降是因为其 Python HTTP 层（uvicorn）成为瓶颈。

**关键结论**：lite-server 的优势随并发和 worker 数量增长而扩大。生产环境多 worker + 并发请求场景下，lite-server 提供显著更高的吞吐和更低的延迟。

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

## 注意事项

- 这些基准测试衡量的是 HTTP + IPC 开销，非模型计算时间。真实 GPU 推理模型的相对差异会更小。
- 1ms sleep 模型刻意轻量化，用于隔离服务框架开销。
- 结果可能因硬件、操作系统和系统配置而异。
