# 性能基准

[English](../benchmark.md)

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
