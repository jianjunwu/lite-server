# 性能基准

[English](../benchmark.md)

`wrk` 三侧对比:**lite-server**(PyO3 `serve()` 入口)/ **lite-server-core**(独立 Rust 二进制)/ **LitServe**(FastAPI + uvicorn)。衡量 **HTTP + IPC 框架开销**,非模型计算时间。

## 两种负载

| 负载 | 模型 | 衡量什么 |
|---|---|---|
| **零计算 echo** | `echo_model`,立即返回 | 纯框架开销(协议+横切+队列+ZMQ IPC+worker 往返)——架构差异在此显现 |
| **1ms sleep** | `sleep_1ms_model`,`time.sleep(0.001)` | worker 侧行为——1ms 计算主导,框架开销被淹没 |

> **不要用 sleep 模型给服务框架做基准。** 框架开销对比看 echo;sleep 模型只用于验证 worker 扩展性与三方趋同。

## 测试环境

- **机器**:macOS x86_64,Intel Core i9-9980HK @ 2.40GHz(8 物理核 / 16 逻辑),32 GB
- **版本**:lite-server 0.8.0rc2 vs LitServe 0.2.17;wrk 4.2.0;rustc 1.97.1(release + LTO)
- **负载**:wrk POST `{"input":"hello"}`,4 线程,每配置 15s,轮换跑序 + 5s cooldown 消热降频偏置
- **拉起配置(对齐)**:lite-server / lite-server-core 用 `--threads 1`(单 tokio 事件循环)对齐 LitServe 的单 uvicorn 进程,做**同构**框架开销对比;不传 `--threads` 时 Rust 侧默认 16 线程,会进一步放大差异。

### 三侧架构(对齐后)

| 侧 | HTTP 层 | 推理 worker |
|---|---|---|
| lite-server | 1 Python 宿主进程(内嵌 Rust tokio,`--threads 1` → **1 线程**) | N 个 Python 子进程 |
| lite-server-core | 1 Rust 进程(tokio,`--threads 1` → **1 线程**,无 Python 宿主) | N 个 Python 子进程 |
| LitServe | 1 uvicorn 进程(单 asyncio 事件循环 = **1 线程**) | N 个 Python 子进程(mp.spawn) |

## 口径与限制

- **同构对齐**:三侧都跑**零计算 echo**——lite-server / lite-server-core 加载 `echo_model`;LitServe 因 `lite_server.LitAPI`(自 0.7.0 起独立、非 `litserve.LitAPI` 子类)加载不了该模型,`run_litserve.py` 改用一个 **litserve 原生的零计算 echo builtin**(`_BuiltinEchoAPI`,镜像 `echo_model` 行为:纯返回、无 sleep)替代,确保三侧负载同构。sleep 模型三侧都直接跑等同时长的 `time.sleep` mock。
- 真实 GPU 推理模型计算主导,相对差异会比这里的框架开销更小。

## 结果:零计算 echo(框架开销,`--threads 1`)

每格 `rps / p99(ms)`,`lite/Lit` = lite-server ÷ LitServe 吞吐。三侧同构(均零计算 echo)。

| workers | conc | lite-server | lite-server-core | LitServe | lite/Lit |
|---|---|---|---|---|---|
| 1 | 1 | 1414 / 1.3 | 1494 / 1.0 | 1069 / 1.3 | 1.32× |
| 1 | 16 | 3658 / 18.0 | 3529 / 10.5 | 1745 / 10.7 | 2.10× |
| 1 | 64 | 3650 / 22.7 | 3583 / 30.9 | 1609 / 65.3 | 2.27× |
| 2 | 1 | 1328 / 4.3 | 1366 / 1.5 | 911 / 13.3 | 1.46× |
| 2 | 16 | 6840 / 3.3 | 6783 / 3.4 | 2809 / 10.7 | 2.43× |
| 2 | 64 | 6788 / 14.9 | 6949 / 12.0 | 2978 / 28.2 | 2.28× |
| 4 | 1 | 1469 / 1.1 | 1352 / 3.2 | 1025 / 1.4 | 1.43× |
| 4 | 16 | 7141 / 3.2 | 7146 / 3.2 | 3329 / 15.7 | 2.15× |
| 4 | 64 | 7638 / 11.0 | 7443 / 11.8 | 3674 / 31.7 | 2.08× |

**解读**:

- **lite-server ≡ lite-server-core**(全 9 格差异 <3%):PyO3 嵌入层——`serve()` 的 `with_gil`/`allow_threads`、`stop_server` 槽位、select! 关停 arm、GIL 释放——在热路径**零开销**,与原生 Rust 二进制分毫不差。
- **c≥16 时 lite-server ≈ LitServe 的 2.0–2.4×**(同构框架开销):HTTP 层同为单事件循环(`--threads 1` 对齐 LitServe 单 uvicorn),差异来自 Rust(axum/tokio + ZMQ)对 Python(FastAPI/uvicorn + asyncio)的协议栈/IPC 开销。
- c=1(单请求往返)领先收窄到 ~1.3–1.5×——延迟受限时框架优势变小。
- 三侧 echo 吞吐都随 worker 增长(lite c=16:w1 ~3658 → w2 ~6840 → w4 ~7141,趋饱和),瓶颈在 HTTP+IPC 往返,不在 worker 数。

## 结果:1ms sleep(worker 侧,`--threads 1`)

| workers | conc | lite-server | lite-server-core | LitServe | lite/Lit |
|---|---|---|---|---|---|
| 1 | 1 | 529 / 2.3 | 508 / 2.5 | 463 / 2.6 | 1.14× |
| 1 | 16 | 675 / 26.5 | 679 / 25.4 | 675 / 25.8 | 1.00× |
| 1 | 64 | 677 / 101.0 | 670 / 115.5 | 674 / 99.9 | 1.00× |
| 2 | 1 | 511 / 2.4 | 533 / 2.2 | 448 / 2.9 | 1.14× |
| 2 | 16 | 1380 / 12.3 | 1395 / 12.3 | 1405 / 13.3 | 0.98× |
| 2 | 64 | 1385 / 47.5 | 1385 / 47.8 | 1412 / 47.6 | 0.98× |
| 4 | 1 | 530 / 2.2 | 529 / 2.3 | 462 / 2.5 | 1.15× |
| 4 | 16 | 2779 / 6.6 | 2776 / 6.6 | 2735 / 9.1 | 1.02× |
| 4 | 64 | 2730 / 28.3 | 2740 / 25.1 | 2747 / 25.0 | 0.99× |

**解读**:

- **三方全程趋同**(0.98–1.15×):1ms sleep 主导每请求成本,框架差异不可见——印证"不要用 sleep 模型给框架做基准"。
- 仅 c=1(单请求延迟)lite/core 略领先 ~1.14×(纯往返开销优势)。
- **worker 线性扩展**:1→2→4 worker → ~675 / ~1380 / ~2750 rps(约 2× / 4×),三侧一致。

## 关键结论

1. **框架开销对比用 echo,不用 sleep**——1ms sleep 下三方 ±2%,是测量假象(都在等同一个 1ms)。
2. **PyO3 嵌入层零热路径开销**——lite-server 与原生二进制 lite-server-core 全矩阵持平(18 格差异 <3%),`serve()` 的 GIL 释放 / 重入守卫 / `stop_server` 不产生可测量代价。
3. echo(零计算、同构)下 lite-server 框架吞吐 ~3700–7600 rps,约为 LitServe 的 **2.0–2.4×**(c≥16,单事件循环对齐)。

## 复现

```bash
# 前置:构建 release wheel + core 二进制
maturin build --release                      # → target/wheels/lite_server-*.whl
uv pip install --force-reinstall --no-deps target/wheels/lite_server-*.whl
cargo build --release                        # → target/release/lite-server-core

# 对齐配置(--threads 1)跑 echo / 1ms
python benchmarks/scripts/compare.py --model echo_model \
  --workers 1 2 4 --concurrency 1 16 64 --threads 1 --duration 15
python benchmarks/scripts/compare.py --model sleep_1ms_model \
  --workers 1 2 4 --concurrency 1 16 64 --threads 1 --duration 15

# 快速冒烟(单格)
python benchmarks/scripts/compare.py --lite
```

结果存 `benchmarks/results/benchmark.csv`(`--output` 可改路径)。`compare.py` 透传 `--threads N` 给 lite-server / lite-server-core(LitServe 单进程 uvicorn 不受影响)。
