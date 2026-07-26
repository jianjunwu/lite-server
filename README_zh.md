# lite-server

高性能模型推理服务器 — Rust 内核处理 I/O，Python 负责推理。

[English](README.md)

## 目录

- [lite-server](#lite-server)
  - [目录](#目录)
  - [为什么选 lite-server？](#为什么选-lite-server)
  - [快速开始](#快速开始)
  - [架构](#架构)
  - [与其他框架对比](#与其他框架对比)
  - [性能基准](#性能基准)
  - [功能特性](#功能特性)
    - [推理模式](#推理模式)
    - [模型管理](#模型管理)
    - [自定义路由](#自定义路由)
    - [Worker 韧性](#worker-韧性)
    - [可观测性](#可观测性)
  - [安装](#安装)
    - [预编译 Wheel（推荐）](#预编译-wheel推荐)
    - [从源码编译](#从源码编译)
  - [CLI 命令](#cli-命令)
  - [示例](#示例)
  - [API 端点](#api-端点)
  - [配置](#配置)
  - [常见问题](#常见问题)
  - [多平台支持](#多平台支持)
  - [开发](#开发)
    - [项目结构](#项目结构)
  - [License](#license)

## 为什么选 lite-server？

| | 特性 | 对你意味着什么 |
|---|------|--------------|
| **快** | Rust HTTP 内核 (axum/tokio)、零拷贝数据路径、自适应 batching | 比纯 Python 方案吞吐更高、延迟更低 |
| **稳** | 异常检测、心跳检测、自动重启、生命周期钩子 | Worker 自动发现卡死并重启，不用人工盯 |
| **简** | `pip install`，写一个 `model.py` 就能上线 | 不用 Docker、不用 Java、不用编译 C++ |
| **灵** | 热重载、多版本、ensemble DAG 编排 | A/B 测试、灰度发布、多模型流水线一站搞定 |
| **明** | Prometheus 指标、时间线、告警 | 生产环境一目了然，不用猜 |
| **轻** | 单二进制 + Python worker，跨平台 | 笔记本能跑，服务器能扩 |

## 快速开始

```bash
# 1. 安装
pip install lite-server

# 2. 脚手架创建项目
python -m lite_server init my_project --template empty
cd my_project

# 3. 启动服务
python -m lite_server serve --config server.yaml

# 4. 测试
python test_request.py
# 或手动测试：
curl -X POST http://localhost:8000/v2/models/my_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}
```

目前仅提供 `empty` 模板。使用 `--wizard` 进入交互式项目设置。

## 架构

```
  HTTP/gRPC 请求
        │
        ▼
  ┌─────────────────┐     ┌──────────────────┐
  │   Rust 内核      │     │  Python Workers   │
  │  (axum/tokio)    │────►│  (子进程)          │
  │                  │ZMQ/ │                   │
  │  ┌────────────┐  │Protobuf ┌─────────────┐│
  │  │ 推理队列    │  │     │  │  model.py   │  │
  │  │ (per model)│  │     │  │  (LitAPI)   │  │
  │  └────────────┘  │     │  └─────────────┘  │
  │  ┌────────────┐  │     └──────────────────┘
  │  │ 指标 & 告警 │  │
  │  └────────────┘  │
  └─────────────────┘
```

Rust 内核处理所有 I/O（HTTP、gRPC、IPC、指标、文件监听），Python worker 负责模型推理。这种分离让你拥有 Rust 级别的吞吐和 Python 级别的简洁。

详见 [docs/zh/architecture.md](docs/zh/architecture.md)。

## 与其他框架对比

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| 语言 | Rust + Python | C++ | Java + Python | Python | Python |
| 安装 | `pip install` | Docker | `pip install` / Docker | `pip install` | `pip install` |
| 热重载 | 文件监听自动重载 | API 模型切换（不支持代码热重载） | 有限支持 | 不支持 | 不支持 |
| 多版本 | 支持（激活/停用切换） | 支持 | 支持 | 手动管理 | 手动管理 |
| Ensemble | DAG 并行分层执行 | 支持 | 不支持 | Pipeline | Deployment graph |
| 异常检测 | Envoy 风格自动剔除 | 不支持 | 不支持 | 不支持 | 不支持 |
| 心跳 + 自动重启 | ZMQ 探测，自动重启卡死 worker | 不支持 | 不支持 | 不支持 | 支持（Replica 健康检查 + 自动重启） |
| 生命周期钩子 | Shell + HTTP 回调 | 不支持 | 不支持 | 支持（`on_startup`/`on_shutdown`） | 支持（`__init__`/`reconfigure`/`on_shutdown`） |
| 流式输出 | SSE + WebSocket + gRPC | 支持（gRPC streaming） | 支持（HTTP streaming） | 支持 | 支持 |
| 最小开销 | ~15MB | ~2GB+ | ~1.5GB+ | ~500MB+ | ~100MB+ |

详见 [docs/zh/comparison.md](docs/zh/comparison.md)。

## 性能基准

> **注意：** 以下数据为初步占位数据，数据点有限。详见 [docs/zh/benchmark.md](docs/zh/benchmark.md) 了解背景和复现步骤。

2 worker、4 并发测试（1ms CPU mock 模型）：

| 服务器 | 吞吐量 | p99 延迟 |
|--------|--------|---------|
| lite-server | 1,583 req/s | 11.5 ms |
| LitServe | 531 req/s | 162.6 ms |
| lite-server-core | 1,364 req/s | 11.6 ms |

详见 [docs/zh/benchmark.md](docs/zh/benchmark.md)。

## 功能特性

### 推理模式

- **标准 batching** — 请求聚合批量处理，提升 GPU 利用率
- **Continuous batching** — LLM 专用，支持 prefill/step/has_finished 钩子
- **流式输出** — 逐 token 输出，支持 SSE、WebSocket、gRPC
- **Ensemble** — DAG 编排，多模型流水线并行执行

### 模型管理

- **Triton 风格仓库** — `model_name/version/model.py` 目录结构
- **热重载** — 修改 model.py，服务器自动感知并重载
- **多版本** — 独立加载、卸载、激活、停用
- **加载策略** — `explicit`（手动指定）、`latest`（最新版本）、`all`（全部加载）
- **模型打包** — `.lma` 格式，SHA256 + HMAC 签名验证
- **模型上传/下载** — 通过 HTTP API 上传 `.lma` 包或原始文件，支持自动加载

### 自定义路由

- **装饰器路由** — 在 `LitAPI` 方法上用 `@route.get("/status")` 声明，挂在 `/v2/models/<model>/<tail>` 下
- **与推理同通道** — 路由 handler 运行在模型 worker 内，无独立进程
- **服务器上下文访问** — handler 中可使用 `ctx.server.registry` 和跨模型 `ctx.server.inference.infer()`
- **回调链** — 模型级回调（认证、限流、CORS、日志）同时覆盖推理和自定义路由（Rust 侧限流/CORS 中间件暂未覆盖自定义路由）

### Worker 韧性

- **异常检测** — 连续错误自动剔除 worker（Envoy 风格）
- **请求重试** — 失败请求自动重试到其他 worker（最多 3 次）
- **最小负载路由** — 请求发给当前最空闲的 worker
- **最大请求数回收** — 处理 N 个请求后自动重启 worker，支持抖动防止惊群效应
- **心跳检测** — 定期 ZMQ 探测检测卡死 worker，自动杀死并重启
- **生命周期钩子** — worker 就绪/退出/异常时触发 shell 命令或 HTTP 回调，便于告警和可观测
- **单请求超时** — 硬超时防止卡死请求阻塞队列

### 可观测性

- **Prometheus 指标** — QPS、P50/P90/P99 延迟、队列深度、TTFT、batch 大小、worker 剔除数
- **自定义指标** — 通过 `register_metric()` / `report_metric()` 从模型代码采集 Gauge、Counter、Histogram
- **时间线** — 每个模型的历史指标采样
- **告警** — 内置异常检测告警规则
- **结构化日志** — 基于 tracing 的日志，包含 model/worker 上下文

## 安装

### 预编译 Wheel（推荐）

支持 Linux、macOS、Windows（x86_64 + aarch64），Python 3.10-3.14：

```bash
pip install lite-server-<version>-py3-none-<platform>.whl
```

### 从源码编译

需要 Rust >= 1.70 和 Python >= 3.10。

```bash
pip install maturin
maturin develop          # 开发构建
maturin build --release  # 发布 wheel
```

## CLI 命令

```bash
lite-server serve                     # 启动推理服务器
lite-server serve --config server.yaml
lite-server serve --port 9000 --max-requests 1000 --max-requests-jitter 100
lite-server config-check server.yaml  # 校验配置
lite-server benchmark --model my_model
lite-server analyze --model my_model
lite-server pack ./my_model --version 1
lite-server unpack my_model_v1.lma
lite-server init my_project           # 脚手架创建项目
```

详见 [docs/zh/cli.md](docs/zh/cli.md) 获取完整 CLI 参考手册。

## 示例

详见 [examples/](examples/) 目录：

| # | 示例 | 说明 |
|---|------|------|
| 01 | [basic](examples/01_basic/) | 最简 echo 模型 |
| 02 | [batching](examples/02_batching/) | 请求 batching，自适应超时 |
| 03 | [streaming](examples/03_streaming/) | 逐 token 流式输出（SSE/WebSocket） |
| 04 | [multi_version](examples/04_multi_version/) | 双版本切换演示 |
| 05 | [ensemble](examples/05_ensemble/) | DAG 多模型流水线 |
| 06 | [custom_route](examples/06_custom_route/) | 自定义 HTTP 路由（`@route` 装饰器） |
| 07 | [custom_params](examples/07_custom_params/) | 配置驱动的模型行为 |
| 09 | [custom_metrics](examples/09_custom_metrics/) | 自定义 Prometheus 指标（Gauge/Counter/Histogram） |
| 10 | [async](examples/10_async/) | 异步推理（统一异步管线） |
| 11 | [logging](examples/11_logging/) | 各阶段结构化日志 |
| 12 | [continuous_batching](examples/12_continuous_batching/) | LLM 连续批处理（prefill/step/has_finished） |
| 13 | [bidi_streaming](examples/13_bidi_streaming/) | 双向流式通信（ASR） |
| 14 | [lifecycle_hooks](examples/14_lifecycle_hooks/) | Worker 生命周期钩子（shell + HTTP） |
| 16 | [grpc](examples/16_grpc/) | gRPC 推理端点 |

详见 [examples/README.md](examples/README.md) 获取学习路径和使用说明。

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v2/models/{name}/infer` | 推理（活跃版本） |
| POST | `/v2/models/{name}/versions/{v}/infer` | 推理（指定版本） |
| POST | `/v2/models/{name}/events` | SSE 流式 |
| POST | `/v2/models/{name}/versions/{v}/events` | SSE 流式（指定版本） |
| GET | `/v2/models/{name}/stream` | WebSocket 流式 |
| GET | `/v2/models/{name}/versions/{v}/stream` | WebSocket 流式（指定版本） |
| GET | `/v2/models` | 列出已加载模型 |
| GET | `/v2/models/{name}/versions` | 多版本总览（状态 / 活跃 / 权重 / worker / loaded_at） |
| GET | `/v2/models/{name}/ready` | 就绪检查（活跃版本） |
| GET | `/v2/models/{name}/versions/{v}/ready` | 就绪检查（指定版本） |
| GET | `/v2/models/{name}/health` | 各 worker 健康状态（路由选中版本） |
| GET | `/v2/models/{name}/versions/{v}/health` | 各 worker 健康状态（指定版本） |
| GET | `/v2/models/{name}/compare` | 比较模型版本 |
| DELETE | `/v2/models/{name}/versions/{v}` | 删除模型版本 |
| POST | `/v2/repository/models/{name}/versions/{v}/load` | 加载模型版本 |
| POST | `/v2/repository/models/{name}/unload` | 卸载活跃版本 |
| POST | `/v2/repository/models/{name}/versions/{v}/unload` | 卸载指定版本 |
| POST | `/v2/repository/index` | 索引模型仓库 |
| POST | `/v2/repository/models/{name}/versions/{v}/upload` | 上传模型文件（.lma 或原始文件） |
| GET | `/v2/repository/models/{name}/versions/{v}/download` | 下载模型文件 |
| GET | `/v2/repository/models/{name}/versions/{v}/files` | 列出版本目录文件 |
| POST | `/v2/models/{name}/reload` | 热重载（活跃版本） |
| POST | `/v2/models/{name}/versions/{v}/reload` | 热重载（指定版本） |
| POST | `/v2/models/{name}/versions/{v}/activate` | 激活版本（硬切换） |
| PUT | `/v2/models/{name}/routing` | 原子设置流量权重（`{"weights":{"v1":90,"v2":10}}`） |
| GET | `/health` | 健康检查（按模型分组的 JSON） |
| GET | `/livez` | 存活探针（始终 200） |
| GET | `/readyz` | 就绪探针（有模型可服务前返回 503） |
| GET | `/startupz` | 启动探针（模型加载中返回 503） |
| GET | `/info` | 服务器信息 |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/metrics/timeline` | 历史指标时间线 |
| GET | `/metrics/timeline/{name}` | 单模型指标时间线 |
| GET | `/metrics/timeline/{name}/versions/{v}` | 单模型单版本指标时间线 |
| GET | `/metrics/alerts` | 告警规则与状态 |

**自定义路由**通过 `LitAPI` 方法上的 `@route` 装饰器声明，挂在 `/v2/models/{name}/<tail>` 下。系统保留字（`infer`、`events`、`stream`、`ready`、`health`、`reload`、`versions`、`compare`、`livez`、`readyz`、`startupz`）不可覆盖。

## 配置

最小 `server.yaml`：

```yaml
server:
  http_port: 8000
  host: 0.0.0.0

model_repository:
  path: ./model_repo
```

单模型配置（`model_repo/my_model/1/config.yaml`）：

```yaml
max_batch_size: 8
batch_timeout: 0.01
stream: false
accelerator: cpu
workers_per_device: 1
request_timeout: 30.0

# Worker 生命周期 — 自动重启 + 抖动 + 心跳 + 钩子
max_requests: 500
max_requests_jitter: 50

heartbeat_interval: 10.0
heartbeat_timeout: 5.0
heartbeat_max_failures: 3

hooks:
  on_ready: 'echo "Worker $WORKER_ID ready"'
  on_error: 'curl -s -X POST http://alerts.internal/worker-error \
    -d "{\"model\":\"$MODEL\",\"reason\":\"$REASON\"}"'
```

详见 [docs/zh/configuration.md](docs/zh/configuration.md) 获取完整配置参考（服务器、模型、编排、CLI 参数）。

详见 [docs/zh/cli.md](docs/zh/cli.md) 获取完整 CLI 参考手册。

详见 [docs/zh/model-authoring.md](docs/zh/model-authoring.md) 获取模型开发指南（LitAPI 接口、流式输出、continuous batching、最佳实践）。

## 常见问题

**Q: lite-server 和 LitServe 有什么区别？**
lite-server 用 Rust HTTP 内核（axum/tokio）替代了 Python 的 uvicorn，多 worker 并发场景下吞吐提升 3 倍。模型代码写法一样（兼容 LitAPI）。

**Q: 需要 Docker 吗？**
不需要。`pip install` 后直接运行。支持 Linux、macOS、Windows。

**Q: 能直接用现有的 LitAPI 代码吗？**
`from lite_server import LitAPI` 适用于所有有 `setup` + `predict` 的模型。0.7.0 起为独立基类（不依赖 litserve），原生支持异步方法——把 `predict` 写成 `async def` 即可。

**Q: 怎么部署多个模型？**
每个模型放在 `model_repo/` 下独立目录，在 `server.yaml` 的 `orchestration` 段落中声明。详见 [examples/05_ensemble](examples/05_ensemble/)。

**Q: 怎么切换模型版本？**
使用激活/停用 API：`POST /v2/models/{name}/versions/{v}/activate`。详见 [examples/04_multi_version](examples/04_multi_version/)。

**Q: Worker 崩溃了怎么办？**
Worker 会自动重启。正在处理的请求会自动重试到其他 worker（最多 3 次）。异常检测会剔除不健康的 worker。心跳探测能发现卡死进程并触发自动重启。生命周期钩子在退出/异常时触发，便于告警。

## 多平台支持

| 平台 | 架构 | Wheel Tag |
|------|------|-----------|
| Linux | x86_64 | manylinux2014_x86_64 |
| Linux | aarch64 | manylinux2014_aarch64 |
| macOS | x86_64 | macosx_10_12_x86_64 |
| macOS | aarch64 (Apple Silicon) | macosx_11_0_arm64 |
| Windows | x86_64 | win_amd64 |
| Windows | aarch64 | win_arm64 |

## 开发

```bash
cargo build --release
cargo test
cd python && python -m pytest tests/
```

### 项目结构

```
.
├── src/              # Rust 核心（HTTP、推理队列、Worker 管理、ensemble、gRPC）
├── python/           # Python 包（CLI、Worker 进程、LitAPI、artifact packer）
├── tests/            # Rust 集成测试
├── examples/         # 示例模型仓库
├── benchmarks/       # 性能基准测试
├── docs/             # 文档
│   ├── architecture.md
│   ├── benchmark.md
│   ├── cli.md              # CLI 参考
│   ├── comparison.md
│   ├── configuration.md
│   ├── model-authoring.md
│   └── zh/                 # 中文文档
│       ├── architecture.md
│       ├── benchmark.md
│       ├── cli.md
│       ├── comparison.md
│       ├── configuration.md
│       └── model-authoring.md
├── Cargo.toml        # Rust 清单
└── pyproject.toml    # Python 打包（maturin）
```

## License

MIT
