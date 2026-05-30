# lite-server

高性能模型推理服务器 — Rust 内核处理 I/O，Python 负责推理。

[English](README.md)

## 为什么选 lite-server？

| | 特性 | 对你意味着什么 |
|---|------|--------------|
| **快** | Rust HTTP 内核 (axum/tokio)、零拷贝数据路径、自适应 batching | 比纯 Python 方案吞吐更高、延迟更低 |
| **稳** | 异常检测、请求重试、worker 自动回收 | Worker 自愈，不用人工盯 |
| **简** | `pip install`，写一个 `model.py` 就能上线 | 不用 Docker、不用 Java、不用编译 C++ |
| **灵** | 热重载、多版本、ensemble DAG 编排 | A/B 测试、灰度发布、多模型流水线一站搞定 |
| **明** | Prometheus 指标、时间线、告警 | 生产环境一目了然，不用猜 |
| **轻** | 单二进制 + Python worker，跨平台 | 笔记本能跑，服务器能扩 |

## 快速开始

```bash
# 1. 安装
pip install litserve  # lite-server 依赖 litserve 的 LitAPI

# 2. 创建模型
mkdir -p model_repo/my_model/1
cat > model_repo/my_model/1/model.py << 'EOF'
from lite_server import LitAPI

class MyAPI(LitAPI):
    def setup(self, device):
        self.model = lambda x: x * 2

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return self.model(x)

    def encode_response(self, output):
        return {"result": output}
EOF

# 3. 启动服务
python -m lite_server serve
# 或者: lite-server-core serve

# 4. 测试
curl -X POST http://localhost:8000/v2/models/my_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"result": 42}
```

## 与其他框架对比

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| 语言 | Rust + Python | C++ | Java + Python | Python | Python |
| 安装 | `pip install` | Docker | Java + Conda | `pip install` | `pip install` |
| 热重载 | 文件监听自动重载 | 不支持 | 有限支持 | 不支持 | 不支持 |
| 多版本 | 支持（激活/停用切换） | 支持 | 支持 | 手动管理 | 手动管理 |
| Ensemble | DAG 并行分层执行 | 支持 | 不支持 | Pipeline | Deployment graph |
| 异常检测 | Envoy 风格自动剔除 | 不支持 | 不支持 | 不支持 | 不支持 |
| 流式输出 | SSE + WebSocket + gRPC | 支持 | 不支持 | 支持 | 支持 |
| 最小开销 | ~10MB | ~500MB | ~200MB | ~50MB | ~100MB |

详见 [docs/comparison_zh.md](docs/comparison_zh.md)。

## 性能基准

2 worker、4 并发测试（1ms CPU mock 模型）：

| 服务器 | 吞吐量 | p99 延迟 |
|--------|--------|---------|
| lite-server | 1,583 req/s | 11.5 ms |
| LitServe | 531 req/s | 162.6 ms |
| lite-server-core | 1,364 req/s | 11.6 ms |

详见 [docs/benchmark.md](docs/benchmark.md)。

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

### Worker 韧性

- **异常检测** — 连续错误自动剔除 worker（Envoy 风格）
- **请求重试** — 失败请求自动重试到其他 worker（最多 3 次）
- **最小负载路由** — 请求发给当前最空闲的 worker
- **最大请求数回收** — 处理 N 个请求后自动重启 worker，防止内存泄漏
- **单请求超时** — 硬超时防止卡死请求阻塞队列

### 可观测性

- **Prometheus 指标** — QPS、P50/P90/P99 延迟、队列深度、TTFT、batch 大小、worker 剔除数
- **时间线** — 每个模型的历史指标采样
- **告警** — 内置异常检测告警规则
- **结构化日志** — 基于 tracing 的日志，包含 model/worker 上下文

## 安装

### 预编译 Wheel（推荐）

支持 Linux、macOS、Windows（x86_64 + aarch64），Python 3.9-3.14：

```bash
pip install lite-server-<version>-py3-none-<platform>.whl
```

### 从源码编译

需要 Rust >= 1.70 和 Python >= 3.9。

```bash
pip install maturin
maturin develop          # 开发构建
maturin build --release  # 发布 wheel
```

## CLI 命令

```bash
python -m lite_server serve                     # 启动推理服务器
python -m lite_server serve --config server.yaml
python -m lite_server serve --port 9000 --workers 4
python -m lite_server config-check server.yaml  # 校验配置
python -m lite_server benchmark --model my_model
python -m lite_server analyze --model my_model
python -m lite_server pack ./my_model --version 1
python -m lite_server unpack my_model_v1.lma
python -m lite_server init my_project           # 脚手架创建项目
```

## 示例

详见 [examples/](examples/) 目录：

| # | 示例 | 说明 |
|---|------|------|
| 01 | [basic](examples/01_basic/) | 最简 echo 模型 |
| 02 | [batching](examples/02_batching/) | 请求 batching，自适应超时 |
| 03 | [streaming](examples/03_streaming/) | 逐 token 流式输出（SSE/WebSocket） |
| 04 | [multi_version](examples/04_multi_version/) | 双版本切换演示 |
| 05 | [ensemble](examples/05_ensemble/) | DAG 多模型流水线 |
| 06 | [custom_endpoint](examples/06_custom_endpoint/) | 自定义 HTTP 端点 |

详见 [examples/README.md](examples/README.md)。

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v2/models/{name}/infer` | 推理（活跃版本） |
| POST | `/v2/models/{name}/versions/{v}/infer` | 推理（指定版本） |
| POST | `/v2/models/{name}/events` | SSE 流式 |
| GET | `/v2/models/{name}/stream` | WebSocket 流式 |
| GET | `/v2/models` | 列出已加载模型 |
| GET | `/v2/models/{name}/versions` | 列出版本 |
| GET | `/v2/models/{name}/ready` | 就绪检查 |
| POST | `/v2/repository/models/{name}/load` | 加载模型 |
| POST | `/v2/repository/models/{name}/unload` | 卸载模型 |
| POST | `/v2/models/{name}/reload` | 热重载 |
| POST | `/v2/models/{name}/versions/{v}/activate` | 激活版本 |
| GET | `/health` | 健康检查 |
| GET | `/info` | 服务器信息 |
| GET | `/metrics` | Prometheus 指标 |

## 配置

```yaml
# server.yaml
server:
  http_port: 8000
  host: 0.0.0.0
  transport: zmq  # zmq 或 uds

model_repository:
  path: ./model_repo

metrics:
  enabled: true

grpc:
  enabled: true
```

单模型配置（`model_repo/my_model/1/config.yaml`）：

```yaml
max_batch_size: 8
batch_timeout: 0.01
stream: false
accelerator: cpu
devices: 1
workers_per_device: 1
max_queue_size: 1000
request_timeout: 30.0
max_requests: 0  # 0 = 不启用
adaptive_batching: true
```

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
├── src/              # Rust 核心（HTTP、推理队列、Worker 管理）
├── python/           # Python 包（CLI、Worker 进程、LitAPI）
├── tests/            # Rust 集成测试
├── examples/         # 示例模型仓库
├── benchmarks/       # 性能基准测试
├── docs/             # 文档
├── Cargo.toml        # Rust 清单
└── pyproject.toml    # Python 打包（maturin）
```

## License

MIT
