# 框架对比：lite-server vs 主流推理服务器

[English](../comparison.md)

## 一句话总结

- **lite-server** — 高性能、轻量级、Rust+Python 混合架构。适合想要生产级特性但不想搭重基础设施的团队。
- **Triton** — NVIDIA 企业级方案。适合有大型 GPU 集群和专职 MLOps 团队的企业。
- **TorchServe** — PyTorch 官方推理框架。适合深度绑定 PyTorch 生态的团队。
- **BentoML** — 通用模型服务框架。适合需要跨平台打包部署模型的场景。
- **Ray Serve** — 分布式推理框架。适合大规模复杂多模型流水线。

## 详细对比

### 架构

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| HTTP 层 | Rust (axum/tokio) | C++ (自研) | Java (Netty) | Python (FastAPI) | Python (uvicorn) |
| 推理层 | Python 子进程 | C++ 插件 | Python 子进程 | Python | Python Actor |
| IPC 机制 | ZMQ/Protobuf | 共享内存 | TorchServe 协议 | HTTP | Ray 对象存储 |
| 进程模型 | 每 worker 独立子进程 | 单进程多后端 | Java + Python worker | 每部署一个进程 | 分布式 Actor |

### 安装与依赖

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| 安装命令 | `pip install` | 仅 Docker | Java + pip | `pip install` | `pip install` |
| 最低依赖 | Python 3.10+ | CUDA, Docker | Java 11+, Python | Python 3.8+ | Python 3.8+ |
| 需要容器 | 否 | 强烈推荐 | 否 | 否 | 否 |
| 二进制大小 | ~15MB | ~1GB | ~500MB | ~50MB | ~200MB |

### 模型管理

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| 模型格式 | Python 类 (LitAPI) | 框架特定格式 | MAR 归档 | Python 类 | Python 类 |
| 版本控制 | 多版本，支持激活/停用切换 | 多版本 | 多版本 | 手动管理 | 手动管理 |
| 热重载 | 文件监听 + 防抖 | 不支持（需重启） | 有限（模型控制 API） | 不支持 | 不支持 |
| 加载策略 | explicit、latest、all | explicit、polling | explicit | N/A | N/A |
| Ensemble/DAG | DAG 并行分层执行 | 支持（模型集成） | 不支持 | Pipeline | Deployment graph |
| 模型打包 | .lma 格式，SHA256+HMAC 签名 | 不支持 | MAR 格式 | Bento | 不支持 |

### 性能与可扩展性

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| Batching | 自适应 + 静态 | 动态 batching | 动态 batching | 手动 | 手动 |
| Continuous batching | 支持（LLM 钩子） | 支持 | 不支持 | 不支持 | 不支持 |
| Worker 调度 | 最小负载 + 异常感知 | 轮询 | 轮询 | 可配置 | Actor 调度 |
| 零拷贝数据路径 | Bytes + Arc | 共享内存 | 无 | 无 | 无 |
| 流式输出 | SSE + WebSocket + gRPC | gRPC 流式 | 不支持 | SSE | Streaming |

### 韧性

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| 异常检测 | Envoy 风格自动剔除 | 不支持 | 不支持 | 不支持 | 不支持 |
| 请求重试 | 最多 3 次，换 worker 重试 | 不支持 | 不支持 | 不支持 | 可配置 |
| Worker 回收 | max_requests 自动重启 + 抖动防惊群 | 不支持 | 不支持 | 不支持 | 不支持 |
| 心跳检测 | ZMQ 探测 + 自动 respawn | 不支持 | 不支持 | 不支持 | 不支持 |
| 生命周期钩子 | Shell + HTTP 回调（就绪/退出/异常） | 不支持 | 不支持 | 不支持 | 不支持 |
| 单请求超时 | 支持（可配置） | 支持 | 支持 | 支持 | 支持 |
| 健康检查 | 深度检查（worker + 模型状态） | 基础 | 基础 | 基础 | 基础 |

### 可观测性

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| 指标 | Prometheus（13+ 项） | Prometheus | Prometheus | Prometheus | Prometheus |
| 时间线 | 内置历史采样 | 不支持 | 不支持 | 不支持 | 不支持 |
| 告警 | 内置告警引擎 | 不支持 | 不支持 | 不支持 | 不支持 |
| 日志 | 基于 tracing 的结构化日志 | 自定义 | log4j | Python logging | Python logging |

### 平台支持

| 维度 | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|------|------------|--------|------------|---------|-----------|
| Linux x86_64 | 支持 | 支持 | 支持 | 支持 | 支持 |
| Linux aarch64 | 支持 | 支持 | 支持 | 支持 | 支持 |
| macOS | 支持 | 不支持 | 支持 | 支持 | 支持 |
| Windows | 支持 | 不支持 | 不支持 | 支持 | 不支持 |
| Python 版本 | 3.10 - 3.14 | 3.8 - 3.10 | 3.8 - 3.11 | 3.8+ | 3.8+ |

## 什么时候选 lite-server

**适合场景：**
- 想要高性能但不想搭 Docker/Java/C++ 基础设施
- 团队写 Python 但想要 Rust 级别的 HTTP 吞吐
- 开发阶段需要热重载快速迭代
- 需要内置异常检测和 worker 自愈
- 需要心跳检测自动重启卡死的 worker
- 需要生命周期钩子实现可观测性和告警（Slack、PagerDuty 等）
- 部署多版本模型做 A/B 测试
- 需要 ensemble 流水线（预处理 -> 模型 -> 后处理）
- 在 macOS 或 Windows 上开发

**考虑其他方案：**
- **Triton** — 有大型 NVIDIA GPU 集群，需要内核级 TensorRT/ONNX 优化
- **TorchServe** — 整个技术栈都是 PyTorch，需要与 TorchServe 模型归档深度集成
- **Ray Serve** — 需要跨 Ray 集群的复杂多节点分布式推理

## 性能说明

lite-server 的性能优势来自：

1. **Rust HTTP 层** — axum/tokio 处理 I/O，不受 Python GIL 限制
2. **零拷贝数据路径** — `Bytes` 共享缓冲区和 `Arc<RequestMeta>` 在热路径避免数据拷贝
3. **DashMap 无锁并发** — 模型注册表和 pending 响应表使用并发哈希表代替互斥锁
4. **自适应 batching** — 根据队列压力动态调整 batch 超时，高负载下立即派发
5. **ZMQ IPC** — Unix 上使用域套接字（UDS），Windows 上使用 TCP；Protobuf 序列化避免 JSON 开销

详见 [benchmark.md](benchmark.md) 的实测性能数据。
