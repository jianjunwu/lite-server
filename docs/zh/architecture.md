# 架构

[English](../architecture.md)

lite-server 是 Rust + Python 混合架构的推理服务器。Rust 内核处理所有 I/O（HTTP、gRPC、IPC、指标、文件监听），Python worker 负责模型推理。

## 整体架构

```
                          ┌─────────────────────────────────┐
                          │          lite-server             │
                          │         （Rust 内核）             │
                          │                                  │
  HTTP 请求 ─────────────►│  ┌──────────┐  ┌─────────────┐  │
  gRPC 请求 ─────────────►│  │  HTTP /   │  │  推理队列    │  │
                          │  │  gRPC     │  │ （每模型一个）│  │
                          │  │  服务器   │──│             │  │
                          │  └──────────┘  └──────┬───────┘  │
                          │                       │          │
                          │  ┌──────────┐         │          │
                          │  │  模型    │         │          │
                          │  │  注册表  │         │          │
                          │  └──────────┘         │          │
                          │  ┌──────────┐         │          │
                          │  │  指标    │         │          │
                          │  │  + 告警  │         │          │
                          │  └──────────┘         │          │
                          └───────────────────────┼──────────┘
                                                  │
                              ZMQ / UDS 传输       │
                                                  ▼
                          ┌─────────────────────────────────┐
                          │       Python Workers             │
                          │                                  │
                          │  ┌──────────┐  ┌──────────┐     │
                          │  │ Worker 1 │  │ Worker 2 │ ... │
                          │  │          │  │          │     │
                          │  │ model.py │  │ model.py │     │
                          │  └──────────┘  └──────────┘     │
                          └─────────────────────────────────┘
```

## 请求生命周期

单个推理请求的处理路径：

```
1. HTTP POST /v2/models/{name}/infer
        │
        ▼
2. axum HTTP handler 解析请求
        │
        ▼
3. 模型注册表查找活跃版本
        │
        ▼
4. 请求入队到 InferenceQueue
   （选择最空闲的 worker）
        │
        ▼
5. Worker 通过 ZMQ/UDS 获取请求
        │
        ▼
6. Python worker 执行：
   decode_request() → predict() → encode_response()
        │
        ▼
7. 响应通过 ZMQ/UDS 返回
        │
        ▼
8. Rust 内核返回 HTTP 响应给客户端
```

### Batching 模式

当 `max_batch_size > 1` 时：

```
请求 A ──┐
请求 B ──┼──► InferenceQueue ──► Batch 收集（最多 N 个请求
请求 C ──┘                        或 batch_timeout 超时）
                                      │
                                      ▼
                              predict([A, B, C])
                                      │
                                      ▼
                              响应逐个分发
```

### 流式模式

当 `stream: true` 且模型实现了 `stream_predict()` 时：

```
1. HTTP POST /v2/models/{name}/events  (SSE)
   GET  /v2/models/{name}/stream       (WebSocket)
        │
        ▼
2. Worker 调用 stream_predict() → 生成器
        │
        ▼
3. 每个 yield 的 chunk → SSE 事件 / WebSocket 帧
        │
        ▼
4. 生成器耗尽时流结束
```

## 核心组件

### Rust 内核（`src/`）

| 组件 | 文件 | 职责 |
|------|------|------|
| HTTP 服务器 | `http/` | 基于 axum 的 HTTP 服务器、路由、请求解析 |
| gRPC 服务器 | `grpc/` | 基于 tonic 的 gRPC 服务器 |
| 推理队列 | `inference_queue.rs` | 每模型请求队列、batch 组装、worker 分发 |
| 模型注册表 | `registry/` | 模型/版本生命周期、热重载、加载策略 |
| Worker 管理器 | `worker/` | Worker 进程管理、健康监控、异常检测 |
| 传输层 | `transport/` | ZMQ 和 UDS 进程间通信 |
| 指标 | `metrics/` | Prometheus 指标、时间线聚合、告警引擎 |
| 文件监听 | `watcher/` | 热重载文件系统监听器 |
| Ensemble | `ensemble.rs` | DAG 多模型流水线编排 |
| 配置 | `config.rs` | YAML 配置加载、CLI 参数覆盖 |
| 服务器 | `server.rs` | 主服务器生命周期、优雅关闭 |

### Python 包（`python/`）

| 组件 | 文件 | 职责 |
|------|------|------|
| CLI | `cli.py` | 命令行接口（serve、benchmark、init 等） |
| LitAPI | `api.py` | 增强的模型开发接口，支持钩子 |
| Worker | `worker/` | 加载和运行模型的 Worker 进程 |
| 分析器 | `analyzer/` | 性能分析工具 |
| 制品 | `artifact/` | 模型打包/解包（.lma 格式） |
| 脚手架 | `init/` | 项目初始化模板 |

## 进程模型

```
lite-server-core（主进程）
  ├── HTTP 服务器（tokio，多线程）
  ├── gRPC 服务器（可选）
  ├── 指标服务器
  ├── 模型注册表
  │     ├── 监听线程（每模型一个）
  │     └── 推理队列（每模型一个）
  └── Worker 进程（子进程）
        ├── Worker 1 → Python 解释器 → model.py
        ├── Worker 2 → Python 解释器 → model.py
        └── ...
```

- 每个 worker 是独立的 Python 子进程
- Worker 通过 ZMQ 或 UDS 与内核通信
- Worker 崩溃后自动重启
- `max_requests` 触发定期重启防止内存泄漏
- 异常检测剔除不健康 worker（Envoy 风格的连续错误计数）
- 心跳探测检测卡死 worker 并自动重启
- 生命周期钩子在就绪/退出/异常时触发回调

## IPC 协议

Worker 使用二进制协议与 Rust 内核通信：

- **ZMQ**（默认）：ZeroMQ REQ/REP 套接字，bincode 序列化
- **UDS**：Unix 域套接字，长度前缀 bincode 帧

传输方式通过 `server.transport` 配置选择（`"zmq"` 或 `"uds"`）。

## 数据路径

```
HTTP 请求（JSON/bytes）
    │
    ▼
Rust：解析 → Bytes（零拷贝引用）
    │
    ▼
InferenceQueue：Arc<RequestMeta>（无数据拷贝）
    │
    ▼
ZMQ/UDS：bincode 序列化 → 发送给 worker
    │
    ▼
Python：bincode 反序列化 → decode_request() → predict() → encode_response()
    │
    ▼
ZMQ/UDS：bincode 序列化 → 发回
    │
    ▼
Rust：bincode 反序列化 → HTTP 响应
```

热路径使用 `Bytes`（共享缓冲区）和 `Arc<RequestMeta>`（共享元数据）避免不必要的数据拷贝。

## 可观测性栈

```
Prometheus ◄── /metrics 端点
    │
    ├── QPS（每秒请求数）
    ├── 延迟（P50/P90/P99）
    ├── 队列深度
    ├── TTFT（首 token 时间）
    ├── TBT（token 间隔时间）
    ├── Batch 大小
    ├── Worker 剔除数
    └── 活跃连接数

告警引擎 ◄── 内置规则
    │
    └── 指标流异常检测

时间线 ◄── 历史采样（可选）
    │
    └── 每模型指标趋势
```

## 热重载流程

```
1. 监听器检测到模型目录文件变更
        │
        ▼
2. 防抖（默认 1 秒）
        │
        ▼
3. 如果模型实现了 on_file_changed()：
   → 调用 on_file_changed(changed_files)
   → 模型自行处理重载逻辑
        │
        ▼
4. 否则：默认行为
   → 重启该模型的所有 worker
   → worker 重新执行 setup() 加载新代码
```
