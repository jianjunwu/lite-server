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
                          │  │  gRPC     │  │（每模型版本  │  │
                          │  │  服务器   │──│  一个）      │  │
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
                           ZMQ / Protobuf IPC      │
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

### 单请求路径

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
5. Worker 通过 ZMQ 获取请求
        │
        ▼
6. Python worker 执行：
   decode_request() → predict() → encode_response()
        │
        ▼
7. 响应通过 ZMQ 返回
        │
        ▼
8. Rust 内核返回 HTTP 响应给客户端
```

### Batching 路径

当用户覆写了 `batch()` / `unbatch()` 且 `max_batch_size > 1` 时：

```
1. 多个独立请求各自 decode_request()
        │
        ▼
2. InferenceQueue 收集 batch（最多 max_batch_size 个请求
   或 batch_timeout 超时）
        │
        ▼
3. batch(decoded_inputs[]) → 合并为单个 batched input
        │
        ▼
4. predict(batched_input)
        │
        ▼
5. unbatch(raw_output) → 拆分为 list[per_input_output]
        │
        ▼
6. 逐个 encode_response() → 分发给各请求
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

### 双向流模式

当模型实现了 `bidi_stream()` 时（ASR、实时对话等场景）：

```
1. WebSocket 连接建立
        │
        ▼
2. on_open(initial_data) → 初始化会话状态
        │
        ▼
3. 每个客户端消息 → on_chunk(chunk, ctx) → 可选返回响应帧
        │
        ▼
4. 连接关闭或取消 → on_close() → 清理资源
```

## Batching 模式

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

### 自适应批处理

当 `adaptive_batching: true` 时，batch_timeout 会根据队列深度动态调整：
系统从 `batch_timeout`（高负载上限）向 `min_batch_timeout`（低负载下限）滑动。流量突增时立即发送已有 batch 而不等待满超时，兼顾吞吐与延迟。

### 持续批处理

模型可以覆写 `add_sequence()` / `remove_sequence()` 实现持续批处理（continuous batching）：序列动态加入/离开正在进行的推理，而非等待 batch 收集完成。适用于 LLM 等可变长度生成场景。

## 核心组件

### Rust 内核（`src/`）

| 组件 | 文件 | 职责 |
|------|------|------|
| HTTP 服务器 | `http/` | 基于 axum 的 HTTP 服务器、路由、请求解析 |
| gRPC 服务器 | `grpc/` | 基于 tonic 的 gRPC 服务器 |
| 推理队列 | `inference_queue.rs` | 每模型版本请求队列、batch 组装、worker 分发、重试逻辑 |
| 模型注册表 | `registry/` | 模型/版本生命周期、热重载、加载策略 |
| Worker 管理器 | `worker/` | Worker 进程管理、健康监控、异常检测、生命周期钩子 |
| Worker 协议 | `worker/protocol.rs` | Rust ↔ Python worker 消息结构体定义 |
| 传输层 | `transport/` | ZMQ 进程间通信（Unix 上用 UDS，Windows 上用 TCP） |
| 流式 | `streaming/` | Protobuf 流式请求构建器（open/chunk/close/cancel） |
| 指标 | `metrics/` | Prometheus 指标、时间线聚合、告警引擎 |
| 限流 | `rate_limit.rs` | 令牌桶速率限制 |
| Ensemble | `ensemble.rs` | DAG 多模型流水线编排 |
| Callback | `callback.rs` | 服务器和推理生命周期回调（Rust trait） |
| 校验 | `validation.rs` | 模型名称、版本标识符格式校验 |
| 配置 | `config.rs` | YAML 配置加载、CLI 参数覆盖 |
| 错误 | `error.rs` | 统一错误类型定义 |
| 日志 | `logging.rs` | Tracing 日志/span 配置 |
| Proto | `proto.rs` + `proto/` | Protobuf 定义及生成代码 |
| 服务器 | `server.rs` | 主服务器生命周期、优雅关闭、文件监听 |

### Python 包（`python/lite_server/`）

| 组件 | 文件 | 职责 |
|------|------|------|
| CLI | `cli.py` | 命令行接口（serve、benchmark、init 等） |
| LitAPI | `api.py` | 模型开发基类，支持 predict / batch / stream / bidi_stream 等钩子 |
| Async LitAPI | `api_async.py` | 原生 async 版 LitAPI 基类 |
| Callback | `callback.py` | Python 侧生命周期回调 |
| Context | `context.py` | 请求上下文（RequestContext），含 request_id、client_ip 等 |
| Middleware | `middleware.py` | 请求/响应中间件管道 |
| Pipeline | `pipeline.py` | 数据预处理/后处理流水线 |
| Route | `route.py` | `@route` 装饰器，声明自定义 HTTP 路由 |
| Server Proxy | `server_proxy.py` | Worker 内 loopback HTTP 代理（回连 Rust 内核） |
| Request | `request.py` | 推理请求数据模型 |
| Response | `response.py` | 推理响应数据模型 |
| Exceptions | `exceptions.py` | Python 侧异常定义 |
| Worker | `worker/inference.py` | 加载和运行模型的 Worker 进程 |
| Proto | `proto/` | Python protobuf 生成代码 |
| 分析器 | `analyzer/` | 性能分析工具（benchmark、report、static） |
| 制品 | `artifact/` | 模型打包/解包（.lma 格式） |
| 脚手架 | `init/` | 项目初始化模板 |

### Python 原生扩展（`python/_lite_server/`）

Rust 内核编译为 Python 原生扩展（`_lite_server.abi3.so`），Worker 进程通过它零拷贝读取 protobuf 消息，避免 Python 侧手动反序列化开销。这是热路径上的关键性能优化。

## 进程模型

```
lite-server-core（主进程）
  ├── HTTP 服务器（tokio，多线程）
  │     ├── 健康探测 /health /livez /readyz /startupz
  │     ├── 推理路由 /v2/models/:name/infer
  │     ├── 版本化路由 /v2/models/:name/versions/:version/infer
  │     └── 加权路由 PUT /v2/models/:name/routing
  ├── gRPC 服务器（可选）
  ├── 指标服务器
  ├── 限流器（令牌桶）
  ├── 模型注册表
  │     ├── 文件监听器（热重载）
  │     └── 推理队列（每模型版本一个）
  └── Worker 进程（子进程）
        ├── Worker 1 → Python 解释器 → model.py
        ├── Worker 2 → Python 解释器 → model.py
        └── ...
```

- 每个 worker 是独立的 Python 子进程
- Worker 通过 ZMQ PAIR 套接字与内核通信（Unix 上用 UDS，Windows 上用 TCP）
- Worker 崩溃后自动重启
- `max_requests` 触发定期重启防止内存泄漏
- 异常检测剔除不健康 worker（Envoy 风格的连续错误计数）
- 心跳探测检测卡死 worker 并自动重启
- Worker 生命周期钩子（shell 命令 + HTTP 回调）：`on_ready`、`on_exit`、`on_error`
- 加权路由支持金丝雀发布（多版本按权重分流）
- 自适应批处理根据队列深度动态调整 batch_timeout

## IPC 协议

Worker 使用 ZeroMQ PAIR 套接字与 Rust 内核通信，序列化协议为 protobuf。Unix 上传输层使用 `ipc://`（Unix 域套接字），Windows 上回退到 `tcp://127.0.0.1:<port>`。

自定义 `@route` handler 运行在模型 worker 内，共用同一通道：未匹配到系统路由的 `/v2/models/<model>/<tail>` 路径会落到 fallback handler，进入该模型的 InferenceQueue，像推理请求一样分发到 worker。路由 handler 可通过 `ctx.server` 经 loopback HTTP（`server_proxy.py`）回连 Rust 内核（registry 查询、跨模型推理）。

## 数据路径

### 推理请求

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
ZMQ：protobuf 序列化 → 发送给 worker
    │
    ▼
Python：protobuf 反序列化 → decode_request() → [batch()] → predict() → [unbatch()] → encode_response()
    │
    ▼
ZMQ：protobuf 序列化 → 发回
    │
    ▼
Rust：protobuf 反序列化 → HTTP 响应
```

热路径使用 `Bytes`（共享缓冲区）和 `Arc<RequestMeta>`（共享元数据）避免不必要的数据拷贝。Python 侧通过原生扩展 `_lite_server.abi3.so` 零拷贝读取 protobuf 消息。

### 流式请求

```
HTTP POST /v2/models/{name}/events  (SSE)
GET  /v2/models/{name}/stream       (WebSocket)
    │
    ▼
Rust：stream_open(stream_id, data) → ZMQ
    │
    ▼
Python：stream_predict() 或 bidi_stream() → yield chunks
    │
    ▼
每个 chunk → ZMQ → Rust → SSE event / WebSocket frame
    │
    ▼
流结束 → stream_close(stream_id)
```

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
1. 文件监听器检测到模型目录文件变更
        │
        ▼
2. 防抖（默认 1 秒）
        │
        ▼
3. 检查模型配置：
   ├── hot_reload = false → 跳过
   └── hot_reload = true → 继续
        │
        ▼
4. 如果配置了 hot_reload_patterns：
   → 只对匹配模式的文件触发重载
   → 不匹配的文件变更被忽略
        │
        ▼
5. 如果模型实现了 on_file_changed()：
   → 调用 on_file_changed(changed_files)
   → 模型自行处理重载逻辑（如热更新权重）
        │
        ▼
6. 否则：默认行为
   → 重启该模型版本的所有 worker
   → worker 重新执行 setup() 加载新代码
```

## 限流

基于令牌桶算法（`rate_limit.rs`），支持：

- 每键独立桶（按 client_ip 或自定义 key 分流）
- 可配置 RPM（每分钟请求数）和 burst（突发容量）
- 自动清理过期桶

配置示例见 [configuration.md](./configuration.md)。

## 中间件

Python 侧支持请求/响应中间件管道（`middleware.py`），在 `decode_request()` 之前和 `encode_response()` 之后执行。典型用途：请求日志、认证鉴权、请求/响应改写。
