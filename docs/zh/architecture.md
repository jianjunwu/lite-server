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
6. Python worker 执行 callback 管线：
   before_decode_request() → decode_request() → after_decode_request() → predict() → after_predict() → encode_response() → after_encode_response()
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

### 解耦流式（P9-1）

`DecoupledInfer`（gRPC）是 1:N 流，其通道生命周期由**模型**控制。与
`stream_predict`（worker *拉取*的生成器，耗尽即结束）不同，模型在
`predict_decoupled(data, sender)` 中收到一个异步 `sender`，且可**在流结束前
返回**——异步推送 N 个响应（逐 token / 多候选 / 进度），最终
`await sender.close()` 结束：

```
gRPC DecoupledInfer ──► Rust 以 StreamOpen.decoupled=true 开流
        │
        ▼
Worker: predict_decoupled(data, sender) 返回（通道保持打开）
        │
        ▼
sender.send(chunk) × N  ──►  DecoupledResponse{is_final=false}
        │
        ▼
sender.close()  ──►  DecoupledResponse{is_final=true}（终结帧）
```

服务端经 `server.decoupled_idle_timeout_secs`（默认 300 秒；0 = 关闭）或客户端
断连（cancel 传播至 worker，sender 失效）回收未关闭的通道。未实现
`predict_decoupled` 的模型返回 `FailedPrecondition`。

> **背压。** Rust↔worker 链路为 ZMQ `PAIR` + 阻塞发送——慢 worker 使发送方阻塞
> 而非静默丢（P9-1 已核实）。Rust 进程内到 gRPC 客户端的 `mpsc(64)` 桥在慢
> *端客户端*时驱逐流（load shedding）——这是所有流共有的既有行为，归可靠性
> 阶段（P-FLOW）；DecoupledInfer 原样继承。

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

模型可以实现 `prefill()` / `step()` / `has_finished()` 钩子实现持续批处理（continuous batching）：序列动态加入/离开正在进行的推理，而非等待 batch 收集完成。适用于 LLM 等可变长度生成场景。

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
| Callbacks | `callbacks/` | 推理管线回调（before_decode_request / after_decode_request / after_predict / after_encode_response）+ 生命周期钩子（before_setup / after_setup / before_teardown / after_teardown） |
| Context | `context.py` | 请求上下文（RequestContext、RequestMeta），含 request_id、client_ip 等 |
| Pipeline | `pipeline.py` | 数据预处理/后处理流水线 |
| Route | `route.py` | `@route` 装饰器，声明自定义 HTTP 路由 |
| Server Proxy | `server_proxy.py` | Worker 内 loopback HTTP 代理（回连 Rust 内核） |
| Response | `response.py` | 推理响应数据模型 |
| Exceptions | `exceptions.py` | Python 侧异常定义 |
| Worker | `worker/inference.py` | 加载和运行模型的 Worker 进程 |
| Proto | `proto/` | Python protobuf 生成代码 |
| 分析器 | `analyzer/` | 静态模型分析（static、report） |
| 压测 | `benchmark/` | 负载压测与 bidi 流式基准 |
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
  │     ├── reconcile 任务（auto 模式下管理版本生命周期）
  │     ├── 文件监听器（目录事件近实时触发 reconcile）
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
- Python Callback 生命周期钩子：`before_setup` / `after_setup` / `before_teardown` / `after_teardown`（异常隔离，失败不传播）
- 加权路由支持金丝雀发布（多版本按权重分流）
- 自适应批处理根据队列深度动态调整 batch_timeout

### Worker 选择与 sequence_id 粘性路由

默认调度是无状态的：unary `Infer` 经 per-(model,version) 队列投递到**最少负载**的 worker（跳过被驱逐者）；流式/batch **直连**一个随机未被驱逐的 worker。请求可经 `sequence_id`（HTTP header `x-sequence-id`，gRPC `InferRequest`/`StreamInferRequest`/`BidiOpen.sequence_id`）开启**跨请求 worker 粘性**：

- 服务端维护 per-process 的 `SequenceRegistry`（`sequence_id → (model, version, worker_id)`）。命中且该 worker 仍注册、未被驱逐→粘到该 worker；未命中/被驱逐→回退正常调度。**可用性优先于粘性**——回退从不拒绝请求。
- 队列路径在派发时结合实时负载与健康解析亲和：粘性 worker 过载（负载超过 `server.balance_abs_threshold` / `balance_rel_threshold`）→回退 power-of-two 选择；worker 下线→其 sequence 经 rendezvous hashing 重分布（平滑重哈希，迁移有界、无热点）。流式仅用核心粘性（无 per-worker 负载信号）。
- **不带** `sequence_id` 的请求调度与现状**完全一致**——该特性纯可选。

### Envelope hints（B3）：priority / affinity_key / direct_worker_id

unary infer（HTTP + gRPC）另识别三个可选调度 hint，经 header（HTTP）或 proto
`headers` map（gRPC）携带：

- **`x-lite-priority: <int>`**——多级优先级队列（P-FLOW B1）；值越大越先派发
  （并列 FIFO）。缺省 = 0 = 纯 FIFO。
- **`x-lite-affinity-key: <string>`**——无状态内容亲和路由：key 经 rendezvous
  哈希落到存活 worker，同一 key 确定性落同一 worker（无需服务端注册表；
  worker 离开时平滑再分布）。`sequence_id` 是其特例且优先；与 `sequence_id`
  不同，它不带跨请求注册表粘性、也不走负载阈值回退（纯哈希）。
- **`x-lite-worker-id: <u32>`**——直连模式：钉到指定 worker 下标（"gateway
  citizen" 扩展——服务端不接管决策）。提交即校验：下标越界或 worker 已被
  剔除 → `400` / gRPC `InvalidArgument`（坏 pin 绝不静默改路由）。提交后到
  派发前 worker 转不可用（剔除竞态/重试排除）→ warn 降级正常挑选——可用性
  优先于 hint。

所有 hint 均为未认证调度 hint，不构成隔离边界（同 `sequence_id`）。仅队列
派发路径（unary infer）消费；batch/stream/bidi 直连 worker，忽略这些 hint。
原预留的 `x-lite-expected-cost` 未兑现即移除——容量感知 picker 落地时可
additive 重新引入。

> **安全与隔离**：`sequence_id` 是**未认证的调度 hint，不构成隔离边界**。客户端只能影响自身请求的落点（猜/复用 sequence id），不能借此跨模型/跨租户访问——隔离仍由 access_control + worker 模型边界保证。错误响应不回显内部 `worker_id`/registry 结构。
>
> **多实例**：`SequenceRegistry` 为**per-process**——多副本下同一 `sequence_id` 在不同实例可能落不同 worker。全局粘性需上游会话亲和（如网关 sticky cookie）；本服务仅提供实例内粘性。

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
Python：protobuf 反序列化 → before_decode_request() → decode_request() → after_decode_request() → [batch()] → predict() → [unbatch()] → after_predict() → encode_response() → after_encode_response()
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

lite-server 的版本/文件热更新由三个独立机制协作完成：

| 机制 | 配置位置 | 管什么 |
|------|---------|--------|
| `control_mode` | `server.yaml` `orchestration` | **版本**生命周期——哪些版本加载/卸载 |
| `hot_reload` | `config.yaml`（模型级） | **文件**变更要不要响应 |
| `on_file_changed` | `model.py`（模型代码） | 文件变了**怎么**处理 |

三者是上下游接力关系：

```
control_mode                 hot_reload                on_file_changed
    │                            │                          │
    └─→ 版本进入注册表             └─→ 文件变更匹配 pattern     └─→ 返回非 None
        （版本的"入口"）               → 发 FILE_CHANGED           → 进程内刷新，不重启
                                      （文件变更的"开关"）
                                                             返回 None / 未实现
                                                               → 回退：重启 worker
```

`control_mode` 控制版本粒度，`hot_reload` 控制文件粒度，两者互不覆盖。
即使 `control_mode=explicit`，`hot_reload=true` 仍然生效——它只关心已加载版本内的文件变化。

lite-server 通过 `orchestration.control_mode` 控制模型版本的生命周期管理：

| control_mode | 行为 |
|---|---|
| `"explicit"`（默认） | 仅加载 `load_models` 中列出的模型版本，不监听目录变化 |
| `"auto"` | 后台 reconcile 任务周期扫描模型仓库 + 目录事件近实时触发 reconcile，自动加载/卸载版本 |

### auto 模式下的 reconcile

`"auto"` 模式下，reconcile 任务是版本生命周期的唯一属主：

```
1. 目录事件（新版本目录 / 版本目录消失）触发 reconcile
        │
        ▼
2. 合并窗口（reconcile_coalesce_secs，默认 2 秒）内的事件合并为一次 reconcile
        │
        ▼
3. reconcile_models()：
   ├── 自动解包 .lma 制品（增量，按 mtime）
   ├── 扫描模型仓库中的可用版本
   ├── 根据 load_policy 计算目标版本集（"all" / "latest" / "explicit"）
   ├── 卸载不在目标集中的版本
   ├── 加载缺失的目标版本
   └── 激活默认版本（如配置）
```

### 已加载版本的热刷新

对已加载且 `hot_reload: true` 的版本，文件变更走 FILE_CHANGED 路径：

```
1. 文件监听器检测到已加载版本目录内的文件变更
        │
        ▼
2. 检查模型配置：
   ├── hot_reload = false → 跳过
   └── hot_reload = true → 继续
        │
        ▼
3. 如果配置了 hot_reload_patterns（默认 ["*.py"]）：
   → 只对匹配模式的文件触发刷新，不匹配的被忽略
        │
        ▼
4. 冷却检查（hot_reload_cooldown_secs，默认 3 秒）：
   → 同一版本在冷却窗口内的重复事件被忽略
        │
        ▼
5. 向该版本的每个 worker 发送 FILE_CHANGED：
   → 各 worker 调用 on_file_changed(changed_files) 钩子
   → 钩子返回非 None = 已处理（如热更新权重，不重启）
        │
        ▼
6. 所有 worker 都报告已处理：完成，不重启
   否则（无钩子 / 返回 None / 抛异常 / 旧版 worker）：默认行为
   → 重启该模型版本的所有 worker
   → worker 重新执行 setup() 加载新代码
```

> **已于 0.7.7 移除**：`control_mode != "auto"` 时，新版本目录不再自动加载，仅记录日志提示。请改用 `control_mode: "auto"` 或通过 Admin API 显式加载。

## 限流

基于令牌桶算法（`rate_limit.rs`），支持：

- 每键独立桶（按 client_ip 或自定义 key 分流）
- 可配置 RPM（每分钟请求数）和 burst（突发容量）
- 自动清理过期桶

> **客户端 IP 与受信代理（P-XFF）。** 喂给 `key: ip` 桶的 `client_ip` 经
> `client_ip.rs` 以直连 TCP peer 为锚做 fail-safe 清洗：**非受信** peer 的
> `X-Forwarded-For` / `X-Real-IP` 一律忽略（客户端无法伪造 IP 绕过按 IP 限流）。
> 仅 `server.trusted_proxies`（默认空）中的 CIDR 被视为代理，其转发链从右向左
> 走到首个非受信 hop。gRPC 路径同款清洗。访问日志并列记录清洗后的 `client_ip`
> 与原始（截断）`X-Forwarded-For`，便于回溯归因。

配置示例见 [configuration.md](./configuration.md)。

## CORS（P-CORS）

自写 `cors_middleware`（`src/http/cors.rs`）处理 CORS——不用 `tower-http::cors`，
因为 per-model 策略覆盖需在请求时按路径解析 model，静态挂载的 CorsLayer 做不到。

- **生效策略**：per-model `policies.cors` 优先于全局 `server.cors`；皆无 → 直通（不附
  头）。admin 端点跳过（不面向浏览器）。
- **Origin 匹配**为精确匹配（规范化后：scheme/host 小写、去默认端口），子域通配
  （`*.example.com`）为显式 opt-in。禁反射、禁 `null`、禁后缀混淆——见
  [CORS 安全规则](./configuration.md#cors)。
- **预检**（`OPTIONS` + `Access-Control-Request-Method`）仅当 Origin 命中时短路返回
  `204` + CORS 头。中间件挂在 **access_control 外**（D21），故预检不触发鉴权（预检不带
  凭证），且在 observability 内，故 `204` 带 `x-request-id`。
- **实际请求**附 `Access-Control-Allow-Origin`（命中的 origin 或 `*`）、按配置的
  `Access-Control-Allow-Credentials`、`Vary: Origin`、`Access-Control-Expose-Headers`。
  `credentials: true` + `*` 拒绝。
- **WebSocket**：浏览器对 WS 握手不发预检、不强制 ACAO，故中间件无法防跨站 WS 劫持。
  WS upgrade handler 独立用同一 Origin 引擎校验（`ws_origin_allowed`）；未配置 CORS 时
  WS 安全完全依赖 `access_control`（P7-1）密钥鉴权。

## 过载保护与取消（P-FLOW）

生产服务须有显式过载与取消语义，否则高压下雪崩（§4.0.9）。P-FLOW 落地：

- **全局在途上限**（`server.max_inflight`）：超过此并发数的推理请求被拒
  （`503` / gRPC `Unavailable` + `Retry-After`）。**health/admin 端点豁免**——
  探活不能挂。由 HTTP admission 中间件（observability 内、按路径分类）与各 gRPC
  推理 handler 顶部的 guard 强制。`0` = 无限（默认，行为不变）。guard 对 unary 覆盖
  全程；对 SSE/WS/gRPC 流式在开流时释放（与在途计数中间件一致的 header 语义）。
- **队列 load shedding**：per-version 队列满 → `503` / `Unavailable` + `Retry-After`
  （HTTP header / gRPC metadata）。`ResourceExhausted` 专给限流（P3-1）——过载落 5xx 族。
- **请求大小上限**（`server.max_request_body_bytes`）：超限 → `413`（HTTP）/
  `ResourceExhausted`（gRPC，tonic 固定映射）。默认 64 MiB（67,108,864）；
  `null` = 平台默认（axum 2MB / tonic 4MB）。
- **多级优先级队列**（B1）：每个 per-version 队列是按请求 `x-lite-priority` header
  （越大越先派发，同优先级 FIFO）排序的优先级堆。无 header（默认 0）时退化为普通 FIFO，
  行为不变。**排队超时 REJECT**（`queue_timeout_secs` + `queue_timeout_action: reject`）
  对等待超 deadline 的请求返回 `503` / gRPC `Unavailable`；`delay`（默认）交给
  `request_timeout` 兜底。
- **取消传播**：任一流上客户端断连 → fire-and-forget `Cancel`（`send_raw`）通知
  worker 停止并释放资源。ensemble 子 step 共享一个取消：每层 `JoinSet` 意味着 parent
  取消（客户端断连、总预算超时、或同层兄弟 step 出错）**abort 所有在途子 step**，
  而非让 worker 为已死请求继续算。unary 断连取消有意不实现（unary 无 stream_id；
  worker 可能跑完已收请求）。

`max_inflight` / `max_request_body_bytes` 详见 [configuration.md](./configuration.md)。

## Deadline 传播与超时状态码（P-DEADLINE）

单请求预算端到端绑定，替代各处超时各自为政：

- **HTTP**：发送 `x-lite-timeout: <秒>`（相对浮点，如 `2.5`）。
- **gRPC**：发送标准 `grpc-timeout` metadata 键。
- 两者皆无，回落 `server.timeout`。

解析出的 deadline 以绝对 UNIX 纳秒时间戳传到 worker（`RequestMeta.deadline_unix_ns`），
worker 协作式检查。ensemble DAG 共享一份 parent 预算（子 step 得 parent − 已耗）；
流式为**两段式**：总时长上限 + chunk 间 idle 超时。chunk-idle 超时**恒开**（复用
`decoupled_idle_timeout_secs`，默认 300s）——卡死的流会被回收而非无界挂起，持续产
chunk 的长流不受影响；总时长上限仅在客户端显式指定 deadline 时激活（默认配置下长流
不被总时长截断）。设 `decoupled_idle_timeout_secs = 0` 可禁用 idle 回收。

**预算耗尽时的状态码**——写重试逻辑前请先读：

| 面 | 状态码 | 含义 |
|---|---|---|
| HTTP（unary / batch / stream / ensemble） | `504 Gateway Timeout` | 服务端预算（客户端指定，或 `server.timeout` 兜底）在等待 worker 时耗尽。 |
| gRPC | `DEADLINE_EXCEEDED` | 同上，gRPC 惯例。 |

**为什么是 504 而非 408：** `408` 的语义是"客户端发送请求太慢"——而这里请求
已完整到达，是*服务端*下游预算耗尽，正是 504 语义。既有 `InferenceTimeout → 504`
映射有意保留（改它会静默打破已在 504 上告警的客户端）；蓝图早期草稿的 408
在实施时被取代。实操：把 504 / `DEADLINE_EXCEEDED` 当"预算耗尽"处理——幂等请求
可退避重试；`x-lite-timeout` 应小于你自己上游的预算，让 deadline 传播而非叠加。

邻近区分：排队超时 REJECT → `503` / `Unavailable`（P-FLOW）；限流 →
`429` / `RESOURCE_EXHAUSTED`（P3-1）。

## Callbacks

Python 侧 Callback 系统（`callbacks/`）在推理管线的关键节点注入自定义逻辑：

```
before_decode_request → [decode_request] → after_decode_request → [predict] → after_predict → [encode_response] → after_encode_response
```

**数据钩子**（`before_decode_request` / `after_decode_request` / `after_predict` / `after_encode_response`）：
接收 `RequestContext`，可修改数据、提前返回 `Response`、或抛出 `HTTPException` 拒绝请求。支持 sync / async，流式模式下每个 chunk 各触发一次 `after_predict` + `after_encode_response`。

**生命周期钩子**（`before_setup` / `after_setup` / `before_teardown` / `after_teardown`）：
在请求路径之外运行，异常隔离（失败仅日志，不传播）。

**错误钩子**（`on_error`）：
请求失败时驱动，异常隔离，不掩盖原始错误。

> `middleware.py` 自 0.7.0 起已废弃，被 Callback 系统取代。内置策略回调（RequireApiKey / RateLimit / Cors / LogRequests）自 0.7.6 起改为 `config.yaml` 中的声明式 `policies` 配置，由 Rust 内核执行。
