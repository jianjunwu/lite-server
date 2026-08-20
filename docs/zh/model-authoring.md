# 模型开发指南

本指南介绍如何为 lite-server 编写模型代码。模型是实现 `LitAPI` 接口的 Python 类。

[English](../model-authoring.md)

## 快速开始

```python
from lite_server import LitAPI

class MyModel(LitAPI):
    def setup(self, device):
        """加载模型权重和初始化资源。"""
        self.model = load_my_model()

    def decode_request(self, request):
        """解析原始 HTTP 请求体。"""
        return request.get("input", "")

    def predict(self, x):
        """运行推理。接收解码后的输入，返回输出。"""
        return self.model(x)

    def encode_response(self, output):
        """将预测结果格式化为 HTTP 响应体。"""
        return {"result": output}
```

保存为 `model_repo/{model_name}/{version}/model.py`。

## 目录结构

```
model_repo/
  {model_name}/
    {version}/
      model.py          # 必需：LitAPI 子类（ensemble 模型除外）
      config.yaml        # 可选：模型配置（ensemble 模型通过此文件定义）
```

- `model_name`：字母、数字、下划线、连字符，最长 64 字符（如 `my_model`、`resnet-v2`）。点号 `.` 不允许。
- `version`：字母、数字、点号、下划线、连字符，最长 64 字符。必须以字母或数字开头，不能以点号开头或结尾，不能含 `..`（如 `1`、`v2`、`latest`、`1.0.0`）

对于 **ensemble 模型**，`model.py` 可以省略——只需在 `config.yaml` 中定义顶层 `ensemble` 字段即可，模型完全由配置描述。DAG 支持 unary 步骤、**末步流式**（输出步骤 `stream: true`）、**管道流式**（流式步骤逐 chunk 级联）、容错（`on_error: skip`、`retries`）、每步 `timeout_secs`/`params`/`when` 条件、嵌套 ensemble、命名 DAG 集合（`dags`，经 `x-lite-dag` 头选择）以及 **MIMO** 命名多输入/多输出（KServe 信封 wire、`inputs` 声明与 `step.outputs` 投影）。ensemble 也接受原始字节根请求（字节直送第一层，binary 值的引用方式有限制）——见[原始字节 / Tensor 请求](../protocol.md)的 *Ensemble 模型* 一节。

执行模型差异：unary 步骤走常规推理队列（受 `max_queue_size` 限制）；**流式步骤绕过队列**（直连流式路径，与其它流式端点语义一致），并发流式 DAG 数由全局旋钮 `server.max_concurrent_streaming_dags` 限制（默认 128，耗尽立即 429）。

DAG 也可用 **Python 声明**（E9-A）：在 `config.yaml` 旁的 `dag.py` 中用 `lite_server.ensemble` 的 `EnsembleDAG` 声明（见 [examples/05_ensemble](../../examples/05_ensemble)），序列化为等价的手写配置；`lite-server analyze` 会对声明与 `config.yaml` 做一致性检查并报告漂移（LS112）。服务端执行的是 `config.yaml`——Python 声明只是编写面，绝不执行。

模型根目录（`{model_name}/`）下还可以放置 `requirements.txt`（Python 依赖）和 `README.md`，打包为 `.lma` 时会自动包含。

## LitAPI 接口

### 必需方法

#### `setup(self, device)`

Worker 启动时调用一次。在此加载模型和资源。

```python
def setup(self, device):
    self.device = device
    self.model = torch.load("weights.pt", map_device=device)
    self.model.eval()
```

- `device` 格式为 `{accelerator}:{index}`（如 `"cpu:0"`、`"cuda:0"`、`"cuda:1"`、`"rocm:0"`、`"mps:0"`）。由 `config.yaml` 中的 `accelerator`（默认 `"cpu"`）、`devices` 和 `workers_per_device` 字段控制。`devices` 接受整数（`4` → 设备 0-3 轮询）、卡号列表（`[1, 3]` → 只用这些卡）或按卡计数 map（`{ "1": 2, "3": 1 }`）。框架不自动检测硬件——`device` 是一个透传标签，由模型的 `setup()` 负责解释。
- 存储在 `self` 上的资源在 worker 生命周期内持续存在

#### `decode_request(self, request)`

将原始 HTTP 请求体（JSON 字典）解析为模型期望的格式。

```python
def decode_request(self, request):
    return {
        "text": request["text"],
        "max_length": request.get("max_length", 128),
    }
```

**原始字节请求（0.8.3）**：当客户端发送非 JSON Content-Type（如
`application/octet-stream`）时，`request` 参数为原始 `bytes`，而非
JSON 字典。用 `isinstance` 分支处理：

```python
def decode_request(self, request, ctx):
    if isinstance(request, bytes):
        # 原始 tensor 字节：从 header 读 shape/dtype
        h = ctx.meta.headers
        dtype = np.dtype(h["x-tensor-dtype"])
        shape = tuple(int(d) for d in h["x-tensor-shape"].split(","))
        return np.frombuffer(request, dtype=dtype).reshape(shape)
    # JSON 路径
    return {"prompt": request["prompt"]}
```

详见 [原始字节 / Tensor 请求](../protocol.md)。

#### `predict(self, x)`

运行推理。接收 `decode_request()` 的输出。

```python
def predict(self, x):
    tokens = self.tokenizer(x["text"], max_length=x["max_length"])
    return self.model(**tokens)
```

启用批处理时（`max_batch_size > 1`），`x` 是解码输入的**列表**：

```python
def predict(self, x):
    # 批处理激活时 x 是列表
    if isinstance(x, list):
        return [self._infer(item) for item in x]
    return self._infer(x)
```

**批处理模式下访问每请求上下文。** `batch`、`unbatch` 以及 `predict`（批处理激活时）都可以声明 `ctx` 参数 —— 注入的是与输入**按位置对齐**的 `list[RequestContext]`（每个批内请求对应一项）：

```python
def batch(self, inputs, ctx):
    for c in ctx:
        self.logger.info("batching request %s", c.meta.request_id)
    return torch.stack(inputs)

def predict(self, batched, ctx):
    # ctx[i] 对应 inputs[i]；写入 ctx[i].state 是逐项隔离的
    return self.model(batched)

def unbatch(self, output, ctx):
    return list(output)
```

`ctx[i]` 始终与 `inputs[i]` 对齐 —— **不要**在 `batch` 内重排输入，否则结果会写回错误的请求。不声明 `ctx` 时行为与之前完全一致（该列表被忽略）。

#### `encode_response(self, output)`

将预测输出格式化为 HTTP 响应体:

- `dict` / `list` 等可 JSON 序列化的值按 JSON 输出(紧凑格式;NaN/Infinity 序列化为 `null`)。
- `str` 按 UTF-8 原文发送 —— **不**加 JSON 引号。非 JSON 负载请搭配 `media_type`(如 `Response(content=html, media_type="text/html")`)。
- `bytes` / `bytearray` 原文发送(如图片、protobuf 负载)。

```python
def encode_response(self, output):
    return {"prediction": output.tolist(), "confidence": float(output.max())}
```

### 可选方法

#### `stream_predict(self, request)`

流式输出生成器。每个 yield 的值通过 SSE/WebSocket/gRPC 作为 chunk 发送。

```python
def stream_predict(self, request):
    prompt = request.get("prompt", "")
    for token in self.model.generate(prompt):
        yield {"token": token}
        time.sleep(0.02)  # 模拟生成延迟
```

在 `config.yaml` 中启用流式：

```yaml
stream: true
```

如果未实现 `stream_predict()`，服务器回退到 `predict()` 并将结果作为单个 chunk 发送。

#### `before_decode_request(self, ctx)`

在 `decode_request()` 之前对原始请求调用。用于鉴权、日志或请求修改。接收单个 :class:`RequestContext` 参数（与 Callback 钩子契约一致）。

```python
def before_decode_request(self, ctx):
    self.logger.info(f"Request from {ctx.meta.client_ip}: {ctx.meta.request_id}")
    if not self._check_auth(ctx.meta.headers):
        raise PermissionError("Unauthorized")
    return ctx.request
```

``ctx.meta`` 是 `RequestMeta` 对象，包含：`route`、`method`、`headers`、`query`、`client_ip`、`request_id`、`timestamp_ns`。其中 `method` 和 `query` 主要用于自定义路由处理器。

#### `after_encode_response(self, ctx)`

在 `encode_response()` 之后、发送给客户端之前调用。用于响应修改或日志。流式路径中也会调用（每个 chunk 编码后）。

```python
def after_encode_response(self, ctx):
    ctx.response["latency_ms"] = (time.time_ns() - ctx.meta.timestamp_ns) / 1_000_000
    return ctx.response
```

要附加自定义 HTTP 响应头，使用 :meth:`ctx.respond() <lite_server.RequestContext.respond>`：

```python
def after_encode_response(self, ctx):
    return ctx.respond(
        ctx.response,
        headers={"X-Request-ID": ctx.meta.request_id},
    )
```

#### `on_file_changed(self, changed_files)`

当模型目录中的文件变化时调用（热更新）。覆盖以实现自定义重载逻辑。

```python
def on_file_changed(self, changed_files):
    if any(f.endswith(".pt") for f in changed_files):
        self.logger.info("Reloading model weights...")
        self.model = torch.load("weights.pt")
```

如果未覆盖，默认行为是重启 worker（重新运行 `setup()`）。

注意：

- 单次 `FILE_CHANGED` 往返的超时默认 60 秒（可经 server.yaml 的
  `tunables.file_changed_timeout_secs` 调整）。任一 worker 超时、出错或未返回
  `{"handled": true}` 时，服务端回退为整版本重启。
- 同一模型/版本两次热重载之间有 3 秒冷却期（`tunables.hot_reload_cooldown_secs`）。

#### `teardown(self)`

模型卸载时调用。在此释放资源。

```python
def teardown(self):
    del self.model
    torch.cuda.empty_cache()
```

自 0.8.0 起该方法在生产中真正执行：模型卸载、重载、LRU 驱逐与服务器优雅
关闭（SIGTERM/SIGINT）时，服务端向 worker 发送 stop 消息，worker 在退出前
运行 `teardown()`（位于 `before_teardown` / `after_teardown` 回调之间）。
它必须在 `worker_kill_timeout`（默认 10 秒）内完成——超时的 worker 会在
teardown 中途被 SIGKILL。

以下路径**不会**执行 `teardown()`：worker 被直接杀死的情形——崩溃/OOM、
健康检查击杀 + respawn、服务器自身死亡（worker 的父进程看门狗经
`os._exit` 硬退出）。切勿把不可丢失的持久化逻辑放在这里。

## Callbacks 回调系统

Callbacks 是一种**可组合的、声明式的**拦截推理请求生命周期的方式。与内联的 `before_decode_request`/`after_encode_response` 钩子不同，Callbacks 是独立的类，可以被复用、共享并跨模型组合。

### Callback 基类

继承 `Callback` 并覆盖你关心的钩子。所有钩子都有默认的 no-op 实现 — 只定义你需要的方法。数据钩子接收单个 `ctx`（`RequestContext`）参数，可以是同步或 `async def`。

```python
from lite_server import Callback

class MyCallback(Callback):
    def before_decode_request(self, ctx):
        """在 decode_request 之前对原始请求调用。"""
        ctx.request["_timestamp"] = ctx.meta.timestamp_ns

    def after_predict(self, ctx):
        """在 predict 之后、encode_response 之前调用。"""
        ctx.output["_latency_ns"] = time.time_ns() - ctx.meta.timestamp_ns

```

**管线阶段**（正常路径）：

```
before_decode_request → decode_request → after_decode_request → predict → after_predict → encode_response → after_encode_response
```

任意阶段或钩子抛出异常时，管线短路到 ``on_error``，之后返回错误响应给客户端。

| 钩子 | 触发时机 | 读写字段 |
|------|---------|---------|
| `before_decode_request` | 原始请求，`decode_request` 之前 | `ctx.request` |
| `after_decode_request` | `decode_request` 之后，`predict` 之前 | `ctx.input` |
| `after_predict` | `predict` 之后，`encode_response` 之前（流式时每个 chunk） | `ctx.output` |
| `after_encode_response` | `encode_response` 之后，发送前（流式时每个 chunk） | `ctx.response` |
| `on_stream_close` | 流式结束（每个流触发一次） | `ctx` + `reason`：`"done"` \| `"error"` \| `"cancel"`；可读 `ctx.stream_stats` |
| `after_batch` | batch 模式：`batch()` 之后、`predict()` 之前（整批张量） | `ctx_list` + `batched`；抛 `HTTPException` = 整批拒绝 |
| `after_unbatch` | batch 模式：`unbatch()` 之后（per-item 输出） | `ctx_list` + `outputs` |
| `on_error` | 任意钩子或阶段抛出异常时 | `ctx` + `exc`（异常对象） |
| `before_setup` | `LitAPI.setup()` 之前 | `(config, device)` |
| `after_setup` | `LitAPI.setup()` 完成后 | `(lit_api)` |
| `before_teardown` | `LitAPI.teardown()` 之前（模型卸载 / worker 关闭时） | `(lit_api)` |
| `after_teardown` | `LitAPI.teardown()` 成功完成后（卸载完成） | `(lit_api)` |

数据钩子可以原地修改 `ctx`，也可以返回替换值（返回 `None` 表示透传）。

### RequestContext

| 字段 | 内容 |
|------|------|
| `ctx.meta` | `RequestMeta`：HTTP 头、路由、客户端 IP、请求 ID、时间戳 |
| `ctx.request` / `ctx.input` / `ctx.output` / `ctx.response` | 各阶段的管线值 |
| `ctx.state` | 跨钩子共享的**每请求**暂存字典 — 用它，**不要**用 `self` 属性（在并发请求间共享） |
| `ctx.early` | 设置后管线短路 |
| `ctx.mode` | 场景标注：`"unary"` \| `"stream"` \| `"bidi"` \| `"decoupled"` \| `"batch"` \| `"cb"`（`@route` 模型为 `None`）— 例如缓存类据此跳过流式 |
| `ctx.stage` | 当前管线阶段（`decode_request` / `predict` / `batch_predict` / `encode_response`）— `on_error` 读取它判断哪个阶段抛错 |
| `ctx.stream_stats` | uni-stream 的 `{chunks, bytes}` 统计；由流消费逻辑填充，在 `on_stream_close` 读取 |
| `ctx.elapsed_ms()` | 请求开始以来的毫秒数（基于 `meta.timestamp_ns`） |
| `ctx.deadline_remaining_ms()` | 距请求 deadline 的毫秒数；无 deadline 为 `None`，过期后为负数 — 每 chunk 检查可用于协作式停流 |

### Early Return 与参数校验

- **Early return**（如缓存命中）：在任意钩子中调用 `ctx.respond(body, status_code=..., headers=...)` 或返回一个 `Response`。后续阶段和之前链上的剩余钩子被跳过。终链 `after_encode_response` 例外：即使已有钩子 `respond(...)`，所有注册的钩子仍会执行——其后没有阶段，respond 只是附加响应头而非短路（保证后续的校验/审计钩子不因注册顺序失效）。
- **参数校验 / 拒绝**：在任意钩子中抛出 `HTTPException`（`BadRequestError`、`UnauthorizedError` 等）。客户端收到对应状态码的结构化错误 — 数据钩子的异常**不会**被吞掉。
- 生命周期钩子（`before_setup` / `after_setup` / `before_teardown` / `after_teardown`）和 `on_error` 保持异常隔离：失败只记日志，不传播（`on_error` 自身的异常也不会掩盖原始错误）。

```python
from lite_server import Callback, BadRequestError

class Validator(Callback):
    def before_decode_request(self, ctx):
        if "input" not in (ctx.request or {}):
            raise BadRequestError("missing field", param="input")

class Cache(Callback):
    def before_decode_request(self, ctx):
        hit = self._cache.get(key(ctx))
        if hit is not None:
            ctx.respond(hit, headers={"X-Cache-Hit": "1"})
```

### Callback 声明方式

Callback 可通过两种方式声明，两者可组合使用（类属性优先）：

**`config.yaml`**——每条目是类路径字符串（无参构造）或**单键
map** `{path: kwargs}`（传构造参数），与类属性路径同构：

```yaml
callbacks:
  - my_package.callbacks.AuditLogger            # 无参
  - my_package.callbacks.MetricsCollector       # 无参
  - lite_server.callbacks.JsonSchemaValidator:  # map 条目 → cls(**kwargs)
      input_schema:
        type: object
        required: [prompt]
        properties:
          prompt: { type: string, minLength: 1 }
```

内置类位于 `lite_server.callbacks`（`JsonSchemaValidator` 需要
`pip install miraserver[validation]`，详见上文 Schema 校验小节）。

> 导入失败或 0.7 之前的旧钩子签名会在加载时响亮报错 —— 被静默跳过的
> callback 可能意味着鉴权/校验逻辑从未执行。

### 完整示例：审计日志

```python
"""审计日志 callback：记录每个请求的输入/输出和延迟。"""
from lite_server import Callback

class AuditLogger(Callback):
    def before_decode_request(self, ctx):
        ctx.request["_audit_id"] = ctx.meta.request_id

    def after_predict(self, ctx):
        print(f"[AUDIT] request_id={ctx.meta.request_id} latency={ctx.elapsed_ms():.2f}ms")

    def before_teardown(self, lit_api):
        print(f"[AUDIT] model torn down, class: {type(lit_api).__name__}")
```

### 内置类：JsonSchemaValidator（Schema 校验）

`lite_server.callbacks.JsonSchemaValidator` 对请求体（`before_decode_request`，
`decode_request` 之前）和响应体（`after_encode_response`，`encode_response` 之后）
做 JSON Schema 校验 — 纯声明式，模型代码零改动。两个 schema 描述的都是
**线上载荷**（客户端发来的 / 客户端收到的）：非法请求在 400 拒绝，
任何模型代码（含 decode）都不会看到它：

```yaml
# config.yaml — 需要 `pip install miraserver[validation]`
callbacks:
  - lite_server.callbacks.JsonSchemaValidator:
      input_schema:
        type: object
        required: [prompt]
        additionalProperties: false
        properties:
          prompt: { type: string, minLength: 1, maxLength: 4096 }
          max_tokens: { type: integer, minimum: 1, maximum: 2048 }
      output_schema:                 # 可选；一并校验模型输出
        type: object
        required: [text]
```

- **失败 → 结构化 400**：`param` 为单条 best-match 错误的 JSON Pointer
  （前缀 `body/`，如 `body/prompt`），`message` 为错误原文。schema 草稿按
  `$schema` 自动选择（缺省 Draft 7）；畸形 schema 在加载时响亮拒绝 —
  静默跳过意味着校验从未执行。
- **输出校验范围**：unary/batch 及 custom route 响应 — 流式 chunk 是增量
  JSON 必不匹配，靠 `ctx.mode` 跳过。
- **跳过规则**：`ctx.request` 恒为已解析的 JSON 请求体，请求侧所有值都
  参与校验 — 标量 / `null` 请求体会触犯 `object`/`array` schema 的顶层
  类型。响应侧纯文本 / bytes 直通载荷是真非 JSON，`object`/`array`
  schema 对其跳过（顶层标量 schema 如 `type: string` 仍正常校验值本身）。
  未配置 `input_schema`/`output_schema` 的对应方向不校验。batch 模式按
  item 独立校验。
- **Custom route**：validator 在路由上同样可用 — `before_decode_request` 在路由
  handler 之前执行，`input_schema` 以同样方式拒绝非法路由请求体；
  `output_schema` 校验路由的（完整）响应载荷。

### 策略（Policies）

鉴权、限流、跨域、访问日志属于 HTTP 层关注点，在模型 config.yaml 中声明，
由 Rust 服务端按模型（精确到版本）执行：

```yaml
# model_repo/my_model/1/config.yaml
policies:
  # API Key 鉴权：默认从 X-API-Key 头读取；keys 为空 = 任意非空值即通过。
  # ${VAR} 形式从环境变量读取密钥，变量未设置时加载失败（fail-closed）。
  auth: { header: "X-API-Key", keys: ["${API_KEYS}"] }

  # 速率限制：key="route" 按路由共享配额，key="ip" 按客户端 IP
  rate_limit: { requests_per_minute: 60, key: ip, burst: 100 }

  # 跨域
  cors:
    allow_origins: ["https://example.com"]
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]

  # 访问日志（方法、路径、状态码、耗时，含被拒绝的请求）
  request_log: {}
```

### 预热 Warmup（P-WARM）

引擎会在首次请求时惰性初始化（CUDA graph capture、`torch.compile`、
分配器缓冲池）——模型加载、扩容或滚动升级后的第一个用户请求可能卡顿
20–30s。预热在加载阶段就跑一遍 dummy 推理，把这笔开销提前到版本接收
流量之前。预热**默认关闭**；一旦开启会阻塞就绪（D33）：版本停留在
`warming_up` 状态（`/readyz` 返回 503、gRPC health 为 `NOT_SERVING`、
`/startupz` 为 `initializing`），预热完成后才翻转为 `ready`。若预热失败
（dummy 输入错误、推理返回错误或超时），版本被标记为 `failed` 并带上
`last_failure` 原因，而不是放出冷模型。

dummy 输入是放在模型目录旁的原始 `/predict` 请求体文件，原样走正常推理
路径派发：

```yaml
# model_repo/my_model/1/config.yaml
policies:
  warmup:
    enabled: true
    samples:                            # 每样本一个文件，覆盖多种输入形状/batch（M7）
      - input_ref: warmup/input.json    # 相对模型目录的路径
        iterations: 3                   # 该样本跑 N 次（默认 1）
    timeout_secs: 30.0                  # 0 = 使用 request_timeout
```

```json
// model_repo/my_model/1/warmup/input.json —— 与客户端 POST 的请求体一致
{ "prompt": "hello", "max_tokens": 8 }
```

覆盖、恢复与可观测语义：

- **每个 worker 都会被预热**(`scope: worker`，默认）：每个 worker 进程
  有独立的引擎状态，因此全量样本会 pin 到每个 worker 上各跑一遍。
  `scope: version` 则保持配置总量（Σ samples×iterations）不变，轮转
  分摊到各 worker。
- **respawn 重暖**(`respawn: true`，默认）：崩溃/健康剔除后的替补
  worker 在版本回到 `ready` 之前先完成重暖（pin 到其槽位）。重暖失败
  会把冷槽位熔断摘出——版本保持 `degraded`，熔断器的半开试探仍可在
  后续真实流量上将其恢复——并计入
  `liteserver_worker_respawn_failures_total{reason="warmup"}`；绝不会
  把版本标记为 `failed`（运行期恢复 ≠ 加载失败）。
- **预算与节奏**:`timeout_secs` 约束单次 dummy 推理；
  `total_timeout_secs`（0 = 无）约束整个预热运行。`concurrency`
  （默认 1 = 串行）并行化 worker 组内的 dummy 推理；`retries`（默认
  0 = 快速失败）在同一 worker 上重跑失败样本，间隔 500ms。
- **指标**:`liteserver_model_warmup_duration_seconds` 与
  `liteserver_model_warmup_total{status=success|failure|timeout}`。
  预热流量**不会**计入 `liteserver_inference_duration_seconds`、
  `liteserver_batch_size` 或 `liteserver_worker_inference_total`——
  这些序列保持纯真实流量信号。
- **`/predict` 之外**（按样本）:`route` 可指定自定义 `@route` handler
  （直连 pin 的 worker 派发，绕过队列）;`headers` 附加请求头。
  `mode: stream` 通过真实 `StreamOpen` 暖流式 TTFT 路径——
  `completion: first_chunk`（默认）以首个 chunk 判定并取消流（成本
  有界）,`completion: drain` 消费至 `Done`。error 帧与其他预热失败
  一样使加载失败。bidi 与 decoupled 流不可暖（样本格式表达不了 chunk
  序列；decoupled 流没有 `Done` 帧）。

> 0.7.6 起，`RequireApiKey` / `Cors` / `RateLimit` / `LogRequests` 四个
> Python policy callback 已移除——它们与 Rust 侧执行是双实现，且按 worker
> 声明存在一致性隐患（"最后声明者胜"）。在 ``callbacks:`` 列表中引用这些
> 类会在加载时报错并给出迁移指引。

### Hook 归属（0.8.0）

所有请求 hook 都定义在 `Callback` 子类上 — `LitAPI` 只承载管线阶段
（`setup` / `decode_request` / `predict` / `encode_response` + 各模式方法）。
0.7–0.8 的 `LitAPI.on_request` / `on_response` 已在 0.8.0 移除：在模型类上
定义 hook 是加载期错误，报错信息会指向 `Callback` 迁移。Callback 通过
config.yaml 的 `callbacks:` 字段或 `LitAPI.callbacks` 类属性注册。

参见 [examples/14_lifecycle_hooks](../../examples/14_lifecycle_hooks/) 获取可运行示例。

## 自定义路由（`@route`）

用 `@route` 装饰器在模型上声明额外的 HTTP 端点。它们挂载在
`/v2/models/<model>/<tail>` 下，通过与推理相同的通道分发到该模型的
worker — 不需要独立进程。

```python
from lite_server import LitAPI, route
from lite_server.response import Response

class PetsAPI(LitAPI):
    @route.get("/pets/{pet_id}")
    def get_pet(self, ctx):
        pet_id = int(ctx.state["path_params"]["pet_id"])
        pet = self.pets.get(pet_id)
        if pet is None:
            return Response(content={"error": "pet not found"}, status_code=404)
        return pet
```

处理器接收一个 `RequestContext`:

- `ctx.request` — 解析后的 JSON body（dict，无 body 时为 `{}`)
- `ctx.meta.method` / `ctx.meta.query` / `ctx.meta.headers` — HTTP 元数据
- `ctx.state["path_params"]` — 从 `{name}` 段提取的路径参数
- `ctx.server` — 指向宿主服务器的 `ServerProxy`（见下文）
- 返回普通值（→ `200 application/json`）或 `Response`（自定义
  状态码 / 响应头 / media type)

系统路由（`infer`、`events`、`stream`、`ready`、`health`、`reload`、
`versions`、`compare`）为保留路由：在这些路径上声明 `@route` 会在加载时
跳过并告警。

### `ctx.server`(ServerProxy)

路由处理器可以通过 loopback HTTP 查询宿主服务器：

| API | 行为 |
|-----|------|
| `ctx.server.registry.list_loaded()` | 实时返回已加载模型列表：`[{"name", "version", "status", "model_type", "workers"}, ...]` |
| `ctx.server.registry.get(name)` | 返回某个模型的首个条目，不存在时为 `None` |
| `await ctx.server.inference.infer(model_name, input_data, version=None)` | 对另一个模型执行推理，返回模型的 JSON 输出 |
| `ctx.server.metrics.query(name, **labels)` | 查询某个 Prometheus 指标的当前值（抓取 `/metrics`）；不存在时为 `None` |

```python
@route.get("/models")
def models(self, ctx):
    return {"loaded": ctx.server.registry.list_loaded()}

@route.post("/embed_query")
async def embed_query(self, ctx):
    out = await ctx.server.inference.infer("embedder", {"text": ctx.request["q"]})
    return {"embedding": out["output"]}
```

- registry 方法是**同步**的（同步处理器运行在线程中，可直接调用；异步
  处理器请用 `asyncio.to_thread` 包装）。
- `inference.infer` 是**异步**的。同步处理器可用 `asyncio.run(...)`
  驱动（同步处理器所在线程没有运行中的事件循环）。
- `metrics.query` 是**同步**的，适合 counter/gauge 类指标；histogram
  以 `<name>_bucket` / `_sum` / `_count` 等独立样本暴露。
- **禁止自推理。** 路由处理器本身占用着 worker，若 `infer` 调回同模型同
  版本，单 worker 时会死锁 — 因此 `infer()` 对本模型/版本直接抛出
  `ValueError`。请改调*其他*模型，或对本模型逻辑直接调用方法。

### 流式路由

返回 `StreamingResponse` 即可逐 chunk 流式输出响应体：

```python
from lite_server.response import StreamingResponse

@route.get("/ticks")
def ticks(self, ctx):
    async def gen():
        for n in range(10):
            yield {"n": n}
    return StreamingResponse(content=gen())
```

- `content` 可以是异步迭代器，也可以是普通（同步）可迭代对象 — 同步
  可迭代对象会在线程上逐项拉取，慢速 `next()` 不会阻塞 worker 事件循环。
- 每个 yield 的项按 chunk 序列化：`bytes` 原样、`str` 按 UTF-8 编码、
  其他一律 JSON。
- 默认 `text/event-stream` media type 下，每个 chunk 封装为一个 SSE
  事件（payload 的每一行各成一条 `data:` 行）。指定其他
  `media_type="..."` 则 chunk 字节原样透传并使用该 content-type —
  例如用 `application/octet-stream` 做类文件下载。
- `StreamingResponse` 上的 `status_code` / `headers` 会成为 HTTP 响应
  头；它们必须在第一个 chunk yield 之前确定。
- 流式中途抛出 `HTTPException` 会发送一个终止性的结构化错误事件（SSE
  模式）或直接截断响应体（其他 media type）— 此时状态行已经发出。

参见 [examples/06_custom_route](../../examples/06_custom_route/) 获取可运行示例。

## 异步模型

所有模型都运行在 worker 的统一 asyncio 事件循环上 — 不再有单独的异步基类（0.7 之前的 `AsyncLitAPI` 已移除）。除 `setup()` 外，任何方法都可以是 `async def`，worker 在加载时自动适配。

### 用法

```python
import asyncio
from lite_server import LitAPI

class AsyncModel(LitAPI):
    def setup(self, device):
        # setup() 始终保持同步
        self.client = create_client()

    async def decode_request(self, request):
        return request.get("input", "")

    async def predict(self, x):
        # 异步 I/O：例如远程 API 调用或异步模型推理
        result = await self.client.predict(x)
        return {"output": result}

    def encode_response(self, output):
        return output
```

### 工作原理

- **全同步模型**在事件循环内联执行 — 零适配开销，行为与 0.7 之前的 standard loop 一致。
- **只要存在异步方法**（任意模型方法或 callback 钩子），同步模型阶段就在单线程 executor 上执行：同步代码绝不并发运行（保持线程安全假设），也绝不阻塞事件循环。
- 批处理、流式、双向流式、连续批处理都同时支持同步和异步方法。
- ``enable_async`` 构造参数自 0.7.0 起接受但**忽略**——所有模型统一运行在 async event loop 上，不再需要显式开启。

参见 [examples/10_async](../../examples/10_async/) 获取可运行的示例。

## 连续批处理（LLM）

对于 LLM 工作负载，启用连续批处理以同时处理多个序列并进行迭代生成。

```yaml
# config.yaml
continuous_batching: true
```

实现三个钩子：

```python
class LLMModel(LitAPI):
    def prefill(self, uid, decoded_input):
        """在 KV 缓存中初始化新序列。"""
        tokens = self.tokenizer.encode(decoded_input["prompt"])
        self.kv_cache.add(uid, tokens)

    def step(self, active_sequences):
        """为所有活跃序列运行一步生成。"""
        new_tokens = []
        for seq in active_sequences:
            token = self.model.generate_step(seq.uid)
            new_tokens.append(token)
        return new_tokens

    def has_finished(self, uid, token, generated_sequence):
        """检查序列是否完成生成。"""
        return token == self.eos_token or len(generated_sequence) >= self.max_length
```

``active_sequences`` 中每个元素是 :class:`CBSequence` 对象，包含以下属性：

| 属性 | 说明 |
|------|------|
| `seq.uid` | 唯一请求标识符 |
| `seq.input` | ``decode_request`` 的输出 |
| `seq.output` | 到目前为止生成的 token 列表 |
| `seq.state` | 每序列用户数据（与 ``ctx.state`` 相同字典） |
| `seq.meta` | 不可变请求元数据（``RequestMeta``） |
| `seq.ctx` | 完整的 ``RequestContext`` |

> **注意：** ``step()`` 操作跨序列，**不支持** ``ctx`` 参数（声明会导致加载时报错）。
> 通过 ``seq.state`` 或 ``seq.ctx`` 访问每序列数据。

## 批处理

启用批处理以在单次 `predict()` 调用中处理多个请求：

```yaml
# config.yaml
max_batch_size: 8
batch_timeout: 0.01
adaptive_batching: true
```

批处理激活时，`predict()` 接收解码输入的**列表**：

```python
def predict(self, x):
    # x 是解码输入的列表
    batch_input = [item["text"] for item in x]
    results = self.model(batch(batch_input))
    return [{"output": r} for r in results]  # 必须返回列表，每个输入一个结果
```

关键规则：
- 返回**列表**，每个输入一个结果
- 顺序必须与输入顺序一致
- `batch_timeout` 控制等待更多请求的时间（自适应批处理会自动调整）

#### 自定义 `batch()` / `unbatch()`

覆盖 `batch()` 以在预测前重塑解码输入，覆盖 `unbatch()` 以将批处理输出拆分为每个请求的响应。完整流程：

```
decode_request → batch → predict → unbatch → encode_response
```

当只有一个请求排队**且用户覆盖了** `batch()` / `unbatch()` 时，它们会被跳过 — `predict()` 直接接收解码后的请求。使用默认 `batch()` / `unbatch()` 时，`predict()` 始终接收 `batch()` 的输出。

```python
class CustomBatchModel(LitAPI):
    def decode_request(self, request):
        return {"value": request["input"], "weight": request.get("weight", 1.0)}

    def batch(self, inputs):
        """将解码后的请求合并为单个批处理字典。"""
        return {
            "values": [x["value"] for x in inputs],
            "weights": [x["weight"] for x in inputs],
            "batch_size": len(inputs),
        }

    def predict(self, batch):
        if isinstance(batch, dict) and "values" in batch:
            # 多个请求 — 通过 batch() 处理
            results = [v * w for v, w in zip(batch["values"], batch["weights"])]
            return {"results": results, "batch_size": batch["batch_size"]}
        # 单个请求 — batch() 被跳过
        return {"output": batch["value"] * batch["weight"], "batch_size": 1}

    def unbatch(self, output):
        """将批处理输出拆分为每个请求的响应。"""
        return [
            {"output": r, "batch_size": output["batch_size"]}
            for r in output["results"]
        ]

    def encode_response(self, output):
        return output
```

参见 [examples/02_batching](../../examples/02_batching/) 获取可运行的示例。

## 双向流式

用于实时双向通信（如 ASR）。重写 `bidi_stream()` 返回一个
:class:`BidiStreamHandler` 子类的实例，实现 `on_open`、`on_chunk` 和
`on_close` 三个钩子：

```python
class ASRModel(LitAPI):
    def bidi_stream(self):
        class Handler:
            def on_chunk(self, chunk):
                # 处理传入的音频 chunk，返回部分结果
                return self.model.process_audio(chunk)

            def on_close(self):
                # 收尾并返回最终结果
                return self.model.finalize()
        return Handler()
```

- 所有钩子均可声明可选的 ``ctx`` 参数——``ctx.state`` 在同一会话的
  ``on_open`` / ``on_chunk`` / ``on_close`` 之间共享，并发安全。
- ``bidi_stream()`` 本身也可声明 ``ctx`` 参数以访问会话元数据。
- ``on_open`` 成功完成后，必定有恰好一次 ``on_close``（正常关闭、取消或
  worker 关闭时）。``on_open`` 失败则不创建会话，也不会触发 ``on_close``。

只需实现 ``bidi_stream()`` 即可——会话由该方法自动检测并通过 gRPC 提供服务，
无需任何配置项。

> **注意：** 在双向（bidi）会话期间，回调钩子中的 ``ctx.request`` 和
> ``ctx.input`` 始终指向初始的 open 负载——它们不会随 chunk 到达而变化。
> 每个 chunk 的数据通过 handler 的 ``on_chunk(chunk)`` 参数获取。

## Decoupled 流式

用于模型驱动流式（生命周期由模型控制，非客户端）：

```python
class MyModel(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    async def predict_decoupled(self, data, sender):
        for i in range(data):
            await sender.send({"index": i})
        await sender.close()

    def encode_response(self, output):
        return output
```

- ``predict_decoupled(data, sender)`` 替代 ``predict``，接收解码后的输入和
  ``ResponseSender``。
- 调用 ``sender.send(payload)`` 推送 chunk；调用 ``sender.close()`` 结束流。
- 方法立即返回——chunk 投递是异步的。

只需实现 ``predict_decoupled`` 即可——会话由该方法自动检测并通过以下传输提供服务：

| 传输 | 端点 |
|------|------|
| gRPC | `DecoupledInfer` RPC（挂 `features.grpc_streaming` 下） |
| HTTP SSE | `POST /v2/models/{m}/decoupled`（含 `/versions/{v}/decoupled`） |
| HTTP WebSocket | `GET /v2/models/{m}/decoupled-stream`（含 `/versions/{v}/decoupled-stream`） |

空闲流在 `server.decoupled_idle_timeout_secs`（默认 300s）后回收。帧约定和传输
细节见 [Decoupled 流式](../streaming.md)。

## 自定义指标

从模型代码中采集应用级指标（Gauge、Counter、Histogram）。指标通过 Prometheus 端点 `/metrics` 自动暴露。

### 工作原理

1. **预注册**：在 `setup()` 中声明指标 → 返回数字 ID
2. **上报**：在 `predict()` / `stream_predict()` 中使用 ID 上报值
3. 指标自动附加到响应并记录到 Prometheus

预注册让服务器预先分配 Prometheus 对象，热路径零分配（`report_metric` 约 50ns）。

### API

```python
def register_metric(self, name: str, metric_type: str) -> int
```

预注册指标。在 `setup()` 中调用。返回数字 ID。

- `name`：Prometheus 指标名（如 `"batch_size"`、`"cache_hit_rate"`）
- `metric_type`：`"gauge"`、`"counter"` 或 `"histogram"`

```python
def report_metric(self, metric_id: int, value: float) -> None
```

通过预注册 ID 上报指标值。在 `predict()` 或 `stream_predict()` 中调用。

### 示例

```python
import time
from lite_server import LitAPI

class MyModel(LitAPI):
    def setup(self, device):
        self.model = load_model()
        # 预注册指标 — 一次性开销
        self.g_batch_size = self.register_metric("my_batch_size", "gauge")
        self.c_predictions = self.register_metric("my_predictions", "counter")
        self.h_latency = self.register_metric("my_inference_ms", "histogram")

    def predict(self, x):
        start = time.time()
        output = self.model(x)
        elapsed_ms = (time.time() - start) * 1000

        # 上报指标 — 热路径，约 50ns 每次
        self.report_metric(self.g_batch_size, len(x) if isinstance(x, list) else 1)
        self.report_metric(self.c_predictions, 1.0)
        self.report_metric(self.h_latency, elapsed_ms)

        return output
```

### Prometheus 输出

发送请求后查看 `/metrics`：

```
# Gauge
lite_server_my_batch_size{model="mymodel"} 32

# Counter
lite_server_my_predictions_total{model="mymodel"} 1542

# Histogram
lite_server_my_inference_ms_count{model="mymodel"} 1542
lite_server_my_inference_ms_sum{model="mymodel"} 462.6
lite_server_my_inference_ms_bucket{model="mymodel",le="0.1"} 1200
lite_server_my_inference_ms_bucket{model="mymodel",le="0.5"} 1400
...
```

### 指标类型

| 类型 | Prometheus 类型 | 使用场景 |
|------|----------------|----------|
| `gauge` | Gauge | 当前值：队列长度、缓存命中率、GPU 利用率 |
| `counter` | Counter（累计） | 单调计数：总预测次数、总错误数、总 token 数 |
| `histogram` | Histogram | 分布：延迟、batch 大小、每请求 token 数 |

### 流式支持

指标在所有模式下均可使用 — 标准、批处理、流式和连续批处理。流式模式下，指标默认在生成器完成后收集并附加到 `StreamDone` 消息。如需按 chunk 收集指标（不等流结束），可在 yield 之间调用 `flush_metrics()`：

```python
def stream_predict(self, request):
    for token in self.model.generate(request["prompt"]):
        yield {"token": token}
    # 生成期间上报的指标在流结束时自动收集
    self.report_metric(self.c_predictions, 1.0)
```

### 注意事项

- 指标名不得与内置 Prometheus 指标冲突（如 `liteserver_requests_total`）
- **Counter 指标不要以 `_total` 结尾** —— Prometheus 会自动追加 `_total` 后缀。如注册名为 `my_predictions_total` 的 counter，实际暴露为 `my_predictions_total_total`
- ID 按 LitAPI 实例隔离 — 不同模型可注册相同指标名（值通过 `model` 标签区分）
- 默认 Histogram 桶：`[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]`

参见 [examples/09_custom_metrics](../../examples/09_custom_metrics/) 获取可运行的示例。

## 自定义参数

`config.yaml` 中的所有字段可通过 `self.config` 在模型代码中访问。这使你无需修改代码即可调整行为。

### 定义参数

在 `config.yaml` 中添加任意自定义字段：

```yaml
# model_repo/my_model/1/config.yaml
max_batch_size: 1
stream: false

# 自定义参数
threshold: 0.5
label: "positive"
model_path: "/opt/models/weights.pt"
```

### 在 model.py 中访问

在 `setup()` 或模型的任何位置使用 `self.config.get(key, default)`：

```python
class MyModel(LitAPI):
    def setup(self, device):
        self.threshold = self.config.get("threshold", 0.5)
        self.label = self.config.get("label", "default")
        model_path = self.config.get("model_path", "model.pt")
        self.model = load_model(model_path)

    def predict(self, x):
        if x["score"] >= self.threshold:
            return {"label": self.label}
        return {"label": "other"}
```

### 使用场景

- **阈值和超参数**：置信度截断、temperature、max_length
- **文件路径**：模型权重、标签文件、查找表
- **特性开关**：按模型版本启用/禁用行为
- **A/B 测试**：不同版本使用不同配置

参见 [examples/07_custom_params](../../examples/07_custom_params/) 获取可运行的示例。

## 日志

每个 `LitAPI` 实例都有一个 `self.logger` 属性（标准的 Python `logging.Logger`），绑定到模型类名。在推理生命周期的任何阶段都可以使用它来输出结构化日志。

### 基本用法

```python
class MyModel(LitAPI):
    def setup(self, device):
        self.logger.info("Loading model on device=%s", device)
        self.model = load_model()

    def predict(self, x):
        self.logger.debug("predict input=%s", x)
        output = self.model(x)
        self.logger.info("predict output=%s", output)
        return output
```

### 日志级别

| 方法 | 使用场景 |
|------|----------|
| `logger.debug(...)` | 详细诊断：原始输入/输出、中间张量 |
| `logger.info(...)` | 生命周期事件：模型加载完成、请求接收、响应发送 |
| `logger.warning(...)` | 可恢复问题：使用了已废弃的功能、触发了回退逻辑 |
| `logger.error(...)` | 会导致请求失败的错误 |

### 控制详细程度

worker 会配置根 logger，所有模型 logger 继承相同的 handler 和级别。通过 `--log-level` CLI 标志控制：

```bash
python -m lite_server serve --config server.yaml --log-level info
```

或在 `server.yaml` 中：

```yaml
server:
  log_level: info
```

### 按请求追踪

使用 `before_decode_request` 和 `after_encode_response` 记录请求元数据：

```python
def before_decode_request(self, ctx):
    self.logger.info(
        "Request from %s | route=%s | request_id=%s",
        ctx.meta.client_ip, ctx.meta.route, ctx.meta.request_id,
    )
    return ctx.request

def after_encode_response(self, ctx):
    self.logger.info(
        "Response ready | request_id=%s | latency_ms=%.2f",
        ctx.meta.request_id,
        (time.time_ns() - ctx.meta.timestamp_ns) / 1_000_000,
    )
    return ctx.response
```

``ctx.meta`` 是一个 `RequestMeta` 对象，包含：`route`、`headers`、`client_ip`、`request_id`、`timestamp_ns`。

参见 [examples/11_logging](../../examples/11_logging/) 获取可运行的示例。

## 最佳实践

### 资源管理

- 在 `setup()` 中加载重型资源（模型权重、分词器），而不是在 `predict()` 中
- 使用 `teardown()` 释放 GPU 内存和文件句柄
- 将所有状态存储在 `self` 上 — worker 是长生命周期进程

### 错误处理

- 在 `predict()` 中抛出异常以发出错误信号 — 服务器会在不同 worker 上重试
- 使用 `before_decode_request()` 进行输入验证 — 抛出异常以提前拒绝
- 避免裸 `except:` — 让意外错误传播以便调试

#### 类型化 HTTP 错误

使用 `HTTPException` 子类返回带有结构化错误信息的类型化 HTTP 错误。子类可用于**所有钩子**（`predict`、`stream_predict`、`bidi_stream`、`decode_request`、`encode_response`、`before_decode_request`、`after_encode_response`、`prefill`、`step`）以及所有协议（HTTP、SSE、WebSocket、gRPC）。

```python
from lite_server.exceptions import (
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    InternalServerError,
    ServiceUnavailableError,
)

class MyModel(LitAPI):
    def predict(self, x):
        if x.get("value") < 0:
            raise BadRequestError("input must be non-negative", "invalid_input")
        if self.model is None:
            raise ServiceUnavailableError("model not loaded yet")
        return self.model(x)

    def before_decode_request(self, ctx):
        if not self._check_auth(ctx.meta.headers):
            raise UnauthorizedError("invalid or missing token")
        return ctx.request
```

| 异常类 | HTTP 状态码 | 默认 error_type |
|--------|------------|-----------------|
| `BadRequestError` | 400 | `invalid_request_error` |
| `UnauthorizedError` | 401 | `authentication_error` |
| `ForbiddenError` | 403 | `permission_denied_error` |
| `NotFoundError` | 404 | `not_found_error` |
| `InternalServerError` | 500 | `server_error` |
| `ServiceUnavailableError` | 503 | `service_unavailable` |

所有异常类都接受 ``error_type`` 作为第二个参数设置错误类型，以及可选的 ``code``、``param`` 和 ``headers`` 关键字参数用于程序化错误处理（OpenAI 惯例）：

```python
raise BadRequestError("input must be non-negative", code="invalid_input", param="value")
```

客户端始终收到四字段结构化响应：

```json
{"error": {"type": "invalid_input", "message": "input must be non-negative", "code": "invalid_input", "param": "value"}}
```

- `code` — 机器可读错误码（snake_case），未设置时为 `null`。服务器生成的错误始终带有 code（如 `model_not_found`、`queue_full`、`invalid_request_body`）。
- `param` — 导致错误的参数名，不适用时为 `null`。

在 gRPC 上，`code`/`param` 以标准 [ErrorInfo](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto) details 传递（`reason` = code，`metadata` = {error_type, param}），状态消息保持 `[error_type] m```python
from lite_server.exceptions import HTTPException

class PaymentRequiredError(HTTPException):
    def __init__(self, detail, error_type="payment_required"):
        super().__init__(402, detail, error_type)
```

#### 响应头

每个 HTTP 响应（成功或错误）都携带：

| 响应头 | 说明 |
|--------|------|
| `x-request-id` | 用于日志/追踪关联的请求 ID。客户端提供 `x-client-request-id`（1–512 ASCII 字符）时回显；否则生成 UUID v4。同一 ID 会传播到推理 worker 和回调。 |
| `x-processing-time-ms` | 服务器端总处理时间（毫秒，墙钟）。 |

框架层错误也已标准化：未知路由返回 404（`code: route_not_found`）、不支持的方法返回 405（`code: method_not_allowed`）、格式错误的 JSON 请求体返回 400（`code: invalid_request_body`）— 均为上述四字段格式。

### 性能

- 保持 `decode_request()` 和 `encode_response()` 轻量 — 它们在每个请求上运行
- 对于批处理推理，确保 `predict()` 按输入顺序返回结果
- 对可变负载工作负载使用 `adaptive_batching: true`

### 测试

模型可以独立测试，无需启动服务器：

```python
import json, asyncio
from lite_server.pipeline import Pipeline
from lite_server.context import RequestMeta, Headers

api = MyModel(max_batch_size=1)
api.setup("cpu")

pipe = Pipeline.build(api)
data = json.dumps({"input": 42}).encode()
meta = RequestMeta(route="/predict", headers=Headers(), client_ip="",
                   request_id="", timestamp_ns=0)
resp_bytes, status, _, _ = asyncio.run(pipe.run_single(data, meta))
assert json.loads(resp_bytes) == {"result": 84}
```

## 示例：完整模型

```python
"""图像分类模型，支持预处理和批处理。"""

import numpy as np
from lite_server import LitAPI

class ImageClassifier(LitAPI):
    def setup(self, device):
        self.device = device
        self.model = load_model("resnet50.pt", device=device)
        self.labels = load_labels("imagenet_labels.txt")

    def decode_request(self, request):
        # request: {"image": base64编码字符串}
        import base64
        img_bytes = base64.b64decode(request["image"])
        return preprocess_image(img_bytes)

    def predict(self, x):
        if isinstance(x, list):
            # 批处理：x 是预处理图像的列表
            batch = np.stack(x)
            outputs = self.model(batch)
            return [self._decode_output(o) for o in outputs]
        return self._decode_output(self.model(x))

    def encode_response(self, output):
        return output  # 已经是包含 label + confidence 的字典

    def _decode_output(self, logits):
        idx = int(np.argmax(logits))
        return {"label": self.labels[idx], "confidence": float(logits[idx])}

    def teardown(self):
        del self.model
```

## 配置参考

参见 [configuration.md](../configuration.md) 获取完整的模型配置字段参考。
