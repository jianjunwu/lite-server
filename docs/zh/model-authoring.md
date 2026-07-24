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
      model.py          # 必需：LitAPI 子类
      config.yaml        # 可选：模型配置
```

- `model_name`：字母、数字、下划线、连字符（如 `my_model`、`resnet-v2`）
- `version`：数字或字符串（如 `1`、`v2`、`latest`）

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

- `device` 是字符串，如 `"cpu"` 或 `"cuda:0"`
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

#### `encode_response(self, output)`

将预测输出格式化为 HTTP 响应体（必须可 JSON 序列化）。

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

#### `on_request(self, ctx)`

在 `decode_request()` 之前对原始请求调用。用于鉴权、日志或请求修改。接收单个 :class:`RequestContext` 参数（与 Callback 钩子契约一致）。

```python
def on_request(self, ctx):
    self.logger.info(f"Request from {ctx.meta.client_ip}: {ctx.meta.request_id}")
    if not self._check_auth(ctx.meta.headers):
        raise PermissionError("Unauthorized")
    return ctx.request
```

``ctx.meta`` 是 `RequestMeta` 对象，包含：`route`、`headers`、`client_ip`、`request_id`、`timestamp_ns`。

#### `on_response(self, ctx)`

在 `encode_response()` 之后、发送给客户端之前调用。用于响应修改或日志。流式路径中也会调用（每个 chunk 编码后）。

```python
def on_response(self, ctx):
    ctx.response["latency_ms"] = (time.time_ns() - ctx.meta.timestamp_ns) / 1_000_000
    return ctx.response
```

要附加自定义 HTTP 响应头，使用 :meth:`ctx.respond() <lite_server.RequestContext.respond>`：

```python
def on_response(self, ctx):
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

#### `teardown(self)`

模型卸载时调用。在此释放资源。

```python
def teardown(self):
    del self.model
    torch.cuda.empty_cache()
```

## Callbacks 回调系统

Callbacks 是一种**可组合的、声明式的**拦截推理请求生命周期的方式。与内联的 `on_request`/`on_response` 钩子不同，Callbacks 是独立的类，可以被复用、共享并跨模型组合。

### Callback 基类

继承 `Callback` 并覆盖你关心的钩子。所有钩子都有默认的 no-op 实现 — 只定义你需要的方法。数据钩子接收单个 `ctx`（`RequestContext`）参数，可以是同步或 `async def`。

```python
from lite_server import Callback

class MyCallback(Callback):
    def on_request(self, ctx):
        """在 decode_request 之前对原始请求调用。"""
        ctx.request["_timestamp"] = ctx.meta.timestamp_ns

    def on_output(self, ctx):
        """在 predict 之后、encode_response 之前调用。"""
        ctx.output["_latency_ns"] = time.time_ns() - ctx.meta.timestamp_ns
```

**钩子点**（管线顺序）：

```
on_request → decode_request → on_input → predict → on_output → encode_response → on_response
```

| 钩子 | 触发时机 | 读写字段 |
|------|---------|---------|
| `on_request` | 原始请求，`decode_request` 之前 | `ctx.request` |
| `on_input` | `decode_request` 之后，`predict` 之前 | `ctx.input` |
| `on_output` | `predict` 之后，`encode_response` 之前（流式时每个 chunk） | `ctx.output` |
| `on_response` | `encode_response` 之后，发送前（流式时每个 chunk） | `ctx.response` |
| `on_before_setup` | `LitAPI.setup()` 之前 | `(config, device)` |
| `on_after_setup` | `LitAPI.setup()` 完成后 | `(lit_api)` |
| `on_teardown` | 模型卸载 / worker 关闭时 | `(lit_api)` |

数据钩子可以原地修改 `ctx`，也可以返回替换值（返回 `None` 表示透传）。

### RequestContext

| 字段 | 内容 |
|------|------|
| `ctx.meta` | `RequestMeta`：HTTP 头、路由、客户端 IP、请求 ID、时间戳 |
| `ctx.request` / `ctx.input` / `ctx.output` / `ctx.response` | 各阶段的管线值 |
| `ctx.state` | 跨钩子共享的**每请求**暂存字典 — 用它，**不要**用 `self` 属性（在并发请求间共享） |
| `ctx.early` | 设置后管线短路 |

### Early Return 与参数校验

- **Early return**（如缓存命中）：在任意钩子中调用 `ctx.respond(body, status_code=..., headers=...)` 或返回一个 `Response`。后续阶段和剩余钩子被跳过。
- **参数校验 / 拒绝**：在任意钩子中抛出 `HTTPException`（`BadRequestError`、`UnauthorizedError` 等）。客户端收到对应状态码的结构化错误 — 数据钩子的异常**不会**被吞掉。
- 生命周期钩子（`on_before_setup` / `on_after_setup` / `on_teardown`）保持异常隔离：失败只记日志，不传播。

```python
from lite_server import Callback, BadRequestError

class Validator(Callback):
    def on_request(self, ctx):
        if "input" not in (ctx.request or {}):
            raise BadRequestError("missing field", param="input")

class Cache(Callback):
    def on_request(self, ctx):
        hit = self._cache.get(key(ctx))
        if hit is not None:
            ctx.respond(hit, headers={"X-Cache-Hit": "1"})
```

### 声明式加载

在 `config.yaml` 中通过 `callbacks` 字段声明 callback 类路径，服务启动时自动加载并注册：

```yaml
# config.yaml
callbacks:
  - my_package.callbacks.AuditLogger
  - my_package.callbacks.MetricsCollector
```

每个类必须是无参构造的 `Callback` 子类。导入失败或 0.7 之前的旧钩子签名会在加载时响亮报错 — 被静默跳过的 callback 可能意味着鉴权/校验逻辑从未执行。

### 完整示例：审计日志

```python
"""审计日志 callback：记录每个请求的输入/输出和延迟。"""
import time
from lite_server import Callback

class AuditLogger(Callback):
    def on_request(self, ctx):
        ctx.state["start_ns"] = time.time_ns()  # 每请求存储，并发安全
        ctx.request["_audit_id"] = ctx.meta.request_id

    def on_output(self, ctx):
        elapsed_ms = (time.time_ns() - ctx.state["start_ns"]) / 1_000_000
        print(f"[AUDIT] request_id={ctx.meta.request_id} latency={elapsed_ms:.2f}ms")

    def on_teardown(self, lit_api):
        print(f"[AUDIT] model torn down, total handled: {lit_api.call_count}")
```

### Callback vs LitAPI 内联 Hook

| 方面 | `Callback` | `LitAPI.on_request` / `on_response` |
|------|-----------|--------------------------------------|
| 定义方式 | 独立类，声明式注册 | 在模型类中内联定义 |
| 复用性 | 可跨模型共享 | 每个模型单独实现 |
| 组合性 | 多个 callback 可链式组合 | 只能在模型内实现一次 |
| 注册方式 | config.yaml 中 `callbacks:` 字段 | 模型代码中覆盖方法 |
| 异常 | `HTTPException` → 结构化错误响应 | 相同 |
| 位置 | 按注册顺序 | `on_request` 在最前，`on_response` 在最后 |

参见 [examples/14_lifecycle_hooks](../examples/14_lifecycle_hooks/) 获取可运行示例。

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

参见 [examples/10_async](../examples/10_async/) 获取可运行的示例。

## 连续批处理（LLM）

对于 LLM 工作负载，启用连续批处理以同时处理多个序列并进行迭代生成。

```yaml
# config.yaml
continuous_batching: true
max_sequence_length: 4096
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
            token = self.model.generate_step(seq["uid"])
            new_tokens.append(token)
        return new_tokens

    def has_finished(self, uid, token, generated_sequence):
        """检查序列是否完成生成。"""
        return token == self.eos_token or len(generated_sequence) >= self.max_length
```

`active_sequences` 中每个元素包含键：`uid`、`input`、`output`（到目前为止的 token 列表）。

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

当只有一个请求排队时，`batch()` 和 `unbatch()` 都会被跳过 — `predict()` 直接接收解码后的请求。

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

参见 [examples/02_batching](../examples/02_batching/) 获取可运行的示例。

## OpenAI 兼容端点

对于 OpenAI 兼容的聊天完成端点，使用 `OpenAIEndpoint` 基类代替 `LitAPI`。它会自动注册 `/v1/chat/completions` 路由并处理 OpenAI 请求/响应格式。

### 基本用法

```python
from lite_server.specs.openai import OpenAIEndpoint

class ChatModel(OpenAIEndpoint):
    model = "my-chat-model"

    def setup(self):
        """初始化模型资源。"""
        self.llm = load_llm()

    def decode_request(self, request):
        """从 OpenAI 消息格式中提取 prompt。"""
        messages = request.get("messages", [])
        # 将消息转换为 prompt 字符串
        return "\n".join(m["content"] for m in messages if m.get("role") == "user")

    def predict(self, x):
        """生成响应。返回 str 或包含 'text' 键的 dict。"""
        return self.llm.generate(x)
```

保存为 `model_repo/{model_name}/{version}/model.py`。端点自动在 `/v1/chat/completions` 可用。

### 流式支持

覆盖 `stream_predict()` 以启用 SSE 流式：

```python
import asyncio

class StreamingChatModel(OpenAIEndpoint):
    model = "streaming-chat"

    def setup(self):
        self.llm = load_llm()

    def decode_request(self, request):
        messages = request.get("messages", [])
        return "\n".join(m["content"] for m in messages if m.get("role") == "user")

    def predict(self, x):
        return self.llm.generate(x)

    async def stream_predict(self, x):
        """生成 OpenAI 流式 chunks。"""
        for token in self.llm.generate_stream(x):
            yield {
                "choices": [{"delta": {"content": token}, "index": 0}]
            }
            await asyncio.sleep(0.02)
        # 最后一个 chunk 表示完成
        yield {
            "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}]
        }
```

当请求中包含 `stream: true` 时，服务器使用 `stream_predict()`。如果未覆盖，回退到 `predict()` 包装为单个 chunk。

### 自定义响应格式

覆盖 `encode_response()` 以自定义 OpenAI 响应格式：

```python
class CustomResponseModel(OpenAIEndpoint):
    def encode_response(self, output):
        return {
            "id": f"chatcmpl-{uuid.uuid4().hex[:8]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": self.model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": output["text"]},
                "finish_reason": "stop"
            }],
            "usage": output.get("usage", {"prompt_tokens": 0, "completion_tokens": 0})
        }
```

### OpenAIEndpoint vs LitAPI

| 方面 | `OpenAIEndpoint` | `LitAPI` |
|------|------------------|----------|
| 路由 | `/v1/chat/completions`（自动注册） | `/v2/models/{name}/infer` |
| 请求格式 | OpenAI 聊天格式（`messages` 数组） | 自定义 JSON |
| 响应格式 | OpenAI 完成格式 | 自定义 JSON |
| 流式 | `stream_predict()` 异步生成器 | `stream_predict()` 生成器 |
| 使用场景 | OpenAI 兼容 API | 自定义推理端点 |

> **注意：** OpenAI 端点示例即将推出。当前可参见 [examples/README.md](../examples/README.md) 获取可用示例，以及 `python/lite_server/specs/openai.py` 中的 `OpenAIEndpoint` 源码。

## 双向流式

用于实时双向通信（如 ASR）：

```python
class ASRModel(LitAPI):
    def bidi_stream(self):
        class Handler:
            def on_chunk(self, chunk):
                # 处理传入的音频 chunk，返回部分结果
                return self.model.process_audio(chunk)

            def on_close(self):
                # 完成并返回最终结果
                return self.model.finalize()
        return Handler()
```

在配置中启用：

```yaml
bidirectional: true
```

> **注意：** 在双向（bidi）会话期间，钩子中的 ``ctx.request`` 和 ``ctx.input``
> 始终指向初始的 open 负载——它们不会随 chunk 到达而变化。每个 chunk 的数据
> 通过 handler 的 ``on_chunk(chunk)`` 参数获取。

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
        self.c_predictions = self.register_metric("my_predictions_total", "counter")
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
lite_server_my_predictions_total_total{model="mymodel"} 1542

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

指标在所有模式下均可使用 — 标准、批处理、流式和连续批处理。流式模式下，指标在生成器完成后收集并附加到 `StreamDone` 消息。

```python
def stream_predict(self, request):
    for token in self.model.generate(request["prompt"]):
        yield {"token": token}
    # 生成期间上报的指标会自动收集
    self.report_metric(self.c_predictions, 1.0)
```

### 注意事项

- 指标名不得与内置 Prometheus 指标冲突（如 `liteserver_requests_total`）
- ID 按 LitAPI 实例隔离 — 不同模型可注册相同指标名（值通过 `model` 标签区分）
- 默认 Histogram 桶：`[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]`

参见 [examples/09_custom_metrics](../examples/09_custom_metrics/) 获取可运行的示例。

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

参见 [examples/07_custom_params](../examples/07_custom_params/) 获取可运行的示例。

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

使用 `on_request` 和 `on_response` 记录请求元数据：

```python
def on_request(self, ctx):
    self.logger.info(
        "Request from %s | route=%s | request_id=%s",
        ctx.meta.client_ip, ctx.meta.route, ctx.meta.request_id,
    )
    return ctx.request

def on_response(self, ctx):
    self.logger.info(
        "Response ready | request_id=%s | latency_ms=%.2f",
        ctx.meta.request_id,
        (time.time_ns() - ctx.meta.timestamp_ns) / 1_000_000,
    )
    return ctx.response
```

``ctx.meta`` 是一个 `RequestMeta` 对象，包含：`route`、`headers`、`client_ip`、`request_id`、`timestamp_ns`。

参见 [examples/11_logging](../examples/11_logging/) 获取可运行的示例。

## 最佳实践

### 资源管理

- 在 `setup()` 中加载重型资源（模型权重、分词器），而不是在 `predict()` 中
- 使用 `teardown()` 释放 GPU 内存和文件句柄
- 将所有状态存储在 `self` 上 — worker 是长生命周期进程

### 错误处理

- 在 `predict()` 中抛出异常以发出错误信号 — 服务器会在不同 worker 上重试
- 使用 `on_request()` 进行输入验证 — 抛出异常以提前拒绝
- 避免裸 `except:` — 让意外错误传播以便调试

#### 类型化 HTTP 错误

使用 `HTTPException` 子类返回带有结构化错误信息的类型化 HTTP 错误。子类可用于**所有钩子**（`predict`、`stream_predict`、`bidi_stream`、`decode_request`、`encode_response`、`on_request`、`on_response`、`prefill`、`step`）以及所有协议（HTTP、SSE、WebSocket、gRPC）。

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
            raise BadRequestError("input must be non-negative", "INVALID_INPUT")
        if self.model is None:
            raise ServiceUnavailableError("model not loaded yet")
        return self.model(x)

    def on_request(self, ctx):
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

所有异常类都接受自定义 `error_type` 作为第二个参数，以及可选的 `code` 和 `param` 关键字参数用于程序化错误处理（OpenAI 惯例）：

```python
raise BadRequestError("input must be non-negative", code="invalid_input", param="value")
```

客户端始终收到四字段结构化响应：

```json
{"error": {"type": "INVALID_INPUT", "message": "input must be non-negative", "code": "invalid_input", "param": "value"}}
```

- `code` — 机器可读错误码（snake_case），未设置时为 `null`。服务器生成的错误始终带有 code（如 `model_not_found`、`queue_full`、`invalid_request_body`）。
- `param` — 导致错误的参数名，不适用时为 `null`。

在 gRPC 上，`code`/`param` 以标准 [ErrorInfo](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto) details 传递（`reason` = code，`metadata` = {error_type, param}），状态消息保持 `[error_type] message` 格式。

`HTTPException` 在自定义端点处理器中同样有效 — 端点返回异常的状态码和相同的结构化错误体。

可通过直接继承 `HTTPException` 支持自定义状态码：

```python
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
api = MyModel(max_batch_size=1)
api.setup("cpu")
result = api.encode_response(api.predict(api.decode_request({"input": 42})))
assert result == {"result": 84}
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
