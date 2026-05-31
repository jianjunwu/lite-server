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
  orchestration.yaml     # 可选：模型加载策略
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

#### `on_request(self, request, meta)`

在 `decode_request()` 之后、`predict()` 之前调用。用于鉴权、日志或请求修改。

```python
def on_request(self, request, meta):
    self.logger.info(f"Request from {meta.client_ip}: {meta.request_id}")
    if not self._check_auth(meta.headers):
        raise PermissionError("Unauthorized")
    return request
```

`meta` 是 `RequestMeta` 对象，包含：`route`、`headers`、`client_ip`、`request_id`、`timestamp_ns`、`payload`。

#### `on_response(self, response, meta)`

在 `encode_response()` 之后、发送给客户端之前调用。用于响应修改或日志。流式路径中也会调用（每个 chunk 编码后）。

```python
def on_response(self, response, meta):
    response["latency_ms"] = (time.time_ns() - meta.timestamp_ns) / 1_000_000
    return response
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

参见 [examples/08_openai_compatible](../examples/08_openai_compatible/) 获取可运行的示例。

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

## 最佳实践

### 资源管理

- 在 `setup()` 中加载重型资源（模型权重、分词器），而不是在 `predict()` 中
- 使用 `teardown()` 释放 GPU 内存和文件句柄
- 将所有状态存储在 `self` 上 — worker 是长生命周期进程

### 错误处理

- 在 `predict()` 中抛出异常以发出错误信号 — 服务器会在不同 worker 上重试
- 使用 `on_request()` 进行输入验证 — 抛出异常以提前拒绝
- 避免裸 `except:` — 让意外错误传播以便调试

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
