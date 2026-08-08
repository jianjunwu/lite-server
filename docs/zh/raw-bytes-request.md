# 原始字节 / Tensor 请求（0.8.3）

lite-server HTTP 推理端点支持**原始字节**请求。当 `Content-Type` 不是
`application/json`（及其 `+json` 后缀变体）时，body 以原始 `bytes` 直传 worker。
JSON 路径实现零拷贝：原始字节直接转发，不再经历 `Value` 物化 → `to_vec` 序列化往返。

## Content-Type 分流

| 客户端发送                  | Rust 提取器              | Worker 收到                |
|-----------------------------|--------------------------|----------------------------|
| 无 `Content-Type`           | `RequestBody::Json`      | `ctx.request` = JSON 解析结果 |
| `application/json`          | `RequestBody::Json`      | `ctx.request` = JSON 解析结果 |
| `application/*+json`        | `RequestBody::Json`      | `ctx.request` = JSON 解析结果 |
| `application/octet-stream`  | `RequestBody::Raw`       | `ctx.request` = 原始 `bytes`  |
| 其他值（含畸形值）          | `RequestBody::Raw`       | `ctx.request` = 原始 `bytes`  |
| 带 `Content-Encoding`       | **415 Unsupported Media**| —                          |
| `Inference-Header-Content-Length` > 0 | `RequestBody::TritonBinary` | `ctx.request` = 解析后 JSON 头;`ctx.binary_data` = 二进制尾视图 |

## Triton Binary Tensor Data Extension（0.8.4，批次 1）

多 tensor 二进制传输，兼容 KServe V2 dataplane（Triton HTTP 协议）。body =
**JSON 头 + 拼接二进制尾**，由 `Inference-Header-Content-Length` 切分：

```http
POST /v2/models/:m/infer
Content-Type: application/octet-stream
Inference-Header-Content-Length: 546      ← JSON 头字节数

{"id": "req-1", "inputs": [{"name": "a", "shape": [2], "datatype": "FP32",
  "parameters": {"binary_data_size": 8}}, ...]}   ← 0..N 字节
<输入 a 的二进制块:8 字节>                        ← N..N+Σ 字节
```

规则（对齐 Triton / KServe）：

- 每个输入用 `parameters.binary_data_size` 声明块大小（该 tensor 总字节数，
  小端 row-major）。tail 按**声明顺序**切分。
- **混合输入合法**：JSON `data` 数组与 `binary_data_size` 可并存；Σ 只加
  二进制者。
- Σ `binary_data_size` 必须等于 tail 长度——不匹配 **400**。
- 重名 input、负数/非整数 size、size 溢出、header 超 body 长度、JSON 头
  畸形——均 **400**。
- `Inference-Header-Content-Length: 0` 落回普通 `Content-Type` 分流
  （原始字节，行为不变）。
- ensemble 模型拒绝 Triton Binary 请求（**400**——JSON 头+二进制尾容器
  须等 §9.6 Option B 的命名槽位容器）。
- FP16 / BF16 在 dataplane 中无 JSON 表示——二进制是唯一通道
  （`datatype: "FP16"` + `binary_data_size`）。
- batch 请求不解析该 header（batch item 共享首 item 元数据，wire 无
  per-item headers）。

### Worker 范式

```python
import numpy as np

class MyModel(LitAPI):
    def decode_request(self, request, ctx):
        # ctx.request = 解析后的 JSON 头 dict（信封原样）
        # ctx.binary_data = {输入名: memoryview}——声明顺序
        # memoryview 是零拷贝视图;np.frombuffer 直接接受。
        a = np.frombuffer(ctx.binary_data["a"], dtype=np.float32)  # 视图,无拷贝
        return {"a": a, "head": ctx.request}

    def predict(self, x):
        return x["a"].sum()

    def encode_response(self, output):
        return {"sum": float(output)}
```

BYTES datatype 的每个元素 = 4 字节小端长度前缀 + 内容（前缀是 *tensor
语义*——server 只按声明 size 切分，解析归 worker）：

```python
def parse_bytes_tensor(mv):
    out = []
    while mv:
        (n,) = struct.unpack("<I", mv[:4])
        out.append(bytes(mv[4 : 4 + n]))
        mv = mv[4 + n :]
    return out
```

非 Triton Binary 请求时 `ctx.binary_data` 为 `None`，`ctx.request` 保持
既有 JSON / 原始字节行为不变。

## 客户端示例

### JSON（不变）

```python
import requests

resp = requests.post(
    "http://localhost:8000/v2/models/my-model/predict",
    json={"prompt": "你好", "max_tokens": 100},
)
```

### 原始 Tensor 字节

```python
import numpy as np
import requests

arr = np.random.rand(1, 3, 224, 224).astype(np.float32)
resp = requests.post(
    "http://localhost:8000/v2/models/vision/predict",
    data=arr.tobytes(),
    headers={
        "Content-Type": "application/octet-stream",
        "x-tensor-dtype": arr.dtype.str,      # 如 "<f4"（小端 float32）
        "x-tensor-shape": ",".join(map(str, arr.shape)),
    },
)
```

### 编码媒体（WAV/JPEG/H.264）

```python
with open("audio.wav", "rb") as f:
    audio_bytes = f.read()

resp = requests.post(
    "http://localhost:8000/v2/models/asr/predict",
    data=audio_bytes,
    headers={
        "Content-Type": "audio/wav",
        "x-media-format": "wav",
    },
)
```

## Worker `decode_request` 范式

Worker 的 `decode_request` 根据客户端 Content-Type 接收不同类型——
解析后的 JSON dict 或原始 `bytes`。用 `isinstance` 分支：

```python
import numpy as np
import math

class MyAPI(LitAPI):
    def decode_request(self, request, ctx):
        if isinstance(request, bytes):
            # 原始字节路径：shape/dtype 走 header 侧带（客户端声明）。
            h = ctx.meta.headers
            dtype = np.dtype(h["x-tensor-dtype"])
            shape = tuple(int(d) for d in h["x-tensor-shape"].split(","))
            expected = math.prod(shape) * dtype.itemsize
            if len(request) != expected:
                raise ValueError(
                    f"body {len(request)}B != 期望 {expected}B ({dtype}{shape})"
                )
            # frombuffer 返回**只读视图**——如需可写数组，加 .copy()。
            return np.frombuffer(request, dtype=dtype).reshape(shape)

        # JSON 路径（现有行为，不变）。
        return request["input"]

    def predict(self, x):
        # x 可能是 numpy 数组（raw 路径）或 dict/str（JSON 路径）。
        ...

    def encode_response(self, output):
        return output
```

### 要点

- `np.frombuffer` 是字节重解释——**长度必须被 `dtype.itemsize` 整除**，
  否则抛 `ValueError`。务必加上面的长度校验。
- **字节序不匹配是静默错误值**——必须通过 header（如 `x-tensor-dtype`）
  声明。`dtype.str` 自带 `"<"`/`">"` 前缀。
- **编码媒体（WAV/MP3/JPEG/H.264）**必须走 codec 解码（ffmpeg/PIL），
  不能用 `frombuffer`。raw bytes 路径面向未编码的 tensor 数据；
  用 `x-media-format` header（或类似约定）标识编码格式。
- **shape 固定的模型**可跳过 header 解析，直接用 `setup()` 里读出的
  `self.input_shape`——但 dtype 字节序仍建议走 header，避免硬编码漂移。

## Ensemble 模型

Ensemble 模型接受原始字节根输入（0.8.3 移除了"ensemble 只收 JSON"的
拒绝逻辑）：字节原样流向第一层，请求的 Content-Type 透传给该 step 的
worker。典型链路——图片字节进、首层返回 JSON 特征、后续 step 保持
JSON——端到端可用。

Binary 流动被刻意限制在 DAG 的两条外沿：

| 引用形态 | 作用于 binary 数据时 | 结果 |
|---|---|---|
| `$request`（整体） | 根 binary 输入 | 透传给该 step |
| `$request.field` | — | **400**（字节无字段语义） |
| `$stepN`（整体或字段） | binary step 输出 | **400**(binary 不许在 step 间流动） |

消费 binary 输入的 step 必须把它声明为*唯一*输入——JSON+binary 混合
输入集会被 400 拒绝。末层 step 自身也可以返回 binary(worker 响应携带
非 JSON `media_type`)：此时 HTTP 响应原样携带字节与 worker 声明的
Content-Type，与 unary 透传完全一致。gRPC 侧字节经 `InferResponse.data`
返回（proto 不携带 content type，客户端按模型契约自知）。

## 请求体大小限制

默认 **64 MiB**。通过 `server.max_request_body_bytes` 配置。超限返回
**413 Payload Too Large**，错误体包含结构化信息：

```json
{"error": {"type": "invalid_request_error", "code": "payload_too_large",
 "message": "request body exceeds the 67108864 bytes limit",
 "max_size": 67108864, "actual_size": null}}
```

内存预算：`max_request_body_bytes × 并发在途请求数`。按实例内存和预期
并发调低此值，或配合既有流控。

## 错误码

| 状态码 | 错误码                      | 触发条件                                     |
|--------|----------------------------|----------------------------------------------|
| 400    | `invalid_request_body`     | JSON 语法错误                                |
| 400    | `invalid_request_body`     | Ensemble：对 binary 数据取字段、binary step 输出被下游引用、或 JSON+binary 混合 step 输入 |
| 413    | `payload_too_large`         | Body 超过 `max_request_body_bytes`           |
| 415    | `unsupported_media_type`    | 请求携带 `Content-Encoding` 头               |
