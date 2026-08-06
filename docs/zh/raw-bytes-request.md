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
| 400    | `invalid_request_body`     | Ensemble 收到非 JSON body                    |
| 413    | `payload_too_large`         | Body 超过 `max_request_body_bytes`           |
| 415    | `unsupported_media_type`    | 请求携带 `Content-Encoding` 头               |
