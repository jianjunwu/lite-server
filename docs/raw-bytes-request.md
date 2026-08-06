# Raw Bytes / Tensor Request (0.8.3)

lite-server HTTP inference endpoints accept **raw bytes** requests when the
`Content-Type` is *not* `application/json` (or its `+json` suffix variants).
JSON requests remain zero-copy: the original bytes are forwarded to the worker
byte-identical, without a `Value` → `to_vec` round-trip.

## Content-Type Dispatch

| Client sends                | Rust extractor           | Worker receives            |
|-----------------------------|--------------------------|----------------------------|
| `Content-Type` missing      | `RequestBody::Json`      | `ctx.request` = parsed JSON |
| `application/json`          | `RequestBody::Json`      | `ctx.request` = parsed JSON |
| `application/*+json`        | `RequestBody::Json`      | `ctx.request` = parsed JSON |
| `application/octet-stream`  | `RequestBody::Raw`       | `ctx.request` = raw `bytes` |
| Any other value (or garbage)| `RequestBody::Raw`       | `ctx.request` = raw `bytes` |
| `Content-Encoding` present  | **415 Unsupported Media**| —                          |

## Client Examples

### JSON (unchanged)

```python
import requests

resp = requests.post(
    "http://localhost:8000/v2/models/my-model/predict",
    json={"prompt": "hello", "max_tokens": 100},
)
```

### Raw Tensor Bytes

```python
import numpy as np
import requests

arr = np.random.rand(1, 3, 224, 224).astype(np.float32)
resp = requests.post(
    "http://localhost:8000/v2/models/vision/predict",
    data=arr.tobytes(),
    headers={
        "Content-Type": "application/octet-stream",
        "x-tensor-dtype": arr.dtype.str,      # e.g. "<f4" (little-endian float32)
        "x-tensor-shape": ",".join(map(str, arr.shape)),
    },
)
```

### Raw Encoded Media (WAV/JPEG/H.264)

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

## Worker `decode_request` Paradigm

The worker's `decode_request` receives either parsed JSON or raw bytes,
depending on the Content-Type sent by the client. Use `isinstance` to
branch:

```python
import numpy as np
import math

class MyAPI(LitAPI):
    def decode_request(self, request, ctx):
        if isinstance(request, bytes):
            # Raw bytes path: read shape/dtype from headers (set by client).
            h = ctx.meta.headers
            dtype = np.dtype(h["x-tensor-dtype"])
            shape = tuple(int(d) for d in h["x-tensor-shape"].split(","))
            expected = math.prod(shape) * dtype.itemsize
            if len(request) != expected:
                raise ValueError(
                    f"body {len(request)}B != expected {expected}B for {dtype}{shape}"
                )
            # frombuffer creates a **read-only view** — call .copy() if you
            # need a writable array.
            return np.frombuffer(request, dtype=dtype).reshape(shape)

        # JSON path (existing behaviour, unchanged).
        return request["input"]

    def predict(self, x):
        # x is a numpy array from the raw path, or a dict/text/etc. from JSON.
        ...

    def encode_response(self, output):
        return output
```

### Key Points

- `np.frombuffer` reinterprets bytes — **length must divide evenly** by
  `dtype.itemsize`, or it raises `ValueError`. Guard with the size check
  shown above.
- **Byte-order mismatches are silent data corruption** — always pass dtype
  via a header (e.g. `x-tensor-dtype`). `dtype.str` carries the `"<"`/`">"`
  prefix.
- **Encoded media (WAV/MP3/JPEG/H.264)** must be decoded with a codec
  (ffmpeg, PIL, etc.), not `frombuffer`. The raw bytes path is for
  *unencoded* tensor data; use the `x-media-format` header (or similar)
  to signal the codec.
- **Shape-constant models** can skip header parsing and use
  `self.input_shape` from `setup()` instead — but dtype byte-order should
  still come from headers, not hardcoded.

## Body Size Limit

Default: **64 MiB**. Controlled by `server.max_request_body_bytes` in the
config. Bodies exceeding the limit return **413 Payload Too Large** with a
structured error body:

```json
{"error": {"type": "invalid_request_error", "code": "payload_too_large",
 "message": "request body exceeds the 67108864 bytes limit",
 "max_size": 67108864, "actual_size": null}}
```

Memory budget: max body size × concurrent in-flight requests. Adjust the
limit for your instance size and expected concurrency.

## Error Codes

| Status | Code                        | Trigger                                      |
|--------|-----------------------------|----------------------------------------------|
| 400    | `invalid_request_body`      | Invalid JSON syntax                          |
| 400    | `invalid_request_body`      | Ensemble receives non-JSON body              |
| 413    | `payload_too_large`         | Body exceeds `max_request_body_bytes`        |
| 415    | `unsupported_media_type`    | `Content-Encoding` header present            |
