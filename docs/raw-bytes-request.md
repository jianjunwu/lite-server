# Raw Bytes / Tensor Request (0.8.4)

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
| `Inference-Header-Content-Length` > 0 | `RequestBody::TritonBinary` | `ctx.request` = parsed JSON head; `ctx.binary_data` = tail views |

## Triton Binary Tensor Data Extension (0.8.4, batch 1)

Multi-tensor binary transport, compatible with the KServe V2 dataplane
(Triton HTTP protocol). The body is a **JSON head + concatenated binary
tail**, split by the `Inference-Header-Content-Length` header:

```http
POST /v2/models/:m/infer
Content-Type: application/octet-stream
Inference-Header-Content-Length: 546      ← JSON head byte count

{"id": "req-1", "inputs": [{"name": "a", "shape": [2], "datatype": "FP32",
  "parameters": {"binary_data_size": 8}}, ...]}   ← 0..N bytes
<binary block for input a: 8 bytes>                  ← N..N+Σ bytes
```

Rules (mirroring Triton / KServe):

- Each input declares its block size via `parameters.binary_data_size`
  (total bytes for that tensor, little-endian row-major). The tail is split
  in **declaration order**.
- **Mixed inputs are legal**: inputs with a JSON `data` array and inputs
  with `binary_data_size` may coexist; Σ counts only the binary ones.
- Σ `binary_data_size` must equal the tail length — mismatch is a **400**.
- Duplicate input names, negative / non-integer sizes, size overflow, a
  header exceeding the body length, or a malformed JSON head are **400**.
- `Inference-Header-Content-Length: 0` falls back to the plain
  `Content-Type` dispatch (raw bytes, unchanged behavior).
- Ensemble models reject Triton Binary requests with a **400** (the
  JSON-head + binary-tail container needs the §9.6 Option-B named-slot
  container first).
- FP16 / BF16 have no JSON representation in the dataplane — binary is
  their only channel (`datatype: "FP16"` + `binary_data_size`).
- Batch requests do not parse this header (batch items share the first
  item's metadata; no per-item headers on the wire).

### Worker paradigm

```python
import numpy as np

class MyModel(LitAPI):
    def decode_request(self, request, ctx):
        # ctx.request = parsed JSON head dict (the envelope, unmodified)
        # ctx.binary_data = {input name: memoryview} — declaration order
        # memoryview is a zero-copy view; np.frombuffer accepts it directly.
        a = np.frombuffer(ctx.binary_data["a"], dtype=np.float32)  # view, no copy
        return {"a": a, "head": ctx.request}

    def predict(self, x):
        return x["a"].sum()

    def encode_response(self, output):
        return {"sum": float(output)}
```

For BYTES datatype tensors, each element is a 4-byte little-endian length
prefix followed by the content (the prefix is *tensor semantics* — the
server only splits by the declared size; parsing is the worker's job):

```python
def parse_bytes_tensor(mv):
    out = []
    while mv:
        (n,) = struct.unpack("<I", mv[:4])
        out.append(bytes(mv[4 : 4 + n]))
        mv = mv[4 + n :]
    return out
```

When the request is not Triton Binary, `ctx.binary_data` is `None` and
`ctx.request` keeps the existing JSON / raw-bytes behavior unchanged.

### tritonclient end-to-end (batch 2, G10)

`binary_data_output` negotiation + response reassembly (request flag →
binary response → client `as_numpy` values match):

```python
import tritonclient.http as httpclient

client = httpclient.InferenceServerClient(url="localhost:8000")
inp = httpclient.InferInput("input0", [2, 2], "FP32")
inp.set_data_from_numpy(arr)  # binary channel automatically (JSON head + tail)
out = httpclient.InferRequestedOutput("output0", binary_data=True)
resp = client.infer("my-model", [inp], outputs=[out])
result = resp.as_numpy("output0")  # reassembled ndarray, values match
```

Workers build the envelope output with the `lite_server.kserve` helper:

```python
from lite_server.kserve import build_response

def encode_response(self, output, ctx):
    return build_response({"output0": output}, request_id=ctx.meta.request_id)
```

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

## Ensemble Models

Ensemble models accept a raw-bytes root input (the `ensemble requires JSON
input` rejection was lifted in 0.8.3): the bytes flow to the first layer
untouched, with the request's Content-Type forwarded to the step's worker.
The canonical chain — image bytes in, the first step returns JSON features,
later steps stay JSON — works end to end.

Binary flow is deliberately limited to the two outer edges of the DAG:

| Reference | On binary data | Result |
|---|---|---|
| `$request` (whole) | root binary input | passthrough to the step |
| `$request.field` | — | **400** (bytes have no field semantics) |
| `$stepN` (whole or field) | binary step output | **400** (binary must not flow between steps) |

A step consuming the binary input must declare it as its *only* input —
mixed JSON+binary input sets are rejected with 400. A final step may itself
return binary (a non-JSON `media_type` on the worker response): the HTTP
response then carries the bytes and the worker-declared Content-Type
verbatim, exactly like the unary passthrough. On gRPC the bytes are returned
in `InferResponse.data` (the proto carries no content type; the client knows
the model contract).

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
| 400    | `invalid_request_body`      | Ensemble: field access on binary data, binary step output referenced downstream, or mixed JSON+binary step inputs |
| 400    | `invalid_triton_binary_head` | Triton Binary: size-sum mismatch / duplicate name / missing name / invalid `binary_data_size` / malformed head structure (worker-side double-check, Python) |
| 400    | `invalid_json`              | Worker-side JSON parse failure of the request payload |
| 413    | `payload_too_large`         | Body exceeds `max_request_body_bytes`        |
| 415    | `unsupported_media_type`    | `Content-Encoding` header present            |
