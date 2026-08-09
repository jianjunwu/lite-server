# 25 KServe V2 Dataplane

Demonstrates the **KServe V2 dataplane** beyond plain JSON: raw tensor bytes
and the **Triton Binary Tensor Data Extension** (JSON head + binary tail),
including binary response negotiation with `binary_data_output`.

[中文](README_zh.md)

## Key Concept

| Model | Path exercised | Worker paradigm |
|---|---|---|
| `raw_tensor` | Raw bytes (`application/octet-stream` + `x-tensor-dtype` / `x-tensor-shape` headers) | `decode_request` receives `bytes`; rebuild with `np.frombuffer` |
| `binary_sum` | Triton Binary (`Inference-Header-Content-Length` + binary tail) | `lite_server.kserve.parse_inputs` → zero-copy ndarray views; `build_response` builds the envelope |

The server is byte-identical on the wire to KServe V2 / Triton — see
[docs/protocol.md](../../docs/protocol.md) for the full spec and the
intentional deviations.

## Prerequisites

```bash
pip install 'tritonclient[http]' numpy   # binary channel client + tensor handling
```

## Run

```bash
cd examples/25_kserve_v2
python -m lite_server serve --config server.yaml
```

## Test

```bash
python test_kserve.py
# raw tensor : sum=10.0 shape=[4] dtype=<f4 PASS
# triton bin : output0=[[11.0, 22.0], [33.0, 44.0]] PASS
```

### Raw tensor bytes (any HTTP client)

```python
import numpy as np
import requests

arr = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
resp = requests.post(
    "http://localhost:8000/v2/models/raw_tensor/infer",
    data=arr.tobytes(),
    headers={
        "Content-Type": "application/octet-stream",
        "x-tensor-dtype": arr.dtype.str,   # "<f4" — byte order is explicit
        "x-tensor-shape": "4",
    },
)
print(resp.json())  # {"sum": 10.0, "shape": [4], "dtype": "<f4"}
```

### Triton Binary with the official client

`tritonclient` does the JSON-head + binary-tail framing automatically and
reassembles the response when `binary_data=True` is requested:

```python
import numpy as np
import tritonclient.http as httpclient

client = httpclient.InferenceServerClient(url="localhost:8000")
a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
b = np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32)
inp_a = httpclient.InferInput("a", [2, 2], "FP32")
inp_a.set_data_from_numpy(a)
inp_b = httpclient.InferInput("b", [2, 2], "FP32")
inp_b.set_data_from_numpy(b)
out = httpclient.InferRequestedOutput("output0", binary_data=True)
resp = client.infer("binary_sum", [inp_a, inp_b], outputs=[out])
print(resp.as_numpy("output0"))  # [[11. 22.] [33. 44.]] — values match
```
