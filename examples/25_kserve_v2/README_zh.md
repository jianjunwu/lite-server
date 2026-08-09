# 25 KServe V2 数据面

演示 **KServe V2 数据面**除普通 JSON 之外的两种传输:原始张量字节
(raw bytes)与 **Triton Binary Tensor Data Extension**(JSON 头 + 二进制尾),
含 `binary_data_output` 二进制响应协商。

[English](README.md)

## 核心概念

| 模型 | 覆盖的传输路径 | Worker 范式 |
|---|---|---|
| `raw_tensor` | 原始字节(`application/octet-stream` + `x-tensor-dtype` / `x-tensor-shape` 头) | `decode_request` 收到 `bytes`;用 `np.frombuffer` 重建 |
| `binary_sum` | Triton Binary(`Inference-Header-Content-Length` + 二进制尾) | `lite_server.kserve.parse_inputs` → 零拷贝 ndarray 视图;`build_response` 构造信封 |

线上字节与 KServe V2 / Triton 完全一致 — 完整规范与刻意偏差见
[docs/protocol.md](../../docs/protocol.md)。

## 前置依赖

```bash
pip install 'tritonclient[http]' numpy   # 二进制通道客户端 + 张量处理
```

## 运行

```bash
cd examples/25_kserve_v2
python -m lite_server serve --config server.yaml
```

## 测试

```bash
python test_kserve.py
# raw tensor : sum=10.0 shape=[4] dtype=<f4 PASS
# triton bin : output0=[[11.0, 22.0], [33.0, 44.0]] PASS
```

### 原始张量字节(任意 HTTP 客户端)

```python
import numpy as np
import requests

arr = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
resp = requests.post(
    "http://localhost:8000/v2/models/raw_tensor/infer",
    data=arr.tobytes(),
    headers={
        "Content-Type": "application/octet-stream",
        "x-tensor-dtype": arr.dtype.str,   # "<f4" — 字节序显式声明
        "x-tensor-shape": "4",
    },
)
print(resp.json())  # {"sum": 10.0, "shape": [4], "dtype": "<f4"}
```

### 官方客户端走 Triton Binary

`tritonclient` 自动完成 JSON 头 + 二进制尾的组帧;请求侧置
`binary_data=True` 时自动重组二进制响应:

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
print(resp.as_numpy("output0"))  # [[11. 22.] [33. 44.]] — 数值一致
```
