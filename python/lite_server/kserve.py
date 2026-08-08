"""KServe V2 信封 helper(G15,阶段 2)。

`parse_inputs`:把 `ctx.binary_data` 的 memoryview 按 JSON 头
datatype/shape 转成 `np.ndarray` **视图**(零拷贝)——`np.frombuffer` 范式
人人手写的封装;`build_response`:信封构造(JSON data 数组 + id 回显)。
analyzer `kserve-v2` profile 的 LS401 codec 对称检查的正解。

numpy 是 dev/benchmark extra 依赖,非 core——函数内 lazy import,
缺失时 raise 明确错误。
"""

from __future__ import annotations

import numpy as np

from lite_server.exceptions import HTTPException

# KServe datatype → numpy dtype。BF16 无原生 dtype,以 uint16 视图呈现
# (内容即 bfloat16 位模式,转换归用户)。
_NP_DTYPE: dict[str, type[np.generic]] = {
    "BOOL": np.bool_,
    "INT8": np.int8,
    "UINT8": np.uint8,
    "INT16": np.int16,
    "UINT16": np.uint16,
    "INT32": np.int32,
    "UINT32": np.uint32,
    "INT64": np.int64,
    "UINT64": np.uint64,
    "FP16": np.float16,
    "FP32": np.float32,
    "FP64": np.float64,
    "BF16": np.uint16,  # bfloat16 位模式视图
}

_NP_DTYPE_INV: dict[np.dtype, str] = {
    np.dtype("bool"): "BOOL",
    np.dtype("int8"): "INT8",
    np.dtype("uint8"): "UINT8",
    np.dtype("int16"): "INT16",
    np.dtype("uint16"): "UINT16",
    np.dtype("int32"): "INT32",
    np.dtype("uint32"): "UINT32",
    np.dtype("int64"): "INT64",
    np.dtype("uint64"): "UINT64",
    np.dtype("float16"): "FP16",
    np.dtype("float32"): "FP32",
    np.dtype("float64"): "FP64",
}


def parse_inputs(ctx) -> dict[str, np.ndarray]:
    """`ctx.binary_data`(memoryview 视图)→ `{name: np.ndarray}` 视图。

    按 JSON 头 `inputs[].datatype/shape` 映射并 reshape——零拷贝
    (np.frombuffer 直接接受 memoryview)。非 Triton 请求
    (`binary_data` 为 None)→ `{}`。
    """
    if ctx.binary_data is None:
        return {}
    head = ctx.request if isinstance(ctx.request, dict) else {}
    inputs = head.get("inputs") or []
    meta = {i.get("name"): i for i in inputs if isinstance(i, dict)}
    result: dict[str, np.ndarray] = {}
    for name, mv in ctx.binary_data.items():
        inp = meta.get(name, {})
        datatype = inp.get("datatype")
        if datatype is None:
            raise HTTPException(
                400, f"input {name} is missing datatype in the JSON head",
                error_type="invalid_request_error", code="invalid_triton_binary_head",
            )
        dtype = _NP_DTYPE.get(datatype)
        if dtype is None:
            raise HTTPException(
                400, f"unsupported datatype {datatype!r} for input {name}",
                error_type="invalid_request_error", code="invalid_triton_binary_head",
            )
        arr = np.frombuffer(mv, dtype=dtype)  # 零拷贝视图
        shape = inp.get("shape")
        if isinstance(shape, list):
            arr = arr.reshape(shape)  # 视图
        result[name] = arr
    return result


def build_response(outputs: dict[str, np.ndarray], request_id: str | None = None) -> dict:
    """构造 KServe 信封 dict:`{id?, outputs: [{name, shape, datatype, data}]}`。

    `data` = `arr.tolist()`(JSON data 数组);客户端需要二进制时在请求侧
    加 `binary_data_output` flag,由 server 响应转换层二进制化。
    """
    out = []
    for name, arr in outputs.items():
        if not isinstance(arr, np.ndarray):
            arr = np.asarray(arr)
        datatype = _NP_DTYPE_INV.get(arr.dtype)
        if datatype is None:
            raise HTTPException(
                400, f"unsupported numpy dtype {arr.dtype} for output {name}",
                error_type="invalid_request_error", code="invalid_envelope",
            )
        out.append({
            "name": name,
            "shape": list(arr.shape),
            "datatype": datatype,
            "data": arr.tolist(),
        })
    resp: dict = {"outputs": out}
    if request_id:
        resp["id"] = request_id
    return resp
