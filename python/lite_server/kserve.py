"""KServe V2 信封 helper(G15,阶段 2)。

`parse_inputs`:把 `ctx.binary_data` 的 memoryview 按 JSON 头
datatype/shape 转成 `np.ndarray` **视图**(零拷贝;BYTES 解析为
`list[bytes]`)——`np.frombuffer` 范式人人手写的封装;`build_response`:
信封构造(JSON data 数组 + id 回显)。
analyzer `kserve-v2` profile 的 LS401 codec 对称检查的正解。

依赖 numpy(dev/benchmark extra,非 core)——导入本模块即需要 numpy。
"""

from __future__ import annotations

import struct

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


def _decode_bytes_elements(mv: memoryview, name: str) -> list[bytes]:
    """BYTES datatype:每元素 4B LE 长度前缀 + 内容(与 Rust 响应侧
    `encode_value` 的 BYTES 臂对称)。截断/越界 → 400。"""
    out: list[bytes] = []
    offset = 0
    total = len(mv)
    while offset < total:
        if offset + 4 > total:
            raise HTTPException(
                400, f"input {name}: truncated BYTES length prefix",
                error_type="invalid_request_error", code="invalid_triton_binary_head",
            )
        (n,) = struct.unpack_from("<I", mv, offset)
        offset += 4
        if offset + n > total:
            raise HTTPException(
                400, f"input {name}: BYTES element overruns the binary block",
                error_type="invalid_request_error", code="invalid_triton_binary_head",
            )
        out.append(bytes(mv[offset:offset + n]))
        offset += n
    return out


def parse_inputs(ctx) -> dict[str, np.ndarray | list[bytes]]:
    """`ctx.binary_data`(memoryview 视图)→ `{name: np.ndarray}` 视图
    (BYTES → `list[bytes]`,审计修复 B11:与 Rust 响应侧对称)。

    按 JSON 头 `inputs[].datatype/shape` 映射并 reshape——零拷贝
    (np.frombuffer 直接接受 memoryview)。非 Triton 请求
    (`binary_data` 为 None)→ `{}`。shape 与数据量矛盾 / 尾长不被
    itemsize 整除 → 400(审计修复 B5:客户端输入错误不得逃逸为 500)。
    """
    if ctx.binary_data is None:
        return {}
    head = ctx.request if isinstance(ctx.request, dict) else {}
    inputs = head.get("inputs") or []
    meta = {i.get("name"): i for i in inputs if isinstance(i, dict)}
    result: dict[str, np.ndarray | list[bytes]] = {}
    for name, mv in ctx.binary_data.items():
        inp = meta.get(name, {})
        datatype = inp.get("datatype")
        if datatype is None:
            raise HTTPException(
                400, f"input {name} is missing datatype in the JSON head",
                error_type="invalid_request_error", code="invalid_triton_binary_head",
            )
        if datatype == "BYTES":
            result[name] = _decode_bytes_elements(mv, name)
            continue
        dtype = _NP_DTYPE.get(datatype)
        if dtype is None:
            raise HTTPException(
                400, f"unsupported datatype {datatype!r} for input {name}",
                error_type="invalid_request_error", code="invalid_triton_binary_head",
            )
        try:
            arr = np.frombuffer(mv, dtype=dtype)  # 零拷贝视图
            shape = inp.get("shape")
            if isinstance(shape, list):
                arr = arr.reshape(shape)  # 视图
        except ValueError as e:
            raise HTTPException(
                400, f"input {name} data does not match declared datatype/shape: {e}",
                error_type="invalid_request_error", code="invalid_triton_binary_head",
            ) from e
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
