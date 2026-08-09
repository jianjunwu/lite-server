"""G15 KServe 信封 helper 测试(阶段 2,批次 2)。

parse_inputs:binary_data memoryview → ndarray 零拷贝视图;
build_response:信封构造 + id 回显。
"""

import numpy as np
import pytest

from lite_server.context import Headers, RequestMeta, RequestContext
from lite_server.exceptions import HTTPException
from lite_server.kserve import build_response, parse_inputs


def _ctx(request: dict, binary_data=None) -> RequestContext:
    meta = RequestMeta(
        route="/predict",
        headers=Headers({"content-type": "application/octet-stream"}),
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=1,
    )
    return RequestContext(meta=meta, request=request, binary_data=binary_data)


class TestParseInputs:
    def test_parse_inputs_returns_views(self):
        """binary_data memoryview → ndarray 数值一致 + 零拷贝视图。"""
        head = {"inputs": [
            {"name": "a", "shape": [2, 2], "datatype": "FP32",
             "parameters": {"binary_data_size": 16}},
            {"name": "b", "shape": [3], "datatype": "INT64",
             "parameters": {"binary_data_size": 24}},
        ]}
        arr_a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        arr_b = np.array([1, 2, 3], dtype=np.int64)
        ctx = _ctx(head, {
            "a": memoryview(arr_a.tobytes()),
            "b": memoryview(arr_b.tobytes()),
        })
        out = parse_inputs(ctx)
        assert set(out.keys()) == {"a", "b"}
        assert out["a"].dtype == np.float32
        assert out["a"].shape == (2, 2)
        np.testing.assert_array_equal(out["a"], arr_a)
        np.testing.assert_array_equal(out["b"], arr_b)
        # 视图(零拷贝):ndarray 与 memoryview 共享底层缓冲
        assert out["a"].base is not None, "np.frombuffer 必须返回视图"

    def test_parse_inputs_empty_when_no_binary(self):
        """非 Triton 请求(binary_data None)→ {}。"""
        ctx = _ctx({"inputs": []}, None)
        assert parse_inputs(ctx) == {}

    def test_parse_inputs_missing_datatype_400(self):
        head = {"inputs": [
            {"name": "a", "shape": [1], "parameters": {"binary_data_size": 4}},
        ]}
        ctx = _ctx(head, {"a": memoryview(b"\x00\x00\x80\x3f")})
        with pytest.raises(HTTPException) as ei:
            parse_inputs(ctx)
        assert ei.value.status_code == 400

    def test_parse_inputs_bf16_view(self):
        """BF16 无原生 dtype → uint16 位模式视图。"""
        head = {"inputs": [
            {"name": "a", "shape": [1], "datatype": "BF16",
             "parameters": {"binary_data_size": 2}},
        ]}
        ctx = _ctx(head, {"a": memoryview(b"\x00\x3f")})  # 1.0 bf16
        out = parse_inputs(ctx)
        assert out["a"].dtype == np.uint16
        assert int(out["a"][0]) == 0x3F00


class TestBuildResponse:
    def test_build_response_envelope_and_id(self):
        arr = np.array([1.0, 2.0], dtype=np.float32)
        resp = build_response({"o": arr}, request_id="r1")
        assert resp["id"] == "r1"
        assert resp["outputs"][0] == {
            "name": "o",
            "shape": [2],
            "datatype": "FP32",
            "data": [1.0, 2.0],
        }

    def test_build_response_without_id(self):
        resp = build_response({"o": np.array([1], dtype=np.int64)})
        assert "id" not in resp
        assert resp["outputs"][0]["datatype"] == "INT64"

    def test_build_response_multidim_data_is_flat(self):
        """KServe 规范:JSON data 为 row-major 1-D 数组(shape 决定维数)。

        多维输出若 data 嵌套(``arr.tolist()``),Rust 响应转换层
        ``encode_value`` 逐元素编码时对非标量元素 400
        (binary_data_output 请求 → "data element is not a JSON number")。
        """
        arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        resp = build_response({"o": arr})
        assert resp["outputs"][0]["shape"] == [2, 2]
        assert resp["outputs"][0]["data"] == [1.0, 2.0, 3.0, 4.0]

    def test_build_response_roundtrip_with_parse(self):
        """parse → build 数值一致(analyzer LS401 codec 对称的正解)。"""
        arr = np.array([[1.5, -2.5]], dtype=np.float32)
        resp = build_response({"a": arr})
        ctx = _ctx({"inputs": [
            {"name": "a", "shape": [1, 2], "datatype": "FP32",
             "parameters": {"binary_data_size": 8}},
        ]}, {"a": memoryview(arr.tobytes())})
        out = parse_inputs(ctx)
        np.testing.assert_array_equal(out["a"], arr)

    def test_build_response_unsupported_dtype_400(self):
        with pytest.raises(HTTPException) as ei:
            build_response({"o": np.array(["x"], dtype="<U1")})
        assert ei.value.status_code == 400
