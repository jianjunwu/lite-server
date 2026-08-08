"""Triton Binary Tensor Data Extension worker 侧切分测试(阶段 1,批次 1)。

覆盖 D4 规则:inference-header-content-length > 0 → ctx.request = JSON 头 dict、
ctx.binary_data = {name: memoryview};header=0/垃圾值 → 既有分流;batch 路径不解析。
"""

import json
import logging

import pytest

from lite_server.api import LitAPI
from lite_server.callbacks import Callback
from lite_server.context import Headers, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import Pipeline, _split_binary_inputs
from lite_server.proto import BatchItem, BatchRequest
from lite_server.worker.dispatch import _handle_batch


class _EchoAPI(LitAPI):
    def setup(self, device):
        pass

    def predict(self, x):
        # 固定可序列化输出:本文件断言的是 ctx.request/binary_data,不是回显
        return {"echo": "ok"}


def _make_meta(headers=None, **overrides):
    kwargs = dict(
        route="/predict",
        headers=Headers({"content-type": "application/json"}),
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=123456789,
    )
    if headers is not None:
        kwargs["headers"] = Headers(headers)
    kwargs.update(overrides)
    return RequestMeta(**kwargs)


class _Capture(Callback):
    """在 before_decode_request 捕获 ctx(request/binary_data 已就位)。"""

    def __init__(self):
        self.ctx = None

    def before_decode_request(self, ctx):
        self.ctx = ctx


def _triton_body(head, tail):
    head_bytes = json.dumps(head).encode()
    return head_bytes + tail, len(head_bytes)


# ============================================================================
# run_single 切分
# ============================================================================


class TestRunSingleTritonBinary:
    @pytest.mark.asyncio
    async def test_run_single_triton_binary(self):
        """header > 0 → ctx.request = JSON 头 dict;binary_data = {name: 视图}。"""
        head = {
            "id": "req-1",
            "inputs": [
                {"name": "a", "shape": [2], "datatype": "FP32",
                 "parameters": {"binary_data_size": 4}},
                {"name": "b", "shape": [1], "datatype": "BYTES",
                 "parameters": {"binary_data_size": 6}},
            ],
        }
        body, head_len = _triton_body(head, b"\x00\x01\x02\x03" + b"\x04\x00\x00\x00hi")
        cap = _Capture()
        pipe = Pipeline.build(_EchoAPI(), [cap])
        meta = _make_meta(headers={
            "content-type": "application/octet-stream",
            "inference-header-content-length": str(head_len),
        })
        await pipe.run_single(body, meta)
        assert cap.ctx.request == head, "ctx.request 必须是被解析的 JSON 头 dict"
        bd = cap.ctx.binary_data
        assert set(bd.keys()) == {"a", "b"}
        assert bd["a"] == b"\x00\x01\x02\x03", "切分顺序 = 声明顺序"
        assert bd["b"] == b"\x04\x00\x00\x00hi"
        assert isinstance(bd["a"], memoryview), "零拷贝视图(D4:避免第二份拷贝)"

    @pytest.mark.asyncio
    async def test_run_single_triton_binary_mixed_inputs(self):
        """部分 input 走 JSON data、部分二进制 → Σ 只加二进制者。"""
        head = {
            "inputs": [
                {"name": "json_in", "shape": [2], "datatype": "INT32", "data": [1, 2]},
                {"name": "bin_in", "shape": [1], "datatype": "BYTES",
                 "parameters": {"binary_data_size": 6}},
            ],
        }
        body, head_len = _triton_body(head, b"\x04\x00\x00\x00xy")
        cap = _Capture()
        pipe = Pipeline.build(_EchoAPI(), [cap])
        meta = _make_meta(headers={
            "content-type": "application/octet-stream",
            "inference-header-content-length": str(head_len),
        })
        await pipe.run_single(body, meta)
        assert cap.ctx.request == head
        assert cap.ctx.binary_data["bin_in"] == b"\x04\x00\x00\x00xy"

    @pytest.mark.asyncio
    async def test_split_binary_inputs_size_mismatch(self):
        """Σ 不匹配 → HTTPException 400(on_error 路径)。"""
        head = {"inputs": [
            {"name": "a", "parameters": {"binary_data_size": 8}},
        ]}
        with pytest.raises(HTTPException) as ei:
            _split_binary_inputs(head, memoryview(b"short"))
        assert ei.value.status_code == 400

    @pytest.mark.asyncio
    async def test_split_binary_inputs_duplicate_name(self):
        """重名 → HTTPException 400(与 Rust 侧双保险)。"""
        head = {"inputs": [
            {"name": "a", "parameters": {"binary_data_size": 2}},
            {"name": "a", "parameters": {"binary_data_size": 2}},
        ]}
        with pytest.raises(HTTPException) as ei:
            _split_binary_inputs(head, memoryview(b"abcd"))
        assert ei.value.status_code == 400


# ============================================================================
# header=0 / 垃圾值 / 无 header 的既有分流
# ============================================================================


class TestDispatchFallbacks:
    @pytest.mark.asyncio
    async def test_triton_binary_zero_header_raw_passthrough(self):
        """header=0 → 落回既有 raw 路径,ctx.request = 原始 bytes,不误 400(C3)。"""
        cap = _Capture()
        pipe = Pipeline.build(_EchoAPI(), [cap])
        meta = _make_meta(headers={
            "content-type": "application/octet-stream",
            "inference-header-content-length": "0",
        })
        await pipe.run_single(b"\x01\x02\x03", meta)
        assert cap.ctx.request == b"\x01\x02\x03"
        assert cap.ctx.binary_data is None

    @pytest.mark.asyncio
    async def test_triton_binary_garbage_header_value(self):
        """header 非数字 → 降级既有 json 分流,不 500(C3)。"""
        cap = _Capture()
        pipe = Pipeline.build(_EchoAPI(), [cap])
        meta = _make_meta(headers={
            "content-type": "application/json",
            "inference-header-content-length": "garbage",
        })
        await pipe.run_single(b'{"x": 1}', meta)
        assert cap.ctx.request == {"x": 1}
        assert cap.ctx.binary_data is None

    @pytest.mark.asyncio
    async def test_no_header_keeps_existing_paths(self):
        """无 header → json/bytes 既有行为回归。"""
        cap = _Capture()
        pipe = Pipeline.build(_EchoAPI(), [cap])
        await pipe.run_single(b'{"x": 1}', _make_meta())  # json
        assert cap.ctx.request == {"x": 1}
        assert cap.ctx.binary_data is None
        cap2 = _Capture()
        pipe2 = Pipeline.build(_EchoAPI(), [cap2])
        await pipe2.run_single(b"\x01\x02", _make_meta(headers={
            "content-type": "application/octet-stream",
        }))
        assert cap2.ctx.request == b"\x01\x02"

    @pytest.mark.asyncio
    async def test_binary_data_defaults_none(self):
        """纯 JSON 请求 → binary_data 保持 None(默认值)。"""
        cap = _Capture()
        pipe = Pipeline.build(_EchoAPI(), [cap])
        await pipe.run_single(b'{"x": 1}', _make_meta())
        assert cap.ctx.binary_data is None


# ============================================================================
# batch 路径不支持(C8)
# ============================================================================


class TestBatchUnsupported:
    @pytest.mark.asyncio
    async def test_triton_binary_batch_unsupported(self):
        """batch 不解析该 header:垃圾值防御降级,不 500、不切分(C8)。"""
        seen = {}

        class BatchModel(_EchoAPI):
            def decode_request(self, request, ctx):
                # batch 路径 ctx.binary_data 恒 None,request = item.data 原样
                seen["binary_data"] = ctx.binary_data
                return request

        pipe = Pipeline.build(BatchModel(), [])
        body, head_len = _triton_body(
            {"inputs": [{"name": "a", "parameters": {"binary_data_size": 4}}]},
            b"\x00\x01\x02\x03",
        )
        meta = _make_meta(headers={
            "content-type": "application/octet-stream",
            "inference-header-content-length": str(head_len),
        })
        resp = await _handle_batch(
            pipe, "b1", BatchRequest(items=[BatchItem(uid="i1", data=body)]),
            meta, BatchModel(), logging.getLogger("test_triton_binary"),
        )
        assert len(resp.batch.items) == 1
        assert resp.batch.items[0].status.code == "Ok", "垃圾值不 500、不切分"
        assert seen["binary_data"] is None
