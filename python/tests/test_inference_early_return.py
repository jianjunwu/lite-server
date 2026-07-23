"""Early-return tests for the unified Pipeline engine.

Any stage (LitAPI method or callback hook) may short-circuit the pipeline by
returning a ``Response`` / ``ResponseWithHeaders``, or via ``ctx.respond()``.
Later stages and remaining hooks are skipped; the response is serialized with
its status code and headers.
"""

import json

import pytest

from lite_server.api import LitAPI, RequestMeta, ResponseWithHeaders
from lite_server.callback import Callback
from lite_server.pipeline import Pipeline
from lite_server.response import Response


def _make_meta(route="/predict", payload=None):
    return RequestMeta(
        route=route,
        headers={"content-type": "application/json"},
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=123456789,
        payload=payload if payload is not None else {"input": "hello"},
    )


class _TrackerAPI(LitAPI):
    """A minimal LitAPI that records which methods were called."""

    def __init__(self):
        super().__init__()
        self.calls = []

    def decode_request(self, request):
        self.calls.append("decode_request")
        return request

    def predict(self, x):
        self.calls.append("predict")
        return x

    def encode_response(self, output):
        self.calls.append("encode_response")
        return output

    def on_request(self, request, meta):
        self.calls.append("on_request")
        return request

    def on_response(self, response, meta):
        self.calls.append("on_response")
        return response


def _pipeline(api, callbacks=()):
    pipe = Pipeline.build(api, list(callbacks))
    api._pipeline = pipe
    return pipe


async def _run(api, callbacks=(), data=None, meta=None):
    if data is None:
        data = json.dumps({"input": "hello"}).encode()
    if meta is None:
        meta = _make_meta()
    return await _pipeline(api, callbacks).run_single(data, meta)


# ---------------------------------------------------------------------------
# Early return at every pipeline point
# ---------------------------------------------------------------------------

class TestEarlyReturnPoints:
    @pytest.mark.asyncio
    async def test_early_return_at_api_on_request(self):
        api = _TrackerAPI()
        api.on_request = lambda req, meta: ResponseWithHeaders(
            body={"early": True}, headers={"X-Stage": "on_request"}
        )

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"early": true}'
        assert headers == {"X-Stage": "on_request"}
        assert "decode_request" not in api.calls
        assert "predict" not in api.calls
        assert "encode_response" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_callback_on_request(self):
        class EarlyCB(Callback):
            def on_request(self, ctx):
                return ResponseWithHeaders(body={"from_cb": 1}, headers={"X-Cb": "on_request"})

        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api, [EarlyCB()])
        assert resp_bytes == b'{"from_cb": 1}'
        assert headers == {"X-Cb": "on_request"}
        assert "decode_request" not in api.calls
        assert "predict" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_decode_request(self):
        api = _TrackerAPI()
        api.decode_request = lambda req: ResponseWithHeaders(
            body={"decoded": True}, headers={"X-Stage": "decode_request"}
        )

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"decoded": true}'
        assert headers == {"X-Stage": "decode_request"}
        assert "predict" not in api.calls
        assert "encode_response" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_callback_on_input(self):
        class EarlyCB(Callback):
            def on_input(self, ctx):
                return ResponseWithHeaders(body={"from_cb": 2}, headers={"X-Cb": "on_input"})

        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api, [EarlyCB()])
        assert resp_bytes == b'{"from_cb": 2}'
        assert headers == {"X-Cb": "on_input"}
        assert "predict" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_predict(self):
        api = _TrackerAPI()
        api.predict = lambda x: ResponseWithHeaders(
            body={"predicted": True}, headers={"X-Stage": "predict"}
        )

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"predicted": true}'
        assert headers == {"X-Stage": "predict"}
        assert "encode_response" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_callback_on_output(self):
        class EarlyCB(Callback):
            def on_output(self, ctx):
                return ResponseWithHeaders(body={"from_cb": 4}, headers={"X-Cb": "on_output"})

        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api, [EarlyCB()])
        assert resp_bytes == b'{"from_cb": 4}'
        assert headers == {"X-Cb": "on_output"}
        assert "encode_response" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_encode_response(self):
        api = _TrackerAPI()
        api.encode_response = lambda out: ResponseWithHeaders(
            body={"encoded": True}, headers={"X-Stage": "encode_response"}
        )

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"encoded": true}'
        assert headers == {"X-Stage": "encode_response"}

    @pytest.mark.asyncio
    async def test_early_return_at_callback_on_response(self):
        class EarlyCB(Callback):
            def on_response(self, ctx):
                return ResponseWithHeaders(body={"from_cb": 6}, headers={"X-Cb": "on_response"})

        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api, [EarlyCB()])
        assert resp_bytes == b'{"from_cb": 6}'
        assert headers == {"X-Cb": "on_response"}

    @pytest.mark.asyncio
    async def test_early_return_at_api_on_response(self):
        api = _TrackerAPI()
        api.on_response = lambda resp, meta: ResponseWithHeaders(
            body={"final": True}, headers={"X-Stage": "on_response"}
        )

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"final": true}'
        assert headers == {"X-Stage": "on_response"}

    # ---- plain Response (status code / media type embedded) ----

    @pytest.mark.asyncio
    async def test_early_return_plain_response_status_code(self):
        api = _TrackerAPI()
        api.decode_request = lambda req: Response(
            content={"error": "bad input"}, status_code=400
        )

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"error": "bad input"}'
        assert headers == {"_sc": "400"}
        assert "predict" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_plain_response_at_predict(self):
        api = _TrackerAPI()
        api.predict = lambda x: Response(content={"rejected": True}, status_code=422)

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"rejected": true}'
        assert headers == {"_sc": "422"}
        assert "encode_response" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_non_json_media_type(self):
        api = _TrackerAPI()
        api.predict = lambda x: Response(content="<h1>hi</h1>", media_type="text/html")

        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'"<h1>hi</h1>"'
        assert headers == {"_mt": "text/html"}

    # ---- ctx.respond ----

    @pytest.mark.asyncio
    async def test_ctx_respond_in_callback(self):
        class CacheCB(Callback):
            def on_request(self, ctx):
                ctx.respond({"cached": True}, status_code=200, headers={"X-Cache": "1"})

        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api, [CacheCB()])
        assert resp_bytes == b'{"cached": true}'
        assert headers == {"X-Cache": "1"}
        assert "predict" not in api.calls


# ---------------------------------------------------------------------------
# Flow semantics
# ---------------------------------------------------------------------------

class TestEarlyReturnFlow:
    @pytest.mark.asyncio
    async def test_normal_flow_still_works(self):
        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"input": "hello"}'
        assert headers is None
        for name in ("on_request", "decode_request", "predict", "encode_response", "on_response"):
            assert name in api.calls

    @pytest.mark.asyncio
    async def test_remaining_hooks_skipped_after_early_return(self):
        """Once a hook sets early return, subsequent hooks (same chain and
        later chains) are skipped — the first early response wins."""
        calls = []

        class CallbackA(Callback):
            def on_request(self, ctx):
                calls.append("A_on_request")
                ctx.respond({"early": True}, headers={"X-Cb": "A"})

        class CallbackB(Callback):
            def on_request(self, ctx):
                calls.append("B_on_request")

            def on_output(self, ctx):
                calls.append("B_on_output")

        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api, [CallbackA(), CallbackB()])
        assert resp_bytes == b'{"early": true}'
        assert headers == {"X-Cb": "A"}
        assert calls == ["A_on_request"]
        assert "predict" not in api.calls

    @pytest.mark.asyncio
    async def test_metrics_collected_on_early_return(self):
        from lite_server.api import _MetricSpec

        class MetricCB(Callback):
            def on_request(self, ctx):
                ctx.respond("done")

        api = _TrackerAPI()
        api._metric_specs = [_MetricSpec("my_gauge", "gauge", 0)]
        api._metric_values = [(0, 3.14)]

        resp_bytes, status, metrics, headers = await _run(api, [MetricCB()])
        assert resp_bytes == b'"done"'
        assert metrics is not None
        assert len(metrics.gauges) == 1
        # protobuf float (f32) has limited precision for 3.14
        assert metrics.gauges[0].value == pytest.approx(3.14, rel=1e-5)

    @pytest.mark.asyncio
    async def test_async_model_early_return(self):
        """Async methods participate in early return identically."""

        class AsyncTracker(LitAPI):
            def __init__(self):
                super().__init__()
                self.calls = []

            async def decode_request(self, request):
                self.calls.append("decode_request")
                return request

            async def predict(self, x):
                self.calls.append("predict")
                return ResponseWithHeaders(body={"pred": True}, headers={"X-S": "p"})

            async def encode_response(self, output):
                self.calls.append("encode_response")
                return output

        api = AsyncTracker()
        resp_bytes, status, metrics, headers = await _run(api)
        assert resp_bytes == b'{"pred": true}'
        assert headers == {"X-S": "p"}
        assert "encode_response" not in api.calls

    @pytest.mark.asyncio
    async def test_async_callback_early_return(self):
        class AsyncEarlyCB(Callback):
            async def on_input(self, ctx):
                return ResponseWithHeaders(body={"cb": 2}, headers={"X-Cb": "async"})

        api = _TrackerAPI()
        resp_bytes, status, metrics, headers = await _run(api, [AsyncEarlyCB()])
        assert resp_bytes == b'{"cb": 2}'
        assert headers == {"X-Cb": "async"}
        assert "predict" not in api.calls
