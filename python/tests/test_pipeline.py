"""Contract tests for the unified Pipeline engine.

Covers hook ordering, early return, error propagation, ctx.state,
sync/async adaptation, capability detection, and lifecycle hooks.
"""

import json
import threading

import pytest

from lite_server.api import LitAPI, RequestMeta, ResponseWithHeaders
from lite_server.callback import Callback, RequestContext
from lite_server.exceptions import BadRequestError, HTTPException
from lite_server.pipeline import (
    Pipeline,
    collect_metrics,
    extract_response_meta,
    unwrap_response,
)
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


class EchoAPI(LitAPI):
    def setup(self, device):
        pass

    def predict(self, x):
        return {"echo": x}


def _body(resp_bytes):
    return json.loads(resp_bytes.decode())


# ---------------------------------------------------------------------------
# Hook ordering
# ---------------------------------------------------------------------------


class TestHookOrder:
    @pytest.mark.asyncio
    async def test_full_order_single_request(self):
        order = []
        meta = _make_meta()

        class Ordered(EchoAPI):
            def on_request(self, request, meta):
                order.append("api.on_request")
                return request

            def decode_request(self, request):
                order.append("decode")
                return request

            def predict(self, x):
                order.append("predict")
                return x

            def encode_response(self, output):
                order.append("encode")
                return output

            def on_response(self, response, meta):
                order.append("api.on_response")
                return response

        class CB(Callback):
            def on_request(self, ctx):
                order.append("cb.on_request")

            def on_input(self, ctx):
                order.append("cb.on_input")

            def on_output(self, ctx):
                order.append("cb.on_output")

            def on_response(self, ctx):
                order.append("cb.on_response")

        pipe = Pipeline.build(Ordered(), [CB()])
        resp_bytes, status, metrics, headers = await pipe.run_single(
            json.dumps({"input": 1}).encode(), meta
        )
        assert order == [
            "api.on_request",
            "cb.on_request",
            "decode",
            "cb.on_input",
            "predict",
            "cb.on_output",
            "encode",
            "cb.on_response",
            "api.on_response",
        ]
        assert status.code == "Ok"
        assert _body(resp_bytes) == {"input": 1}

    @pytest.mark.asyncio
    async def test_multiple_callbacks_run_in_registration_order(self):
        order = []

        class C1(Callback):
            def on_request(self, ctx):
                order.append("c1")

        class C2(Callback):
            def on_request(self, ctx):
                order.append("c2")

        pipe = Pipeline.build(EchoAPI(), [C1(), C2()])
        await pipe.run_single(b"{}", _make_meta())
        assert order == ["c1", "c2"]


# ---------------------------------------------------------------------------
# Early return
# ---------------------------------------------------------------------------


class TestEarlyReturn:
    @pytest.mark.asyncio
    async def test_hook_returning_response_short_circuits(self):
        called = []

        class CacheCB(Callback):
            def on_request(self, ctx):
                return Response(
                    content={"cached": True}, status_code=200, headers={"X-Cache": "1"}
                )

        class API(EchoAPI):
            def predict(self, x):
                called.append("predict")
                return x

        pipe = Pipeline.build(API(), [CacheCB()])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert called == []
        assert _body(resp_bytes) == {"cached": True}
        assert headers == {"X-Cache": "1"}

    @pytest.mark.asyncio
    async def test_ctx_respond_short_circuits(self):
        called = []

        class Validator(Callback):
            def on_input(self, ctx):
                if not isinstance(ctx.input, dict) or "x" not in ctx.input:
                    ctx.respond({"error": "missing x"}, status_code=400)

        class API(EchoAPI):
            def decode_request(self, request):
                return request

            def predict(self, x):
                called.append("predict")
                return x

        pipe = Pipeline.build(API(), [Validator()])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert called == []
        assert _body(resp_bytes) == {"error": "missing x"}
        sc, mt, clean = extract_response_meta(headers)
        assert sc == 400

    @pytest.mark.asyncio
    async def test_predict_returning_response_short_circuits_encode(self):
        called = []

        class API(EchoAPI):
            def predict(self, x):
                return Response(content={"direct": True})

            def encode_response(self, output):
                called.append("encode")
                return output

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b"{}", _make_meta())
        assert called == []
        assert _body(resp_bytes) == {"direct": True}

    @pytest.mark.asyncio
    async def test_decode_returning_response_short_circuits(self):
        class API(EchoAPI):
            def decode_request(self, request):
                return ResponseWithHeaders(body={"early": True}, headers={"X-E": "1"})

            def predict(self, x):
                raise AssertionError("predict must not run")

        pipe = Pipeline.build(API(), [])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert _body(resp_bytes) == {"early": True}
        assert headers == {"X-E": "1"}

    @pytest.mark.asyncio
    async def test_hooks_after_early_return_are_skipped(self):
        order = []

        class First(Callback):
            def on_request(self, ctx):
                order.append("first")
                ctx.respond({"stop": True})

        class Second(Callback):
            def on_request(self, ctx):
                order.append("second")

            def on_output(self, ctx):
                order.append("second.output")

        pipe = Pipeline.build(EchoAPI(), [First(), Second()])
        resp_bytes, *_ = await pipe.run_single(b"{}", _make_meta())
        assert order == ["first"]
        assert _body(resp_bytes) == {"stop": True}


# ---------------------------------------------------------------------------
# Error propagation (validation semantics)
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    @pytest.mark.asyncio
    async def test_http_exception_from_hook_propagates(self):
        class Auth(Callback):
            def on_request(self, ctx):
                raise BadRequestError("bad input", param="x")

        pipe = Pipeline.build(EchoAPI(), [Auth()])
        with pytest.raises(HTTPException) as exc_info:
            await pipe.run_single(b"{}", _make_meta())
        assert exc_info.value.status_code == 400
        assert exc_info.value.param == "x"

    @pytest.mark.asyncio
    async def test_generic_exception_from_hook_propagates(self):
        """Data-hook exceptions are NOT swallowed (pre-0.7 they were)."""

        class Boom(Callback):
            def on_output(self, ctx):
                raise ValueError("nope")

        pipe = Pipeline.build(EchoAPI(), [Boom()])
        with pytest.raises(ValueError):
            await pipe.run_single(b"{}", _make_meta())

    @pytest.mark.asyncio
    async def test_http_exception_from_api_on_request_propagates(self):
        class API(EchoAPI):
            def on_request(self, request, meta):
                raise BadRequestError("rejected")

        pipe = Pipeline.build(API(), [])
        with pytest.raises(HTTPException):
            await pipe.run_single(b"{}", _make_meta())


# ---------------------------------------------------------------------------
# ctx.state and value replacement
# ---------------------------------------------------------------------------


class TestContextState:
    @pytest.mark.asyncio
    async def test_state_shared_between_hooks(self):
        seen = {}

        class Tracer(Callback):
            def on_request(self, ctx):
                ctx.state["t0"] = 123

            def on_output(self, ctx):
                seen["t0"] = ctx.state["t0"]

        pipe = Pipeline.build(EchoAPI(), [Tracer()])
        await pipe.run_single(b"{}", _make_meta())
        assert seen["t0"] == 123

    @pytest.mark.asyncio
    async def test_hook_return_replaces_value(self):
        class Rewrite(Callback):
            def on_request(self, ctx):
                return {"replaced": True}

        seen = {}

        class API(EchoAPI):
            def decode_request(self, request):
                seen["request"] = request
                return request

        pipe = Pipeline.build(API(), [Rewrite()])
        await pipe.run_single(b"{}", _make_meta())
        assert seen["request"] == {"replaced": True}

    @pytest.mark.asyncio
    async def test_hook_inplace_mutation(self):
        class Mutate(Callback):
            def on_input(self, ctx):
                ctx.input["extra"] = 1

        seen = {}

        class API(EchoAPI):
            def predict(self, x):
                seen["input"] = x
                return x

        pipe = Pipeline.build(API(), [Mutate()])
        await pipe.run_single(json.dumps({"a": 1}).encode(), _make_meta())
        assert seen["input"] == {"a": 1, "extra": 1}

    @pytest.mark.asyncio
    async def test_state_is_per_request(self):
        class Counter(Callback):
            def on_request(self, ctx):
                ctx.state["n"] = ctx.request.get("n")

        captured = []

        class API(EchoAPI):
            def on_response(self, response, meta):
                return response

            def predict(self, x):
                return x

        pipe = Pipeline.build(API(), [Counter()])
        ctx1 = RequestContext(meta=_make_meta(), request={"n": 1})
        ctx2 = RequestContext(meta=_make_meta(), request={"n": 2})
        await pipe.preprocess(ctx1)
        await pipe.preprocess(ctx2)
        assert ctx1.state["n"] == 1
        assert ctx2.state["n"] == 2


# ---------------------------------------------------------------------------
# Sync/async adaptation
# ---------------------------------------------------------------------------


class TestSyncAsyncAdaptation:
    @pytest.mark.asyncio
    async def test_fully_sync_model_uses_no_executor(self):
        pipe = Pipeline.build(EchoAPI(), [])
        assert pipe.any_async is False
        assert pipe._executor is None
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_meta())
        assert _body(resp_bytes) == {"echo": {"x": 1}}

    @pytest.mark.asyncio
    async def test_async_predict_runs_natively(self):
        class AsyncAPI(EchoAPI):
            async def predict(self, x):
                return {"async": x}

        pipe = Pipeline.build(AsyncAPI(), [])
        assert pipe.any_async is True
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_meta())
        assert _body(resp_bytes) == {"async": {"x": 1}}

    @pytest.mark.asyncio
    async def test_sync_stage_runs_on_single_thread_executor_in_mixed_mode(self):
        threads = []

        class Mixed(EchoAPI):
            async def decode_request(self, request):
                return request

            def predict(self, x):
                threads.append(threading.current_thread().name)
                return x

            def encode_response(self, output):
                threads.append(threading.current_thread().name)
                return output

        pipe = Pipeline.build(Mixed(), [])
        assert pipe.any_async is True
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_meta())
        assert _body(resp_bytes) == {"x": 1}
        # Both sync stages ran on the SAME executor thread, not the loop thread
        assert len(threads) == 2
        assert threads[0] == threads[1]
        assert "lite-sync" in threads[0]
        pipe.close()

    @pytest.mark.asyncio
    async def test_async_callback_hook(self):
        order = []

        class AsyncCB(Callback):
            async def on_request(self, ctx):
                order.append("async.on_request")

        pipe = Pipeline.build(EchoAPI(), [AsyncCB()])
        assert pipe.any_async is True
        await pipe.run_single(b"{}", _make_meta())
        assert order == ["async.on_request"]
        pipe.close()

    @pytest.mark.asyncio
    async def test_concurrent_sync_predicts_are_serialized_in_mixed_mode(self):
        """Sync code must never run concurrently (pre-0.7 standard-loop
        semantics preserved)."""
        import asyncio

        active = 0
        max_active = 0

        class Mixed(EchoAPI):
            async def decode_request(self, request):
                return request

            def predict(self, x):
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                import time

                time.sleep(0.01)
                active -= 1
                return x

        pipe = Pipeline.build(Mixed(), [])
        await asyncio.gather(
            *[pipe.run_single(b'{"x": 1}', _make_meta()) for _ in range(5)]
        )
        assert max_active == 1
        pipe.close()


# ---------------------------------------------------------------------------
# Capability detection
# ---------------------------------------------------------------------------


class TestCapabilityDetection:
    def test_has_batch_methods_requires_both_overridden(self):
        class OnlyBatch(EchoAPI):
            def batch(self, inputs):
                return inputs

        class Both(EchoAPI):
            def batch(self, inputs):
                return inputs

            def unbatch(self, output):
                return output

        assert Pipeline.build(EchoAPI(), []).has_batch_methods is False
        assert Pipeline.build(OnlyBatch(), []).has_batch_methods is False
        assert Pipeline.build(Both(), []).has_batch_methods is True

    def test_has_stream_predict_only_when_overridden(self):
        class Streamer(EchoAPI):
            def stream_predict(self, request):
                yield {"x": 1}

        assert Pipeline.build(EchoAPI(), []).has_stream_predict is False
        assert Pipeline.build(Streamer(), []).has_stream_predict is True

    def test_has_bidi_stream_only_when_overridden(self):
        from lite_server.api import BidiStreamHandler

        class H(BidiStreamHandler):
            pass

        class Bidi(EchoAPI):
            def bidi_stream(self):
                return H()

        assert Pipeline.build(EchoAPI(), []).has_bidi_stream is False
        assert Pipeline.build(Bidi(), []).has_bidi_stream is True

    @pytest.mark.asyncio
    async def test_batch_predict_arity_check(self):
        class Bad(EchoAPI):
            def batch(self, inputs):
                return inputs

            def predict(self, x):
                return x

            def unbatch(self, output):
                return output[:1]  # wrong arity

        pipe = Pipeline.build(Bad(), [])
        with pytest.raises(ValueError, match="unbatch"):
            await pipe.batch_predict([1, 2, 3])


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------


class TestLifecycleHooks:
    def test_setup_and_teardown_hooks_fire_in_order(self):
        order = []

        class LC(Callback):
            def on_before_setup(self, config, device):
                order.append(("before", device))

            def on_after_setup(self, lit_api):
                order.append(("after", type(lit_api).__name__))

            def on_teardown(self, lit_api):
                order.append(("teardown",))

        api = EchoAPI()
        pipe = Pipeline.build(api, [LC()])
        pipe.trigger_lifecycle("on_before_setup", {"k": 1}, "cpu")
        api.setup("cpu")
        pipe.trigger_lifecycle("on_after_setup", api)
        pipe.trigger_lifecycle("on_teardown", api)
        assert order == [("before", "cpu"), ("after", "EchoAPI"), ("teardown",)]

    def test_lifecycle_exceptions_are_isolated(self):
        class Bad(Callback):
            def on_teardown(self, lit_api):
                raise RuntimeError("boom")

        class Good(Callback):
            def __init__(self):
                self.called = False

            def on_teardown(self, lit_api):
                self.called = True

        good = Good()
        pipe = Pipeline.build(EchoAPI(), [Bad(), good])
        pipe.trigger_lifecycle("on_teardown", EchoAPI())  # must not raise
        assert good.called is True

    def test_async_lifecycle_hook_is_driven(self):
        called = []

        class AsyncLC(Callback):
            async def on_after_setup(self, lit_api):
                called.append("async setup")

        pipe = Pipeline.build(EchoAPI(), [AsyncLC()])
        pipe.trigger_lifecycle("on_after_setup", EchoAPI())
        assert called == ["async setup"]

    def test_multiple_async_lifecycle_hooks_all_driven(self):
        """F5: all async lifecycle hooks are drained in a single event loop."""
        called = []

        class LC1(Callback):
            async def on_teardown(self, lit_api):
                called.append("lc1")

        class LC2(Callback):
            async def on_teardown(self, lit_api):
                called.append("lc2")

        pipe = Pipeline.build(EchoAPI(), [LC1(), LC2()])
        pipe.trigger_lifecycle("on_teardown", EchoAPI())
        assert called == ["lc1", "lc2"]

    def test_async_lifecycle_exception_isolation(self):
        """F5: an exception in one async hook does not prevent others."""
        called = []

        class BadLC(Callback):
            async def on_teardown(self, lit_api):
                raise RuntimeError("boom")

        class GoodLC(Callback):
            async def on_teardown(self, lit_api):
                called.append("good")

        pipe = Pipeline.build(EchoAPI(), [BadLC(), GoodLC()])
        # Must not raise
        pipe.trigger_lifecycle("on_teardown", EchoAPI())
        assert called == ["good"]


# ---------------------------------------------------------------------------
# finalize / helpers
# ---------------------------------------------------------------------------


class TestFinalize:
    @pytest.mark.asyncio
    async def test_status_code_and_media_type_embedded(self):
        class API(EchoAPI):
            def predict(self, x):
                return Response(content="plain", status_code=201, media_type="text/plain")

        pipe = Pipeline.build(API(), [])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        sc, mt, clean = extract_response_meta(headers)
        assert sc == 201
        assert mt == "text/plain"
        assert clean is None
        assert resp_bytes == b'"plain"'

    def test_unwrap_plain_value(self):
        assert unwrap_response({"a": 1}) == ({"a": 1}, None)

    def test_unwrap_response_with_headers(self):
        rwh = ResponseWithHeaders(body={"b": 2}, headers={"X-1": "v"})
        assert unwrap_response(rwh) == ({"b": 2}, {"X-1": "v"})

    def test_collect_metrics_empty(self):
        assert collect_metrics(EchoAPI()) is None

    def test_collect_metrics_gauge(self):
        api = EchoAPI()
        mid = api.register_metric("my_gauge", "gauge")
        api.report_metric(mid, 1.5)
        m = collect_metrics(api)
        assert m is not None
        assert len(m.gauges) == 1
        assert m.gauges[0].value == 1.5
        # buffer cleared after collection
        assert collect_metrics(api) is None
