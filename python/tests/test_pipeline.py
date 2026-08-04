"""Contract tests for the unified Pipeline engine.

Covers hook ordering, early return, error propagation, ctx.state,
sync/async adaptation, capability detection, lifecycle hooks, and
ctx injection (0.7.0 context unification).
"""

import json
import threading

import pytest

from lite_server.api import LitAPI
from lite_server.callbacks import (
    Callback,
    JsonSchemaValidator,
)
from lite_server.context import RequestContext, RequestMeta, Headers
from lite_server.exceptions import BadRequestError, HTTPException
from lite_server.pipeline import (
    Pipeline,
    collect_metrics,
    extract_response_meta,
    unwrap_response,
)
from lite_server.response import Response


def _make_meta(route="/predict"):
    return RequestMeta(
        route=route,
        headers={"content-type": "application/json"},
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=123456789,
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
        meta = _make_route_meta()

        class Ordered(EchoAPI):
            def decode_request(self, request):
                order.append("decode")
                return request

            def predict(self, x):
                order.append("predict")
                return x

            def encode_response(self, output):
                order.append("encode")
                return output

        class CB(Callback):
            def before_decode_request(self, ctx):
                order.append("cb.before_decode_request")

            def after_decode_request(self, ctx):
                order.append("cb.after_decode_request")

            def after_predict(self, ctx):
                order.append("cb.after_predict")

            def after_encode_response(self, ctx):
                order.append("cb.after_encode_response")

        pipe = Pipeline.build(Ordered(), [CB()])
        resp_bytes, status, metrics, headers = await pipe.run_single(
            json.dumps({"input": 1}).encode(), meta
        )
        assert order == [
            "cb.before_decode_request",
            "decode",
            "cb.after_decode_request",
            "predict",
            "cb.after_predict",
            "encode",
            "cb.after_encode_response",
        ]
        assert status.code == "Ok"
        assert _body(resp_bytes) == {"input": 1}

    @pytest.mark.asyncio
    async def test_multiple_callbacks_run_in_registration_order(self):
        order = []

        class C1(Callback):
            def before_decode_request(self, ctx):
                order.append("c1")

        class C2(Callback):
            def before_decode_request(self, ctx):
                order.append("c2")

        pipe = Pipeline.build(EchoAPI(), [C1(), C2()])
        await pipe.run_single(b"{}", _make_route_meta())
        assert order == ["c1", "c2"]


class TestModeThreading:
    @pytest.mark.asyncio
    async def test_run_single_defaults_mode_to_unary(self):
        seen = {}

        class Rec(Callback):
            def before_decode_request(self, ctx):
                seen["mode"] = ctx.mode

        pipe = Pipeline.build(EchoAPI(), [Rec()])
        await pipe.run_single(b"{}", _make_meta())
        assert seen["mode"] == "unary"

    @pytest.mark.asyncio
    async def test_run_single_threads_explicit_mode(self):
        seen = {}

        class Rec(Callback):
            def before_decode_request(self, ctx):
                seen["mode"] = ctx.mode

        pipe = Pipeline.build(EchoAPI(), [Rec()])
        await pipe.run_single(b"{}", _make_meta(), mode="stream")
        assert seen["mode"] == "stream"


class TestStage:
    @pytest.mark.asyncio
    async def test_decode_stage_when_preprocess_fails(self):
        seen = {}

        class RecErr(Callback):
            def on_error(self, ctx, exc):
                seen["stage"] = ctx.stage

        class API(EchoAPI):
            def decode_request(self, request):
                raise ValueError("boom")

        pipe = Pipeline.build(API(), [RecErr()])
        with pytest.raises(ValueError):
            await pipe.run_single(b"{}", _make_meta())
        assert seen["stage"] == "decode_request"

    @pytest.mark.asyncio
    async def test_predict_stage_when_predict_fails(self):
        seen = {}

        class RecErr(Callback):
            def on_error(self, ctx, exc):
                seen["stage"] = ctx.stage

        class API(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(API(), [RecErr()])
        with pytest.raises(ValueError):
            await pipe.run_single(b"{}", _make_meta())
        assert seen["stage"] == "predict"

    @pytest.mark.asyncio
    async def test_encode_stage_when_postprocess_fails(self):
        seen = {}

        class RecErr(Callback):
            def on_error(self, ctx, exc):
                seen["stage"] = ctx.stage

        class API(EchoAPI):
            def encode_response(self, output):
                raise ValueError("boom")

        pipe = Pipeline.build(API(), [RecErr()])
        with pytest.raises(ValueError):
            await pipe.run_single(b"{}", _make_meta())
        assert seen["stage"] == "encode_response"

    @pytest.mark.asyncio
    async def test_batch_predict_stage_set_on_ctx_list(self):
        class API(EchoAPI):
            def batch(self, inputs):
                raise ValueError("boom")

        pipe = Pipeline.build(API(), [])
        ctx_a = RequestContext(meta=_make_meta(), mode="batch")
        ctx_b = RequestContext(meta=_make_meta(), mode="batch")
        with pytest.raises(ValueError):
            await pipe.batch_predict([1, 2], [ctx_a, ctx_b])
        assert ctx_a.stage == "batch_predict"
        assert ctx_b.stage == "batch_predict"


class _BatchAPI(EchoAPI):
    """Batch-capable stub: no-op batch/unbatch, predict = +1 per item."""

    max_batch_size = 4

    def batch(self, inputs):
        return list(inputs)

    def unbatch(self, output):
        return list(output)

    def predict(self, xs):
        return [x + 1 for x in xs]


class TestBatchHooks:
    @pytest.mark.asyncio
    async def test_after_batch_replaces_batched(self):
        seen = {}

        class Mul(Callback):
            def after_batch(self, ctx_list, batched):
                seen["batched"] = batched
                return [v * 100 for v in batched]

        pipe = Pipeline.build(_BatchAPI(), [Mul()])
        ctxs = [RequestContext(meta=_make_meta(), mode="batch") for _ in range(2)]
        outputs = await pipe.batch_predict([1, 2], ctxs)
        assert outputs == [101, 201]
        assert seen["batched"] == [1, 2]

    @pytest.mark.asyncio
    async def test_after_unbatch_replaces_outputs(self):
        class Tag(Callback):
            def after_unbatch(self, ctx_list, outputs):
                return [f"out:{o}" for o in outputs]

        pipe = Pipeline.build(_BatchAPI(), [Tag()])
        ctxs = [RequestContext(meta=_make_meta(), mode="batch") for _ in range(2)]
        outputs = await pipe.batch_predict([1, 2], ctxs)
        assert outputs == ["out:2", "out:3"]

    @pytest.mark.asyncio
    async def test_after_batch_http_exception_rejects_whole_batch(self):
        class Reject(Callback):
            def after_batch(self, ctx_list, batched):
                raise BadRequestError("bad batch")

        pipe = Pipeline.build(_BatchAPI(), [Reject()])
        ctxs = [RequestContext(meta=_make_meta(), mode="batch") for _ in range(2)]
        with pytest.raises(HTTPException):
            await pipe.batch_predict([1, 2], ctxs)

    @pytest.mark.asyncio
    async def test_batch_predict_unaffected_without_batch_hooks(self):
        class NoOp(Callback):
            def before_decode_request(self, ctx):
                pass

        pipe = Pipeline.build(_BatchAPI(), [NoOp()])
        ctxs = [RequestContext(meta=_make_meta(), mode="batch") for _ in range(2)]
        outputs = await pipe.batch_predict([1, 2], ctxs)
        assert outputs == [2, 3]

    def test_for_route_rejects_batch_hooks(self):
        class BadIn(Callback):
            def after_batch(self, ctx_list, batched):
                pass

        class BadOut(Callback):
            def after_unbatch(self, ctx_list, outputs):
                pass

        with pytest.raises(RuntimeError, match="after_batch"):
            Pipeline.for_route([BadIn()])
        with pytest.raises(RuntimeError, match="after_unbatch"):
            Pipeline.for_route([BadOut()])


class TestStreamClose:
    @pytest.mark.asyncio
    async def test_run_on_stream_close_drives_hooks_with_reason(self):
        seen = []

        class Rec(Callback):
            def on_stream_close(self, ctx, reason):
                seen.append((ctx.meta.request_id, reason))

        pipe = Pipeline.build(EchoAPI(), [Rec()])
        ctx = RequestContext(meta=_make_meta(), mode="stream")
        await pipe.run_on_stream_close(ctx, "done")
        assert seen == [("req-1", "done")]

    @pytest.mark.asyncio
    async def test_run_on_stream_close_is_exception_isolated(self):
        calls = []

        class Bad(Callback):
            def on_stream_close(self, ctx, reason):
                calls.append("bad")
                raise RuntimeError("boom")

        class Good(Callback):
            def on_stream_close(self, ctx, reason):
                calls.append("good")

        pipe = Pipeline.build(EchoAPI(), [Bad(), Good()])
        ctx = RequestContext(meta=_make_meta(), mode="stream")
        await pipe.run_on_stream_close(ctx, "error")  # must not raise
        assert calls == ["bad", "good"]


# ---------------------------------------------------------------------------
# Early return
# ---------------------------------------------------------------------------


class TestEarlyReturn:
    @pytest.mark.asyncio
    async def test_hook_returning_response_short_circuits(self):
        called = []

        class CacheCB(Callback):
            def before_decode_request(self, ctx):
                return Response(
                    content={"cached": True}, status_code=200, headers={"X-Cache": "1"}
                )

        class API(EchoAPI):
            def predict(self, x):
                called.append("predict")
                return x

        pipe = Pipeline.build(API(), [CacheCB()])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_route_meta())
        assert called == []
        assert _body(resp_bytes) == {"cached": True}
        assert headers == {"X-Cache": "1"}

    @pytest.mark.asyncio
    async def test_ctx_respond_short_circuits(self):
        called = []

        class Validator(Callback):
            def after_decode_request(self, ctx):
                if not isinstance(ctx.input, dict) or "x" not in ctx.input:
                    ctx.respond({"error": "missing x"}, status_code=400)

        class API(EchoAPI):
            def decode_request(self, request):
                return request

            def predict(self, x):
                called.append("predict")
                return x

        pipe = Pipeline.build(API(), [Validator()])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_route_meta())
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
        resp_bytes, *_ = await pipe.run_single(b"{}", _make_route_meta())
        assert called == []
        assert _body(resp_bytes) == {"direct": True}

    @pytest.mark.asyncio
    async def test_decode_returning_response_short_circuits(self):
        class API(EchoAPI):
            def decode_request(self, request):
                return Response(content={"early": True}, headers={"X-E": "1"})

            def predict(self, x):
                raise AssertionError("predict must not run")

        pipe = Pipeline.build(API(), [])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_route_meta())
        assert _body(resp_bytes) == {"early": True}
        assert headers == {"X-E": "1"}

    @pytest.mark.asyncio
    async def test_hooks_after_early_return_are_skipped(self):
        order = []

        class First(Callback):
            def before_decode_request(self, ctx):
                order.append("first")
                ctx.respond({"stop": True})

        class Second(Callback):
            def before_decode_request(self, ctx):
                order.append("second")

            def after_predict(self, ctx):
                order.append("second.output")

        pipe = Pipeline.build(EchoAPI(), [First(), Second()])
        resp_bytes, *_ = await pipe.run_single(b"{}", _make_route_meta())
        assert order == ["first"]
        assert _body(resp_bytes) == {"stop": True}


# ---------------------------------------------------------------------------
# Error propagation (validation semantics)
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    @pytest.mark.asyncio
    async def test_http_exception_from_hook_propagates(self):
        class Auth(Callback):
            def before_decode_request(self, ctx):
                raise BadRequestError("bad input", param="x")

        pipe = Pipeline.build(EchoAPI(), [Auth()])
        with pytest.raises(HTTPException) as exc_info:
            await pipe.run_single(b"{}", _make_route_meta())
        assert exc_info.value.status_code == 400
        assert exc_info.value.param == "x"

    @pytest.mark.asyncio
    async def test_generic_exception_from_hook_propagates(self):
        """Data-hook exceptions are NOT swallowed (pre-0.7 they were)."""

        class Boom(Callback):
            def after_predict(self, ctx):
                raise ValueError("nope")

        pipe = Pipeline.build(EchoAPI(), [Boom()])
        with pytest.raises(ValueError):
            await pipe.run_single(b"{}", _make_route_meta())

    @pytest.mark.asyncio
    async def test_http_exception_from_hook_propagates(self):
        class Reject(Callback):
            def before_decode_request(self, ctx):
                raise BadRequestError("rejected")

        pipe = Pipeline.build(EchoAPI(), [Reject()])
        with pytest.raises(HTTPException):
            await pipe.run_single(b"{}", _make_route_meta())


# ---------------------------------------------------------------------------
# ctx.state and value replacement
# ---------------------------------------------------------------------------


class TestContextState:
    @pytest.mark.asyncio
    async def test_state_shared_between_hooks(self):
        seen = {}

        class Tracer(Callback):
            def before_decode_request(self, ctx):
                ctx.state["t0"] = 123

            def after_predict(self, ctx):
                seen["t0"] = ctx.state["t0"]

        pipe = Pipeline.build(EchoAPI(), [Tracer()])
        await pipe.run_single(b"{}", _make_route_meta())
        assert seen["t0"] == 123

    @pytest.mark.asyncio
    async def test_hook_return_replaces_value(self):
        class Rewrite(Callback):
            def before_decode_request(self, ctx):
                return {"replaced": True}

        seen = {}

        class API(EchoAPI):
            def decode_request(self, request):
                seen["request"] = request
                return request

        pipe = Pipeline.build(API(), [Rewrite()])
        await pipe.run_single(b"{}", _make_route_meta())
        assert seen["request"] == {"replaced": True}

    @pytest.mark.asyncio
    async def test_hook_inplace_mutation(self):
        class Mutate(Callback):
            def after_decode_request(self, ctx):
                ctx.input["extra"] = 1

        seen = {}

        class API(EchoAPI):
            def predict(self, x):
                seen["input"] = x
                return x

        pipe = Pipeline.build(API(), [Mutate()])
        await pipe.run_single(json.dumps({"a": 1}).encode(), _make_route_meta())
        assert seen["input"] == {"a": 1, "extra": 1}

    @pytest.mark.asyncio
    async def test_state_is_per_request(self):
        class Counter(Callback):
            def before_decode_request(self, ctx):
                ctx.state["n"] = ctx.request.get("n")

        captured = []

        class API(EchoAPI):
            def predict(self, x):
                return x

        pipe = Pipeline.build(API(), [Counter()])
        ctx1 = RequestContext(meta=_make_route_meta(), request={"n": 1})
        ctx2 = RequestContext(meta=_make_route_meta(), request={"n": 2})
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
        assert pipe._gen_executor is None
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_route_meta())
        assert _body(resp_bytes) == {"echo": {"x": 1}}

    @pytest.mark.asyncio
    async def test_async_predict_runs_natively(self):
        class AsyncAPI(EchoAPI):
            async def predict(self, x):
                return {"async": x}

        pipe = Pipeline.build(AsyncAPI(), [])
        assert pipe.any_async is True
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_route_meta())
        assert _body(resp_bytes) == {"async": {"x": 1}}

    @pytest.mark.asyncio
    async def test_sync_stage_runs_inline_on_loop_in_mixed_mode(self):
        idents = []

        class Mixed(EchoAPI):
            async def decode_request(self, request):
                return request

            def predict(self, x):
                idents.append(threading.get_ident())
                return x

            def encode_response(self, output):
                idents.append(threading.get_ident())
                return output

        pipe = Pipeline.build(Mixed(), [])
        assert pipe.any_async is True
        loop_ident = threading.get_ident()
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_route_meta())
        assert _body(resp_bytes) == {"x": 1}
        # Sync stages run inline on the event loop thread (0.6.x semantics),
        # not on a dispatched executor thread.
        assert idents == [loop_ident, loop_ident]
        pipe.close()

    @pytest.mark.asyncio
    async def test_run_blocking_uses_dedicated_thread_in_mixed_mode(self):
        """Sync-generator consumption must stay off the loop thread even
        though sync stages now run inline."""

        class Mixed(EchoAPI):
            async def decode_request(self, request):
                return request

        pipe = Pipeline.build(Mixed(), [])
        loop_ident = threading.get_ident()
        seen = []

        def work():
            seen.append(threading.get_ident())
            return 42

        result = await pipe.run_blocking(work)
        assert result == 42
        assert seen[0] != loop_ident
        pipe.close()

    @pytest.mark.asyncio
    async def test_run_blocking_uses_thread_even_in_all_sync_mode(self):
        """All-sync pipelines have no stage executor; generator consumption
        must still run on a dedicated thread (never inline on the loop)."""
        pipe = Pipeline.build(EchoAPI(), [])
        assert pipe.any_async is False
        loop_ident = threading.get_ident()
        seen = []

        def work():
            seen.append(threading.get_ident())
            return 42

        result = await pipe.run_blocking(work)
        assert result == 42
        assert seen[0] != loop_ident
        pipe.close()

    @pytest.mark.asyncio
    async def test_async_callback_hook(self):
        order = []

        class AsyncCB(Callback):
            async def before_decode_request(self, ctx):
                order.append("async.before_decode_request")

        pipe = Pipeline.build(EchoAPI(), [AsyncCB()])
        assert pipe.any_async is True
        await pipe.run_single(b"{}", _make_route_meta())
        assert order == ["async.before_decode_request"]
        pipe.close()

    @pytest.mark.asyncio
    async def test_async_on_error_callback_sets_any_async(self):
        """C6: an async on_error callback must count toward async detection
        on the inference pipeline, so the single-thread executor exists when
        it is driven."""
        class AsyncErrCB(Callback):
            async def on_error(self, ctx, exc):
                pass

        pipe = Pipeline.build(EchoAPI(), [AsyncErrCB()])
        assert pipe.any_async is True
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
            *[pipe.run_single(b'{"x": 1}', _make_route_meta()) for _ in range(5)]
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
        ctxs = [RequestContext(meta=_make_meta()) for _ in range(3)]
        with pytest.raises(ValueError, match="unbatch"):
            await pipe.batch_predict([1, 2, 3], ctxs)


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------


class TestLifecycleHooks:
    def test_setup_and_teardown_hooks_fire_in_order(self):
        order = []

        class LC(Callback):
            def before_setup(self, config, device):
                order.append(("before", device))

            def after_setup(self, lit_api):
                order.append(("after", type(lit_api).__name__))

            def before_teardown(self, lit_api):
                order.append(("before_teardown",))

            def after_teardown(self, lit_api):
                order.append(("after_teardown",))

        api = EchoAPI()
        pipe = Pipeline.build(api, [LC()])
        pipe.trigger_lifecycle("before_setup", {"k": 1}, "cpu")
        api.setup("cpu")
        pipe.trigger_lifecycle("after_setup", api)
        pipe.trigger_lifecycle("before_teardown", api)
        pipe.trigger_lifecycle("after_teardown", api)
        assert order == [
            ("before", "cpu"),
            ("after", "EchoAPI"),
            ("before_teardown",),
            ("after_teardown",),
        ]

    def test_lifecycle_exceptions_are_isolated(self):
        class Bad(Callback):
            def before_teardown(self, lit_api):
                raise RuntimeError("boom")

        class Good(Callback):
            def __init__(self):
                self.called = False

            def before_teardown(self, lit_api):
                self.called = True

        good = Good()
        pipe = Pipeline.build(EchoAPI(), [Bad(), good])
        pipe.trigger_lifecycle("before_teardown", EchoAPI())  # must not raise
        assert good.called is True

    def test_async_lifecycle_hook_is_driven(self):
        called = []

        class AsyncLC(Callback):
            async def after_setup(self, lit_api):
                called.append("async setup")

        pipe = Pipeline.build(EchoAPI(), [AsyncLC()])
        pipe.trigger_lifecycle("after_setup", EchoAPI())
        assert called == ["async setup"]

    def test_multiple_async_lifecycle_hooks_all_driven(self):
        """F5: all async lifecycle hooks are drained in a single event loop."""
        called = []

        class LC1(Callback):
            async def before_teardown(self, lit_api):
                called.append("lc1")

        class LC2(Callback):
            async def before_teardown(self, lit_api):
                called.append("lc2")

        pipe = Pipeline.build(EchoAPI(), [LC1(), LC2()])
        pipe.trigger_lifecycle("before_teardown", EchoAPI())
        assert called == ["lc1", "lc2"]

    def test_async_lifecycle_exception_isolation(self):
        """F5: an exception in one async hook does not prevent others."""
        called = []

        class BadLC(Callback):
            async def before_teardown(self, lit_api):
                raise RuntimeError("boom")

        class GoodLC(Callback):
            async def before_teardown(self, lit_api):
                called.append("good")

        pipe = Pipeline.build(EchoAPI(), [BadLC(), GoodLC()])
        # Must not raise
        pipe.trigger_lifecycle("before_teardown", EchoAPI())
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
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_route_meta())
        sc, mt, clean = extract_response_meta(headers)
        assert sc == 201
        assert mt == "text/plain"
        assert clean is None
        # P3: str 直发(配合 media_type=text/plain),不再 JSON 加引号
        assert resp_bytes == b"plain"

    def test_unwrap_plain_value(self):
        assert unwrap_response({"a": 1}) == ({"a": 1}, None)

    def test_unwrap_response_with_headers(self):
        r = Response(content={"b": 2}, headers={"X-1": "v"})
        assert unwrap_response(r) == ({"b": 2}, {"X-1": "v"})

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


# ---------------------------------------------------------------------------
# Ctx injection: all data hooks receive the same RequestContext (0.7.0)
# ---------------------------------------------------------------------------


class TestHookCtx:
    @pytest.mark.asyncio
    async def test_all_hooks_share_same_ctx(self):
        seen = []

        class CB1(Callback):
            def before_decode_request(self, ctx):
                seen.append(("b1", ctx))

            def after_encode_response(self, ctx):
                seen.append(("a1", ctx))

        class CB2(Callback):
            def before_decode_request(self, ctx):
                seen.append(("b2", ctx))

        pipe = Pipeline.build(EchoAPI(), [CB1(), CB2()])
        await pipe.run_single(b"{}", _make_route_meta())
        assert [s[0] for s in seen] == ["b1", "b2", "a1"]
        # All hooks see the same ctx object
        assert seen[0][1] is seen[1][1] is seen[2][1]

    @pytest.mark.asyncio
    async def test_hook_early_return_via_respond(self):
        """before_decode_request returns ctx.respond(...) — pipeline short-circuits."""
        called = []

        class Reject(Callback):
            def before_decode_request(self, ctx):
                return ctx.respond({"denied": True}, status_code=401)

        class API(EchoAPI):
            def decode_request(self, request):
                called.append("decode")
                return request

            def predict(self, x):
                called.append("predict")
                return x

        pipe = Pipeline.build(API(), [Reject()])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_route_meta())
        assert called == []
        assert _body(resp_bytes) == {"denied": True}
        sc, mt, clean = extract_response_meta(headers)
        assert sc == 401

    def test_litapi_hook_raises_at_build(self):
        """0.8.0 removed request hooks from LitAPI — defining one is a loud error
        (a silent no-op would mean auth/validation that never runs)."""
        for name in ("before_decode_request", "after_encode_response"):
            class BadAPI(EchoAPI):
                pass
            setattr(BadAPI, name, lambda self, ctx: ctx)
            with pytest.raises(RuntimeError, match="removed from LitAPI"):
                Pipeline.build(BadAPI(), [])

    def test_old_name_litapi_hook_raises_at_build(self):
        """0.7-era LitAPI hook names are caught by the same guard."""
        class OldAPI(EchoAPI):
            def on_request(self, ctx):
                return ctx.request

        with pytest.raises(RuntimeError, match="removed from LitAPI"):
            Pipeline.build(OldAPI(), [])

    def test_no_callbacks_chains_empty(self):
        pipe = Pipeline.build(EchoAPI(), [])
        assert pipe._chains["before_decode_request"] == []
        assert pipe._chains["after_encode_response"] == []


# ---------------------------------------------------------------------------
# Ctx injection: decode / predict / encode
# ---------------------------------------------------------------------------


class TestCtxInjection:
    """Declaring a parameter named 'ctx' on decode_request / predict /
    encode_response opts into receiving the RequestContext."""

    @pytest.mark.asyncio
    async def test_decode_request_ctx_injection(self):
        captured = []

        class API(EchoAPI):
            def decode_request(self, request, ctx: RequestContext | None = None):
                captured.append(ctx)
                return {"prompt": request.get("q"), "uid": ctx.meta.request_id}

            def predict(self, x):
                return x

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"q": "hello"}', _make_route_meta())
        assert len(captured) == 1
        assert captured[0].meta.request_id == "req-1"
        assert _body(resp_bytes) == {"prompt": "hello", "uid": "req-1"}

    @pytest.mark.asyncio
    async def test_encode_response_ctx_injection(self):
        captured = []

        class API(EchoAPI):
            def encode_response(self, output, ctx: RequestContext | None = None):
                captured.append(ctx.meta.route)
                return {"result": output, "route": ctx.meta.route}

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_route_meta())
        assert captured == ["/predict"]
        assert _body(resp_bytes)["route"] == "/predict"

    @pytest.mark.asyncio
    async def test_predict_ctx_injection_single(self):
        captured = []

        class API(EchoAPI):
            def predict(self, x, ctx: RequestContext | None = None):
                captured.append(ctx.meta.request_id)
                return {"echo": x, "req": ctx.meta.request_id}

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_route_meta())
        assert captured == ["req-1"]
        assert _body(resp_bytes)["req"] == "req-1"

    @pytest.mark.asyncio
    async def test_keyword_only_ctx_injection(self):
        """Keyword-only 'ctx' parameter (e.g. `*, ctx`) is supported."""

        class API(EchoAPI):
            def decode_request(self, request, *, ctx: RequestContext | None = None):
                ctx.state["from_decode"] = True
                return request

            def predict(self, x, *, ctx: RequestContext | None = None):
                assert ctx.state["from_decode"] is True
                return x

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_route_meta())
        assert _body(resp_bytes) == {"x": 1}

    @pytest.mark.asyncio
    async def test_state_threaded_before_decode_to_decode(self):
        class Tracer(Callback):
            def before_decode_request(self, ctx):
                ctx.state["user"] = "alice"
                return ctx.request

        class API(EchoAPI):
            def decode_request(self, request, ctx: RequestContext | None = None):
                return {"user": ctx.state["user"], **request}

            def predict(self, x):
                return x

        pipe = Pipeline.build(API(), [Tracer()])
        resp_bytes, *_ = await pipe.run_single(b'{"q": "hi"}', _make_route_meta())
        assert _body(resp_bytes)["user"] == "alice"

    @pytest.mark.asyncio
    async def test_after_encode_respond_attaches_headers(self):
        """after_encode_response can use ctx.respond() to attach custom headers —
        replaces the old ResponseWithHeaders pattern."""

        class HeaderCB(Callback):
            def after_encode_response(self, ctx):
                return ctx.respond(
                    ctx.response,
                    headers={"X-Request-ID": ctx.meta.request_id, "X-Cache": "HIT"},
                )

        class API(EchoAPI):
            def predict(self, x):
                return {"result": x.get("val")}

        pipe = Pipeline.build(API(), [HeaderCB()])
        resp_bytes, status, metrics, headers = await pipe.run_single(
            b'{"val": 42}', _make_route_meta()
        )
        assert _body(resp_bytes) == {"result": 42}
        assert headers == {"X-Request-ID": "req-1", "X-Cache": "HIT"}


# ---------------------------------------------------------------------------
# Ctx injection: forbidden methods (step only)
# ---------------------------------------------------------------------------


class TestCtxForbidden:
    """Declaring 'ctx' on step is a load-time error — it operates across
    sequences and has no single per-request context.

    batch / unbatch / predict may declare ctx: in batch mode the framework
    injects a ``list[RequestContext]`` aligned with the inputs (see
    :class:`TestBatchCtxInjection`).  step is different — its
    ``active_sequences`` already carry per-sequence ctx via CBSequence."""

    @pytest.mark.parametrize("method_name", ["step"])
    def test_ctx_forbidden_methods_fail_at_build(self, method_name):
        # Override the forbidden method with a ctx param.
        overrides = {
            method_name: lambda self, x, ctx: x,
        }
        # step needs prefill + has_finished to be a valid CB model.
        if method_name == "step":
            overrides["prefill"] = lambda self, uid, inp: None
            overrides["has_finished"] = lambda self, uid, tok, seq: True

        BadAPI = type("BadAPI", (EchoAPI,), overrides)
        with pytest.raises(RuntimeError, match=method_name):
            Pipeline.build(BadAPI(), [])

    def test_positional_only_ctx_fails_at_build(self):
        """A positional-only 'ctx' parameter (/) cannot be injected — the
        wrapper passes ctx as a keyword, so this must fail at load time."""

        # Create a callable whose ctx parameter is positional-only.
        # We use exec to define a function with positional-only ctx at runtime.
        namespace: dict = {}
        exec(
            "def _pos_only_fn(self, x, ctx, /):\n    return x\n",
            namespace,
        )
        pos_only_fn = namespace["_pos_only_fn"]

        # Monkey-patch decode_request on an instance
        api = EchoAPI()
        api.decode_request = pos_only_fn.__get__(api, type(api))
        with pytest.raises(RuntimeError, match="positional-only"):
            Pipeline.build(api, [])


# ---------------------------------------------------------------------------
# Ctx injection: batch / unbatch / predict (list[RequestContext])
# ---------------------------------------------------------------------------


class TestBatchCtxInjection:
    """In batch mode, declaring ``ctx`` on batch / unbatch / predict
    receives a ``list[RequestContext]`` aligned positionally with the
    inputs/outputs — no need to thread per-request data through the
    decoded input."""

    @staticmethod
    def _ctx(rid: str) -> RequestContext:
        return RequestContext(
            meta=RequestMeta(
                route="/predict",
                headers=Headers({}),
                client_ip="127.0.0.1",
                request_id=rid,
                timestamp_ns=1,
            )
        )

    @pytest.mark.asyncio
    async def test_batch_unbatch_predict_receive_aligned_ctx_list(self):
        seen: dict[str, list[str]] = {}

        class M(LitAPI):
            def batch(self, inputs, ctx):
                seen["batch"] = [c.meta.request_id for c in ctx]
                assert len(ctx) == len(inputs)
                return inputs

            def predict(self, batched, ctx):
                seen["predict"] = [c.meta.request_id for c in ctx]
                # Echo each request_id to prove end-to-end alignment.
                return [c.meta.request_id for c in ctx]

            def unbatch(self, output, ctx):
                seen["unbatch"] = [c.meta.request_id for c in ctx]
                return list(output)

        pipe = Pipeline.build(M(max_batch_size=2), [])
        ctxs = [self._ctx("a"), self._ctx("b"), self._ctx("c")]
        outputs = await pipe.batch_predict([1, 2, 3], ctxs)
        assert seen["batch"] == ["a", "b", "c"]
        assert seen["predict"] == ["a", "b", "c"]
        assert seen["unbatch"] == ["a", "b", "c"]
        assert outputs == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_methods_without_ctx_param_ignore_injected_list(self):
        """Backward compat: not declaring ctx behaves exactly as before."""

        class M(LitAPI):
            def batch(self, inputs):
                return inputs

            def predict(self, x):
                return [v * 10 for v in x]

            def unbatch(self, output):
                return list(output)

        pipe = Pipeline.build(M(max_batch_size=2), [])
        ctxs = [self._ctx("a"), self._ctx("b")]
        outputs = await pipe.batch_predict([1, 2], ctxs)
        assert outputs == [10, 20]

    @pytest.mark.asyncio
    async def test_predict_in_batch_mode_may_mutate_per_item_state(self):
        """ctx list items are the same objects written back to ctx_map:
        state set in predict stays visible per item."""

        class M(LitAPI):
            def batch(self, inputs, ctx):
                return inputs

            def predict(self, batched, ctx):
                for i, c in enumerate(ctx):
                    c.state["tag"] = f"t{i}"
                return list(batched)

            def unbatch(self, output, ctx):
                return list(output)

        pipe = Pipeline.build(M(max_batch_size=2), [])
        ctxs = [self._ctx("a"), self._ctx("b")]
        await pipe.batch_predict([1, 2], ctxs)
        assert ctxs[0].state["tag"] == "t0"
        assert ctxs[1].state["tag"] == "t1"


def _make_route_meta(**kwargs):
    defaults = dict(
        route="/predict",
        headers=Headers({}),
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=1,
        method="POST",
    )
    defaults.update(kwargs)
    return RequestMeta(**defaults)


# ---------------------------------------------------------------------------
# for_route
# ---------------------------------------------------------------------------


class TestForRoute:
    def test_rejects_after_decode_request_hook(self):
        class BadCB(Callback):
            def after_decode_request(self, ctx):
                pass

        with pytest.raises(RuntimeError, match="after_decode_request"):
            Pipeline.for_route([BadCB()])

    def test_rejects_after_predict_hook(self):
        class BadCB(Callback):
            def after_predict(self, ctx):
                pass

        with pytest.raises(RuntimeError, match="after_predict"):
            Pipeline.for_route([BadCB()])

    def test_accepts_before_decode_and_after_encode_hooks(self):
        class GoodCB(Callback):
            def before_decode_request(self, ctx):
                pass

            def after_encode_response(self, ctx):
                pass

        pipe = Pipeline.for_route([GoodCB()])
        assert pipe.lit_api is None
        assert len(pipe.callbacks) == 1

    def test_accepts_multiple_callbacks(self):
        pipe = Pipeline.for_route([Callback(), Callback()])
        assert len(pipe.callbacks) == 2


# ---------------------------------------------------------------------------
# run_route
# ---------------------------------------------------------------------------


class DummyLitAPI(LitAPI):
    def predict(self, x):
        return x


# ---------------------------------------------------------------------------
# JsonSchemaValidator hook placement: before_decode_request (input_schema) / after_encode_response
# (output_schema) — schema validation runs on the wire payload, before any
# model code on the input side and after encode on the output side.
# ---------------------------------------------------------------------------


class TestJsonSchemaValidatorHooks:
    @pytest.mark.asyncio
    async def test_input_rejected_before_decode_and_predict(self):
        calls = []

        class Guarded(EchoAPI):
            def decode_request(self, request):
                calls.append("decode")
                return request

            def predict(self, x):
                calls.append("predict")
                return x

        v = JsonSchemaValidator(input_schema={
            "type": "object",
            "required": ["sessionId"],
            "properties": {"sessionId": {"type": "string", "minLength": 1}},
        })
        pipe = Pipeline.build(Guarded(), [v])
        with pytest.raises(BadRequestError) as ei:
            await pipe.run_single(json.dumps({"unitName": "x"}).encode(), _make_meta())
        assert ei.value.status_code == 400
        assert calls == []  # decode_request 与 predict 都未运行

    @pytest.mark.asyncio
    async def test_output_rejected_after_encode_response(self):
        calls = []

        class Model(EchoAPI):
            def predict(self, x):
                return {"wrong": 1}

            def encode_response(self, output):
                calls.append("encode")
                return output

        v = JsonSchemaValidator(output_schema={
            "type": "object",
            "required": ["text"],
        })
        pipe = Pipeline.build(Model(), [v])
        with pytest.raises(BadRequestError):
            await pipe.run_single(json.dumps({"x": 1}).encode(), _make_meta())
        assert calls == ["encode"]  # after_encode_response 在 encode_response 之后校验

    def test_for_route_accepts_json_schema_validator(self):
        v = JsonSchemaValidator(input_schema={
            "type": "object",
            "required": ["sessionId"],
        })
        pipe = Pipeline.for_route([v])
        assert len(pipe.callbacks) == 1

    @pytest.mark.asyncio
    async def test_route_input_rejected_before_handler(self):
        v = JsonSchemaValidator(input_schema={
            "type": "object",
            "required": ["sessionId"],
        })
        pipe = Pipeline.for_route([v])
        ctx = RequestContext(meta=_make_route_meta(), request={"unitName": "x"})
        called = False

        async def handler(ctx_arg):
            nonlocal called
            called = True

        with pytest.raises(BadRequestError):
            await pipe.run_route(ctx, handler)
        assert not called


class TestRunRoute:
    @pytest.mark.asyncio
    async def test_handler_called_with_ctx(self):
        pipe = Pipeline.for_route([])
        ctx = RequestContext(meta=_make_route_meta(), request={"k": "v"})
        called = []

        async def handler(ctx_arg):
            called.append(ctx_arg)

        await pipe.run_route(ctx, handler)
        assert len(called) == 1
        assert called[0] is ctx

    @pytest.mark.asyncio
    async def test_sync_handler_runs_inline_on_loop(self):
        """Sync route handlers run inline on the loop thread (no executor),
        even when an async callback makes the pipeline mixed-mode."""

        class AsyncCB(Callback):
            async def before_decode_request(self, ctx):
                pass

        pipe = Pipeline.for_route([AsyncCB()])
        assert pipe.any_async is True
        ctx = RequestContext(meta=_make_route_meta())
        loop_ident = threading.get_ident()
        seen = []

        def handler(ctx_arg):
            seen.append(threading.get_ident())
            return {"ok": True}

        await pipe.run_route(ctx, handler)
        assert seen == [loop_ident]

    @pytest.mark.asyncio
    async def test_before_decode_early_return_skips_handler(self):
        class EarlyCB(Callback):
            def before_decode_request(self, ctx):
                ctx.respond({"blocked": True}, status_code=403)

        pipe = Pipeline.for_route([EarlyCB()])
        ctx = RequestContext(meta=_make_route_meta())
        called = False

        async def handler(ctx_arg):
            nonlocal called
            called = True

        await pipe.run_route(ctx, handler)
        assert not called
        assert ctx.early is not None
        assert ctx.early.status_code == 403

    @pytest.mark.asyncio
    async def test_handler_result_stored_as_response(self):
        pipe = Pipeline.for_route([])
        ctx = RequestContext(meta=_make_route_meta())

        async def handler(ctx_arg):
            return {"result": "ok"}

        await pipe.run_route(ctx, handler)
        assert ctx.response == {"result": "ok"}

    @pytest.mark.asyncio
    async def test_after_encode_response_after_handler(self):
        responses = []

        class TrackCB(Callback):
            def after_encode_response(self, ctx):
                responses.append(ctx.response)

        pipe = Pipeline.for_route([TrackCB()])
        ctx = RequestContext(meta=_make_route_meta())

        async def handler(ctx_arg):
            return {"x": 1}

        await pipe.run_route(ctx, handler)
        assert responses == [{"x": 1}]

    @pytest.mark.asyncio
    async def test_on_error_driven_on_httpexception(self):
        errors = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                errors.append(exc)

        pipe = Pipeline.for_route([ErrCB()])
        ctx = RequestContext(meta=_make_route_meta())

        async def handler(ctx_arg):
            raise HTTPException(400, "bad")

        with pytest.raises(HTTPException):
            await pipe.run_route(ctx, handler)
        assert len(errors) == 1
        assert isinstance(errors[0], HTTPException)

    @pytest.mark.asyncio
    async def test_on_error_does_not_mask_original(self):
        class ExplodingCB(Callback):
            def on_error(self, ctx, exc):
                raise RuntimeError("boom")

        pipe = Pipeline.for_route([ExplodingCB()])
        ctx = RequestContext(meta=_make_route_meta())

        async def handler(ctx_arg):
            raise HTTPException(400, "bad")

        with pytest.raises(HTTPException):  # original, not RuntimeError
            await pipe.run_route(ctx, handler)

    @pytest.mark.asyncio
    async def test_on_error_threads_response_headers(self):
        """run_route threads ctx.response_headers → e._response_headers after
        on_error, so worker-layer handlers can merge them into the error
        response (parity with run_single)."""
        class HeadersCB(Callback):
            def on_error(self, ctx, exc):
                ctx.response_headers["X-On-Error"] = "caught"
                ctx.response_headers["X-Exc-Type"] = type(exc).__name__

        pipe = Pipeline.for_route([HeadersCB()])
        ctx = RequestContext(meta=_make_route_meta())

        async def handler(ctx_arg):
            raise RuntimeError("route boom")

        with pytest.raises(RuntimeError) as exc_info:
            await pipe.run_route(ctx, handler)

        hdrs = getattr(exc_info.value, "_response_headers", None)
        assert hdrs is not None, "run_route must thread _response_headers"
        assert hdrs["X-On-Error"] == "caught"
        assert hdrs["X-Exc-Type"] == "RuntimeError"

    @pytest.mark.asyncio
    async def test_sync_handler_in_async_pipeline(self):
        """Sync handler runs on executor when any callback is async."""
        class AsyncCB(Callback):
            async def before_decode_request(self, ctx):
                pass

        pipe = Pipeline.for_route([AsyncCB()])
        ctx = RequestContext(meta=_make_route_meta())

        def sync_handler(ctx_arg):
            return {"sync": True}

        await pipe.run_route(ctx, handler=sync_handler)
        assert ctx.response == {"sync": True}

    @pytest.mark.asyncio
    async def test_sync_handler_returning_coroutine_is_awaited(self):
        """B7: a sync callable that returns a coroutine (e.g. an object with
        async __call__, invisible to iscoroutinefunction) runs through
        run_blocking and must be awaited — not stored raw on ctx.response.
        Mirrors the runtime guard already present in _adapt."""
        import asyncio

        class AsyncCB(Callback):
            async def before_decode_request(self, ctx):
                pass

        pipe = Pipeline.for_route([AsyncCB()])
        assert pipe.any_async is True
        ctx = RequestContext(meta=_make_route_meta())

        class CallableObj:
            async def __call__(self, ctx_arg):
                return {"answered": True}

        await pipe.run_route(ctx, handler=CallableObj())
        assert ctx.response == {"answered": True}
        assert not asyncio.iscoroutine(ctx.response)


# ---------------------------------------------------------------------------
# run_single + on_error
# ---------------------------------------------------------------------------


class TestRunSingleOnError:
    @pytest.mark.asyncio
    async def test_hook_exception_drives_on_error(self):
        errors = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                errors.append(exc)

        class FailingCB(Callback):
            def before_decode_request(self, ctx):
                raise BadRequestError("bad input")

        api = DummyLitAPI()
        pipe = Pipeline.build(api, [ErrCB(), FailingCB()])
        meta = _make_route_meta()

        with pytest.raises(HTTPException):
            await pipe.run_single(b'{"x": 1}', meta)
        assert len(errors) == 1
        assert isinstance(errors[0], BadRequestError)

    @pytest.mark.asyncio
    async def test_predict_exception_drives_on_error(self):
        errors = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                errors.append(exc)

        class FailingAPI(LitAPI):
            def predict(self, x):
                raise RuntimeError("predict failed")

        api = FailingAPI()
        pipe = Pipeline.build(api, [ErrCB()])
        meta = _make_route_meta()

        with pytest.raises(RuntimeError):
            await pipe.run_single(b'{"x": 1}', meta)
        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_on_error_threads_response_headers_for_plain_exception(self):
        """run_single threads ctx.response_headers → e._response_headers for
        plain exceptions (ZeroDivisionError etc.), not just HTTPException."""
        class HeadersCB(Callback):
            def on_error(self, ctx, exc):
                ctx.response_headers["X-On-Error"] = "caught"
                ctx.response_headers["X-Exc-Type"] = type(exc).__name__

        class FailingAPI(LitAPI):
            def predict(self, x):
                raise RuntimeError("predict crash")

        api = FailingAPI()
        pipe = Pipeline.build(api, [HeadersCB()])
        meta = _make_route_meta()

        with pytest.raises(RuntimeError) as exc_info:
            await pipe.run_single(b'{"x": 1}', meta)

        hdrs = getattr(exc_info.value, "_response_headers", None)
        assert hdrs is not None, "run_single must thread _response_headers"
        assert hdrs["X-On-Error"] == "caught"
        assert hdrs["X-Exc-Type"] == "RuntimeError"


# ---------------------------------------------------------------------------
# finalize + response_headers
# ---------------------------------------------------------------------------


class TestFinalizeResponseHeaders:
    def test_response_headers_merged(self):
        ctx = RequestContext(meta=_make_route_meta(), response={"result": "ok"})
        ctx.response_headers["X-Custom"] = "val1"
        ctx.response_headers["X-Shared"] = "from_ctx"

        api = DummyLitAPI()
        pipe = Pipeline.build(api, [])
        resp_bytes, status, metrics, headers = pipe.finalize(ctx)

        assert headers is not None
        assert headers["X-Custom"] == "val1"
        assert headers["X-Shared"] == "from_ctx"

    def test_response_explicit_header_overrides_response_headers(self):
        from lite_server.response import Response as LiteResponse

        ctx = RequestContext(meta=_make_route_meta())
        ctx.response_headers["X-Custom"] = "from_ctx"
        ctx.response = LiteResponse(
            content={"ok": True}, headers={"X-Custom": "from_resp", "X-New": "new_val"}
        )

        api = DummyLitAPI()
        pipe = Pipeline.build(api, [])
        resp_bytes, status, metrics, headers = pipe.finalize(ctx)

        assert headers is not None
        assert headers["X-Custom"] == "from_resp"  # explicit wins
        assert headers["X-New"] == "new_val"

    def test_response_headers_preserved_with_early(self):
        from lite_server.response import Response as LiteResponse

        ctx = RequestContext(meta=_make_route_meta())
        ctx.response_headers["X-Ctx"] = "ctx_val"
        ctx.early = LiteResponse(content={"early": True}, headers={"X-Early": "early_val"})

        api = DummyLitAPI()
        pipe = Pipeline.build(api, [])
        resp_bytes, status, metrics, headers = pipe.finalize(ctx)

        assert headers is not None
        assert headers["X-Ctx"] == "ctx_val"
        assert headers["X-Early"] == "early_val"


# ---------------------------------------------------------------------------
# finalize bytes/str passthrough (P3)
# ---------------------------------------------------------------------------


class TestFinalizeBytesPassthrough:
    """encode_response 返回 bytes/str 时不经 JSON 序列化(P3):bytes 直发、
    str 直接 utf-8 encode,结构化数据才走 _json.dumps。"""

    def test_bytes_returned_verbatim(self):
        ctx = RequestContext(meta=_make_route_meta(), response=b"\x00\x01\x02")
        pipe = Pipeline.build(DummyLitAPI(), [])
        resp_bytes, _, _, _ = pipe.finalize(ctx)
        assert resp_bytes == b"\x00\x01\x02"

    def test_bytearray_returned_as_bytes(self):
        ctx = RequestContext(meta=_make_route_meta(), response=bytearray(b"abc"))
        pipe = Pipeline.build(DummyLitAPI(), [])
        resp_bytes, _, _, _ = pipe.finalize(ctx)
        assert resp_bytes == b"abc"
        assert isinstance(resp_bytes, bytes)

    def test_str_encoded_without_json_quoting(self):
        # 模型返回已序列化的 JSON 字符串时不被二次编码。
        ctx = RequestContext(meta=_make_route_meta(), response='{"a":1}')
        pipe = Pipeline.build(DummyLitAPI(), [])
        resp_bytes, _, _, _ = pipe.finalize(ctx)
        assert resp_bytes == b'{"a":1}'

    def test_dict_still_json_serialized(self):
        ctx = RequestContext(meta=_make_route_meta(), response={"a": 1})
        pipe = Pipeline.build(DummyLitAPI(), [])
        resp_bytes, _, _, _ = pipe.finalize(ctx)
        assert resp_bytes == b'{"a":1}'

    def test_json_dumps_quotes_str_unlike_finalize_passthrough(self):
        """Bx: _json.dumps wraps str in JSON quotes; Pipeline.finalize passes
        through verbatim (P3).  The batch handler (_store_result) and all five
        streaming paths call _json.dumps() without the bytes/str check, so a
        model whose encode_response returns str gets different wire formats in
        single mode (raw) vs batch/streaming modes (JSON-quoted)."""
        from lite_server import _json

        body_str = "hello world"
        body_bytes = b"raw bytes"

        # Pipeline.finalize passthrough (correct after P3):
        ctx = RequestContext(meta=_make_route_meta(), response=body_str)
        pipe = Pipeline.build(DummyLitAPI(), [])
        resp_bytes, _, _, _ = pipe.finalize(ctx)
        assert resp_bytes == b"hello world", "str must pass through verbatim"

        ctx2 = RequestContext(meta=_make_route_meta(), response=body_bytes)
        resp_bytes2, _, _, _ = pipe.finalize(ctx2)
        assert resp_bytes2 == b"raw bytes", "bytes must pass through verbatim"

        # _json.dumps (used by batch _store_result + streaming paths):
        # wraps str in JSON quotes
        assert _json.dumps(body_str) == b'"hello world"', (
            "_json.dumps wraps str in quotes; batch/streaming paths using "
            "_json.dumps without passthrough would emit JSON-quoted str"
        )
        # bytes → TypeError (orjson) or base64 (stdlib fallback); in either
        # case NOT raw passthrough — inconsistent with Pipeline.finalize
        throws = False
        try:
            result = _json.dumps(body_bytes)
            # stdlib fallback: base64-encodes bytes, so result != raw bytes
            assert result != body_bytes, (
                f"_json.dumps returned {result!r} for bytes; "
                f"batch/streaming paths would emit this, not raw bytes"
            )
        except TypeError:
            throws = True
        assert throws or result != body_bytes, (
            "_json.dumps(bytes) must not return raw bytes; "
            "batch/streaming paths diverge from Pipeline.finalize passthrough"
        )


# ---------------------------------------------------------------------------
# on_error custom response (2026-08-04)
# ---------------------------------------------------------------------------

class TestOnErrorCustomResponse:
    """on_error hook may return a Response to override the default error response."""

    @pytest.mark.asyncio
    async def test_unary_on_error_returns_response_400(self):
        """on_error returning Response(400) → body/status/headers from Response,
        not the default {"error": {...}}."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(
                    content={"custom": "bad request"},
                    status_code=400,
                    headers={"X-Custom": "yes"},
                )

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [CustomErrorCB()])
        body, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert json.loads(body) == {"custom": "bad request"}
        assert headers is not None
        assert headers.get("_sc") == "400"
        assert headers.get("X-Custom") == "yes"

    @pytest.mark.asyncio
    async def test_unary_on_error_returns_response_500(self):
        """on_error returning Response(500) + custom body."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(
                    content={"error": "internal", "request_id": ctx.meta.request_id},
                    status_code=500,
                )

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise RuntimeError("boom")

        pipe = Pipeline.build(FailingAPI(), [CustomErrorCB()])
        meta = _make_meta()
        body, status, metrics, headers = await pipe.run_single(b"{}", meta)
        data = json.loads(body)
        assert data["error"] == "internal"
        assert data["request_id"] == meta.request_id
        assert headers is not None
        assert headers.get("_sc") == "500"

    @pytest.mark.asyncio
    async def test_unary_on_error_returns_none_is_backward_compat(self):
        """on_error returning None → original exception re-raised (unchanged)."""

        class NoopErrorCB(Callback):
            def on_error(self, ctx, exc):
                return None

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [NoopErrorCB()])
        with pytest.raises(ValueError, match="boom"):
            await pipe.run_single(b"{}", _make_meta())

    @pytest.mark.asyncio
    async def test_unary_no_on_error_hook_is_unchanged(self):
        """No on_error hook → original exception re-raised (unchanged)."""

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [])
        with pytest.raises(ValueError, match="boom"):
            await pipe.run_single(b"{}", _make_meta())

    @pytest.mark.asyncio
    async def test_multi_hook_last_response_wins(self):
        """Multiple on_error hooks: last returning Response wins."""
        from lite_server.response import Response as LiteResponse

        class FirstCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"from": "first"}, status_code=400)

        class SecondCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"from": "second"}, status_code=500)

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [FirstCB(), SecondCB()])
        body, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert json.loads(body) == {"from": "second"}
        assert headers.get("_sc") == "500"

    @pytest.mark.asyncio
    async def test_multi_hook_first_wins_when_second_returns_none(self):
        """Multiple hooks: first returns Response, second None → first wins."""
        from lite_server.response import Response as LiteResponse

        class FirstCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"from": "first"}, status_code=400)

        class SecondCB(Callback):
            def on_error(self, ctx, exc):
                return None

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [FirstCB(), SecondCB()])
        body, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert json.loads(body) == {"from": "first"}

    @pytest.mark.asyncio
    async def test_failing_on_error_hook_does_not_break_collection(self):
        """A failing on_error hook is logged but doesn't prevent later hooks
        from providing a custom response."""
        from lite_server.response import Response as LiteResponse

        class ExplodingCB(Callback):
            def on_error(self, ctx, exc):
                raise RuntimeError("on_error itself failed")

        class FallbackCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"recovered": True}, status_code=500)

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [ExplodingCB(), FallbackCB()])
        body, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert json.loads(body) == {"recovered": True}

    @pytest.mark.asyncio
    async def test_route_on_error_returns_response(self):
        """Route: on_error returning Response → _build_route_response reads ctx.early."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(
                    content={"route_error": True},
                    status_code=503,
                    headers={"X-Route-Error": "1"},
                )

        pipe = Pipeline.for_route([CustomErrorCB()])
        ctx = RequestContext(meta=_make_route_meta())

        async def handler(ctx_arg):
            raise RuntimeError("route boom")

        await pipe.run_route(ctx, handler)
        assert ctx.early is not None
        assert ctx.early.status_code == 503
        assert ctx.early.content == {"route_error": True}

    @pytest.mark.asyncio
    async def test_d2_after_encode_response_early_plus_error_does_not_swallow(self):
        """D2: after_encode_response hook A returns Response (ctx.early set),
        hook B raises, on_error returns None → exception re-raised normally
        (ctx.early from A is NOT used, the exception is not swallowed)."""
        from lite_server.response import Response as LiteResponse

        calls = []

        class HookA(Callback):
            def after_encode_response(self, ctx):
                calls.append("A")
                return LiteResponse(content={"from": "A"})

        class HookB(Callback):
            def after_encode_response(self, ctx):
                calls.append("B")
                raise RuntimeError("hook B failed")

        class NoopErrorCB(Callback):
            def on_error(self, ctx, exc):
                calls.append("on_error")
                return None

        class API(EchoAPI):
            pass

        pipe = Pipeline.build(API(), [HookA(), HookB(), NoopErrorCB()])
        with pytest.raises(RuntimeError, match="hook B failed"):
            await pipe.run_single(b"{}", _make_meta())
        assert "A" in calls
        assert "B" in calls
        assert "on_error" in calls

    @pytest.mark.asyncio
    async def test_error_overridden_flag_set_on_custom_response(self):
        """run_single sets ctx.error_overridden=True when on_error returns a Response."""
        from lite_server.response import Response as LiteResponse

        flag_value = None

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                nonlocal flag_value
                flag_value = ctx.error_overridden  # should be False at hook time
                return LiteResponse(content={"ok": True})

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [CustomErrorCB()])
        body, status, metrics, headers = await pipe.run_single(b"{}", _make_meta())
        # At hook time error_overridden is False (set by caller AFTER hook returns)
        assert flag_value is False
