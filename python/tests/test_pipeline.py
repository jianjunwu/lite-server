"""Contract tests for the unified Pipeline engine.

Covers hook ordering, early return, error propagation, ctx.state,
sync/async adaptation, capability detection, lifecycle hooks, and
ctx injection (0.7.0 context unification).
"""

import json
import threading

import pytest

from lite_server.api import LitAPI
from lite_server.callback import (
    Callback,
    Cors,
    LogRequests,
    RateLimit,
    RequireApiKey,
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
        meta = _make_endpoint_meta()

        class Ordered(EchoAPI):
            def on_request(self, ctx):
                order.append("api.on_request")
                return ctx.request

            def decode_request(self, request):
                order.append("decode")
                return request

            def predict(self, x):
                order.append("predict")
                return x

            def encode_response(self, output):
                order.append("encode")
                return output

            def on_response(self, ctx):
                order.append("api.on_response")
                return ctx.response

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
        await pipe.run_single(b"{}", _make_endpoint_meta())
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
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_endpoint_meta())
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
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_endpoint_meta())
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
        resp_bytes, *_ = await pipe.run_single(b"{}", _make_endpoint_meta())
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
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_endpoint_meta())
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
        resp_bytes, *_ = await pipe.run_single(b"{}", _make_endpoint_meta())
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
            await pipe.run_single(b"{}", _make_endpoint_meta())
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
            await pipe.run_single(b"{}", _make_endpoint_meta())

    @pytest.mark.asyncio
    async def test_http_exception_from_api_on_request_propagates(self):
        class API(EchoAPI):
            def on_request(self, ctx):
                raise BadRequestError("rejected")

        pipe = Pipeline.build(API(), [])
        with pytest.raises(HTTPException):
            await pipe.run_single(b"{}", _make_endpoint_meta())


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
        await pipe.run_single(b"{}", _make_endpoint_meta())
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
        await pipe.run_single(b"{}", _make_endpoint_meta())
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
        await pipe.run_single(json.dumps({"a": 1}).encode(), _make_endpoint_meta())
        assert seen["input"] == {"a": 1, "extra": 1}

    @pytest.mark.asyncio
    async def test_state_is_per_request(self):
        class Counter(Callback):
            def on_request(self, ctx):
                ctx.state["n"] = ctx.request.get("n")

        captured = []

        class API(EchoAPI):
            def predict(self, x):
                return x

        pipe = Pipeline.build(API(), [Counter()])
        ctx1 = RequestContext(meta=_make_endpoint_meta(), request={"n": 1})
        ctx2 = RequestContext(meta=_make_endpoint_meta(), request={"n": 2})
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
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_endpoint_meta())
        assert _body(resp_bytes) == {"echo": {"x": 1}}

    @pytest.mark.asyncio
    async def test_async_predict_runs_natively(self):
        class AsyncAPI(EchoAPI):
            async def predict(self, x):
                return {"async": x}

        pipe = Pipeline.build(AsyncAPI(), [])
        assert pipe.any_async is True
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_endpoint_meta())
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
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_endpoint_meta())
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
        await pipe.run_single(b"{}", _make_endpoint_meta())
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
            *[pipe.run_single(b'{"x": 1}', _make_endpoint_meta()) for _ in range(5)]
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
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_endpoint_meta())
        sc, mt, clean = extract_response_meta(headers)
        assert sc == 201
        assert mt == "text/plain"
        assert clean is None
        assert resp_bytes == b'"plain"'

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
# Ctx injection: API hooks unified with Callback hooks (0.7.0)
# ---------------------------------------------------------------------------


class TestApiHookCtxUnification:
    """LitAPI.on_request / on_response receive the same RequestContext as
    Callback hooks — single ctx parameter, no more (request, meta)."""

    @pytest.mark.asyncio
    async def test_api_hook_receives_same_ctx_as_callbacks(self):
        api_ctx = []
        cb_ctx = []

        class API(EchoAPI):
            def on_request(self, ctx):
                api_ctx.append(ctx)
                return ctx.request

            def on_response(self, ctx):
                api_ctx.append(ctx)
                return ctx.response

        class CB(Callback):
            def on_request(self, ctx):
                cb_ctx.append(ctx)

            def on_response(self, ctx):
                cb_ctx.append(ctx)

        pipe = Pipeline.build(API(), [CB()])
        await pipe.run_single(b"{}", _make_endpoint_meta())
        assert len(api_ctx) == 2
        assert len(cb_ctx) == 2
        # All four hooks see the same ctx object
        assert api_ctx[0] is cb_ctx[0] is api_ctx[1] is cb_ctx[1]

    @pytest.mark.asyncio
    async def test_api_hook_early_return_via_respond(self):
        """on_request returns ctx.respond(...) — pipeline short-circuits."""
        called = []

        class API(EchoAPI):
            def on_request(self, ctx):
                return ctx.respond({"denied": True}, status_code=401)

            def decode_request(self, request):
                called.append("decode")
                return request

            def predict(self, x):
                called.append("predict")
                return x

        pipe = Pipeline.build(API(), [])
        resp_bytes, status, metrics, headers = await pipe.run_single(b"{}", _make_endpoint_meta())
        assert called == []
        assert _body(resp_bytes) == {"denied": True}
        sc, mt, clean = extract_response_meta(headers)
        assert sc == 401

    def test_old_two_arg_api_hook_fails_at_build(self):
        class OldAPI(EchoAPI):
            def on_request(self, request, meta):
                return request

        with pytest.raises(RuntimeError, match="on_request"):
            Pipeline.build(OldAPI(), [])

    def test_old_two_arg_on_response_fails_at_build(self):
        class OldAPI(EchoAPI):
            def on_response(self, response, meta):
                return response

        with pytest.raises(RuntimeError, match="on_response"):
            Pipeline.build(OldAPI(), [])

    def test_single_param_named_request_fails_at_build(self):
        """A hook with a single 'request' param (old shape with default meta)
        must be rejected — silently binding ctx to 'request' is wrong."""

        class BadAPI(EchoAPI):
            def on_request(self, request, meta=None):
                return request

        with pytest.raises(RuntimeError, match="on_request"):
            Pipeline.build(BadAPI(), [])


class TestUnoverriddenApiHooksSkipped:
    """When a LitAPI subclass does NOT override on_request/on_response,
    the base no-ops must not be added to the hook chain — saves 2 async
    calls per request."""

    def test_unoverridden_api_hooks_not_in_chain(self):
        pipe = Pipeline.build(EchoAPI(), [])
        assert pipe._chains["on_request"] == []
        assert pipe._chains["on_response"] == []

    def test_overridden_on_request_added_to_chain(self):
        class API(EchoAPI):
            def on_request(self, ctx):
                return ctx.request

        pipe = Pipeline.build(API(), [])
        assert len(pipe._chains["on_request"]) == 1

    def test_overridden_on_response_added_to_chain(self):
        class API(EchoAPI):
            def on_response(self, ctx):
                return ctx.response

        pipe = Pipeline.build(API(), [])
        assert len(pipe._chains["on_response"]) == 1


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
            def decode_request(self, request, ctx):
                captured.append(ctx)
                return {"prompt": request.get("q"), "uid": ctx.meta.request_id}

            def predict(self, x):
                return x

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"q": "hello"}', _make_endpoint_meta())
        assert len(captured) == 1
        assert captured[0].meta.request_id == "req-1"
        assert _body(resp_bytes) == {"prompt": "hello", "uid": "req-1"}

    @pytest.mark.asyncio
    async def test_encode_response_ctx_injection(self):
        captured = []

        class API(EchoAPI):
            def encode_response(self, output, ctx):
                captured.append(ctx.meta.route)
                return {"result": output, "route": ctx.meta.route}

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_endpoint_meta())
        assert captured == ["/predict"]
        assert _body(resp_bytes)["route"] == "/predict"

    @pytest.mark.asyncio
    async def test_predict_ctx_injection_single(self):
        captured = []

        class API(EchoAPI):
            def predict(self, x, ctx):
                captured.append(ctx.meta.request_id)
                return {"echo": x, "req": ctx.meta.request_id}

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_endpoint_meta())
        assert captured == ["req-1"]
        assert _body(resp_bytes)["req"] == "req-1"

    @pytest.mark.asyncio
    async def test_keyword_only_ctx_injection(self):
        """Keyword-only 'ctx' parameter (e.g. `*, ctx`) is supported."""

        class API(EchoAPI):
            def decode_request(self, request, *, ctx):
                ctx.state["from_decode"] = True
                return request

            def predict(self, x, *, ctx):
                assert ctx.state["from_decode"] is True
                return x

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"x": 1}', _make_endpoint_meta())
        assert _body(resp_bytes) == {"x": 1}

    @pytest.mark.asyncio
    async def test_state_threaded_on_request_to_decode(self):
        class API(EchoAPI):
            def on_request(self, ctx):
                ctx.state["user"] = "alice"
                return ctx.request

            def decode_request(self, request, ctx):
                return {"user": ctx.state["user"], **request}

            def predict(self, x):
                return x

        pipe = Pipeline.build(API(), [])
        resp_bytes, *_ = await pipe.run_single(b'{"q": "hi"}', _make_endpoint_meta())
        assert _body(resp_bytes)["user"] == "alice"

    @pytest.mark.asyncio
    async def test_on_response_respond_attaches_headers(self):
        """on_response can use ctx.respond() to attach custom headers —
        replaces the old ResponseWithHeaders pattern."""

        class API(EchoAPI):
            def predict(self, x):
                return {"result": x.get("val")}

            def on_response(self, ctx):
                return ctx.respond(
                    ctx.response,
                    headers={"X-Request-ID": ctx.meta.request_id, "X-Cache": "HIT"},
                )

        pipe = Pipeline.build(API(), [])
        resp_bytes, status, metrics, headers = await pipe.run_single(
            b'{"val": 42}', _make_endpoint_meta()
        )
        assert _body(resp_bytes) == {"result": 42}
        assert headers == {"X-Request-ID": "req-1", "X-Cache": "HIT"}


# ---------------------------------------------------------------------------
# Ctx injection: forbidden methods (batch / unbatch / step)
# ---------------------------------------------------------------------------


class TestCtxForbidden:
    """Declaring 'ctx' on batch, unbatch, or step is a load-time error —
    these methods operate across items/sequences and have no single context."""

    def test_predict_ctx_with_batch_unbatch_fails_at_build(self):
        class API(EchoAPI):
            def batch(self, inputs):
                return inputs

            def unbatch(self, output):
                return output

            def predict(self, x, ctx):
                return x

        with pytest.raises(RuntimeError, match="predict.*ctx.*batch"):
            Pipeline.build(API(), [])

    @pytest.mark.parametrize("method_name", ["batch", "unbatch", "step"])
    def test_ctx_forbidden_methods_fail_at_build(self, method_name):
        # Create a class that overrides the forbidden method with a ctx param
        overrides = {
            method_name: lambda self, x, ctx: x,
        }
        # step needs prefill + has_finished to be a valid CB model
        if method_name == "step":
            overrides["prefill"] = lambda self, uid, inp: None
            overrides["has_finished"] = lambda self, uid, tok, seq: True
        # batch needs unbatch too
        if method_name == "batch":
            overrides["unbatch"] = lambda self, x: x

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


def _make_endpoint_meta(**kwargs):
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
# for_endpoint
# ---------------------------------------------------------------------------


class TestForEndpoint:
    def test_rejects_on_input_hook(self):
        class BadCB(Callback):
            def on_input(self, ctx):
                pass

        with pytest.raises(RuntimeError, match="on_input"):
            Pipeline.for_endpoint([BadCB()])

    def test_rejects_on_output_hook(self):
        class BadCB(Callback):
            def on_output(self, ctx):
                pass

        with pytest.raises(RuntimeError, match="on_output"):
            Pipeline.for_endpoint([BadCB()])

    def test_accepts_on_request_and_on_response(self):
        class GoodCB(Callback):
            def on_request(self, ctx):
                pass

            def on_response(self, ctx):
                pass

        pipe = Pipeline.for_endpoint([GoodCB()])
        assert pipe.lit_api is None
        assert len(pipe.callbacks) == 1

    def test_accepts_builtin_callbacks(self):
        pipe = Pipeline.for_endpoint([
            RequireApiKey(keys=["sk-123"]),
            RateLimit(requests_per_minute=60),
            LogRequests(),
            Cors(),
        ])
        assert len(pipe.callbacks) == 4


# ---------------------------------------------------------------------------
# run_endpoint
# ---------------------------------------------------------------------------


class DummyLitAPI(LitAPI):
    def predict(self, x):
        return x


class TestRunEndpoint:
    @pytest.mark.asyncio
    async def test_handler_called_with_ctx(self):
        pipe = Pipeline.for_endpoint([])
        ctx = RequestContext(meta=_make_endpoint_meta(), request={"k": "v"})
        called = []

        async def handler(ctx_arg):
            called.append(ctx_arg)

        await pipe.run_endpoint(ctx, handler)
        assert len(called) == 1
        assert called[0] is ctx

    @pytest.mark.asyncio
    async def test_on_request_early_return_skips_handler(self):
        class EarlyCB(Callback):
            def on_request(self, ctx):
                ctx.respond({"blocked": True}, status_code=403)

        pipe = Pipeline.for_endpoint([EarlyCB()])
        ctx = RequestContext(meta=_make_endpoint_meta())
        called = False

        async def handler(ctx_arg):
            nonlocal called
            called = True

        await pipe.run_endpoint(ctx, handler)
        assert not called
        assert ctx.early is not None
        assert ctx.early.status_code == 403

    @pytest.mark.asyncio
    async def test_handler_result_stored_as_response(self):
        pipe = Pipeline.for_endpoint([])
        ctx = RequestContext(meta=_make_endpoint_meta())

        async def handler(ctx_arg):
            return {"result": "ok"}

        await pipe.run_endpoint(ctx, handler)
        assert ctx.response == {"result": "ok"}

    @pytest.mark.asyncio
    async def test_on_response_after_handler(self):
        responses = []

        class TrackCB(Callback):
            def on_response(self, ctx):
                responses.append(ctx.response)

        pipe = Pipeline.for_endpoint([TrackCB()])
        ctx = RequestContext(meta=_make_endpoint_meta())

        async def handler(ctx_arg):
            return {"x": 1}

        await pipe.run_endpoint(ctx, handler)
        assert responses == [{"x": 1}]

    @pytest.mark.asyncio
    async def test_on_error_driven_on_httpexception(self):
        errors = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                errors.append(exc)

        pipe = Pipeline.for_endpoint([ErrCB()])
        ctx = RequestContext(meta=_make_endpoint_meta())

        async def handler(ctx_arg):
            raise HTTPException(400, "bad")

        with pytest.raises(HTTPException):
            await pipe.run_endpoint(ctx, handler)
        assert len(errors) == 1
        assert isinstance(errors[0], HTTPException)

    @pytest.mark.asyncio
    async def test_on_error_does_not_mask_original(self):
        class ExplodingCB(Callback):
            def on_error(self, ctx, exc):
                raise RuntimeError("boom")

        pipe = Pipeline.for_endpoint([ExplodingCB()])
        ctx = RequestContext(meta=_make_endpoint_meta())

        async def handler(ctx_arg):
            raise HTTPException(400, "bad")

        with pytest.raises(HTTPException):  # original, not RuntimeError
            await pipe.run_endpoint(ctx, handler)

    @pytest.mark.asyncio
    async def test_sync_handler_in_async_pipeline(self):
        """Sync handler runs on executor when any callback is async."""
        class AsyncCB(Callback):
            async def on_request(self, ctx):
                pass

        pipe = Pipeline.for_endpoint([AsyncCB()])
        ctx = RequestContext(meta=_make_endpoint_meta())

        def sync_handler(ctx_arg):
            return {"sync": True}

        await pipe.run_endpoint(ctx, handler=sync_handler)
        assert ctx.response == {"sync": True}

    @pytest.mark.asyncio
    async def test_sync_handler_returning_coroutine_is_awaited(self):
        """B7: a sync callable that returns a coroutine (e.g. an object with
        async __call__, invisible to iscoroutinefunction) runs through
        run_blocking and must be awaited — not stored raw on ctx.response.
        Mirrors the runtime guard already present in _adapt."""
        import asyncio

        class AsyncCB(Callback):
            async def on_request(self, ctx):
                pass

        pipe = Pipeline.for_endpoint([AsyncCB()])
        assert pipe._executor is not None  # mixed mode -> run_blocking path
        ctx = RequestContext(meta=_make_endpoint_meta())

        class CallableObj:
            async def __call__(self, ctx_arg):
                return {"answered": True}

        await pipe.run_endpoint(ctx, handler=CallableObj())
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
            def on_request(self, ctx):
                raise BadRequestError("bad input")

        api = DummyLitAPI()
        pipe = Pipeline.build(api, [ErrCB(), FailingCB()])
        meta = _make_endpoint_meta()

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
        meta = _make_endpoint_meta()

        with pytest.raises(RuntimeError):
            await pipe.run_single(b'{"x": 1}', meta)
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# finalize + response_headers
# ---------------------------------------------------------------------------


class TestFinalizeResponseHeaders:
    def test_response_headers_merged(self):
        ctx = RequestContext(meta=_make_endpoint_meta(), response={"result": "ok"})
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

        ctx = RequestContext(meta=_make_endpoint_meta())
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

        ctx = RequestContext(meta=_make_endpoint_meta())
        ctx.response_headers["X-Ctx"] = "ctx_val"
        ctx.early = LiteResponse(content={"early": True}, headers={"X-Early": "early_val"})

        api = DummyLitAPI()
        pipe = Pipeline.build(api, [])
        resp_bytes, status, metrics, headers = pipe.finalize(ctx)

        assert headers is not None
        assert headers["X-Ctx"] == "ctx_val"
        assert headers["X-Early"] == "early_val"
