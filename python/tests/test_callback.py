"""Tests for lite_server.callback module."""

import pytest
from lite_server.callback import Callback, CallbackRunner, load_callbacks
from lite_server.api import RequestMeta


def _make_meta(route="/predict", payload=None):
    return RequestMeta(
        route=route,
        headers={"content-type": "application/json"},
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=123456789,
        payload=payload or {"input": "hello"},
    )


# ---------------------------------------------------------------------------
# Callback base class
# ---------------------------------------------------------------------------

class TestCallbackBase:
    def test_can_instantiate(self):
        cb = Callback()
        assert cb is not None

    def test_all_hooks_are_noop_by_default(self):
        cb = Callback()
        meta = _make_meta()
        # None of these should raise
        cb.on_before_setup({}, "cpu")
        cb.on_after_setup(object())
        cb.on_teardown(object())
        assert cb.on_before_decode({"x": 1}, meta) is None
        assert cb.on_after_decode({"x": 1}, meta) is None
        assert cb.on_before_predict({"x": 1}, meta) is None
        assert cb.on_after_predict({"y": 2}, meta) is None
        assert cb.on_before_encode({"y": 2}, meta) is None
        assert cb.on_after_encode({"y": 2}, meta) is None

    def test_subclass_can_override_single_hook(self):
        class MyCallback(Callback):
            def on_after_predict(self, output, meta):
                return {"result": output}

        cb = MyCallback()
        result = cb.on_after_predict({"raw": 42}, _make_meta())
        assert result == {"result": {"raw": 42}}

    def test_subclass_inherits_other_hooks(self):
        class MyCallback(Callback):
            def on_after_predict(self, output, meta):
                return {"result": output}

        cb = MyCallback()
        # Other hooks should still be no-ops
        assert cb.on_before_decode({"x": 1}, _make_meta()) is None


# ---------------------------------------------------------------------------
# CallbackRunner
# ---------------------------------------------------------------------------

class TestCallbackRunner:
    def test_empty_runner_returns_value_unchanged(self):
        runner = CallbackRunner()
        result = runner.trigger("on_before_decode", {"x": 1}, _make_meta())
        assert result == {"x": 1}

    def test_empty_runner_has_callbacks_false(self):
        runner = CallbackRunner()
        assert not runner.has_callbacks()

    def test_runner_with_callbacks_has_callbacks_true(self):
        runner = CallbackRunner([Callback()])
        assert runner.has_callbacks()

    def test_single_callback_transforms_data(self):
        class AddVersion(Callback):
            def on_before_decode(self, request, meta):
                request["_version"] = 1
                return request

        runner = CallbackRunner([AddVersion()])
        result = runner.trigger("on_before_decode", {"x": 1}, _make_meta())
        assert result == {"x": 1, "_version": 1}

    def test_multiple_callbacks_chain(self):
        class AddA(Callback):
            def on_before_decode(self, request, meta):
                request["a"] = 1
                return request

        class AddB(Callback):
            def on_before_decode(self, request, meta):
                request["b"] = 2
                return request

        runner = CallbackRunner([AddA(), AddB()])
        result = runner.trigger("on_before_decode", {}, _make_meta())
        assert result == {"a": 1, "b": 2}

    def test_exception_isolation(self):
        class GoodCallback(Callback):
            def on_before_decode(self, request, meta):
                request["good"] = True
                return request

        class BadCallback(Callback):
            def on_before_decode(self, request, meta):
                raise RuntimeError("boom")

        runner = CallbackRunner([BadCallback(), GoodCallback()])
        # Should not raise; GoodCallback still runs after BadCallback fails
        result = runner.trigger("on_before_decode", {}, _make_meta())
        assert result == {"good": True}

    def test_exception_isolation_first_good_then_bad(self):
        class GoodCallback(Callback):
            def on_before_decode(self, request, meta):
                request["good"] = True
                return request

        class BadCallback(Callback):
            def on_before_decode(self, request, meta):
                raise RuntimeError("boom")

        runner = CallbackRunner([GoodCallback(), BadCallback()])
        result = runner.trigger("on_before_decode", {}, _make_meta())
        # GoodCallback's modification should be visible
        assert result == {"good": True}

    def test_none_return_passes_through(self):
        class NoopCallback(Callback):
            def on_before_decode(self, request, meta):
                return None  # should pass through unchanged

        runner = CallbackRunner([NoopCallback()])
        result = runner.trigger("on_before_decode", {"x": 1}, _make_meta())
        assert result == {"x": 1}

    def test_trigger_void(self):
        calls = []

        class TrackTeardown(Callback):
            def on_teardown(self, lit_api):
                calls.append("torn_down")

        runner = CallbackRunner([TrackTeardown()])
        runner.trigger_void("on_teardown", "fake_litapi")
        assert calls == ["torn_down"]

    def test_trigger_void_exception_isolation(self):
        calls = []

        class GoodTeardown(Callback):
            def on_teardown(self, lit_api):
                calls.append("good")

        class BadTeardown(Callback):
            def on_teardown(self, lit_api):
                raise RuntimeError("boom")

        runner = CallbackRunner([BadTeardown(), GoodTeardown()])
        runner.trigger_void("on_teardown", "fake_litapi")
        assert calls == ["good"]

    def test_register_adds_callback(self):
        runner = CallbackRunner()
        assert not runner.has_callbacks()
        runner.register(Callback())
        assert runner.has_callbacks()

    def test_callbacks_property_returns_copy(self):
        cb = Callback()
        runner = CallbackRunner([cb])
        assert runner.callbacks == [cb]
        # Should be a copy, not the internal list
        assert runner.callbacks is not runner._callbacks


# ---------------------------------------------------------------------------
# Async trigger
# ---------------------------------------------------------------------------

class TestCallbackRunnerAsync:
    @pytest.mark.asyncio
    async def test_trigger_async_with_sync_callback(self):
        class AddVersion(Callback):
            def on_before_decode(self, request, meta):
                request["_v"] = 1
                return request

        runner = CallbackRunner([AddVersion()])
        result = await runner.trigger_async("on_before_decode", {"x": 1}, _make_meta())
        assert result == {"x": 1, "_v": 1}

    @pytest.mark.asyncio
    async def test_trigger_async_with_async_callback(self):
        class AsyncCallback(Callback):
            async def on_before_decode(self, request, meta):
                request["async"] = True
                return request

        runner = CallbackRunner([AsyncCallback()])
        result = await runner.trigger_async("on_before_decode", {"x": 1}, _make_meta())
        assert result == {"x": 1, "async": True}

    @pytest.mark.asyncio
    async def test_trigger_async_exception_isolation(self):
        class GoodCallback(Callback):
            async def on_before_decode(self, request, meta):
                request["good"] = True
                return request

        class BadCallback(Callback):
            async def on_before_decode(self, request, meta):
                raise RuntimeError("async boom")

        runner = CallbackRunner([BadCallback(), GoodCallback()])
        result = await runner.trigger_async("on_before_decode", {}, _make_meta())
        assert result == {"good": True}

    @pytest.mark.asyncio
    async def test_trigger_void_async(self):
        calls = []

        class AsyncTeardown(Callback):
            async def on_teardown(self, lit_api):
                calls.append("async_torn_down")

        runner = CallbackRunner([AsyncTeardown()])
        await runner.trigger_void_async("on_teardown", "fake_litapi")
        assert calls == ["async_torn_down"]

    @pytest.mark.asyncio
    async def test_trigger_void_async_mixed(self):
        calls = []

        class SyncCb(Callback):
            def on_teardown(self, lit_api):
                calls.append("sync")

        class AsyncCb(Callback):
            async def on_teardown(self, lit_api):
                calls.append("async")

        runner = CallbackRunner([SyncCb(), AsyncCb()])
        await runner.trigger_void_async("on_teardown", "fake_litapi")
        assert calls == ["sync", "async"]


# ---------------------------------------------------------------------------
# load_callbacks
# ---------------------------------------------------------------------------

class TestLoadCallbacks:
    def test_empty_config_returns_empty_runner(self):
        runner = load_callbacks({})
        assert not runner.has_callbacks()

    def test_no_callbacks_key_returns_empty_runner(self):
        runner = load_callbacks({"max_batch_size": 4})
        assert not runner.has_callbacks()

    def test_loads_callback_from_config(self):
        config = {"callbacks": ["lite_server.callback.Callback"]}
        runner = load_callbacks(config)
        assert runner.has_callbacks()
        assert len(runner.callbacks) == 1
        assert isinstance(runner.callbacks[0], Callback)

    def test_skips_non_callback_class(self):
        config = {"callbacks": ["json.JSONEncoder"]}
        runner = load_callbacks(config)
        assert not runner.has_callbacks()

    def test_skips_invalid_path(self):
        config = {"callbacks": ["nonexistent.module.Class"]}
        runner = load_callbacks(config)
        assert not runner.has_callbacks()

    def test_loads_multiple_callbacks(self):
        config = {
            "callbacks": [
                "lite_server.callback.Callback",
                "lite_server.callback.Callback",
            ]
        }
        runner = load_callbacks(config)
        assert len(runner.callbacks) == 2


# ---------------------------------------------------------------------------
# Hook coverage: each hook type with CallbackRunner
# ---------------------------------------------------------------------------

class TestHookDispatch:
    """Verify that trigger() dispatches to the correct hook method."""

    def test_on_before_decode_dispatched(self):
        class Cb(Callback):
            def on_before_decode(self, request, meta):
                request["hooked"] = True
                return request

        runner = CallbackRunner([Cb()])
        result = runner.trigger("on_before_decode", {}, _make_meta())
        assert result == {"hooked": True}

    def test_on_after_decode_dispatched(self):
        class Cb(Callback):
            def on_after_decode(self, decoded, meta):
                return {"wrapped": decoded}

        runner = CallbackRunner([Cb()])
        result = runner.trigger("on_after_decode", [1, 2, 3], _make_meta())
        assert result == {"wrapped": [1, 2, 3]}

    def test_on_before_predict_dispatched(self):
        class Cb(Callback):
            def on_before_predict(self, decoded, meta):
                return {"input": decoded}

        runner = CallbackRunner([Cb()])
        result = runner.trigger("on_before_predict", "raw", _make_meta())
        assert result == {"input": "raw"}

    def test_on_after_predict_dispatched(self):
        class Cb(Callback):
            def on_after_predict(self, output, meta):
                output["confidence"] = 0.95
                return output

        runner = CallbackRunner([Cb()])
        result = runner.trigger("on_after_predict", {"label": "cat"}, _make_meta())
        assert result == {"label": "cat", "confidence": 0.95}

    def test_on_before_encode_dispatched(self):
        class Cb(Callback):
            def on_before_encode(self, output, meta):
                output["_ts"] = meta.timestamp_ns
                return output

        runner = CallbackRunner([Cb()])
        result = runner.trigger("on_before_encode", {"text": "hi"}, _make_meta())
        assert result == {"text": "hi", "_ts": 123456789}

    def test_on_after_encode_dispatched(self):
        class Cb(Callback):
            def on_after_encode(self, encoded, meta):
                if isinstance(encoded, dict):
                    encoded["_responded"] = True
                return encoded

        runner = CallbackRunner([Cb()])
        result = runner.trigger("on_after_encode", {"msg": "ok"}, _make_meta())
        assert result == {"msg": "ok", "_responded": True}

    def test_does_not_dispatch_to_wrong_hook(self):
        """Triggering 'on_before_decode' should not invoke 'on_after_decode'."""
        calls = []

        class Cb(Callback):
            def on_before_decode(self, request, meta):
                calls.append("before")
                return request

            def on_after_decode(self, decoded, meta):
                calls.append("after")
                return decoded

        runner = CallbackRunner([Cb()])
        runner.trigger("on_before_decode", {}, _make_meta())
        assert calls == ["before"]

    def test_hook_not_overridden_is_skipped(self):
        """A callback that doesn't override a hook should be silently skipped."""
        runner = CallbackRunner([Callback()])
        # Should not raise, should return unchanged value
        result = runner.trigger("on_before_decode", {"x": 1}, _make_meta())
        assert result == {"x": 1}


# ---------------------------------------------------------------------------
# Pre-computation: _hooked dict avoids getattr on hot path
# ---------------------------------------------------------------------------

class TestCallbackIndexing:
    """Verify that _hooked dict correctly pre-computes which callbacks
    override which hooks, avoiding per-request getattr lookups."""

    def test_only_overridden_hooks_are_indexed(self):
        class Cb(Callback):
            def on_before_decode(self, request, meta):
                return request

        runner = CallbackRunner([Cb()])
        # on_before_decode is overridden
        assert len(runner._hooked["on_before_decode"]) == 1
        # on_after_decode is NOT overridden
        assert len(runner._hooked["on_after_decode"]) == 0

    def test_base_callback_indexes_nothing(self):
        runner = CallbackRunner([Callback()])
        for hooks in runner._hooked.values():
            assert len(hooks) == 0

    def test_multiple_callbacks_partial_overlap(self):
        class CbA(Callback):
            def on_before_decode(self, request, meta):
                return request

        class CbB(Callback):
            def on_after_predict(self, output, meta):
                return output

        runner = CallbackRunner([CbA(), CbB()])
        assert len(runner._hooked["on_before_decode"]) == 1
        assert len(runner._hooked["on_after_predict"]) == 1
        assert len(runner._hooked["on_before_predict"]) == 0

    def test_register_updates_hooked(self):
        class Cb(Callback):
            def on_before_decode(self, request, meta):
                return request

        runner = CallbackRunner()
        assert len(runner._hooked["on_before_decode"]) == 0
        runner.register(Cb())
        assert len(runner._hooked["on_before_decode"]) == 1

    def test_inherited_override_is_indexed(self):
        class BaseCb(Callback):
            def on_before_decode(self, request, meta):
                return request

        class SubCb(BaseCb):
            def on_after_decode(self, decoded, meta):
                return decoded

        runner = CallbackRunner([SubCb()])
        # SubCb inherits on_before_decode from BaseCb
        assert len(runner._hooked["on_before_decode"]) == 1
        # SubCb defines on_after_decode directly
        assert len(runner._hooked["on_after_decode"]) == 1

    def test_trigger_still_works_with_hooked_optimization(self):
        class AddVersion(Callback):
            def on_before_decode(self, request, meta):
                request["_version"] = 1
                return request

        class NoopCb(Callback):
            pass  # overrides nothing

        runner = CallbackRunner([NoopCb(), AddVersion()])
        # NoopCb should be skipped in _hooked, AddVersion should run
        result = runner.trigger("on_before_decode", {"x": 1}, _make_meta())
        assert result == {"x": 1, "_version": 1}

    def test_trigger_async_still_works_with_hooked_optimization(self):
        class AsyncCb(Callback):
            async def on_before_decode(self, request, meta):
                request["async"] = True
                return request

        class NoopCb(Callback):
            pass

        runner = CallbackRunner([NoopCb(), AsyncCb()])
        import asyncio
        result = asyncio.run(runner.trigger_async("on_before_decode", {"x": 1}, _make_meta()))
        assert result == {"x": 1, "async": True}

    def test_void_hooks_are_indexed(self):
        calls = []

        class Cb(Callback):
            def on_teardown(self, lit_api):
                calls.append("teardown")

        runner = CallbackRunner([Cb()])
        assert len(runner._hooked["on_teardown"]) == 1
        assert len(runner._hooked["on_before_setup"]) == 0
        runner.trigger_void("on_teardown", "fake")
        assert calls == ["teardown"]

    def test_void_async_hooks_are_indexed(self):
        class VoidAsyncCb(Callback):
            async def on_teardown(self, lit_api):
                pass

        class NoopCb(Callback):
            pass

        runner = CallbackRunner([NoopCb(), VoidAsyncCb()])
        assert len(runner._hooked["on_teardown"]) == 1
        # NoopCb should not be in on_teardown
        assert runner._hooked["on_teardown"][0].__class__ == VoidAsyncCb

    def test_multiple_overrides_same_hook_indexed(self):
        class Cb1(Callback):
            def on_before_decode(self, request, meta):
                return request

        class Cb2(Callback):
            def on_before_decode(self, request, meta):
                return request

        runner = CallbackRunner([Cb1(), Cb2()])
        assert len(runner._hooked["on_before_decode"]) == 2
