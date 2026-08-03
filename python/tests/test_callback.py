"""Tests for Callback base, RequestContext, load_callbacks, builtin callbacks,
and migration errors."""

import os
import time

import pytest

from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.callbacks import (
    Callback,
    load_callbacks,
    validate_callback,
)
from lite_server.exceptions import HTTPException, UnauthorizedError


def _make_meta():
    return RequestMeta(
        route="/predict",
        headers=Headers({}),
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=1,
    )


class TestRequestContext:
    def test_respond_sets_early_with_status_and_headers(self):
        ctx = RequestContext(meta=_make_meta())
        ctx.respond({"err": "x"}, status_code=403, headers={"X-A": "1"})
        assert ctx.early is not None
        assert ctx.early.content == {"err": "x"}
        assert ctx.early.status_code == 403
        assert ctx.early.headers == {"X-A": "1"}

    def test_state_defaults_to_empty_dict(self):
        ctx = RequestContext(meta=_make_meta())
        assert ctx.state == {}
        ctx.state["k"] = 1
        assert ctx.state["k"] == 1

    def test_mode_defaults_to_none(self):
        ctx = RequestContext(meta=_make_meta())
        assert ctx.mode is None

    def test_stage_defaults_to_none(self):
        ctx = RequestContext(meta=_make_meta())
        assert ctx.stage is None


class TestElapsedMs:
    def test_nonnegative_right_after_construction(self):
        meta = RequestMeta(
            route="/predict", headers=Headers({}), client_ip="127.0.0.1",
            request_id="r", timestamp_ns=time.time_ns(),
        )
        ctx = RequestContext(meta=meta)
        assert ctx.elapsed_ms() >= 0

    def test_increases_with_time(self):
        meta = RequestMeta(
            route="/predict", headers=Headers({}), client_ip="127.0.0.1",
            request_id="r", timestamp_ns=time.time_ns(),
        )
        ctx = RequestContext(meta=meta)
        first = ctx.elapsed_ms()
        time.sleep(0.01)
        assert ctx.elapsed_ms() > first


class TestDeadlineRemaining:
    def test_returns_none_when_no_deadline(self):
        ctx = RequestContext(meta=_make_meta())  # _make_meta omits deadline → None
        assert ctx.deadline_remaining_ms() is None

    def test_returns_positive_ms_when_deadline_in_future(self):
        future_ns = time.time_ns() + 1_000_000_000  # 1s ahead
        meta = RequestMeta(
            route="/predict", headers=Headers({}), client_ip="127.0.0.1",
            request_id="req-1", timestamp_ns=1, deadline_unix_ns=future_ns,
        )
        ctx = RequestContext(meta=meta)
        remaining = ctx.deadline_remaining_ms()
        assert remaining is not None
        assert 0 < remaining <= 1000.0

    def test_returns_negative_ms_when_deadline_passed(self):
        past_ns = time.time_ns() - 1_000_000_000  # 1s ago
        meta = RequestMeta(
            route="/predict", headers=Headers({}), client_ip="127.0.0.1",
            request_id="req-1", timestamp_ns=1, deadline_unix_ns=past_ns,
        )
        ctx = RequestContext(meta=meta)
        remaining = ctx.deadline_remaining_ms()
        assert remaining is not None
        assert remaining < 0


class TestValidateCallback:
    def test_new_style_callback_passes(self):
        class Good(Callback):
            def before_decode_request(self, ctx):
                pass

            def after_predict(self, ctx):
                pass

        validate_callback(Good())  # must not raise

    def test_base_callback_passes(self):
        validate_callback(Callback())

    @pytest.mark.parametrize(
        "old_name,new_name",
        [
            ("on_before_decode", "before_decode_request"),
            ("on_after_decode", "after_decode_request"),
            ("on_before_predict", "after_decode_request"),
            ("on_after_predict", "after_predict"),
            ("on_before_encode", "after_predict"),
            ("on_after_encode", "after_encode_response"),
        ],
    )
    def test_removed_hook_names_raise_with_migration_hint(self, old_name, new_name):
        cls = type(
            "Old",
            (Callback,),
            {old_name: lambda self, value, meta: value},
        )
        with pytest.raises(RuntimeError, match=old_name) as exc_info:
            validate_callback(cls())
        assert new_name in str(exc_info.value)
        assert "ctx" in str(exc_info.value)

    @pytest.mark.parametrize(
        "old_name,new_name",
        [
            ("on_request", "before_decode_request"),
            ("on_input", "after_decode_request"),
            ("on_output", "after_predict"),
            ("on_response", "after_encode_response"),
            ("on_batch_input", "after_batch"),
            ("on_batch_output", "after_unbatch"),
            ("on_before_setup", "before_setup"),
            ("on_after_setup", "after_setup"),
            ("on_teardown", "before_teardown"),
        ],
    )
    def test_renamed_hook_names_raise_with_rename_hint(self, old_name, new_name):
        """0.7–0.8 hook names are a pure rename — loud error, not a silent skip."""
        cls = type(
            "Old",
            (Callback,),
            {old_name: lambda self, ctx: None},
        )
        with pytest.raises(RuntimeError, match=old_name) as exc_info:
            validate_callback(cls())
        assert new_name in str(exc_info.value)
        assert "renamed" in str(exc_info.value)

    def test_wrong_arity_new_hook_raises(self):
        class Bad(Callback):
            def before_decode_request(self, ctx, meta):  # old two-arg shape on a new name
                pass

        with pytest.raises(RuntimeError, match="before_decode_request"):
            validate_callback(Bad())

    def test_batch_hook_wrong_arity_raises(self):
        class Bad(Callback):
            def after_batch(self, ctx_list):  # needs (ctx_list, value)
                pass

        with pytest.raises(RuntimeError, match="after_batch"):
            validate_callback(Bad())

    def test_stream_close_wrong_arity_raises(self):
        class Bad(Callback):
            def on_stream_close(self, ctx):  # needs (ctx, reason)
                pass

        with pytest.raises(RuntimeError, match="on_stream_close"):
            validate_callback(Bad())

    def test_lifecycle_hooks_unaffected(self):
        class LC(Callback):
            def before_setup(self, config, device):
                pass

            def after_setup(self, lit_api):
                pass

            def before_teardown(self, lit_api):
                pass

            def after_teardown(self, lit_api):
                pass

        validate_callback(LC())  # must not raise

    def test_non_callback_instance_is_rejected(self):
        """C3: a non-Callback object gets a friendly, specific error."""
        for bad in (object(), "not a callback", 42, lambda ctx: None):
            with pytest.raises(RuntimeError, match="lite_server.Callback"):
                validate_callback(bad)


class TestLoadCallbacks:
    def test_empty_config_returns_empty_list(self):
        assert load_callbacks({}) == []
        assert load_callbacks({"callbacks": []}) == []
        assert load_callbacks({"callbacks": None}) == []

    def test_loads_valid_callbacks_in_order(self, tmp_path, monkeypatch):
        (tmp_path / "my_callbacks.py").write_text(
            "from lite_server import Callback\n"
            "class A(Callback):\n"
            "    def before_decode_request(self, ctx):\n"
            "        pass\n"
            "class B(Callback):\n"
            "    pass\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        cbs = load_callbacks({"callbacks": ["my_callbacks.A", "my_callbacks.B"]})
        assert [type(c).__name__ for c in cbs] == ["A", "B"]

    def test_import_failure_is_loud(self):
        with pytest.raises(RuntimeError, match="Failed to import callback"):
            load_callbacks({"callbacks": ["no_such_module_xyz.CB"]})

    def test_non_callback_class_is_loud(self, tmp_path, monkeypatch):
        (tmp_path / "plain.py").write_text("class C:\n    pass\n")
        monkeypatch.syspath_prepend(str(tmp_path))
        with pytest.raises(RuntimeError, match="not a lite_server.Callback subclass"):
            load_callbacks({"callbacks": ["plain.C"]})

    def test_non_noarg_constructor_is_loud(self, tmp_path, monkeypatch):
        (tmp_path / "needs_args.py").write_text(
            "from lite_server import Callback\n"
            "class C(Callback):\n"
            "    def __init__(self, required):\n"
            "        self.required = required\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        with pytest.raises(RuntimeError, match="no-arg"):
            load_callbacks({"callbacks": ["needs_args.C"]})

    def test_old_signature_callback_is_loud(self, tmp_path, monkeypatch):
        (tmp_path / "old_style.py").write_text(
            "from lite_server import Callback\n"
            "class Old(Callback):\n"
            "    def on_before_decode(self, request, meta):\n"
            "        return request\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        with pytest.raises(RuntimeError, match="on_before_decode"):
            load_callbacks({"callbacks": ["old_style.Old"]})

    def test_loads_callback_with_map_kwargs(self, tmp_path, monkeypatch):
        (tmp_path / "mapkw.py").write_text(
            "from lite_server import Callback\n"
            "class C(Callback):\n"
            "    def __init__(self, tag='none'):\n"
            "        self.tag = tag\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        cbs = load_callbacks({"callbacks": [{"mapkw.C": {"tag": "x"}}]})
        assert len(cbs) == 1
        assert cbs[0].tag == "x"

    def test_mixed_str_and_map_entries(self, tmp_path, monkeypatch):
        (tmp_path / "mixkw.py").write_text(
            "from lite_server import Callback\n"
            "class C(Callback):\n"
            "    def __init__(self, tag='none'):\n"
            "        self.tag = tag\n"
            "class D(Callback):\n"
            "    pass\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        cbs = load_callbacks({"callbacks": ["mixkw.D", {"mixkw.C": {"tag": "y"}}]})
        assert [type(c).__name__ for c in cbs] == ["D", "C"]
        assert cbs[1].tag == "y"

    def test_map_entry_with_multiple_keys_raises(self):
        with pytest.raises(RuntimeError, match="exactly one key"):
            load_callbacks({"callbacks": [{"a.b": {}, "c.d": {}}]})

    def test_non_str_non_dict_entry_raises(self):
        with pytest.raises(RuntimeError, match="class-path string"):
            load_callbacks({"callbacks": [42]})


# ---------------------------------------------------------------------------
# _TokenBucket
# ---------------------------------------------------------------------------
# load_callbacks with LitAPI class attribute
# ---------------------------------------------------------------------------


class _TagCB(Callback):
    """Data-hook callback with constructor args (class-attribute loading)."""

    def __init__(self, tag="default"):
        self.tag = tag


class TestLoadCallbacksWithLitAPI:
    def test_class_attribute_priority(self, tmp_path, monkeypatch):
        """LitAPI.callbacks take priority and support constructor args."""
        from lite_server.api import LitAPI

        class MyAPI(LitAPI):
            callbacks = (_TagCB(tag="sk-123"),)

        api = MyAPI()
        cbs = load_callbacks({}, api)
        assert len(cbs) == 1
        assert isinstance(cbs[0], _TagCB)

    def test_class_attr_and_yaml_merged(self, tmp_path, monkeypatch):
        from lite_server.api import LitAPI

        (tmp_path / "my_cb.py").write_text(
            "from lite_server import Callback\n"
            "class LoggerCB(Callback):\n"
            "    pass\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        class MyAPI(LitAPI):
            callbacks = (_TagCB(tag="sk-123"),)

        api = MyAPI()
        cbs = load_callbacks({"callbacks": ["my_cb.LoggerCB"]}, api)
        assert len(cbs) == 2
        assert isinstance(cbs[0], _TagCB)

    def test_class_attr_validated(self):
        """Old-style hooks on class-attribute callbacks are loud errors."""
        from lite_server.api import LitAPI

        class BadCB(Callback):
            def on_before_decode(self, request, meta):
                return request

        class MyAPI(LitAPI):
            callbacks = (BadCB(),)

        api = MyAPI()
        with pytest.raises(RuntimeError, match="on_before_decode"):
            load_callbacks({}, api)


class TestRemovedPolicyCallbacks:
    @pytest.mark.parametrize(
        "path",
        [
            "lite_server.callbacks.RequireApiKey",
            "lite_server.callbacks.Cors",
            "lite_server.callbacks.RateLimit",
            "lite_server.callbacks.LogRequests",
            "my_package.auth.RequireApiKey",
        ],
    )
    def test_removed_policy_callback_is_loud_with_migration_hint(self, path):
        with pytest.raises(RuntimeError, match="removed in 0.7.6"):
            load_callbacks({"callbacks": [path]})
