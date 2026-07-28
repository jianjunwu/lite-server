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


class TestValidateCallback:
    def test_new_style_callback_passes(self):
        class Good(Callback):
            def on_request(self, ctx):
                pass

            def on_output(self, ctx):
                pass

        validate_callback(Good())  # must not raise

    def test_base_callback_passes(self):
        validate_callback(Callback())

    @pytest.mark.parametrize(
        "old_name,new_name",
        [
            ("on_before_decode", "on_request"),
            ("on_after_decode", "on_input"),
            ("on_before_predict", "on_input"),
            ("on_after_predict", "on_output"),
            ("on_before_encode", "on_output"),
            ("on_after_encode", "on_response"),
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

    def test_wrong_arity_new_hook_raises(self):
        class Bad(Callback):
            def on_request(self, ctx, meta):  # old two-arg shape on a new name
                pass

        with pytest.raises(RuntimeError, match="on_request"):
            validate_callback(Bad())

    def test_lifecycle_hooks_unaffected(self):
        class LC(Callback):
            def on_before_setup(self, config, device):
                pass

            def on_after_setup(self, lit_api):
                pass

            def on_teardown(self, lit_api):
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
            "    def on_request(self, ctx):\n"
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
