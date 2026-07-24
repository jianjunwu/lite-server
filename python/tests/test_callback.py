"""Tests for Callback base, RequestContext, load_callbacks, and migration errors."""

import pytest

from lite_server.context import Headers, RequestMeta
from lite_server.callback import (
    Callback,
    RequestContext,
    load_callbacks,
    validate_callback,
)


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
