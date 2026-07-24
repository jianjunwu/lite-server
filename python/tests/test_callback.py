"""Tests for Callback base, RequestContext, load_callbacks, builtin callbacks,
and migration errors."""

import os
import time

import pytest

from lite_server.context import Headers, RequestMeta
from lite_server.callback import (
    Callback,
    Cors,
    LogRequests,
    RateLimit,
    RequireApiKey,
    RequestContext,
    _TokenBucket,
    extract_policies,
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


class TestTokenBucket:
    def test_acquire_succeeds_when_tokens_available(self):
        tb = _TokenBucket(rate=10.0, capacity=10.0)
        assert tb.acquire() is True

    def test_acquire_fails_when_exhausted(self):
        tb = _TokenBucket(rate=0.0, capacity=1.0)
        assert tb.acquire() is True
        assert tb.acquire() is False

    def test_refill_over_time(self):
        tb = _TokenBucket(rate=100.0, capacity=5.0)
        # Consume all tokens
        for _ in range(5):
            assert tb.acquire() is True
        assert tb.acquire() is False
        # Wait for refill
        time.sleep(0.02)  # 100 tokens/s → 2 tokens in 0.02s
        assert tb.acquire() is True

    def test_burst_capacity_upper_bound(self):
        tb = _TokenBucket(rate=1000.0, capacity=5.0)
        time.sleep(0.1)  # would be 100 tokens without cap
        for _ in range(5):
            assert tb.acquire() is True
        assert tb.acquire() is False


# ---------------------------------------------------------------------------
# RequireApiKey
# ---------------------------------------------------------------------------


class TestRequireApiKey:
    def _ctx(self, headers=None):
        return RequestContext(
            meta=RequestMeta(
                route="/predict",
                headers=Headers(headers or {}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
            )
        )

    def test_missing_header_raises_unauthorized(self):
        cb = RequireApiKey(header="X-API-Key", keys=["sk-123"])
        with pytest.raises(UnauthorizedError, match="missing API key"):
            cb.on_request(self._ctx())

    def test_wrong_key_raises_unauthorized(self):
        cb = RequireApiKey(header="X-API-Key", keys=["sk-123"])
        with pytest.raises(UnauthorizedError, match="invalid API key"):
            cb.on_request(self._ctx({"X-API-Key": "wrong"}))

    def test_correct_key_passes(self):
        cb = RequireApiKey(header="X-API-Key", keys=["sk-123"])
        cb.on_request(self._ctx({"X-API-Key": "sk-123"}))  # no raise

    def test_empty_keys_allows_any_nonempty_value(self):
        cb = RequireApiKey(header="Authorization")
        cb.on_request(self._ctx({"Authorization": "Bearer xxx"}))  # no raise

    def test_empty_keys_rejects_empty_value(self):
        cb = RequireApiKey(header="Authorization")
        with pytest.raises(UnauthorizedError):
            cb.on_request(self._ctx({"Authorization": ""}))


# ---------------------------------------------------------------------------
# RateLimit
# ---------------------------------------------------------------------------


class TestRateLimit:
    def _ctx(self, route="/predict", client_ip="127.0.0.1"):
        return RequestContext(
            meta=RequestMeta(
                route=route,
                headers=Headers({}),
                client_ip=client_ip,
                request_id="r1",
                timestamp_ns=1,
            )
        )

    def test_declaration_fields(self):
        cb = RateLimit(requests_per_minute=120, key="ip", burst=10.0)
        assert cb.requests_per_minute == 120
        assert cb.key == "ip"
        assert cb.burst == 10.0

    def test_burst_defaults_to_1_5x_rpm(self):
        cb = RateLimit(requests_per_minute=60)
        assert cb.burst == 90.0

    def test_invalid_key_raises(self):
        with pytest.raises(ValueError, match="key"):
            RateLimit(key="host")

    def test_fallback_allows_under_limit(self):
        cb = RateLimit(requests_per_minute=6000)  # 100/s
        for _ in range(10):
            cb.on_request(self._ctx())  # no raise

    def test_fallback_rejects_over_limit(self):
        cb = RateLimit(requests_per_minute=6, burst=1.0)  # 0.1/s, 1 burst
        cb.on_request(self._ctx())  # consume burst token
        with pytest.raises(HTTPException) as exc_info:
            cb.on_request(self._ctx())
        assert exc_info.value.status_code == 429
        assert exc_info.value.headers is not None
        assert "Retry-After" in exc_info.value.headers

    def test_fallback_buckets_by_route(self):
        cb = RateLimit(requests_per_minute=6, key="route", burst=1.0)
        cb.on_request(self._ctx(route="/a"))
        # /a exhausted, /b should still work
        cb.on_request(self._ctx(route="/b"))  # no raise

    def test_fallback_buckets_by_ip(self):
        cb = RateLimit(requests_per_minute=6, key="ip", burst=1.0)
        cb.on_request(self._ctx(client_ip="10.0.0.1"))
        with pytest.raises(HTTPException):
            cb.on_request(self._ctx(client_ip="10.0.0.1"))
        cb.on_request(self._ctx(client_ip="10.0.0.2"))  # different IP, no raise

    def test_managed_mode_noop(self, monkeypatch):
        monkeypatch.setenv("LITE_POLICY_MANAGED", "1")
        cb = RateLimit(requests_per_minute=1, burst=0.0)  # would fail immediately
        cb.on_request(self._ctx())  # no raise — Rust handles it
        cb.on_request(self._ctx())  # still no raise


# ---------------------------------------------------------------------------
# LogRequests
# ---------------------------------------------------------------------------


class TestLogRequests:
    def _ctx(self, route="/predict", method="POST"):
        return RequestContext(
            meta=RequestMeta(
                route=route,
                headers=Headers({}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
                method=method,
            )
        )

    def test_on_request_stores_start_time(self):
        cb = LogRequests()
        ctx = self._ctx()
        cb.on_request(ctx)
        key = f"_logreq_start_{id(cb)}"
        assert key in ctx.state
        assert isinstance(ctx.state[key], float)

    def test_on_response_logs_info(self, caplog):
        import logging
        caplog.set_level(logging.INFO)
        logging.getLogger("lite_server.requests").setLevel(logging.INFO)
        cb = LogRequests()
        ctx = self._ctx()
        cb.on_request(ctx)
        cb.on_response(ctx)
        log_records = [r for r in caplog.records if r.name == "lite_server.requests"]
        assert len(log_records) == 1, f"records: {[(r.name, r.getMessage()) for r in caplog.records]}"
        assert "POST" in log_records[0].getMessage()
        assert "/predict" in log_records[0].getMessage()
        assert "200" in log_records[0].getMessage()

    def test_on_error_logs_401(self, caplog):
        import logging
        caplog.set_level(logging.INFO)
        logging.getLogger("lite_server.requests").setLevel(logging.INFO)
        from lite_server.exceptions import UnauthorizedError
        cb = LogRequests()
        ctx = self._ctx()
        cb.on_request(ctx)
        cb.on_error(ctx, UnauthorizedError("bad"))
        log_records = [r for r in caplog.records if r.name == "lite_server.requests"]
        assert len(log_records) == 1
        assert "401" in log_records[0].getMessage()

    def test_on_error_logs_500_for_unknown_exception(self, caplog):
        import logging
        caplog.set_level(logging.INFO)
        logging.getLogger("lite_server.requests").setLevel(logging.INFO)
        cb = LogRequests()
        ctx = self._ctx()
        cb.on_request(ctx)
        cb.on_error(ctx, RuntimeError("boom"))
        log_records = [r for r in caplog.records if r.name == "lite_server.requests"]
        assert len(log_records) == 1
        assert "500" in log_records[0].getMessage()

    def test_two_instances_no_state_collision(self):
        cb1 = LogRequests()
        cb2 = LogRequests()
        ctx = self._ctx()
        cb1.on_request(ctx)
        k1 = f"_logreq_start_{id(cb1)}"
        k2 = f"_logreq_start_{id(cb2)}"
        assert k1 in ctx.state
        assert k2 not in ctx.state


# ---------------------------------------------------------------------------
# Cors
# ---------------------------------------------------------------------------


class TestCors:
    def _ctx(self, method="POST"):
        return RequestContext(
            meta=RequestMeta(
                route="/predict",
                headers=Headers({}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
                method=method,
            )
        )

    def test_fallback_stashes_response_headers(self):
        cb = Cors(allow_origins=["https://app.example.com"])
        ctx = self._ctx()
        cb.on_request(ctx)
        assert ctx.early is None  # NOT an early return
        assert "Access-Control-Allow-Origin" in ctx.response_headers
        assert ctx.response_headers["Access-Control-Allow-Origin"] == "https://app.example.com"

    def test_fallback_options_returns_204_with_headers(self):
        cb = Cors()
        ctx = self._ctx(method="OPTIONS")
        cb.on_request(ctx)
        assert ctx.early is not None
        assert ctx.early.status_code == 204
        assert "Access-Control-Allow-Origin" in ctx.early.headers

    def test_fallback_post_does_not_early_return(self):
        cb = Cors()
        ctx = self._ctx(method="POST")
        cb.on_request(ctx)
        assert ctx.early is None

    def test_managed_mode_noop(self, monkeypatch):
        monkeypatch.setenv("LITE_POLICY_MANAGED", "1")
        cb = Cors()
        ctx = self._ctx(method="OPTIONS")
        cb.on_request(ctx)
        assert ctx.early is None
        assert ctx.response_headers == {}

    def test_default_values(self):
        cb = Cors()
        assert cb.allow_origins == ["*"]
        assert "GET" in cb.allow_methods
        assert "POST" in cb.allow_methods


# ---------------------------------------------------------------------------
# on_error hook validation
# ---------------------------------------------------------------------------


class TestOnErrorHook:
    def test_valid_on_error_passes_validation(self):
        class WithError(Callback):
            def on_error(self, ctx, exc):
                pass

        validate_callback(WithError())  # must not raise

    def test_on_error_bad_arity_raises(self):
        class BadError(Callback):
            def on_error(self, ctx):
                pass

        with pytest.raises(RuntimeError, match="on_error"):
            validate_callback(BadError())


# ---------------------------------------------------------------------------
# extract_policies
# ---------------------------------------------------------------------------


class TestExtractPolicies:
    def test_empty_list_returns_empty(self):
        assert extract_policies([]) == {}

    def test_extracts_rate_limit(self):
        cb = RateLimit(requests_per_minute=120, key="ip", burst=200.0)
        policies = extract_policies([cb])
        assert policies["rate_limit"] == {
            "requests_per_minute": 120,
            "key": "ip",
            "burst": 200.0,
        }

    def test_extracts_cors(self):
        cb = Cors(allow_origins=["https://a.com"], allow_methods=["GET"])
        policies = extract_policies([cb])
        assert policies["cors"]["allow_origins"] == ["https://a.com"]
        assert policies["cors"]["allow_methods"] == ["GET"]

    def test_both_policies_extracted(self):
        policies = extract_policies([
            RateLimit(requests_per_minute=60),
            Cors(allow_origins=["*"]),
        ])
        assert "rate_limit" in policies
        assert "cors" in policies

    def test_last_declaration_wins(self):
        policies = extract_policies([
            RateLimit(requests_per_minute=60),
            RateLimit(requests_per_minute=120),
        ])
        assert policies["rate_limit"]["requests_per_minute"] == 120

    def test_ignores_other_callbacks(self):
        policies = extract_policies([
            RequireApiKey(keys=["x"]),
            LogRequests(),
        ])
        assert policies == {}


# ---------------------------------------------------------------------------
# load_callbacks with LitAPI class attribute
# ---------------------------------------------------------------------------


class TestLoadCallbacksWithLitAPI:
    def test_class_attribute_priority(self, tmp_path, monkeypatch):
        """LitAPI.callbacks take priority and support constructor args."""
        from lite_server.api import LitAPI

        class MyAPI(LitAPI):
            callbacks = (RequireApiKey(keys=["sk-123"]),)

        api = MyAPI()
        cbs = load_callbacks({}, api)
        assert len(cbs) == 1
        assert isinstance(cbs[0], RequireApiKey)

    def test_class_attr_and_yaml_merged(self, tmp_path, monkeypatch):
        from lite_server.api import LitAPI

        (tmp_path / "my_cb.py").write_text(
            "from lite_server import Callback\n"
            "class LoggerCB(Callback):\n"
            "    pass\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        class MyAPI(LitAPI):
            callbacks = (RequireApiKey(keys=["sk-123"]),)

        api = MyAPI()
        cbs = load_callbacks({"callbacks": ["my_cb.LoggerCB"]}, api)
        assert len(cbs) == 2
        assert isinstance(cbs[0], RequireApiKey)

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
