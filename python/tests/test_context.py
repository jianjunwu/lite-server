"""Unit tests for lite_server.context — RequestMeta, RequestContext, CBSequence, Headers."""

import dataclasses

import pytest

from lite_server.context import CBSequence, Headers, RequestContext, RequestMeta
from lite_server.response import Response


def _make_meta(**overrides):
    kwargs = dict(
        route="/predict",
        headers=Headers({"content-type": "application/json"}),
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=123456789,
    )
    kwargs.update(overrides)
    return RequestMeta(**kwargs)


# ============================================================================
# RequestMeta
# ============================================================================


class TestRequestMeta:
    def test_meta_frozen_blocks_assignment(self):
        meta = _make_meta()
        with pytest.raises(dataclasses.FrozenInstanceError):
            meta.route = "/other"

    def test_meta_headers_case_insensitive(self):
        meta = _make_meta(headers=Headers({"Content-Type": "text/plain", "X-Auth": "tok"}))
        assert meta.headers.get("content-type") == "text/plain"
        assert meta.headers.get("x-auth") == "tok"
        assert meta.headers["content-type"] == "text/plain"

    def test_meta_has_no_payload_field(self):
        meta = _make_meta()
        assert not hasattr(meta, "payload")


# ============================================================================
# Headers
# ============================================================================


class TestHeaders:
    def test_case_insensitive_lookup(self):
        h = Headers({"Content-Type": "application/json"})
        assert h.get("content-type") == "application/json"
        assert h["content-type"] == "application/json"
        assert "content-type" in h

    def test_missing_key_returns_default(self):
        h = Headers()
        assert h.get("x-missing") is None
        assert h.get("x-missing", "fallback") == "fallback"

    def test_missing_key_raises_keyerror(self):
        h = Headers()
        with pytest.raises(KeyError):
            _ = h["x-missing"]

    def test_items_returns_first_value_per_key(self):
        h = Headers({"a": "1", "b": "2"})
        items = dict(h.items())
        assert items == {"a": "1", "b": "2"}

    def test_getlist_returns_all_values(self):
        h = Headers()
        h._data["x"] = ["a", "b"]
        assert h.getlist("x") == ["a", "b"]
        assert h.getlist("X") == ["a", "b"]

    def test_empty_headers(self):
        h = Headers()
        assert h.get("anything") is None
        assert h.items() == []
        assert h.keys() == []
        assert h.values() == []

    def test_none_raw(self):
        h = Headers(None)
        assert h.get("anything") is None

    def test_repr(self):
        h = Headers({"A": "1"})
        assert "Headers" in repr(h)


# ============================================================================
# RequestContext
# ============================================================================


class TestRequestContext:
    def test_respond_sets_early_with_status_and_headers(self):
        ctx = RequestContext(meta=_make_meta())
        ctx.respond({"err": "x"}, status_code=403, headers={"X-A": "1"})
        assert ctx.early is not None
        assert ctx.early.content == {"err": "x"}
        assert ctx.early.status_code == 403
        assert ctx.early.headers == {"X-A": "1"}

    def test_respond_defaults(self):
        ctx = RequestContext(meta=_make_meta())
        r = ctx.respond({"ok": True})
        assert r.status_code == 200
        assert r.media_type == "application/json"
        assert r.headers == {}

    def test_respond_custom_media_type(self):
        ctx = RequestContext(meta=_make_meta())
        r = ctx.respond("plain", media_type="text/plain")
        assert r.media_type == "text/plain"

    def test_state_defaults_to_empty_dict(self):
        ctx = RequestContext(meta=_make_meta())
        assert ctx.state == {}
        ctx.state["k"] = 1
        assert ctx.state["k"] == 1

    def test_meta_cannot_be_none(self):
        with pytest.raises(TypeError):
            RequestContext(meta=None)  # type: ignore

    def test_default_field_values(self):
        ctx = RequestContext(meta=_make_meta())
        assert ctx.request is None
        assert ctx.input is None
        assert ctx.output is None
        assert ctx.response is None
        assert ctx.early is None


# ============================================================================
# CBSequence
# ============================================================================


class TestCBSequence:
    def test_cbsequence_exposes_state_as_ctx_state(self):
        meta = _make_meta()
        ctx = RequestContext(meta=meta)
        ctx.state["k"] = 42

        seq = CBSequence(uid="uid-1", ctx=ctx)
        assert seq.state is ctx.state
        assert seq.state["k"] == 42

        # Mutating via seq.state is visible via ctx.state
        seq.state["new_key"] = "v"
        assert ctx.state["new_key"] == "v"

    def test_cbsequence_attributes(self):
        meta = _make_meta()
        ctx = RequestContext(meta=meta)
        ctx.input = {"prompt": "hello"}

        seq = CBSequence(uid="uid-1", ctx=ctx)
        assert seq.uid == "uid-1"
        assert seq.ctx is ctx
        assert seq.input is ctx.input
        assert seq.output == []
        assert seq.meta is ctx.meta
        assert seq.prefilled is False

    def test_cbsequence_has_no_dict_access(self):
        meta = _make_meta()
        ctx = RequestContext(meta=meta)
        seq = CBSequence(uid="uid-1", ctx=ctx)

        # CBSequence is NOT a dict — attribute access only
        with pytest.raises(TypeError):
            _ = seq["uid"]  # type: ignore
