"""Audit tests for the early-return (ctx.early) mechanism.

Originally written to demonstrate confirmed defects (B1: header priority
inversion between finalize() and _build_route_response(); B2: bidi on_chunk
early return skipping on_close).  Both defects are now fixed — these tests
pass and serve as regression coverage.
"""

import pytest

from lite_server.api import LitAPI
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.pipeline import Pipeline
from lite_server.response import Response


def _make_meta(route="/predict"):
    return RequestMeta(
        route=route,
        headers=Headers({"content-type": "application/json"}),
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=123456789,
    )


class _DummyAPI(LitAPI):
    def decode_request(self, request):
        return request

    def predict(self, x):
        return x

    def encode_response(self, output):
        return output


# ===========================================================================
# B1 — Header merge priority inverted between Pipeline.finalize() and
#      _build_route_response() when early return carries headers that
#      conflict with ctx.response_headers
# ===========================================================================

class TestAuditB1_HeaderPriorityInversion:
    """Both serialization paths — ``Pipeline.finalize()`` and
    ``_build_route_response()`` — must merge headers with the same
    priority: explicit Response headers win over ambient
    ``ctx.response_headers`` (``finalize``'s documented convention).

    Regression coverage for the former inversion, where
    ``_build_route_response`` let ``ctx.response_headers`` override the
    handler's explicit headers.
    """

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _finalize_headers(ctx: RequestContext) -> dict | None:
        api = _DummyAPI()
        pipe = Pipeline.build(api, [])
        _resp_bytes, _status, _metrics, headers = pipe.finalize(ctx)
        return headers

    @staticmethod
    def _route_headers(ctx: RequestContext) -> dict:
        from lite_server.worker.inference import _build_route_response
        single = _build_route_response("u1", ctx).single
        return dict(single.headers)

    # -- test: both paths agree on header priority ------------------------

    def test_finalize_and_route_agree_on_header_priority(self):
        """Both serialization paths produce the same result for the same
        context: the conflicting key resolves to the explicit early
        Response header value."""
        ctx = RequestContext(meta=_make_meta())
        ctx.response_headers["X-Conflict"] = "from_response_headers"
        ctx.early = Response(
            content={"ok": True},
            headers={"X-Conflict": "from_early_response"},
        )

        f_headers = self._finalize_headers(ctx)
        r_headers = self._route_headers(ctx)

        assert f_headers is not None
        assert f_headers["X-Conflict"] == r_headers["X-Conflict"]
        assert f_headers["X-Conflict"] == "from_early_response"

    def test_route_explicit_early_headers_override_response_headers(self):
        """Explicit headers from the early Response override ambient
        ``ctx.response_headers`` — matching ``finalize``'s documented
        priority ("explicit headers win")."""
        ctx = RequestContext(meta=_make_meta())
        ctx.response_headers["X-Conflict"] = "from_response_headers"
        ctx.early = Response(
            content={"ok": True},
            headers={"X-Conflict": "from_early_response"},
        )

        headers = self._route_headers(ctx)

        assert headers["X-Conflict"] == "from_early_response"


# ===========================================================================
# B2 — Bidi on_chunk early return must call on_close cleanup and drop the
#      session, symmetric with the on_open early return path
# ===========================================================================

class TestAuditB2_BidiOnChunkEarlyClosesSession:
    """When a bidi ``on_chunk`` output triggers early return during
    ``postprocess``, the session must be released exactly like the
    ``on_open`` early path: ``on_close`` runs once and the session is
    removed from ``active_streams`` (so a later client close/cancel cannot
    double-fire ``on_close``).
    """

    @pytest.mark.asyncio
    async def test_on_chunk_early_return_calls_on_close_and_removes_session(self):
        import json
        import logging

        from lite_server.api import BidiStreamHandler
        from lite_server.proto import (
            Request as ProtoRequest,
            StreamChunk,
            StreamClose,
            StreamOpen,
            StreamRequest,
        )
        from lite_server.worker import inference

        log = logging.getLogger("test_audit_b2")
        close_calls = []

        class H(BidiStreamHandler):
            def on_chunk(self, chunk):
                return {"echo": chunk}

            def on_close(self):
                close_calls.append(True)

        class EarlyAPI(LitAPI):
            def decode_request(self, request):
                return request

            def predict(self, x):
                return x

            def encode_response(self, output):
                # Any postprocess after on_chunk yields an early Response.
                return Response(content={"early": True})

            def bidi_stream(self):
                return H()

        class Sock:
            def __init__(self):
                self.sent = []

            async def send(self, data: bytes):
                self.sent.append(data)

        api = EarlyAPI()
        sock = Sock()
        active: dict = {}

        # Open: default on_open returns None → session registered, no output.
        open_req = StreamRequest(stream_id="s-b2", open=StreamOpen(data=b"{}"))
        await inference._handle_stream_open_async(api, open_req, sock, active, log)
        assert "s-b2" in active
        assert close_calls == []

        # Chunk: postprocess triggers early return → on_close runs, session gone.
        chunk_req = ProtoRequest(
            uid="c1",
            stream=StreamRequest(
                stream_id="s-b2",
                chunk=StreamChunk(data=json.dumps({"tok": 1}).encode()),
            ),
        )
        await inference._handle_stream_async(api, chunk_req, sock, active, log)
        assert close_calls == [True], "on_chunk early return must run on_close"
        assert "s-b2" not in active, "session must be removed on early return"

        # A later client close must NOT fire on_close a second time.
        close_req = ProtoRequest(
            uid="c2",
            stream=StreamRequest(stream_id="s-b2", close=StreamClose()),
        )
        await inference._handle_stream_async(api, close_req, sock, active, log)
        assert close_calls == [True], "on_close must fire exactly once"
