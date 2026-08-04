"""Audit verification tests (2026-08-04) — commit 69a0a98 on_error 自定义响应.

Covers the plan (.claude/on-error-custom-response.md §3) scenarios NOT covered
by the committed tests:
  - scenario 5:  decoupled open error + on_error returns Response
  - scenario 11: bidi on_open encode error → extra_cleanup (on_close balance)
                 in BOTH the custom and the None branch (D1)
  - scenario 17: _ResponseSender.send error + custom → closed=True, send no-op,
                 on_error driven exactly once (B 类)
Plus hypothesis probes:
  - unary custom response must still merge ctx.response_headers (finalize path)
  - on_error returning a non-Response value must be ignored (backward compat)
  - decoupled send error after session registration: session is left in
    active_streams and a later server cancel double-fires on_stream_close
    (pre-existing behavior in the refactored helper path — documented here)
"""

import asyncio
import json
import logging

import pytest

from lite_server.api import BidiStreamHandler
from lite_server.callbacks import Callback
from lite_server.context import RequestContext
from lite_server.pipeline import Pipeline
from lite_server.response import Response as LiteResponse
from lite_server.worker import inference
from lite_server.worker.streaming import _ResponseSender

from test_streaming import AsyncSocket, EchoAPI, _decoupled_req, _is_error, _stream_req

log = logging.getLogger("test_audit_onerror")


def _make_meta():
    from lite_server.context import Headers, RequestMeta
    return RequestMeta(
        route="/predict", headers=Headers(), client_ip="127.0.0.1",
        request_id="audit-1", timestamp_ns=0,
    )


class TestAuditScenario5Decoupled:
    """Plan §3-5: decoupled — on_error 返回 Response → StreamChunk+StreamDone."""

    @pytest.mark.asyncio
    async def test_decoupled_open_error_custom_response_sends_chunk_and_done(self):
        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"decoupled_error": True})

        class FailingDecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                raise RuntimeError("decoupled boom")

        api = FailingDecoupledAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _decoupled_req("dc-custom"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("dc-custom")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"decoupled_error": True}
        assert len(dones) == 1
        assert len(errors) == 0


class TestAuditScenario11D1:
    """Plan §3-11 (D1): bidi on_open encode error → extra_cleanup runs in BOTH
    branches — on_close must be balanced exactly once, session must not leak."""

    @pytest.mark.asyncio
    async def test_custom_branch_balances_on_close(self):
        on_close_calls = []
        close_reasons = []

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"encode_err": True})

        class ExplodingEncodeCB(Callback):
            def after_predict(self, ctx):
                raise RuntimeError("encode boom")

        class CloseRecCB(Callback):
            def on_stream_close(self, ctx, reason):
                close_reasons.append(reason)

        class OkHandler(BidiStreamHandler):
            async def on_open(self, data, ctx=None):
                return {"opened": True}

            async def on_chunk(self, data, ctx=None):
                return None

            async def on_close(self, ctx=None):
                on_close_calls.append("close")

        class BidiAPI(EchoAPI):
            def bidi_stream(self, ctx=None):
                return OkHandler()

        api = BidiAPI()
        api._pipeline = Pipeline.build(
            api, [CustomErrorCB(), ExplodingEncodeCB(), CloseRecCB()])
        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            api, _stream_req("b-enc-custom"), sock, active, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("b-enc-custom")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"encode_err": True}
        assert len(dones) == 1
        assert len(errors) == 0
        # D1: on_close balanced exactly once even though a custom response was sent
        assert on_close_calls == ["close"]
        # D1: no half-open session left behind
        assert "b-enc-custom" not in active
        # S2: close reason stays "error"
        assert close_reasons == ["error"]

    @pytest.mark.asyncio
    async def test_none_branch_balances_on_close(self):
        on_close_calls = []

        class NoopErrorCB(Callback):
            def on_error(self, ctx, exc):
                return None

        class ExplodingEncodeCB(Callback):
            def after_predict(self, ctx):
                raise RuntimeError("encode boom")

        class OkHandler(BidiStreamHandler):
            async def on_open(self, data, ctx=None):
                return {"opened": True}

            async def on_chunk(self, data, ctx=None):
                return None

            async def on_close(self, ctx=None):
                on_close_calls.append("close")

        class BidiAPI(EchoAPI):
            def bidi_stream(self, ctx=None):
                return OkHandler()

        api = BidiAPI()
        api._pipeline = Pipeline.build(api, [NoopErrorCB(), ExplodingEncodeCB()])
        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            api, _stream_req("b-enc-none"), sock, active, log
        )
        await sock.wait_for(lambda r: _is_error(r, "b-enc-none"))
        errors = [r for r in sock.stream_responses("b-enc-none")
                  if r.stream.HasField("error")]
        assert len(errors) == 1
        assert on_close_calls == ["close"]
        assert "b-enc-none" not in active


class TestAuditScenario17ResponseSender:
    """Plan §3-17 (B 类): _ResponseSender.send 错误 + on_error 返回 Response →
    closed=True 生效，后续 send 为 no-op；on_error 仅驱动一次。"""

    async def _make_sender(self, callbacks, active, sock):
        """A decoupled session that reaches its first send() (open succeeded)."""
        holder = {}

        class DecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                holder["sender"] = sender  # keep the channel open

        api = DecoupledAPI()
        api._pipeline = Pipeline.build(api, callbacks)
        await inference._handle_stream_open_async(
            api, _decoupled_req("dc-send"), sock, active, log
        )
        return api, holder["sender"]

    @pytest.mark.asyncio
    async def test_send_error_custom_closes_sender_and_isolates(self):
        on_error_calls = []

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                on_error_calls.append(exc)
                return LiteResponse(content={"send_err": True})

        class ExplodingEncodeCB(Callback):
            def after_predict(self, ctx):
                raise RuntimeError("encode boom")

        sock = AsyncSocket()
        active = {}
        api, sender = await self._make_sender(
            [CustomErrorCB(), ExplodingEncodeCB()], active, sock)
        assert "dc-send" in active

        await sender.send({"x": 1})

        responses = sock.stream_responses("dc-send")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"send_err": True}
        assert len(dones) == 1
        assert len(errors) == 0
        # B 类状态位：closed=True, subsequent send is a no-op
        assert sender.closed is True
        sent_before = len(sock.sent)
        await sender.send({"x": 2})
        await sender.close()
        assert len(sock.sent) == sent_before
        # on_error driven exactly once for the single failed send
        assert len(on_error_calls) == 1

    @pytest.mark.asyncio
    async def test_send_error_none_sends_stream_error_and_closes(self):
        class NoopErrorCB(Callback):
            def on_error(self, ctx, exc):
                return None

        class ExplodingEncodeCB(Callback):
            def after_predict(self, ctx):
                raise RuntimeError("encode boom")

        sock = AsyncSocket()
        active = {}
        _api, sender = await self._make_sender(
            [NoopErrorCB(), ExplodingEncodeCB()], active, sock)

        await sender.send({"x": 1})

        responses = sock.stream_responses("dc-send")
        errors = [r for r in responses if r.stream.HasField("error")]
        dones = [r for r in responses if r.stream.HasField("done")]
        assert len(errors) == 1
        assert len(dones) == 0
        assert sender.closed is True


class TestAuditHypotheses:
    """Hypothesis probes on the new unary path."""

    @pytest.mark.asyncio
    async def test_unary_custom_response_still_merges_ctx_response_headers(self):
        """run_single custom branch skips e._response_headers threading; the
        headers accumulated before the failure must survive via finalize's
        merge — otherwise Cors-style headers are lost on overridden errors."""
        class CorsCB(Callback):
            def before_decode_request(self, ctx):
                ctx.response_headers["Access-Control-Allow-Origin"] = "*"

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(
                    content={"e": 1}, status_code=500,
                    headers={"X-Custom": "yes"})

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [CorsCB(), CustomErrorCB()])
        _body, _status, _metrics, headers = await pipe.run_single(b"{}", _make_meta())
        assert headers is not None
        assert headers.get("Access-Control-Allow-Origin") == "*"
        assert headers.get("X-Custom") == "yes"

    @pytest.mark.asyncio
    async def test_on_error_returning_non_response_is_ignored(self):
        """A hook returning a non-Response (e.g. a dict) must NOT override —
        the original exception propagates (backward compat)."""
        class DictCB(Callback):
            def on_error(self, ctx, exc):
                return {"not": "a response"}

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("boom")

        pipe = Pipeline.build(FailingAPI(), [DictCB()])
        with pytest.raises(ValueError, match="boom"):
            await pipe.run_single(b"{}", _make_meta())

    @pytest.mark.asyncio
    async def test_decoupled_send_error_close_reason_not_double_fired(self):
        """After a send error terminates a registered decoupled session, the
        stream is terminal (close 'error' already fired). The session must be
        reclaimed so the server's cleanup cancel (the gRPC DecoupledInfer
        handler ALWAYS sends one, src/grpc/rpc/decoupled.rs:287) cannot fire
        on_stream_close a second time for the same ctx."""
        close_reasons = []

        class NoopErrorCB(Callback):
            def on_error(self, ctx, exc):
                return None

        class CloseRecCB(Callback):
            def on_stream_close(self, ctx, reason):
                close_reasons.append(reason)

        class ExplodingEncodeCB(Callback):
            def after_predict(self, ctx):
                raise RuntimeError("encode boom")

        sock = AsyncSocket()
        active = {}
        holder = {}

        class DecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                holder["sender"] = sender

        api = DecoupledAPI()
        api._pipeline = Pipeline.build(
            api, [NoopErrorCB(), CloseRecCB(), ExplodingEncodeCB()])
        await inference._handle_stream_open_async(
            api, _decoupled_req("dc-dbl"), sock, active, log
        )
        assert "dc-dbl" in active

        await holder["sender"].send({"x": 1})
        assert close_reasons == ["error"]
        # The terminal error must reclaim the session up front.
        assert "dc-dbl" not in active

        # Simulate the server-side cleanup cancel that always follows.
        from lite_server.proto import Request, StreamCancel, StreamRequest
        cancel_req = Request(stream=StreamRequest(
            stream_id="dc-dbl", cancel=StreamCancel()))
        await inference._handle_stream_async(api, cancel_req, sock, active, log)
        assert close_reasons == ["error"], (
            f"on_stream_close fired more than once: {close_reasons}"
        )

    @pytest.mark.asyncio
    async def test_bidi_on_chunk_error_close_reason_not_double_fired(self):
        """Same structural issue via the bidi path: on_chunk error terminates
        the stream (close 'error') but leaves the _BidiSession registered, so
        the server's cleanup cancel double-fires on_stream_close. Reclaiming
        must keep on_close balanced exactly once (the cancel path used to run
        it — after reclaim, extra_cleanup must)."""
        close_reasons = []
        on_close_calls = []

        class NoopErrorCB(Callback):
            def on_error(self, ctx, exc):
                return None

        class CloseRecCB(Callback):
            def on_stream_close(self, ctx, reason):
                close_reasons.append(reason)

        class ExplodingChunkHandler(BidiStreamHandler):
            async def on_open(self, data, ctx=None):
                return None

            async def on_chunk(self, data, ctx=None):
                raise RuntimeError("chunk boom")

            async def on_close(self, ctx=None):
                on_close_calls.append("close")
                return None

        class BidiAPI(EchoAPI):
            def bidi_stream(self, ctx=None):
                return ExplodingChunkHandler()

        api = BidiAPI()
        api._pipeline = Pipeline.build(api, [NoopErrorCB(), CloseRecCB()])
        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            api, _stream_req("b-dbl"), sock, active, log
        )
        assert "b-dbl" in active

        from lite_server.proto import (
            Request, StreamCancel, StreamChunk, StreamRequest,
        )
        chunk_req = Request(stream=StreamRequest(
            stream_id="b-dbl", chunk=StreamChunk(data=b"{}")))
        await inference._handle_stream_async(api, chunk_req, sock, active, log)
        assert close_reasons == ["error"]
        # Terminal error reclaims the session up front…
        assert "b-dbl" not in active
        # …and on_close stays balanced exactly once (D1-style extra_cleanup).
        assert on_close_calls == ["close"]

        cancel_req = Request(stream=StreamRequest(
            stream_id="b-dbl", cancel=StreamCancel()))
        await inference._handle_stream_async(api, cancel_req, sock, active, log)
        assert close_reasons == ["error"], (
            f"on_stream_close fired more than once: {close_reasons}"
        )
        assert on_close_calls == ["close"], (
            f"on_close fired more than once: {on_close_calls}"
        )
