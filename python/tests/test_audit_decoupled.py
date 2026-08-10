"""Audit evidence tests — P9-1 DecoupledInfer 1:N worker side (蓝图 §4.4).

Each test FAILS on current code, demonstrating a confirmed defect found by
the targeted audit of ``lite_server.worker.streaming``'s decoupled path.

Contract under test (``Callback.on_stream_close`` docstring,
callbacks/_base.py:152): "Called **once** when a stream terminates
(stream/bidi/decoupled paths)" with reason "done" / "error" / "cancel".

Defect summary:
- B3 (concurrency): a server cancel landing while ``_ResponseSender.close()``
  is suspended inside the terminal hook makes on_stream_close fire TWICE
  ("done" from close(), then "cancel" from the cancel path).
- B4 (control flow): the decoupled preprocess early-return path ends the
  stream without firing on_stream_close at all — the uni-stream and bidi
  early paths both fire reason="done".
- B5 (control flow): worker shutdown reclaims an open decoupled session via
  ``sender.cancel()`` but never fires on_stream_close; the message-cancel
  path (same file) and the uni-stream shutdown path both fire "cancel".
"""

import asyncio
import json
import logging

import pytest

from lite_server.api import LitAPI
from lite_server.callbacks import Callback
from lite_server.pipeline import Pipeline
from lite_server.proto import (
    Request,
    Response,
    StopRequest,
    StreamCancel,
    StreamOpen,
    StreamRequest,
)
from lite_server.worker import inference

log = logging.getLogger("test_audit_decoupled")


class AsyncSocket:
    """Async fake socket capturing sent Response protos (test_streaming.py convention)."""

    def __init__(self):
        self.sent: list[Response] = []
        self._event = asyncio.Event()

    async def send(self, data: bytes):
        resp = Response()
        resp.ParseFromString(data)
        self.sent.append(resp)
        self._event.set()

    def stream_responses(self, stream_id: str) -> list[Response]:
        return [
            r for r in self.sent
            if r.HasField("stream") and r.stream.stream_id == stream_id
        ]


class _LoopSocket:
    """Fake zmq.asyncio socket feeding scripted requests for run_async_loop."""

    def __init__(self, script):
        self.script = list(script)
        self.sent: list[Response] = []

    async def recv(self):
        await asyncio.sleep(0)
        if self.script:
            return self.script.pop(0)
        import zmq

        raise zmq.ZMQError(zmq.ETERM)

    async def send(self, data: bytes):
        resp = Response()
        resp.ParseFromString(data)
        self.sent.append(resp)


class EchoAPI(LitAPI):
    def setup(self, device):
        pass

    def predict(self, x):
        return x


def _decoupled_req(stream_id: str, data: bytes = b"{}") -> StreamRequest:
    """A StreamOpen carrying the additive P9-1 `decoupled=true` flag."""
    return StreamRequest(stream_id=stream_id, open=StreamOpen(data=data, decoupled=True))


class TestAuditDecoupledStreamCloseContract:
    @pytest.mark.asyncio
    async def test_audit_concurrency_cancel_during_close_double_fires_hook(self):
        """B3: cancel landing mid-close fires on_stream_close twice.

        _ResponseSender.close() sets closed=True and then AWAITS the terminal
        hook (model cleanup commonly awaits I/O). While it is suspended, the
        recv loop processes the server's StreamCancel: the cancel path pops
        the session and fires on_stream_close("cancel"); close() then resumes
        and fires on_stream_close("done") — two hook calls for one stream.
        """
        calls: list[str] = []
        release = asyncio.Event()

        class CloseHook(Callback):
            async def on_stream_close(self, ctx, reason):
                calls.append(reason)
                if reason == "done":
                    await release.wait()  # model hook doing async cleanup I/O

        class DecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                return  # background-lifetime stream; session stays registered

        api = DecoupledAPI()
        api._pipeline = Pipeline.build(api, [CloseHook()])
        sock = AsyncSocket()
        active: dict = {}
        await inference._handle_stream_open_async(
            api, _decoupled_req("s-audit-race"), sock, active, log
        )
        assert "s-audit-race" in active
        sender = active["s-audit-race"].sender

        # Model's delayed close() (e.g. from its background push task).
        close_task = asyncio.create_task(sender.close())
        await asyncio.sleep(0.05)  # let close() park inside the "done" hook
        assert calls == ["done"]

        # Server cancel (client disconnect) is processed while close() is suspended.
        cancel_req = Request(
            stream=StreamRequest(stream_id="s-audit-race", cancel=StreamCancel())
        )
        await inference._handle_stream_async(api, cancel_req, sock, active, log)

        release.set()
        await close_task

        assert len(calls) == 1, (
            "on_stream_close fired more than once for one stream "
            f"(reasons={calls}): a cancel landing while close() was suspended "
            "in the terminal hook re-fires the hook — violates the 'Called "
            "once when a stream terminates' contract (Callback.on_stream_close)."
        )

    @pytest.mark.asyncio
    async def test_audit_control_flow_early_return_skips_on_stream_close(self):
        """B4: decoupled preprocess early-return never fires on_stream_close.

        streaming.py:451-454 sends the early chunk + StreamDone and returns
        without run_on_stream_close — the only early path with that gap
        (uni-stream streaming.py:605-608 and bidi :551-555 both fire "done").
        """
        calls: list[str] = []

        class CacheCB(Callback):
            def before_decode_request(self, ctx):
                ctx.respond({"cached": True})

        class CloseHook(Callback):
            def on_stream_close(self, ctx, reason):
                calls.append(reason)

        class DecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                raise AssertionError("must not run — preprocess early-returns")

        api = DecoupledAPI()
        api._pipeline = Pipeline.build(api, [CacheCB(), CloseHook()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _decoupled_req("s-audit-early", b'{"x": 1}'), sock, {}, log
        )
        responses = sock.stream_responses("s-audit-early")
        # The stream DID terminate: early chunk + StreamDone on the wire.
        assert len(responses) == 2
        assert json.loads(responses[0].stream.chunk.data) == {"cached": True}
        assert responses[1].stream.HasField("done")
        assert calls == ["done"], (
            "decoupled preprocess early-return terminated the stream without "
            "firing on_stream_close; uni-stream/bidi early paths fire "
            "reason='done'. Hook calls: " + str(calls)
        )

    @pytest.mark.asyncio
    async def test_audit_control_flow_shutdown_reclaim_skips_on_stream_close(self):
        """B5: worker shutdown reclaims a decoupled session without the hook.

        run_async_loop's shutdown cleanup calls sender.cancel() + pop for
        _DecoupledSession but never run_on_stream_close — the message-cancel
        path for the same session type fires "cancel" (streaming.py:393-397),
        and a uni-stream task cancelled at shutdown fires "cancel" via its
        CancelledError handler.
        """
        calls: list[str] = []

        class CloseHook(Callback):
            def on_stream_close(self, ctx, reason):
                calls.append(reason)

        class IdleDecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                return  # model holds the channel open; never closes

        api = IdleDecoupledAPI()
        api._pipeline = Pipeline.build(api, [CloseHook()])
        script = [
            Request(
                uid="open-s",
                stream=StreamRequest(
                    stream_id="s-audit-shutdown",
                    open=StreamOpen(data=b"{}", decoupled=True),
                ),
            ).SerializeToString(),
            Request(uid="stop", stop=StopRequest()).SerializeToString(),
        ]
        sock = _LoopSocket(script)
        await inference.run_async_loop(api, sock, "test-model", log)
        assert calls == ["cancel"], (
            "worker shutdown reclaimed an open decoupled session without "
            "firing on_stream_close; the message-cancel path fires "
            "reason='cancel' for the same session type. Hook calls: "
            + str(calls)
        )

    @pytest.mark.asyncio
    async def test_audit_decoupled_early_send_emits_two_stream_done_frames(self):
        """_ResponseSender.send() early path emits TWO StreamDone frames.

        streaming.py:220-226: when postprocess sets ctx.early, send() calls
        ``_send_stream_early`` (which sends an early chunk + StreamDone) and
        then ``self.close()`` — close() is not gated on the early branch and
        sends a SECOND StreamDone. The terminal frame must be emitted exactly
        once per stream; the duplicate is dropped by the Rust actor with a
        "Received response for unknown uid" warn per stream.
        """
        class EarlyCB(Callback):
            def after_encode_response(self, ctx):
                ctx.respond({"early": True})

        class DecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                await sender.send({"token": 1})

        api = DecoupledAPI()
        api._pipeline = Pipeline.build(api, [EarlyCB()])
        sock = AsyncSocket()
        active: dict = {}
        await inference._handle_stream_open_async(
            api, _decoupled_req("s-dbl-done", b'{"x": 1}'), sock, active, log
        )
        responses = sock.stream_responses("s-dbl-done")
        done_frames = [r for r in responses if r.stream.HasField("done")]
        assert len(done_frames) == 1, (
            f"expected exactly one StreamDone, got {len(done_frames)}: "
            "send()'s early path calls _send_stream_early (sends Done) and "
            "then close() (sends Done again)"
        )
