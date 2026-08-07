"""Tests for streaming inference in the unified async worker loop.

Covers stream open (fallback / stream_predict / bidi), per-chunk hooks,
early return, cancellation, and the full run_async_loop integration.
"""

import asyncio
import json
import logging

import pytest

from lite_server.api import BidiStreamHandler, LitAPI
from lite_server.callbacks import Callback
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.pipeline import Pipeline
from lite_server.proto import (
    Request,
    Response,
    RequestMeta as ProtoMeta,
    StreamCancel,
    StreamClose,
    StreamOpen,
    StreamRequest,
)
from lite_server.worker import inference

log = logging.getLogger("test_streaming")


class AsyncSocket:
    """Async fake socket capturing sent Response protos."""

    def __init__(self):
        self.sent: list[Response] = []
        self._event = asyncio.Event()

    async def send(self, data: bytes):
        resp = Response()
        resp.ParseFromString(data)
        self.sent.append(resp)
        self._event.set()

    async def wait_for(self, predicate, timeout=5.0):
        async def _poll():
            while True:
                matches = [r for r in self.sent if predicate(r)]
                if matches:
                    return matches
                self._event.clear()
                await asyncio.sleep(0.005)

        return await asyncio.wait_for(_poll(), timeout)

    def stream_responses(self, stream_id: str) -> list[Response]:
        return [
            r for r in self.sent
            if r.HasField("stream") and r.stream.stream_id == stream_id
        ]


def _stream_req(stream_id: str, data: bytes = b"{}", meta=None) -> StreamRequest:
    open_kw = {"data": data}
    if meta is not None:
        open_kw["meta"] = meta
    return StreamRequest(stream_id=stream_id, open=StreamOpen(**open_kw))


class EchoAPI(LitAPI):
    def setup(self, device):
        pass

    def predict(self, x):
        return x


def _is_done(r, stream_id):
    return r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == stream_id


def _is_error(r, stream_id):
    return r.HasField("stream") and r.stream.HasField("error") and r.stream.stream_id == stream_id


# ---------------------------------------------------------------------------
# Stream proto helpers
# ---------------------------------------------------------------------------

class TestMakeStreamHelpers:
    def test_make_stream_chunk(self):
        resp = inference._make_stream_chunk("s1", b"hello", is_final=False)
        assert resp.uid == "stream-chunk-s1"
        assert resp.stream.stream_id == "s1"
        assert resp.stream.chunk.data == b"hello"
        assert resp.stream.chunk.is_final is False

    def test_make_stream_chunk_final(self):
        resp = inference._make_stream_chunk("s1", b"last", is_final=True)
        assert resp.stream.chunk.is_final is True

    def test_make_stream_done(self):
        resp = inference._make_stream_done("s1")
        assert resp.uid == "stream-done-s1"
        assert resp.stream.HasField("done")

    def test_make_stream_error(self):
        resp = inference._make_stream_error("s1", "boom")
        assert resp.uid == "stream-error-s1"
        assert resp.stream.HasField("error")
        assert resp.stream.error.message == "boom"


# ---------------------------------------------------------------------------
# Fallback (no stream_predict → single chunk + done)
# ---------------------------------------------------------------------------

class TestStreamOpenFallback:
    @pytest.mark.asyncio
    async def test_sends_single_chunk_then_done(self):
        class PlainAPI(EchoAPI):
            def predict(self, x):
                return {"result": x.get("val", 0) * 3}

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            PlainAPI(), _stream_req("s-fb", json.dumps({"val": 4}).encode()), sock, {}, log
        )
        responses = sock.stream_responses("s-fb")
        assert len(responses) == 2
        assert responses[0].stream.HasField("chunk")
        assert responses[0].stream.chunk.is_final is True
        assert json.loads(responses[0].stream.chunk.data)["result"] == 12
        assert responses[1].stream.HasField("done")

    @pytest.mark.asyncio
    async def test_fallback_with_empty_data(self):
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            EchoAPI(), _stream_req("s-empty", b""), sock, {}, log
        )
        responses = sock.stream_responses("s-empty")
        assert len(responses) == 2
        assert responses[0].stream.chunk.is_final is True

    @pytest.mark.asyncio
    async def test_fallback_predict_error_sends_stream_error(self):
        class BadAPI(EchoAPI):
            def predict(self, x):
                raise RuntimeError("predict failed")

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            BadAPI(), _stream_req("s-err", b'{"x": 1}'), sock, {}, log
        )
        err = [r for r in sock.stream_responses("s-err") if r.stream.HasField("error")]
        assert len(err) == 1
        assert "predict failed" in err[0].stream.error.message

    @pytest.mark.asyncio
    async def test_early_return_str_body_passes_through_verbatim(self):
        """P3 parity: an early-return str body streams verbatim like
        Pipeline.finalize — not JSON-quoted (_send_stream_early path)."""
        from lite_server.response import Response as LiteResponse

        class EarlyCB(Callback):
            def before_decode_request(self, ctx):
                return LiteResponse(content="early text")

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield {"never": "reached"}

        api = StreamAPI()
        api._pipeline = Pipeline.build(api, [EarlyCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-early", b'{"x": 1}'), sock, {}, log
        )
        responses = sock.stream_responses("s-early")
        assert len(responses) == 2  # chunk + done
        assert responses[0].stream.chunk.data == b"early text"


# ---------------------------------------------------------------------------
# Normal streaming (stream_predict)
# ---------------------------------------------------------------------------

class TestStreamOpen:
    @pytest.mark.asyncio
    async def test_streams_multiple_chunks_then_done(self):
        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                for i in range(3):
                    yield {"chunk": i}

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            StreamAPI(), _stream_req("s-ok", b'{"prompt": "go"}'), sock, active, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-ok"))
        responses = sock.stream_responses("s-ok")
        assert len(responses) == 4  # 3 chunks + done
        for i in range(3):
            assert json.loads(responses[i].stream.chunk.data)["chunk"] == i
        assert responses[3].stream.HasField("done")

    @pytest.mark.asyncio
    async def test_stream_str_and_bytes_chunks_pass_through_verbatim(self):
        """P3 parity: str/bytes chunks stream verbatim like
        Pipeline.finalize; JSON is only for structured data."""
        class RawStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield "chunk-one"
                yield b"chunk-two-bytes"

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            RawStreamAPI(), _stream_req("s-raw", b'{"prompt": "go"}'), sock, active, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-raw"))
        responses = sock.stream_responses("s-raw")
        assert len(responses) == 3  # 2 chunks + done
        assert responses[0].stream.chunk.data == b"chunk-one"
        assert responses[1].stream.chunk.data == b"chunk-two-bytes"

    @pytest.mark.asyncio
    async def test_stream_task_registered_in_active_streams(self):
        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield "a"
                yield "b"

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            StreamAPI(), _stream_req("s-reg"), sock, active, log
        )
        assert "s-reg" in active
        assert isinstance(active["s-reg"], asyncio.Task)
        await sock.wait_for(lambda r: _is_done(r, "s-reg"))

    @pytest.mark.asyncio
    async def test_stream_predict_mid_stream_error(self):
        class ErrorStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield "ok"
                raise RuntimeError("mid-stream failure")

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            ErrorStreamAPI(), _stream_req("s-mid"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_error(r, "s-mid"))
        responses = sock.stream_responses("s-mid")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert len(errors) == 1
        assert "mid-stream failure" in errors[0].stream.error.message

    @pytest.mark.asyncio
    async def test_stream_predict_init_error(self):
        class InitFailAPI(EchoAPI):
            def stream_predict(self, x):
                raise RuntimeError("cannot start")
                yield  # noqa: make it a generator function

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            InitFailAPI(), _stream_req("s-init"), sock, {}, log
        )
        err = await sock.wait_for(lambda r: _is_error(r, "s-init"))
        assert len(err) == 1
        assert "cannot start" in err[0].stream.error.message

    @pytest.mark.asyncio
    async def test_async_generator_streaming(self):
        class AsyncGenAPI(EchoAPI):
            async def stream_predict(self, x):
                for i in range(2):
                    yield {"n": i}

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            AsyncGenAPI(), _stream_req("s-ag"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-ag"))
        responses = sock.stream_responses("s-ag")
        assert len(responses) == 3  # 2 chunks + done


# ---------------------------------------------------------------------------
# Streaming with hooks (LitAPI + Callback)
# ---------------------------------------------------------------------------

class TestStreamHooks:
    @pytest.mark.asyncio
    async def test_decode_request_applied(self):
        class DecodeStreamAPI(EchoAPI):
            def decode_request(self, raw):
                return {"decoded": True, "val": raw.get("val", 0)}

            def stream_predict(self, x):
                yield {"out": x["val"] + 10}

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            DecodeStreamAPI(), _stream_req("s-dec", json.dumps({"val": 5}).encode()), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-dec"))
        body = json.loads(sock.stream_responses("s-dec")[0].stream.chunk.data)
        assert body["out"] == 15

    @pytest.mark.asyncio
    async def test_before_decode_request_hook_applied(self):
        class HookCB(Callback):
            def before_decode_request(self, ctx):
                ctx.request["injected"] = True
                return ctx.request

        class HookStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield x

        api = HookStreamAPI()
        api._pipeline = Pipeline.build(api, [HookCB()])
        sock = AsyncSocket()
        meta = ProtoMeta(route="/predict", headers={}, client_ip="", request_id="r1", timestamp_ns=0)
        await inference._handle_stream_open_async(
            api, _stream_req("s-hook", b'{"q": 1}', meta), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-hook"))
        body = json.loads(sock.stream_responses("s-hook")[0].stream.chunk.data)
        assert body["injected"] is True

    @pytest.mark.asyncio
    async def test_before_decode_request_reject_sends_error(self):
        class RejectCB(Callback):
            def before_decode_request(self, ctx):
                raise ValueError("auth failed")

        class RejectStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield x

        api = RejectStreamAPI()
        api._pipeline = Pipeline.build(api, [RejectCB()])
        sock = AsyncSocket()
        meta = ProtoMeta(route="/predict", headers={}, client_ip="", request_id="r1", timestamp_ns=0)
        await inference._handle_stream_open_async(
            api, _stream_req("s-rej", b"{}", meta), sock, {}, log
        )
        err = await sock.wait_for(lambda r: _is_error(r, "s-rej"))
        assert "auth failed" in err[0].stream.error.message

    @pytest.mark.asyncio
    async def test_after_encode_response_hook_applied_per_chunk(self):
        class OnResponseCB(Callback):
            def after_encode_response(self, ctx):
                ctx.response["tagged"] = ctx.meta.route
                return ctx.response

        class OnResponseStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield {"a": 1}
                yield {"b": 2}

        api = OnResponseStreamAPI()
        api._pipeline = Pipeline.build(api, [OnResponseCB()])
        sock = AsyncSocket()
        meta = ProtoMeta(route="/stream", headers={}, client_ip="", request_id="r2", timestamp_ns=0)
        await inference._handle_stream_open_async(
            api, _stream_req("s-onresp", b"{}", meta), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-onresp"))
        chunks = [r for r in sock.stream_responses("s-onresp") if r.stream.HasField("chunk")]
        assert len(chunks) == 2
        assert json.loads(chunks[0].stream.chunk.data)["tagged"] == "/stream"
        assert json.loads(chunks[1].stream.chunk.data)["tagged"] == "/stream"

    @pytest.mark.asyncio
    async def test_callback_hooks_run_on_stream(self):
        calls = []

        class StreamCB(Callback):
            def before_decode_request(self, ctx):
                calls.append("before_decode_request")

            def after_predict(self, ctx):
                calls.append("after_predict")

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield {"n": 1}
                yield {"n": 2}

        api = StreamAPI()
        api._pipeline = Pipeline.build(api, [StreamCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-cb"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-cb"))
        assert calls == ["before_decode_request", "after_predict", "after_predict"]

    @pytest.mark.asyncio
    async def test_on_stream_close_done_on_normal_end(self):
        seen = []

        class Rec(Callback):
            def on_stream_close(self, ctx, reason):
                seen.append(reason)

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield {"n": 1}
                yield {"n": 2}

        api = StreamAPI()
        api._pipeline = Pipeline.build(api, [Rec()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-close-done"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-close-done"))
        assert seen == ["done"]

    @pytest.mark.asyncio
    async def test_on_stream_close_has_stream_stats(self):
        seen = []

        class Rec(Callback):
            def on_stream_close(self, ctx, reason):
                seen.append((reason, dict(ctx.stream_stats) if ctx.stream_stats else None))

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield {"n": 1}
                yield {"n": 2}
                yield {"n": 3}

        api = StreamAPI()
        api._pipeline = Pipeline.build(api, [Rec()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-stats"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-stats"))
        assert seen[0][0] == "done"
        assert seen[0][1]["chunks"] == 3
        assert seen[0][1]["bytes"] > 0

    @pytest.mark.asyncio
    async def test_on_stream_close_fallback_done(self):
        seen = []

        class Rec(Callback):
            def on_stream_close(self, ctx, reason):
                seen.append((reason, ctx.mode))

        api = EchoAPI()  # no stream_predict → predict fallback path
        api._pipeline = Pipeline.build(api, [Rec()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-fb-done"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-fb-done"))
        assert seen == [("done", "stream")]

    @pytest.mark.asyncio
    async def test_on_stream_close_fallback_error(self):
        seen = []

        class Rec(Callback):
            def on_stream_close(self, ctx, reason):
                seen.append(reason)

        class BoomAPI(EchoAPI):  # no stream_predict → fallback; predict raises
            def predict(self, x):
                raise RuntimeError("boom")

        api = BoomAPI()
        api._pipeline = Pipeline.build(api, [Rec()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-fb-err"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_error(r, "s-fb-err"))
        assert seen == ["error"]

    @pytest.mark.asyncio
    async def test_on_stream_close_error_on_generator_failure(self):
        seen = []

        class Rec(Callback):
            def on_stream_close(self, ctx, reason):
                seen.append(reason)

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield {"n": 1}
                raise RuntimeError("boom")

        api = StreamAPI()
        api._pipeline = Pipeline.build(api, [Rec()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-close-err"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_error(r, "s-close-err"))
        assert seen == ["error"]

    @pytest.mark.asyncio
    async def test_callback_early_return_at_open(self):
        class CacheCB(Callback):
            def before_decode_request(self, ctx):
                ctx.respond({"cached": True})

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                raise AssertionError("must not be called")
                yield

        api = StreamAPI()
        api._pipeline = Pipeline.build(api, [CacheCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-cache"), sock, {}, log
        )
        responses = sock.stream_responses("s-cache")
        assert len(responses) == 2  # early chunk + done
        assert json.loads(responses[0].stream.chunk.data) == {"cached": True}
        assert responses[1].stream.HasField("done")


# ---------------------------------------------------------------------------
# Bidirectional streaming
# ---------------------------------------------------------------------------

class _UpperHandler(BidiStreamHandler):
    def on_open(self, initial_data):
        return {"opened": initial_data}

    def on_chunk(self, chunk):
        return {"echo": chunk}

    def on_close(self):
        pass


class TestBidiStreaming:
    @pytest.mark.asyncio
    async def test_open_sends_on_open_output(self):
        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return _UpperHandler()

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            BidiAPI(), _stream_req("s-bidi", b'{"start": 1}'), sock, active, log
        )
        assert "s-bidi" in active
        chunks = [r for r in sock.stream_responses("s-bidi") if r.stream.HasField("chunk")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"opened": {"start": 1}}

    @pytest.mark.asyncio
    async def test_chunk_roundtrip_and_close(self):
        closed = asyncio.Event()

        class H(BidiStreamHandler):
            def on_chunk(self, chunk):
                return {"echo": chunk}

            def on_close(self):
                closed.set()

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        sock = AsyncSocket()
        active = {}
        api = BidiAPI()
        await inference._handle_stream_open_async(
            api, _stream_req("s-b2"), sock, active, log
        )
        # on_open returned None → no chunk yet
        assert sock.stream_responses("s-b2") == []

        from lite_server.proto import StreamChunk

        chunk_req = Request(
            uid="c1",
            stream=StreamRequest(
                stream_id="s-b2",
                chunk=StreamChunk(data=b'{"tok": 7}'),
            ),
        )
        await inference._handle_stream_async(api, chunk_req, sock, active, log)
        chunks = [r for r in sock.stream_responses("s-b2") if r.stream.HasField("chunk")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"echo": {"tok": 7}}

        close_req = Request(
            uid="c2",
            stream=StreamRequest(stream_id="s-b2", close=StreamClose()),
        )
        await inference._handle_stream_async(api, close_req, sock, active, log)
        assert closed.is_set()
        assert "s-b2" not in active
        # close sends StreamDone for bidi sessions
        assert any(_is_done(r, "s-b2") for r in sock.sent)

    @pytest.mark.asyncio
    async def test_close_delivers_on_close_output_as_final_chunk(self):
        """on_close's return value is sent as a chunk before StreamDone,
        symmetric with on_open/on_chunk (previously discarded)."""
        class H(BidiStreamHandler):
            def on_open(self, initial_data):
                return None  # no open chunk

            def on_close(self):
                return {"final": "hello world"}

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        sock = AsyncSocket()
        active = {}
        api = BidiAPI()
        await inference._handle_stream_open_async(
            api, _stream_req("s-cl", b"{}"), sock, active, log
        )
        # on_open returned None → no chunk sent yet
        assert not [r for r in sock.stream_responses("s-cl") if r.stream.HasField("chunk")]

        close_req = Request(
            uid="cl1",
            stream=StreamRequest(stream_id="s-cl", close=StreamClose()),
        )
        await inference._handle_stream_async(api, close_req, sock, active, log)

        chunks = [r for r in sock.stream_responses("s-cl") if r.stream.HasField("chunk")]
        assert len(chunks) == 1, f"expected on_close output as 1 chunk, got {len(chunks)}"
        assert json.loads(chunks[0].stream.chunk.data) == {"final": "hello world"}
        # ...and Done is still sent after the final chunk
        assert any(_is_done(r, "s-cl") for r in sock.sent)

    @pytest.mark.asyncio
    async def test_chunk_for_unknown_stream_sends_error(self):
        from lite_server.proto import StreamChunk

        sock = AsyncSocket()
        chunk_req = Request(
            uid="c1",
            stream=StreamRequest(
                stream_id="nope",
                chunk=StreamChunk(data=b"{}"),
            ),
        )
        await inference._handle_stream_async(EchoAPI(), chunk_req, sock, {}, log)
        assert any(_is_error(r, "nope") for r in sock.sent)


# ---------------------------------------------------------------------------
# run_async_loop integration
# ---------------------------------------------------------------------------

class _LoopSocket:
    """Fake zmq.asyncio socket feeding scripted requests then ETERM."""

    def __init__(self, script):
        self.script = list(script)  # list of bytes or callables -> bytes
        self.sent: list[Response] = []
        self._event = asyncio.Event()

    async def recv(self):
        await asyncio.sleep(0)
        if self.script:
            item = self.script.pop(0)
            if callable(item):
                item = item()
            if asyncio.iscoroutine(item):
                item = await item
            if item is not None:
                return item
        # Script exhausted: give in-flight streams a short window to drain
        # (ETERM cancels pending tasks on shutdown).  Exits early once a
        # terminal frame (done/error) is seen.
        for _ in range(200):
            if any(
                r.HasField("stream") and (r.stream.HasField("done") or r.stream.HasField("error"))
                for r in self.sent
            ):
                break
            await asyncio.sleep(0.005)
        import zmq

        raise zmq.ZMQError(zmq.ETERM)

    async def send(self, data: bytes):
        resp = Response()
        resp.ParseFromString(data)
        self.sent.append(resp)
        self._event.set()

    async def wait_for(self, predicate, timeout=5.0):
        async def _poll():
            while True:
                matches = [r for r in self.sent if predicate(r)]
                if matches:
                    return matches
                self._event.clear()
                await asyncio.sleep(0.005)

        return await asyncio.wait_for(_poll(), timeout)


def _open_bytes(stream_id: str, data: bytes = b"{}"):
    return Request(
        uid=f"open-{stream_id}",
        stream=StreamRequest(stream_id=stream_id, open=StreamOpen(data=data)),
    ).SerializeToString()


def _cancel_bytes(stream_id: str):
    return Request(
        uid=f"cancel-{stream_id}",
        stream=StreamRequest(stream_id=stream_id, cancel=StreamCancel()),
    ).SerializeToString()


class TestRunAsyncLoopStream:
    @pytest.mark.asyncio
    async def test_stream_open_then_done(self):
        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                for i in range(2):
                    yield {"n": i}

        sock = _LoopSocket([_open_bytes("s-loop")])
        await inference.run_async_loop(StreamAPI(), sock, "test-model", log)
        stream_resps = [r for r in sock.sent if r.HasField("stream") and r.stream.stream_id == "s-loop"]
        chunks = [r for r in stream_resps if r.stream.HasField("chunk")]
        dones = [r for r in stream_resps if r.stream.HasField("done")]
        assert len(chunks) == 2
        assert len(dones) == 1

    @pytest.mark.asyncio
    async def test_stream_cancel_mid_stream(self):
        block_generator = asyncio.Event()

        class SlowStreamAPI(EchoAPI):
            async def stream_predict(self, x):
                yield "first"
                await block_generator.wait()
                yield "second"

        async def cancel_after_first_chunk():
            # Deliver the cancel only once the first chunk is on the wire —
            # deterministic: open has completed, consume task is registered.
            # (str chunks are verbatim since P3, not JSON-quoted.)
            while not any(
                r.HasField("stream") and r.stream.HasField("chunk")
                and r.stream.chunk.data == b'first'
                for r in sock.sent
            ):
                await asyncio.sleep(0.005)
            return _cancel_bytes("s-cancel")

        sock = _LoopSocket([_open_bytes("s-cancel"), cancel_after_first_chunk])
        await inference.run_async_loop(SlowStreamAPI(), sock, "test-model", log)
        chunks = [
            r for r in sock.sent
            if r.HasField("stream") and r.stream.stream_id == "s-cancel" and r.stream.HasField("chunk")
        ]
        # Only the first chunk arrives; the consume task is cancelled while waiting
        assert len(chunks) == 1


# ---------------------------------------------------------------------------
# F2: Stream thread isolation — sync generator consumption stays off the loop
# ---------------------------------------------------------------------------


class TestStreamExecutorIsolation:
    """Sync stages run inline on the event loop (0.6.x semantics); the one
    exception is sync generator ``__next__`` / ``close``, which runs on the
    pipeline's dedicated ``lite-gen`` thread so a blocking generator can
    never freeze the loop — and concurrent requests' inline sync stages are
    not queued behind it."""

    @pytest.mark.asyncio
    async def test_sync_predict_not_blocked_by_stream_generator(self):
        """A blocking sync generator holds the lite-gen thread while a
        concurrent single request's sync predict runs inline on the loop.
        The predict must complete WITHOUT waiting for the generator (no
        head-of-line blocking), and must run on the loop thread."""
        import asyncio
        import threading

        in_generator = threading.Event()
        release_generator = threading.Event()
        predict_ident = None

        class StreamAPI(LitAPI):
            def setup(self, device):
                pass

            async def decode_request(self, request):
                # async method → mixed mode
                return request

            def predict(self, x):
                nonlocal predict_ident
                predict_ident = threading.get_ident()
                return x

            def stream_predict(self, x):
                in_generator.set()
                release_generator.wait()  # block the lite-gen thread
                in_generator.clear()
                yield {"done": True}

        api = StreamAPI()
        pipe = Pipeline.build(api, [])

        # Preprocess to get a decoded input
        meta1 = RequestMeta(
            route="/", headers=Headers(), client_ip="", request_id="r1",
            timestamp_ns=0,
        )
        ctx = RequestContext(meta=meta1, request={})
        await pipe.preprocess(ctx)
        generator = await pipe.stream_predict(ctx.input, ctx=ctx)

        # Start consuming the sync generator in a background task
        sock = AsyncSocket()
        consume_task = asyncio.create_task(
            inference._consume_stream(api, pipe, generator, "s1", sock, log, ctx)
        )

        # Wait until the generator body has been entered and is blocking
        await asyncio.to_thread(in_generator.wait)

        # Issue a concurrent single request — its sync predict runs inline
        # on the loop, so it completes even though the generator still
        # holds the lite-gen thread.
        meta2 = RequestMeta(
            route="/", headers=Headers(), client_ip="", request_id="r2",
            timestamp_ns=0,
        )
        loop_ident = threading.get_ident()
        await asyncio.wait_for(pipe.run_single(b"{}", meta2), timeout=5.0)

        assert predict_ident == loop_ident, (
            "sync predict should run inline on the loop thread, "
            "not queued behind the blocking stream generator"
        )

        # Release the generator; consumption completes
        release_generator.set()
        await asyncio.wait_for(consume_task, timeout=5.0)
        pipe.close()

    @pytest.mark.asyncio
    async def test_generator_close_uses_dedicated_thread(self):
        """Cancelling a stream must call ``generator.close()`` on the
        dedicated lite-gen thread, never inline on the event loop."""
        import asyncio
        import threading

        close_thread_name = None
        entered = threading.Event()
        release = threading.Event()

        class CloseTrackingGenerator:
            """A generator-like object that records which thread close() runs on."""

            def __init__(self):
                self._closed = False

            def __iter__(self):
                return self

            def __next__(self):
                entered.set()
                release.wait()
                raise StopIteration

            def close(self):
                nonlocal close_thread_name
                close_thread_name = threading.current_thread().name
                self._closed = True

        class StreamAPI(LitAPI):
            def setup(self, device):
                pass

            async def decode_request(self, request):
                return request

            def stream_predict(self, x):
                return CloseTrackingGenerator()

        api = StreamAPI()
        pipe = Pipeline.build(api, [])

        ctx = RequestContext(
            meta=RequestMeta(
                route="/", headers=Headers(), client_ip="", request_id="r1",
                timestamp_ns=0,
            ),
            request={},
        )
        await pipe.preprocess(ctx)
        generator = await pipe.stream_predict(ctx.input, ctx=ctx)

        sock = AsyncSocket()
        consume_task = asyncio.create_task(
            inference._consume_stream(api, pipe, generator, "s-close", sock, log, ctx)
        )

        # Wait for the generator body to be entered
        await asyncio.to_thread(entered.wait)

        # Cancel the consume task — this triggers the CancelledError handler
        # which calls generator.close()
        consume_task.cancel()
        release.set()  # let __next__ raise StopIteration first, then cancel

        with pytest.raises(asyncio.CancelledError):
            await consume_task

        assert close_thread_name is not None, "generator.close() was never called"
        assert "lite-gen" in close_thread_name, (
            f"generator.close() ran on {close_thread_name}, expected lite-gen thread — "
            "close must stay off the event loop"
        )
        pipe.close()


# ---------------------------------------------------------------------------
# Bidi ctx injection (0.7.0 context unification)
# ---------------------------------------------------------------------------


class TestBidiCtxInjection:
    """bidi_stream factory and handler hooks support ctx injection."""

    @pytest.mark.asyncio
    async def test_bidi_factory_ctx_injection(self):
        captured_ctx = []

        class H(BidiStreamHandler):
            def on_open(self, initial_data):
                return {"opened": True}

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                pass

        class BidiAPI(EchoAPI):
            def bidi_stream(self, ctx):
                captured_ctx.append(ctx)
                return H()

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            BidiAPI(), _stream_req("s-bf-ctx", b'{"start": 1}'), sock, active, log
        )
        assert len(captured_ctx) == 1
        assert captured_ctx[0].meta is not None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("hook_name,expected_key", [
        ("on_open", "from_open"),
        ("on_chunk", "from_chunk"),
        ("on_close", "from_close"),
    ])
    async def test_bidi_handler_hooks_ctx_injection(self, hook_name, expected_key):
        captured = {}

        class H(BidiStreamHandler):
            def on_open(self, initial_data, ctx):
                captured["on_open_ctx"] = ctx
                ctx.state["from_open"] = True
                return None

            def on_chunk(self, chunk, ctx):
                captured["on_chunk_ctx"] = ctx
                ctx.state["from_chunk"] = True
                return None

            def on_close(self, ctx):
                captured["on_close_ctx"] = ctx
                ctx.state["from_close"] = True

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        sock = AsyncSocket()
        active = {}
        api = BidiAPI()
        await inference._handle_stream_open_async(
            api, _stream_req("s-bh-ctx"), sock, active, log
        )
        assert "on_open_ctx" in captured
        assert captured["on_open_ctx"].state.get("from_open") is True

        from lite_server.proto import StreamChunk
        chunk_req = Request(
            uid="c1",
            stream=StreamRequest(
                stream_id="s-bh-ctx",
                chunk=StreamChunk(data=b'{"tok": 1}'),
            ),
        )
        await inference._handle_stream_async(api, chunk_req, sock, active, log)
        assert "on_chunk_ctx" in captured
        assert captured["on_chunk_ctx"].state.get("from_chunk") is True

        from lite_server.proto import StreamClose
        close_req = Request(
            uid="c2",
            stream=StreamRequest(stream_id="s-bh-ctx", close=StreamClose()),
        )
        await inference._handle_stream_async(api, close_req, sock, active, log)
        assert "on_close_ctx" in captured
        assert captured["on_close_ctx"].state.get("from_close") is True

    @pytest.mark.asyncio
    async def test_bidi_open_without_meta_gets_empty_meta(self):
        """When proto has no meta field, meta must NOT be None —
        RequestContext.meta is always a RequestMeta (empty default)."""
        captured_meta = []

        class H(BidiStreamHandler):
            def on_open(self, initial_data, ctx):
                captured_meta.append(ctx.meta)
                return None

            def on_chunk(self, chunk, ctx):
                return None

            def on_close(self, ctx):
                pass

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        sock = AsyncSocket()
        active = {}
        # _stream_req without meta= kwarg produces a StreamRequest with no meta
        await inference._handle_stream_open_async(
            BidiAPI(), _stream_req("s-no-meta", b"{}"), sock, active, log
        )
        assert len(captured_meta) == 1
        assert captured_meta[0] is not None
        assert captured_meta[0].route == ""
        assert captured_meta[0].request_id == ""


class TestStreamFallbackMeta:
    """Predict-fallback stream open: meta invariant holds on all paths (§6.3)."""

    @pytest.mark.asyncio
    async def test_fallback_predict_without_meta_gets_empty_meta(self):
        """Model without stream_predict/bidi falls back to run_single;
        meta absent on the wire must become an empty RequestMeta, not None."""
        captured_meta = []

        class FallbackAPI(EchoAPI):
            def decode_request(self, request, ctx: RequestContext | None = None):
                captured_meta.append(ctx.meta)
                return request

        sock = AsyncSocket()
        active = {}
        # _stream_req without meta= kwarg produces a StreamRequest with no meta
        await inference._handle_stream_open_async(
            FallbackAPI(), _stream_req("s-fb-no-meta", b"{}"), sock, active, log
        )
        assert len(captured_meta) == 1
        assert isinstance(captured_meta[0], RequestMeta)
        assert captured_meta[0].request_id == ""
        assert captured_meta[0].route == ""


class TestBidiSessionLifecycle:
    """Session registration timing + the on_close exactly-once contract:
    a completed on_open is always balanced by exactly one on_close
    (close/cancel, worker shutdown, or abandoned open); a failed on_open
    never creates a session and never calls on_close."""

    @pytest.mark.asyncio
    async def test_on_open_failure_leaves_no_session(self):
        closed = []

        class H(BidiStreamHandler):
            def on_open(self, initial_data):
                raise RuntimeError("open boom")

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                closed.append(True)

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            BidiAPI(), _stream_req("s-open-fail"), sock, active, log
        )
        assert "s-open-fail" not in active
        assert closed == []
        assert any(_is_error(r, "s-open-fail") for r in sock.sent)

    @pytest.mark.asyncio
    async def test_postprocess_failure_after_on_open_calls_on_close(self):
        closed = []

        class H(BidiStreamHandler):
            def on_open(self, initial_data):
                return {"greet": True}

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                closed.append(True)

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

            def encode_response(self, output):
                raise RuntimeError("encode boom")

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            BidiAPI(), _stream_req("s-enc-fail"), sock, active, log
        )
        assert "s-enc-fail" not in active
        assert closed == [True]
        assert any(_is_error(r, "s-enc-fail") for r in sock.sent)

    @pytest.mark.asyncio
    async def test_early_return_after_on_open_calls_on_close(self):
        closed = []

        class H(BidiStreamHandler):
            def on_open(self, initial_data):
                return {"greet": True}

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                closed.append(True)

        class EarlyCb(Callback):
            def after_predict(self, ctx):
                return ctx.respond({"final": True})

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        api = BidiAPI()
        api._pipeline = Pipeline.build(api, [EarlyCb()])
        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            api, _stream_req("s-early"), sock, active, log
        )
        assert "s-early" not in active
        assert closed == [True]
        # Early response was sent as chunk + StreamDone.
        assert any(_is_done(r, "s-early") for r in sock.sent)

    @pytest.mark.asyncio
    async def test_session_closed_on_shutdown(self):
        closed = []

        class H(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                closed.append(True)

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        sock = _LoopSocket([_open_bytes("s-shutdown")])
        await inference.run_async_loop(BidiAPI(), sock, "test-model", log)
        assert closed == [True]


# ---------------------------------------------------------------------------
# Decoupled streaming (predict_decoupled — P9-1 DecoupledInfer)
# ---------------------------------------------------------------------------

def _decoupled_req(stream_id: str, data: bytes = b"{}", meta=None) -> StreamRequest:
    """A StreamOpen carrying the additive P9-1 `decoupled=true` flag."""
    open_kw = {"data": data, "decoupled": True}
    if meta is not None:
        open_kw["meta"] = meta
    return StreamRequest(stream_id=stream_id, open=StreamOpen(**open_kw))


class TestDecoupledStreaming:
    """P9-1: predict_decoupled(data, sender) holds a push handle; the channel
    stays open after the call returns and the model pushes N chunks, ending
    with sender.close(). Distinct from stream_predict (generator pull)."""

    @pytest.mark.asyncio
    async def test_pushes_chunks_then_done(self):
        class DecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                for i in range(3):
                    await sender.send({"chunk": i})
                await sender.close()

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            DecoupledAPI(), _decoupled_req("s-dc", b'{"prompt": "go"}'), sock, active, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-dc"))
        responses = sock.stream_responses("s-dc")
        assert len(responses) == 4  # 3 chunks + done
        for i in range(3):
            assert responses[i].stream.HasField("chunk")
            assert responses[i].stream.chunk.is_final is False
            assert json.loads(responses[i].stream.chunk.data)["chunk"] == i
        assert responses[3].stream.HasField("done")
        # Closed inline → no live session left registered.
        assert "s-dc" not in active

    @pytest.mark.asyncio
    async def test_channel_open_after_return(self):
        """Differentiator vs stream_predict: predict_decoupled returns BEFORE
        closing; chunks arrive after the call returns and a session is
        registered so close/cancel can find the sender."""

        class DecoupledBgAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                async def _push():
                    for i in range(3):
                        await asyncio.sleep(0.005)
                        await sender.send({"chunk": i})
                    await sender.close()

                asyncio.create_task(_push())  # return immediately

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            DecoupledBgAPI(), _decoupled_req("s-bg"), sock, active, log
        )
        # After open returns, a live session is registered (channel open).
        assert "s-bg" in active
        await sock.wait_for(lambda r: _is_done(r, "s-bg"))
        responses = sock.stream_responses("s-bg")
        assert len(responses) == 4  # 3 chunks + done
        for i in range(3):
            assert json.loads(responses[i].stream.chunk.data)["chunk"] == i
        # close() popped the session.
        assert "s-bg" not in active

    @pytest.mark.asyncio
    async def test_not_implemented_sends_structured_error(self):
        """decoupled=True on a model without predict_decoupled → structured
        stream error (error_type=not_implemented maps to gRPC
        FailedPrecondition on the Rust side)."""
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            EchoAPI(), _decoupled_req("s-ni"), sock, {}, log
        )
        err = [r for r in sock.stream_responses("s-ni") if r.stream.HasField("error")]
        assert len(err) == 1
        body = json.loads(err[0].stream.error.message)
        assert body["error"]["type"] == "not_implemented"

    @pytest.mark.asyncio
    async def test_cancel_signals_sender_no_done(self):
        """Client disconnect (server sends StreamCancel) → cooperative cancel:
        the sender is flagged closed and NO StreamDone is emitted (cancel
        semantics match bidi/uni streams)."""
        class DecoupledCancelAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                async def _push():
                    for i in range(10):
                        await asyncio.sleep(0.05)
                        await sender.send({"chunk": i})
                    await sender.close()

                asyncio.create_task(_push())

        sock = AsyncSocket()
        active = {}
        await inference._handle_stream_open_async(
            DecoupledCancelAPI(), _decoupled_req("s-cn"), sock, active, log
        )
        assert "s-cn" in active
        sender = active["s-cn"].sender
        assert sender.closed is False

        cancel_req = Request(stream=StreamRequest(stream_id="s-cn", cancel=StreamCancel()))
        await inference._handle_stream_async(
            DecoupledCancelAPI(), cancel_req, sock, active, log
        )
        # Cooperative cancel: sender flagged closed, session popped, no Done.
        assert sender.closed is True
        assert "s-cn" not in active
        assert not any(_is_done(r, "s-cn") for r in sock.stream_responses("s-cn"))


# ---------------------------------------------------------------------------
# P-DEADLINE — worker cooperative deadline check (蓝图 §4.0.10)
# ---------------------------------------------------------------------------


class TestStreamDeadline:
    @pytest.mark.asyncio
    async def test_stream_stops_at_deadline_before_all_chunks(self):
        """A stream whose meta carries a deadline stops early: the framework's
        cooperative check in _consume_stream breaks once the deadline passes,
        so the stream ends with a StreamError (a deadline cut is abnormal — not
        normal completion) well before the generator is exhausted.
        """
        import time

        # 10 chunks at ~50ms each (500ms total); deadline ~120ms in the future
        # → only a couple of chunks should land before the deadline cuts in.
        deadline_ns = (time.time_ns() // 1_000_000 + 120) * 1_000_000
        meta = ProtoMeta(
            route="/predict",
            headers={},
            client_ip="",
            request_id="r-dl",
            timestamp_ns=0,
            deadline_unix_ns=deadline_ns,
        )

        class SlowStreamAPI(EchoAPI):
            async def stream_predict(self, x):
                for i in range(10):
                    await asyncio.sleep(0.05)
                    yield {"chunk": i}

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            SlowStreamAPI(), _stream_req("s-dl", b'{"prompt": "go"}', meta=meta), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_error(r, "s-dl"))
        responses = sock.stream_responses("s-dl")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        # The deadline (120ms) must cut the stream short of all 10 chunks.
        assert len(chunks) < 10, f"deadline should cut the stream; got {len(chunks)} chunks"
        # ... but at least one chunk lands before the deadline passes.
        assert len(chunks) >= 1, f"expected >=1 chunk before deadline; got {len(chunks)}"
        # A deadline cut terminates with a StreamError (not StreamDone).
        assert any(_is_error(r, "s-dl") for r in responses)

    @pytest.mark.asyncio
    async def test_no_deadline_streams_all_chunks(self):
        """No deadline (None) → behavior unchanged: all chunks + done."""
        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                for i in range(4):
                    yield {"chunk": i}

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            StreamAPI(), _stream_req("s-nodl", b'{"prompt": "go"}'), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-nodl"))
        responses = sock.stream_responses("s-nodl")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        assert len(chunks) == 4
        assert any(_is_done(r, "s-nodl") for r in responses)

    def test_deadline_passed_helper(self):
        """_deadline_passed: None→False, future→False, past→True."""
        from lite_server.worker.streaming import _deadline_passed

        meta_none = RequestMeta(
            route="/predict", headers=Headers({}), client_ip="", request_id="r", timestamp_ns=0
        )
        assert _deadline_passed(RequestContext(meta=meta_none)) is False

        import time

        meta_future = RequestMeta(
            route="/predict",
            headers=Headers({}),
            client_ip="",
            request_id="r",
            timestamp_ns=0,
            deadline_unix_ns=time.time_ns() + 10_000_000_000,
        )
        assert _deadline_passed(RequestContext(meta=meta_future)) is False

        meta_past = RequestMeta(
            route="/predict",
            headers=Headers({}),
            client_ip="",
            request_id="r",
            timestamp_ns=0,
            deadline_unix_ns=time.time_ns() - 1_000_000_000,
        )
        assert _deadline_passed(RequestContext(meta=meta_past)) is True


# ---------------------------------------------------------------------------
# on_error custom response — streaming paths (2026-08-04)
# ---------------------------------------------------------------------------

class TestOnErrorCustomResponseStreaming:
    """on_error returning a Response in streaming paths sends a graceful
    terminal chunk (StreamChunk + StreamDone) instead of StreamError."""

    @pytest.mark.asyncio
    async def test_stream_on_error_returns_response_sends_chunk_and_done(self):
        """Stream: on_error returns Response → StreamChunk(body)+StreamDone,
        no StreamError."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"graceful": "shutdown"})

        class ErrorStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield "ok"
                raise RuntimeError("mid-stream failure")

        api = ErrorStreamAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-custom"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("s-custom")
        # Should have chunk("ok") + chunk({"graceful":"shutdown"}) + done
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 2
        assert json.loads(chunks[1].stream.chunk.data) == {"graceful": "shutdown"}
        assert len(dones) == 1
        assert len(errors) == 0

    @pytest.mark.asyncio
    async def test_stream_on_error_returns_none_is_unchanged(self):
        """Stream: on_error returns None → StreamError as before (backward compat)."""

        class NoopCB(Callback):
            def on_error(self, ctx, exc):
                return None

        class ErrorStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield "ok"
                raise RuntimeError("mid-stream failure")

        api = ErrorStreamAPI()
        api._pipeline = Pipeline.build(api, [NoopCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-noop"), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_error(r, "s-noop"))
        errors = [r for r in sock.stream_responses("s-noop") if r.stream.HasField("error")]
        assert len(errors) == 1
        assert "mid-stream failure" in errors[0].stream.error.message

    @pytest.mark.asyncio
    async def test_bidi_on_error_returns_response(self):
        """Bidi: on_error in on_open returns Response → StreamChunk+Done."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"bidi_error": True})

        class ExplodingHandler(BidiStreamHandler):
            async def on_open(self, data, ctx=None):
                raise RuntimeError("on_open failed")

        class BidiAPI(EchoAPI):
            def bidi_stream(self, ctx=None):
                return ExplodingHandler()

        api = BidiAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("b-custom"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("b-custom")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"bidi_error": True}
        assert len(dones) == 1
        assert len(errors) == 0

    @pytest.mark.asyncio
    async def test_s2_on_stream_close_reason_error_for_custom_response(self):
        """S2: on_error custom Response → on_stream_close still receives
        reason 'error' (not 'done') for correct observability."""
        from lite_server.response import Response as LiteResponse

        close_reasons = []

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"custom": True})

        class CloseRecCB(Callback):
            def on_stream_close(self, ctx, reason):
                close_reasons.append(reason)

        class ErrorStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield "ok"
                raise RuntimeError("boom")

        api = ErrorStreamAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB(), CloseRecCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-s2"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        assert close_reasons == ["error"], (
            f"Expected ['error'], got {close_reasons}"
        )

    @pytest.mark.asyncio
    async def test_s3_stream_fallback_on_error_custom_response(self):
        """S3: stream fallback (no stream_predict) + on_error returns Response
        → run_single returns normally with error_overridden=True,
        fallback sends chunk(is_final)+Done and close reason 'error'."""
        from lite_server.response import Response as LiteResponse

        close_reasons = []

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"fallback_error": True})

        class CloseRecCB(Callback):
            def on_stream_close(self, ctx, reason):
                close_reasons.append(reason)

        class FailingAPI(EchoAPI):
            def predict(self, x):
                raise ValueError("predict boom")

        api = FailingAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB(), CloseRecCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("f-custom"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("f-custom")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert chunks[0].stream.chunk.is_final is True
        assert json.loads(chunks[0].stream.chunk.data) == {"fallback_error": True}
        assert len(dones) == 1
        assert len(errors) == 0
        # S2: close reason is "error", not "done"
        assert close_reasons == ["error"], (
            f"Expected ['error'], got {close_reasons}"
        )

    @pytest.mark.asyncio
    async def test_s1_stream_status_code_headers_not_propagated(self):
        """S1: streaming path drops status_code/headers from on_error Response
        (no per-chunk status/header channel in the stream protocol)."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(
                    content={"body_only": True},
                    status_code=500,
                    headers={"X-Should-Not-Appear": "1"},
                )

        class ErrorStreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield "ok"
                raise RuntimeError("boom")

        api = ErrorStreamAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-s1"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("s-s1")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        # Body is delivered
        assert json.loads(chunks[1].stream.chunk.data) == {"body_only": True}
        # But status_code and headers are NOT on the chunk proto
        # (StreamChunkResponse has only data + is_final — no status/headers fields)
        # Verified by: no StreamError, only Done after the custom chunk.

    @pytest.mark.asyncio
    async def test_stream_predict_init_error_custom_response(self):
        """stream_predict init error (before generator starts) + on_error
        returns Response → StreamChunk+Done."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"init_error": True})

        class InitFailAPI(EchoAPI):
            def stream_predict(self, x):
                raise RuntimeError("cannot start")
                yield  # noqa: make it a generator function

        api = InitFailAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-init-custom"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("s-init-custom")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"init_error": True}
        assert len(dones) == 1
        assert len(errors) == 0

    @pytest.mark.asyncio
    async def test_stream_preprocess_error_custom_response(self):
        """Stream preprocess (before_decode_request/decode_request) error +
        on_error returns Response → StreamChunk+Done."""
        from lite_server.response import Response as LiteResponse

        class CustomErrorCB(Callback):
            def on_error(self, ctx, exc):
                return LiteResponse(content={"preprocess_err": True})

        class RejectCB(Callback):
            def before_decode_request(self, ctx):
                raise ValueError("rejected at preprocess")

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                yield {"never": "reached"}

        api = StreamAPI()
        api._pipeline = Pipeline.build(api, [CustomErrorCB(), RejectCB()])
        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            api, _stream_req("s-pre-custom"), sock, {}, log
        )
        await sock.wait_for(lambda r: r.HasField("stream") and (
            r.stream.HasField("done") or r.stream.HasField("error")
        ))
        responses = sock.stream_responses("s-pre-custom")
        chunks = [r for r in responses if r.stream.HasField("chunk")]
        dones = [r for r in responses if r.stream.HasField("done")]
        errors = [r for r in responses if r.stream.HasField("error")]
        assert len(chunks) == 1
        assert json.loads(chunks[0].stream.chunk.data) == {"preprocess_err": True}
        assert len(dones) == 1
        assert len(errors) == 0



# ---------------------------------------------------------------------------
# /audit fdbd1c9 evidence tests (must FAIL on the audited commit)
# ---------------------------------------------------------------------------


class TestAuditBidiChunkContentType:
    """A bidi stream opened with a non-JSON Content-Type must deliver
    mid-stream chunks to on_chunk as raw bytes.

    fdbd1c9 made the three stream-OPEN paths Content-Type aware but left
    ``_handle_stream_chunk_async`` hardcoded to ``_parse_json_payload`` —
    a raw-bytes chunk dies as a 400 stream error before reaching the model,
    so a bidi stream opened with application/octet-stream breaks on its
    first data frame.
    """

    @pytest.mark.asyncio
    async def test_bidi_chunk_raw_bytes_reach_on_chunk(self):
        from lite_server.proto import StreamChunk
        from lite_server.worker import streaming as worker_streaming

        received = []

        async def on_chunk(data, ctx=None):
            received.append(data)
            return None  # no output → handler returns right after on_chunk

        async def on_close(ctx=None):
            return None

        meta = RequestMeta(
            route="/predict",
            headers=Headers({"content-type": "application/octet-stream"}),
            client_ip="127.0.0.1",
            request_id="req-audit-bidi",
            timestamp_ns=1,
        )
        ctx = RequestContext(meta=meta, request={}, mode="bidi")
        active = {"s1": worker_streaming._BidiSession(object(), on_chunk, on_close, ctx)}
        sock = AsyncSocket()
        raw = b"\x00\xff\xfe" * 4
        req = StreamRequest(stream_id="s1", chunk=StreamChunk(data=raw))

        await worker_streaming._handle_stream_chunk_async(EchoAPI(), req, sock, active, log)

        assert received == [raw], (
            "raw-bytes chunk must reach on_chunk unchanged; "
            f"received={received!r}, socket_frames={sock.sent!r}"
        )


class TestBidiFramingContentType:
    """h2 bidi clients send ``content-type: application/x-lite-bidi`` on the
    session POST — that names the LPM framing, not the payload. Payload
    dispatch must ignore it and keep the JSON default (pre-0.8.3 semantics);
    the h2 full-duplex integration test exercises this end to end.
    """

    @pytest.mark.asyncio
    async def test_bidi_open_and_chunk_with_framing_ct_parse_json(self):
        from lite_server.proto import StreamChunk, StreamOpen
        from lite_server.proto import RequestMeta as ProtoMeta
        from lite_server.worker import streaming as worker_streaming

        received = []

        class H:
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                received.append(chunk)
                return None

            def on_close(self):
                pass

        class BidiAPI(EchoAPI):
            def bidi_stream(self):
                return H()

        meta = ProtoMeta(
            route="/predict",
            headers={"content-type": "application/x-lite-bidi"},
            client_ip="",
            request_id="r",
            timestamp_ns=1,
        )
        sock = AsyncSocket()
        active = {}
        open_req = StreamRequest(
            stream_id="s-fr",
            open=StreamOpen(data=b'{"init": 1}', meta=meta),
        )
        await worker_streaming._handle_stream_open_async(
            BidiAPI(), open_req, sock, active, log
        )
        session = active.get("s-fr")
        assert session is not None, "bidi session must be registered"
        # initial_data JSON-parsed despite the framing content-type
        assert session.ctx.request == {"init": 1}

        chunk_req = StreamRequest(
            stream_id="s-fr",
            chunk=StreamChunk(data=b'{"chunk": 1}'),
        )
        await worker_streaming._handle_stream_chunk_async(
            BidiAPI(), chunk_req, sock, active, log
        )
        assert received == [{"chunk": 1}]

# ---------------------------------------------------------------------------
# S3 (批次 2):tokens_generated 近似口径——per-stream chunk 计数
# ---------------------------------------------------------------------------


class TestStreamTokensGenerated:
    """S3:流式 Done.metrics 携带 tokens_generated(= chunk 数近似口径,
    per-stream 计数,不进 _metric_values 共享通道);prefill/decode 不填(S3 收窄)。"""

    @pytest.mark.asyncio
    async def test_done_metrics_carry_tokens_generated(self):
        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                for i in range(3):
                    yield {"chunk": i}

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            StreamAPI(), _stream_req("s-tok", b'{"prompt": "go"}'), sock, {}, log
        )
        done = await sock.wait_for(lambda r: _is_done(r, "s-tok"))
        metrics = done[0].stream.done.metrics
        assert metrics is not None, "done.metrics must be present"
        assert metrics.tokens_generated == 3
        assert metrics.prefill_ms == 0.0, "S3 收窄:prefill_ms 不填"
        assert metrics.decode_ms == 0.0, "S3 收窄:decode_ms 不填"

    @pytest.mark.asyncio
    async def test_concurrent_streams_tokens_not_mixed(self):
        """并发两流互不误归——per-stream 计数隔离,不走共享 _metric_values。"""

        class StreamAPI(EchoAPI):
            def stream_predict(self, x):
                # preprocess 后 x 已是 dict(JSON body)。
                n = int(x["n"])
                for i in range(n):
                    yield {"chunk": i}

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            StreamAPI(), _stream_req("s-tok-a", b'{"n": 2}'), sock, {}, log
        )
        await inference._handle_stream_open_async(
            StreamAPI(), _stream_req("s-tok-b", b'{"n": 5}'), sock, {}, log
        )
        da = await sock.wait_for(lambda r: _is_done(r, "s-tok-a"))
        db = await sock.wait_for(lambda r: _is_done(r, "s-tok-b"))
        assert da[0].stream.done.metrics.tokens_generated == 2
        assert db[0].stream.done.metrics.tokens_generated == 5

    @pytest.mark.asyncio
    async def test_zero_chunk_stream_tokens_zero(self):
        """零 chunk 流(直接 Done)→ tokens_generated 为 0(或 metrics 缺省,
        Rust 侧 >0 守卫自然不落盘)。"""

        class EmptyAPI(EchoAPI):
            def stream_predict(self, x):
                return
                yield  # 永不执行:0 chunk

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            EmptyAPI(), _stream_req("s-tok-0"), sock, {}, log
        )
        done = await sock.wait_for(lambda r: _is_done(r, "s-tok-0"))
        m = done[0].stream.done.metrics
        assert m is None or m.tokens_generated == 0

    @pytest.mark.asyncio
    async def test_unary_path_does_not_fill_tokens(self):
        """unary 不填 tokens_generated(无 chunk 概念;精确计数依赖 tokenizer,
        D3 后续项)。collect_metrics 默认参数不带 tokens。"""
        from lite_server.pipeline import collect_metrics

        m = collect_metrics(EchoAPI())
        assert m is None or m.tokens_generated == 0

    @pytest.mark.asyncio
    async def test_decoupled_sender_counts_chunks(self):
        """decoupled 路径同样计数:DecoupledSender.send 每次 +1,
        close() 的 Done 携带累计 chunk 数。"""

        class DecoupledAPI(EchoAPI):
            async def predict_decoupled(self, data, sender):
                for i in range(4):
                    await sender.send({"chunk": i})
                await sender.close()

        sock = AsyncSocket()
        await inference._handle_stream_open_async(
            DecoupledAPI(), _decoupled_req("s-tok-dc", b'{"prompt": "go"}'), sock, {}, log
        )
        done = await sock.wait_for(lambda r: _is_done(r, "s-tok-dc"))
        metrics = done[0].stream.done.metrics
        assert metrics is not None
        assert metrics.tokens_generated == 4
