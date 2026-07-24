"""Tests for streaming inference in the unified async worker loop.

Covers stream open (fallback / stream_predict / bidi), per-chunk hooks,
early return, cancellation, and the full run_async_loop integration.
"""

import asyncio
import json
import logging

import pytest

from lite_server.api import BidiStreamHandler, LitAPI
from lite_server.callback import Callback
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
    async def test_on_request_hook_applied(self):
        class HookStreamAPI(EchoAPI):
            def on_request(self, ctx):
                ctx.request["injected"] = True
                return ctx.request

            def stream_predict(self, x):
                yield x

        sock = AsyncSocket()
        meta = ProtoMeta(route="/predict", headers={}, client_ip="", request_id="r1", timestamp_ns=0)
        await inference._handle_stream_open_async(
            HookStreamAPI(), _stream_req("s-hook", b'{"q": 1}', meta), sock, {}, log
        )
        await sock.wait_for(lambda r: _is_done(r, "s-hook"))
        body = json.loads(sock.stream_responses("s-hook")[0].stream.chunk.data)
        assert body["injected"] is True

    @pytest.mark.asyncio
    async def test_on_request_reject_sends_error(self):
        class RejectStreamAPI(EchoAPI):
            def on_request(self, ctx):
                raise ValueError("auth failed")

            def stream_predict(self, x):
                yield x

        sock = AsyncSocket()
        meta = ProtoMeta(route="/predict", headers={}, client_ip="", request_id="r1", timestamp_ns=0)
        await inference._handle_stream_open_async(
            RejectStreamAPI(), _stream_req("s-rej", b"{}", meta), sock, {}, log
        )
        err = await sock.wait_for(lambda r: _is_error(r, "s-rej"))
        assert "auth failed" in err[0].stream.error.message

    @pytest.mark.asyncio
    async def test_on_response_hook_applied_per_chunk(self):
        class OnResponseStreamAPI(EchoAPI):
            def on_response(self, ctx):
                ctx.response["tagged"] = ctx.meta.route
                return ctx.response

            def stream_predict(self, x):
                yield {"a": 1}
                yield {"b": 2}

        sock = AsyncSocket()
        meta = ProtoMeta(route="/stream", headers={}, client_ip="", request_id="r2", timestamp_ns=0)
        await inference._handle_stream_open_async(
            OnResponseStreamAPI(), _stream_req("s-onresp", b"{}", meta), sock, {}, log
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
            def on_request(self, ctx):
                calls.append("on_request")

            def on_output(self, ctx):
                calls.append("on_output")

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
        assert calls == ["on_request", "on_output", "on_output"]

    @pytest.mark.asyncio
    async def test_callback_early_return_at_open(self):
        class CacheCB(Callback):
            def on_request(self, ctx):
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
            while not any(
                r.HasField("stream") and r.stream.HasField("chunk")
                and r.stream.chunk.data == b'"first"'
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
# F2: Stream executor isolation — sync generator must use pipe._executor
# ---------------------------------------------------------------------------


class TestStreamExecutorIsolation:
    """F2: sync generator ``__next__`` / ``close`` must dispatch to the
    pipeline's single-thread executor, NOT ``asyncio.to_thread``'s default
    pool.  Otherwise sync code from stream consumption and sync code from
    other requests (predict, encode) can run concurrently, violating the
    core invariant that sync code never runs concurrently."""

    @pytest.mark.asyncio
    async def test_sync_generator_and_predict_never_overlap(self):
        """Deterministic concurrency test: a blocking sync generator holds the
        executor while a concurrent single request's sync predict is
        dispatched.  Before the fix (generator __next__ on default pool),
        predict runs on pipe._executor in parallel → overlap.  After the fix
        (both on pipe._executor), predict is queued behind the generator."""
        import asyncio
        import threading
        import time

        in_generator = threading.Event()
        release_generator = threading.Event()
        overlap_detected = False

        class StreamAPI(LitAPI):
            def setup(self, device):
                pass

            async def decode_request(self, request):
                # async method → forces executor mode (any_async = True)
                return request

            def predict(self, x):
                if in_generator.is_set():
                    nonlocal overlap_detected
                    overlap_detected = True
                return x

            def stream_predict(self, x):
                in_generator.set()
                release_generator.wait()  # block the executor thread
                in_generator.clear()
                yield {"done": True}

        api = StreamAPI()
        pipe = Pipeline.build(api, [])
        assert pipe._executor is not None, "mixed mode must create executor"

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

        # Issue a concurrent single request — its sync predict will be
        # dispatched to pipe._executor.  Before fix: runs on a different
        # thread pool → overlap_detected = True.  After fix: queued on the
        # same single-thread executor → overlap_detected stays False.
        meta2 = RequestMeta(
            route="/", headers=Headers(), client_ip="", request_id="r2",
            timestamp_ns=0,
        )
        single_task = asyncio.create_task(pipe.run_single(b"{}", meta2))

        # Give the single request time to queue up on the executor
        await asyncio.sleep(0.1)

        # Release the generator
        release_generator.set()

        # Both tasks should complete
        await asyncio.wait_for(consume_task, timeout=5.0)
        await asyncio.wait_for(single_task, timeout=5.0)

        assert not overlap_detected, (
            "sync generator __next__ ran on a different executor than sync predict — "
            "generator is bypassing the pipeline's single-thread executor"
        )
        pipe.close()

    @pytest.mark.asyncio
    async def test_generator_close_uses_pipeline_executor(self):
        """Cancelling a stream must call ``generator.close()`` through the
        pipeline executor, not the default thread pool."""
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
        assert pipe._executor is not None

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
        assert "lite-sync" in close_thread_name, (
            f"generator.close() ran on {close_thread_name}, expected lite-sync thread — "
            "close is using the wrong executor"
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
            def decode_request(self, request, ctx):
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
            def on_output(self, ctx):
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
