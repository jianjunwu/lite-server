"""Tests for streaming inference in lite_server.worker.inference.

All tests use threading.Event for synchronization — no time.sleep race conditions.
"""

import json
import logging
import threading
from unittest.mock import MagicMock
from lite_server.worker.inference import _StreamTracker

import pytest

from lite_server.worker import inference
from lite_server.proto import (
    Request,
    Response,
    StreamRequest,
    StreamOpen,
    StreamCancel,
    StreamClose,
    RequestMeta as ProtoMeta,
)
from lite_server.api import RequestMeta


log = logging.getLogger("test_streaming")


class SyncSocket:
    """Thread-safe mock socket that signals when messages arrive."""

    def __init__(self):
        self.sent: list[Response] = []
        self._lock = threading.Lock()
        self._event = threading.Event()  # signaled on each send

    def send(self, data: bytes):
        resp = Response()
        resp.ParseFromString(data)
        with self._lock:
            self.sent.append(resp)
        self._event.set()

    def wait_for(self, predicate, timeout=5.0):
        """Block until predicate(sent) is truthy. Returns matching responses."""
        deadline = threading.Event()

        def watchdog():
            deadline.wait(timeout)
            deadline.set()

        threading.Thread(target=watchdog, daemon=True).start()

        while not deadline.is_set():
            with self._lock:
                matches = [r for r in self.sent if predicate(r)]
            if matches:
                return matches
            self._event.clear()
            self._event.wait(timeout=0.05)
        with self._lock:
            return [r for r in self.sent if predicate(r)]

    def get_stream_responses(self, stream_id: str) -> list[Response]:
        with self._lock:
            return [r for r in self.sent if r.HasField("stream") and r.stream.stream_id == stream_id]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestHasStreamPredict:
    def test_returns_true_when_stream_predict_defined(self):
        class StreamAPI:
            def stream_predict(self, x):
                yield 1

        assert inference._has_stream_predict(StreamAPI()) is True

    def test_returns_false_when_no_stream_predict(self):
        class PlainAPI:
            def predict(self, x):
                return x

        assert inference._has_stream_predict(PlainAPI()) is False

    def test_returns_false_when_stream_predict_not_callable(self):
        class WeirdAPI:
            stream_predict = "not a method"

        assert inference._has_stream_predict(WeirdAPI()) is False


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
        assert resp.stream.stream_id == "s1"
        assert resp.stream.HasField("done")

    def test_make_stream_error(self):
        resp = inference._make_stream_error("s1", "boom")
        assert resp.uid == "stream-error-s1"
        assert resp.stream.HasField("error")
        assert resp.stream.error.message == "boom"


# ---------------------------------------------------------------------------
# Fallback (no stream_predict)
# ---------------------------------------------------------------------------

class TestHandleStreamOpenFallback:
    """When LitAPI has no stream_predict, _handle_stream_open falls back to predict()."""

    def test_sends_single_chunk_then_done(self):
        class PlainAPI:
            def predict(self, x):
                return {"result": x.get("val", 0) * 3}

        sock = SyncSocket()
        stream_req = StreamRequest(
            stream_id="s-fallback",
            open=StreamOpen(data=json.dumps({"val": 4}).encode()),
        )
        inference._handle_stream_open(PlainAPI(), stream_req, sock, _StreamTracker(), log)

        # Wait for done signal — deterministic, no sleep race
        done_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == "s-fallback"
        )
        assert len(done_resps) >= 1

        responses = sock.get_stream_responses("s-fallback")
        assert len(responses) == 2
        # First: chunk with is_final=True
        assert responses[0].stream.HasField("chunk")
        assert responses[0].stream.chunk.is_final is True
        body = json.loads(responses[0].stream.chunk.data)
        assert body["result"] == 12
        # Second: done
        assert responses[1].stream.HasField("done")

    def test_fallback_with_empty_data(self):
        class EchoAPI:
            def predict(self, x):
                return x

        sock = SyncSocket()
        stream_req = StreamRequest(
            stream_id="s-empty",
            open=StreamOpen(data=b""),
        )
        inference._handle_stream_open(EchoAPI(), stream_req, sock, _StreamTracker(), log)

        done_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == "s-empty"
        )
        assert len(done_resps) >= 1

        responses = sock.get_stream_responses("s-empty")
        assert len(responses) == 2
        assert responses[0].stream.chunk.is_final is True

    def test_fallback_predict_error_sends_stream_error(self):
        class BadAPI:
            def predict(self, x):
                raise RuntimeError("predict failed")

        sock = SyncSocket()
        stream_req = StreamRequest(
            stream_id="s-err",
            open=StreamOpen(data=b'{"x": 1}'),
        )
        inference._handle_stream_open(BadAPI(), stream_req, sock, _StreamTracker(), log)

        err_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("error") and r.stream.stream_id == "s-err"
        )
        assert len(err_resps) == 1
        assert "predict failed" in err_resps[0].stream.error.message


# ---------------------------------------------------------------------------
# Normal streaming (with stream_predict)
# ---------------------------------------------------------------------------

class TestHandleStreamOpen:
    """When LitAPI has stream_predict, _handle_stream_open starts a generator."""

    def test_streams_multiple_chunks_then_done(self):
        class StreamAPI:
            def stream_predict(self, x):
                for i in range(3):
                    yield {"chunk": i}

        sock = SyncSocket()
        stream_req = StreamRequest(
            stream_id="s-ok",
            open=StreamOpen(data=json.dumps({"prompt": "go"}).encode()),
        )
        inference._handle_stream_open(StreamAPI(), stream_req, sock, _StreamTracker(), log)

        done_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == "s-ok"
        )
        assert len(done_resps) >= 1

        responses = sock.get_stream_responses("s-ok")
        assert len(responses) == 4  # 3 chunks + 1 done
        for i in range(3):
            assert responses[i].stream.HasField("chunk")
            body = json.loads(responses[i].stream.chunk.data)
            assert body["chunk"] == i
        assert responses[3].stream.HasField("done")

    def test_stream_registered_in_active_streams(self):
        class StreamAPI:
            def stream_predict(self, x):
                yield "a"
                yield "b"

        sock = SyncSocket()
        active = _StreamTracker()
        stream_req = StreamRequest(
            stream_id="s-reg",
            open=StreamOpen(data=b'{}'),
        )
        inference._handle_stream_open(StreamAPI(), stream_req, sock, active, log)
        # Stream is registered immediately, then consumed by background thread.
        # Wait for done signal to verify the stream was processed.
        sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == "s-reg"
        )
        # Verify stream was cleaned up after completion
        assert "s-reg" not in active._streams

    def test_stream_predict_error_sends_stream_error(self):
        class ErrorStreamAPI:
            def stream_predict(self, x):
                yield "ok"
                raise RuntimeError("mid-stream failure")

        sock = SyncSocket()
        stream_req = StreamRequest(
            stream_id="s-mid-err",
            open=StreamOpen(data=b'{}'),
        )
        inference._handle_stream_open(ErrorStreamAPI(), stream_req, sock, _StreamTracker(), log)

        err_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("error") and r.stream.stream_id == "s-mid-err"
        )
        assert len(err_resps) >= 1

        responses = sock.get_stream_responses("s-mid-err")
        # chunk("ok") + error
        chunk_resps = [r for r in responses if r.stream.HasField("chunk")]
        error_resps = [r for r in responses if r.stream.HasField("error")]
        assert len(chunk_resps) == 1
        assert len(error_resps) == 1
        assert "mid-stream failure" in error_resps[0].stream.error.message

    def test_stream_predict_init_error(self):
        class InitFailAPI:
            def stream_predict(self, x):
                raise RuntimeError("cannot start")
                yield  # noqa: make it a generator function

        sock = SyncSocket()
        stream_req = StreamRequest(
            stream_id="s-init-err",
            open=StreamOpen(data=b'{}'),
        )
        inference._handle_stream_open(InitFailAPI(), stream_req, sock, _StreamTracker(), log)

        err_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("error") and r.stream.stream_id == "s-init-err"
        )
        assert len(err_resps) == 1
        assert "cannot start" in err_resps[0].stream.error.message


# ---------------------------------------------------------------------------
# Streaming with hooks
# ---------------------------------------------------------------------------

class TestHandleStreamOpenWithHooks:
    def test_decode_request_applied(self):
        class DecodeStreamAPI:
            def decode_request(self, raw):
                return {"decoded": True, "val": raw.get("val", 0)}

            def stream_predict(self, x):
                yield {"out": x["val"] + 10}

        sock = SyncSocket()
        stream_req = StreamRequest(
            stream_id="s-dec",
            open=StreamOpen(data=json.dumps({"val": 5}).encode()),
        )
        inference._handle_stream_open(DecodeStreamAPI(), stream_req, sock, _StreamTracker(), log)

        done_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == "s-dec"
        )
        assert len(done_resps) >= 1

        responses = sock.get_stream_responses("s-dec")
        chunk_body = json.loads(responses[0].stream.chunk.data)
        assert chunk_body["out"] == 15

    def test_on_request_hook_applied(self):
        class HookStreamAPI:
            def on_request(self, request, meta):
                request["injected"] = True
                return request

            def stream_predict(self, x):
                yield x

        sock = SyncSocket()
        meta = ProtoMeta(route="/predict", headers={}, client_ip="", request_id="r1", timestamp_ns=0)
        stream_req = StreamRequest(
            stream_id="s-hook",
            open=StreamOpen(data=json.dumps({"q": 1}).encode(), meta=meta),
        )
        inference._handle_stream_open(HookStreamAPI(), stream_req, sock, _StreamTracker(), log)

        done_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == "s-hook"
        )
        assert len(done_resps) >= 1

        responses = sock.get_stream_responses("s-hook")
        chunk_body = json.loads(responses[0].stream.chunk.data)
        assert chunk_body["injected"] is True

    def test_on_request_reject_sends_error(self):
        class RejectStreamAPI:
            def on_request(self, request, meta):
                raise ValueError("auth failed")

            def stream_predict(self, x):
                yield x

        sock = SyncSocket()
        meta = ProtoMeta(route="/predict", headers={}, client_ip="", request_id="r1", timestamp_ns=0)
        stream_req = StreamRequest(
            stream_id="s-reject",
            open=StreamOpen(data=b'{}', meta=meta),
        )
        inference._handle_stream_open(RejectStreamAPI(), stream_req, sock, _StreamTracker(), log)

        err_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("error") and r.stream.stream_id == "s-reject"
        )
        assert len(err_resps) == 1
        assert "auth failed" in err_resps[0].stream.error.message

    def test_on_response_hook_applied(self):
        class OnResponseStreamAPI:
            def on_response(self, response, meta):
                response["tagged"] = meta.route
                return response

            def stream_predict(self, x):
                yield {"a": 1}
                yield {"b": 2}

        sock = SyncSocket()
        meta = ProtoMeta(route="/stream", headers={}, client_ip="", request_id="r2", timestamp_ns=0)
        stream_req = StreamRequest(
            stream_id="s-onresp",
            open=StreamOpen(data=b'{}', meta=meta),
        )
        inference._handle_stream_open(OnResponseStreamAPI(), stream_req, sock, _StreamTracker(), log)

        done_resps = sock.wait_for(
            lambda r: r.HasField("stream") and r.stream.HasField("done") and r.stream.stream_id == "s-onresp"
        )
        assert len(done_resps) >= 1

        responses = sock.get_stream_responses("s-onresp")
        chunk_resps = [r for r in responses if r.stream.HasField("chunk")]
        assert len(chunk_resps) == 2

        body0 = json.loads(chunk_resps[0].stream.chunk.data)
        assert body0["a"] == 1
        assert body0["tagged"] == "/stream"

        body1 = json.loads(chunk_resps[1].stream.chunk.data)
        assert body1["b"] == 2
        assert body1["tagged"] == "/stream"


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------

class TestHandleStreamCancel:
    def test_cancel_removes_from_active(self):
        gen = iter([1, 2, 3])
        sock = SyncSocket()
        active = _StreamTracker()
        active.add("s1", gen)
        inference._handle_stream_close("s1", active, sock, log)
        assert "s1" not in active._streams

    def test_cancel_nonexistent_is_noop(self):
        sock = SyncSocket()
        active = _StreamTracker()
        inference._handle_stream_close("nope", active, sock, log)
        assert "nope" not in active._streams

    def test_cancel_closes_generator(self):
        closed = threading.Event()

        def gen():
            try:
                yield 1
                yield 2
            except GeneratorExit:
                closed.set()
                raise

        g = gen()
        next(g)
        sock = SyncSocket()
        active = _StreamTracker()
        active.add("s-close", g)
        inference._handle_stream_close("s-close", active, sock, log)
        assert closed.wait(timeout=2.0)


# ---------------------------------------------------------------------------
# run_standard_loop with streaming
# ---------------------------------------------------------------------------

class TestRunStandardLoopStream:
    """Integration: run_standard_loop handling stream requests."""

    def _make_stream_open_bytes(self, stream_id: str, data: bytes = b"{}") -> bytes:
        req = Request(
            uid=f"stream-open-{stream_id}",
            stream=StreamRequest(
                stream_id=stream_id,
                open=StreamOpen(data=data),
            ),
        )
        return req.SerializeToString()

    def _make_stream_cancel_bytes(self, stream_id: str) -> bytes:
        req = Request(
            uid=f"stream-cancel-{stream_id}",
            stream=StreamRequest(
                stream_id=stream_id,
                cancel=StreamCancel(),
            ),
        )
        return req.SerializeToString()

    def test_stream_open_then_done(self):
        """Stream request with stream_predict yields chunks then done."""
        done_event = threading.Event()

        class StreamAPI:
            def stream_predict(self, x):
                for i in range(2):
                    yield {"n": i}

        sent_responses = []
        lock = threading.Lock()

        def capture_send(data):
            resp = Response()
            resp.ParseFromString(data)
            with lock:
                sent_responses.append(resp)
            if resp.HasField("stream") and resp.stream.HasField("done"):
                done_event.set()

        socket = MagicMock()
        socket.send.side_effect = capture_send

        recv_count = {"n": 0}

        def mock_recv():
            recv_count["n"] += 1
            if recv_count["n"] == 1:
                return self._make_stream_open_bytes("s-loop")
            # Wait for stream to finish before exiting
            done_event.wait(timeout=5.0)
            raise KeyboardInterrupt()

        socket.recv.side_effect = mock_recv

        with pytest.raises(KeyboardInterrupt):
            inference.run_standard_loop(StreamAPI(), socket, "test-model", log)

        with lock:
            stream_resps = [r for r in sent_responses if r.HasField("stream") and r.stream.stream_id == "s-loop"]
        chunk_resps = [r for r in stream_resps if r.stream.HasField("chunk")]
        done_resps = [r for r in stream_resps if r.stream.HasField("done")]
        assert len(chunk_resps) == 2
        assert len(done_resps) == 1

    def test_stream_cancel_during_streaming(self):
        """Cancel a stream mid-flight."""
        first_chunk_sent = threading.Event()
        block_generator = threading.Event()  # not set → generator blocks

        class SlowStreamAPI:
            def stream_predict(self, x):
                yield "first"
                # Block here until test signals to finish
                block_generator.wait(timeout=5.0)
                yield "second"

        sent_responses = []
        lock = threading.Lock()

        def capture_send(data):
            resp = Response()
            resp.ParseFromString(data)
            with lock:
                sent_responses.append(resp)
            # Signal after first chunk is sent
            if (resp.HasField("stream") and resp.stream.HasField("chunk")
                    and resp.stream.chunk.data == b'"first"'):
                first_chunk_sent.set()

        socket = MagicMock()
        socket.send.side_effect = capture_send

        recv_count = {"n": 0}

        def mock_recv():
            recv_count["n"] += 1
            if recv_count["n"] == 1:
                return self._make_stream_open_bytes("s-cancel")
            elif recv_count["n"] == 2:
                # Wait for first chunk to be sent, then send cancel
                first_chunk_sent.wait(timeout=5.0)
                return self._make_stream_cancel_bytes("s-cancel")
            else:
                # Let generator finish (in case cancel didn't close it)
                block_generator.set()
                raise KeyboardInterrupt()

        socket.recv.side_effect = mock_recv

        with pytest.raises(KeyboardInterrupt):
            inference.run_standard_loop(SlowStreamAPI(), socket, "test-model", log)

        # Unblock generator in case it's still waiting
        block_generator.set()

        with lock:
            stream_resps = [r for r in sent_responses if r.HasField("stream") and r.stream.stream_id == "s-cancel"]
        chunk_resps = [r for r in stream_resps if r.stream.HasField("chunk")]
        # Only the first chunk should arrive (second blocked by block_generator)
        assert len(chunk_resps) == 1
