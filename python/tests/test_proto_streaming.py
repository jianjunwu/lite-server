"""Tests for protobuf streaming message serialization/deserialization."""

import json

from lite_server.proto import (
    Request,
    Response,
    StreamRequest,
    StreamResponse,
    StreamOpen,
    StreamChunk,
    StreamClose,
    StreamCancel,
    StreamChunkResponse,
    StreamDone,
    StreamError,
    RequestMeta,
    SingleRequest,
    Status,
)


class TestStreamRequestOpen:
    def test_serialize_open_with_data(self):
        meta = RequestMeta(
            route="/predict",
            headers={"x-auth": "tok"},
            client_ip="1.2.3.4",
            request_id="r1",
            timestamp_ns=100,
            payload=b'{"prompt": "hi"}',
        )
        req = Request(
            uid="stream-open-s1",
            meta=meta,
            stream=StreamRequest(
                stream_id="s1",
                open=StreamOpen(data=b'{"prompt": "hi"}', meta=meta),
            ),
        )
        raw = req.SerializeToString()
        assert len(raw) > 0

        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.uid == "stream-open-s1"
        assert parsed.HasField("stream")
        assert parsed.stream.stream_id == "s1"
        assert parsed.stream.HasField("open")
        assert parsed.stream.open.data == b'{"prompt": "hi"}'
        assert parsed.stream.open.meta.headers["x-auth"] == "tok"

    def test_serialize_open_without_meta(self):
        req = Request(
            uid="stream-open-s2",
            stream=StreamRequest(
                stream_id="s2",
                open=StreamOpen(data=b"hello"),
            ),
        )
        raw = req.SerializeToString()
        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.stream.open.data == b"hello"
        assert not parsed.stream.open.HasField("meta")


class TestStreamRequestChunk:
    def test_serialize_chunk(self):
        req = Request(
            uid="stream-chunk-s1",
            stream=StreamRequest(
                stream_id="s1",
                chunk=StreamChunk(data=b"chunk-data"),
            ),
        )
        raw = req.SerializeToString()
        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.stream.stream_id == "s1"
        assert parsed.stream.HasField("chunk")
        assert parsed.stream.chunk.data == b"chunk-data"


class TestStreamRequestClose:
    def test_serialize_close(self):
        req = Request(
            uid="stream-close-s1",
            stream=StreamRequest(
                stream_id="s1",
                close=StreamClose(),
            ),
        )
        raw = req.SerializeToString()
        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.stream.HasField("close")


class TestStreamRequestCancel:
    def test_serialize_cancel(self):
        req = Request(
            uid="stream-cancel-s1",
            stream=StreamRequest(
                stream_id="s1",
                cancel=StreamCancel(),
            ),
        )
        raw = req.SerializeToString()
        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.stream.HasField("cancel")


class TestStreamResponseChunk:
    def test_serialize_chunk_response(self):
        resp = Response(
            uid="stream-chunk-s1",
            stream=StreamResponse(
                stream_id="s1",
                chunk=StreamChunkResponse(data=b'{"token": "hello"}', is_final=False),
            ),
        )
        raw = resp.SerializeToString()
        parsed = Response()
        parsed.ParseFromString(raw)
        assert parsed.stream.stream_id == "s1"
        assert parsed.stream.chunk.data == b'{"token": "hello"}'
        assert parsed.stream.chunk.is_final is False

    def test_serialize_final_chunk(self):
        resp = Response(
            uid="stream-chunk-s1",
            stream=StreamResponse(
                stream_id="s1",
                chunk=StreamChunkResponse(data=b"last", is_final=True),
            ),
        )
        raw = resp.SerializeToString()
        parsed = Response()
        parsed.ParseFromString(raw)
        assert parsed.stream.chunk.is_final is True


class TestStreamResponseDone:
    def test_serialize_done(self):
        resp = Response(
            uid="stream-done-s1",
            stream=StreamResponse(
                stream_id="s1",
                done=StreamDone(),
            ),
        )
        raw = resp.SerializeToString()
        parsed = Response()
        parsed.ParseFromString(raw)
        assert parsed.stream.HasField("done")


class TestStreamResponseError:
    def test_serialize_error(self):
        resp = Response(
            uid="stream-error-s1",
            stream=StreamResponse(
                stream_id="s1",
                error=StreamError(message="something went wrong"),
            ),
        )
        raw = resp.SerializeToString()
        parsed = Response()
        parsed.ParseFromString(raw)
        assert parsed.stream.HasField("error")
        assert parsed.stream.error.message == "something went wrong"


class TestStreamResponseWhichOneof:
    def test_which_oneof_chunk(self):
        resp = Response(
            uid="x",
            stream=StreamResponse(
                stream_id="s1",
                chunk=StreamChunkResponse(data=b"data"),
            ),
        )
        assert resp.stream.WhichOneof("payload") == "chunk"

    def test_which_oneof_done(self):
        resp = Response(
            uid="x",
            stream=StreamResponse(stream_id="s1", done=StreamDone()),
        )
        assert resp.stream.WhichOneof("payload") == "done"

    def test_which_oneof_error(self):
        resp = Response(
            uid="x",
            stream=StreamResponse(stream_id="s1", error=StreamError(message="e")),
        )
        assert resp.stream.WhichOneof("payload") == "error"


class TestRequestWhichOneof:
    def test_which_oneof_stream(self):
        req = Request(
            uid="x",
            stream=StreamRequest(stream_id="s1", open=StreamOpen(data=b"")),
        )
        assert req.WhichOneof("payload") == "stream"

    def test_which_oneof_single(self):
        req = Request(uid="x", single=SingleRequest(data=b"hi"))
        assert req.WhichOneof("payload") == "single"
