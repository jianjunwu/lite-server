"""Tests for gRPC-related protobuf messages used by the lite-server protocol.

Since GrpcService is implemented in Rust, these tests validate:
1. InferRequest / InferResponse serialization
2. BatchInferRequest / BatchInferResponse serialization
3. StreamInferRequest / StreamChunk serialization
4. BidiChunk serialization (open/data/close)
5. Error status propagation in gRPC responses
"""

import json

import pytest

from lite_server.proto import (
    InferRequest,
    InferResponse,
    BatchInferRequest,
    BatchInferResponse,
    StreamInferRequest,
    StreamChunk,
    BidiChunk,
    BidiOpen,
    BidiData,
    BidiClose,
    Status,
    Metrics,
    RequestMeta,
    Request,
    Response,
    SingleRequest,
    SingleResponse,
    BatchRequest,
    BatchItem,
    BatchItemResponse,
    StreamRequest,
    StreamResponse,
    StreamOpen,
    StreamChunkResponse,
    StreamDone,
    StreamError,
)


class TestInferRequestSerialization:
    def test_basic_infer_request(self):
        req = InferRequest(
            model_name="my_model",
            version="1",
            data=json.dumps({"prompt": "hello"}).encode(),
        )
        raw = req.SerializeToString()
        parsed = InferRequest()
        parsed.ParseFromString(raw)
        assert parsed.model_name == "my_model"
        assert parsed.version == "1"
        assert json.loads(parsed.data) == {"prompt": "hello"}

    def test_infer_request_with_headers(self):
        req = InferRequest(
            model_name="m",
            version="",
            data=b"input",
            headers={"authorization": "Bearer tok", "x-request-id": "abc"},
        )
        raw = req.SerializeToString()
        parsed = InferRequest()
        parsed.ParseFromString(raw)
        assert parsed.headers["authorization"] == "Bearer tok"
        assert parsed.headers["x-request-id"] == "abc"

    def test_infer_request_empty_version(self):
        req = InferRequest(model_name="m", data=b"x")
        raw = req.SerializeToString()
        parsed = InferRequest()
        parsed.ParseFromString(raw)
        assert parsed.version == ""


class TestInferResponseSerialization:
    def test_success_response(self):
        resp = InferResponse(
            data=json.dumps({"output": 42}).encode(),
            status=Status(code="Ok", message=""),
        )
        raw = resp.SerializeToString()
        parsed = InferResponse()
        parsed.ParseFromString(raw)
        assert parsed.status.code == "Ok"
        assert json.loads(parsed.data) == {"output": 42}

    def test_error_response(self):
        resp = InferResponse(
            data=b"",
            status=Status(code="Error", message="model crashed"),
        )
        raw = resp.SerializeToString()
        parsed = InferResponse()
        parsed.ParseFromString(raw)
        assert parsed.status.code == "Error"
        assert parsed.status.message == "model crashed"

    def test_response_with_metrics(self):
        resp = InferResponse(
            data=b"ok",
            status=Status(code="Ok"),
            metrics=Metrics(prefill_ms=1.5, decode_ms=0.3, tokens_generated=10),
        )
        raw = resp.SerializeToString()
        parsed = InferResponse()
        parsed.ParseFromString(raw)
        assert parsed.metrics.prefill_ms == pytest.approx(1.5, abs=1e-6)
        assert parsed.metrics.decode_ms == pytest.approx(0.3, abs=1e-6)
        assert parsed.metrics.tokens_generated == 10


class TestBatchInferRequestSerialization:
    def test_batch_request(self):
        req = BatchInferRequest(
            model_name="m",
            version="1",
            items=[b'{"a": 1}', b'{"b": 2}', b'{"c": 3}'],
        )
        raw = req.SerializeToString()
        parsed = BatchInferRequest()
        parsed.ParseFromString(raw)
        assert len(parsed.items) == 3
        assert json.loads(parsed.items[0]) == {"a": 1}

    def test_batch_request_with_headers(self):
        req = BatchInferRequest(
            model_name="m",
            items=[b"x"],
            headers={"x-auth": "tok"},
        )
        raw = req.SerializeToString()
        parsed = BatchInferRequest()
        parsed.ParseFromString(raw)
        assert parsed.headers["x-auth"] == "tok"


class TestBatchInferResponseSerialization:
    def test_batch_response(self):
        resp = BatchInferResponse(items=[
            InferResponse(data=b'{"r": 1}', status=Status(code="Ok")),
            InferResponse(data=b'{"r": 2}', status=Status(code="Ok")),
        ])
        raw = resp.SerializeToString()
        parsed = BatchInferResponse()
        parsed.ParseFromString(raw)
        assert len(parsed.items) == 2
        assert json.loads(parsed.items[1].data) == {"r": 2}

    def test_batch_response_partial_error(self):
        resp = BatchInferResponse(items=[
            InferResponse(data=b'{"r": 1}', status=Status(code="Ok")),
            InferResponse(data=b"", status=Status(code="Error", message="timeout")),
        ])
        raw = resp.SerializeToString()
        parsed = BatchInferResponse()
        parsed.ParseFromString(raw)
        assert parsed.items[0].status.code == "Ok"
        assert parsed.items[1].status.code == "Error"
        assert parsed.items[1].status.message == "timeout"


class TestStreamInferRequestSerialization:
    def test_stream_infer_request(self):
        req = StreamInferRequest(
            model_name="llm",
            version="2",
            data=json.dumps({"prompt": "tell me a story"}).encode(),
        )
        raw = req.SerializeToString()
        parsed = StreamInferRequest()
        parsed.ParseFromString(raw)
        assert parsed.model_name == "llm"
        assert parsed.version == "2"
        assert json.loads(parsed.data) == {"prompt": "tell me a story"}

    def test_stream_infer_request_with_headers(self):
        req = StreamInferRequest(
            model_name="m",
            data=b"x",
            headers={"content-type": "application/json"},
        )
        raw = req.SerializeToString()
        parsed = StreamInferRequest()
        parsed.ParseFromString(raw)
        assert parsed.headers["content-type"] == "application/json"


class TestStreamChunkSerialization:
    def test_stream_chunk(self):
        chunk = StreamChunk(data=json.dumps({"token": "hello"}).encode())
        raw = chunk.SerializeToString()
        parsed = StreamChunk()
        parsed.ParseFromString(raw)
        assert json.loads(parsed.data) == {"token": "hello"}

    def test_stream_chunk_binary_data(self):
        chunk = StreamChunk(data=b"\x00\x01\x02\xff")
        raw = chunk.SerializeToString()
        parsed = StreamChunk()
        parsed.ParseFromString(raw)
        assert parsed.data == b"\x00\x01\x02\xff"


class TestBidiChunkSerialization:
    def test_bidi_open(self):
        chunk = BidiChunk(
            stream_id="bidi-1",
            open=BidiOpen(
                model_name="asr",
                version="1",
                initial_data=b"audio_header",
            ),
        )
        raw = chunk.SerializeToString()
        parsed = BidiChunk()
        parsed.ParseFromString(raw)
        assert parsed.stream_id == "bidi-1"
        assert parsed.HasField("open")
        assert parsed.open.model_name == "asr"
        assert parsed.open.initial_data == b"audio_header"

    def test_bidi_data(self):
        chunk = BidiChunk(
            stream_id="bidi-1",
            data=BidiData(data=b"audio_chunk_2"),
        )
        raw = chunk.SerializeToString()
        parsed = BidiChunk()
        parsed.ParseFromString(raw)
        assert parsed.HasField("data")
        assert parsed.data.data == b"audio_chunk_2"

    def test_bidi_close(self):
        chunk = BidiChunk(
            stream_id="bidi-1",
            close=BidiClose(),
        )
        raw = chunk.SerializeToString()
        parsed = BidiChunk()
        parsed.ParseFromString(raw)
        assert parsed.HasField("close")

    def test_bidi_which_oneof(self):
        assert BidiChunk(stream_id="x", open=BidiOpen(model_name="m", initial_data=b"")).WhichOneof("payload") == "open"
        assert BidiChunk(stream_id="x", data=BidiData(data=b"d")).WhichOneof("payload") == "data"
        assert BidiChunk(stream_id="x", close=BidiClose()).WhichOneof("payload") == "close"


class TestInternalRequestMapping:
    """Test how gRPC requests map to internal protobuf Request messages (as done in GrpcService)."""

    def test_infer_to_single_request(self):
        """Simulates how GrpcService.infer builds an internal Request."""
        infer_req = InferRequest(
            model_name="m",
            version="1",
            data=json.dumps({"x": 1}).encode(),
            headers={"h": "v"},
        )

        meta = RequestMeta(
            route="/predict",
            headers=dict(infer_req.headers),
            client_ip="",
            request_id="req-1",
            timestamp_ns=100,
            payload=infer_req.data,
        )
        internal = Request(
            uid="grpc-m-uuid",
            meta=meta,
            single=SingleRequest(data=infer_req.data),
        )

        raw = internal.SerializeToString()
        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.HasField("single")
        assert parsed.single.data == infer_req.data
        assert parsed.meta.route == "/predict"
        assert parsed.meta.headers["h"] == "v"

    def test_batch_infer_to_batch_request(self):
        """Simulates how GrpcService.batch_infer builds an internal Request."""
        batch_req = BatchInferRequest(
            model_name="m",
            version="1",
            items=[b'{"a": 1}', b'{"b": 2}'],
        )

        items = [
            BatchItem(uid=f"grpc-batch-{i}", data=item)
            for i, item in enumerate(batch_req.items)
        ]
        internal = Request(
            uid="grpc-batch-uuid",
            meta=RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0),
            batch=BatchRequest(items=items),
        )

        raw = internal.SerializeToString()
        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.HasField("batch")
        assert len(parsed.batch.items) == 2
        assert parsed.batch.items[0].uid == "grpc-batch-0"
        assert parsed.batch.items[1].data == b'{"b": 2}'

    def test_stream_infer_to_stream_request(self):
        """Simulates how GrpcService.stream_infer builds an internal Request."""
        stream_req = StreamInferRequest(
            model_name="llm",
            data=b"prompt",
        )
        stream_id = "grpc-stream-uuid"

        meta = RequestMeta(
            route="/predict",
            headers={},
            client_ip="",
            request_id="r1",
            timestamp_ns=0,
            payload=stream_req.data,
        )
        internal = Request(
            uid=f"stream-open-{stream_id}",
            meta=meta,
            stream=StreamRequest(
                stream_id=stream_id,
                open=StreamOpen(data=stream_req.data, meta=meta),
            ),
        )

        raw = internal.SerializeToString()
        parsed = Request()
        parsed.ParseFromString(raw)
        assert parsed.HasField("stream")
        assert parsed.stream.HasField("open")
        assert parsed.stream.open.data == b"prompt"

    def test_worker_response_to_grpc_response(self):
        """Simulates converting worker Response back to InferResponse."""
        worker_resp = Response(
            uid="grpc-m-uuid",
            single=SingleResponse(
                data=json.dumps({"result": 42}).encode(),
                status=Status(code="Ok"),
            ),
        )

        # Extract as GrpcService does
        assert worker_resp.HasField("single")
        single = worker_resp.single
        grpc_resp = InferResponse(
            data=single.data,
            status=single.status,
        )
        assert grpc_resp.status.code == "Ok"
        assert json.loads(grpc_resp.data) == {"result": 42}

    def test_worker_stream_to_grpc_chunks(self):
        """Simulates converting worker StreamResponse chunks to StreamChunk."""
        worker_chunks = [
            Response(
                uid="c1",
                stream=StreamResponse(
                    stream_id="s1",
                    chunk=StreamChunkResponse(data=b'{"t": "h"}'),
                ),
            ),
            Response(
                uid="c2",
                stream=StreamResponse(
                    stream_id="s1",
                    chunk=StreamChunkResponse(data=b'{"t": "i"}'),
                ),
            ),
            Response(
                uid="c3",
                stream=StreamResponse(stream_id="s1", done=StreamDone()),
            ),
        ]

        grpc_chunks = []
        for chunk in worker_chunks:
            if chunk.stream.HasField("chunk"):
                grpc_chunks.append(StreamChunk(data=chunk.stream.chunk.data))
            elif chunk.stream.HasField("done"):
                break  # gRPC stream ends

        assert len(grpc_chunks) == 2
        assert json.loads(grpc_chunks[0].data) == {"t": "h"}
        assert json.loads(grpc_chunks[1].data) == {"t": "i"}
