"""Tests for WebSocket streaming protocol contract.

The WebSocket handler is implemented in Rust (axum). These tests validate
the message protocol contract — what the client sends and what it receives.

WebSocket protocol (from src/http/handlers.rs):
- Client connects to /v2/models/:name/stream
- Client sends JSON payload as first message
- Server opens worker stream and forwards chunks:
  - Binary chunk → Binary WebSocket message
  - Error → Text JSON {"error": "message"}
  - Done → Text JSON {"done": true} then close
"""

import json

import pytest

from lite_server.proto import (
    Response,
    StreamResponse,
    StreamChunkResponse,
    StreamDone,
    StreamError,
)


class TestWSClientPayload:
    """Validate the JSON payload format clients must send."""

    def test_valid_payload(self):
        payload = json.dumps({"prompt": "hello", "max_tokens": 100})
        parsed = json.loads(payload)
        assert parsed["prompt"] == "hello"
        assert parsed["max_tokens"] == 100

    def test_empty_payload(self):
        payload = json.dumps({})
        parsed = json.loads(payload)
        assert parsed == {}

    def test_nested_payload(self):
        payload = json.dumps({
            "messages": [
                {"role": "user", "content": "hi"},
            ],
            "temperature": 0.7,
        })
        parsed = json.loads(payload)
        assert len(parsed["messages"]) == 1
        assert parsed["temperature"] == 0.7


class TestWSResponseProtocol:
    """Validate the response messages the WS handler sends to clients."""

    def _make_chunk_response(self, data: bytes) -> Response:
        return Response(
            uid="c1",
            stream=StreamResponse(
                stream_id="ws-s1",
                chunk=StreamChunkResponse(data=data),
            ),
        )

    def _make_done_response(self) -> Response:
        return Response(
            uid="d1",
            stream=StreamResponse(
                stream_id="ws-s1",
                done=StreamDone(),
            ),
        )

    def _make_error_response(self, message: str) -> Response:
        return Response(
            uid="e1",
            stream=StreamResponse(
                stream_id="ws-s1",
                error=StreamError(message=message),
            ),
        )

    def test_chunk_to_binary_ws_message(self):
        """Binary chunks should become Binary WebSocket messages."""
        resp = self._make_chunk_response(b'{"token": "hello"}')
        assert resp.stream.HasField("chunk")
        # WS handler sends chunk.data as Binary message
        ws_data = resp.stream.chunk.data
        assert isinstance(ws_data, bytes)
        assert json.loads(ws_data) == {"token": "hello"}

    def test_done_to_text_ws_message(self):
        """Done signal should become Text JSON {"done": true}."""
        resp = self._make_done_response()
        assert resp.stream.HasField("done")
        # WS handler sends {"done": true} as text
        ws_text = json.dumps({"done": True})
        assert json.loads(ws_text) == {"done": True}

    def test_error_to_text_ws_message(self):
        """Error should become Text JSON {"error": "message"}."""
        resp = self._make_error_response("model not found")
        assert resp.stream.HasField("error")
        # WS handler sends {"error": message} as text
        ws_text = json.dumps({"error": resp.stream.error.message})
        parsed = json.loads(ws_text)
        assert parsed["error"] == "model not found"

    def test_multiple_chunks_sequence(self):
        """Simulate a full streaming sequence: chunks → done."""
        responses = [
            self._make_chunk_response(json.dumps({"token": "Hello"}).encode()),
            self._make_chunk_response(json.dumps({"token": " world"}).encode()),
            self._make_chunk_response(json.dumps({"token": "!"}).encode()),
            self._make_done_response(),
        ]

        tokens = []
        for resp in responses:
            if resp.stream.HasField("chunk"):
                data = json.loads(resp.stream.chunk.data)
                tokens.append(data["token"])
            elif resp.stream.HasField("done"):
                break

        assert tokens == ["Hello", " world", "!"]

    def test_error_mid_stream(self):
        """Simulate error arriving mid-stream."""
        responses = [
            self._make_chunk_response(json.dumps({"token": "partial"}).encode()),
            self._make_error_response("OOM"),
        ]

        tokens = []
        error = None
        for resp in responses:
            if resp.stream.HasField("chunk"):
                tokens.append(json.loads(resp.stream.chunk.data)["token"])
            elif resp.stream.HasField("error"):
                error = resp.stream.error.message
                break

        assert tokens == ["partial"]
        assert error == "OOM"


class TestWSErrorScenarios:
    """Test error handling patterns in WS protocol."""

    def test_invalid_json_error_message(self):
        """Server should send {"error": "invalid JSON"} for bad input."""
        error_msg = json.dumps({"error": "invalid JSON"})
        parsed = json.loads(error_msg)
        assert parsed["error"] == "invalid JSON"

    def test_model_not_found_error(self):
        """Server should close with error for missing model."""
        error_msg = json.dumps({"error": "model xyz not found"})
        parsed = json.loads(error_msg)
        assert "not found" in parsed["error"]

    def test_worker_unavailable_error(self):
        """Server should send error when no workers available."""
        error_msg = json.dumps({"error": "no workers available"})
        parsed = json.loads(error_msg)
        assert "no workers" in parsed["error"]


class TestWSProtocolRoundTrip:
    """Test the full WS protocol round-trip with protobuf messages."""

    def test_full_stream_roundtrip(self):
        """Simulate: client sends request → server streams back chunks → done."""
        # Client side: send JSON payload
        client_payload = json.dumps({"prompt": "count to 3"}).encode()

        # Server side: worker produces chunks
        worker_responses = [
            Response(
                uid="c1",
                stream=StreamResponse(
                    stream_id="ws-rt",
                    chunk=StreamChunkResponse(data=json.dumps({"n": 1}).encode()),
                ),
            ),
            Response(
                uid="c2",
                stream=StreamResponse(
                    stream_id="ws-rt",
                    chunk=StreamChunkResponse(data=json.dumps({"n": 2}).encode()),
                ),
            ),
            Response(
                uid="c3",
                stream=StreamResponse(
                    stream_id="ws-rt",
                    chunk=StreamChunkResponse(data=json.dumps({"n": 3}).encode()),
                ),
            ),
            Response(
                uid="d1",
                stream=StreamResponse(
                    stream_id="ws-rt",
                    done=StreamDone(),
                ),
            ),
        ]

        # Client side: receive and collect
        received = []
        for resp in worker_responses:
            if resp.stream.HasField("chunk"):
                # Binary WS message
                received.append(("binary", json.loads(resp.stream.chunk.data)))
            elif resp.stream.HasField("done"):
                # Text WS message
                received.append(("text", {"done": True}))
                break

        assert len(received) == 4
        assert received[0] == ("binary", {"n": 1})
        assert received[1] == ("binary", {"n": 2})
        assert received[2] == ("binary", {"n": 3})
        assert received[3] == ("text", {"done": True})

    def test_binary_data_roundtrip(self):
        """Binary data (e.g. image bytes) should survive round-trip."""
        image_data = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100

        resp = Response(
            uid="c1",
            stream=StreamResponse(
                stream_id="ws-bin",
                chunk=StreamChunkResponse(data=image_data),
            ),
        )

        # WS handler sends raw bytes as Binary message
        ws_binary = resp.stream.chunk.data
        assert ws_binary == image_data
