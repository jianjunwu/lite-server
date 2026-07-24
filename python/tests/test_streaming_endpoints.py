"""TDD tests for custom endpoint streaming support (P1).

Tests are written BEFORE implementation. They should all FAIL initially.
"""

import asyncio
import json
import struct

import pytest

from lite_server.specs.openai import OpenAIEndpoint


def _envelope(body: dict, request_id: str = "") -> dict:
    """The EndpointRequest envelope handle() receives at dispatch time."""
    return {
        "method": "POST",
        "route": "/v1/chat/completions",
        "headers": {},
        "query": {},
        "body": body,
        "request_id": request_id,
    }


# ===== Streaming Test Models =====

class StreamingChatEndpoint(OpenAIEndpoint):
    """Model that yields tokens one by one."""

    def setup(self):
        pass

    def decode_request(self, request):
        messages = request.get("messages", [])
        return messages[-1]["content"] if messages else ""

    def predict(self, x):
        # Non-streaming fallback
        return f"Echo: {x}"

    async def stream_predict(self, x):
        """Yield tokens one by one."""
        for token in f"Echo: {x}".split():
            yield {"choices": [{"delta": {"content": token + " "}, "index": 0}]}
            await asyncio.sleep(0)  # yield control
        yield {"choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}]}


class NonStreamingEndpoint(OpenAIEndpoint):
    """Model without stream_predict — should fall back to single response."""

    def setup(self):
        pass

    def decode_request(self, request):
        return request.get("messages", [{}])[0].get("content", "")

    def predict(self, x):
        return f"Response: {x}"


class BrokenStreamEndpoint(OpenAIEndpoint):
    """Model whose stream_predict raises mid-stream."""

    def setup(self):
        pass

    def decode_request(self, request):
        return request.get("messages", [{}])[0].get("content", "")

    def predict(self, x):
        return x

    async def stream_predict(self, x):
        yield {"choices": [{"delta": {"content": "ok"}, "index": 0}]}
        raise RuntimeError("stream broken")


# ===== OpenAIEndpoint Streaming Tests =====

class TestStreamPredict:
    @pytest.mark.asyncio
    async def test_stream_request_with_stream_true(self):
        ep = StreamingChatEndpoint()
        request = {
            "request_id": "s1",
            "messages": [{"role": "user", "content": "hello world"}],
            "stream": True,
        }
        response = await ep.handle(_envelope(request))

        # Should indicate streaming
        assert response["status_code"] == 200
        assert response["stream"] is True
        assert "chunks" in response
        assert isinstance(response["chunks"], list)
        assert len(response["chunks"]) > 0

    @pytest.mark.asyncio
    async def test_stream_chunks_contain_deltas(self):
        ep = StreamingChatEndpoint()
        request = {
            "request_id": "s2",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        }
        response = await ep.handle(_envelope(request))
        chunks = response["chunks"]

        # Each chunk should have OpenAI streaming format
        for chunk in chunks[:-1]:  # all except last
            assert "choices" in chunk
            assert "delta" in chunk["choices"][0]

    @pytest.mark.asyncio
    async def test_stream_ends_with_finish_reason(self):
        ep = StreamingChatEndpoint()
        request = {
            "request_id": "s3",
            "messages": [{"role": "user", "content": "test"}],
            "stream": True,
        }
        response = await ep.handle(_envelope(request))
        chunks = response["chunks"]
        last_chunk = chunks[-1]
        assert last_chunk["choices"][0].get("finish_reason") == "stop"

    @pytest.mark.asyncio
    async def test_non_streaming_falls_back_to_single_response(self):
        ep = NonStreamingEndpoint()
        request = {
            "request_id": "s4",
            "messages": [{"role": "user", "content": "test"}],
            "stream": True,
        }
        response = await ep.handle(_envelope(request))
        # Should fall back to single response wrapped as stream
        assert response["status_code"] == 200
        assert response["stream"] is True
        chunks = response["chunks"]
        assert len(chunks) >= 1

    @pytest.mark.asyncio
    async def test_stream_false_returns_normal_response(self):
        ep = StreamingChatEndpoint()
        request = {
            "request_id": "s5",
            "messages": [{"role": "user", "content": "test"}],
            "stream": False,
        }
        response = await ep.handle(_envelope(request))
        assert response["status_code"] == 200
        assert "stream" not in response
        assert "body" in response

    @pytest.mark.asyncio
    async def test_stream_error_mid_stream(self):
        ep = BrokenStreamEndpoint()
        request = {
            "request_id": "s6",
            "messages": [{"role": "user", "content": "test"}],
            "stream": True,
        }
        response = await ep.handle(_envelope(request))
        assert response["status_code"] == 200
        assert response["stream"] is True
        chunks = response["chunks"]
        # Should have at least one good chunk + error chunk
        last_chunk = chunks[-1]
        assert "error" in last_chunk

    @pytest.mark.asyncio
    async def test_stream_preserves_request_id(self):
        ep = StreamingChatEndpoint()
        request = {
            "messages": [{"role": "user", "content": "test"}],
            "stream": True,
        }
        response = await ep.handle(_envelope(request, request_id="req-abc"))
        assert response["request_id"] == "req-abc"

    @pytest.mark.asyncio
    async def test_stream_empty_messages_returns_error(self):
        ep = StreamingChatEndpoint()
        request = {"request_id": "s7", "messages": [], "stream": True}
        response = await ep.handle(_envelope(request))
        assert response["status_code"] == 400


# ===== Wire Protocol Tests =====
# These test the JSON frame format used over UDS

class TestWireProtocol:
    def test_stream_response_json_format(self):
        """Stream response should serialize to valid JSON with expected fields."""
        response = {
            "request_id": "r1",
            "status_code": 200,
            "stream": True,
            "chunks": [
                {"choices": [{"delta": {"content": "Hello"}, "index": 0}]},
                {"choices": [{"delta": {"content": " world"}, "index": 0}]},
                {"choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}]},
            ],
        }
        encoded = json.dumps(response).encode("utf-8")
        decoded = json.loads(encoded)
        assert decoded["stream"] is True
        assert len(decoded["chunks"]) == 3

    def test_normal_response_json_format(self):
        """Normal response should serialize without stream/chunks fields."""
        response = {
            "request_id": "r2",
            "status_code": 200,
            "headers": None,
            "body": {"choices": [{"message": {"content": "hi"}}]},
        }
        encoded = json.dumps(response).encode("utf-8")
        decoded = json.loads(encoded)
        assert "stream" not in decoded
        assert "chunks" not in decoded

    def test_stream_response_fits_frame_protocol(self):
        """Stream response should work with length-prefixed framing."""
        response = {
            "request_id": "r3",
            "status_code": 200,
            "stream": True,
            "chunks": [{"choices": [{"delta": {"content": "token"}}]}],
        }
        payload = json.dumps(response).encode("utf-8")
        frame = struct.pack(">I", len(payload)) + payload
        # Verify roundtrip
        recv_len = struct.unpack(">I", frame[:4])[0]
        recv_payload = json.loads(frame[4:4 + recv_len].decode("utf-8"))
        assert recv_payload["stream"] is True
