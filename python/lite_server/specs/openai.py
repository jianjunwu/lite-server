"""OpenAI-compatible endpoint spec.

Provides ``OpenAIEndpoint`` base class. Subclass it and implement
``setup``, ``decode_request``, and ``predict`` to get an OpenAI-compatible
chat completion endpoint with zero boilerplate.
"""

from __future__ import annotations

import inspect
import time
import uuid
from typing import Any, AsyncIterator

from lite_server.specs.base import EndpointSpec


class OpenAIEndpoint(EndpointSpec):
    """Base class for OpenAI-compatible endpoints.

    Subclasses must implement:
      - setup(): initialize model/resources
      - decode_request(request) -> decoded input for predict
      - predict(x) -> model output (str or dict with "text" key)

    Optionally override:
      - stream_predict(x) -> async generator yielding OpenAI stream chunks
      - encode_response(output) -> custom OpenAI response dict
      - get_routes() -> custom route list
    """

    # Override to set the model name in responses
    model: str = "lite-server"

    # Override to register custom routes
    routes: list[str] | None = None

    # Default routes registered when `routes` is None
    _DEFAULT_ROUTES = ["/v1/chat/completions"]

    @classmethod
    def detect(cls, mod) -> list[OpenAIEndpoint]:
        """Discover concrete OpenAIEndpoint subclasses in a loaded module."""
        instances: list[OpenAIEndpoint] = []
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, cls)
                and attr is not cls
                and not getattr(attr, "__abstractmethods__", None)
            ):
                instances.append(attr())
        return instances

    def setup(self) -> None:
        """Initialize model. Called once at startup."""
        raise NotImplementedError

    def decode_request(self, request: dict[str, Any]) -> Any:
        """Parse OpenAI request into model input."""
        raise NotImplementedError

    def predict(self, x: Any) -> Any:
        """Run inference. Return str, dict with 'text' key, or custom."""
        raise NotImplementedError

    async def stream_predict(self, x: Any) -> AsyncIterator[dict[str, Any]]:
        """Yield OpenAI streaming chunks. Override to enable streaming.

        Each yielded value should be an OpenAI streaming chunk dict, e.g.:
            {"choices": [{"delta": {"content": "token"}, "index": 0}]}

        The final chunk should have finish_reason="stop":
            {"choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}]}

        If not overridden, streaming requests fall back to predict() wrapped
        as a single chunk.
        """
        # Not implemented — handle() will fall back to predict()
        raise NotImplementedError
        yield  # make this a generator  # type: ignore[misc]

    def encode_response(self, output: Any) -> dict[str, Any]:
        """Format model output as OpenAI response. Override for custom format."""
        if isinstance(output, dict) and "text" in output:
            text = output["text"]
            usage = output.get("usage", {"prompt_tokens": 0, "completion_tokens": 0})
        elif isinstance(output, str):
            text = output
            usage = {"prompt_tokens": 0, "completion_tokens": 0}
        else:
            text = str(output)
            usage = {"prompt_tokens": 0, "completion_tokens": 0}

        return {
            "id": f"chatcmpl-{uuid.uuid4().hex[:8]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": self.model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                }
            ],
            "usage": usage,
        }

    def get_routes(self) -> list[dict[str, Any]]:
        """Return route definitions for registration."""
        route_list = self.routes or self._DEFAULT_ROUTES
        return [{"route": r, "methods": ["POST"]} for r in route_list]

    def _has_stream_predict(self) -> bool:
        """Check if stream_predict is overridden (not the base stub)."""
        return self.stream_predict.__func__ is not OpenAIEndpoint.stream_predict

    async def _collect_stream(self, decoded: Any) -> list[dict[str, Any]]:
        """Collect all chunks from stream_predict into a list."""
        chunks = []
        async for chunk in self.stream_predict(decoded):
            chunks.append(chunk)
        return chunks

    def _wrap_as_stream_chunk(self, output: Any) -> dict[str, Any]:
        """Wrap a single predict() output as an OpenAI streaming chunk."""
        if isinstance(output, dict) and "text" in output:
            text = output["text"]
        elif isinstance(output, str):
            text = output
        else:
            text = str(output)

        return {
            "id": f"chatcmpl-{uuid.uuid4().hex[:8]}",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": self.model,
            "choices": [
                {
                    "index": 0,
                    "delta": {"role": "assistant", "content": text},
                    "finish_reason": None,
                }
            ],
        }

    async def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        """Full request lifecycle: validate -> decode -> predict/stream_predict -> encode."""
        request_id = request.get("request_id", "")
        is_stream = request.get("stream", False)

        try:
            # Validate: must have messages
            messages = request.get("messages")
            if not messages or not isinstance(messages, list) or len(messages) == 0:
                return {
                    "request_id": request_id,
                    "status_code": 400,
                    "headers": None,
                    "body": {"error": "messages is required and must be a non-empty list"},
                }

            # Decode
            decoded = self.decode_request(request)

            if is_stream:
                return await self._handle_stream(request_id, decoded)

            # Non-streaming: predict -> encode
            output = self.predict(decoded)
            body = self.encode_response(output)

            return {
                "request_id": request_id,
                "status_code": 200,
                "headers": None,
                "body": body,
            }
        except Exception as e:
            return {
                "request_id": request_id,
                "status_code": 500,
                "headers": None,
                "body": {"error": str(e)},
            }

    async def _handle_stream(self, request_id: str, decoded: Any) -> dict[str, Any]:
        """Handle a streaming request."""
        if self._has_stream_predict():
            # Collect chunks from stream_predict
            chunks = []
            try:
                async for chunk in self.stream_predict(decoded):
                    chunks.append(chunk)
            except Exception as e:
                # Add error as final chunk
                chunks.append({"error": str(e)})

            return {
                "request_id": request_id,
                "status_code": 200,
                "stream": True,
                "chunks": chunks,
            }
        else:
            # Fallback: wrap predict() as single stream chunk
            output = self.predict(decoded)
            chunk = self._wrap_as_stream_chunk(output)
            # Add finish_reason stop chunk
            stop_chunk = {
                "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
            }
            return {
                "request_id": request_id,
                "status_code": 200,
                "stream": True,
                "chunks": [chunk, stop_chunk],
            }
