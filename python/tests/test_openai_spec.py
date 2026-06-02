"""TDD tests for OpenAI Spec (P0).

Tests are written BEFORE implementation. They should all FAIL initially.
"""

import pytest

from lite_server.specs.openai import OpenAIEndpoint


# ===== Minimal test model =====

class EchoChatEndpoint(OpenAIEndpoint):
    """Trivial model that echoes back the user message."""

    def setup(self):
        pass

    def decode_request(self, request):
        messages = request.get("messages", [])
        prompt = messages[-1]["content"] if messages else ""
        return {
            "prompt": prompt,
            "max_tokens": request.get("max_tokens", 128),
            "temperature": request.get("temperature", 0.7),
            "stream": request.get("stream", False),
        }

    def predict(self, x):
        return {
            "text": f"Echo: {x['prompt']}",
            "usage": {"prompt_tokens": 10, "completion_tokens": 5},
        }

    def encode_response(self, output):
        return {
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "echo-model",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": output["text"]},
                    "finish_reason": "stop",
                }
            ],
            "usage": output["usage"],
        }


class MinimalEndpoint(OpenAIEndpoint):
    """Model with only required methods — encode_response uses default."""

    def setup(self):
        pass

    def decode_request(self, request):
        messages = request.get("messages", [])
        return messages[-1]["content"] if messages else ""

    def predict(self, x):
        return f"Response to: {x}"


# ===== Route Registration =====

class TestRouteRegistration:
    def test_has_default_chat_completions_route(self):
        ep = EchoChatEndpoint()
        routes = ep.get_routes()
        assert any(r["route"] == "/v1/chat/completions" for r in routes)

    def test_default_methods_is_post(self):
        ep = EchoChatEndpoint()
        routes = ep.get_routes()
        chat_route = next(r for r in routes if r["route"] == "/v1/chat/completions")
        assert chat_route["methods"] == ["POST"]

    def test_custom_route_override(self):
        class CustomEndpoint(OpenAIEndpoint):
            routes = ["/v1/custom/chat"]

            def setup(self):
                pass

            def decode_request(self, request):
                return request

            def predict(self, x):
                return x

        ep = CustomEndpoint()
        routes = ep.get_routes()
        assert any(r["route"] == "/v1/custom/chat" for r in routes)
        assert not any(r["route"] == "/v1/chat/completions" for r in routes)


# ===== Request Handling =====

class TestRequestHandling:
    @pytest.fixture
    def endpoint(self):
        return EchoChatEndpoint()

    @pytest.mark.asyncio
    async def test_basic_chat_request(self, endpoint):
        request = {
            "messages": [{"role": "user", "content": "Hello"}],
        }
        response = await endpoint.handle(request)
        assert response["status_code"] == 200
        body = response["body"]
        assert body["object"] == "chat.completion"
        assert body["choices"][0]["message"]["content"] == "Echo: Hello"
        assert body["choices"][0]["message"]["role"] == "assistant"

    @pytest.mark.asyncio
    async def test_preserves_request_id(self, endpoint):
        request = {
            "request_id": "req-123",
            "messages": [{"role": "user", "content": "Hi"}],
        }
        response = await endpoint.handle(request)
        assert response["request_id"] == "req-123"

    @pytest.mark.asyncio
    async def test_multiple_messages_uses_last(self, endpoint):
        request = {
            "messages": [
                {"role": "system", "content": "You are helpful."},
                {"role": "user", "content": "First"},
                {"role": "assistant", "content": "Sure"},
                {"role": "user", "content": "Second"},
            ],
        }
        response = await endpoint.handle(request)
        assert response["status_code"] == 200
        assert response["body"]["choices"][0]["message"]["content"] == "Echo: Second"

    @pytest.mark.asyncio
    async def test_passes_max_tokens_and_temperature(self, endpoint):
        """decode_request should receive all OpenAI params."""
        request = {
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 256,
            "temperature": 0.3,
        }
        # Should not raise — params are passed through decode_request
        response = await endpoint.handle(request)
        assert response["status_code"] == 200

    @pytest.mark.asyncio
    async def test_empty_messages_returns_error(self, endpoint):
        request = {"messages": []}
        response = await endpoint.handle(request)
        assert response["status_code"] == 400
        assert "error" in response["body"]

    @pytest.mark.asyncio
    async def test_missing_messages_returns_error(self, endpoint):
        request = {}
        response = await endpoint.handle(request)
        assert response["status_code"] == 400

    @pytest.mark.asyncio
    async def test_handler_exception_returns_500(self):
        class BrokenEndpoint(OpenAIEndpoint):
            def setup(self):
                pass

            def decode_request(self, request):
                return request.get("messages", [{}])[0].get("content", "")

            def predict(self, x):
                raise RuntimeError("model exploded")

            def encode_response(self, output):
                return {"choices": [{"message": {"content": output}}]}

        ep = BrokenEndpoint()
        request = {"messages": [{"role": "user", "content": "Hi"}]}
        response = await ep.handle(request)
        assert response["status_code"] == 500
        assert "model exploded" in response["body"]["error"]


# ===== Default encode_response =====

class TestDefaultEncodeResponse:
    @pytest.mark.asyncio
    async def test_string_predict_wrapped_in_openai_format(self):
        ep = MinimalEndpoint()
        request = {"messages": [{"role": "user", "content": "test"}]}
        response = await ep.handle(request)
        assert response["status_code"] == 200
        body = response["body"]
        assert body["object"] == "chat.completion"
        assert body["choices"][0]["message"]["role"] == "assistant"
        assert "Response to: test" in body["choices"][0]["message"]["content"]

    @pytest.mark.asyncio
    async def test_dict_predict_with_text_key(self):
        class DictEndpoint(OpenAIEndpoint):
            def setup(self):
                pass

            def decode_request(self, request):
                return request.get("messages", [{}])[0].get("content", "")

            def predict(self, x):
                return {"text": "result", "usage": {"prompt_tokens": 1, "completion_tokens": 1}}

        ep = DictEndpoint()
        request = {"messages": [{"role": "user", "content": "Hi"}]}
        response = await ep.handle(request)
        body = response["body"]
        assert body["choices"][0]["message"]["content"] == "result"
        assert body["usage"]["prompt_tokens"] == 1


# ===== OpenAI Response Format Compliance =====

class TestResponseFormat:
    @pytest.mark.asyncio
    async def test_response_has_required_fields(self, ):
        ep = EchoChatEndpoint()
        request = {"messages": [{"role": "user", "content": "Hi"}]}
        response = await ep.handle(request)
        body = response["body"]

        # Required fields per OpenAI spec
        assert "id" in body
        assert body["object"] == "chat.completion"
        assert "created" in body
        assert "choices" in body
        assert isinstance(body["choices"], list)
        assert len(body["choices"]) > 0

        choice = body["choices"][0]
        assert "index" in choice
        assert "message" in choice
        assert "role" in choice["message"]
        assert "content" in choice["message"]
        assert "finish_reason" in choice

    @pytest.mark.asyncio
    async def test_response_has_model_field(self):
        class ModelFieldEndpoint(OpenAIEndpoint):
            model = "test-model-v1"

            def setup(self):
                pass

            def decode_request(self, request):
                return request.get("messages", [{}])[0].get("content", "")

            def predict(self, x):
                return "ok"

        ep = ModelFieldEndpoint()
        request = {"messages": [{"role": "user", "content": "Hi"}]}
        response = await ep.handle(request)
        assert response["body"]["model"] == "test-model-v1"


# ===== Integration with load_endpoints =====

class TestLoadEndpointsIntegration:
    def test_openai_endpoint_detected(self, tmp_path):
        """load_endpoints should detect OpenAIEndpoint subclasses."""
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "chat.py"
        ep_file.write_text(
            "from lite_server.specs.openai import OpenAIEndpoint\n"
            "\n"
            "class ChatEndpoint(OpenAIEndpoint):\n"
            "    def setup(self): pass\n"
            "    def decode_request(self, request):\n"
            "        return request.get('messages', [{}])[0].get('content', '')\n"
            "    def predict(self, x):\n"
            "        return f'echo: {x}'\n"
        )
        from lite_server.worker.endpoints import load_endpoints
        endpoints = load_endpoints(str(tmp_path))

        # Should register /v1/chat/completions
        assert "/v1/chat/completions" in endpoints
        assert "POST" in endpoints["/v1/chat/completions"]["methods"]

    def test_openai_endpoint_handler_is_callable(self, tmp_path):
        """The registered handler should be the endpoint's handle method."""
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "chat.py"
        ep_file.write_text(
            "from lite_server.specs.openai import OpenAIEndpoint\n"
            "\n"
            "class ChatEndpoint(OpenAIEndpoint):\n"
            "    def setup(self): pass\n"
            "    def decode_request(self, request):\n"
            "        return request.get('messages', [{}])[0].get('content', '')\n"
            "    def predict(self, x):\n"
            "        return f'echo: {x}'\n"
        )
        from lite_server.worker.endpoints import load_endpoints
        endpoints = load_endpoints(str(tmp_path))
        handler = endpoints["/v1/chat/completions"]["handler"]
        assert callable(handler)
