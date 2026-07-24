"""TDD tests for OpenAI Spec (P0).

Tests are written BEFORE implementation. They should all FAIL initially.
"""

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
        response = await endpoint.handle(_envelope(request))
        assert response["status_code"] == 200
        body = response["body"]
        assert body["object"] == "chat.completion"
        assert body["choices"][0]["message"]["content"] == "Echo: Hello"
        assert body["choices"][0]["message"]["role"] == "assistant"

    @pytest.mark.asyncio
    async def test_preserves_request_id(self, endpoint):
        request = {
            "messages": [{"role": "user", "content": "Hi"}],
        }
        response = await endpoint.handle(_envelope(request, request_id="req-123"))
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
        response = await endpoint.handle(_envelope(request))
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
        response = await endpoint.handle(_envelope(request))
        assert response["status_code"] == 200

    @pytest.mark.asyncio
    async def test_empty_messages_returns_error(self, endpoint):
        request = {"messages": []}
        response = await endpoint.handle(_envelope(request))
        assert response["status_code"] == 400
        assert "error" in response["body"]

    @pytest.mark.asyncio
    async def test_missing_messages_returns_error(self, endpoint):
        request = {}
        response = await endpoint.handle(_envelope(request))
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
        response = await ep.handle(_envelope(request))
        assert response["status_code"] == 500
        assert "model exploded" in response["body"]["error"]


# ===== Default encode_response =====

class TestDefaultEncodeResponse:
    @pytest.mark.asyncio
    async def test_string_predict_wrapped_in_openai_format(self):
        ep = MinimalEndpoint()
        request = {"messages": [{"role": "user", "content": "test"}]}
        response = await ep.handle(_envelope(request))
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
        response = await ep.handle(_envelope(request))
        body = response["body"]
        assert body["choices"][0]["message"]["content"] == "result"
        assert body["usage"]["prompt_tokens"] == 1


# ===== OpenAI Response Format Compliance =====

class TestResponseFormat:
    @pytest.mark.asyncio
    async def test_response_has_required_fields(self, ):
        ep = EchoChatEndpoint()
        request = {"messages": [{"role": "user", "content": "Hi"}]}
        response = await ep.handle(_envelope(request))
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
        response = await ep.handle(_envelope(request))
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


# ===== EndpointSpec detect() and registry =====

class TestDetectClassMethod:
    """Test EndpointSpec.detect() discovers subclasses in loaded modules."""

    @pytest.fixture
    def minimal_module_dict(self):
        """Create a dict of classes simulating a loaded module's namespace."""
        from lite_server.specs.openai import OpenAIEndpoint

        class MyEndpoint(OpenAIEndpoint):
            def setup(self):
                pass

            def decode_request(self, request):
                return request.get("messages", [{}])[0].get("content", "")

            def predict(self, x):
                return f"echo: {x}"

        class NotAnEndpoint:
            pass

        return {
            "MyEndpoint": MyEndpoint,
            "NotAnEndpoint": NotAnEndpoint,
            "SomeConstant": 42,
            "OpenAIEndpoint": OpenAIEndpoint,
        }

    def test_detect_finds_concrete_subclass(self, minimal_module_dict):
        """detect() should return instances of concrete OpenAIEndpoint subclasses."""
        import types
        mod = types.SimpleNamespace(**minimal_module_dict)
        from lite_server.specs.openai import OpenAIEndpoint
        instances = OpenAIEndpoint.detect(mod)
        assert len(instances) == 1
        assert isinstance(instances[0], OpenAIEndpoint)

    def test_detect_excludes_base_class(self, minimal_module_dict):
        """detect() should NOT return an instance of the abstract OpenAIEndpoint base."""
        import types
        mod = types.SimpleNamespace(**minimal_module_dict)
        from lite_server.specs.openai import OpenAIEndpoint
        instances = OpenAIEndpoint.detect(mod)
        assert not any(i.__class__ is OpenAIEndpoint for i in instances)

    def test_detect_skips_non_type_entries(self, minimal_module_dict):
        """detect() should skip module entries that are not classes."""
        import types
        mod = types.SimpleNamespace(**minimal_module_dict)
        from lite_server.specs.openai import OpenAIEndpoint
        instances = OpenAIEndpoint.detect(mod)
        assert len(instances) > 0

    def test_detect_returns_empty_when_no_subclass(self):
        """detect() should return empty list when no subclass exists in module."""
        import types
        mod = types.SimpleNamespace()
        from lite_server.specs.openai import OpenAIEndpoint
        instances = OpenAIEndpoint.detect(mod)
        assert instances == []

    def test_detect_skips_abstract_endpoints(self):
        """detect() should skip subclasses that have abstract methods."""
        import types
        from abc import ABC, abstractmethod
        from lite_server.specs.openai import OpenAIEndpoint

        class AbstractEndpoint(OpenAIEndpoint, ABC):
            @abstractmethod
            def missing_method(self): pass

        mod = types.SimpleNamespace(AbstractEndpoint=AbstractEndpoint)
        instances = OpenAIEndpoint.detect(mod)
        assert len(instances) == 0

    def test_detect_multiple_concrete_subclasses(self):
        """detect() should find all concrete subclasses in a module."""
        import types
        from lite_server.specs.openai import OpenAIEndpoint

        class EndpointOne(OpenAIEndpoint):
            def setup(self): pass
            def decode_request(self, req): return req
            def predict(self, x): return x

        class EndpointTwo(OpenAIEndpoint):
            def setup(self): pass
            def decode_request(self, req): return req
            def predict(self, x): return x

        mod = types.SimpleNamespace(EndpointOne=EndpointOne, EndpointTwo=EndpointTwo)
        instances = OpenAIEndpoint.detect(mod)
        assert len(instances) == 2


class TestRegistryAutoRegistration:
    """Test that EndpointSpec subclasses are auto-registered."""

    def test_openai_endpoint_in_registry(self):
        """OpenAIEndpoint should be auto-registered in _SPEC_REGISTRY."""
        from lite_server.specs.base import _SPEC_REGISTRY
        from lite_server.specs.openai import OpenAIEndpoint
        assert OpenAIEndpoint in _SPEC_REGISTRY

    def test_registry_contains_only_concrete_specs(self):
        """_SPEC_REGISTRY should not contain abstract classes."""
        from lite_server.specs.base import _SPEC_REGISTRY
        from lite_server.specs.openai import OpenAIEndpoint
        # OpenAIEndpoint must be concrete (no abstract methods)
        assert OpenAIEndpoint in _SPEC_REGISTRY
        assert not OpenAIEndpoint.__abstractmethods__, (
            f"OpenAIEndpoint has abstract methods: {OpenAIEndpoint.__abstractmethods__}"
        )

    def test_registry_is_list(self):
        """_SPEC_REGISTRY should be a list supporting iteration."""
        from lite_server.specs.base import _SPEC_REGISTRY
        assert isinstance(_SPEC_REGISTRY, list)


# ===== Dispatch-level e2e (load_endpoints → handle_request) =====

class TestDispatchE2E:
    """OpenAIEndpoint through the real dispatch: envelope in, frame out."""

    @staticmethod
    def _repo(tmp_path):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        (ep_dir / "chat.py").write_text(
            "from lite_server.specs.openai import OpenAIEndpoint\n"
            "\n"
            "class ChatEndpoint(OpenAIEndpoint):\n"
            "    def setup(self): pass\n"
            "    def decode_request(self, request):\n"
            "        return request.get('messages', [{}])[-1].get('content', '')\n"
            "    def predict(self, x):\n"
            "        return f'echo: {x}'\n"
        )
        from lite_server.worker.endpoints import load_endpoints
        return load_endpoints(str(tmp_path))

    @staticmethod
    def _req(body):
        return {
            "request_id": "req-e2e",
            "route": "/v1/chat/completions",
            "method": "POST",
            "headers": {},
            "query": {},
            "body": body,
            "server_state": {},
        }

    @pytest.mark.asyncio
    async def test_non_streaming_not_nested(self, tmp_path):
        from lite_server.worker.endpoints import handle_request
        endpoints = self._repo(tmp_path)
        resp = await handle_request(endpoints, self._req(
            {"messages": [{"role": "user", "content": "hi"}]}
        ))
        assert resp["status_code"] == 200
        # The OpenAI completion is the body directly — no frame nesting.
        assert resp["body"]["object"] == "chat.completion"
        assert resp["body"]["choices"][0]["message"]["content"] == "echo: hi"
        assert resp["request_id"] == "req-e2e"

    @pytest.mark.asyncio
    async def test_empty_messages_400(self, tmp_path):
        from lite_server.worker.endpoints import handle_request
        endpoints = self._repo(tmp_path)
        resp = await handle_request(endpoints, self._req({"messages": []}))
        assert resp["status_code"] == 400
        assert "error" in resp["body"]

    @pytest.mark.asyncio
    async def test_streaming_frame_via_dispatch(self, tmp_path):
        from lite_server.worker.endpoints import handle_request
        endpoints = self._repo(tmp_path)
        resp = await handle_request(endpoints, self._req(
            {"messages": [{"role": "user", "content": "hi"}], "stream": True}
        ))
        assert resp["status_code"] == 200
        assert resp["stream"] is True
        assert len(resp["chunks"]) >= 1
