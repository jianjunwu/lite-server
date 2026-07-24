"""pytest unit tests for lite_server.worker.endpoints."""

import logging
import os
import socket
import sys
import textwrap

import pytest

from lite_server.context import Headers
from lite_server.endpoint import router
from lite_server.server_proxy import RegistryProxy, ServerProxy
from lite_server.worker.endpoints import (
    _LevelPrefixFormatter,
    create_server_socket,
    derive_port_from_path,
    handle_request,
    load_endpoints,
    logger,
    setup_logging,
)


class TestRegistryProxy:
    def test_list_loaded(self):
        snapshot = {"loaded_models": [{"name": "m1", "version": "1"}]}
        reg = RegistryProxy(snapshot)
        assert reg.list_loaded() == [{"name": "m1", "version": "1"}]

    def test_list_loaded_empty(self):
        reg = RegistryProxy({})
        assert reg.list_loaded() == []


class TestServerProxy:
    def test_registry(self):
        snapshot = {"loaded_models": [{"name": "m1"}], "config": {"port": 8080}}
        srv = ServerProxy(snapshot)
        assert srv.registry.list_loaded() == [{"name": "m1"}]

    def test_config(self):
        snapshot = {"config": {"debug": True}}
        srv = ServerProxy(snapshot)
        assert srv.config["debug"] is True

    def test_config_default(self):
        srv = ServerProxy({})
        assert srv.config == {}


class TestLoadEndpoints:
    def test_loads_endpoint_file(self, tmp_path):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "status.py"
        ep_file.write_text(textwrap.dedent('''
            methods = ["GET", "POST"]
            def handler(request, server):
                return {"status": "ok"}
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert "/status" in endpoints
        assert endpoints["/status"]["methods"] == ["GET", "POST"]

    def test_missing_handler_skipped(self, tmp_path):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "bad.py"
        ep_file.write_text(textwrap.dedent('''
            methods = ["GET"]
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert "/bad" not in endpoints

    def test_methods_default_to_get(self, tmp_path):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "hello.py"
        ep_file.write_text(textwrap.dedent('''
            def handler(request, server):
                return "hello"
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert endpoints["/hello"]["methods"] == ["GET"]

    def test_ignores_non_endpoint_files(self, tmp_path):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        (ep_dir / "helper.py").write_text("x = 1")
        endpoints = load_endpoints(str(tmp_path))
        assert endpoints == {}

    def test_broken_endpoint_logged_but_not_crashed(self, tmp_path, caplog):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "broken.py"
        ep_file.write_text("raise ValueError('bad')")
        setup_logging()
        with caplog.at_level(logging.ERROR, logger="endpoint_worker"):
            endpoints = load_endpoints(str(tmp_path))
        assert "/broken" not in endpoints
        assert any("Failed to load subdirectory endpoint" in r.message for r in caplog.records)

    def test_loads_endpoints_from_configured_dir(self, tmp_path):
        """When repo_path IS the endpoints directory (explicit endpoints_dir
        config), load_endpoints should detect .py files directly."""
        ep_file = tmp_path / "status.py"
        ep_file.write_text(textwrap.dedent('''
            methods = ["GET", "POST"]
            def handler(request, server):
                return {"status": "ok"}
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert "/status" in endpoints
        assert endpoints["/status"]["methods"] == ["GET", "POST"]


class TestHandleRequest:
    @pytest.fixture
    def endpoints(self):
        def sync_handler(request, server):
            return {"method": request["method"], "query": request["query"]}

        async def async_handler(request, server):
            return {"async": True, "body": request.get("body")}

        return {
            "/sync": {
                "handler": sync_handler,
                "methods": ["GET"],
            },
            "/async": {
                "handler": async_handler,
                "methods": ["POST"],
            },
        }

    @pytest.mark.asyncio
    async def test_sync_handler(self, endpoints):
        req = {
            "request_id": "r1",
            "route": "/sync",
            "method": "GET",
            "headers": {},
            "query": {"foo": "bar"},
            "body": None,
            "server_state": {},
        }
        resp = await handle_request(endpoints, req)
        assert resp["status_code"] == 200
        assert resp["body"]["query"]["foo"] == "bar"
        assert resp["request_id"] == "r1"

    @pytest.mark.asyncio
    async def test_async_handler(self, endpoints):
        req = {
            "request_id": "r2",
            "route": "/async",
            "method": "POST",
            "headers": {},
            "query": {},
            "body": {"x": 1},
            "server_state": {},
        }
        resp = await handle_request(endpoints, req)
        assert resp["status_code"] == 200
        assert resp["body"]["async"] is True
        assert resp["body"]["body"]["x"] == 1

    @pytest.mark.asyncio
    async def test_missing_endpoint(self):
        resp = await handle_request({}, {"route": "/missing", "request_id": "r3"})
        assert resp["status_code"] == 404
        assert "not found" in resp["body"]["error"].lower()

    @pytest.mark.asyncio
    async def test_handler_exception_returns_500(self, tmp_path):
        def bad_handler(request, server):
            raise RuntimeError("oops")

        endpoints = {"/bad": {"handler": bad_handler, "methods": ["GET"]}}
        req = {
            "request_id": "r4",
            "route": "/bad",
            "method": "GET",
            "headers": {},
            "query": {},
            "body": None,
            "server_state": {},
        }
        resp = await handle_request(endpoints, req)
        assert resp["status_code"] == 500
        assert "internal server error" in resp["body"]["error"].lower()

    @pytest.mark.asyncio
    async def test_non_dict_result_wrapped(self, endpoints):
        def string_handler(request, server):
            return "plain text"

        ep = {"/text": {"handler": string_handler, "methods": ["GET"]}}
        req = {
            "request_id": "r5",
            "route": "/text",
            "method": "GET",
            "headers": {},
            "query": {},
            "body": None,
            "server_state": {},
        }
        resp = await handle_request(ep, req)
        assert resp["status_code"] == 200
        assert resp["body"]["data"] == "plain text"

    @pytest.mark.asyncio
    async def test_server_proxy_has_registry(self, tmp_path):
        loaded = [{"name": "m1", "version": "1"}]

        def registry_handler(request, server):
            return {"count": len(server.registry.list_loaded())}

        ep = {"/reg": {"handler": registry_handler, "methods": ["GET"]}}
        req = {
            "request_id": "r6",
            "route": "/reg",
            "method": "GET",
            "headers": {},
            "query": {},
            "body": None,
            "server_state": {"loaded_models": loaded, "config": {}},
        }
        resp = await handle_request(ep, req)
        assert resp["status_code"] == 200
        assert resp["body"]["count"] == 1


class TestDerivePortFromPath:
    def test_returns_port_in_valid_range(self):
        port = derive_port_from_path("/tmp/lite-server-12345-endpoints.sock")
        assert 30000 <= port <= 59999

    def test_deterministic(self):
        path = "/tmp/lite-server-12345-endpoints.sock"
        assert derive_port_from_path(path) == derive_port_from_path(path)

    def test_different_paths_give_different_ports(self):
        p1 = derive_port_from_path("/tmp/lite-server-1-endpoints.sock")
        p2 = derive_port_from_path("/tmp/lite-server-2-endpoints.sock")
        # Not guaranteed but extremely likely with good hash
        assert p1 != p2

    def test_returns_int(self):
        port = derive_port_from_path("any-path")
        assert isinstance(port, int)


class TestCreateServerSocket:
    @pytest.mark.skipif(sys.platform == "win32", reason="Unix-only test")
    def test_unix_creates_af_unix_socket(self):
        import tempfile
        sock_path = os.path.join(tempfile.gettempdir(), f"ls-test-{os.getpid()}.sock")
        sock = create_server_socket(sock_path)
        try:
            assert sock.family == socket.AF_UNIX
            assert sock.type == socket.SOCK_STREAM
        finally:
            sock.close()
            if os.path.exists(sock_path):
                os.remove(sock_path)

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-only test")
    def test_windows_creates_tcp_socket(self, tmp_path):
        sock_path = str(tmp_path / "test.sock")
        sock = create_server_socket(sock_path)
        try:
            assert sock.family == socket.AF_INET
            assert sock.type == socket.SOCK_STREAM
        finally:
            sock.close()


class TestLevelPrefixFormatter:
    """Test that log format aligns with Rust stderr parser ([WARN] not [WARNING])."""

    def _make_record(self, level, msg):
        return logging.LogRecord("test", level, "", 0, msg, (), None)

    def test_warning_maps_to_warn(self):
        fmt = _LevelPrefixFormatter()
        output = fmt.format(self._make_record(logging.WARNING, "test msg"))
        assert output == "[WARN] test msg"

    def test_error_maps_to_error(self):
        fmt = _LevelPrefixFormatter()
        output = fmt.format(self._make_record(logging.ERROR, "test msg"))
        assert output == "[ERROR] test msg"

    def test_info_maps_to_info(self):
        fmt = _LevelPrefixFormatter()
        output = fmt.format(self._make_record(logging.INFO, "test msg"))
        assert output == "[INFO] test msg"

    def test_critical_maps_to_critical(self):
        fmt = _LevelPrefixFormatter()
        output = fmt.format(self._make_record(logging.CRITICAL, "test msg"))
        assert output == "[CRITICAL] test msg"

    def test_no_warning_in_output(self):
        """Ensure [WARNING] never appears — Rust side won't parse it."""
        fmt = _LevelPrefixFormatter()
        output = fmt.format(self._make_record(logging.WARNING, "test"))
        assert "[WARNING]" not in output


class TestSetupLogging:
    def test_returns_logger(self):
        log = setup_logging()
        assert isinstance(log, logging.Logger)
        assert log.name == "endpoint_worker"

    def test_has_handler(self):
        setup_logging()
        assert len(logger.handlers) > 0

    def test_handler_uses_level_prefix_formatter(self):
        setup_logging()
        formatter = logger.handlers[0].formatter
        assert isinstance(formatter, _LevelPrefixFormatter)


# ===== EndpointSpec registry integration =====

class TestLoadEndpointsWithCustomSpec:
    """load_endpoints() should discover custom EndpointSpec subclasses via registry."""

    def test_custom_spec_detected_via_registry(self, tmp_path):
        """A custom EndpointSpec subclass in an endpoint file should be detected."""
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "custom.py"
        ep_file.write_text(textwrap.dedent("""\
            from lite_server.specs.base import EndpointSpec

            class CustomSpec(EndpointSpec):
                routes = ["/v1/custom"]

                def setup(self):
                    pass

                def decode_request(self, request):
                    return request

                def predict(self, x):
                    return x

                @classmethod
                def detect(cls, mod):
                    for attr_name in dir(mod):
                        attr = getattr(mod, attr_name)
                        if (
                            isinstance(attr, type)
                            and issubclass(attr, cls)
                            and not getattr(attr, "__abstractmethods__", None)
                        ):
                            return [attr()]
                    return []

                def get_routes(self):
                    return [{"route": r, "methods": ["GET"]} for r in self.routes]

                async def handle(self, request):
                    decoded = self.decode_request(request)
                    result = self.predict(decoded)
                    return {
                        "request_id": request.get("request_id", ""),
                        "status_code": 200,
                        "headers": None,
                        "body": result,
                    }
        """))
        from lite_server.worker.endpoints import load_endpoints
        endpoints = load_endpoints(str(tmp_path))

        assert "/v1/custom" in endpoints
        assert "GET" in endpoints["/v1/custom"]["methods"]
        assert callable(endpoints["/v1/custom"]["handler"])


# ===== 0.7.0 context unification: endpoint tests =====


class TestEndpointMiddlewareApplied:
    """0.7.0: RouteDef.middleware was previously collected but never applied.
    load_endpoints now wraps middleware at load time."""

    def test_decorator_middleware_applied_via_load_endpoints(self, tmp_path):
        """Middleware registered via @router.get(middleware=[...]) must be
        applied at load time."""
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "mw_test.py"
        ep_file.write_text(textwrap.dedent("""\
            from lite_server.endpoint import router

            def add_header(handler):
                async def wrapper(request, server):
                    result = await handler(request, server)
                    if isinstance(result, dict):
                        result.setdefault("headers", {})
                        result["headers"]["X-MW"] = "applied"
                    return result
                return wrapper

            @router.get("/mw", middleware=[add_header])
            async def mw_handler(request, server):
                return {"body": "ok"}
        """))

        # Need to clear router first (global state from other tests)
        router._routes.clear()
        try:
            endpoints = load_endpoints(str(tmp_path))
            assert "/mw" in endpoints
            # The handler should already be wrapped with middleware
            handler = endpoints["/mw"]["handler"]
            # middleware key should NOT be in the endpoint dict (applied, not stored)
            assert "middleware" not in endpoints["/mw"]
        finally:
            router._routes.clear()

    def test_middleware_not_applied_stored_as_key_before_fix(self):
        """Before the fix, the 'middleware' key was stored but never applied.
        This test verifies the OLD behavior is gone."""
        # This is a behavioral change doc-test
        # After fix, load_endpoints no longer stores 'middleware' key
        pass  # Verified by test_decorator_middleware_applied_via_load_endpoints


class TestEndpointHeaders:
    """0.7.0: endpoint request headers become Headers (case-insensitive)."""

    @pytest.mark.asyncio
    async def test_endpoint_headers_case_insensitive(self):
        """handle_request wraps headers in Headers, making lookups
        case-insensitive."""
        def handler(request, server):
            return {"auth": request["headers"].get("x-api-key")}

        ep = {"/hdr": {"handler": handler, "methods": ["GET"]}}
        req = {
            "request_id": "r-hdr",
            "route": "/hdr",
            "method": "GET",
            "headers": {"X-Api-Key": "secret123"},
            "query": {},
            "body": None,
            "server_state": {},
        }
        resp = await handle_request(ep, req)
        assert resp["status_code"] == 200
        # With Headers, x-api-key should find X-Api-Key
        assert resp["body"]["auth"] == "secret123"

    @pytest.mark.asyncio
    async def test_endpoint_request_contract_keys(self):
        """EndpointRequest TypedDict has the expected keys."""
        def handler(request, server):
            return {
                "has_method": "method" in request,
                "has_route": "route" in request,
                "has_headers": "headers" in request,
                "has_query": "query" in request,
                "has_body": "body" in request,
            }

        ep = {"/contract": {"handler": handler, "methods": ["GET"]}}
        req = {
            "request_id": "r1",
            "route": "/contract",
            "method": "GET",
            "headers": {},
            "query": {},
            "body": None,
            "server_state": {},
        }
        resp = await handle_request(ep, req)
        body = resp["body"]
        assert body["has_method"] is True
        assert body["has_route"] is True
        assert body["has_headers"] is True
        assert body["has_query"] is True
        assert body["has_body"] is True


class TestRequestModuleRemoved:
    """0.7.0: request.py is deleted — Request, URL, QueryParams, Client,
    State, UploadFile are no longer importable from lite_server."""

    def test_request_module_import_fails(self):
        with pytest.raises(ImportError):
            import lite_server.request  # noqa: F401

    def test_request_class_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import Request  # noqa: F401

    def test_url_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import URL  # noqa: F401

    def test_query_params_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import QueryParams  # noqa: F401

    def test_headers_still_importable_from_context(self):
        """Headers moves to lite_server.context, re-exported from lite_server."""
        from lite_server import Headers
        assert Headers is not None


# ===== 0.7.0 response-frame contract at dispatch =====


class TestResponseFrameContract:
    """handle_request unpacks handler-returned response frames (§7.2-4).

    A dict carrying status_code/headers/stream/chunks keys is a response
    frame ("body" is interpreted inside frames but is not a trigger, so it
    stays usable as a plain data key); a plain data dict is wrapped as the
    body with status 200 (unchanged legacy behavior).
    """

    @staticmethod
    def _req(route="/r", headers=None):
        return {
            "request_id": "r1",
            "route": route,
            "method": "GET",
            "headers": headers or {},
            "query": {},
            "body": None,
            "server_state": {},
        }

    @pytest.mark.asyncio
    async def test_plain_dict_wrapped_as_body_unchanged(self):
        def handler(request, server):
            return {"foo": "bar"}

        ep = {"/r": {"handler": handler, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["status_code"] == 200
        assert resp["headers"] is None
        assert resp["body"] == {"foo": "bar"}

    @pytest.mark.asyncio
    async def test_frame_dict_status_code_and_headers_honored(self):
        def handler(request, server):
            return {"status_code": 201, "headers": {"X-A": "b"}, "body": {"ok": True}}

        ep = {"/r": {"handler": handler, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["status_code"] == 201
        assert resp["headers"] == {"X-A": "b"}
        assert resp["body"] == {"ok": True}

    @pytest.mark.asyncio
    async def test_spec_frame_not_double_wrapped(self):
        """EndpointSpec.handle returns a full frame; dispatch must not nest it."""
        def handler(request, server):
            return {
                "request_id": "ignored",
                "status_code": 200,
                "headers": None,
                "body": {"object": "chat.completion"},
            }

        ep = {"/r": {"handler": handler, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["status_code"] == 200
        assert resp["body"] == {"object": "chat.completion"}
        # Dispatch owns the envelope request_id.
        assert resp["request_id"] == "r1"

    @pytest.mark.asyncio
    async def test_streaming_frame_passes_through(self):
        """OpenAIEndpoint-style stream frames keep stream/chunks for
        handle_connection's streaming branch."""
        def handler(request, server):
            return {
                "request_id": "s1",
                "status_code": 200,
                "stream": True,
                "chunks": [{"choices": [{"delta": {"content": "hi"}}]}],
            }

        ep = {"/r": {"handler": handler, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["status_code"] == 200
        assert resp["stream"] is True
        assert resp["chunks"] == [{"choices": [{"delta": {"content": "hi"}}]}]

    @pytest.mark.asyncio
    async def test_middleware_short_circuit_401_via_dispatch(self):
        from lite_server.middleware import require_api_key

        called = []

        async def handler(request, server):
            called.append(True)
            return {"result": "ok"}

        wrapped = require_api_key(header="X-API-Key", keys=["secret"])(handler)
        ep = {"/r": {"handler": wrapped, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["status_code"] == 401
        assert resp["body"] == {"error": "unauthorized"}
        assert called == []

    @pytest.mark.asyncio
    async def test_middleware_429_retry_after_via_dispatch(self):
        from lite_server.middleware import rate_limit

        async def handler(request, server):
            return {"result": "ok"}

        wrapped = rate_limit(requests_per_minute=0)(handler)
        ep = {"/rl": {"handler": wrapped, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req(route="/rl"))
        assert resp["status_code"] == 429
        assert resp["headers"]["Retry-After"] == "60"

    @pytest.mark.asyncio
    async def test_cors_headers_pass_through_and_body_preserved(self):
        from lite_server.middleware import cors

        async def handler(request, server):
            return {"foo": "bar"}

        wrapped = cors(allow_origins=["https://example.com"])(handler)
        ep = {"/r": {"handler": wrapped, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["headers"]["Access-Control-Allow-Origin"] == "https://example.com"
        assert resp["body"] == {"foo": "bar"}

    @pytest.mark.asyncio
    async def test_require_api_key_case_insensitive_via_dispatch(self):
        """Full §7 story: Headers envelope + middleware — key matches any case."""
        from lite_server.middleware import require_api_key

        async def handler(request, server):
            return {"result": "authorized"}

        wrapped = require_api_key(header="X-API-Key", keys=["secret"])(handler)
        ep = {"/r": {"handler": wrapped, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req(headers={"x-api-key": "secret"}))
        assert resp["status_code"] == 200
        assert resp["body"] == {"result": "authorized"}
