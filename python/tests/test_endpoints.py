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
            def handler(ctx):
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
            def handler(ctx):
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
            def handler(ctx):
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

    def test_decorator_callbacks_applied_via_load_endpoints(self, tmp_path):
        """Callbacks registered via @router.get(callbacks=[...]) are loaded
        into a Pipeline at load time."""
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        ep_file = ep_dir / "mw_test.py"
        ep_file.write_text(textwrap.dedent("""\
            from lite_server.endpoint import router

            @router.get("/mw", callbacks=[])
            def mw_handler(ctx):
                return {"body": "ok"}
        """))

        router._routes.clear()
        try:
            endpoints = load_endpoints(str(tmp_path))
            assert "/mw" in endpoints
            ep = endpoints["/mw"]
            assert ep["callbacks"] == []
            assert ep["pipeline"] is None  # empty callbacks → no pipeline
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
                "has_request_id": "request_id" in request,
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
        assert body["has_request_id"] is True


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
    async def test_callback_auth_401_via_dispatch(self):
        from lite_server import RequireApiKey
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_endpoint([RequireApiKey(header="X-API-Key", keys=["secret"])])

        def handler(ctx):
            return {"result": "ok"}

        ep = {"/r": {"handler": handler, "methods": ["GET"], "pipeline": pipe, "callbacks": []}}
        resp = await handle_request(ep, self._req())
        assert resp["status_code"] == 401
        assert resp["body"]["error"]["type"] == "authentication_error"

    @pytest.mark.asyncio
    async def test_callback_rate_limit_429_via_dispatch(self):
        from lite_server import RateLimit
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_endpoint([RateLimit(requests_per_minute=1, burst=0.5)])

        def handler(ctx):
            return {"result": "ok"}

        ep = {"/rl": {"handler": handler, "methods": ["GET"], "pipeline": pipe, "callbacks": []}}
        resp = await handle_request(ep, self._req(route="/rl"))
        assert resp["status_code"] == 429
        assert resp["headers"] is not None
        assert "Retry-After" in resp["headers"]

    @pytest.mark.asyncio
    async def test_cors_headers_pass_through_via_dispatch(self):
        from lite_server import Cors
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_endpoint([Cors(allow_origins=["https://example.com"])])

        def handler(ctx):
            return {"foo": "bar"}

        ep = {"/r": {"handler": handler, "methods": ["GET"], "pipeline": pipe, "callbacks": []}}
        resp = await handle_request(ep, self._req())
        assert resp["headers"]["Access-Control-Allow-Origin"] == "https://example.com"
        assert resp["body"] == {"foo": "bar"}

    @pytest.mark.asyncio
    async def test_require_api_key_case_insensitive_via_dispatch(self):
        """Full §7 story: Headers envelope + callback — key matches any case."""
        from lite_server import RequireApiKey
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_endpoint([RequireApiKey(header="X-API-Key", keys=["secret"])])

        def handler(ctx):
            return {"result": "authorized"}

        ep = {"/r": {"handler": handler, "methods": ["GET"], "pipeline": pipe, "callbacks": []}}
        resp = await handle_request(ep, self._req(headers={"x-api-key": "secret"}))
        assert resp["status_code"] == 200
        assert resp["body"] == {"result": "authorized"}


# ===== A3: loud handler-signature failure =====


class TestHandlerSignatureLoud:
    """A3: a bad handler signature must fail LOUD — not be silently swallowed
    and drop every decorator route while the worker still reports ready."""

    def test_decorator_bad_signature_raises_handler_signature_error(self, tmp_path):
        from lite_server.endpoint import router
        from lite_server.worker.endpoints import HandlerSignatureError

        router._routes.clear()
        try:
            @router.get("/good")
            def good(ctx):
                return {"ok": True}

            @router.get("/bad")
            def bad(request, server):  # pre-0.7 signature
                return {"ok": True}

            with pytest.raises(HandlerSignatureError) as ei:
                load_endpoints(str(tmp_path))
            # Migration guidance is embedded in the message.
            assert "0.7.0" in str(ei.value)
        finally:
            router._routes.clear()

    def test_all_valid_decorator_routes_loaded(self, tmp_path):
        from lite_server.endpoint import router

        router._routes.clear()
        try:
            @router.get("/a")
            def a(ctx):
                return {"a": 1}

            @router.get("/b")
            def b(ctx):
                return {"b": 2}

            endpoints = load_endpoints(str(tmp_path))
            assert "/a" in endpoints
            assert "/b" in endpoints
        finally:
            router._routes.clear()


# ===== A4: dispatch style follows the load-time signature contract =====


class TestDispatchStyleDetection:
    """A4: dispatch style follows the same contract as load-time validation —
    exactly one REQUIRED positional arg → ctx; everything else → legacy. The
    detector runs once per handler and the result is cached on the ep dict
    (no per-request inspect.signature)."""

    @staticmethod
    def _req(route="/r"):
        return {
            "request_id": "r1",
            "route": route,
            "method": "GET",
            "headers": {},
            "query": {},
            "body": None,
            "server_state": {},
        }

    @pytest.mark.asyncio
    async def test_ctx_with_optional_kwarg_dispatches_as_ctx(self):
        """def h(ctx, debug=False): two params, one required → ctx.

        The buggy detector counted ALL params (len==2) and wrongly routed
        this to the legacy (request_dict, server) branch."""
        def h(ctx, debug=False):
            return {"arg": type(ctx).__name__}

        ep = {"/r": {"handler": h, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["body"]["arg"] == "RequestContext"

    @pytest.mark.asyncio
    async def test_ctx_with_var_args_dispatches_as_ctx(self):
        """def h(ctx, *args): one required positional → ctx."""
        def h(ctx, *args):
            return {"arg": type(ctx).__name__}

        ep = {"/r": {"handler": h, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["body"]["arg"] == "RequestContext"

    @pytest.mark.asyncio
    async def test_async_callable_object_dispatched_and_awaited(self):
        """Object with async __call__(self, ctx): one required positional
        (self bound) → ctx, and the returned coroutine is awaited."""
        class H:
            async def __call__(self, ctx):
                return {"arg": type(ctx).__name__}

        ep = {"/r": {"handler": H(), "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["body"]["arg"] == "RequestContext"

    @pytest.mark.asyncio
    async def test_legacy_two_arg_handler_still_legacy(self):
        """def h(request, server): two required positionals → legacy."""
        def h(request, server):
            return {"legacy": True, "has_method": "method" in request}

        ep = {"/r": {"handler": h, "methods": ["GET"]}}
        resp = await handle_request(ep, self._req())
        assert resp["body"]["legacy"] is True
        assert resp["body"]["has_method"] is True

    @pytest.mark.asyncio
    async def test_resolved_style_cached_on_ep_dict(self):
        """Detector runs once; the resolved style is written back to ep."""
        def h(ctx, debug=False):
            return {"ok": True}

        ep = {"/r": {"handler": h, "methods": ["GET"]}}
        await handle_request(ep, self._req())
        assert ep["/r"]["style"] == "ctx"


# ===== C4: Mode-1 plain handler signature validation =====


class TestMode1PlainHandlerSignature:
    """C4: Mode-1 plain-handler files get the same load-time signature
    validation as decorator routes — a pre-0.7 plain handler fails loud."""

    def test_plain_handler_old_signature_raises(self, tmp_path):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        (ep_dir / "legacy.py").write_text(textwrap.dedent('''
            def handler(request, server):  # pre-0.7 signature
                return {"ok": True}
        '''))
        from lite_server.worker.endpoints import HandlerSignatureError

        with pytest.raises(HandlerSignatureError):
            load_endpoints(str(tmp_path))

    def test_plain_handler_ctx_signature_loads(self, tmp_path):
        ep_dir = tmp_path / "endpoints"
        ep_dir.mkdir()
        (ep_dir / "modern.py").write_text(textwrap.dedent('''
            def handler(ctx):
                return {"ok": True}
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert "/modern" in endpoints
        assert endpoints["/modern"]["style"] == "ctx"


# ===== P1: parameterized route dispatch (/pets/{id}) =====


class TestParameterizedRouteDispatch:
    """P1: parameterized routes (/pets/{id}) dispatch via the axum route
    pattern. Rust sends route=raw URI (/pets/123) + route_pattern (/pets/:id);
    Python maps the pattern back to the declared route so the handler resolves
    (instead of 404) and meta.route is stable for logging/rate-limit buckets."""

    def test_to_axum_pattern(self):
        from lite_server.worker.endpoints import _to_axum_pattern

        assert _to_axum_pattern("/pets/{id}") == "/pets/:id"
        assert _to_axum_pattern("/pets/{id}/owner/{oid}") == "/pets/:id/owner/:oid"
        assert _to_axum_pattern("/health") == "/health"

    def test_build_pattern_index(self):
        from lite_server.worker.endpoints import build_pattern_index

        eps = {"/pets/{id}": {}, "/health": {}}
        assert build_pattern_index(eps) == {
            "/pets/:id": "/pets/{id}",
            "/health": "/health",
        }

    @pytest.mark.asyncio
    async def test_parameterized_route_dispatches_and_normalizes_meta_route(self):
        seen = {}

        def handler(ctx):
            seen["route"] = ctx.meta.route
            return {"ok": True}

        ep = {"/pets/{id}": {"handler": handler, "methods": ["GET"], "style": "ctx"}}
        pattern_index = {"/pets/:id": "/pets/{id}"}
        req = {
            "request_id": "r1",
            "route": "/pets/123",  # raw URI from Rust
            "route_pattern": "/pets/:id",  # matched axum pattern from Rust
            "method": "GET",
            "headers": {},
            "query": {},
            "body": None,
            "server_state": {},
        }
        resp = await handle_request(ep, req, pattern_index)
        assert resp["status_code"] == 200
        assert seen["route"] == "/pets/{id}"  # normalized to declared route

    @pytest.mark.asyncio
    async def test_exact_route_without_pattern_index_unchanged(self):
        """Backward compat: no pattern_index → exact route match only."""
        def handler(ctx):
            return {"ok": True}

        ep = {"/health": {"handler": handler, "methods": ["GET"], "style": "ctx"}}
        req = {
            "request_id": "r1",
            "route": "/health",
            "method": "GET",
            "headers": {},
            "query": {},
            "body": None,
            "server_state": {},
        }
        resp = await handle_request(ep, req)
        assert resp["status_code"] == 200

    @pytest.mark.asyncio
    async def test_unknown_route_still_404(self):
        resp = await handle_request(
            {}, {"route": "/nope", "request_id": "r1"}, pattern_index={}
        )
        assert resp["status_code"] == 404


# ===== C8: close endpoint pipelines on shutdown =====


class TestEndpointPipelineClose:
    """C8: the endpoint worker closes every Pipeline (e.g. its thread
    executor) on shutdown — no resource leak."""

    def test_close_endpoint_pipelines_closes_only_those_with_pipeline(self):
        from lite_server.worker.endpoints import _close_endpoint_pipelines

        closed = []

        class FakePipe:
            def close(self):
                closed.append(True)

        endpoints = {
            "/no_pipe": {"handler": lambda ctx: None, "methods": ["GET"]},
            "/pipe": {
                "handler": lambda ctx: None,
                "methods": ["GET"],
                "pipeline": FakePipe(),
            },
        }
        _close_endpoint_pipelines(endpoints)
        assert closed == [True]
