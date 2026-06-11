"""pytest unit tests for lite_server.worker.endpoints."""

import logging
import os
import socket
import sys
import textwrap

import pytest

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
