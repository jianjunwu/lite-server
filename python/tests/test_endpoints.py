"""pytest unit tests for lite_server.worker.endpoints."""

import textwrap

import pytest

from lite_server.worker.endpoints import (
    RegistryProxy,
    ServerProxy,
    handle_request,
    load_endpoints,
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
        ep_file = tmp_path / "status_endpoint.py"
        ep_file.write_text(textwrap.dedent('''
            methods = ["GET", "POST"]
            def handler(request, server):
                return {"status": "ok"}
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert "/status" in endpoints
        assert endpoints["/status"]["methods"] == ["GET", "POST"]

    def test_missing_handler_skipped(self, tmp_path):
        ep_file = tmp_path / "bad_endpoint.py"
        ep_file.write_text(textwrap.dedent('''
            methods = ["GET"]
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert "/bad" not in endpoints

    def test_methods_default_to_get(self, tmp_path):
        ep_file = tmp_path / "hello_endpoint.py"
        ep_file.write_text(textwrap.dedent('''
            def handler(request, server):
                return "hello"
        '''))
        endpoints = load_endpoints(str(tmp_path))
        assert endpoints["/hello"]["methods"] == ["GET"]

    def test_ignores_non_endpoint_files(self, tmp_path):
        (tmp_path / "helper.py").write_text("x = 1")
        endpoints = load_endpoints(str(tmp_path))
        assert endpoints == {}

    def test_broken_endpoint_logged_but_not_crashed(self, tmp_path, capsys):
        ep_file = tmp_path / "broken_endpoint.py"
        ep_file.write_text("raise ValueError('bad')")
        endpoints = load_endpoints(str(tmp_path))
        assert "/broken" not in endpoints
        captured = capsys.readouterr()
        assert "Failed to load endpoint" in captured.err


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
        assert "oops" in resp["body"]["error"]

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
