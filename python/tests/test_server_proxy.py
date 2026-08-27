"""Tests for ServerProxy (``ctx.server``) — the loopback HTTP proxy that
@route handlers use to query the hosting server (registry + cross-model
inference)."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from lite_server.server_proxy import ServerProxy, ServerProxyError


class _Handler(BaseHTTPRequestHandler):
    models: list = []
    models_status = 200
    infer_status = 200
    infer_response: dict = {}
    metrics_text = ""
    metrics_status = 200
    requests: list = []
    raw_models: bytes | None = None

    @classmethod
    def reset(cls):
        cls.models = [
            {"name": "pets", "version": "1", "status": "Ready",
             "model_type": "Standard", "workers": 1},
        ]
        cls.models_status = 200
        cls.infer_status = 200
        cls.infer_response = {"output": 42}
        cls.metrics_text = (
            "# HELP lite_server_infer_requests_total Total inference requests\n"
            "# TYPE lite_server_infer_requests_total counter\n"
            'lite_server_infer_requests_total{model="pets",version="1"} 42\n'
            'lite_server_infer_requests_total{model="cats",version="1"} 7\n'
            "# TYPE lite_server_uptime_seconds gauge\n"
            "lite_server_uptime_seconds 3600.5\n"
        )
        cls.metrics_status = 200
        cls.requests = []
        cls.raw_models = None

    def _json(self, code: int, payload):
        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        type(self).requests.append(("GET", self.path, None))
        if self.path == "/v2/models" and type(self).raw_models is not None:
            data = type(self).raw_models
            self.send_response(type(self).models_status)
            self.send_header("content-type", "text/html")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/v2/models":
            self._json(type(self).models_status, {"models": type(self).models})
        elif self.path == "/metrics":
            data = type(self).metrics_text.encode()
            self.send_response(type(self).metrics_status)
            self.send_header("content-type", "text/plain; charset=utf-8")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        body = json.loads(self.rfile.read(length) or b"null")
        type(self).requests.append(("POST", self.path, body))
        if self.path.endswith("/infer"):
            self._json(type(self).infer_status, type(self).infer_response)
        else:
            self._json(404, {"error": "not found"})

    def log_message(self, *args):
        pass


@pytest.fixture
def base_url():
    _Handler.reset()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()
        httpd.server_close()


class TestRegistryProxy:
    def test_list_loaded_returns_models(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.registry.list_loaded() == _Handler.models
        assert ("GET", "/v2/models", None) in _Handler.requests

    def test_list_loaded_empty(self, base_url):
        _Handler.models = []
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.registry.list_loaded() == []

    def test_get_returns_first_match(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.registry.get("pets")["version"] == "1"

    def test_get_missing_returns_none(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.registry.get("nope") is None

    def test_http_error_raises(self, base_url):
        _Handler.models_status = 500
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(ServerProxyError):
            srv.registry.list_loaded()


class TestInferenceProxy:
    @pytest.mark.asyncio
    async def test_infer_posts_to_unversioned_path(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        out = await srv.inference.infer("adder", {"input": 5})
        assert out == {"output": 42}
        assert ("POST", "/v2/models/adder/infer", {"input": 5}) in _Handler.requests

    @pytest.mark.asyncio
    async def test_infer_with_version_uses_versioned_path(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        await srv.inference.infer("adder", {"input": 5}, version="2")
        assert _Handler.requests[-1][1] == "/v2/models/adder/versions/2/infer"

    @pytest.mark.asyncio
    async def test_infer_own_model_rejected_before_http(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(ValueError, match="pets"):
            await srv.inference.infer("pets", {"input": 5})
        assert _Handler.requests == []

    @pytest.mark.asyncio
    async def test_infer_own_model_same_version_rejected(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(ValueError):
            await srv.inference.infer("pets", {"input": 5}, version="1")
        assert _Handler.requests == []

    @pytest.mark.asyncio
    async def test_infer_own_model_other_version_allowed(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        out = await srv.inference.infer("pets", {"input": 5}, version="2")
        assert out == {"output": 42}
        assert _Handler.requests[-1][1] == "/v2/models/pets/versions/2/infer"

    @pytest.mark.asyncio
    async def test_infer_http_error_raises(self, base_url):
        _Handler.infer_status = 404
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(ServerProxyError):
            await srv.inference.infer("adder", {"input": 5})


class TestMetricsProxy:
    """``server.metrics.query`` scrapes the server's ``/metrics`` (Prometheus
    text format) and returns the first sample matching name + labels."""

    def test_query_by_name_returns_value(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.metrics.query("lite_server_uptime_seconds") == 3600.5
        assert ("GET", "/metrics", None) in _Handler.requests

    def test_query_with_labels_matches_sample(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.metrics.query(
            "lite_server_infer_requests_total", model="cats") == 7.0

    def test_query_first_match_when_unlabeled(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.metrics.query("lite_server_infer_requests_total") == 42.0

    def test_query_unknown_metric_returns_none(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.metrics.query("no_such_metric") is None

    def test_query_label_mismatch_returns_none(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        assert srv.metrics.query(
            "lite_server_infer_requests_total", model="dogs") is None

    def test_query_http_error_raises(self, base_url):
        _Handler.metrics_status = 500
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(ServerProxyError):
            srv.metrics.query("lite_server_uptime_seconds")


class TestAdminStubs:
    """Remote admin ops stay out of scope — still loud stubs."""

    def test_load_model_still_stubbed(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(NotImplementedError):
            srv.load_model("m", "1")

    def test_unload_model_still_stubbed(self, base_url):
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(NotImplementedError):
            srv.unload_model("m")


class TestRegistryProxyAudit:
    """Audit: a 2xx non-JSON body must surface as ServerProxyError, not a raw
    JSON decode error (the documented error contract)."""

    def test_non_json_200_raises_server_proxy_error(self, base_url):
        _Handler.raw_models = b"<html>gateway error</html>"
        srv = ServerProxy.for_model(base_url, "pets", "1")
        with pytest.raises(ServerProxyError):
            srv.registry.list_loaded()
