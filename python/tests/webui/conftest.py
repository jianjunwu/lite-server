"""Shared fixtures for webui tests: upstream stub server and live uvicorn runner."""

from __future__ import annotations

import gzip
import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
import uvicorn

from lite_server.webui.config import InstanceConfig


def unused_port() -> int:
    """A port that was just closed — connection to it is refused."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class MapRegistry:
    """Plain in-memory registry for proxy tests (no yaml backing)."""

    def __init__(self, instances: list[InstanceConfig]):
        self._instances = {i.id: i for i in instances}

    def list(self) -> list[InstanceConfig]:
        return list(self._instances.values())

    def get(self, inst_id: str) -> InstanceConfig | None:
        return self._instances.get(inst_id)


class _UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send_json(self, payload: bytes, status: int = 200):
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _record_and_route(self):
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        self.server.last_request = {
            "headers": {k.lower(): v for k, v in self.headers.items()},
            "body": body.decode(),
            "url": self.path,
        }

        if self.path == "/sse":
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("connection", "close")
            self.end_headers()
            self.wfile.write(b"data: one\n\n")
            self.wfile.flush()
            self.wfile.write(b"data: two\n\n")
            self.wfile.flush()
            self.close_connection = True
            return
        if self.path == "/sse-slow":
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("connection", "close")
            self.end_headers()
            self.wfile.write(b"data: one\n\n")
            self.wfile.flush()
            time.sleep(0.5)
            self.wfile.write(b"data: two\n\n")
            self.wfile.flush()
            self.close_connection = True
            return
        if self.path == "/setcookie":
            payload = json.dumps({"ok": True}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("set-cookie", "lite_ui_token=forged; Path=/")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/gzip":
            # Compresses regardless of accept-encoding, to simulate an upstream
            # (or its own reverse proxy) that always gzips.
            payload = gzip.compress(b"gzip-me")
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-encoding", "gzip")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/hang":
            # Simulates an instance that accepts but stalls mid-response.
            time.sleep(2)
        if self.command == "DELETE" and self.path.endswith("/versions"):
            # Batch version delete: per-version results like the instance
            # ({"deleted": [...], "failed": [...]}); version "bad" fails.
            requested = (json.loads(body).get("versions") or []) if body else []
            payload = json.dumps({
                "deleted": [v for v in requested if v != "bad"],
                "failed": [{"version": v, "error": "version is locked"}
                           for v in requested if v == "bad"],
            }).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path.endswith("/upload-sessions") and self.command == "POST":
            # Chunked-upload session init: the instance returns a small JSON
            # with the new session id (201 Created).
            payload = json.dumps({"session_id": "stub-session-id", "chunk_size": 64}).encode()
            self.send_response(201)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/v2/models/m/versions/1/config" and self.command in ("GET", "PATCH"):
            # Config PATCH waits for a synchronous model reload on the
            # instance, which can far exceed the unary read timeout.
            time.sleep(1)
        if self.path.endswith(("/upload", "/download")):
            # Simulates a long upload-finalize / download-pack before the
            # response headers arrive (bounded by the transfer timeout, not
            # the unary one).
            time.sleep(1)
        if self.path == "/v2/models":
            # Loaded-models list, shaped like the instance handler.
            payload = json.dumps({"models": [
                {"name": "alpha", "version": "1", "status": "ready",
                 "model_type": "LitAPI", "workers": 1},
                {"name": "beta", "version": "1", "status": "ready",
                 "model_type": "LitAPI", "workers": 1},
            ]}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/v2/repository/index" and self.command == "POST":
            # On-disk scan, shaped like the instance handler.
            payload = json.dumps({"models": [
                {"name": "alpha", "version": "1", "path": "/r/alpha/1",
                 "has_config": True, "type": "litapi"},
                {"name": "beta", "version": "2", "path": "/r/beta/2",
                 "has_config": True, "type": "litapi"},
                {"name": "gamma", "version": "1", "path": "/r/gamma/1",
                 "has_config": True, "type": "litapi"},
            ]}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/metrics/timeline":
            payload = json.dumps({"snapshots": [
                {"model": "alpha", "version": "1", "entries": []},
                {"model": "beta", "version": "1", "entries": []},
            ]}).encode()
            self._send_json(payload)
            return
        if self.path == "/metrics/alerts":
            payload = json.dumps({"alerts": [
                {"model": "alpha", "version": "1", "rule": "qps", "message": "a",
                 "severity": "warning", "timestamp": 1, "value": 1, "threshold": 2},
                {"model": "beta", "version": "1", "rule": "qps", "message": "b",
                 "severity": "warning", "timestamp": 1, "value": 1, "threshold": 2},
            ]}).encode()
            self._send_json(payload)
            return
        if self.path == "/health":
            payload = json.dumps({"status": "ready", "models": [
                {"name": "alpha", "active_version": "1", "versions": []},
                {"name": "beta", "active_version": "1", "versions": []},
            ]}).encode()
            self._send_json(payload)
            return
        if self.path == "/info":
            # Same shape as the real instance (src/http/handlers/health.rs):
            # loaded_models rows are "name/version" strings.
            payload = json.dumps({"server": "lite-server", "version": "0.1.0",
                                  "loaded_models": ["alpha/1", "beta/1"]}).encode()
            self._send_json(payload)
            return
        if self.path == "/v2/repository/drift":
            payload = json.dumps({
                "configured_missing": [{"model": "alpha", "version": "9"}],
                "on_disk_unconfigured": [
                    {"model": "alpha", "version": "3", "size_bytes": 1,
                     "ensemble_referenced": False},
                    {"model": "beta", "version": "2", "size_bytes": 1,
                     "ensemble_referenced": False},
                ],
            }).encode()
            self._send_json(payload)
            return
        if self.path == "/fail500":
            payload = json.dumps({"error": "boom"}).encode()
            self.send_response(500)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        payload = json.dumps({"ok": True, "url": self.path}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("x-request-id", "req-123")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = _record_and_route
    do_POST = _record_and_route
    do_PUT = _record_and_route
    do_DELETE = _record_and_route
    do_PATCH = _record_and_route

    def log_message(self, *args):
        pass


class UpstreamServer:
    def __init__(self):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _UpstreamHandler)
        self.httpd.last_request = None
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    @property
    def last_request(self):
        return self.httpd.last_request

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


class LiveServer:
    """Runs an ASGI app under uvicorn in a background thread on a free port."""

    def __init__(self, app):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        sock.close()
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        for _ in range(500):
            if self.server.started:
                break
            time.sleep(0.02)
        else:
            raise RuntimeError("uvicorn failed to start")
        self.base_url = f"http://127.0.0.1:{self.port}"

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=5)


@pytest.fixture
def upstream():
    server = UpstreamServer()
    yield server
    server.stop()


@pytest.fixture
def live():
    servers = []

    def factory(app) -> LiveServer:
        server = LiveServer(app)
        servers.append(server)
        return server

    yield factory
    for server in servers:
        server.stop()


@pytest.fixture
def client_factory(live):
    """Returns a function app -> httpx.Client bound to a live server."""

    def make(app, **kwargs) -> httpx.Client:
        server = live(app)
        return httpx.Client(base_url=server.base_url, **kwargs)

    return make
