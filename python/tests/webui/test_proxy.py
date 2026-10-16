"""Port of ui/server/test/proxy.test.ts."""

import asyncio
import threading
import time

import httpx
import pytest

from lite_server.webui.app import build_app
from lite_server.webui.config import InstanceConfig

from .conftest import MapRegistry, unused_port


@pytest.fixture
def registry(upstream):
    return MapRegistry([
        InstanceConfig(id="plain", name="Plain", base_url=upstream.base_url),
        InstanceConfig(id="keyed", name="Keyed", base_url=upstream.base_url, admin_key="server-key"),
        InstanceConfig(id="dead", name="Dead", base_url=f"http://127.0.0.1:{unused_port()}"),
    ])


@pytest.fixture
def client(registry, client_factory):
    return client_factory(build_app(registry))


def test_should_list_instances_without_leaking_admin_key(client):
    res = client.get("/api/instances")
    assert res.status_code == 200
    body = res.json()
    assert len(body["instances"]) == 3
    keyed = next(i for i in body["instances"] if i["id"] == "keyed")
    assert keyed["has_admin_key"] is True
    assert "server-key" not in res.text


def test_should_forward_get_with_query_and_return_upstream_response(client):
    res = client.get("/api/i/plain/v2/models?x=1")
    assert res.status_code == 200
    assert res.json() == {"ok": True, "url": "/v2/models?x=1"}
    assert res.headers["x-request-id"] == "req-123"


def test_should_return_404_for_unknown_instance(client):
    res = client.get("/api/i/nope/v2/models")
    assert res.status_code == 404
    assert res.json()["error"] == "unknown_instance"


def test_should_inject_instance_admin_key_when_browser_sends_none(client, upstream):
    client.get("/api/i/keyed/v2/models")
    assert upstream.last_request["headers"].get("x-admin-key") == "server-key"


def test_should_prefer_browser_admin_key_over_instance_key(client, upstream):
    client.get("/api/i/keyed/v2/models", headers={"x-admin-key": "browser-key"})
    assert upstream.last_request["headers"].get("x-admin-key") == "browser-key"


def test_should_not_send_admin_key_header_for_keyless_instance(client, upstream):
    client.get("/api/i/plain/v2/models")
    assert "x-admin-key" not in upstream.last_request["headers"]


def test_should_forward_post_body(client, upstream):
    res = client.post("/api/i/plain/v2/models/m/infer",
                      content=b'{"input": 21}', headers={"content-type": "application/json"})
    assert res.status_code == 200
    assert upstream.last_request["body"] == '{"input": 21}'


def test_should_stream_sse_response_end_to_end(client):
    res = client.get("/api/i/plain/sse")
    assert res.status_code == 200
    assert "text/event-stream" in res.headers["content-type"]
    assert res.text == "data: one\n\ndata: two\n\n"


def test_should_not_buffer_sse_chunks(registry, live):
    server = live(build_app(registry))
    start = time.monotonic()
    with httpx.Client(base_url=server.base_url) as client:
        with client.stream("GET", "/api/i/plain/sse-slow") as res:
            first_chunk_at = None
            chunks = []
            for chunk in res.iter_bytes():
                if first_chunk_at is None:
                    first_chunk_at = time.monotonic() - start
                chunks.append(chunk)
    # The upstream waits 0.5s between events; the first must arrive well before that.
    assert first_chunk_at is not None and first_chunk_at < 0.4
    assert b"".join(chunks) == b"data: one\n\ndata: two\n\n"


def test_should_pass_through_upstream_error_status_and_body(client):
    res = client.get("/api/i/plain/fail500")
    assert res.status_code == 500
    assert res.json() == {"error": "boom"}


def test_should_return_502_when_instance_unreachable(client):
    res = client.get("/api/i/dead/v2/models")
    assert res.status_code == 502
    body = res.json()
    assert body["error"] == "instance_unreachable"
    assert body["instance"] == "dead"


def test_should_not_forward_browser_credentials_to_upstream(client, upstream):
    client.get("/api/i/plain/v2/models",
               headers={"cookie": "lite_ui_token=SECRETJWT", "authorization": "Bearer abc"})
    assert "cookie" not in upstream.last_request["headers"]
    assert "authorization" not in upstream.last_request["headers"]


def test_should_not_forward_browser_origin_or_referer_to_upstream(client, upstream):
    # The proxy is the origin of the upstream request; the browser's own
    # Origin/Referer must not be relayed (a cross-origin value would trip the
    # upstream's admin CSRF guard and break the admin UI).
    client.get("/api/i/plain/v2/models", headers={
        "origin": "http://localhost:5173",
        "referer": "http://localhost:5173/settings",
    })
    assert "origin" not in upstream.last_request["headers"]
    assert "referer" not in upstream.last_request["headers"]


def test_should_inject_admin_key_while_stripping_browser_origin(client, upstream):
    client.get("/api/i/keyed/v2/models", headers={"origin": "http://localhost:5173"})
    assert upstream.last_request["headers"].get("x-admin-key") == "server-key"
    assert "origin" not in upstream.last_request["headers"]


def test_should_not_forward_upstream_set_cookie_to_browser(client):
    res = client.get("/api/i/plain/setcookie")
    assert res.status_code == 200
    assert "set-cookie" not in res.headers


def test_should_round_trip_gzipped_upstream_body_intact(client):
    # The body and its content-encoding must stay consistent end to end:
    # either raw bytes + encoding header, or decoded bytes + no header.
    res = client.get("/api/i/plain/gzip")
    assert res.status_code == 200
    assert res.text == "gzip-me"


def test_should_stream_request_body_without_buffering(client, upstream, monkeypatch):
    async def _boom(self):
        raise AssertionError("proxy must not buffer the request body in memory")

    monkeypatch.setattr("starlette.requests.Request.body", _boom)
    payload = b"x" * 4096
    res = client.post("/api/i/plain/v2/repository/models/m", content=payload)
    assert res.status_code == 200
    assert upstream.last_request["body"].encode() == payload


def test_should_time_out_unary_request_when_upstream_hangs(registry, live):
    app = build_app(registry, unary_timeout=0.5)
    start = time.monotonic()
    with httpx.Client(base_url=live(app).base_url, timeout=10) as c:
        res = c.get("/api/i/plain/hang")
    assert res.status_code == 502
    assert time.monotonic() - start < 2


def test_should_not_time_out_sse_stream(registry, live):
    # SSE responses are exempt from the unary read timeout.
    app = build_app(registry, unary_timeout=0.3)
    with httpx.Client(base_url=live(app).base_url, timeout=10) as c:
        res = c.get("/api/i/plain/sse-slow", headers={"accept": "text/event-stream"})
    assert res.status_code == 200
    assert res.text == "data: one\n\ndata: two\n\n"


def test_should_not_time_out_slow_upload_finalize(registry, live):
    # Upload routes are exempt from the unary read timeout: the instance may
    # spend minutes unpacking/committing a multi-GB artifact before responding.
    app = build_app(registry, unary_timeout=0.3)
    with httpx.Client(base_url=live(app).base_url, timeout=10) as c:
        res = c.post("/api/i/plain/v2/repository/models/m/versions/1/upload",
                     content=b"chunk-bytes")
    assert res.status_code == 200


def test_should_not_time_out_slow_download_pack(registry, live):
    # Download routes are exempt too: a first-time download repacks the
    # version tree before the first response byte.
    app = build_app(registry, unary_timeout=0.3)
    with httpx.Client(base_url=live(app).base_url, timeout=10) as c:
        res = c.get("/api/i/plain/v2/repository/models/m/versions/1/download")
    assert res.status_code == 200


def test_should_not_time_out_slow_config_patch(registry, live):
    # Config PATCH waits for a synchronous reload_model on the instance,
    # which takes minutes for large models; it must not be bounded by the
    # unary read timeout, or a successful write+reload surfaces as a 502.
    app = build_app(registry, unary_timeout=0.3)
    with httpx.Client(base_url=live(app).base_url, timeout=10) as c:
        res = c.patch("/api/i/plain/v2/models/m/versions/1/config",
                      content=b'{"max_batch_size": 8}',
                      headers={"content-type": "application/json"})
    assert res.status_code == 200


def test_should_still_time_out_config_get(registry, live):
    # Only the PATCH is long-running; config GET stays on the bounded unary
    # client so a stalled instance cannot pin a connection.
    app = build_app(registry, unary_timeout=0.3)
    start = time.monotonic()
    with httpx.Client(base_url=live(app).base_url, timeout=10) as c:
        res = c.get("/api/i/plain/v2/models/m/versions/1/config")
    assert res.status_code == 502
    assert time.monotonic() - start < 2


def test_should_default_config_patch_timeout_to_600_seconds():
    # Reload of a large model is minute-scale; align with worker startup
    # timeout magnitude rather than the 60s unary default.
    app = build_app(MapRegistry([]))
    assert app.state.http_long.timeout.read == 600.0


def test_should_default_stream_pool_to_500_connections():
    # SSE inference streams and file transfers each pin one upstream
    # connection for their lifetime; the default httpx pool (100) would
    # queue new streams well below expected concurrent load.
    app = build_app(MapRegistry([]))
    assert app.state.http_stream._transport._pool._max_connections == 500


def test_should_honor_stream_max_connections_override():
    app = build_app(MapRegistry([]), stream_max_connections=7)
    assert app.state.http_stream._transport._pool._max_connections == 7


def test_stream_client_timeouts_bound_idle_gaps_and_pool_queue():
    # A half-open upstream (TCP alive, zero bytes) must not pin a pool slot
    # forever, and pool saturation must surface as an error, not an
    # infinite queue.
    app = build_app(MapRegistry([]))
    timeout = app.state.http_stream.timeout
    assert timeout.connect == 10.0
    assert timeout.read == 300.0
    assert timeout.pool == 60.0


def test_should_close_upstream_when_client_disconnects_mid_stream(registry, live, monkeypatch):
    closed = threading.Event()
    original_aclose = httpx.Response.aclose

    async def _spy(self):
        closed.set()
        await original_aclose(self)

    monkeypatch.setattr(httpx.Response, "aclose", _spy)
    server = live(build_app(registry))
    with httpx.Client(base_url=server.base_url) as client:
        with client.stream("GET", "/api/i/plain/sse-slow") as res:
            for _ in res.iter_bytes():
                break  # simulate the browser going away mid-stream
    assert closed.wait(timeout=5)


def test_should_return_499_when_client_aborts_mid_upload(upstream, monkeypatch):
    from starlette.requests import ClientDisconnect

    async def _abort(self):
        raise ClientDisconnect()
        yield b""

    monkeypatch.setattr("starlette.requests.Request.stream", _abort)
    app = build_app(MapRegistry([
        InstanceConfig(id="plain", name="Plain", base_url=upstream.base_url),
    ]))

    async def _run():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            return await c.post("/api/i/plain/v2/repository/models/m/versions/1/upload",
                                content=b"x" * 16)

    res = asyncio.run(_run())
    assert res.status_code == 499


def test_record_success_failure_still_closes_upstream(monkeypatch):
    """Leak guard: on a non-buffered mutation, an ownership bookkeeping
    failure (e.g. SQLite fault in record_success) must still close the
    upstream response — otherwise the connection-pool slot leaks to GC."""
    from types import SimpleNamespace

    from starlette.requests import Request

    from lite_server.webui import ownership, proxy

    class FakeUpstream:
        status_code = 200
        headers = {"content-type": "application/json"}

        def __init__(self):
            self.closed = False

        async def aiter_raw(self):
            yield b"{}"

        async def aclose(self):
            self.closed = True

    class FakeClient:
        def __init__(self, upstream):
            self.upstream = upstream

        def build_request(self, *args, **kwargs):
            return object()

        async def send(self, req, stream=False):
            return self.upstream

    upstream = FakeUpstream()
    inst = InstanceConfig(id="inst", name="Inst", base_url="http://upstream")
    app = SimpleNamespace(state=SimpleNamespace(
        registry=MapRegistry([inst]),
        user_store=object(),
        http=FakeClient(upstream),
        http_long=FakeClient(upstream),
        http_stream=FakeClient(upstream),
    ))

    # Ownership gates pass; the bookkeeping write itself fails.
    monkeypatch.setattr(ownership, "check_model_grant", lambda *a, **k: None)
    monkeypatch.setattr(ownership, "check_ownership", lambda *a, **k: None)

    def _boom(*a, **k):
        raise RuntimeError("sqlite gone")

    monkeypatch.setattr(ownership, "record_success", _boom)

    async def _receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    scope = {
        "type": "http", "method": "DELETE", "headers": [],
        "query_string": b"", "path": "/api/i/inst/v2/models/m/versions/1",
        "app": app, "state": {"user": {"username": "alice"}},
    }
    request = Request(scope, _receive)

    async def _run():
        with pytest.raises(RuntimeError, match="sqlite gone"):
            await proxy._proxy(request, "inst", "v2/models/m/versions/1")

    asyncio.run(_run())
    assert upstream.closed, "upstream response must be closed when record_success raises"


def test_lifespan_closes_remaining_clients_when_one_raises():
    """Leak guard: a failing aclose() on one http client must not skip the
    remaining clients and the auth store during shutdown."""
    from types import SimpleNamespace

    from lite_server.webui.app import _lifespan

    closed = []

    class FakeClient:
        def __init__(self, name, exc=None):
            self.name = name
            self.exc = exc

        async def aclose(self):
            if self.exc is not None:
                raise self.exc
            closed.append(self.name)

    class FakeStore:
        def close(self):
            closed.append("store")

    app = SimpleNamespace(state=SimpleNamespace(
        http=FakeClient("http", RuntimeError("pool stuck")),
        http_long=FakeClient("http_long"),
        http_stream=FakeClient("http_stream"),
        user_store=FakeStore(),
    ))

    async def _run():
        async with _lifespan(app):
            pass

    asyncio.run(_run())
    assert sorted(closed) == ["http_long", "http_stream", "store"], (
        f"shutdown must close every resource even when one fails, got {closed}"
    )
