"""Port of ui/server/test/proxy.test.ts."""

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
