"""Security response headers on API and SPA responses."""

from types import SimpleNamespace

import httpx
import pytest

import lite_server.webui.auth as auth_module
from lite_server.webui.app import build_app
from lite_server.webui.auth import UserStore
from lite_server.webui.config import InstanceStore


@pytest.fixture(autouse=True)
def _fast_bcrypt(monkeypatch):
    """bcrypt cost 12 makes every hash ~250ms; tests use the minimum cost."""
    monkeypatch.setattr(auth_module, "BCRYPT_COST", 4)


@pytest.fixture
def ctx(tmp_path, upstream, live):
    web_dist = tmp_path / "dist"
    web_dist.mkdir()
    (web_dist / "index.html").write_text("<html></html>", encoding="utf-8")
    store = UserStore(
        str(tmp_path / "auth.db"), {"LITE_UI_ADMIN_PASSWORD": "boot-pass-1"},
        legacy_auth_path=str(tmp_path / "auth.yaml"),
    )
    config_path = tmp_path / "instances.yaml"
    config_path.write_text(
        f'instances:\n  - {{ id: plain, name: P, base_url: "{upstream.base_url}" }}\n',
        encoding="utf-8",
    )
    registry = InstanceStore(str(config_path), {})
    server = live(build_app(registry, web_dist=str(web_dist), user_store=store, auth_enabled=True))
    client = httpx.Client(base_url=server.base_url)
    yield SimpleNamespace(store=store, client=client, base_url=server.base_url)
    client.close()


def _assert_security_headers(res: httpx.Response):
    csp = res.headers["content-security-policy"]
    assert "default-src 'self'" in csp
    assert "script-src 'self'" in csp
    assert res.headers["x-content-type-options"] == "nosniff"
    assert res.headers["x-frame-options"] == "DENY"
    assert res.headers["referrer-policy"] == "same-origin"


def test_should_set_security_headers_on_api_responses(ctx):
    _assert_security_headers(ctx.client.get("/api/instances"))


def test_should_set_security_headers_on_spa_responses(ctx):
    res = ctx.client.get("/")
    assert res.status_code == 200
    _assert_security_headers(res)
