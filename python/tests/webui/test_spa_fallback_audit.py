"""Audit: unknown /api/* GETs must 404, not fall through to the SPA shell.

``_SpaFiles.get_response`` catches every 404 and serves ``index.html`` —
including for ``/api/...`` paths the routers don't own, so a typo'd or
removed API endpoint reads as a 200 HTML page instead of a JSON 404.
"""

from __future__ import annotations

import httpx
import pytest

from lite_server.webui.app import build_app
from lite_server.webui.auth import UserStore
from lite_server.webui.config import InstanceConfig
from lite_server.webui.registry import InstanceStore

from .conftest import MapRegistry

CSRF = {"x-requested-with": "lite-ui"}


@pytest.fixture
def ctx(tmp_path, live):
    store = UserStore(
        str(tmp_path / "auth.db"), {"LITE_UI_ADMIN_PASSWORD": "boot-pass-1"},
        legacy_auth_path=str(tmp_path / "auth.yaml"),
    )
    store.set_password("admin", "Admin-pass-1234")
    registry = MapRegistry([])
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<html>spa</html>")
    server = live(build_app(registry, web_dist=str(static),
                            user_store=store, auth_enabled=True))
    client = httpx.Client(base_url=server.base_url)
    res = client.post("/api/auth/login", headers=CSRF,
                      json={"username": "admin", "password": "Admin-pass-1234"})
    assert res.status_code == 200, res.text
    yield client
    client.close()


def test_unknown_api_get_returns_json_404_not_spa_html(ctx):
    res = ctx.get("/api/definitely-not-a-route")
    assert res.status_code == 404, res.text[:200]
