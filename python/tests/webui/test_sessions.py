"""Session lifecycle: opaque tokens, revocation, expiry, session management."""

import hashlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

import lite_server.webui.auth as auth_module
from lite_server.webui.app import build_app
from lite_server.webui.auth import COOKIE_NAME, UserStore
from lite_server.webui.config import InstanceStore


@pytest.fixture(autouse=True)
def _fast_bcrypt(monkeypatch):
    """bcrypt cost 12 makes every hash ~250ms; tests use the minimum cost."""
    monkeypatch.setattr(auth_module, "BCRYPT_COST", 4)


CSRF = {"x-requested-with": "lite-ui"}


def login(client: httpx.Client, username: str, password: str) -> httpx.Response:
    return client.post("/api/auth/login", headers=CSRF,
                       json={"username": username, "password": password})


@pytest.fixture
def ctx(tmp_path, upstream, live):
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
    server = live(build_app(registry, user_store=store, auth_enabled=True))
    client = httpx.Client(base_url=server.base_url)
    yield SimpleNamespace(store=store, client=client, base_url=server.base_url)
    client.close()


def second_client(ctx) -> httpx.Client:
    return httpx.Client(base_url=ctx.base_url)


def test_should_issue_opaque_cookie_and_store_only_its_hash(ctx):
    res = login(ctx.client, "admin", "boot-pass-1")
    assert res.status_code == 200
    token = res.cookies[COOKIE_NAME]
    assert "." not in token  # not a JWT
    row = ctx.store._db.query_one("SELECT id FROM sessions")
    assert row["id"] == hashlib.sha256(token.encode()).hexdigest()


def test_should_reject_replayed_token_after_logout(ctx):
    login(ctx.client, "admin", "boot-pass-1")
    stolen = ctx.client.cookies[COOKIE_NAME]
    attacker = second_client(ctx)
    attacker.cookies.set(COOKIE_NAME, stolen)
    res = ctx.client.post("/api/auth/logout", headers=CSRF)
    assert res.status_code == 200
    assert attacker.get("/api/auth/me").status_code == 401
    attacker.close()


def test_should_revoke_other_sessions_on_password_change_but_keep_current(ctx):
    login(ctx.client, "admin", "boot-pass-1")
    other = second_client(ctx)
    login(other, "admin", "boot-pass-1")
    res = ctx.client.post("/api/auth/change-password", headers=CSRF, json={
        "currentPassword": "boot-pass-1", "newPassword": "New-pass-1234"})
    assert res.status_code == 200
    assert ctx.client.get("/api/auth/me").status_code == 200
    assert other.get("/api/auth/me").status_code == 401
    other.close()


def test_should_list_own_sessions_and_revoke_one(ctx):
    ctx.store.set_password("admin", "Admin-pass-1234")  # clear must_change_password
    login(ctx.client, "admin", "Admin-pass-1234")
    other = second_client(ctx)
    login(other, "admin", "Admin-pass-1234")
    sessions = ctx.client.get("/api/auth/sessions").json()["sessions"]
    assert len(sessions) == 2
    current = [s for s in sessions if s["current"]]
    assert len(current) == 1
    victim = next(s for s in sessions if not s["current"])
    res = ctx.client.delete(f"/api/auth/sessions/{victim['id']}", headers=CSRF)
    assert res.status_code == 200
    assert other.get("/api/auth/me").status_code == 401
    other.close()


def test_should_not_let_user_revoke_another_users_session(ctx):
    ctx.store.set_password("admin", "Admin-pass-1234")
    ctx.store.create({"username": "viewer1", "password": "Viewer-pass-123", "role": "viewer"})
    ctx.store.set_password("viewer1", "Viewer-pass-123")
    login(ctx.client, "admin", "Admin-pass-1234")
    viewer = second_client(ctx)
    login(viewer, "viewer1", "Viewer-pass-123")
    admin_sessions = ctx.client.get("/api/auth/sessions").json()["sessions"]
    admin_session_id = next(s["id"] for s in admin_sessions if s["current"])
    res = viewer.delete(f"/api/auth/sessions/{admin_session_id}", headers=CSRF)
    assert res.status_code == 200  # accepted but scoped: nothing revoked
    assert ctx.client.get("/api/auth/me").status_code == 200
    viewer.close()


def test_should_let_admin_kick_user_session(ctx):
    ctx.store.set_password("admin", "Admin-pass-1234")
    ctx.store.create({"username": "viewer1", "password": "Viewer-pass-123", "role": "viewer"})
    ctx.store.set_password("viewer1", "Viewer-pass-123")
    viewer = second_client(ctx)
    login(viewer, "viewer1", "Viewer-pass-123")
    login(ctx.client, "admin", "Admin-pass-1234")
    sessions = ctx.client.get("/api/users/viewer1/sessions").json()["sessions"]
    assert len(sessions) == 1
    res = ctx.client.delete(f"/api/users/viewer1/sessions/{sessions[0]['id']}", headers=CSRF)
    assert res.status_code == 200
    assert viewer.get("/api/auth/me").status_code == 401
    viewer.close()


def test_should_expire_session_after_ttl(ctx):
    login(ctx.client, "admin", "boot-pass-1")
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    ctx.store._db.execute("UPDATE sessions SET expires_at = ?", (past,))
    assert ctx.client.get("/api/auth/me").status_code == 401


def test_should_delete_sessions_when_user_deleted(ctx):
    ctx.store.create({"username": "viewer1", "password": "Viewer-pass-123", "role": "viewer"})
    ctx.store.set_password("viewer1", "Viewer-pass-123")
    viewer = second_client(ctx)
    login(viewer, "viewer1", "Viewer-pass-123")
    ctx.store.remove("viewer1", actor="admin")
    assert viewer.get("/api/auth/me").status_code == 401
    assert ctx.store._db.query("SELECT id FROM sessions") == []
    viewer.close()


def test_should_return_namespaced_marker_on_unauthenticated(ctx):
    res = ctx.client.get("/api/instances")
    assert res.status_code == 401
    assert res.json()["error"] == "bff_unauthenticated"
