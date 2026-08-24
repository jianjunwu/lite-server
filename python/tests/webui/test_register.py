"""Open first-user registration and invite-code registration."""

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


CSRF = {"x-requested-with": "lite-ui"}


def register(client: httpx.Client, username: str, password: str,
             invite_code: str | None = None) -> httpx.Response:
    body: dict = {"username": username, "password": password}
    if invite_code is not None:
        body["inviteCode"] = invite_code
    return client.post("/api/auth/register", headers=CSRF, json=body)


@pytest.fixture
def ctx(tmp_path, upstream, live):
    # No LITE_UI_ADMIN_PASSWORD: the store starts empty, so registration is open.
    store = UserStore(
        str(tmp_path / "auth.db"), {},
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


def _make_invite(store, role="viewer", max_uses=1, expires_in_hours=72, created_by="admin"):
    return store.create_invite(role=role, max_uses=max_uses,
                               expires_in_hours=expires_in_hours, created_by=created_by)


def test_should_report_registration_open_when_no_users(ctx):
    res = ctx.client.get("/api/auth/registration")
    assert res.json() == {"open": True, "inviteRequired": False}


def test_should_allow_first_user_registration_as_admin_then_close(ctx):
    res = register(ctx.client, "founder", "Founder-pass-1")
    assert res.status_code == 201
    assert res.json()["user"]["role"] == "admin"
    assert res.json()["user"]["mustChangePassword"] is False
    status = ctx.client.get("/api/auth/registration").json()
    assert status == {"open": False, "inviteRequired": True}


def test_should_auto_login_after_register(ctx):
    register(ctx.client, "founder", "Founder-pass-1")
    res = ctx.client.get("/api/auth/me")
    assert res.status_code == 200
    assert res.json()["user"]["username"] == "founder"


def test_should_require_invite_after_first_user(ctx):
    register(ctx.client, "founder", "Founder-pass-1")
    res = register(ctx.client, "second", "Second-pass-1")
    assert res.status_code == 403
    assert res.json()["error"] == "invite_required"


def test_should_register_with_invite_and_bind_its_role(ctx):
    register(ctx.client, "founder", "Founder-pass-1")
    invite = _make_invite(ctx.store, role="operator")
    res = register(ctx.client, "op2", "Operator-pass-1", invite_code=invite["code"])
    assert res.status_code == 201
    assert res.json()["user"]["role"] == "operator"


def test_should_consume_invite_once_and_reject_reuse(ctx):
    register(ctx.client, "founder", "Founder-pass-1")
    invite = _make_invite(ctx.store)
    assert register(ctx.client, "u1", "First-pass-123", invite_code=invite["code"]).status_code == 201
    res = register(ctx.client, "u2", "Second-pass-12", invite_code=invite["code"])
    assert res.status_code == 400
    assert res.json()["error"] == "invalid_invite"


def test_should_reject_unknown_expired_and_revoked_invites(ctx):
    register(ctx.client, "founder", "Founder-pass-1")
    assert register(ctx.client, "u1", "First-pass-123", invite_code="nope").status_code == 400

    expired = _make_invite(ctx.store, expires_in_hours=0)
    assert register(ctx.client, "u1", "First-pass-123", invite_code=expired["code"]).status_code == 400

    revoked = _make_invite(ctx.store)
    ctx.store.revoke_invite(revoked["code"])
    assert register(ctx.client, "u1", "First-pass-123", invite_code=revoked["code"]).status_code == 400


def test_should_enforce_password_policy_on_register(ctx):
    res = register(ctx.client, "founder", "weak")
    assert res.status_code == 400


def test_should_require_csrf_header_on_register(ctx):
    res = ctx.client.post("/api/auth/register",
                          json={"username": "founder", "password": "Founder-pass-1"})
    assert res.status_code == 403
    assert res.json()["error"] == "csrf_header_missing"


def test_should_let_admin_manage_invites(ctx):
    register(ctx.client, "founder", "Founder-pass-1")
    res = ctx.client.post("/api/invites", headers=CSRF, json={"role": "viewer", "maxUses": 2})
    assert res.status_code == 201
    code = res.json()["invite"]["code"]
    listing = ctx.client.get("/api/invites").json()["invites"]
    assert any(i["code"] == code and i["maxUses"] == 2 for i in listing)
    assert ctx.client.delete(f"/api/invites/{code}", headers=CSRF).status_code == 200
    listing = ctx.client.get("/api/invites").json()["invites"]
    assert next(i for i in listing if i["code"] == code)["revokedAt"] is not None


def test_should_forbid_invite_management_for_non_admin(ctx):
    register(ctx.client, "founder", "Founder-pass-1")
    invite = _make_invite(ctx.store)
    viewer = httpx.Client(base_url=ctx.base_url)
    register(viewer, "v1", "Viewer-pass-12", invite_code=invite["code"])
    assert viewer.get("/api/invites").status_code == 403
    assert viewer.post("/api/invites", headers=CSRF, json={}).status_code == 403
    viewer.close()


def test_should_bootstrap_from_env_password_without_open_registration(tmp_path, upstream, live):
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
    assert store.verify("admin", "boot-pass-1") is not None
    assert client.get("/api/auth/registration").json() == {"open": False, "inviteRequired": True}
    client.close()
