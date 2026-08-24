"""TOTP two-factor auth: enrollment, login challenge, backup codes, admin reset."""

from types import SimpleNamespace

import httpx
import pyotp
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
    store.set_password("admin", "Admin-pass-1234")  # clear must_change_password
    config_path = tmp_path / "instances.yaml"
    config_path.write_text(
        f'instances:\n  - {{ id: plain, name: P, base_url: "{upstream.base_url}" }}\n',
        encoding="utf-8",
    )
    registry = InstanceStore(str(config_path), {})
    server = live(build_app(registry, user_store=store, auth_enabled=True))
    client = httpx.Client(base_url=server.base_url)
    login(client, "admin", "Admin-pass-1234")
    yield SimpleNamespace(store=store, client=client, base_url=server.base_url)
    client.close()


def enroll(client: httpx.Client) -> str:
    res = client.post("/api/auth/totp/enroll", headers=CSRF)
    assert res.status_code == 200
    body = res.json()
    assert body["otpauthUrl"].startswith("otpauth://totp/")
    return body["secret"]


def confirm(client: httpx.Client, secret: str) -> list[str]:
    res = client.post("/api/auth/totp/confirm", headers=CSRF,
                      json={"code": pyotp.TOTP(secret).now()})
    assert res.status_code == 200
    return res.json()["backupCodes"]


def test_should_enroll_pending_and_require_confirm_to_activate(ctx):
    secret = enroll(ctx.client)
    # Pending (unconfirmed) TOTP must not gate logins yet.
    fresh = httpx.Client(base_url=ctx.base_url)
    res = login(fresh, "admin", "Admin-pass-1234")
    assert res.status_code == 200
    assert "user" in res.json()
    fresh.close()
    assert ctx.client.get("/api/auth/me").json()["user"]["totpEnabled"] is False
    confirm(ctx.client, secret)
    assert ctx.client.get("/api/auth/me").json()["user"]["totpEnabled"] is True


def test_should_return_challenge_without_cookie_on_login_when_totp_enabled(ctx):
    secret = enroll(ctx.client)
    confirm(ctx.client, secret)
    fresh = httpx.Client(base_url=ctx.base_url)
    res = login(fresh, "admin", "Admin-pass-1234")
    assert res.status_code == 200
    assert res.json()["totpRequired"] is True
    assert res.json()["challenge"]
    assert COOKIE_NAME not in fresh.cookies
    fresh.close()


def test_should_complete_login_with_valid_totp_code(ctx):
    secret = enroll(ctx.client)
    confirm(ctx.client, secret)
    fresh = httpx.Client(base_url=ctx.base_url)
    challenge = login(fresh, "admin", "Admin-pass-1234").json()["challenge"]
    res = fresh.post("/api/auth/totp", headers=CSRF,
                     json={"challenge": challenge, "code": pyotp.TOTP(secret).now()})
    assert res.status_code == 200
    assert res.json()["user"]["username"] == "admin"
    assert COOKIE_NAME in fresh.cookies
    fresh.close()


def test_should_reject_wrong_totp_code(ctx):
    secret = enroll(ctx.client)
    confirm(ctx.client, secret)
    fresh = httpx.Client(base_url=ctx.base_url)
    challenge = login(fresh, "admin", "Admin-pass-1234").json()["challenge"]
    res = fresh.post("/api/auth/totp", headers=CSRF,
                     json={"challenge": challenge, "code": "000000"})
    assert res.status_code == 401
    assert COOKIE_NAME not in fresh.cookies
    fresh.close()


def test_should_invalidate_challenge_after_5_bad_codes(ctx):
    secret = enroll(ctx.client)
    confirm(ctx.client, secret)
    fresh = httpx.Client(base_url=ctx.base_url)
    challenge = login(fresh, "admin", "Admin-pass-1234").json()["challenge"]
    for _ in range(5):
        fresh.post("/api/auth/totp", headers=CSRF, json={"challenge": challenge, "code": "000000"})
    res = fresh.post("/api/auth/totp", headers=CSRF,
                     json={"challenge": challenge, "code": pyotp.TOTP(secret).now()})
    assert res.status_code == 401
    fresh.close()


def test_should_accept_backup_code_once(ctx):
    secret = enroll(ctx.client)
    backup_codes = confirm(ctx.client, secret)
    assert len(backup_codes) == 8
    fresh = httpx.Client(base_url=ctx.base_url)
    challenge = login(fresh, "admin", "Admin-pass-1234").json()["challenge"]
    res = fresh.post("/api/auth/totp", headers=CSRF,
                     json={"challenge": challenge, "code": backup_codes[0]})
    assert res.status_code == 200
    # The same backup code is consumed and must not work again.
    challenge = login(fresh, "admin", "Admin-pass-1234").json()["challenge"]
    res = fresh.post("/api/auth/totp", headers=CSRF,
                     json={"challenge": challenge, "code": backup_codes[0]})
    assert res.status_code == 401
    fresh.close()


def test_should_require_valid_code_to_disable_totp(ctx):
    secret = enroll(ctx.client)
    confirm(ctx.client, secret)
    bad = ctx.client.post("/api/auth/totp/disable", headers=CSRF, json={"code": "000000"})
    assert bad.status_code == 401
    good = ctx.client.post("/api/auth/totp/disable", headers=CSRF,
                           json={"code": pyotp.TOTP(secret).now()})
    assert good.status_code == 200
    assert ctx.client.get("/api/auth/me").json()["user"]["totpEnabled"] is False


def test_should_let_admin_reset_user_totp(ctx):
    ctx.store.create({"username": "viewer1", "password": "Viewer-pass-123", "role": "viewer"})
    ctx.store.set_password("viewer1", "Viewer-pass-123")
    viewer = httpx.Client(base_url=ctx.base_url)
    login(viewer, "viewer1", "Viewer-pass-123")
    secret = enroll(viewer)
    confirm(viewer, secret)
    res = ctx.client.delete("/api/users/viewer1/totp", headers=CSRF)
    assert res.status_code == 200
    # Login no longer asks for a second factor.
    res = login(viewer, "viewer1", "Viewer-pass-123")
    assert "user" in res.json()
    viewer.close()


def test_should_require_csrf_header_on_totp_verify(ctx):
    secret = enroll(ctx.client)
    confirm(ctx.client, secret)
    fresh = httpx.Client(base_url=ctx.base_url)
    challenge = login(fresh, "admin", "Admin-pass-1234").json()["challenge"]
    res = fresh.post("/api/auth/totp", json={"challenge": challenge, "code": "000000"})
    assert res.status_code == 403
    fresh.close()
