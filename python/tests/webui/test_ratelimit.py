"""Login rate limiting and account lockout."""

from datetime import datetime, timedelta, timezone
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


def test_should_lock_account_after_5_failures_with_423(ctx):
    for _ in range(5):
        assert login(ctx.client, "admin", "wrong-pass-1").status_code == 401
    res = login(ctx.client, "admin", "wrong-pass-1")
    assert res.status_code == 423
    assert res.json()["error"] == "account_locked"
    assert res.json()["retryAfterSec"] > 0


def test_should_refuse_even_the_correct_password_while_locked(ctx):
    for _ in range(5):
        login(ctx.client, "admin", "wrong-pass-1")
    assert login(ctx.client, "admin", "boot-pass-1").status_code == 423


def test_should_unlock_account_after_window_expires(ctx):
    for _ in range(5):
        login(ctx.client, "admin", "wrong-pass-1")
    assert login(ctx.client, "admin", "boot-pass-1").status_code == 423
    past = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    ctx.store._db.execute("UPDATE login_attempts SET ts = ?", (past,))
    assert login(ctx.client, "admin", "boot-pass-1").status_code == 200


def test_should_clear_failures_on_successful_login(ctx):
    for _ in range(4):
        login(ctx.client, "admin", "wrong-pass-1")
    assert login(ctx.client, "admin", "boot-pass-1").status_code == 200
    # The counter restarted: 4 fresh failures must not lock the account.
    for _ in range(4):
        login(ctx.client, "admin", "wrong-pass-1")
    assert login(ctx.client, "admin", "wrong-pass-1").status_code == 401


def test_should_throttle_ip_after_threshold_with_429(ctx, monkeypatch):
    monkeypatch.setattr(auth_module, "IP_LOCK_THRESHOLD", 3)
    for i in range(3):
        assert login(ctx.client, f"ghost{i}", "wrong-pass-1").status_code == 401
    res = login(ctx.client, "admin", "boot-pass-1")
    assert res.status_code == 429
    assert res.json()["error"] == "too_many_attempts"
    assert "retry-after" in {k.lower() for k in res.headers.keys()}


def test_should_let_admin_unlock_account(ctx):
    ctx.store.set_password("admin", "Admin-pass-1234")
    ctx.store.create({"username": "viewer1", "password": "Viewer-pass-123", "role": "viewer"})
    ctx.store.set_password("viewer1", "Viewer-pass-123")
    for _ in range(5):
        login(ctx.client, "viewer1", "wrong-pass-1")
    assert login(ctx.client, "viewer1", "Viewer-pass-123").status_code == 423
    assert login(ctx.client, "admin", "Admin-pass-1234").status_code == 200
    res = ctx.client.post("/api/users/viewer1/unlock", headers=CSRF)
    assert res.status_code == 200
    assert login(ctx.client, "viewer1", "Viewer-pass-123").status_code == 200


def test_should_require_admin_for_unlock(ctx):
    ctx.store.set_password("admin", "Admin-pass-1234")
    ctx.store.create({"username": "viewer1", "password": "Viewer-pass-123", "role": "viewer"})
    ctx.store.set_password("viewer1", "Viewer-pass-123")
    login(ctx.client, "viewer1", "Viewer-pass-123")
    res = ctx.client.post("/api/users/viewer1/unlock", headers=CSRF)
    assert res.status_code == 403
