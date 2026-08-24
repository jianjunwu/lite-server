"""Security audit trail: SQLite-backed events, admin API, optional file sink."""

import logging
from types import SimpleNamespace

import httpx
import pytest

import lite_server.webui.auth as auth_module
from lite_server.webui.app import build_app
from lite_server.webui.auth import UserStore
from lite_server.webui.config import InstanceStore
from lite_server.webui.server import configure_audit_logging


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
    yield SimpleNamespace(store=store, client=client, base_url=server.base_url)
    client.close()


def audit_rows(store, action=None):
    if action is None:
        return store._db.query("SELECT * FROM audit ORDER BY id")
    return store._db.query("SELECT * FROM audit WHERE action = ? ORDER BY id", (action,))


def test_should_record_login_success_and_failure_with_ip(ctx):
    login(ctx.client, "admin", "wrong-pass-1")
    login(ctx.client, "admin", "Admin-pass-1234")
    failure = audit_rows(ctx.store, "login_failure")
    assert len(failure) == 1
    assert failure[0]["target"] == "admin"
    assert failure[0]["ip"] == "127.0.0.1"
    success = audit_rows(ctx.store, "login_success")
    assert len(success) == 1
    assert success[0]["actor"] == "admin"


def test_should_record_lockout_once(ctx):
    for _ in range(6):
        login(ctx.client, "admin", "wrong-pass-1")
    assert len(audit_rows(ctx.store, "account_locked")) == 1


def test_should_record_user_crud_password_change_and_session_revoke(ctx):
    login(ctx.client, "admin", "Admin-pass-1234")
    ctx.client.post("/api/users", headers=CSRF,
                    json={"username": "u1", "password": "User-pass-1234", "role": "viewer"})
    ctx.client.post("/api/auth/change-password", headers=CSRF,
                    json={"currentPassword": "Admin-pass-1234", "newPassword": "Admin-pass-5678"})
    ctx.client.post("/api/auth/logout", headers=CSRF)
    actions = [r["action"] for r in audit_rows(ctx.store)]
    assert "user_created" in actions
    assert "password_changed" in actions
    assert "session_revoked" in actions


def test_should_require_admin_for_audit_api(ctx):
    ctx.store.create({"username": "viewer1", "password": "Viewer-pass-123", "role": "viewer"})
    ctx.store.set_password("viewer1", "Viewer-pass-123")
    login(ctx.client, "viewer1", "Viewer-pass-123")
    res = ctx.client.get("/api/audit")
    assert res.status_code == 403


def test_should_serve_audit_entries_to_admin(ctx):
    login(ctx.client, "admin", "Admin-pass-1234")
    res = ctx.client.get("/api/audit")
    assert res.status_code == 200
    entries = res.json()["entries"]
    assert any(e["action"] == "login_success" for e in entries)


@pytest.fixture
def audit_logger_cleanup():
    yield
    logger = logging.getLogger("lite_server.webui.audit")
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def test_should_attach_rotating_file_handler_when_env_set(tmp_path, audit_logger_cleanup):
    log_path = tmp_path / "audit.log"
    configure_audit_logging({"LITE_UI_AUDIT_LOG": str(log_path)})
    logging.getLogger("lite_server.webui.audit").info("test event")
    assert "test event" in log_path.read_text(encoding="utf-8")


def test_should_not_attach_handler_without_env(audit_logger_cleanup):
    logger = logging.getLogger("lite_server.webui.audit")
    before = len(logger.handlers)
    configure_audit_logging({})
    assert len(logger.handlers) == before
