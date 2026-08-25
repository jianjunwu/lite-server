"""Password hashing must not run on the event loop: bcrypt at cost 12 is
~250ms of CPU, and on the loop thread it stalls every concurrent request
(SSE inference streams, file transfers, other users' API calls)."""

import asyncio
from types import SimpleNamespace

import pytest

import lite_server.webui.auth as auth_module
from lite_server.webui.app import build_app
from lite_server.webui.auth import UserStore
from lite_server.webui.config import InstanceStore

CSRF = {"x-requested-with": "lite-ui"}


@pytest.fixture(autouse=True)
def _fast_bcrypt(monkeypatch):
    """bcrypt cost 12 makes every hash ~250ms; tests use the minimum cost."""
    monkeypatch.setattr(auth_module, "BCRYPT_COST", 4)


@pytest.fixture
def hash_spy(monkeypatch):
    """Records where each _hash call runs: on the event-loop thread a running
    loop is visible; on an anyio worker thread there is none."""
    calls = []
    real_hash = auth_module._hash

    def spy(password):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            calls.append("worker-thread")
        else:
            calls.append("event-loop")
        return real_hash(password)

    monkeypatch.setattr(auth_module, "_hash", spy)
    return calls


def _make_app(tmp_path, upstream, env):
    store = UserStore(
        str(tmp_path / "auth.db"), env,
        legacy_auth_path=str(tmp_path / "auth.yaml"),
    )
    config_path = tmp_path / "instances.yaml"
    config_path.write_text(
        f'instances:\n  - {{ id: plain, name: P, base_url: "{upstream.base_url}" }}\n',
        encoding="utf-8",
    )
    registry = InstanceStore(str(config_path), {})
    return build_app(registry, user_store=store, auth_enabled=True), store


@pytest.fixture
def open_ctx(tmp_path, upstream, client_factory):
    # Empty store: open registration, first user becomes admin.
    app, store = _make_app(tmp_path, upstream, env={})
    return SimpleNamespace(store=store, client=client_factory(app))


@pytest.fixture
def admin_ctx(tmp_path, upstream, client_factory):
    app, store = _make_app(tmp_path, upstream, env={"LITE_UI_ADMIN_PASSWORD": "boot-pass-1"})
    store.set_password("admin", "admin-pass-1")  # clears must_change_password
    client = client_factory(app)
    client.post("/api/auth/login", headers=CSRF,
                json={"username": "admin", "password": "admin-pass-1"})
    return SimpleNamespace(store=store, client=client)


def test_should_hash_off_loop_on_register(open_ctx, hash_spy):
    hash_spy.clear()
    res = open_ctx.client.post("/api/auth/register", headers=CSRF,
                               json={"username": "founder", "password": "Founder-pass-1"})
    assert res.status_code == 201
    assert hash_spy and set(hash_spy) == {"worker-thread"}


def test_should_hash_off_loop_on_change_password(admin_ctx, hash_spy):
    hash_spy.clear()
    res = admin_ctx.client.post("/api/auth/change-password", headers=CSRF,
                                json={"currentPassword": "admin-pass-1",
                                      "newPassword": "new-pass-123"})
    assert res.status_code == 200
    assert hash_spy and set(hash_spy) == {"worker-thread"}


def test_should_hash_off_loop_on_create_user(admin_ctx, hash_spy):
    hash_spy.clear()
    res = admin_ctx.client.post("/api/users", headers=CSRF,
                                json={"username": "u2", "password": "u2-pass-1234",
                                      "role": "viewer"})
    assert res.status_code == 201
    assert hash_spy and set(hash_spy) == {"worker-thread"}


def test_should_hash_off_loop_on_update_user_password(admin_ctx, hash_spy):
    admin_ctx.store.create(
        {"username": "u2", "password": "u2-pass-1234", "role": "viewer"})
    hash_spy.clear()
    res = admin_ctx.client.put("/api/users/u2", headers=CSRF,
                               json={"password": "u2-new-pass-1"})
    assert res.status_code == 200
    assert hash_spy and set(hash_spy) == {"worker-thread"}
