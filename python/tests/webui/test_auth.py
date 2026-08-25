"""Local-account auth: login, cookie sessions, RBAC, password policy."""

import sqlite3

import bcrypt
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


def make_setup(tmp_path, upstream, env=None, auth_enabled=True):
    db_path = tmp_path / "auth.db"
    store = UserStore(
        str(db_path), env or {},
        legacy_auth_path=str(tmp_path / "auth.yaml"),
    )
    config_path = tmp_path / "instances.yaml"
    config_path.write_text(
        f'instances:\n  - {{ id: plain, name: P, base_url: "{upstream.base_url}" }}\n',
        encoding="utf-8",
    )
    registry = InstanceStore(str(config_path), {})
    app = build_app(registry, user_store=store, auth_enabled=auth_enabled)
    return {"app": app, "store": store, "db_path": str(db_path)}


CSRF = {"x-requested-with": "lite-ui"}


def login(client: httpx.Client, username: str, password: str) -> httpx.Response:
    return client.post("/api/auth/login", headers=CSRF,
                       json={"username": username, "password": password})


@pytest.fixture
def ctx(tmp_path, upstream, client_factory):
    ctx = make_setup(tmp_path, upstream, env={"LITE_UI_ADMIN_PASSWORD": "boot-pass-1"})
    ctx["client"] = client_factory(ctx["app"])
    return ctx


def test_should_bootstrap_admin_from_env_password_and_persist(ctx):
    admin = ctx["store"].verify("admin", "boot-pass-1")
    assert admin is not None
    assert admin.role == "admin"
    assert admin.must_change_password is True
    conn = sqlite3.connect(ctx["db_path"])
    assert conn.execute("SELECT username FROM users WHERE username = 'admin'").fetchone()
    conn.close()


def test_should_login_and_set_httponly_cookie(ctx):
    res = login(ctx["client"], "admin", "boot-pass-1")
    assert res.status_code == 200
    assert "lite_ui_token=" in res.headers["set-cookie"]
    assert "httponly" in res.headers["set-cookie"].lower()
    assert res.json()["user"]["username"] == "admin"


def test_should_reject_wrong_password_with_401(ctx):
    res = login(ctx["client"], "admin", "nope")
    assert res.status_code == 401
    assert "lite_ui_token" not in res.cookies


def test_should_return_401_for_api_without_cookie(ctx):
    res = ctx["client"].get("/api/instances")
    assert res.status_code == 401
    assert res.json()["error"] == "bff_unauthenticated"


@pytest.fixture
def rbac_ctx(ctx):
    store = ctx["store"]
    store.set_password("admin", "admin-pass-1")  # clear mustChangePassword
    store.create({"username": "viewer1", "password": "viewer-pass-1", "role": "viewer"})
    store.create({"username": "op1", "password": "op-pass-1234", "role": "operator"})
    # create() sets mustChangePassword; clear it so these users can act.
    store.set_password("viewer1", "viewer-pass-1")
    store.set_password("op1", "op-pass-1234")
    return ctx


def test_should_allow_viewer_get_but_forbid_proxy_mutation(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    get = client.get("/api/i/plain/v2/models")
    assert get.status_code == 200
    post = client.post("/api/i/plain/v2/models/m/reload", headers=CSRF)
    assert post.status_code == 403
    assert post.json()["error"] == "forbidden"


def test_should_allow_viewer_inference_post_but_not_admin_post(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    infer = client.post("/api/i/plain/v2/models/m/infer", headers=CSRF, json={"input": 1})
    assert infer.status_code == 200
    events = client.post("/api/i/plain/v2/models/m/events", headers=CSRF, json={"input": 1})
    assert events.status_code == 200
    reload = client.post("/api/i/plain/v2/models/m/reload", headers=CSRF)
    assert reload.status_code == 403


def test_should_allow_viewer_inference_post_with_query_string(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    res = client.post("/api/i/plain/v2/models/m/infer?stream=true", headers=CSRF, json={"input": 1})
    assert res.status_code == 200


def test_should_not_let_viewer_smuggle_infer_exemption_via_query_string(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    unload = client.post("/api/i/plain/v2/models/m/unload?next=/infer", headers=CSRF)
    assert unload.status_code == 403
    routing = client.put("/api/i/plain/v2/models/m/routing?a=/events", headers=CSRF,
                         json={"weights": {}})
    assert routing.status_code == 403


def test_should_allow_viewer_repository_index_post(rbac_ctx):
    """POST /v2/repository/index is a read-only repo scan — viewers allowed."""
    client = rbac_ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    index = client.post("/api/i/plain/v2/repository/index", headers=CSRF)
    assert index.status_code == 200


def test_should_not_let_viewer_broaden_repository_index_exemption(rbac_ctx):
    """Only the exact index path is exempt; other repository POSTs stay 403."""
    client = rbac_ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    load = client.post("/api/i/plain/v2/repository/models/m/load", headers=CSRF)
    assert load.status_code == 403
    drift = client.post("/api/i/plain/v2/repository/indexx", headers=CSRF)
    assert drift.status_code == 403


def test_should_allow_operator_proxy_mutation_but_forbid_instance_write(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "op1", "op-pass-1234")
    post = client.post("/api/i/plain/v2/models/m/reload", headers=CSRF)
    assert post.status_code == 200
    put = client.put("/api/instances/plain", headers=CSRF, json={"name": "x"})
    assert put.status_code == 403


def test_should_allow_admin_everything(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.post("/api/users", headers=CSRF,
                      json={"username": "u2", "password": "u2-pass-1234", "role": "viewer"})
    assert res.status_code == 201


def test_should_reject_mutation_without_csrf_header(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.post("/api/i/plain/v2/models/m/reload")
    assert res.status_code == 403
    assert res.json()["error"] == "csrf_header_missing"


def test_should_change_password_and_clear_must_change_flag(ctx):
    client = ctx["client"]
    login(client, "admin", "boot-pass-1")
    res = client.post("/api/auth/change-password", headers=CSRF,
                      json={"currentPassword": "boot-pass-1", "newPassword": "new-pass-123"})
    assert res.status_code == 200
    me = client.get("/api/auth/me")
    assert me.json()["user"]["mustChangePassword"] is False


def test_should_block_api_until_password_changed_when_flag_set(ctx):
    client = ctx["client"]
    login(client, "admin", "boot-pass-1")
    res = client.get("/api/instances")
    assert res.status_code == 403
    assert res.json()["error"] == "password_change_required"


def test_should_forbid_deleting_self_and_last_admin(ctx):
    ctx["store"].set_password("admin", "admin-pass-1")
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.delete("/api/users/admin", headers=CSRF)
    assert res.status_code == 403  # cannot delete yourself / last admin


def test_should_reject_duplicate_username_with_409(ctx):
    ctx["store"].set_password("admin", "admin-pass-1")
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.post("/api/users", headers=CSRF,
                      json={"username": "admin", "password": "whatever-123", "role": "viewer"})
    assert res.status_code == 409


def test_should_require_csrf_header_on_login(ctx):
    res = ctx["client"].post("/api/auth/login",
                             json={"username": "admin", "password": "boot-pass-1"})
    assert res.status_code == 403
    assert res.json()["error"] == "csrf_header_missing"


def test_should_enforce_role_change_within_token_lifetime(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "op1", "op-pass-1234")
    # Admin demotes op1 after the token was issued; the old token must not
    # keep operator rights for the rest of its lifetime.
    rbac_ctx["store"].update("op1", {"role": "viewer"}, actor="admin")
    res = client.post("/api/i/plain/v2/models/m/reload", headers=CSRF)
    assert res.status_code == 403


def test_should_reject_requests_from_deleted_user(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    rbac_ctx["store"].remove("viewer1", actor="admin")
    res = client.get("/api/instances")
    assert res.status_code == 401


def test_should_reject_overlong_password_with_400_not_500(ctx):
    ctx["store"].set_password("admin", "admin-pass-1")
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.post("/api/users", headers=CSRF,
                      json={"username": "longpw", "password": "p" * 100, "role": "viewer"})
    assert res.status_code == 400


def test_should_return_401_not_500_when_login_password_exceeds_bcrypt_limit(ctx):
    # bcrypt rejects passwords over 72 bytes with ValueError; the login
    # endpoint must turn that into a plain invalid-credentials 401.
    res = login(ctx["client"], "admin", "x" * 100)
    assert res.status_code == 401


def test_should_pass_everything_through_with_synthetic_admin(tmp_path, upstream, client_factory):
    ctx = make_setup(tmp_path, upstream, env={}, auth_enabled=False)
    client = client_factory(ctx["app"])
    res = client.get("/api/instances")
    assert res.status_code == 200


def test_should_return_synthetic_user_on_me_when_auth_off(tmp_path, upstream, client_factory):
    ctx = make_setup(tmp_path, upstream, env={}, auth_enabled=False)
    client = client_factory(ctx["app"])
    res = client.get("/api/auth/me")
    assert res.status_code == 200
    user = res.json()["user"]
    assert user["username"] == "local"
    assert user["role"] == "admin"
    assert user["mustChangePassword"] is False


def test_should_list_empty_sessions_when_auth_off(tmp_path, upstream, client_factory):
    ctx = make_setup(tmp_path, upstream, env={}, auth_enabled=False)
    client = client_factory(ctx["app"])
    res = client.get("/api/auth/sessions")
    assert res.status_code == 200
    assert res.json() == {"sessions": []}


def test_should_logout_cleanly_when_auth_off(tmp_path, upstream, client_factory):
    ctx = make_setup(tmp_path, upstream, env={}, auth_enabled=False)
    client = client_factory(ctx["app"])
    res = client.post("/api/auth/logout", headers=CSRF)
    assert res.status_code == 200
    assert res.json() == {"ok": True}


def test_should_upgrade_cost10_hash_transparently_on_login(ctx, monkeypatch):
    # A legacy cost-10 hash keeps working and is rehashed at the current cost
    # on the next successful verification.
    monkeypatch.setattr(auth_module, "BCRYPT_COST", 12)
    old_hash = bcrypt.hashpw(b"legacy-pass-1", bcrypt.gensalt(10)).decode()
    ctx["store"]._db.execute(
        "INSERT INTO users (username, password_hash, role, created_at, must_change_password)"
        " VALUES (?, ?, ?, ?, 0)",
        ("legacy", old_hash, "viewer", "2026-01-01T00:00:00+00:00"),
    )
    assert ctx["store"].verify("legacy", "legacy-pass-1") is not None
    upgraded = ctx["store"].get("legacy").password_hash
    assert upgraded.startswith("$2b$12$")


def test_should_equalize_timing_with_dummy_hash_for_unknown_user(ctx, monkeypatch):
    # Verifying an unknown username must still run bcrypt so account existence
    # cannot be probed by response latency.
    calls = []
    real_checkpw = bcrypt.checkpw

    def spy(pw, hashed):
        calls.append(hashed)
        return real_checkpw(pw, hashed)

    monkeypatch.setattr(auth_module.bcrypt, "checkpw", spy)
    assert ctx["store"].verify("ghost", "whatever-pass-1") is None
    assert len(calls) == 1


def test_should_reject_weak_passwords_on_create(ctx):
    ctx["store"].set_password("admin", "admin-pass-1")
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    too_short = client.post("/api/users", headers=CSRF, json={
        "username": "shorty", "password": "Aa1!aaaaaaa", "role": "viewer"})  # 11 chars
    assert too_short.status_code == 400
    too_simple = client.post("/api/users", headers=CSRF, json={
        "username": "simple", "password": "alllowercase1", "role": "viewer"})  # 2 char classes
    assert too_simple.status_code == 400


def test_should_reject_weak_passwords_on_change(ctx):
    client = ctx["client"]
    login(client, "admin", "boot-pass-1")
    res = client.post("/api/auth/change-password", headers=CSRF, json={
        "currentPassword": "boot-pass-1", "newPassword": "alllowercase1"})
    assert res.status_code == 400


def test_should_accept_existing_weak_password_user_login(ctx):
    # The policy applies to new passwords only; imported/legacy accounts with
    # weak passwords must still be able to log in.
    weak_hash = bcrypt.hashpw(b"weak", bcrypt.gensalt(4)).decode()
    ctx["store"]._db.execute(
        "INSERT INTO users (username, password_hash, role, created_at, must_change_password)"
        " VALUES (?, ?, ?, ?, 0)",
        ("legacy", weak_hash, "viewer", "2026-01-01T00:00:00+00:00"),
    )
    res = login(ctx["client"], "legacy", "weak")
    assert res.status_code == 200


def test_should_reject_new_password_equal_to_old(ctx):
    client = ctx["client"]
    login(client, "admin", "boot-pass-1")
    res = client.post("/api/auth/change-password", headers=CSRF, json={
        "currentPassword": "boot-pass-1", "newPassword": "boot-pass-1"})
    assert res.status_code == 400
