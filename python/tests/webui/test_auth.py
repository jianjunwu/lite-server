"""Port of ui/server/test/auth.test.ts."""

import httpx
import pytest

from lite_server.webui.app import build_app
from lite_server.webui.auth import UserStore
from lite_server.webui.config import InstanceStore


def make_setup(tmp_path, upstream, env=None, auth_enabled=True):
    auth_path = tmp_path / "auth.yaml"
    secret_path = tmp_path / "auth.secret"
    store = UserStore(str(auth_path), str(secret_path), env or {})
    config_path = tmp_path / "instances.yaml"
    config_path.write_text(
        f'instances:\n  - {{ id: plain, name: P, base_url: "{upstream.base_url}" }}\n',
        encoding="utf-8",
    )
    registry = InstanceStore(str(config_path), {})
    app = build_app(registry, user_store=store, auth_enabled=auth_enabled)
    return {"app": app, "store": store, "auth_path": str(auth_path)}


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
    assert "admin" in open(ctx["auth_path"], encoding="utf-8").read()


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
    assert res.json()["error"] == "unauthenticated"


@pytest.fixture
def rbac_ctx(ctx):
    store = ctx["store"]
    store.set_password("admin", "admin-pass-1")  # clear mustChangePassword
    store.create({"username": "viewer1", "password": "viewer-pass-1", "role": "viewer"})
    store.create({"username": "op1", "password": "op-pass-123", "role": "operator"})
    # create() sets mustChangePassword; clear it so these users can act.
    store.set_password("viewer1", "viewer-pass-1")
    store.set_password("op1", "op-pass-123")
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


def test_should_allow_operator_proxy_mutation_but_forbid_instance_write(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "op1", "op-pass-123")
    post = client.post("/api/i/plain/v2/models/m/reload", headers=CSRF)
    assert post.status_code == 200
    put = client.put("/api/instances/plain", headers=CSRF, json={"name": "x"})
    assert put.status_code == 403


def test_should_allow_admin_everything(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.post("/api/users", headers=CSRF,
                      json={"username": "u2", "password": "u2-pass-123", "role": "viewer"})
    assert res.status_code == 201


def test_should_reject_mutation_without_csrf_header(rbac_ctx):
    client = rbac_ctx["client"]
    login(client, "op1", "op-pass-123")
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
    login(client, "op1", "op-pass-123")
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
