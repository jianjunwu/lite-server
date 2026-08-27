"""Per-instance role grants (M2): store, guard enforcement, management API."""

import httpx
import pytest

import lite_server.webui.auth as auth_module
from lite_server.webui.app import build_app
from lite_server.webui.auth import UserStore
from lite_server.webui.config import InstanceStore


@pytest.fixture(autouse=True)
def _fast_bcrypt(monkeypatch):
    monkeypatch.setattr(auth_module, "BCRYPT_COST", 4)


CSRF = {"x-requested-with": "lite-ui"}


def login(client: httpx.Client, username: str, password: str) -> httpx.Response:
    return client.post("/api/auth/login", headers=CSRF,
                       json={"username": username, "password": password})


@pytest.fixture
def ctx(tmp_path, upstream, client_factory):
    store = UserStore(
        str(tmp_path / "auth.db"), {"LITE_UI_ADMIN_PASSWORD": "boot-pass-1"},
        legacy_auth_path=str(tmp_path / "auth.yaml"),
    )
    config_path = tmp_path / "instances.yaml"
    config_path.write_text(
        "instances:\n"
        f'  - {{ id: dev, name: Dev, base_url: "{upstream.base_url}" }}\n'
        f'  - {{ id: prod, name: Prod, base_url: "{upstream.base_url}" }}\n',
        encoding="utf-8",
    )
    registry = InstanceStore(str(config_path), {})
    app = build_app(registry, user_store=store, auth_enabled=True)
    store.set_password("admin", "admin-pass-1")
    store.create({"username": "viewer1", "password": "viewer-pass-1", "role": "viewer"})
    store.create({"username": "op1", "password": "op-pass-1234", "role": "operator"})
    store.set_password("viewer1", "viewer-pass-1")
    store.set_password("op1", "op-pass-1234")
    return {"app": app, "store": store, "client": client_factory(app)}


# ---- store level ----

def test_should_default_effective_role_to_global_role(ctx):
    store = ctx["store"]
    assert store.effective_role("op1", "operator", "prod") == "operator"


def test_should_override_effective_role_with_grant(ctx):
    store = ctx["store"]
    store.set_instance_grant("op1", "prod", "viewer")
    assert store.effective_role("op1", "operator", "prod") == "viewer"
    assert store.effective_role("op1", "operator", "dev") == "operator"


def test_should_list_and_remove_grants(ctx):
    store = ctx["store"]
    store.set_instance_grant("op1", "prod", "viewer")
    store.set_instance_grant("op1", "dev", "admin")
    assert store.list_instance_grants("op1") == [
        {"instance_id": "dev", "role": "admin"},
        {"instance_id": "prod", "role": "viewer"},
    ]
    store.remove_instance_grant("op1", "prod")
    assert store.list_instance_grants("op1") == [{"instance_id": "dev", "role": "admin"}]


def test_should_reject_unknown_grant_role(ctx):
    with pytest.raises(Exception):
        ctx["store"].set_instance_grant("op1", "prod", "superuser")


def test_should_cascade_grants_on_user_delete(ctx):
    store = ctx["store"]
    store.set_instance_grant("op1", "prod", "viewer")
    store.remove("op1", "admin")
    assert store.list_instance_grants("op1") == []


def test_should_cascade_grants_on_instance_removal(ctx):
    store = ctx["store"]
    store.set_instance_grant("op1", "prod", "viewer")
    store.remove_instance_ownership("prod")
    assert store.list_instance_grants("op1") == []


# ---- guard enforcement ----

def test_should_allow_mutation_when_grant_raises_role(ctx):
    ctx["store"].set_instance_grant("viewer1", "dev", "operator")
    client = ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    assert client.post("/api/i/dev/v2/models/m/reload", headers=CSRF).status_code == 200
    # Global role still applies where no grant exists.
    assert client.post("/api/i/prod/v2/models/m/reload", headers=CSRF).status_code == 403


def test_should_forbid_mutation_when_grant_lowers_role(ctx):
    ctx["store"].set_instance_grant("op1", "prod", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.post("/api/i/prod/v2/models/m/reload", headers=CSRF).status_code == 403
    assert client.get("/api/i/prod/v2/models").status_code == 200
    assert client.post("/api/i/dev/v2/models/m/reload", headers=CSRF).status_code == 200


def test_should_deny_all_access_when_grant_is_none(ctx):
    ctx["store"].set_instance_grant("op1", "prod", "none")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/prod/v2/models")
    assert res.status_code == 403
    assert res.json()["reason"] == "instance_denied"
    assert res.json()["instance"] == "prod"


def test_should_annotate_instance_list_with_effective_role(ctx):
    ctx["store"].set_instance_grant("op1", "prod", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/instances")
    roles = {i["id"]: i["effective_role"] for i in res.json()["instances"]}
    assert roles == {"dev": "operator", "prod": "viewer"}


def test_should_hide_none_instances_from_instance_list(ctx):
    ctx["store"].set_instance_grant("op1", "prod", "none")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/instances")
    assert [i["id"] for i in res.json()["instances"]] == ["dev"]


def test_should_keep_instance_management_global_for_admin_with_grant(ctx):
    """An admin demoted on one instance keeps global management rights."""
    ctx["store"].set_instance_grant("admin", "prod", "viewer")
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.put("/api/instances/prod", headers=CSRF, json={"name": "Prod 2"})
    assert res.status_code == 200
    # ...but on the proxied instance the grant applies.
    assert client.post("/api/i/prod/v2/models/m/reload", headers=CSRF).status_code == 403


def test_should_let_instance_admin_reclaim_ownerless_version(ctx):
    """ownership.py admin checks run on the effective role: an operator with
    an instance-level admin grant reclaims on that instance only."""
    ctx["store"].set_instance_grant("op1", "dev", "admin")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    # No ownership record for m/1: global operator is denied (reclaim rule),
    # instance admin passes through to the upstream.
    assert client.delete("/api/i/dev/v2/models/m/versions/1", headers=CSRF).status_code == 200
    assert client.delete("/api/i/prod/v2/models/m/versions/1", headers=CSRF).status_code == 403


# ---- management API ----

def test_should_manage_grants_via_api_as_admin(ctx):
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.put("/api/users/op1/grants/prod", headers=CSRF, json={"role": "viewer"})
    assert res.status_code == 200
    assert res.json()["grants"] == [{"instance_id": "prod", "role": "viewer"}]
    res = client.get("/api/users/op1/grants")
    assert res.json()["grants"] == [{"instance_id": "prod", "role": "viewer"}]
    # role=default removes the row, restoring the global role.
    res = client.put("/api/users/op1/grants/prod", headers=CSRF, json={"role": "default"})
    assert res.status_code == 200
    assert res.json()["grants"] == []


def test_should_forbid_grants_api_for_non_admin(ctx):
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.put("/api/users/op1/grants/prod", headers=CSRF,
                      json={"role": "viewer"}).status_code == 403
    assert client.get("/api/users/op1/grants").status_code == 403


def test_should_validate_grant_role_via_api(ctx):
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.put("/api/users/op1/grants/prod", headers=CSRF, json={"role": "superuser"})
    assert res.status_code == 400


def test_should_404_grants_for_unknown_user(ctx):
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    assert client.get("/api/users/ghost/grants").status_code == 404
    assert client.put("/api/users/ghost/grants/prod", headers=CSRF,
                      json={"role": "viewer"}).status_code == 404


def test_should_audit_grant_changes(ctx):
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    client.put("/api/users/op1/grants/prod", headers=CSRF, json={"role": "viewer"})
    entries = ctx["store"].list_audit(action="instance_grant")
    assert len(entries) == 1
    assert entries[0]["actor"] == "admin"
    assert entries[0]["target"] == "op1"
    assert entries[0]["detail"] == {"instance_id": "prod", "role": "viewer"}


# ---- M5: instance config read (GET /v2/server/config) ----

def test_should_let_viewer_read_server_config(ctx):
    # No model segment in the path, so the model whitelist never engages;
    # any role with instance access reads the effective server config.
    client = ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    res = client.get("/api/i/dev/v2/server/config")
    assert res.status_code == 200


def test_should_deny_server_config_when_grant_is_none(ctx):
    ctx["store"].set_instance_grant("viewer1", "dev", "none")
    client = ctx["client"]
    login(client, "viewer1", "viewer-pass-1")
    res = client.get("/api/i/dev/v2/server/config")
    assert res.status_code == 403
    assert res.json()["reason"] == "instance_denied"
