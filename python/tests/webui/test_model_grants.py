"""Per-model ACL (M3): whitelist grants, proxy enforcement, read filtering."""

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
    store.create({"username": "op1", "password": "op-pass-1234", "role": "operator"})
    store.set_password("op1", "op-pass-1234")
    return {"app": app, "store": store, "client": client_factory(app)}


# ---- store level ----

def test_should_report_whitelist_inactive_without_rows(ctx):
    store = ctx["store"]
    assert store.has_model_grants("op1", "dev") is False
    assert store.model_grant("op1", "dev", "alpha") is None


def test_should_set_list_and_remove_model_grants(ctx):
    store = ctx["store"]
    store.set_model_grant("op1", "dev", "alpha", "operator")
    store.set_model_grant("op1", "dev", "beta", "viewer")
    store.set_model_grant("op1", "prod", "alpha", "operator")
    assert store.has_model_grants("op1", "dev") is True
    assert store.model_grant("op1", "dev", "beta") == "viewer"
    assert store.list_model_grants("op1", "dev") == [
        {"instance_id": "dev", "model": "alpha", "role": "operator"},
        {"instance_id": "dev", "model": "beta", "role": "viewer"},
    ]
    assert store.list_model_grant_users("dev", "alpha") == [
        {"username": "op1", "role": "operator"},
    ]
    store.remove_model_grant("op1", "dev", "alpha")
    assert store.list_model_grants("op1", "dev") == [
        {"instance_id": "dev", "model": "beta", "role": "viewer"},
    ]


def test_should_reject_admin_as_model_grant_role(ctx):
    with pytest.raises(Exception):
        ctx["store"].set_model_grant("op1", "dev", "alpha", "admin")


def test_should_cascade_model_grants_on_user_and_instance_delete(ctx):
    store = ctx["store"]
    store.set_model_grant("op1", "dev", "alpha", "operator")
    store.remove_instance_ownership("dev")
    assert store.list_model_grants("op1") == []
    store.set_model_grant("op1", "prod", "alpha", "operator")
    store.remove("op1", "admin")
    assert store.list_model_grants("op1") == []


# ---- proxy enforcement ----

def test_should_not_restrict_mutations_when_whitelist_inactive(ctx):
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.post("/api/i/dev/v2/models/alpha/reload", headers=CSRF).status_code == 200


def test_should_deny_mutation_on_ungranted_model_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "operator")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.post("/api/i/dev/v2/models/alpha/reload", headers=CSRF).status_code == 200
    res = client.post("/api/i/dev/v2/models/beta/reload", headers=CSRF)
    assert res.status_code == 403
    assert res.json()["reason"] == "model_denied"
    assert res.json()["model"] == "beta"
    # The whitelist is per instance: prod has no rows, so it is unrestricted.
    assert client.post("/api/i/prod/v2/models/beta/reload", headers=CSRF).status_code == 200


def test_should_deny_new_model_upload_when_whitelist_active(ctx):
    """A whitelist that allowed fresh uploads could be bypassed by picking a
    new model name — model creation needs an unrestricted operator."""
    ctx["store"].set_model_grant("op1", "dev", "alpha", "operator")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.post("/api/i/dev/v2/repository/models/fresh/upload", headers=CSRF)
    assert res.status_code == 403
    assert res.json()["reason"] == "model_denied"


def test_should_treat_viewer_grant_as_read_only(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.get("/api/i/dev/v2/models/alpha/versions").status_code == 200
    res = client.post("/api/i/dev/v2/models/alpha/reload", headers=CSRF)
    assert res.status_code == 403


def test_should_gate_inference_like_a_read(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.post("/api/i/dev/v2/models/alpha/infer", headers=CSRF,
                       json={"input": 1}).status_code == 200
    assert client.post("/api/i/dev/v2/models/beta/infer", headers=CSRF,
                       json={"input": 1}).status_code == 403


def test_should_bypass_model_acl_for_instance_admin(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "operator")
    ctx["store"].set_instance_grant("op1", "dev", "admin")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.post("/api/i/dev/v2/models/beta/reload", headers=CSRF).status_code == 200


def test_should_bypass_model_acl_for_global_admin(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "operator")
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    assert client.post("/api/i/dev/v2/models/beta/reload", headers=CSRF).status_code == 200


# ---- read-side filtering ----

def test_should_filter_model_list_response_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/v2/models")
    assert res.status_code == 200
    assert [m["name"] for m in res.json()["models"]] == ["alpha"]


def test_should_filter_repository_index_response_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.post("/api/i/dev/v2/repository/index", headers=CSRF)
    assert res.status_code == 200
    assert {m["name"] for m in res.json()["models"]} == {"alpha"}


def test_should_not_filter_lists_when_whitelist_inactive(ctx):
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/v2/models")
    assert {m["name"] for m in res.json()["models"]} == {"alpha", "beta"}


def test_should_deny_single_model_get_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/v2/models/beta/ready")
    assert res.status_code == 403
    assert res.json()["reason"] == "model_denied"


def test_should_gate_version_config_read_like_other_model_reads(ctx):
    # M1: GET /v2/models/{m}/versions/{v}/config is a model-scoped read —
    # the whitelist decides by the model segment, same as /ready.
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/v2/models/beta/versions/1/config")
    assert res.status_code == 403
    assert res.json()["reason"] == "model_denied"


# ---- management API ----

def test_should_manage_model_grants_via_api_as_admin(ctx):
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.put("/api/users/op1/model-grants/dev/alpha", headers=CSRF,
                     json={"role": "operator"})
    assert res.status_code == 200
    assert res.json()["grants"] == [{"instance_id": "dev", "model": "alpha", "role": "operator"}]
    res = client.get("/api/users/op1/model-grants")
    assert res.json()["grants"] == [{"instance_id": "dev", "model": "alpha", "role": "operator"}]
    res = client.get("/api/model-grants", params={"instance_id": "dev", "model": "alpha"})
    assert res.json()["grants"] == [{"username": "op1", "role": "operator"}]
    res = client.put("/api/users/op1/model-grants/dev/alpha", headers=CSRF,
                     json={"role": "default"})
    assert res.json()["grants"] == []


def test_should_forbid_model_grants_api_for_non_admin(ctx):
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.get("/api/users/op1/model-grants").status_code == 403
    assert client.put("/api/users/op1/model-grants/dev/alpha", headers=CSRF,
                      json={"role": "viewer"}).status_code == 403
    assert client.get("/api/model-grants",
                      params={"instance_id": "dev", "model": "alpha"}).status_code == 403


def test_should_validate_model_grant_role_via_api(ctx):
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.put("/api/users/op1/model-grants/dev/alpha", headers=CSRF,
                     json={"role": "admin"})
    assert res.status_code == 400


def test_should_audit_model_grant_changes(ctx):
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    client.put("/api/users/op1/model-grants/dev/alpha", headers=CSRF, json={"role": "viewer"})
    entries = ctx["store"].list_audit(action="model_grant")
    assert len(entries) == 1
    assert entries[0]["actor"] == "admin"
    assert entries[0]["target"] == "op1"
    assert entries[0]["detail"] == {"instance_id": "dev", "model": "alpha", "role": "viewer"}


# ---- observability endpoints: same isolation as the model lists ----

def test_should_deny_single_model_timeline_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    assert client.get("/api/i/dev/metrics/timeline/alpha").status_code == 200
    res = client.get("/api/i/dev/metrics/timeline/beta")
    assert res.status_code == 403
    assert res.json()["reason"] == "model_denied"


def test_should_filter_timeline_all_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/metrics/timeline")
    assert [s["model"] for s in res.json()["snapshots"]] == ["alpha"]


def test_should_filter_alerts_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/metrics/alerts")
    assert [a["model"] for a in res.json()["alerts"]] == ["alpha"]


def test_should_filter_health_models_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/health")
    body = res.json()
    assert body["status"] == "ready"  # non-model fields survive
    assert [m["name"] for m in body["models"]] == ["alpha"]


def test_should_filter_info_loaded_models_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/info")
    body = res.json()
    assert body["version"] == "0.1.0"
    assert body["loaded_models"] == ["alpha"]


def test_should_filter_drift_entries_when_whitelist_active(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/v2/repository/drift")
    body = res.json()
    assert [e["model"] for e in body["configured_missing"]] == ["alpha"]
    assert [e["model"] for e in body["on_disk_unconfigured"]] == ["alpha"]


def test_should_not_filter_observability_when_whitelist_inactive(ctx):
    client = ctx["client"]
    login(client, "op1", "op-pass-1234")
    res = client.get("/api/i/dev/metrics/timeline")
    assert {s["model"] for s in res.json()["snapshots"]} == {"alpha", "beta"}
    res = client.get("/api/i/dev/info")
    assert res.json()["loaded_models"] == ["alpha", "beta"]


def test_should_not_filter_observability_for_admin(ctx):
    ctx["store"].set_model_grant("op1", "dev", "alpha", "viewer")
    client = ctx["client"]
    login(client, "admin", "admin-pass-1")
    res = client.get("/api/i/dev/metrics/timeline")
    assert {s["model"] for s in res.json()["snapshots"]} == {"alpha", "beta"}
