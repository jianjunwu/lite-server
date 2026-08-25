"""Ownership layer (M2): version/upload-session ownership recorded at the
proxy, ownership-scoped authorization for overwrite/delete, x-lite-user
identity forwarding."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

from lite_server.webui.app import build_app
from lite_server.webui.auth import UserStore
from lite_server.webui.authdb import AuthDB
from lite_server.webui.config import InstanceConfig
from lite_server.webui.ownership import parse_model_mutation
from lite_server.webui.registry import InstanceStore

from .conftest import MapRegistry

CSRF = {"x-requested-with": "lite-ui"}


def login(client: httpx.Client, username: str, password: str) -> None:
    res = client.post("/api/auth/login", headers=CSRF,
                      json={"username": username, "password": password})
    assert res.status_code == 200, res.text


@pytest.fixture
def ctx(tmp_path, upstream, live):
    store = UserStore(
        str(tmp_path / "auth.db"), {"LITE_UI_ADMIN_PASSWORD": "boot-pass-1"},
        legacy_auth_path=str(tmp_path / "auth.yaml"),
    )
    store.set_password("admin", "Admin-pass-1234")
    store.create({"username": "op1", "password": "Operator-pass-1", "role": "operator"})
    store.create({"username": "op2", "password": "Operator-pass-2", "role": "operator"})
    # create() marks users must_change_password; a password set clears it.
    store.set_password("op1", "Operator-pass-1")
    store.set_password("op2", "Operator-pass-2")
    config_path = tmp_path / "instances.yaml"
    config_path.write_text(
        f'instances:\n  - {{ id: plain, name: P, base_url: "{upstream.base_url}" }}\n',
        encoding="utf-8",
    )
    registry = InstanceStore(str(config_path), {})
    server = live(build_app(registry, user_store=store, auth_enabled=True))
    yield SimpleNamespace(store=store, base_url=server.base_url)


def client_for(ctx, username: str, password: str) -> httpx.Client:
    client = httpx.Client(base_url=ctx.base_url)
    login(client, username, password)
    return client


# ===== schema v6 =====

def test_v6_migration_creates_ownership_tables(tmp_path):
    db = AuthDB(str(tmp_path / "auth.db"))
    db.query("SELECT * FROM version_owners")
    db.query("SELECT * FROM upload_session_owners")
    db.close()
    # Reopening the same database must be a no-op (idempotent migrations).
    db = AuthDB(str(tmp_path / "auth.db"))
    db.query("SELECT * FROM version_owners")
    db.close()


# ===== tail parser =====

@pytest.mark.parametrize("tail,kind,model,version,sid", [
    ("v2/repository/models/m/versions/1/upload", "upload", "m", "1", None),
    ("v2/repository/models/m/upload", "model_upload", "m", None, None),
    ("v2/repository/models/m/versions/1/upload-sessions", "init", "m", "1", None),
    ("v2/repository/models/m/versions/1/upload-sessions/abc/complete", "complete", "m", "1", "abc"),
    ("v2/repository/models/m/versions/1/upload-sessions/abc/files/0/chunks/3", "chunk", "m", "1", "abc"),
    ("v2/repository/models/m/versions/1/upload-sessions/abc", "session", "m", "1", "abc"),
    ("v2/models/m/versions/1", "delete", "m", "1", None),
    ("v2/models/m/versions", "batch_delete", "m", None, None),
])
def test_parse_model_mutation_routes(tail, kind, model, version, sid):
    m = parse_model_mutation(tail)
    assert m is not None, tail
    assert (m.kind, m.model, m.version, m.session_id) == (kind, model, version, sid)


@pytest.mark.parametrize("tail", [
    "v2/models",
    "v2/models/m/infer",
    "v2/repository/index",
    "v2/models/m/versions/1/activate",
    "metrics/timeline",
])
def test_parse_model_mutation_ignores_non_mutations(tail):
    assert parse_model_mutation(tail) is None


# ===== session ownership =====

def test_init_records_session_owner(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    res = op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload-sessions",
                   headers=CSRF, json={"files": [{"name": "w.bin", "size": 64}]})
    assert res.status_code == 201
    row = ctx.store.session_owner("plain", "stub-session-id")
    assert row is not None
    assert (row["owner"], row["model"], row["version"]) == ("op1", "m", "1")


def test_session_ops_require_owner(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    op2 = client_for(ctx, "op2", "Operator-pass-2")
    admin = client_for(ctx, "admin", "Admin-pass-1234")
    op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload-sessions",
             headers=CSRF, json={"files": [{"name": "w.bin", "size": 64}]})

    chunk = "/api/i/plain/v2/repository/models/m/versions/1/upload-sessions/stub-session-id/files/0/chunks/0"
    # Another operator cannot write chunks to op1's session.
    res = op2.put(chunk, headers=CSRF, content=b"x" * 64)
    assert res.status_code == 403
    # The owner can.
    res = op1.put(chunk, headers=CSRF, content=b"x" * 64)
    assert res.status_code == 200
    # Another operator cannot abort it; admin can.
    session = "/api/i/plain/v2/repository/models/m/versions/1/upload-sessions/stub-session-id"
    assert op2.delete(session, headers=CSRF).status_code == 403
    assert admin.delete(session, headers=CSRF).status_code == 200


# ===== version ownership (force / delete) =====

def test_upload_2xx_records_version_owner(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    res = op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload?load=false",
                   headers=CSRF, content=b"raw-bytes")
    assert res.status_code == 200
    assert ctx.store.version_owner("plain", "m", "1") == "op1"


def test_force_overwrite_requires_version_owner(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    op2 = client_for(ctx, "op2", "Operator-pass-2")
    admin = client_for(ctx, "admin", "Admin-pass-1234")
    op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload?load=false",
             headers=CSRF, content=b"v1")

    url = "/api/i/plain/v2/repository/models/m/versions/1/upload?load=false&force=true"
    # A non-owner cannot force-overwrite op1's version.
    assert op2.post(url, headers=CSRF, content=b"v2").status_code == 403
    # The owner can.
    assert op1.post(url, headers=CSRF, content=b"v2").status_code == 200
    # Admin can, and ownership stays with the creator.
    assert admin.post(url, headers=CSRF, content=b"v3").status_code == 200
    assert ctx.store.version_owner("plain", "m", "1") == "op1"


def test_force_on_unowned_version_is_admin_only(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    admin = client_for(ctx, "admin", "Admin-pass-1234")
    url = "/api/i/plain/v2/repository/models/m/versions/9/upload?load=false&force=true"
    # No ownership record (pre-M2 version / drift): operator denied, admin ok.
    assert op1.post(url, headers=CSRF, content=b"x").status_code == 403
    assert admin.post(url, headers=CSRF, content=b"x").status_code == 200


def test_delete_requires_version_owner_and_clears_record(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    op2 = client_for(ctx, "op2", "Operator-pass-2")
    op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload?load=false",
             headers=CSRF, content=b"v1")

    url = "/api/i/plain/v2/models/m/versions/1"
    assert op2.delete(url, headers=CSRF).status_code == 403
    assert op1.delete(url, headers=CSRF).status_code == 200
    assert ctx.store.version_owner("plain", "m", "1") is None


def test_batch_delete_is_admin_only(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    admin = client_for(ctx, "admin", "Admin-pass-1234")
    url = "/api/i/plain/v2/models/m/versions"
    assert op1.delete(url, headers=CSRF).status_code == 403
    assert admin.delete(url, headers=CSRF).status_code == 200


def test_get_on_mutation_shaped_paths_is_not_policed(ctx):
    """Mutations are classified by path shape; a GET on the same path is a
    read and must not be judged by mutation rules (regression: operators
    were denied the versions list as 'batch delete is admin-only')."""
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    assert op1.get("/api/i/plain/v2/models/m/versions").status_code == 200
    assert op1.get("/api/i/plain/v2/models/m/versions/1").status_code == 200


def test_batch_delete_removes_ownership_of_deleted_versions(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    admin = client_for(ctx, "admin", "Admin-pass-1234")
    op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload?load=false",
             headers=CSRF, content=b"v1")
    op1.post("/api/i/plain/v2/repository/models/m/versions/2/upload?load=false",
             headers=CSRF, content=b"v2")

    res = admin.request("DELETE", "/api/i/plain/v2/models/m/versions", headers=CSRF,
                        json={"versions": ["1", "2"]})
    assert res.status_code == 200
    assert ctx.store.version_owner("plain", "m", "1") is None
    assert ctx.store.version_owner("plain", "m", "2") is None


def test_batch_delete_keeps_ownership_of_failed_versions(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    admin = client_for(ctx, "admin", "Admin-pass-1234")
    op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload?load=false",
             headers=CSRF, content=b"v1")
    op1.post("/api/i/plain/v2/repository/models/m/versions/bad/upload?load=false",
             headers=CSRF, content=b"v2")

    # The stub upstream fails version "bad"; its ownership row must survive.
    res = admin.request("DELETE", "/api/i/plain/v2/models/m/versions", headers=CSRF,
                        json={"versions": ["1", "bad"]})
    assert res.status_code == 200
    assert ctx.store.version_owner("plain", "m", "1") is None
    assert ctx.store.version_owner("plain", "m", "bad") == "op1"


# ===== ownership record lifecycle =====

def test_stale_session_owner_rows_are_swept_on_next_init(tmp_path):
    store = UserStore(str(tmp_path / "auth.db"), {})
    stale = (datetime.now(timezone.utc) - timedelta(hours=49)).isoformat()
    store._db.execute(
        "INSERT INTO upload_session_owners"
        " (instance_id, session_id, model, version, owner, created_at)"
        " VALUES ('i1', 'stale-sid', 'm', '1', 'op1', ?)",
        (stale,),
    )
    # Recording a new session lazily sweeps rows older than the TTL: the
    # abandoned one goes, the fresh one stays.
    store.record_session_owner("i1", "fresh-sid", "m", "1", "op1")
    assert store.session_owner("i1", "stale-sid") is None
    assert store.session_owner("i1", "fresh-sid") is not None
    store.close()


def test_removing_instance_cascades_ownership_rows(ctx):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    admin = client_for(ctx, "admin", "Admin-pass-1234")
    op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload?load=false",
             headers=CSRF, content=b"v1")
    op1.post("/api/i/plain/v2/repository/models/m/versions/1/upload-sessions",
             headers=CSRF, json={"files": [{"name": "w.bin", "size": 64}]})
    assert ctx.store.version_owner("plain", "m", "1") == "op1"
    assert ctx.store.session_owner("plain", "stub-session-id") is not None

    res = admin.delete("/api/instances/plain", headers=CSRF)
    assert res.status_code == 200
    assert ctx.store.version_owner("plain", "m", "1") is None
    assert ctx.store.session_owner("plain", "stub-session-id") is None


def test_lifespan_closes_auth_db_on_shutdown(tmp_path, upstream, live):
    store = UserStore(str(tmp_path / "auth.db"), {})
    registry = MapRegistry([
        InstanceConfig(id="plain", name="P", base_url=upstream.base_url),
    ])
    server = live(build_app(registry, user_store=store))
    server.stop()
    # The lifespan teardown closed the store; closing again is a safe no-op.
    store.close()
    with pytest.raises(sqlite3.ProgrammingError):
        store.list()


# ===== identity forwarding =====

def test_x_lite_user_forwarded_and_browser_value_stripped(ctx, upstream):
    op1 = client_for(ctx, "op1", "Operator-pass-1")
    op1.get("/api/i/plain/v2/models", headers={"x-lite-user": "forged"})
    assert upstream.last_request["headers"].get("x-lite-user") == "op1"
