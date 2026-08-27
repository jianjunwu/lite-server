"""Audit: user-store mutations must be atomic and the auth SQLite must not
leave world-readable sidecar files behind.

- ``UserStore.update`` commits the role change before validating the new
  password — a 400 still leaves the role applied.
- The WAL/SHM sidecar files are created (world-readable) before
  ``os.chmod(path, 0o600)`` and never chmod'd themselves.
"""

from __future__ import annotations

import pytest

from lite_server.webui.auth import AuthError, UserStore
from lite_server.webui.authdb import AuthDB, utcnow


@pytest.fixture
def store(tmp_path):
    return UserStore(
        str(tmp_path / "auth.db"), {"LITE_UI_ADMIN_PASSWORD": "boot-pass-1"},
        legacy_auth_path=str(tmp_path / "auth.yaml"),
    )


# ===== atomicity of update() =====

def test_update_rejects_bad_password_without_committing_role(store):
    store.set_password("admin", "Admin-pass-1234")
    store.create({"username": "alice", "password": "Valid-pass-1234", "role": "viewer"})
    store.set_password("alice", "Valid-pass-1234")

    with pytest.raises(AuthError):
        store.update("alice", {"role": "admin", "password": "short"}, actor="admin")

    assert store.get("alice").role == "viewer", (
        "a failed update must not leave the role applied"
    )


# ===== auth.db sidecar file permissions =====

def test_auth_db_sidecar_files_are_not_world_readable(tmp_path):
    db = AuthDB(str(tmp_path / "auth.db"))
    try:
        db.execute(
            "INSERT INTO users (username, password_hash, role, created_at,"
            " must_change_password) VALUES ('a', 'x', 'viewer', ?, 0)",
            (utcnow(),),
        )
        sidecars = [p for suffix in ("-wal", "-shm")
                    if (p := tmp_path / f"auth.db{suffix}").exists()]
        assert sidecars, "expected WAL/SHM sidecar files after a write"
        for p in sidecars:
            mode = p.stat().st_mode & 0o077
            assert mode == 0, (
                f"{p.name} must not be group/other readable "
                f"(mode {oct(p.stat().st_mode & 0o777)})"
            )
    finally:
        db.close()
