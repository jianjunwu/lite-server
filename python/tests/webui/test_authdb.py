"""SQLite auth store: schema, migrations, legacy auth.yaml import."""

import os
import stat

import bcrypt
import yaml

from lite_server.webui.authdb import MIGRATIONS, AuthDB


def test_should_create_db_with_wal_and_0600_permissions(tmp_path):
    db_path = tmp_path / "auth.db"
    db = AuthDB(str(db_path))
    assert stat.S_IMODE(os.stat(db_path).st_mode) == 0o600
    assert db.query_one("PRAGMA journal_mode")[0] == "wal"
    db.close()


def test_should_apply_migrations_in_order_and_record_user_version(tmp_path):
    db = AuthDB(str(tmp_path / "auth.db"))
    assert db.query_one("PRAGMA user_version")[0] == len(MIGRATIONS)
    db.execute(
        "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
        ("alice", "hash", "viewer", "2026-01-01T00:00:00+00:00"),
    )
    assert db.query_one("SELECT username FROM users")["username"] == "alice"
    db.close()


def test_should_keep_data_and_version_across_reopen(tmp_path):
    db_path = str(tmp_path / "auth.db")
    db = AuthDB(db_path)
    db.execute(
        "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
        ("alice", "hash", "admin", "2026-01-01T00:00:00+00:00"),
    )
    db.close()
    reopened = AuthDB(db_path)
    assert reopened.query_one("PRAGMA user_version")[0] == len(MIGRATIONS)
    assert reopened.query_one("SELECT role FROM users WHERE username = 'alice'")["role"] == "admin"
    reopened.close()


def _write_legacy_yaml(path, users):
    path.write_text(yaml.safe_dump({"users": users}), encoding="utf-8")


def test_should_import_users_from_legacy_auth_yaml_and_rename_it(tmp_path):
    pw = bcrypt.hashpw(b"legacy-pass-1", bcrypt.gensalt(4)).decode()
    legacy = tmp_path / "auth.yaml"
    _write_legacy_yaml(legacy, [
        {
            "username": "admin",
            "password_hash": pw,
            "role": "admin",
            "created_at": "2026-01-01T00:00:00+00:00",
            "must_change_password": True,
        },
        {
            "username": "bad-entry-no-hash",
        },
    ])
    db = AuthDB(str(tmp_path / "auth.db"), legacy_auth_path=str(legacy))
    row = db.query_one("SELECT * FROM users WHERE username = 'admin'")
    assert row is not None
    assert row["role"] == "admin"
    assert row["must_change_password"] == 1
    assert row["password_hash"] == pw
    # Entry without a password hash is skipped.
    assert db.query_one("SELECT COUNT(*) FROM users")[0] == 1
    assert not legacy.exists()
    assert (tmp_path / "auth.yaml.migrated").exists()
    db.close()


def test_should_not_reimport_when_db_already_has_users(tmp_path):
    db_path = str(tmp_path / "auth.db")
    db = AuthDB(db_path)
    db.execute(
        "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
        ("existing", "hash", "admin", "2026-01-01T00:00:00+00:00"),
    )
    db.close()

    legacy = tmp_path / "auth.yaml"
    _write_legacy_yaml(legacy, [
        {"username": "intruder", "password_hash": "x", "role": "admin"},
    ])
    reopened = AuthDB(db_path, legacy_auth_path=str(legacy))
    assert reopened.query_one("SELECT COUNT(*) FROM users")[0] == 1
    assert reopened.query_one("SELECT username FROM users")["username"] == "existing"
    # The legacy file is left alone when no import happened.
    assert legacy.exists()
    reopened.close()
