"""SQLite storage for webui auth state: users, sessions, invites, audit.

Single-connection store with serialized access and WAL. Migrations are plain
SQL strings indexed by schema version, applied via PRAGMA user_version. The
web UI is a single-process console; a module lock plus busy_timeout makes
writes safe without a connection pool.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import yaml

_logger = logging.getLogger("lite_server.webui")

ROLES = ("viewer", "operator", "admin")

MIGRATIONS: list[str] = [
    # v1: local accounts
    """
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK (role IN ('viewer', 'operator', 'admin')),
        created_at TEXT NOT NULL,
        must_change_password INTEGER NOT NULL DEFAULT 0
    )
    """,
    # v2: opaque-token sessions (id = sha256 of the cookie token)
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        ip TEXT,
        user_agent TEXT,
        revoked_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(username)
    """,
    # v3: login throttling and the security audit trail
    """
    CREATE TABLE IF NOT EXISTS login_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        ip TEXT,
        username TEXT NOT NULL,
        success INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_attempts_user ON login_attempts(username, ts);
    CREATE INDEX IF NOT EXISTS idx_attempts_ip ON login_attempts(ip, ts);
    CREATE TABLE IF NOT EXISTS audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        actor TEXT,
        action TEXT NOT NULL,
        target TEXT,
        ip TEXT,
        detail TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit(ts)
    """,
    # v4: invite codes for registration
    """
    CREATE TABLE IF NOT EXISTS invites (
        code TEXT PRIMARY KEY,
        role TEXT NOT NULL,
        max_uses INTEGER NOT NULL DEFAULT 1,
        use_count INTEGER NOT NULL DEFAULT 0,
        expires_at TEXT,
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        revoked_at TEXT
    )
    """,
    # v5: TOTP two-factor auth (backup codes stored as sha256 hashes)
    """
    ALTER TABLE users ADD COLUMN totp_secret TEXT;
    ALTER TABLE users ADD COLUMN totp_pending_secret TEXT;
    ALTER TABLE users ADD COLUMN backup_codes TEXT;
    CREATE TABLE IF NOT EXISTS login_challenges (
        id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0
    )
    """,
    # v6: ownership records (M2) — who created a model version and who owns
    # an in-flight chunked-upload session, per proxied instance. The first
    # committer owns a version; force-overwrite does not transfer ownership.
    # Versions with no record (pre-v6, direct writes) fall to the admin-only
    # reclaim rule at the policy layer.
    """
    CREATE TABLE IF NOT EXISTS version_owners (
        instance_id TEXT NOT NULL,
        model TEXT NOT NULL,
        version TEXT NOT NULL,
        owner TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (instance_id, model, version)
    );
    CREATE TABLE IF NOT EXISTS upload_session_owners (
        instance_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        model TEXT NOT NULL,
        version TEXT NOT NULL,
        owner TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (instance_id, session_id)
    )
    """,
    # v7: per-instance role grants. A row overrides the user's global role on
    # that instance (role "none" hides/denies the instance entirely); no row
    # means the global role applies, which keeps pre-v7 behavior.
    """
    CREATE TABLE IF NOT EXISTS instance_grants (
        username TEXT NOT NULL,
        instance_id TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (username, instance_id)
    )
    """,
]


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class AuthDB:
    def __init__(self, path: str, *, legacy_auth_path: str | None = None):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._migrate()
            if legacy_auth_path is not None:
                self._import_legacy_yaml(legacy_auth_path)
        os.chmod(path, 0o600)

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    @contextmanager
    def transaction(self):
        """Multi-statement atomic write under the store lock (e.g. consuming
        an invite and inserting the user must not interleave)."""
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _migrate(self) -> None:
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        for idx in range(version, len(MIGRATIONS)):
            # Applied per statement so a crash mid-migration can be retried:
            # CREATEs are IF NOT EXISTS and duplicate-column ALTERs (the only
            # non-idempotent DDL here) are tolerated.
            for statement in MIGRATIONS[idx].split(";"):
                statement = statement.strip()
                if not statement:
                    continue
                try:
                    self._conn.execute(statement)
                except sqlite3.OperationalError as e:
                    if "duplicate column name" not in str(e):
                        raise
            self._conn.execute(f"PRAGMA user_version = {idx + 1}")
            self._conn.commit()

    def _import_legacy_yaml(self, legacy_path: str) -> None:
        """One-shot import from the pre-SQLite auth.yaml; renames the file so
        it is never imported twice. Skipped entirely when users exist."""
        path = Path(legacy_path)
        if not path.exists():
            return
        if self._conn.execute("SELECT username FROM users LIMIT 1").fetchone() is not None:
            return
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        imported = 0
        for raw in doc.get("users") or []:
            if not isinstance(raw.get("username"), str) or not isinstance(raw.get("password_hash"), str):
                continue
            self._conn.execute(
                "INSERT OR IGNORE INTO users"
                " (username, password_hash, role, created_at, must_change_password)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    raw["username"],
                    raw["password_hash"],
                    raw["role"] if raw.get("role") in ROLES else "viewer",
                    raw["created_at"] if isinstance(raw.get("created_at"), str) else utcnow(),
                    1 if raw.get("must_change_password") is True else 0,
                ),
            )
            imported += 1
        self._conn.commit()
        if imported:
            migrated = path.with_name(path.name + ".migrated")
            path.rename(migrated)
            _logger.info("imported %d user(s) from %s; renamed to %s", imported, path, migrated)
