"""Local-account auth: users and sessions in SQLite, three-role RBAC."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping

import anyio
import bcrypt
import pyotp
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .authdb import ROLES, AuthDB, utcnow

USERNAME_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]{2,32}$")
COOKIE_NAME = "lite_ui_token"
CSRF_HEADER = "x-requested-with"
CSRF_VALUE = "lite-ui"
SESSION_TTL = timedelta(hours=12)
BCRYPT_COST = 12
# Login throttling: an account locks after LOCK_THRESHOLD failures inside
# LOCK_WINDOW; one source IP is throttled after IP_LOCK_THRESHOLD failures in
# the same window (higher, to tolerate NAT'd legitimate users).
LOCK_THRESHOLD = 5
LOCK_WINDOW = timedelta(minutes=15)
IP_LOCK_THRESHOLD = 30
# Second-factor login challenge: single-use, short-lived, bounded attempts.
TOTP_CHALLENGE_TTL = timedelta(minutes=5)
TOTP_CHALLENGE_MAX_ATTEMPTS = 5
BACKUP_CODE_COUNT = 8
# Abandoned chunked-upload sessions (never completed or aborted through the
# BFF) would leak ownership rows forever. 48h outlives the instance's own
# 24h staging-dir sweep, so a row never dies before the session it guards.
SESSION_OWNER_TTL = timedelta(hours=48)

_logger = logging.getLogger("lite_server.webui.audit")


def role_rank(role: str) -> int:
    return ROLES.index(role)


# Instance-grant roles: the three base roles plus "none" (instance hidden and
# fully denied). Ranked checks never see "none" — the guard rejects it first.
GRANT_ROLES = ROLES + ("none",)

# Model-grant roles: admin is deliberately absent — instance/global admins
# bypass the model whitelist instead of holding per-model admin rows.
MODEL_GRANT_ROLES = ("viewer", "operator")


class AuthError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


_STATUS_FOR = {"invalid": 400, "duplicate": 409, "not_found": 404, "forbidden": 403}


def _now() -> str:
    return utcnow()


def _hash(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(BCRYPT_COST)).decode()


def _hash_cost(password_hash: str) -> int:
    try:
        return int(password_hash.split("$")[2])
    except (IndexError, ValueError):
        return 0


_DUMMY_HASH: str | None = None


def _dummy_hash() -> str:
    """Precomputed hash verified for unknown usernames, so a missing account
    costs the same bcrypt work as a wrong password (timing side channel)."""
    global _DUMMY_HASH
    if _DUMMY_HASH is None:
        _DUMMY_HASH = _hash("dummy-password-for-timing")
    return _DUMMY_HASH


@dataclass
class UserRecord:
    username: str
    password_hash: str
    role: str
    created_at: str
    must_change_password: bool
    totp_secret: str | None = None


def public_user(u: UserRecord) -> dict:
    return {
        "username": u.username,
        "role": u.role,
        "createdAt": u.created_at,
        "mustChangePassword": u.must_change_password,
        "totpEnabled": u.totp_secret is not None,
    }


class UserStore:
    """Local account and session store backed by SQLite (AuthDB). Bootstrap:
    with LITE_UI_ADMIN_PASSWORD set and no users, an `admin` account is
    created (must change password on first login). Without it the store stays
    empty and the first run is served by open registration (first registrant
    becomes admin)."""

    def __init__(self, db_path: str, env: Mapping[str, str],
                 legacy_auth_path: str | None = None):
        self._db = AuthDB(db_path, legacy_auth_path=legacy_auth_path)

        if self._db.query_one("SELECT username FROM users LIMIT 1") is None:
            from_env = env.get("LITE_UI_ADMIN_PASSWORD")
            if from_env:
                self._db.execute(
                    "INSERT INTO users"
                    " (username, password_hash, role, created_at, must_change_password)"
                    " VALUES (?, ?, ?, ?, 1)",
                    ("admin", _hash(from_env), "admin", _now()),
                )

    @staticmethod
    def _to_record(row) -> UserRecord:
        return UserRecord(
            username=row["username"],
            password_hash=row["password_hash"],
            role=row["role"],
            created_at=row["created_at"],
            must_change_password=bool(row["must_change_password"]),
            totp_secret=row["totp_secret"],
        )

    def list(self) -> list[dict]:
        rows = self._db.query("SELECT * FROM users ORDER BY username")
        return [public_user(self._to_record(r)) for r in rows]

    def get(self, username: str) -> UserRecord | None:
        row = self._db.query_one("SELECT * FROM users WHERE username = ?", (username,))
        return self._to_record(row) if row is not None else None

    def verify(self, username: str, password: str) -> UserRecord | None:
        if not isinstance(password, str) or len(password.encode()) > 72:
            return None
        # bcrypt rejects passwords over 72 bytes with ValueError.
        user = self.get(username) if isinstance(username, str) else None
        if user is None:
            bcrypt.checkpw(password.encode(), _dummy_hash().encode())
            return None
        if not bcrypt.checkpw(password.encode(), user.password_hash.encode()):
            return None
        if _hash_cost(user.password_hash) < BCRYPT_COST:
            # Transparently upgrade older hashes to the current cost factor.
            self._db.execute(
                "UPDATE users SET password_hash = ? WHERE username = ?",
                (_hash(password), username),
            )
        return user

    @staticmethod
    def _validate_username(username) -> str:
        if not isinstance(username, str) or not USERNAME_PATTERN.match(username):
            raise AuthError("invalid", f"invalid username: {username!r}")
        return username

    @staticmethod
    def _validate_password(password) -> str:
        if not isinstance(password, str) or len(password) < 12:
            raise AuthError("invalid", "password must be at least 12 characters")
        if len(password.encode()) > 72:
            # bcrypt hard limit; it raises ValueError beyond this.
            raise AuthError("invalid", "password must be at most 72 bytes")
        classes = sum((
            any(c.islower() for c in password),
            any(c.isupper() for c in password),
            any(c.isdigit() for c in password),
            any(not c.isalnum() for c in password),
        ))
        if classes < 3:
            raise AuthError(
                "invalid",
                "password must use at least 3 of: lowercase, uppercase, digits, symbols",
            )
        return password

    @staticmethod
    def _validate_role(role) -> str:
        if role not in ROLES:
            raise AuthError("invalid", f"invalid role: {role!r}")
        return role

    def create(self, raw: dict) -> dict:
        username = self._validate_username(raw.get("username"))
        password = self._validate_password(raw.get("password"))
        role = self._validate_role(raw.get("role"))
        if self.get(username) is not None:
            raise AuthError("duplicate", f'user "{username}" already exists')
        self._db.execute(
            "INSERT INTO users (username, password_hash, role, created_at, must_change_password)"
            " VALUES (?, ?, ?, ?, 1)",
            (username, _hash(password), role, _now()),
        )
        return public_user(self.get(username))

    def update(self, username: str, patch: dict, actor: str) -> dict:
        existing = self.get(username)
        if existing is None:
            raise AuthError("not_found", f'unknown user "{username}"')
        if patch.get("role") is not None:
            role = self._validate_role(patch["role"])
            if existing.role == "admin" and role != "admin" and self._admin_count() <= 1:
                raise AuthError("forbidden", "cannot demote the last admin")
            self._db.execute("UPDATE users SET role = ? WHERE username = ?", (role, username))
        if patch.get("password") is not None:
            self._db.execute(
                "UPDATE users SET password_hash = ?, must_change_password = 1 WHERE username = ?",
                (_hash(self._validate_password(patch["password"])), username),
            )
        return public_user(self.get(username))

    def set_password(self, username: str, password: str) -> None:
        """Self-service password change; clears the must-change flag."""
        if self.get(username) is None:
            raise AuthError("not_found", f'unknown user "{username}"')
        self._db.execute(
            "UPDATE users SET password_hash = ?, must_change_password = 0 WHERE username = ?",
            (_hash(self._validate_password(password)), username),
        )

    def remove(self, username: str, actor: str) -> None:
        existing = self.get(username)
        if existing is None:
            raise AuthError("not_found", f'unknown user "{username}"')
        if username == actor:
            raise AuthError("forbidden", "cannot delete yourself")
        if existing.role == "admin" and self._admin_count() <= 1:
            raise AuthError("forbidden", "cannot delete the last admin")
        self._db.execute("DELETE FROM users WHERE username = ?", (username,))
        self._db.execute("DELETE FROM instance_grants WHERE username = ?", (username,))
        self._db.execute("DELETE FROM model_grants WHERE username = ?", (username,))

    def _admin_count(self) -> int:
        return self._db.query_one("SELECT COUNT(*) FROM users WHERE role = 'admin'")[0]

    def user_count(self) -> int:
        return self._db.query_one("SELECT COUNT(*) FROM users")[0]

    # ---- registration and invites ----

    def register(self, raw: dict) -> dict:
        """Public registration: open (first user becomes admin) when the store
        is empty, otherwise a valid invite code is required. The invite check,
        its consumption, and the user insert are one transaction."""
        username = self._validate_username(raw.get("username"))
        password = self._validate_password(raw.get("password"))
        with self._db.transaction() as conn:
            count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if count == 0:
                role = "admin"
            else:
                code = raw.get("inviteCode")
                if not isinstance(code, str) or not code:
                    raise AuthError("forbidden", "invite_required")
                invite = conn.execute(
                    "SELECT * FROM invites WHERE code = ?", (code,)
                ).fetchone()
                if (invite is None or invite["revoked_at"] is not None
                        or invite["use_count"] >= invite["max_uses"]
                        or (invite["expires_at"] is not None and invite["expires_at"] <= _now())):
                    raise AuthError("invalid", "invalid_invite")
                role = invite["role"]
                conn.execute("UPDATE invites SET use_count = use_count + 1 WHERE code = ?", (code,))
            if conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone():
                raise AuthError("duplicate", f'user "{username}" already exists')
            conn.execute(
                "INSERT INTO users (username, password_hash, role, created_at, must_change_password)"
                " VALUES (?, ?, ?, ?, 0)",
                (username, _hash(password), role, _now()),
            )
        return public_user(self.get(username))

    def create_invite(self, *, role: str = "viewer", max_uses: int = 1,
                      expires_in_hours: int | None = 72, created_by: str) -> dict:
        self._validate_role(role)
        code = secrets.token_urlsafe(12)
        expires_at = (
            (datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)).isoformat()
            if expires_in_hours is not None else None
        )
        self._db.execute(
            "INSERT INTO invites (code, role, max_uses, use_count, expires_at, created_by, created_at)"
            " VALUES (?, ?, ?, 0, ?, ?, ?)",
            (code, role, max(1, int(max_uses)), expires_at, created_by, _now()),
        )
        return self._public_invite(self._db.query_one("SELECT * FROM invites WHERE code = ?", (code,)))

    def list_invites(self) -> list[dict]:
        rows = self._db.query("SELECT * FROM invites ORDER BY created_at DESC")
        return [self._public_invite(r) for r in rows]

    def revoke_invite(self, code: str) -> bool:
        cur = self._db.execute(
            "UPDATE invites SET revoked_at = ? WHERE code = ? AND revoked_at IS NULL",
            (_now(), code),
        )
        return cur.rowcount > 0

    @staticmethod
    def _public_invite(row) -> dict:
        return {
            "code": row["code"],
            "role": row["role"],
            "maxUses": row["max_uses"],
            "useCount": row["use_count"],
            "expiresAt": row["expires_at"],
            "createdBy": row["created_by"],
            "createdAt": row["created_at"],
            "revokedAt": row["revoked_at"],
        }

    # ---- sessions ----

    def create_session(self, username: str, ip: str | None, user_agent: str | None) -> str:
        """Issues an opaque cookie token; only its sha256 is stored."""
        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        self._db.execute(
            "INSERT INTO sessions (id, username, created_at, expires_at, last_seen_at, ip, user_agent)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (_session_id(token), username, _now(), (now + SESSION_TTL).isoformat(),
             _now(), ip, user_agent),
        )
        # Lazy cleanup of expired and long-revoked rows.
        self._db.execute(
            "DELETE FROM sessions WHERE expires_at < ? OR revoked_at < ?",
            (_now(), (now - timedelta(days=7)).isoformat()),
        )
        return token

    def lookup_session(self, token: str):
        """Returns the joined session+user row for a valid cookie token."""
        if not token:
            return None
        row = self._db.query_one(
            "SELECT s.id AS session_id, s.last_seen_at, u.* FROM sessions s"
            " JOIN users u ON u.username = s.username"
            " WHERE s.id = ? AND s.revoked_at IS NULL AND s.expires_at > ?",
            (_session_id(token), _now()),
        )
        if row is None:
            return None
        # Touch last_seen at most once a minute (SSE streams would otherwise
        # turn every event into a write).
        last_seen = datetime.fromisoformat(row["last_seen_at"])
        if datetime.now(timezone.utc) - last_seen > timedelta(seconds=60):
            self._db.execute(
                "UPDATE sessions SET last_seen_at = ? WHERE id = ?", (_now(), row["session_id"])
            )
        return row

    def list_sessions(self, username: str, current_id: str | None = None) -> list[dict]:
        rows = self._db.query(
            "SELECT id, created_at, last_seen_at, ip, user_agent FROM sessions"
            " WHERE username = ? AND revoked_at IS NULL AND expires_at > ?"
            " ORDER BY created_at DESC",
            (username, _now()),
        )
        return [
            {
                "id": r["id"],
                "createdAt": r["created_at"],
                "lastSeenAt": r["last_seen_at"],
                "ip": r["ip"],
                "userAgent": r["user_agent"],
                "current": r["id"] == current_id,
            }
            for r in rows
        ]

    def revoke_session(self, session_id: str, *, username: str | None = None) -> bool:
        """Revokes one session. With username given, only that user's own
        session can be targeted (self-service scoping)."""
        if username is None:
            cur = self._db.execute(
                "UPDATE sessions SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
                (_now(), session_id),
            )
        else:
            cur = self._db.execute(
                "UPDATE sessions SET revoked_at = ?"
                " WHERE id = ? AND username = ? AND revoked_at IS NULL",
                (_now(), session_id, username),
            )
        return cur.rowcount > 0

    def revoke_other_sessions(self, username: str, keep_id: str) -> None:
        self._db.execute(
            "UPDATE sessions SET revoked_at = ?"
            " WHERE username = ? AND id != ? AND revoked_at IS NULL",
            (_now(), username, keep_id),
        )

    # ---- login throttling ----

    def record_failed_attempt(self, ip: str | None, username: str) -> None:
        self._db.execute(
            "INSERT INTO login_attempts (ts, ip, username, success) VALUES (?, ?, ?, 0)",
            (_now(), ip, username),
        )
        # Keep the table bounded; anything outside the window is irrelevant.
        cutoff = (datetime.now(timezone.utc) - LOCK_WINDOW).isoformat()
        self._db.execute("DELETE FROM login_attempts WHERE ts < ?", (cutoff,))

    def failed_attempts(self, *, username: str | None = None, ip: str | None = None,
                        since: str) -> list[str]:
        """Failure timestamps in ascending order, filtered by user or IP."""
        if username is not None:
            rows = self._db.query(
                "SELECT ts FROM login_attempts WHERE username = ? AND success = 0 AND ts > ?"
                " ORDER BY ts ASC",
                (username, since),
            )
        else:
            rows = self._db.query(
                "SELECT ts FROM login_attempts WHERE ip = ? AND success = 0 AND ts > ?"
                " ORDER BY ts ASC",
                (ip, since),
            )
        return [r["ts"] for r in rows]

    def clear_failed_attempts(self, username: str) -> None:
        self._db.execute("DELETE FROM login_attempts WHERE username = ?", (username,))

    # ---- audit ----

    def record_audit(self, action: str, *, actor: str | None = None,
                     target: str | None = None, ip: str | None = None,
                     detail: dict | None = None) -> None:
        self._db.execute(
            "INSERT INTO audit (ts, actor, action, target, ip, detail) VALUES (?, ?, ?, ?, ?, ?)",
            (utcnow(), actor, action, target, ip,
             json.dumps(detail, sort_keys=True) if detail else None),
        )
        _logger.info("action=%s actor=%s target=%s ip=%s detail=%s",
                     action, actor, target, ip, detail)

    # ---- ownership records (M2, see ownership.py) ----

    def version_owner(self, instance_id: str, model: str, version: str) -> str | None:
        row = self._db.query_one(
            "SELECT owner FROM version_owners"
            " WHERE instance_id = ? AND model = ? AND version = ?",
            (instance_id, model, version))
        return row["owner"] if row else None

    def record_version_owner(self, instance_id: str, model: str, version: str,
                             owner: str) -> None:
        # First committer owns; force-overwrite does not transfer ownership.
        self._db.execute(
            "INSERT OR IGNORE INTO version_owners"
            " (instance_id, model, version, owner, created_at) VALUES (?, ?, ?, ?, ?)",
            (instance_id, model, version, owner, _now()))

    def remove_version_owner(self, instance_id: str, model: str, version: str) -> None:
        self._db.execute(
            "DELETE FROM version_owners"
            " WHERE instance_id = ? AND model = ? AND version = ?",
            (instance_id, model, version))

    def session_owner_row(self, instance_id: str, session_id: str):
        return self._db.query_one(
            "SELECT * FROM upload_session_owners"
            " WHERE instance_id = ? AND session_id = ?",
            (instance_id, session_id))

    def session_owner(self, instance_id: str, session_id: str):
        """Row lookup alias kept for tests/diagnostics."""
        return self.session_owner_row(instance_id, session_id)

    def record_session_owner(self, instance_id: str, session_id: str, model: str,
                             version: str, owner: str) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO upload_session_owners"
            " (instance_id, session_id, model, version, owner, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (instance_id, session_id, model, version, owner, _now()))
        # Lazy sweep of rows from abandoned sessions (same pattern as the
        # expired-session cleanup in create_session).
        cutoff = (datetime.now(timezone.utc) - SESSION_OWNER_TTL).isoformat()
        self._db.execute(
            "DELETE FROM upload_session_owners WHERE created_at < ?", (cutoff,))

    def remove_instance_ownership(self, instance_id: str) -> None:
        """Drops every ownership record of an instance; called when the
        instance is removed from the registry so stale rows cannot
        resurrect if the same id is recreated later."""
        self._db.execute(
            "DELETE FROM version_owners WHERE instance_id = ?", (instance_id,))
        self._db.execute(
            "DELETE FROM upload_session_owners WHERE instance_id = ?", (instance_id,))
        self._db.execute(
            "DELETE FROM instance_grants WHERE instance_id = ?", (instance_id,))
        self._db.execute(
            "DELETE FROM model_grants WHERE instance_id = ?", (instance_id,))

    # ---- per-instance role grants (M2) ----

    def effective_role(self, username: str, global_role: str, instance_id: str) -> str:
        """Grant overrides the global role on one instance; no row = global."""
        row = self._db.query_one(
            "SELECT role FROM instance_grants WHERE username = ? AND instance_id = ?",
            (username, instance_id))
        return row["role"] if row else global_role

    def list_instance_grants(self, username: str) -> list[dict]:
        rows = self._db.query(
            "SELECT instance_id, role FROM instance_grants"
            " WHERE username = ? ORDER BY instance_id",
            (username,))
        return [{"instance_id": r["instance_id"], "role": r["role"]} for r in rows]

    def set_instance_grant(self, username: str, instance_id: str, role: str) -> None:
        if role not in GRANT_ROLES:
            raise AuthError("invalid", f'unknown grant role "{role}"')
        self._db.execute(
            "INSERT INTO instance_grants (username, instance_id, role, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(username, instance_id)"
            " DO UPDATE SET role = excluded.role, updated_at = excluded.updated_at",
            (username, instance_id, role, _now(), _now()))

    def remove_instance_grant(self, username: str, instance_id: str) -> None:
        self._db.execute(
            "DELETE FROM instance_grants WHERE username = ? AND instance_id = ?",
            (username, instance_id))

    # ---- per-model ACL (M3) ----

    def model_grant(self, username: str, instance_id: str, model: str) -> str | None:
        row = self._db.query_one(
            "SELECT role FROM model_grants"
            " WHERE username = ? AND instance_id = ? AND model = ?",
            (username, instance_id, model))
        return row["role"] if row else None

    def has_model_grants(self, username: str, instance_id: str) -> bool:
        """Any row activates whitelist mode for this user on this instance."""
        return self._db.query_one(
            "SELECT 1 FROM model_grants WHERE username = ? AND instance_id = ? LIMIT 1",
            (username, instance_id)) is not None

    def list_model_grants(self, username: str, instance_id: str | None = None) -> list[dict]:
        if instance_id is None:
            rows = self._db.query(
                "SELECT instance_id, model, role FROM model_grants"
                " WHERE username = ? ORDER BY instance_id, model",
                (username,))
        else:
            rows = self._db.query(
                "SELECT instance_id, model, role FROM model_grants"
                " WHERE username = ? AND instance_id = ? ORDER BY model",
                (username, instance_id))
        return [{"instance_id": r["instance_id"], "model": r["model"], "role": r["role"]}
                for r in rows]

    def list_model_grant_users(self, instance_id: str, model: str) -> list[dict]:
        rows = self._db.query(
            "SELECT username, role FROM model_grants"
            " WHERE instance_id = ? AND model = ? ORDER BY username",
            (instance_id, model))
        return [{"username": r["username"], "role": r["role"]} for r in rows]

    def set_model_grant(self, username: str, instance_id: str, model: str,
                        role: str) -> None:
        if role not in MODEL_GRANT_ROLES:
            raise AuthError("invalid", f'unknown model grant role "{role}"')
        self._db.execute(
            "INSERT INTO model_grants (username, instance_id, model, role, created_at)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(username, instance_id, model)"
            " DO UPDATE SET role = excluded.role",
            (username, instance_id, model, role, _now()))

    def remove_model_grant(self, username: str, instance_id: str, model: str) -> None:
        self._db.execute(
            "DELETE FROM model_grants WHERE username = ? AND instance_id = ? AND model = ?",
            (username, instance_id, model))

    def close(self) -> None:
        """Closes the underlying AuthDB (WAL checkpoint on shutdown)."""
        self._db.close()

    def remove_session_owner(self, instance_id: str, session_id: str) -> None:
        self._db.execute(
            "DELETE FROM upload_session_owners"
            " WHERE instance_id = ? AND session_id = ?",
            (instance_id, session_id))

    def list_audit(self, *, limit: int = 100, offset: int = 0,
                   action: str | None = None, actor: str | None = None) -> list[dict]:
        sql = "SELECT * FROM audit"
        clauses: list[str] = []
        params: list = []
        if action:
            clauses.append("action = ?")
            params.append(action)
        if actor:
            clauses.append("actor = ?")
            params.append(actor)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
        params.extend((min(limit, 500), offset))
        rows = self._db.query(sql, tuple(params))
        return [
            {
                "id": r["id"],
                "ts": r["ts"],
                "actor": r["actor"],
                "action": r["action"],
                "target": r["target"],
                "ip": r["ip"],
                "detail": json.loads(r["detail"]) if r["detail"] else None,
            }
            for r in rows
        ]

    # ---- TOTP ----

    def totp_pending_secret(self, username: str) -> str | None:
        row = self._db.query_one(
            "SELECT totp_pending_secret FROM users WHERE username = ?", (username,)
        )
        return row["totp_pending_secret"] if row else None

    def set_totp_pending(self, username: str, secret: str) -> None:
        self._db.execute(
            "UPDATE users SET totp_pending_secret = ? WHERE username = ?", (secret, username)
        )

    def enable_totp(self, username: str, backup_hashes: list[str]) -> None:
        """Promotes the pending secret to active and stores backup code hashes."""
        self._db.execute(
            "UPDATE users SET totp_secret = totp_pending_secret,"
            " totp_pending_secret = NULL, backup_codes = ? WHERE username = ?",
            (json.dumps(backup_hashes), username),
        )

    def disable_totp(self, username: str) -> None:
        self._db.execute(
            "UPDATE users SET totp_secret = NULL, totp_pending_secret = NULL,"
            " backup_codes = NULL WHERE username = ?",
            (username,),
        )

    def consume_backup_code(self, username: str, code: str) -> bool:
        """Single-use backup codes, stored as sha256 hashes only."""
        row = self._db.query_one("SELECT backup_codes FROM users WHERE username = ?", (username,))
        if row is None or not row["backup_codes"]:
            return False
        hashes: list[str] = json.loads(row["backup_codes"])
        digest = hashlib.sha256(code.encode()).hexdigest()
        if digest not in hashes:
            return False
        hashes.remove(digest)
        self._db.execute(
            "UPDATE users SET backup_codes = ? WHERE username = ?",
            (json.dumps(hashes), username),
        )
        return True

    # ---- second-factor login challenges ----

    def create_challenge(self, username: str) -> str:
        challenge_id = secrets.token_urlsafe(24)
        now = datetime.now(timezone.utc)
        self._db.execute(
            "INSERT INTO login_challenges (id, username, created_at, expires_at, attempts)"
            " VALUES (?, ?, ?, ?, 0)",
            (challenge_id, username, _now(), (now + TOTP_CHALLENGE_TTL).isoformat()),
        )
        self._db.execute("DELETE FROM login_challenges WHERE expires_at < ?", (_now(),))
        return challenge_id

    def get_challenge(self, challenge_id: str):
        return self._db.query_one(
            "SELECT * FROM login_challenges WHERE id = ? AND expires_at > ?",
            (challenge_id, _now()),
        )

    def bump_challenge_attempts(self, challenge_id: str) -> int:
        self._db.execute(
            "UPDATE login_challenges SET attempts = attempts + 1 WHERE id = ?", (challenge_id,)
        )
        row = self._db.query_one(
            "SELECT attempts FROM login_challenges WHERE id = ?", (challenge_id,)
        )
        return row["attempts"] if row else TOTP_CHALLENGE_MAX_ATTEMPTS

    def delete_challenge(self, challenge_id: str) -> None:
        self._db.execute("DELETE FROM login_challenges WHERE id = ?", (challenge_id,))


def _session_id(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def _json_body(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def _error(exc: AuthError) -> JSONResponse:
    return JSONResponse({"error": str(exc)}, status_code=_STATUS_FOR.get(exc.code, 500))


def _set_session_cookie(response: JSONResponse, request: Request, token: str) -> None:
    response.set_cookie(
        COOKIE_NAME,
        token,
        path="/",
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
    )


def _finish_login(request: Request, store: UserStore, user: UserRecord) -> JSONResponse:
    """Issues the session cookie once every factor has verified."""
    token = store.create_session(
        user.username,
        ip=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
    )
    response = JSONResponse({"user": public_user(user)})
    _set_session_cookie(response, request, token)
    return response


# Inference calls (unary/SSE) are open to viewers; everything else on the proxy
# is admin-class and needs operator+. Matched against the URL path only —
# query strings must never influence the exemption.
_INFER_RE = re.compile(r"/(infer|events)$")

# POST /v2/repository/index is a read-only disk scan (KServe shape forces POST),
# so it shares the viewer exemption — the Models page needs it to list
# repository models that are not loaded.
_REPOSITORY_INDEX_RE = re.compile(r"/v2/repository/index$")


def check_request(store: UserStore, enabled: bool, request: Request) -> JSONResponse | None:
    """RBAC guard, enforced at the route layer (never trusted to the frontend).

    Returns a deny response, or None when the request may proceed."""
    path = request.url.path
    if not enabled:
        request.state.user = {"username": "local", "role": "admin"}
        request.state.session_id = None
        return None
    if not path.startswith("/api/"):
        return None  # static assets are public
    if path in ("/api/auth/login", "/api/auth/register", "/api/auth/totp") and request.method == "POST":
        # These set a session cookie, so they need the same CSRF protection as
        # every other mutation (SameSite=Lax does not stop a cross-site POST
        # from planting a cookie in the response).
        if request.headers.get(CSRF_HEADER) != CSRF_VALUE:
            return JSONResponse({"error": "csrf_header_missing"}, status_code=403)
        return None
    if path == "/api/auth/registration" and request.method == "GET":
        return None

    session = store.lookup_session(request.cookies.get(COOKIE_NAME, ""))
    if session is None:
        # Namespaced marker: the frontend logs out only on this exact body, so
        # a 401 relayed from a proxied instance can never be mistaken for an
        # expired BFF session.
        return JSONResponse({"error": "bff_unauthenticated"}, status_code=401)
    request.state.user = {"username": session["username"], "role": session["role"]}
    request.state.session_id = session["session_id"]

    # Roles and account deletion take effect immediately: the store row, not
    # any token payload, is authoritative.
    role = session["role"]
    must_change = bool(session["must_change_password"])
    is_password_flow = path in ("/api/auth/me", "/api/auth/change-password", "/api/auth/logout")
    if must_change and not is_password_flow:
        return JSONResponse({"error": "password_change_required"}, status_code=403)

    if path.startswith("/api/i/"):
        # Per-instance grant overrides the global role on the proxied paths;
        # "none" hides the instance entirely (reads included). The effective
        # role is what ownership.py and the operator check below consume.
        parts = path.split("/")
        inst_id = parts[3] if len(parts) > 3 else ""
        role = store.effective_role(session["username"], role, inst_id)
        if role == "none":
            return JSONResponse(
                {"error": "forbidden", "reason": "instance_denied", "instance": inst_id},
                status_code=403)
        request.state.user = {"username": session["username"], "role": role}

    if request.method != "GET":
        # CSRF: cookie auth + custom header a cross-site form cannot send.
        if request.headers.get(CSRF_HEADER) != CSRF_VALUE:
            return JSONResponse({"error": "csrf_header_missing"}, status_code=403)
        if path.startswith("/api/auth/"):
            return None  # any authenticated user
        if path.startswith("/api/i/"):
            if _INFER_RE.search(path) or _REPOSITORY_INDEX_RE.search(path):
                return None
            if role_rank(role) >= role_rank("operator"):
                return None
            return JSONResponse({"error": "forbidden", "required": "operator"}, status_code=403)
        # Everything else (/api/instances, /api/users, ...) is admin territory.
        if role_rank(role) >= role_rank("admin"):
            return None
        return JSONResponse({"error": "forbidden", "required": "admin"}, status_code=403)

    # GETs: any authenticated user — except the user directory, audit trail,
    # and invite list.
    if path.startswith(("/api/users", "/api/audit", "/api/invites", "/api/model-grants")) and role_rank(role) < role_rank("admin"):
        return JSONResponse({"error": "forbidden", "required": "admin"}, status_code=403)
    return None


def create_router(store: UserStore) -> APIRouter:
    router = APIRouter()

    @router.post("/api/auth/login")
    async def login(request: Request):
        body = await _json_body(request)
        username = body.get("username") if isinstance(body.get("username"), str) else ""
        password = body.get("password") or ""
        ip = request.client.host if request.client else None
        now = datetime.now(timezone.utc)
        window_start = (now - LOCK_WINDOW).isoformat()

        # IP throttle: sustained failures from one source, any account.
        if ip and len(store.failed_attempts(ip=ip, since=window_start)) >= IP_LOCK_THRESHOLD:
            store.record_audit("login_throttled", target=username or None, ip=ip)
            return JSONResponse(
                {"error": "too_many_attempts"},
                status_code=429,
                headers={"Retry-After": str(int(LOCK_WINDOW.total_seconds()))},
            )

        # Account lockout: exact per-account anti-bruteforce. Locked accounts
        # skip bcrypt entirely (the lock status is not a secret).
        if username:
            failures = store.failed_attempts(username=username, since=window_start)
            if len(failures) >= LOCK_THRESHOLD:
                oldest_of_last_five = datetime.fromisoformat(failures[-LOCK_THRESHOLD])
                retry_after = max(1, int((oldest_of_last_five + LOCK_WINDOW - now).total_seconds()))
                return JSONResponse(
                    {"error": "account_locked", "retryAfterSec": retry_after},
                    status_code=423,
                )

        # bcrypt at cost 12 blocks the event loop for ~250ms; run it off-loop.
        user = await anyio.to_thread.run_sync(store.verify, username, password) if username else None
        if user is None:
            store.record_failed_attempt(ip, username)
            store.record_audit("login_failure", target=username or None, ip=ip)
            if username and len(store.failed_attempts(username=username, since=window_start)) == LOCK_THRESHOLD:
                store.record_audit("account_locked", target=username, ip=ip)
            return JSONResponse({"error": "invalid_credentials"}, status_code=401)

        store.clear_failed_attempts(user.username)
        if user.totp_secret is not None:
            # Password verified; the second factor gates the actual session.
            challenge = store.create_challenge(user.username)
            return JSONResponse({"totpRequired": True, "challenge": challenge})
        store.record_audit("login_success", actor=user.username, ip=ip)
        return _finish_login(request, store, user)

    @router.post("/api/auth/totp")
    async def totp_verify(request: Request):
        body = await _json_body(request)
        challenge_id = body.get("challenge") if isinstance(body.get("challenge"), str) else ""
        code = body.get("code") or ""
        ip = _client_ip(request)
        challenge = store.get_challenge(challenge_id)
        if challenge is None:
            return JSONResponse({"error": "invalid_challenge"}, status_code=401)
        username = challenge["username"]
        user = store.get(username)
        verified = user is not None and user.totp_secret is not None and (
            pyotp.TOTP(user.totp_secret).verify(code, valid_window=1)
            or store.consume_backup_code(username, code)
        )
        if not verified:
            store.record_audit("totp_failure", target=username, ip=ip)
            if store.bump_challenge_attempts(challenge_id) >= TOTP_CHALLENGE_MAX_ATTEMPTS:
                store.delete_challenge(challenge_id)
            return JSONResponse({"error": "invalid_code"}, status_code=401)
        store.delete_challenge(challenge_id)
        store.record_audit("login_success", actor=username, ip=ip, detail={"secondFactor": True})
        return _finish_login(request, store, user)

    @router.post("/api/auth/totp/enroll")
    async def totp_enroll(request: Request):
        username = request.state.user["username"]
        secret = pyotp.random_base32()
        store.set_totp_pending(username, secret)
        uri = pyotp.TOTP(secret).provisioning_uri(name=username, issuer_name="lite-ui")
        return {"secret": secret, "otpauthUrl": uri}

    @router.post("/api/auth/totp/confirm")
    async def totp_confirm(request: Request):
        username = request.state.user["username"]
        code = (await _json_body(request)).get("code") or ""
        pending = store.totp_pending_secret(username)
        if pending is None or not pyotp.TOTP(pending).verify(code, valid_window=1):
            return JSONResponse({"error": "invalid_code"}, status_code=401)
        backup_codes = [secrets.token_hex(4) for _ in range(BACKUP_CODE_COUNT)]
        store.enable_totp(username, [hashlib.sha256(c.encode()).hexdigest() for c in backup_codes])
        store.record_audit("totp_enabled", actor=username, ip=_client_ip(request))
        # Shown once; only the hashes are stored.
        return {"backupCodes": backup_codes}

    @router.post("/api/auth/totp/disable")
    async def totp_disable(request: Request):
        username = request.state.user["username"]
        code = (await _json_body(request)).get("code") or ""
        user = store.get(username)
        if user is None or user.totp_secret is None:
            return JSONResponse({"error": "totp_not_enabled"}, status_code=400)
        if not (pyotp.TOTP(user.totp_secret).verify(code, valid_window=1)
                or store.consume_backup_code(username, code)):
            return JSONResponse({"error": "invalid_code"}, status_code=401)
        store.disable_totp(username)
        store.record_audit("totp_disabled", actor=username, ip=_client_ip(request))
        return {"ok": True}

    @router.get("/api/auth/registration")
    async def registration_status():
        open_ = store.user_count() == 0
        return {"open": open_, "inviteRequired": not open_}

    @router.post("/api/auth/register")
    async def register(request: Request):
        try:
            # bcrypt at cost 12 blocks the event loop for ~250ms (see login).
            user = await anyio.to_thread.run_sync(store.register, await _json_body(request))
        except AuthError as e:
            return _error(e)
        ip = _client_ip(request)
        store.record_audit("register", actor=user["username"], ip=ip,
                           detail={"role": user["role"]})
        token = store.create_session(
            user["username"], ip=ip, user_agent=request.headers.get("user-agent")
        )
        response = JSONResponse({"user": user}, status_code=201)
        _set_session_cookie(response, request, token)
        return response

    @router.post("/api/auth/logout")
    async def logout(request: Request):
        session_id = request.state.session_id
        if session_id is not None:  # None in auth-off mode: nothing to revoke
            store.revoke_session(session_id)
            store.record_audit("session_revoked", actor=request.state.user["username"],
                               ip=_client_ip(request), detail={"session": session_id[:12]})
        response = JSONResponse({"ok": True})
        response.delete_cookie(COOKIE_NAME, path="/")
        return response

    @router.get("/api/auth/me")
    async def me(request: Request):
        current = store.get(request.state.user["username"])
        if current is None:
            # Auth-off mode: the guard injects a synthetic local admin that
            # has no row in the user store.
            return {"user": {**request.state.user, "createdAt": None,
                             "mustChangePassword": False, "totpEnabled": False}}
        return {"user": public_user(current)}

    @router.post("/api/auth/change-password")
    async def change_password(request: Request):
        body = await _json_body(request)
        payload = request.state.user
        current_password = body.get("currentPassword") or ""
        new_password = body.get("newPassword") or ""
        verified = await anyio.to_thread.run_sync(store.verify, payload["username"], current_password)
        if verified is None:
            return JSONResponse({"error": "invalid_credentials"}, status_code=401)
        if new_password == current_password:
            return JSONResponse({"error": "password_reused"}, status_code=400)
        try:
            await anyio.to_thread.run_sync(store.set_password, payload["username"], new_password)
        except AuthError as e:
            return _error(e)
        # A password change kicks every other session; the current one stays.
        store.revoke_other_sessions(payload["username"], request.state.session_id)
        store.record_audit("password_changed", actor=payload["username"], ip=_client_ip(request))
        user = store.get(payload["username"])
        return JSONResponse({"user": public_user(user)})

    @router.get("/api/auth/sessions")
    async def my_sessions(request: Request):
        return {"sessions": store.list_sessions(
            request.state.user["username"], current_id=request.state.session_id)}

    @router.delete("/api/auth/sessions/{session_id}")
    async def revoke_my_session(session_id: str, request: Request):
        if store.revoke_session(session_id, username=request.state.user["username"]):
            store.record_audit("session_revoked", actor=request.state.user["username"],
                               ip=_client_ip(request), detail={"session": session_id[:12]})
        return {"ok": True}

    # ---- user management (admin; enforced by the guard) ----

    @router.get("/api/users")
    async def list_users():
        return {"users": store.list()}

    @router.post("/api/users")
    async def create_user(request: Request):
        try:
            user = await anyio.to_thread.run_sync(store.create, await _json_body(request))
        except AuthError as e:
            return _error(e)
        store.record_audit("user_created", actor=request.state.user["username"],
                           target=user["username"], ip=_client_ip(request),
                           detail={"role": user["role"]})
        return JSONResponse({"user": user}, status_code=201)

    @router.put("/api/users/{name}")
    async def update_user(name: str, request: Request):
        try:
            user = await anyio.to_thread.run_sync(
                store.update, name, await _json_body(request), request.state.user["username"])
        except AuthError as e:
            return _error(e)
        store.record_audit("user_updated", actor=request.state.user["username"],
                           target=name, ip=_client_ip(request))
        return {"user": user}

    @router.delete("/api/users/{name}")
    async def delete_user(name: str, request: Request):
        try:
            store.remove(name, request.state.user["username"])
        except AuthError as e:
            return _error(e)
        store.record_audit("user_deleted", actor=request.state.user["username"],
                           target=name, ip=_client_ip(request))
        return {"ok": True}

    @router.post("/api/users/{name}/unlock")
    async def unlock_user(name: str, request: Request):
        store.clear_failed_attempts(name)
        store.record_audit("unlock", actor=request.state.user["username"],
                           target=name, ip=_client_ip(request))
        return {"ok": True}

    @router.delete("/api/users/{name}/totp")
    async def reset_user_totp(name: str, request: Request):
        store.disable_totp(name)
        store.record_audit("totp_reset", actor=request.state.user["username"],
                           target=name, ip=_client_ip(request))
        return {"ok": True}

    @router.get("/api/users/{name}/sessions")
    async def user_sessions(name: str):
        return {"sessions": store.list_sessions(name)}

    @router.get("/api/users/{name}/grants")
    async def list_user_grants(name: str):
        if store.get(name) is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return {"grants": store.list_instance_grants(name)}

    @router.put("/api/users/{name}/grants/{instance_id}")
    async def put_user_grant(name: str, instance_id: str, request: Request):
        if store.get(name) is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        body = await _json_body(request)
        role = (body or {}).get("role")
        if role == "default":
            # Removing the row restores the global role on that instance.
            store.remove_instance_grant(name, instance_id)
        else:
            try:
                store.set_instance_grant(name, instance_id, role)
            except AuthError as e:
                return _error(e)
        store.record_audit("instance_grant", actor=request.state.user["username"],
                           target=name, ip=_client_ip(request),
                           detail={"instance_id": instance_id, "role": role})
        return {"grants": store.list_instance_grants(name)}

    @router.get("/api/users/{name}/model-grants")
    async def list_user_model_grants(name: str, instance_id: str | None = None):
        if store.get(name) is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return {"grants": store.list_model_grants(name, instance_id)}

    @router.put("/api/users/{name}/model-grants/{instance_id}/{model}")
    async def put_user_model_grant(name: str, instance_id: str, model: str,
                                   request: Request):
        if store.get(name) is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        body = await _json_body(request)
        role = (body or {}).get("role")
        if role == "default":
            # Removing the row may deactivate the whitelist (last row).
            store.remove_model_grant(name, instance_id, model)
        else:
            try:
                store.set_model_grant(name, instance_id, model, role)
            except AuthError as e:
                return _error(e)
        store.record_audit("model_grant", actor=request.state.user["username"],
                           target=name, ip=_client_ip(request),
                           detail={"instance_id": instance_id, "model": model, "role": role})
        return {"grants": store.list_model_grants(name, instance_id)}

    @router.get("/api/model-grants")
    async def list_model_grants_for_model(instance_id: str, model: str):
        """Model-centric view: which users hold grants on this model."""
        return {"grants": store.list_model_grant_users(instance_id, model)}

    @router.delete("/api/users/{name}/sessions/{session_id}")
    async def kick_user_session(name: str, session_id: str, request: Request):
        if store.revoke_session(session_id, username=name):
            store.record_audit("session_revoked", actor=request.state.user["username"],
                               target=name, ip=_client_ip(request),
                               detail={"session": session_id[:12]})
        return {"ok": True}

    @router.get("/api/invites")
    async def list_invites():
        return {"invites": store.list_invites()}

    @router.post("/api/invites")
    async def create_invite(request: Request):
        body = await _json_body(request)
        try:
            invite = store.create_invite(
                role=body.get("role") or "viewer",
                max_uses=body.get("maxUses") or 1,
                expires_in_hours=body.get("expiresInHours", 72),
                created_by=request.state.user["username"],
            )
        except AuthError as e:
            return _error(e)
        store.record_audit("invite_created", actor=request.state.user["username"],
                           ip=_client_ip(request),
                           detail={"role": invite["role"], "maxUses": invite["maxUses"]})
        return JSONResponse({"invite": invite}, status_code=201)

    @router.delete("/api/invites/{code}")
    async def revoke_invite(code: str, request: Request):
        store.revoke_invite(code)
        store.record_audit("invite_revoked", actor=request.state.user["username"],
                           ip=_client_ip(request))
        return {"ok": True}

    @router.get("/api/audit")
    async def list_audit(limit: int = 100, offset: int = 0,
                         action: str | None = None, actor: str | None = None):
        return {"entries": store.list_audit(limit=limit, offset=offset, action=action, actor=actor)}

    return router
