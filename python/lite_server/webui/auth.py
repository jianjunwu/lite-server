"""Local-account auth: users in auth.yaml, JWT cookie, three-role RBAC."""

from __future__ import annotations

import logging
import os
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import bcrypt
import jwt
import yaml
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

ROLES = ["viewer", "operator", "admin"]
USERNAME_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]{2,32}$")
COOKIE_NAME = "lite_ui_token"
CSRF_HEADER = "x-requested-with"
CSRF_VALUE = "lite-ui"
TOKEN_TTL = timedelta(hours=12)

_logger = logging.getLogger("lite_server.webui.audit")


def role_rank(role: str) -> int:
    return ROLES.index(role)


class AuthError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


_STATUS_FOR = {"invalid": 400, "duplicate": 409, "not_found": 404, "forbidden": 403}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(10)).decode()


@dataclass
class UserRecord:
    username: str
    password_hash: str
    role: str
    created_at: str
    must_change_password: bool


def public_user(u: UserRecord) -> dict:
    return {
        "username": u.username,
        "role": u.role,
        "createdAt": u.created_at,
        "mustChangePassword": u.must_change_password,
    }


class UserStore:
    """Local account store: users in auth.yaml (bcrypt hashes), JWT secret in a
    sibling 0600 file. Bootstrap: first run with no users creates `admin` from
    LITE_UI_ADMIN_PASSWORD or a random password printed once."""

    def __init__(self, auth_path: str, secret_path: str, env: Mapping[str, str]):
        self._auth_path = str(auth_path)
        self._users: dict[str, UserRecord] = {}

        path = Path(self._auth_path)
        if path.exists():
            doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            for raw in doc.get("users") or []:
                if not isinstance(raw.get("username"), str) or not isinstance(raw.get("password_hash"), str):
                    continue
                self._users[raw["username"]] = UserRecord(
                    username=raw["username"],
                    password_hash=raw["password_hash"],
                    role=raw["role"] if raw.get("role") in ROLES else "viewer",
                    created_at=raw["created_at"] if isinstance(raw.get("created_at"), str) else _now(),
                    must_change_password=raw.get("must_change_password") is True,
                )

        if not self._users:
            from_env = env.get("LITE_UI_ADMIN_PASSWORD")
            password = from_env or secrets.token_urlsafe(9)
            self._users["admin"] = UserRecord(
                username="admin",
                password_hash=_hash(password),
                role="admin",
                created_at=_now(),
                must_change_password=True,
            )
            self._persist()
            if not from_env:
                # Printed once; the file only stores the hash.
                print(f"[lite-ui] bootstrap admin password: {password} (you must change it on first login)")

        secret_file = Path(secret_path)
        if secret_file.exists():
            self.secret = secret_file.read_text(encoding="utf-8").strip()
        else:
            self.secret = secrets.token_hex(48)
            secret_file.write_text(self.secret, encoding="utf-8")
            os.chmod(secret_path, 0o600)

    def list(self) -> list[dict]:
        return [public_user(u) for u in self._users.values()]

    def get(self, username: str) -> UserRecord | None:
        return self._users.get(username)

    def verify(self, username: str, password: str) -> UserRecord | None:
        user = self._users.get(username)
        if user is None or not isinstance(password, str):
            return None
        # bcrypt rejects passwords over 72 bytes with ValueError.
        if len(password.encode()) > 72:
            return None
        return user if bcrypt.checkpw(password.encode(), user.password_hash.encode()) else None

    @staticmethod
    def _validate_username(username) -> str:
        if not isinstance(username, str) or not USERNAME_PATTERN.match(username):
            raise AuthError("invalid", f"invalid username: {username!r}")
        return username

    @staticmethod
    def _validate_password(password) -> str:
        if not isinstance(password, str) or len(password) < 8:
            raise AuthError("invalid", "password must be at least 8 characters")
        if len(password.encode()) > 72:
            # bcrypt hard limit; it raises ValueError beyond this.
            raise AuthError("invalid", "password must be at most 72 bytes")
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
        if username in self._users:
            raise AuthError("duplicate", f'user "{username}" already exists')
        record = UserRecord(
            username=username,
            password_hash=_hash(password),
            role=role,
            created_at=_now(),
            must_change_password=True,
        )
        self._users[username] = record
        self._persist()
        return public_user(record)

    def update(self, username: str, patch: dict, actor: str) -> dict:
        existing = self._users.get(username)
        if existing is None:
            raise AuthError("not_found", f'unknown user "{username}"')
        if patch.get("role") is not None:
            role = self._validate_role(patch["role"])
            if existing.role == "admin" and role != "admin" and self._admin_count() <= 1:
                raise AuthError("forbidden", "cannot demote the last admin")
            existing.role = role
        if patch.get("password") is not None:
            existing.password_hash = _hash(self._validate_password(patch["password"]))
            existing.must_change_password = True
        self._persist()
        return public_user(existing)

    def set_password(self, username: str, password: str) -> None:
        """Self-service password change; clears the must-change flag."""
        existing = self._users.get(username)
        if existing is None:
            raise AuthError("not_found", f'unknown user "{username}"')
        existing.password_hash = _hash(self._validate_password(password))
        existing.must_change_password = False
        self._persist()

    def remove(self, username: str, actor: str) -> None:
        existing = self._users.get(username)
        if existing is None:
            raise AuthError("not_found", f'unknown user "{username}"')
        if username == actor:
            raise AuthError("forbidden", "cannot delete yourself")
        if existing.role == "admin" and self._admin_count() <= 1:
            raise AuthError("forbidden", "cannot delete the last admin")
        del self._users[username]
        self._persist()

    def _admin_count(self) -> int:
        return sum(1 for u in self._users.values() if u.role == "admin")

    def _persist(self) -> None:
        doc = {
            "users": [
                {
                    "username": u.username,
                    "password_hash": u.password_hash,
                    "role": u.role,
                    "created_at": u.created_at,
                    "must_change_password": u.must_change_password,
                }
                for u in self._users.values()
            ]
        }
        tmp = f"{self._auth_path}.tmp-{os.getpid()}"
        Path(tmp).write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self._auth_path)


def issue_token(store: UserStore, user: UserRecord) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"username": user.username, "role": user.role, "iat": now, "exp": now + TOKEN_TTL},
        store.secret,
        algorithm="HS256",
    )


def verify_token(store: UserStore, token: str) -> dict | None:
    try:
        return jwt.decode(token, store.secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        return None


async def _json_body(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


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


# Inference calls (unary/SSE) are open to viewers; everything else on the proxy
# is admin-class and needs operator+. Matched against the URL path only —
# query strings must never influence the exemption.
_INFER_RE = re.compile(r"/(infer|events)$")


def check_request(store: UserStore, enabled: bool, request: Request) -> JSONResponse | None:
    """RBAC guard, enforced at the route layer (never trusted to the frontend).

    Returns a deny response, or None when the request may proceed."""
    path = request.url.path
    if not enabled:
        request.state.user = {"username": "local", "role": "admin"}
        return None
    if not path.startswith("/api/"):
        return None  # static assets are public
    if path == "/api/auth/login" and request.method == "POST":
        # Login sets a session cookie, so it needs the same CSRF protection as
        # every other mutation (SameSite=Lax does not stop a cross-site POST
        # from planting a cookie in the response).
        if request.headers.get(CSRF_HEADER) != CSRF_VALUE:
            return JSONResponse({"error": "csrf_header_missing"}, status_code=403)
        return None

    payload = verify_token(store, request.cookies.get(COOKIE_NAME, ""))
    if payload is None:
        return JSONResponse({"error": "unauthenticated"}, status_code=401)
    request.state.user = payload

    # Roles and account deletion take effect immediately: the store, not the
    # token payload, is authoritative.
    current = store.get(payload.get("username", ""))
    if current is None:
        return JSONResponse({"error": "unauthenticated"}, status_code=401)
    role = current.role
    must_change = current.must_change_password
    is_password_flow = path in ("/api/auth/me", "/api/auth/change-password", "/api/auth/logout")
    if must_change and not is_password_flow:
        return JSONResponse({"error": "password_change_required"}, status_code=403)

    if request.method != "GET":
        # CSRF: cookie auth + custom header a cross-site form cannot send.
        if request.headers.get(CSRF_HEADER) != CSRF_VALUE:
            return JSONResponse({"error": "csrf_header_missing"}, status_code=403)
        if path.startswith("/api/auth/"):
            return None  # any authenticated user
        if path.startswith("/api/i/"):
            if _INFER_RE.search(path):
                return None
            if role_rank(role) >= role_rank("operator"):
                return None
            return JSONResponse({"error": "forbidden", "required": "operator"}, status_code=403)
        # Everything else (/api/instances, /api/users, ...) is admin territory.
        if role_rank(role) >= role_rank("admin"):
            return None
        return JSONResponse({"error": "forbidden", "required": "admin"}, status_code=403)

    # GETs: any authenticated user — except the user directory itself.
    if path.startswith("/api/users") and role_rank(role) < role_rank("admin"):
        return JSONResponse({"error": "forbidden", "required": "admin"}, status_code=403)
    return None


def create_router(store: UserStore) -> APIRouter:
    router = APIRouter()

    @router.post("/api/auth/login")
    async def login(request: Request):
        body = await _json_body(request)
        username = body.get("username")
        user = store.verify(username, body.get("password") or "") if isinstance(username, str) else None
        if user is None:
            return JSONResponse({"error": "invalid_credentials"}, status_code=401)
        response = JSONResponse({"user": public_user(user)})
        _set_session_cookie(response, request, issue_token(store, user))
        return response

    @router.post("/api/auth/logout")
    async def logout():
        response = JSONResponse({"ok": True})
        response.delete_cookie(COOKIE_NAME, path="/")
        return response

    @router.get("/api/auth/me")
    async def me(request: Request):
        payload = request.state.user
        current = store.get(payload["username"])
        if current:
            return {"user": public_user(current)}
        return {"user": {**payload, "mustChangePassword": False}}

    @router.post("/api/auth/change-password")
    async def change_password(request: Request):
        body = await _json_body(request)
        payload = request.state.user
        if store.verify(payload["username"], body.get("currentPassword") or "") is None:
            return JSONResponse({"error": "invalid_credentials"}, status_code=401)
        try:
            store.set_password(payload["username"], body.get("newPassword") or "")
        except AuthError as e:
            return _error(e)
        user = store.get(payload["username"])
        response = JSONResponse({"user": public_user(user)})
        _set_session_cookie(response, request, issue_token(store, user))
        return response

    # ---- user management (admin; enforced by the guard) ----

    @router.get("/api/users")
    async def list_users():
        return {"users": store.list()}

    @router.post("/api/users")
    async def create_user(request: Request):
        try:
            user = store.create(await _json_body(request))
        except AuthError as e:
            return _error(e)
        return JSONResponse({"user": user}, status_code=201)

    @router.put("/api/users/{name}")
    async def update_user(name: str, request: Request):
        try:
            user = store.update(name, await _json_body(request), request.state.user["username"])
        except AuthError as e:
            return _error(e)
        return {"user": user}

    @router.delete("/api/users/{name}")
    async def delete_user(name: str, request: Request):
        try:
            store.remove(name, request.state.user["username"])
        except AuthError as e:
            return _error(e)
        return {"ok": True}

    return router
