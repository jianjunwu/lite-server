"""Ownership policy (M2): who created a model version and who owns an
in-flight chunked-upload session, enforced at the proxy before forwarding.

The BFF is the policy layer — it decides WHO may attempt an overwrite or
delete. The instance remains the mechanism layer: its own version-op lock
and 409-on-existing guard decide the actual winner under concurrency, so
a stale ownership read here can never produce a mixed on-disk state.

Recording rules:
- versioned upload / chunked complete returning 2xx → the FIRST committer
  owns the version (INSERT OR IGNORE; force-overwrite does not transfer);
- upload-session init → the initiating user owns the session id;
- version delete / session abort-or-complete 2xx → records removed;
- batch delete 2xx → records removed for the versions the instance
  reports as deleted (partial failures keep theirs).

Enforcement rules (roles still apply first — see auth.check_request):
- force=true on a versioned upload or complete → version owner or admin;
- any op on an upload session (chunk PUT, complete, GET, DELETE) →
  session owner or admin;
- version DELETE → version owner or admin;
- batch DELETE and force=true on the model-level upload (version unknown
  until the instance reads the manifest) → admin only;
- no ownership record (pre-v6 versions, direct instance writes) → admin
  only (reclaim rule).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from fastapi.responses import JSONResponse


@dataclass(frozen=True)
class Mutation:
    kind: str  # upload | model_upload | init | complete | chunk | session | delete | batch_delete
    model: str
    version: str | None
    session_id: str | None


# Inference calls are read-class for the model whitelist (they are open to
# viewers at the role layer — same regex as auth._INFER_RE).
_INFER_TAIL_RE = re.compile(r"/(infer|events)$")

# Small-JSON list endpoints whose responses are filtered for whitelist users.
_MODEL_LIST_TAILS = frozenset({"v2/models", "v2/repository/index"})


def parse_model_path(tail: str) -> str | None:
    """Model name carried by a model-scoped proxied path, or None."""
    segs = [s for s in tail.split("/") if s]
    if len(segs) >= 3 and segs[:2] == ["v2", "models"]:
        return segs[2]
    if len(segs) >= 4 and segs[:3] == ["v2", "repository", "models"]:
        return segs[3]
    return None


def is_inference_tail(tail: str) -> bool:
    return bool(_INFER_TAIL_RE.search(tail))


def is_model_list_tail(tail: str) -> bool:
    return tail.strip("/") in _MODEL_LIST_TAILS


def whitelist_active(store, inst_id: str, user: dict) -> bool:
    """Instance/global admins bypass the model whitelist entirely."""
    if user.get("role") == "admin":
        return False
    return store.has_model_grants(user.get("username", ""), inst_id)


def check_model_grant(store, inst_id: str, user: dict, model: str, *,
                      write: bool) -> JSONResponse | None:
    """Model whitelist gate, run AFTER the role guard and BEFORE ownership.
    Active (any grant row for this user on this instance): reads need a
    viewer+ grant on the target model, mutations need operator. Inactive:
    unrestricted — this keeps pre-v8 behavior."""
    if not whitelist_active(store, inst_id, user):
        return None
    grant = store.model_grant(user.get("username", ""), inst_id, model)
    if write and grant != "operator":
        return _deny("model_denied", model=model)
    if not write and grant is None:
        return _deny("model_denied", model=model)
    return None


def filter_model_list(store, inst_id: str, user: dict, payload):
    """Drop non-granted models from a {"models": [...]} list response.
    Only called when the whitelist is active for this user."""
    if not isinstance(payload, dict) or not isinstance(payload.get("models"), list):
        return payload
    username = user.get("username", "")
    payload = dict(payload)
    payload["models"] = [
        row for row in payload["models"]
        if not isinstance(row, dict)
        or store.model_grant(username, inst_id, str(row.get("name", ""))) is not None
    ]
    return payload


def parse_model_mutation(tail: str) -> Mutation | None:
    """Classify a proxied path tail as a model/version mutation, or None."""
    segs = [s for s in tail.split("/") if s]
    # v2/repository/models/{m}/versions/{v}/upload
    # v2/repository/models/{m}/versions/{v}/upload-sessions[/{sid}[/complete|files/...]]
    # v2/repository/models/{m}/upload
    if len(segs) >= 4 and segs[:3] == ["v2", "repository", "models"]:
        model = segs[3]
        if len(segs) == 5 and segs[4] == "upload":
            return Mutation("model_upload", model, None, None)
        if len(segs) >= 6 and segs[4] == "versions":
            version = segs[5]
            rest = segs[6:]
            if rest == ["upload"]:
                return Mutation("upload", model, version, None)
            if rest and rest[0] == "upload-sessions":
                if len(rest) == 1:
                    return Mutation("init", model, version, None)
                sid = rest[1]
                if len(rest) == 2:
                    return Mutation("session", model, version, sid)
                if rest[2:] == ["complete"]:
                    return Mutation("complete", model, version, sid)
                if (len(rest) == 6 and rest[2] == "files" and rest[4] == "chunks"
                        and rest[3].isdigit() and rest[5].isdigit()):
                    return Mutation("chunk", model, version, sid)
                return None
            return None
        return None
    # v2/models/{m}/versions[/{v}]
    if len(segs) >= 3 and segs[:2] == ["v2", "models"]:
        model = segs[2]
        if len(segs) == 4 and segs[3] == "versions":
            return Mutation("batch_delete", model, None, None)
        if len(segs) == 5 and segs[3] == "versions":
            return Mutation("delete", model, segs[4], None)
    return None


def _deny(reason: str, **extra) -> JSONResponse:
    return JSONResponse({"error": "forbidden", "reason": reason, **extra}, status_code=403)


def check_ownership(store, inst_id: str, user: dict, mutation: Mutation,
                    query_params, method: str = "GET") -> JSONResponse | None:
    """Ownership gate, run AFTER the role guard. Returns a deny response or
    None when the request may proceed. `store` is the UserStore.

    Mutations are classified by path shape, so the method must agree before
    any rule fires — a GET on /v2/models/{m}/versions is a read, not the
    batch delete the same path is for DELETE."""
    method_for = {"upload": "POST", "model_upload": "POST", "init": "POST",
                  "complete": "POST", "chunk": "PUT",
                  "delete": "DELETE", "batch_delete": "DELETE"}
    expected = method_for.get(mutation.kind)
    if expected is not None and method != expected:
        return None
    # "session" is deliberately unlisted: both GET (status) and DELETE
    # (abort) stay owner-gated.
    role = user.get("role", "viewer")
    if role == "admin":
        return None
    username = user.get("username", "")

    if mutation.kind == "batch_delete":
        return _deny("admin_required", detail="batch version delete is admin-only")

    if mutation.kind == "init":
        return None  # any operator may start a session (role guard already ran)

    if mutation.kind in ("chunk", "session", "complete"):
        row = store.session_owner_row(inst_id, mutation.session_id)
        if row is None or row["owner"] != username:
            return _deny("not_session_owner")
        # The URL must agree with the recorded session binding — a sid from
        # one version must not be driven through another version's URL.
        if row["model"] != mutation.model or row["version"] != mutation.version:
            return _deny("session_binding_mismatch")
        if mutation.kind == "complete" and query_params.get("force") == "true":
            return _require_version_owner(store, inst_id, mutation, username)
        return None

    if mutation.kind in ("upload", "model_upload"):
        if query_params.get("force") != "true":
            return None  # the instance 409s if the version exists (M1)
        if mutation.kind == "model_upload":
            # The version is only known after the instance reads the
            # manifest — ownership cannot be checked up front.
            return _deny("admin_required",
                         detail="force on a model-level upload is admin-only")
        return _require_version_owner(store, inst_id, mutation, username)

    if mutation.kind == "delete":
        return _require_version_owner(store, inst_id, mutation, username)

    return None


def _require_version_owner(store, inst_id: str, mutation: Mutation,
                           username: str) -> JSONResponse | None:
    owner = store.version_owner(inst_id, mutation.model, mutation.version)
    if owner is None:
        return _deny("admin_required",
                     detail="version has no ownership record; admin only")
    if owner != username:
        return _deny("not_version_owner", owner=owner)
    return None


def record_success(store, inst_id: str, user: dict, mutation: Mutation,
                   method: str, session_id: str | None = None,
                   deleted_versions: list[str] | None = None) -> None:
    """Post-2xx bookkeeping. `session_id` is set only for init and
    `deleted_versions` only for batch delete (both parsed from the instance
    response by the proxy). Model-level uploads are skipped: their version
    is only known to the instance."""
    username = user.get("username", "")
    if mutation.kind == "init" and method == "POST" and session_id:
        store.record_session_owner(inst_id, session_id, mutation.model,
                                   mutation.version, username)
    elif mutation.kind in ("upload", "complete") and method == "POST":
        store.record_version_owner(inst_id, mutation.model, mutation.version, username)
        if mutation.kind == "complete":
            store.remove_session_owner(inst_id, mutation.session_id)
    elif mutation.kind in ("delete", "session") and method == "DELETE":
        if mutation.kind == "delete":
            store.remove_version_owner(inst_id, mutation.model, mutation.version)
        else:
            store.remove_session_owner(inst_id, mutation.session_id)
    elif mutation.kind == "batch_delete" and method == "DELETE":
        # Only versions the instance actually deleted; partial failures
        # keep their ownership rows.
        for version in deleted_versions or []:
            store.remove_version_owner(inst_id, mutation.model, version)
