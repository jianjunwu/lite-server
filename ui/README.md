# lite-ui

Web console for [lite-server](../README.md): multi-instance dashboard, model
lifecycle management, canary routing, and an inference playground.

Architecture: a React SPA (`ui/web/`) served by a Python BFF
(`lite_server.webui`, FastAPI) that reverse-proxies each configured
lite-server instance. The browser only talks to the BFF; lite-server itself
needs **no changes** (its admin endpoints are deliberately not browser-facing,
so direct browser access would fail CORS). The BFF ships inside the
`miraserver` wheel — no Node runtime needed at deploy time.

## Run (from the wheel)

```bash
pip install miraserver
lite-server web        # UI + API on :8600
```

Open http://localhost:8600. On first run with an empty user store, open
registration is available: the first registered user becomes `admin`, after
which registration requires an invite code (Settings → Invites). For
unattended installs, set `LITE_UI_ADMIN_PASSWORD` instead — it bootstraps an
`admin` account (password change forced on first login) and registration
stays invite-only.

Auth state lives in `auth.db` (SQLite, users/sessions/invites/audit). A
pre-existing `auth.yaml` is imported once and renamed to `auth.yaml.migrated`.
Sessions are opaque httpOnly cookies (12h); logout, password changes, and
admin kicks revoke them server-side. Login is rate-limited (account locks
after 5 failures in 15 min, source IPs throttle after 30) and every security
event lands in the audit trail (Settings → Audit). TOTP two-factor auth can
be enabled per user in Settings → Security.

## Develop

```bash
# Terminal A — BFF (API only; Vite serves the SPA in dev)
uv run lite-server web

# Terminal B — SPA dev server with HMR, proxying /api to :8600
cd ui && pnpm install && pnpm -C web dev   # http://localhost:5173
```

Point the BFF at your instances via `./instances.yaml` (see
`python/lite_server/webui/instances.example.yaml`) or `LITE_UI_INSTANCES`.

## Configuration (env)

| Var | Default | Purpose |
|---|---|---|
| `LITE_UI_PORT` / `LITE_UI_HOST` | `8600` / `0.0.0.0` | BFF listen address |
| `LITE_UI_INSTANCES_FILE` | `./instances.yaml` | Instance registry file |
| `LITE_UI_INSTANCES` | — | JSON array of extra instances (marked readonly) |
| `LITE_UI_AUTH` | `true` | Set `false` to disable login entirely (local use) |
| `LITE_UI_AUTH_DB` | `./auth.db` | SQLite auth store (users, sessions, invites, audit) |
| `LITE_UI_AUTH_FILE` | `./auth.yaml` | Legacy user file, imported once into the DB |
| `LITE_UI_ADMIN_PASSWORD` | — | Bootstrap admin password (optional; skips open registration) |
| `LITE_UI_PROXY_HEADERS` | `false` | Trust `X-Forwarded-Proto`/`-For` — only behind a trusted reverse proxy |
| `LITE_UI_AUDIT_LOG` | — | Optional rotating file sink for the audit trail |
| `LITE_UI_AUDIT_LOG_MAX_BYTES` / `LITE_UI_AUDIT_LOG_BACKUPS` | `10485760` / `5` | Audit log rotation |
| `LITE_UI_WEB_DIST` | wheel-bundled assets | SPA directory served by the BFF |

All of the above have `lite-server web` CLI flag equivalents (see
`lite-server web --help`).

## Instances

`instances.yaml`:

```yaml
instances:
  - id: local
    name: Local dev
    base_url: http://localhost:8000
    # admin_key: injected by the BFF, never sent to browsers
    # admin_key_env: READ_FROM_ENV
```

Instances can also be added/edited/deleted in the UI (Settings → Instances,
admin role); changes are written back to this file atomically.

## Roles

| Role | Capabilities |
|---|---|
| `viewer` | Read-only pages, inference playground |
| `operator` | + model lifecycle ops (load/unload/activate/routing/upload) |
| `admin` | + instance CRUD, user management |

## Tests & build

```bash
uv run pytest python/tests/webui/   # BFF contract tests (pytest)
pnpm -C ui test                     # web vitest
pnpm -C ui build                    # ui/web/dist
```

Release wheels bundle `ui/web/dist` automatically (CI builds it before
maturin runs).

Design plan: [`.claude/frontend-ui-plan.md`](../.claude/frontend-ui-plan.md) (Chinese).
