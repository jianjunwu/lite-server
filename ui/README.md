# lite-ui

Web console for [lite-server](../README.md): multi-instance dashboard, model
lifecycle management, canary routing, and an inference playground.

Architecture: a React SPA served by a small Node BFF (Fastify) that
reverse-proxies each configured lite-server instance. The browser only talks
to the BFF; lite-server itself needs **no changes** (its admin endpoints are
deliberately not browser-facing, so direct browser access would fail CORS).

## Run from source

```bash
cd ui
pnpm install
cp server/instances.example.yaml server/instances.yaml   # edit to taste
pnpm dev        # BFF on :8600, Vite dev server on :5173
```

Open http://localhost:5173. First login: user `admin` with the password
printed once at BFF startup (or set `LITE_UI_ADMIN_PASSWORD`); you must
change it on first login.

## Run the release tarball

```bash
tar -xzf lite-ui-<version>.tgz && cd lite-ui-<version>
npm install --omit=dev
node server-dist/index.js   # serves the SPA + API on :8600
```

## Configuration (env)

| Var | Default | Purpose |
|---|---|---|
| `LITE_UI_PORT` / `LITE_UI_HOST` | `8600` / `0.0.0.0` | BFF listen address |
| `LITE_UI_INSTANCES_FILE` | `./instances.yaml` (next to the BFF) | Instance registry file |
| `LITE_UI_INSTANCES` | — | JSON array of extra instances (marked readonly) |
| `LITE_UI_AUTH` | `true` | Set `false` to disable login entirely (local use) |
| `LITE_UI_AUTH_FILE` | `./auth.yaml` | User store file (`<file>.secret` holds the JWT key) |
| `LITE_UI_ADMIN_PASSWORD` | random (printed once) | Bootstrap admin password |
| `LITE_UI_WEB_DIST` | `../web/dist` | SPA directory served by the BFF |

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
pnpm -r test          # vitest (BFF + web)
pnpm -r build         # web/dist + server/dist
./scripts/pack.sh     # dist-release/lite-ui-<version>.tgz
```

Design plan: [`.claude/frontend-ui-plan.md`](../.claude/frontend-ui-plan.md) (Chinese).
