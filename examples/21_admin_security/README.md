# 21. Admin API & Security (P6 / P7)

The **admin gRPC service** (11 RPCs: GetInfo, ListModels, ListVersions,
ModelReady, ModelHealth, LoadModel, UnloadModel, ReloadModel, ActivateVersion,
SetRouting, GetModelStats) runs on a **separate bind**, both admin channels
require an **API key**, and control-plane mutations emit a **structured audit
log**.

[中文版](README_zh.md)

## What this example shows

- `grpc.admin_bind: unix:./admin.sock` (P7-2) — the `LiteAdmin` service is
  not exposed on the public gRPC port; it listens on its own Unix socket
  (owner-only `0o600` by default).
- `access_control.admin` (P7-1) — API key required for both HTTP admin paths
  (`/v2/models/.../activate`, `.../routing`, …) and the gRPC admin service.
  Unconfigured admin defaults to **loopback-only** (fail-closed); key
  comparison is constant-time.
- **Audit log** (P6-2) — every admin mutation (load / unload / reload /
  activate / set_routing) writes a structured record with
  `action / model / version / request_id / client_ip / principal` to the
  `lite_server::audit` tracing target (`logging.info_output`).

## Layout

```
model_repo/
  admin_echo/1/    — echo model
server.yaml        — admin_bind UDS + access_control keys + audit log file
```

## Running

```bash
lite-server serve --config server.yaml
```

## Verify

```bash
# 1. HTTP admin without key → 401:
curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST http://localhost:8000/v2/models/admin_echo/versions/v1/activate
# => 401

# 2. HTTP admin with key → ok:
curl -s -X POST http://localhost:8000/v2/models/admin_echo/versions/v1/activate \
  -H 'x-admin-key: secret-admin-key'
# => {"success": true, ...}

# 3. gRPC admin over the UDS, without key → Unauthenticated:
grpcurl -unix -plaintext -d '{}' \
  -import-path /path/to/lite_server/proto -proto liteserver.proto \
  /tmp/.../admin.sock liteserver.Admin/GetInfo
# => ERROR: Code=Unauthenticated

# 4. gRPC admin over the UDS, with key → ok (see run_all.py check_21 for the
#    Python client; GetInfo lists loaded_models, ActivateVersion mutates).

# 5. Audit trail — after a mutation, the log file records it:
grep "admin control-plane mutation" audit.log
# => ... action=activate model=admin_echo version=Some("v1") ... client_ip=...
```

## Notes

- The inference channel is untouched: model inference on `:8000`/`:8001`
  needs no key. `access_control.inference` / `health` exist for locking those
  down too (see docs/configuration.md).
- The `metrics_port` listener is intentionally not covered by access control
  — scrape Prometheus there.
- Secrets can come from `value_env` / `value_file` instead of inline
  `value` (resolved at startup, missing source fails fast).
