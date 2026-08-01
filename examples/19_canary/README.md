# 19. Canary Routing (P5-2)

Split traffic between two model versions by **weight** and pin individual
requests to a version with the `x-lite-version` header.

[中文版](README_zh.md)

## What this example shows

- `orchestration.models[].weights` — version `v1` gets 20% of requests,
  `v2` gets 80%. Versions not listed get weight 0.
- `features.canary_override: true` — honors the **`x-lite-version`** request
  header, letting a client pin itself to a specific version (e.g. for
  A/B tests or pre-release verification).
- **Default is `canary_override: false`** — the header is ignored and clients
  cannot self-pin onto a canary version. Enable it only in gray/debug
  environments (config [migration M3](../docs/migration.md)).

## Layout

```
model_repo/
  canary_echo/v1/   — old behavior (input + 1), weight 20
  canary_echo/v2/   — new behavior (input * 2), weight 80
server.yaml         — weights + canary_override enabled
```

## Running

```bash
lite-server serve --config server.yaml
```

## Verify

```bash
# 1. Traffic splits by weight (run a few times — ~20% should hit v1):
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/v2/models/canary_echo/infer \
       -H 'Content-Type: application/json' -d '{"input": 5}'
  echo
done
# => {"output": 6,  "version": "v1"}  (≈2 of 10)
# => {"output": 10, "version": "v2"}  (≈8 of 10)

# 2. Pin a request to v1 explicitly:
curl -s -X POST http://localhost:8000/v2/models/canary_echo/infer \
     -H 'Content-Type: application/json' -H 'x-lite-version: v1' \
     -d '{"input": 5}'
# => {"output": 6, "version": "v1"}

# 3. Pin to v2:
curl -s -X POST http://localhost:8000/v2/models/canary_echo/infer \
     -H 'Content-Type: application/json' -H 'x-lite-version: v2' \
     -d '{"input": 5}'
# => {"output": 10, "version": "v2"}
```

## Notes

- Weights are set in `server.yaml` and read once at startup. To change the
  split at runtime use the admin `SetRouting` RPC (see example 21).
- The same canary machinery covers gRPC (`Infer` carries a
  `x-lite-version` metadata key).
