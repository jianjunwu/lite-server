# 17 · Configuration Templates & Environment Variables

Patterns for keeping configuration out of code — environment variables, custom
YAML fields, and per-environment server configs.

[中文](README_zh.md)

## Key Concepts

Three complementary patterns for externalising configuration:

| Pattern | Mechanism | Best for |
|---------|-----------|----------|
| `os.environ` in `setup()` | Standard Python | Secrets, backend URLs, feature flags — never in YAML |
| `self.config` custom YAML | `config.yaml` arbitrary keys | Tunables you want to change without touching code |
| `${VAR}` in auth keys | Framework-native expansion | API keys — fail-closed: unset var = load error |

Plus per-environment `server.yaml` overrides (dev vs staging vs prod).

## Model: `EnvDemoAPI`

Reads three config sources in `setup()`:

- `DEMO_BACKEND` env var (default `"cpu"`) — which backend to use
- `DEMO_LOG_VERBOSE` env var (default `"0"`) — whether to log each prediction
- `self.config["greeting"]` / `self.config["version_label"]` — custom YAML fields

Every response echoes the active config so you can see which values were picked
up without reading logs.

## Run

```bash
cd examples/17_config_templates

# Dev — local-only, port 8000, debug logging
DEMO_API_KEY=dev-secret python -m lite_server serve --config server.yaml

# Prod — port 8080 + gRPC on 9001, warn logging, bind all interfaces
DEMO_API_KEY=prod-secret DEMO_BACKEND=gpu \
  python -m lite_server serve --config server.prod.yaml
```

> **${DEMO_API_KEY} is mandatory** — the config references `${DEMO_API_KEY}` in
> `policies.auth.keys`. If the variable is unset the model **fails to load**
> (fail-closed). There is no default or fallback.

## Test

```bash
# Dev (port 8000) — local-only, auth with X-API-Key
curl -s -X POST http://127.0.0.1:8000/v2/models/env_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: dev-secret' \
  -d '{"input": "world"}' | python -m json.tool
# → {"output": "hello, world", "backend": "cpu", "version": "dev"}

# No auth header → 401
curl -s -X POST http://127.0.0.1:8000/v2/models/env_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "world"}'
# → 401 {"error": {"type": "authentication_error", ...}}

# Prod (port 8080) — bind all interfaces
curl -s -X POST http://localhost:8080/v2/models/env_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: prod-secret' \
  -d '{"input": "production"}' | python -m json.tool
# → {"output": "hello, production", "backend": "gpu", "version": "dev"}

# gRPC in prod (port 9001)
grpcurl -plaintext -H 'x-api-key: prod-secret' \
  -d '{"model_name": "env_demo", "input": {"input": "grpc"}}' \
  localhost:9001 liteserver.LiteServer/Infer
```

## What You Learn

- Three config layers: env vars (secrets), YAML (tunables), CLI --config (env)
- `${VAR}` expansion in `policies.auth.keys` — fail-closed by design
- Structuring per-environment `server.yaml` overrides
- How to verify which config values are active via the model output
