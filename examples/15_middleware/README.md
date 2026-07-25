# 15 Callbacks (auth, rate limit, CORS, logging)

Demonstrates the unified **callback** API: a model-level chain of callbacks —
authentication, rate limiting, CORS, and request logging — declared once on
the `LitAPI` class and applied to every route of the model.

[中文](README_zh.md)

> In 0.7.0 the "middleware" layer became **callbacks**, and every handler
> receives a single `RequestContext` (`ctx`) argument. See
> `docs/migration-0.7.md` for the full mapping.

## Key concept

The `callbacks` class attribute attaches a chain of callbacks to the whole
model. The chain runs on **standard inference** (`/v2/models/protected/infer`)
and on **custom `@route` handlers** (`/v2/models/protected/status`) alike —
there is no per-route chain. A callback may short-circuit the request (auth
rejects), mutate the context, or add response headers.

```python
class ProtectedAPI(LitAPI):
    callbacks = (
        RequireApiKey(header="X-API-Key", keys=VALID_KEYS),
        RateLimit(requests_per_minute=10),
        Cors(allow_origins=["*"]),
        LogRequests(),
    )
```

The four builtin callbacks:

| Callback | Purpose |
|---|---|
| `RequireApiKey(header=..., keys=[...])` | Reject (401) unless the request carries a valid API-key header |
| `RateLimit(requests_per_minute=N, key="route"\|"ip", burst=...)` | Reject (429) over the limit, with `Retry-After` |
| `Cors(allow_origins=[...])` | Attach CORS headers to every response (incl. errors) and answer OPTIONS preflight |
| `LogRequests()` | Log each request/response and errors |

Handlers receive a single `ctx` (`RequestContext`): read `ctx.request` (the
body), `ctx.meta` (`headers` / `query` / `method` / `route` / `request_id`),
`ctx.server` (server proxy), `ctx.state` (per-request dict); return a dict
(serialized as the JSON body), or call `ctx.respond(...)` / return a `Response`
for full control.

> **Scope note:** `RateLimit` and `Cors` are executed by the Rust HTTP layer
> on the inference routes; on custom `@route` handlers the Python-side hooks
> (auth, logging) run, but Rust-managed policies currently do not apply.

## Run

```bash
cd examples/15_middleware
python -m lite_server serve --config server.yaml
```

If you have changed the Rust side, rebuild the extension first: `maturin develop`.

## Try it

```bash
# Inference — requires X-API-Key
curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -d '{"input": "hello"}'
# => HTTP 401
#    {"error":{"type":"authentication_error","message":"missing API key","code":null,"param":"X-API-Key"}}

curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: secret-api-key-123' \
  -d '{"input": "hello"}'
# => {"output": "protected: hello"}

# Custom route /status — guarded by the same chain
curl http://localhost:8000/v2/models/protected/status
# => HTTP 401

curl -H 'X-API-Key: secret-api-key-123' http://localhost:8000/v2/models/protected/status
# => HTTP 200
#    {"server":"lite-server","loaded_models":[{"name":"protected","version":"1",...}],
#     "request_id":"..."}

# CORS preflight on inference — answered at the HTTP layer (204 + headers)
curl -i -X OPTIONS -H 'Origin: http://app.example' -H 'Access-Control-Request-Method: POST' \
  http://localhost:8000/v2/models/protected/infer
# => HTTP 204   access-control-allow-origin: *

# Rate limit on inference — 10 req/min (burst 15); ~15 rapid requests, then 429
for i in $(seq 1 20); do
  curl -s -o /dev/null -w '%{http_code} ' -H 'X-API-Key: secret-api-key-123' \
    -H 'Content-Type: application/json' -X POST -d '{"input":"x"}' \
    http://localhost:8000/v2/models/protected/infer
done
echo
# => 200 200 200 200 200 200 200 200 200 200 200 200 200 200 200 429 429 429 429 429
```

## What you learn

- Declaring a model-level callback chain via the `callbacks` class attribute
- The chain covers inference and custom `@route` handlers with one declaration
- The single-`ctx` handler contract (`ctx.request`, `ctx.meta`, `ctx.server`)
- The four builtin callbacks and what each guarantees
- Policies execute in the HTTP layer: CORS on every inference response
  (including 401/429 errors), OPTIONS preflight, and rate-limiting run before
  the model code is reached
