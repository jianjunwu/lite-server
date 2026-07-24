# 15 Callbacks (auth, rate limit, CORS, logging)

Demonstrates the unified **callback** API on custom endpoints: authentication,
rate limiting, CORS, and request logging — composed per-route via `callbacks=[...]`.

[中文](README_zh.md)

> In 0.7.0 the "middleware" layer became **callbacks**, and every endpoint
> handler receives a single `RequestContext` (`ctx`) argument. See
> `docs/migration-0.7.md` for the full mapping.

## Key concept

A custom endpoint attaches a chain of callbacks to a route. Each callback runs
on every request to that route, in registration order. A callback may
short-circuit the request (auth rejects), mutate the context, or add response
headers.

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

## Run

```bash
cd examples/15_middleware
python -m lite_server serve --config server.yaml
```

If you have changed the Rust side, rebuild the extension first: `maturin develop`.

## Try it

```bash
# Standard inference — the loaded `protected` model (no callbacks)
curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -d '{"input": "hello"}'
# => {"output": "protected: hello"}

# Public endpoint — CORS only, no auth
curl http://localhost:8000/public
# => {"message": "this endpoint is public", "request_id": "..."}

# Protected /status — requires X-API-Key
curl http://localhost:8000/status
# => HTTP 401
#    {"error":{"type":"authentication_error","message":"missing API key","code":null,"param":"X-API-Key"}}

curl -H 'X-API-Key: secret-api-key-123' http://localhost:8000/status
# => HTTP 200
#    {"server":"lite-server","loaded_models":[{"name":"protected","version":"1"}],
#     "request_id":"...","endpoint":"status (callback-protected)"}

# CORS preflight — answered at the HTTP layer (204 + headers)
curl -i -X OPTIONS -H 'Origin: http://app.example' -H 'Access-Control-Request-Method: GET' \
  http://localhost:8000/status
# => HTTP 204   access-control-allow-origin: *

# Rate limit — the endpoint allows 10 req/min (burst 15); ~15 rapid requests, then 429
for i in $(seq 1 20); do
  curl -s -o /dev/null -w '%{http_code} ' -H 'X-API-Key: secret-api-key-123' http://localhost:8000/status
done
echo
# => 200 200 200 200 200 200 200 200 200 200 200 200 200 200 429 429 429 429 429 429
```

## What you learn

- Composing callbacks per-route via `callbacks=[...]`
- The single-`ctx` handler contract (`ctx.request`, `ctx.meta`, `ctx.server`)
- The four builtin callbacks and what each guarantees
- Policies execute in the HTTP layer: CORS on every response (including 401/429
  errors), OPTIONS preflight, and rate-limiting run before the handler is reached
