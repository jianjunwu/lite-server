# Migrating to 0.7.0 (Callbacks / Context Unification)

0.7.0 replaces the pre-0.7 **middleware** layer with a unified **callback**
model built around a single `RequestContext`. Custom endpoints and inference
hooks now receive one `ctx` argument instead of the old `(value, meta)` /
`(request, server)` shapes, and the builtin policies (`RequireApiKey`,
`RateLimit`, `Cors`, `LogRequests`) are declared as callback instances on a
route via `callbacks=[...]`.

This document covers the API mapping and the behavior changes / known
limitations introduced during the 0.7.0 hardening pass (R1–P3).

## What changed

- **`middleware=[...]` → `callbacks=[...]`** on `@route.get/post/...`.
- **Middleware factories → callback classes**: `require_api_key(...)` →
  `RequireApiKey(...)`, `rate_limit(...)` → `RateLimit(...)`, `cors(...)` →
  `Cors(...)`, `log_requests()` → `LogRequests()`.
- **Handler signature**: `async def handler(request, server)` →
  `def handler(ctx)` (may be `async`). The handler reads `ctx.request`,
  `ctx.meta`, `ctx.state`, `ctx.server`, and returns a dict or `Response`.
- **Data hooks** (`on_request`/`on_input`/`on_output`/`on_response`) take a
  single `ctx` and read/write `ctx.input` / `ctx.output` / `ctx.response`
  instead of the old `(value, meta)` pair.
- **Policies execute in the Rust HTTP layer** when a worker is managed by
  lite-server; the Python callback instances are declarations (and a local
  fallback for unit tests / standalone runs).
- **Loud errors**: old-shape callbacks (removed hook names, two-arg data
  hooks) and bad handler signatures now fail loudly at load time instead of
  being silently adapted.

## API mapping

| Pre-0.7 | 0.7.0 |
|---|---|
| `require_api_key(keys=[...])` | `RequireApiKey(header="X-API-Key", keys=[...])` |
| `rate_limit(requests_per_minute=N)` | `RateLimit(requests_per_minute=N, key="route"\|"ip", burst=...)` |
| `cors(allow_origins=[...])` | `Cors(allow_origins=[...])` |
| `log_requests()` | `LogRequests()` |
| `@route.get("/p", middleware=[...])` | `@route.get("/p", callbacks=[...])` |
| `async def handler(request, server):` | `def handler(ctx):` |
| `request["body"]` | `ctx.request` |
| `request["query"]` | `ctx.meta.query` |
| `request["headers"].get("X-Key")` | `ctx.meta.headers.get("X-Key")` (case-insensitive) |
| `request["method"]` / `request["route"]` / `request["request_id"]` | `ctx.meta.method` / `ctx.meta.route` / `ctx.meta.request_id` |
| `server.registry.list_models()` | `ctx.server.registry.list_loaded()` |
| `_rate_limiters` global dict | Rust global `RateLimiter` (production) / per-instance fallback (tests) |
| `config.yaml` `callbacks` needing ctor args | prefer `LitAPI.callbacks` class attribute |
| dict return carrying `status_code`/`headers` frame | unchanged (response-frame contract) |

## Example

```python
# Before
@route.get("/status", middleware=[require_api_key(keys=["s"])])
async def handler(request: dict, server):
    return {"status": "ok"}

# After
@route.get("/status", callbacks=[RequireApiKey(keys=["s"]), Cors()])
def handler(ctx):
    models = ctx.server.registry.list_loaded() if ctx.server else []
    return {"status": "ok", "models": models, "request_id": ctx.meta.request_id}
```

## Behavior changes (hardening pass)

- **Invalid JSON now returns 400.** Request-body JSON is parsed inside the
  pipeline `try`; a parse failure raises `HTTPException(400,
  code="invalid_json")` and drives `on_error`, instead of escaping as a 500
  (or hanging the continuous-batching thread). Clients that branched on the
  old 500 must now handle 400.
- **HTTPException headers reach the client.** `Retry-After` on 429/503 and
  other `HTTPException(headers=...)` values are carried on unary, batch, and
  continuous-batching error responses (previously dropped).
- **RateLimit validates its parameters at construction.** `requests_per_minute
  <= 0` (or an explicit `burst <= 0`) now raises `ValueError` instead of
  silently rejecting every request.
- **Bad endpoint handler signatures fail loudly at load.** A misconfigured
  handler no longer silently drops every decorator route while the worker
  reports ready.

## Known limitations

- **LogRequests does not log early-return / `LiteResponse` success paths.**
  When a handler short-circuits via `ctx.respond(...)` (or returns a
  `LiteResponse`), the request skips the `on_response` chain, so LogRequests'
  `on_response` does not fire — successful requests are unlogged (failed
  requests are still recorded via `on_error`). Avoid `on_request`
  early-returns if you need full success logging.
- **Streaming error frames carry no headers.** The `StreamError` wire message
  has only a `message` field; `Retry-After` and other headers from a streaming
  `HTTPException` (including rate-limit rejections) are not delivered to the
  client. Unary / batch / continuous-batching error frames are unaffected
  (they go through `SingleResponse.headers`).
- **Multi-version policy semantics.** Rate limiting is keyed by the *resolved*
  version; CORS by the *active* version. If multiple versions of one model
  declare different rate-limit parameters under a shared bucket key, hot
  reloads overwrite each other's bucket params — avoid declaring different
  rate-limit parameters across versions.
- **`Access-Control-Allow-Origin` with multiple origins is comma-joined.**
  Browsers only accept a single value or `*`; `Cors(allow_origins=[a, b])`
  produces `a, b` which browsers reject. Per-request Origin echo (true
  multi-domain whitelist) is a follow-up; in production, handle CORS at a
  reverse proxy.
- **WebSocket rate limiting with `key="ip"` degrades to a shared bucket.** The
  WS handshake has no HTTP headers, so the client IP (XFF) is unavailable and
  the limiter scope collapses to a single shared bucket for the whole model.
- **`key="ip"` trusts `x-forwarded-for`.** A direct-exposed deployment lets
  clients spoof XFF to evade rate limiting — put lite-server behind a proxy
  that scrubs/overwrites XFF. (Trusted-proxy configuration is a follow-up.)
- **Rate limiting runs before auth** (Rust layer). With `key="route"`,
  unauthenticated traffic can exhaust a route's budget (a credential-less DoS
  against legitimate users) — this is intentional queue protection; prefer
  `key="ip"` for public routes. Per-API-key quotas are out of scope for 0.7.0.
