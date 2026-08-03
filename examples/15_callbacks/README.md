# 15 Callbacks

A detailed tour of the Python `Callback` system: every hook point, both registration paths, rejection, early return, and per-request state.

[中文](README_zh.md)

## Key Concepts

Callbacks observe and transform the inference pipeline at four data hook points around the three model stages:

```
before_decode_request → decode_request → after_decode_request → predict
→ after_predict → encode_response → after_encode_response
```

Plus `on_error` (request failed) and three lifecycle hooks (`on_before_setup` / `on_after_setup` / `on_teardown`).

This example registers five callbacks plus one built-in class — see `model_repo/callbacks_demo/1/callbacks.py` and `config.yaml`:

| Callback | Hook(s) | Demonstrates |
|----------|---------|--------------|
| `ApiKeyAuth` | `before_decode_request` | Rejecting a request: raise `UnauthorizedError` → 401 |
| `RequestTimer` | `before_decode_request`, `after_encode_response` | Per-request state in `ctx.state` (concurrency-safe) |
| `SimpleCache` | `before_decode_request`, `after_predict` | Early return via `ctx.respond(...)`, custom response headers |
| `JsonSchemaValidator` (built-in) | `before_decode_request`, `after_encode_response` | Declarative schema validation from config.yaml — no Python code (needs `pip install lite-server[validation]`) |
| `ErrorMetrics` | `on_error` | The exception-isolated error hook |
| `LifecycleTracer` | setup/teardown | `on_before_setup` / `on_after_setup` / `on_teardown` |

### Two registration paths

- **`LitAPI.callbacks` class attribute** (in `model.py`): takes priority and supports constructor arguments — use it when a callback needs configuration. Runs *before* config.yaml callbacks.
- **`callbacks:` in config.yaml**: each entry is a fully-qualified class path (no-arg) or a single-key map `{path: kwargs}` with constructor arguments — the built-in `JsonSchemaValidator` here is configured declaratively with its `input_schema`. Appended after the class-attribute ones.

Here `ApiKeyAuth` is registered via the class attribute (it takes the accepted keys as a constructor argument, so auth always runs before the cache), the rest via config.yaml.

### Semantics worth knowing

- Data hooks receive a single `ctx` (`RequestContext`) and may be sync or async. They may mutate `ctx.request` / `ctx.input` / `ctx.output` / `ctx.response` in place, or return a replacement value.
- Per-request data belongs in `ctx.state` — never in `self` attributes, which are shared across concurrent requests.
- Exceptions from data hooks are **not** swallowed: raising `HTTPException` (or a subclass like `BadRequestError` / `UnauthorizedError`) rejects the request with that status and a machine-readable error body.
- `ctx.respond(body, status_code=..., headers=...)` short-circuits the pipeline — later stages and hooks are skipped.
- `on_error` and the lifecycle hooks are exception-isolated: failures are logged, never propagated.
- In streaming mode, `after_predict` / `after_encode_response` run **once per yielded chunk**, and `on_error` runs once per failed chunk.
- Log with the `logging` module, never `print()` — stdout carries the worker startup handshake, and writing to it (e.g. from `on_before_setup`) breaks worker startup.
- Auth / rate-limit / CORS for production should use the declarative `policies:` section in config.yaml — `ApiKeyAuth` here only teaches the hook mechanism.

## Run

```bash
cd examples/15_callbacks
pip install lite-server[validation]   # schema validation extra
python -m lite_server serve --config server.yaml
```

On startup you should see `[LifecycleTracer] before setup` / `setup done` as the worker loads.

## Test

The schema in `config.yaml` (`input_schema`) validates the whole request
body: `text` (required, non-empty string), `note` (required, but `null`
allowed), `max_tokens` / `temperature` (optional, bounded), `messages`
(optional list of `{role, content}` objects), and no unknown fields. The
`param` in the 400 error is a JSON Pointer to the failing location.

```bash
# No API key -> 401 from ApiKeyAuth.before_decode_request
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"text": "hello", "note": null}'
# => HTTP 401 {"error": {"type": "authentication_error", "message": "missing or invalid X-API-Key header"}}

# Missing required field (text) -> 400 from the built-in JsonSchemaValidator;
# the model's predict() never runs
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"note": null}'
# => HTTP 400 ... "'text' is a required property", "param": "body"
# (a missing-field error has an empty JSON Pointer, so param is the root)

# Empty string (text has minLength: 1)
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "", "note": null}'
# => HTTP 400 ... "'' should be non-empty", "param": "body/text"

# Number out of range (temperature has maximum: 2.0)
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "temperature": 3.0}'
# => HTTP 400 ... "3.0 is greater than the maximum of 2.0", "param": "body/temperature"

# Invalid enum inside a list item -> the pointer walks into the array
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "messages": [{"role": "admin", "content": "hi"}]}'
# => HTTP 400 ... "'admin' is not one of ['user', 'assistant', 'system']",
#    "param": "body/messages/0/role"

# Unknown field (additionalProperties: false)
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "extra": 1}'
# => HTTP 400 ... "Additional properties are not allowed ('extra' was unexpected)",
#    "param": "body"

# Valid request -> 200; note is required but may be null.
# [RequestTimer] logs latency per request
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "max_tokens": 128, "temperature": 0.7,
       "messages": [{"role": "user", "content": "hi"}]}'
# => HTTP 200 {"output": {"text": "hello", "note": null, "max_tokens": 128, ...}}

# Same request again -> cache hit: X-Cache header + "cached": true,
# predict() never runs
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "max_tokens": 128, "temperature": 0.7,
       "messages": [{"role": "user", "content": "hi"}]}'
# => HTTP 200, X-Cache: hit, {"output": {...}, "cached": true}
```

> Note: fire the failing curls above slowly — the server ejects a worker
> after consecutive rejected requests (30s backoff), during which infer
> returns a transient `503 model_not_ready`. It auto-recovers.

The accepted keys default to `demo-key`; override with `DEMO_API_KEYS=key1,key2`.

On shutdown (Ctrl+C) you should see `[LifecycleTracer] model unloading, teardown`.

## What You Learn

- All callback hook points and their order: `before_decode_request` → decode → `after_decode_request` → predict → `after_predict` → encode → `after_encode_response`, plus `on_error` and the lifecycle hooks
- Both registration paths and when to use which (`LitAPI.callbacks` with constructor args vs `callbacks:` in config.yaml, string vs single-key-map entries)
- Rejecting requests by raising `HTTPException` subclasses (401/400) from a data hook
- Validating request input declaratively with the built-in `JsonSchemaValidator`
- Short-circuiting with `ctx.respond(...)` and attaching custom response headers
- Carrying per-request data in `ctx.state`
