# 15 Callbacks

A detailed tour of the Python `Callback` system: every hook point, both registration paths, rejection, early return, and per-request state.

[中文](README_zh.md)

## Key Concepts

Callbacks observe and transform the inference pipeline at four data hook points around the three model stages:

```
on_request → decode_request → on_input → predict
→ on_output → encode_response → on_response
```

Plus `on_error` (request failed) and three lifecycle hooks (`on_before_setup` / `on_after_setup` / `on_teardown`).

This example registers five callbacks plus one built-in class — see `model_repo/callbacks_demo/1/callbacks.py` and `config.yaml`:

| Callback | Hook(s) | Demonstrates |
|----------|---------|--------------|
| `ApiKeyAuth` | `on_request` | Rejecting a request: raise `UnauthorizedError` → 401 |
| `RequestTimer` | `on_request`, `on_response` | Per-request state in `ctx.state` (concurrency-safe) |
| `SimpleCache` | `on_request`, `on_output` | Early return via `ctx.respond(...)`, custom response headers |
| `JsonSchemaValidator` (built-in) | `on_input` | Declarative schema validation from config.yaml — no Python code (needs `pip install lite-server[validation]`) |
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
- In streaming mode, `on_output` / `on_response` run **once per yielded chunk**, and `on_error` runs once per failed chunk.
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

```bash
# No API key -> 401 from ApiKeyAuth.on_request
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => HTTP 401 {"error": {"type": "authentication_error", "message": "missing or invalid X-API-Key header"}}

# Invalid input (empty string) -> 400 from the built-in JsonSchemaValidator;
# the model's predict() never runs
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"input": ""}'
# => HTTP 400 {"error": {"type": "invalid_request_error", "message": "'' should be non-empty", "param": "body"}}

# Missing input field -> decode_request returns None -> also 400
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{}'
# => HTTP 400 {"error": {"type": "invalid_request_error", "message": "None is not of type 'string'", "param": "body"}}

# Valid request -> 200; [RequestTimer] logs latency per request
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"input": "hello"}'
# => HTTP 200 {"output": "hello"}

# Same request again -> cache hit: X-Cache header + "cached": true,
# predict() never runs
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"input": "hello"}'
# => HTTP 200, X-Cache: hit, {"output": "hello", "cached": true}
```

The accepted keys default to `demo-key`; override with `DEMO_API_KEYS=key1,key2`.

On shutdown (Ctrl+C) you should see `[LifecycleTracer] model unloading, teardown`.

## What You Learn

- All callback hook points and their order: `on_request` → decode → `on_input` → predict → `on_output` → encode → `on_response`, plus `on_error` and the lifecycle hooks
- Both registration paths and when to use which (`LitAPI.callbacks` with constructor args vs `callbacks:` in config.yaml, string vs single-key-map entries)
- Rejecting requests by raising `HTTPException` subclasses (401/400) from a data hook
- Validating request input declaratively with the built-in `JsonSchemaValidator`
- Short-circuiting with `ctx.respond(...)` and attaching custom response headers
- Carrying per-request data in `ctx.state`
