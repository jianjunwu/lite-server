# 14 Lifecycle Hooks

Demonstrates both worker lifecycle hooks and the Callback system for inference request interception.

[中文](README_zh.md)

## Key Concepts

**Worker lifecycle hooks** (`hooks:` in config.yaml): shell commands and HTTP callbacks for `on_ready`, `on_error`, and `on_exit` events. Environment variables like `$MODEL`, `$WORKER_ID`, `$DEVICE`, `$REASON`, `$EXIT_CODE` are available in shell command templates.

**Inference callbacks** (`callbacks:` in config.yaml): Python `Callback` subclasses that intercept the inference request pipeline. Callbacks are composable and reusable across models.

Two example callbacks are provided in the model's `callbacks.py`:
- `AuditLogger`: records request timing and logs each inference call
- `ResponseEnricher`: adds request metadata (`_meta`) to each response

Since 0.7.0, callback data hooks receive a single `ctx` (RequestContext). Per-request data belongs in `ctx.state` (not `self` attributes, which are shared across concurrent requests). A hook can validate and reject a request by raising `HTTPException`, or short-circuit it with `ctx.respond(...)` — exceptions from data hooks are **not** swallowed. Lifecycle hooks (`on_before_setup` / `on_after_setup` / `on_teardown`) remain exception-isolated (failures are logged, never propagated).

## Run

```bash
cd examples/14_lifecycle_hooks
python -m lite_server serve --config server.yaml
```

When the server starts, check the console output. You should see:

- `on_ready`: fires when each worker finishes `setup()`
- `[AuditLogger]`: logs request metadata and inference timing for each request
- `on_exit`: fires when a worker stops
- `on_error`: fires when a worker crashes

## Test

```bash
# Each response includes _meta added by ResponseEnricher
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "hello", "count": 1, "_meta": {"request_id": "...", ...}}

# Send more requests — count increments per call
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "world"}'
# => {"output": "world", "count": 2, "_meta": {"request_id": "...", ...}}
```

## What You Learn

- How to configure shell command hooks: `hooks.on_ready`, `hooks.on_error`, `hooks.on_exit`
- How to configure HTTP callback hooks: `hooks.on_ready_http`, `hooks.on_error_http`
- Available shell template variables: `$MODEL`, `$WORKER_ID`, `$DEVICE`, `$REASON`, `$EXIT_CODE`, `$EXIT_SIGNAL`
- How to write and register `Callback` subclasses for inference pipeline interception
- The callback hook points: `on_request` → decode → `on_input` → predict → `on_output` → encode → `on_response`, plus lifecycle hooks `on_before_setup`, `on_after_setup`, `on_teardown`
