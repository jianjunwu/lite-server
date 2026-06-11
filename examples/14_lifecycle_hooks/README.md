# 14 Lifecycle Hooks

Demonstrates both worker lifecycle hooks and the new Callback system for inference request interception.

[中文](README_zh.md)

## Key Concepts

**Worker lifecycle hooks** (`hooks:` in config.yaml): shell commands and HTTP callbacks for `on_ready`, `on_error`, and `on_exit` events. Environment variables like `$MODEL`, `$WORKER_ID`, `$DEVICE`, `$REASON`, `$EXIT_CODE` are available in shell command templates.

**Inference callbacks** (`callbacks:` in config.yaml): Python `Callback` subclasses that intercept the inference request pipeline. Callbacks are composable, reusable across models, and have automatic exception isolation.

Two example callbacks are provided in the model's `callbacks.py`:
- `AuditLogger`: records request timing and logs each inference call
- `ResponseEnricher`: adds request metadata (`_meta`) to each response

Note: Callback exceptions are intentionally swallowed (exception isolation). Callbacks should transform data or produce side effects — use `LitAPI.on_request()` to reject requests.

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
- How callbacks chain with exception isolation via `callbacks:` in config.yaml
- The 9 available callback hooks: `on_before_setup`, `on_after_setup`, `on_teardown`, `on_before_decode`, `on_after_decode`, `on_before_predict`, `on_after_predict`, `on_before_encode`, `on_after_encode`
