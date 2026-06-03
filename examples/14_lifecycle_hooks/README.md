# 14 Lifecycle Hooks

Demonstrates worker lifecycle hooks: shell commands and HTTP callbacks for `on_ready`, `on_error`, and `on_exit` events.

[中文](README_zh.md)

## Key Concept

lite-server can execute shell commands or HTTP requests when workers change state. This is useful for alerting, logging, and external monitoring integration. Environment variables like `$MODEL`, `$WORKER_ID`, `$DEVICE`, `$REASON`, `$EXIT_CODE` are available in shell command templates.

## Run

```bash
cd examples/14_lifecycle_hooks
python -m lite_server serve --config server.yaml
```

When the server starts, check the console output. You should see hook commands executing:
- `on_ready`: fires when each worker finishes `setup()`
- `on_exit`: fires when a worker stops
- `on_error`: fires when a worker crashes

## Test

```bash
# The model works normally alongside hooks
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "hello", "count": 1}

# Send more requests — count increments per call
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "world"}'
# => {"output": "world", "count": 2}
```

## What You Learn

- How to configure shell command hooks: `hooks.on_ready`, `hooks.on_error`, `hooks.on_exit`
- How to configure HTTP callback hooks: `hooks.on_ready_http`, `hooks.on_error_http`
- Available shell template variables: `$MODEL`, `$WORKER_ID`, `$DEVICE`, `$REASON`, `$EXIT_CODE`, `$EXIT_SIGNAL`
- Hooks are fire-and-forget, non-blocking
