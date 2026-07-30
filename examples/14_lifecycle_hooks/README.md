# 14 Lifecycle Hooks

Demonstrates worker lifecycle hooks: shell commands and HTTP callbacks fired around the worker's life.

[中文](README_zh.md)

## Key Concepts

**Worker lifecycle hooks** (`hooks:` in config.yaml): shell commands and HTTP callbacks for `on_ready`, `on_error`, and `on_exit` events. Environment variables like `$MODEL`, `$WORKER_ID`, `$DEVICE`, `$REASON`, `$EXIT_CODE` are available in shell command templates.

For intercepting the inference request pipeline in Python (`Callback` subclasses), see [15_callbacks](../15_callbacks/).

## Run

```bash
cd examples/14_lifecycle_hooks
python -m lite_server serve --config server.yaml
```

When the server starts, check the console output. You should see:

- `on_ready`: fires when each worker finishes `setup()`
- `on_exit`: fires when a worker stops
- `on_error`: fires when a worker crashes

## Test

```bash
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
