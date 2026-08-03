# 11 Logging Model

Demonstrates structured logging at every stage of the inference lifecycle.

## Run

```bash
cd examples/11_logging
# Default log level is "warn"; use "info" or "debug" to see more
python -m lite_server serve --config server.yaml --log-level info
```

## Test

```bash
curl -X POST http://localhost:8000/v2/models/logged_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42, "call_count": 1}
```

## Log Levels

| Level | What You See |
|-------|-------------|
| `warn` (default) | Warnings and errors only |
| `info` | + setup messages, per-request summaries |
| `debug` | + detailed input/output at every stage |

## What You Learn

- `self.logger` is available in every `LitAPI` method
- Use `.debug()` / `.info()` / `.warning()` / `.error()` as appropriate
- Control verbosity with `--log-level` (or the `log_level` config field)
- Log in `before_decode_request` / `after_encode_response` to capture request metadata (client IP, request ID, route)
