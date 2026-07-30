# 08 · Error Handling & Robustness

Exercises the framework's exception-to-HTTP mapping, per-request timeout, and
`on_error` callback — the "what happens when things go wrong" tour.

[中文](README_zh.md)

## Key Concepts

- **Framework exceptions** (`BadRequestError`, `NotFoundError`, …) map to HTTP
  status codes and produce machine-readable error bodies.
- **Unhandled exceptions** (like `RuntimeError`) become 500 Internal Server
  Error — the worker catches them so a single bad request never crashes the
  process.
- **`request_timeout`** — per-request hard deadline in seconds. 0 = disabled.
  A request that exceeds it is terminated and returns a timeout error.
- **`on_error` callback** — runs when any hook or stage raises. It is
  *exception-isolated*: a failing `on_error` is logged, never masks the
  original error. Use it to collect error telemetry.
- **Worker ejection** — after `ejection_error_threshold` consecutive errors the
  worker is ejected for `ejection_timeout` seconds, then auto-recovers. This
  prevents a poisoned worker from burning CPU on repeated failures.

## Model: `ErrorDemoAPI`

Accepts `{"input": "...", "mode": "<mode>"}`. Modes:

| Mode | Behavior | HTTP status |
|------|----------|-------------|
| `normal` | Echoes the input | 200 |
| `bad_request` | `raise BadRequestError(...)` | 400 |
| `not_found` | `raise NotFoundError(...)` | 404 |
| `server_error` | `raise RuntimeError(...)` | 500 |
| `slow` | `await asyncio.sleep(5)` — exceeds `request_timeout` of 2s | timeout error |

Invalid modes also return 400 via `BadRequestError`.

## Run

```bash
cd examples/08_error_handling
python -m lite_server serve --config server.yaml
```

## Test

```bash
# Normal — 200
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello", "mode": "normal"}' | python -m json.tool
# → {"output": "ok: hello", "timeout": "request_timeout=2.0s"}

# Bad request — 400
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "", "mode": "bad_request"}' | python -m json.tool
# → {"error": {"type": "invalid_request_error", "message": "client sent invalid data"}}

# Not found — 404
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "missing-key", "mode": "not_found"}' | python -m json.tool
# → {"error": {"type": "not_found_error", "message": "resource not found: missing-key"}}

# Server error — 500
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "boom", "mode": "server_error"}' | python -m json.tool
# → {"error": {"type": "internal_error", "message": "simulated crash processing 'boom'"}}

# Slow — times out after 2s (request_timeout)
curl -s -w "\nHTTP %{http_code}\n" -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "", "mode": "slow"}'
# → HTTP 504 or timeout error

# Invalid mode — also 400 (caught in decode_request)
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "", "mode": "unknown"}' | python -m json.tool
# → {"error": {"type": "invalid_request_error", "message": "unknown mode: 'unknown'..."}}
```

Check the server console for `[ErrorMetrics]` log lines counting each failure
type. After two consecutive `server_error` failures, the worker is ejected and
auto-recovers after 30s.

## What You Learn

- Which exception class maps to which HTTP status code
- How `request_timeout` protects against hung inferences
- How `on_error` provides error telemetry without masking the original failure
- The worker ejection → auto-recovery lifecycle
- That unhandled exceptions don't crash the worker process
