# 10 Async Model

Demonstrates the unified async pipeline: any method may be `async def` — no separate base class is needed (since 0.7.0 `AsyncLitAPI` is gone; every model runs on the same asyncio loop).

## Run

```bash
cd examples/10_async
python -m lite_server serve --config server.yaml
```

## Test

```bash
curl -X POST http://localhost:8000/v2/models/async_echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "async_echo: hello"}
```

Send concurrent requests to see async workers handle them without blocking:

```bash
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/v2/models/async_echo/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": \"msg-$i\"}" &
done
wait
```

## What You Learn

- Subclass `LitAPI` and make `predict()` an `async def` — that's all
- `decode_request` / `encode_response` / hooks can each be sync or async — the worker adapts automatically at load time
- `setup()` always stays synchronous
- Use `asyncio.sleep` or `await` for I/O-bound operations to keep the event loop responsive
