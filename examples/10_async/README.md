# 10 Async Model

Demonstrates `AsyncLitAPI` for inference pipelines that involve async I/O (e.g. remote API calls, async model libraries).

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

- Subclass `AsyncLitAPI` instead of `LitAPI`
- `predict()` must be `async def`
- `decode_request` / `encode_response` / hooks can be sync or async — worker adapts automatically
- `max_batch_size` is forced to 1 (async does not support batching)
- Use `asyncio.sleep` or `await` for I/O-bound operations to keep the event loop responsive
