# 03 Streaming Output

Demonstrates server-side streaming. Each request produces multiple chunks sent via SSE or WebSocket, enabling real-time token-by-token output.

## Run

```bash
cd examples/03_streaming
python -m lite_server serve --config server.yaml
```

## Test

### SSE Streaming

```bash
curl -N -X POST http://localhost:8000/v2/models/streaming/events \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "hello world test example", "max_tokens": 4}'
```

Each chunk arrives as an SSE event:
```
data: {"token": "hello", "index": 0}
data: {"token": "world", "index": 1}
data: {"token": "test", "index": 2}
data: {"token": "example", "index": 3}
```

### WebSocket Streaming

```bash
# Install websocat: brew install websocat
echo '{"prompt": "hello world test", "max_tokens": 3}' | \
  websocat ws://localhost:8000/v2/models/streaming/stream
```

## What You Learn

- How to implement `stream_predict()` for streaming output
- How to enable streaming via `stream: true` in config.yaml
- The difference between SSE (`/events`) and WebSocket (`/stream`) endpoints
- How `predict()` serves as a non-streaming fallback

## Key Config

```yaml
stream: true
```

## Key Code

```python
def stream_predict(self, request):
    """Yield tokens one at a time."""
    words = request.get("prompt", "").split()
    for i, word in enumerate(words[:request.get("max_tokens", 5)]):
        time.sleep(0.05)  # simulate latency
        yield {"token": word, "index": i}
```
