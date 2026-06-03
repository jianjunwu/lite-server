# 13 Bidirectional Streaming

Demonstrates bidirectional streaming for real-time communication (e.g., ASR, dialogue).

[中文](README_zh.md)

## Key Concept

`bidi_stream()` returns a `BidiStreamHandler` with three hooks:
- `on_open(initial_data)` — called when stream opens, returns initial response
- `on_chunk(chunk)` — called for each incoming client message, returns optional response
- `on_close()` — called when stream closes, returns final response

## Run

```bash
cd examples/13_bidi_streaming
python -m lite_server serve --config server.yaml
```

## Test

```bash
# Bidirectional streaming via WebSocket
# Connect to the bidi stream endpoint, then send chunks interactively:
websocat ws://localhost:8000/v2/models/asr/stream

# Send messages (type in the WebSocket):
> {"text": "hello"}
< {"partial": "hello", "is_final": false}
> {"text": "world"}
< {"partial": "hello world", "is_final": false}
> {"text": "test"}
< {"partial": "hello world test", "is_final": false}
# Close the connection to trigger on_close()
< {"final": "hello world test", "is_final": true, "buffer": ["hello", "world", "test"]}
```

## What You Learn

- How to implement `BidiStreamHandler` with `on_open`, `on_chunk`, `on_close`
- How to return `BidiStreamHandler` from `bidi_stream()`
- Config pattern: `bidirectional: true` and `stream: true` in config.yaml
