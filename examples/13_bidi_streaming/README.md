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

Bidirectional streaming runs over **gRPC** (port 8001) — the `/stream`
WebSocket path is server-side streaming only. The included client opens a
session and exercises all three hooks:

```bash
pip install grpcio          # only if not already installed
python test_bidi.py
```

Expected output (one frame per hook — `on_open`, each `on_chunk`, `on_close`):

```
open  : {"status": "ready", "sample_rate": 16000}
chunk : {"partial": "hello", "is_final": false}
chunk : {"partial": "hello world", "is_final": false}
chunk : {"partial": "hello world test", "is_final": false}
close : {"final": "hello world test", "is_final": true, "buffer": ["hello", "world", "test"]}
```

## What You Learn

- How to implement `BidiStreamHandler` with `on_open`, `on_chunk`, `on_close`
- How to return `BidiStreamHandler` from `bidi_stream()`
- Config pattern: `bidirectional: true` and `stream: true` in config.yaml
