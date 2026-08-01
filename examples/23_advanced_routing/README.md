# 23. Advanced Routing (P8-1 / P9-1)

**Sequence-sticky routing** pins a client sequence to one worker, and
**DecoupledInfer** gives models a 1:N push stream over gRPC whose lifetime
the model controls.

[中文版](README_zh.md)

## What this example shows

- `x-sequence-id` request header (P8-1) — all requests sharing a sequence id
  are routed to the **same worker** (the responses carry the worker `pid`).
  `server.sequence_ttl_secs` / `max_sequences` bound the sticky mapping.
  Absent header = routing exactly as before (least-loaded).
- `predict_decoupled(data, sender)` (P9-1) — unlike `stream_predict` (a
  generator the worker pulls from), the model gets a push `sender` and may
  **return before the stream is done**: it pushes N chunks asynchronously and
  ends with `await sender.close()`. The channel is reclaimed by the server on
  idle timeout (`decoupled_idle_timeout_secs`) or client disconnect.

## Layout

```
model_repo/
  sticky_echo/v1/
    model.py       — echoes pid per request; predict_decoupled pushes 3 chunks
    config.yaml    — 3 workers so stickiness is observable
server.yaml        — sequence_ttl_secs
```

## Running

```bash
lite-server serve --config server.yaml
```

## Verify

```bash
# 1. Sequence stickiness — 5 requests with the same x-sequence-id hit the
#    same worker (pid is constant):
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/v2/models/sticky_echo/infer \
    -H 'Content-Type: application/json' -H 'x-sequence-id: session-42' \
    -d '{"input": 1}'
  echo
done
# => {"output": {"echo": 1, "pid": 12345}} × 5   (same pid each time)

# 2. Different sequences may land on different workers:
curl -s -X POST http://localhost:8000/v2/models/sticky_echo/infer \
  -H 'Content-Type: application/json' -H 'x-sequence-id: session-1' -d '{"input": 1}'; echo
curl -s -X POST http://localhost:8000/v2/models/sticky_echo/infer \
  -H 'Content-Type: application/json' -H 'x-sequence-id: session-2' -d '{"input": 1}'; echo

# 3. DecoupledInfer over gRPC — the model pushes 3 chunks then closes
#    (see run_all.py check_23 for the Python client):
#    chunk 0 → {"chunk": 0, "echo": 1, "pid": ...}
#    chunk 1 → {"chunk": 1, ...}
#    chunk 2 → {"chunk": 2, ...}
#    final   → is_final=true (stream closed by the model)
```

## Notes

- `sequence_ttl_secs` is a soft pin: a pinned worker that becomes overloaded
  (beyond `balance_abs_threshold` / `balance_rel_threshold`) is abandoned and
  the sequence re-pins to the least-loaded worker.
- DecoupledInfer works over gRPC only; the sequence id also flows through
  `BidiStream`/`StreamInfer` for sticky streaming.
