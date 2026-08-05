# HTTP Decoupled Streaming

Decoupled streams have a **model-driven** lifetime: the worker returns immediately
from `predict_decoupled` and pushes chunks asynchronously via `ResponseSender`,
explicitly calling `sender.close()` when done. The client is a pure receiver —
there is no client→server data flow after the initial request payload.

lite-server exposes two HTTP transports for decoupled streaming, both translated
onto the same worker stream protocol the gRPC `DecoupledInfer` RPC uses
(`predict_decoupled` on the model side — see
[Model Authoring](model-authoring.md)). The model and worker need no
transport-specific code.

| Transport | Endpoint | Framing | Gate |
|-----------|----------|---------|------|
| SSE | `POST /v2/models/{m}/decoupled` (+ `/versions/{v}/decoupled`) | `text/event-stream` | `features.streaming && features.sse && features.decoupled` |
| WebSocket | `GET /v2/models/{m}/decoupled-stream` (+ `/versions/{v}/decoupled-stream`) | WS messages (JSON + binary) | `features.streaming && features.websocket_streaming && features.decoupled` |

`x-sequence-id` is honored on both (worker affinity), as are auth, rate
limits, deadlines (`x-lite-timeout`), `x-lite-worker-id` (direct pin), and
the inference callbacks.

The `features.decoupled` flag defaults to `true`. Setting it to `false`
unmounts both routes at the router (404).

> **Note:** `@route` declarations named `decoupled` or `decoupled-stream` are
> shadowed by the built-in endpoints — the same tradeoff as `/bidi` and
> `/stream`.

## SSE `POST .../decoupled`

```
Request:  POST /v2/models/{m}/decoupled
          headers: authorization / x-sequence-id / x-lite-timeout / x-lite-worker-id
          body: JSON (arbitrary model payload)
Response: 200 OK, content-type: text/event-stream
          data: <chunk 1>            ← model push (String::from_utf8_lossy)
          data: <chunk 2>
          data: {"error":{...}}      ← terminal error (structured HTTPException)
          data: [DONE]               ← terminal (model close())
```

Terminal frames (Error or Done) close the response and send a **targeted**
cancel to the worker (only the worker that owns the stream — not a broadcast).

Client disconnect → `event_tx` fails → forwarder breaks → targeted cancel
(reuses the existing teardown path).

## WebSocket `GET .../decoupled-stream`

```
Handshake: GET /v2/models/{m}/decoupled-stream  → 101
           (CORS: browsers send no preflight for WS, so Origin is checked
           at upgrade — `ws_origin_allowed`)
C→S       Text {"prompt": ...}         first frame = request payload
                                       (Binary is accepted, decoded as lossy UTF-8)
S→C       Binary <chunk>               model push ×N
C→S       Text {"type":"cancel"}       optional cancel
C→S       Text {"type":"close"}        cancel alias (same behavior)
S→C       Text {"done":true}           terminal (model close())
S→C       Text {"error":...}           terminal error / protocol error
```

### Control frames (C→S after the first frame)

| C→S Frame | Server Action |
|------------|---------------|
| Text `{"type":"cancel"}` | Send targeted `build_stream_cancel` to worker → close WS normally (1000) |
| Text `{"type":"close"}` | Cancel alias — identical to `{"type":"cancel"}` |
| Binary (after first frame) | Send `{"error":"decoupled stream accepts no data frames"}` → cancel worker → close |
| Other Text | Send `{"error":"unknown control frame"}` → cancel worker → close |
| Hard disconnect | Prompt targeted cancel (gone signal, does not wait for idle timeout) |

S→C frames: **Binary** = chunk; **Text** `{"error":...}` = terminal error;
**Text** `{"done":true}` = terminal. After a terminal frame the server closes
the socket.

## Shared Semantics

- **Route**: `RequestMeta.route = "/predict"`, `Protocol::Sse` / `Protocol::WebSocket`,
  `InferenceContext.route = "/predict"` — identical to gRPC decoupled and
  coupled SSE/WS.
- **Timeout** (方案 C, zero change from coupled): overall deadline only when
  the client specifies `x-lite-timeout`; chunk-idle
  (`server.decoupled_idle_timeout_secs`, default 300s) is always on.
- **Backpressure**: forwarder `mpsc(64)` bounded channel (inherited from
  coupled forwarder).
- **Worker missing `predict_decoupled`**: worker sends Error frame → mapped
  per the existing terminal-error path (worker-side `FailedPrecondition`
  semantics).
- **Canary**: SSE/WS have no canary path (parity with coupled SSE/WS).
- **Auth / rate-limit**: identical to SSE `infer` / WS `stream` — model
  policies evaluated in the same order (auth first, then rate limit; WS
  errors go as frames, not HTTP status codes).
- **Cancel idempotency**: cancel may fire twice on the same stream (reader +
  main task); the worker ignores duplicate Cancel for unknown/terminated
  streams — safe by construction.

## Observability

- Streaming metrics (`record_stream_open/ttft/tbt/chunk/close`) reuse the
  `"sse"` / `"websocket"` label — same as coupled SSE/WS (gRPC decoupled
  also reuses `"grpc"`).
- Inference callbacks fire with `Protocol::Sse` / `Protocol::WebSocket`;
  `InferenceRequest` fires when the worker stream opens, `InferenceResponse`
  exactly once on the terminal frame.
- Long-running decoupled streams (potentially minutes) that are reclaimed by
  the idle timeout still emit `stream_close`.

## Comparison with gRPC DecoupledInfer

| Aspect | gRPC `DecoupledInfer` | HTTP SSE | HTTP WS |
|--------|----------------------|----------|---------|
| Worker method | `predict_decoupled` | same | same |
| `StreamOpen.decoupled` | `true` | `true` | `true` |
| Timeout semantics | 方案 C (overall + idle) | same | same |
| Cancel | targeted | targeted (D4) | targeted |
| Metrics label | `"grpc"` | `"sse"` | `"websocket"` |
| Feature gate | `features.grpc_streaming` | `streaming && sse && decoupled` | `streaming && websocket_streaming && decoupled` |

## Quick Examples

### SSE (curl)

```bash
curl -N -X POST http://localhost:8900/v2/models/my-model/decoupled \
  -H "Content-Type: application/json" \
  -d '{"input": 3}'
# data: {"index":0}
# data: {"index":1}
# data: {"index":2}
# data: [DONE]
```

### WebSocket (wscat)

```bash
wscat -c ws://localhost:8900/v2/models/my-model/decoupled-stream
> {"input": 3}
< {"index":0}     # Binary
< {"index":1}     # Binary
< {"index":2}     # Binary
< {"done":true}   # Text
```
