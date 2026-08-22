# Streaming

lite-server exposes three streaming families over four transports (SSE,
WebSocket, HTTP/2, gRPC), all translated onto the same worker stream protocol
— the model and worker need no transport-specific code.

- **Coupled streaming** — request → token-by-token response (`stream_predict`).
  SSE `/events`, WebSocket `/stream` (one frame, legacy mode), gRPC `StreamInfer`.
- **Bidirectional (bidi) streaming** — the client keeps sending after the
  first frame (`on_open` / `on_chunk` / `on_close` on the model side — the
  gRPC `BidiStream` protocol). WebSocket `/stream`, HTTP/2 `/bidi`.
- **Decoupled streaming** — model-driven lifetime: the worker returns
  immediately from `predict_decoupled` and pushes chunks asynchronously via
  `ResponseSender`, explicitly calling `sender.close()` when done. The client
  is a pure receiver — no client→server data flow after the initial payload.
  SSE `/decoupled`, WebSocket `/decoupled-stream`, gRPC `DecoupledInfer`.

Model-side hooks (`on_open` / `on_chunk` / `on_close`, `predict_decoupled`,
`stream_predict`): see [Model Authoring](model-authoring.md).

## Transports at a glance

| Family | Transport | Endpoint | Framing | Gate |
|--------|-----------|----------|---------|------|
| Bidi | WebSocket | `GET /v2/models/{m}/stream` (+ `/versions/{v}/stream`) | WS messages (JSON + binary) | `features.streaming && features.websocket_streaming` |
| Bidi | HTTP/2 | `POST /v2/models/{m}/bidi` (+ `/versions/{v}/bidi`) | LPM frames (protobuf) | `features.streaming && features.http_bidi` (default on) |
| Decoupled | SSE | `POST /v2/models/{m}/decoupled` (+ `/versions/{v}/decoupled`) | `text/event-stream` | `features.streaming && features.sse && features.decoupled` |
| Decoupled | WebSocket | `GET /v2/models/{m}/decoupled-stream` (+ `/versions/{v}/decoupled-stream`) | WS messages (JSON + binary) | `features.streaming && features.websocket_streaming && features.decoupled` |

All four honor `x-sequence-id` (worker affinity), auth, rate limits, deadlines
(`x-lite-timeout`), and the inference callbacks. The decoupled endpoints also
honor `x-lite-worker-id` (direct pin).

`features.decoupled` defaults to `true`; setting it to `false` unmounts both
decoupled routes at the router (404).

> **Note:** `@route` declarations named `decoupled`, `decoupled-stream`,
> `bidi`, or `stream` are shadowed by the built-in endpoints.

## Bidirectional Streaming

### WebSocket `/stream` bidi frames

The endpoint is backward compatible: legacy clients that send one frame and
only read behave exactly as before. Bidi clients may keep sending after the
first frame.

| Direction | Frame | Format | Meaning |
|-----------|-------|--------|---------|
| C→S | first frame | **Text** = JSON payload; **Binary** = raw bytes — the frame type alone decides | initial input (`on_open`) |
| C→S | Data ×N | **Binary** — raw bytes | appended input (`on_chunk`) |
| C→S | Close | Text `{"type":"close"}` | end input gracefully (`on_close`); output continues |
| C→S | any other Text | — | protocol error: server sends `{"error":"unknown control frame"}`, closes, cancels the worker |
| S→C | Data ×N | Binary | worker output chunks |
| S→C | Error | Text `{"error":...}` | terminal frame |
| S→C | Done | Text `{"done":true}` | terminal frame, then the socket closes |

**First-frame dispatch:** the frame *type* — not any header — decides how the
first frame is interpreted, because browser WebSocket clients cannot set custom
headers on the upgrade request:

| First frame | Server validation | Content-Type the worker sees |
|---|---|---|
| Text | must be valid JSON (else `{"error":"invalid JSON"}` + close) | upgrade request's, unchanged |
| Binary | none (opaque bytes) | missing → `application/octet-stream` injected; a non-JSON value is kept as payload metadata; a JSON value is rewritten to `application/octet-stream` (logged as a warning) |

> **Behavior change (0.8.3):** in 0.8.2 a Binary first frame was lossy
> UTF-8-decoded and required to contain JSON text. Binary first frames are
> now opaque bytes end-to-end — **send JSON payloads as Text frames**.

A client disconnect cancels the worker promptly (no idle-timeout wait).

```
C→S  Text   {"prompt":"..."}      # first frame = initial payload
S→C  Binary <chunk 1>
C→S  Binary <extra input>         # bidi: keep sending after the first frame
S→C  Binary <chunk 2>
C→S  Text   {"type":"close"}      # graceful input end → on_close
S→C  Text   {"done":true}         # terminal frame → socket closes
```

### HTTP/2 `/bidi` (LPM framing)

Binary protobuf framing for machine-to-machine clients; reuses the gRPC
`BidiChunk` message, so one protobuf definition covers both transports.

**LPM frame** (Lite Protocol Message):

```
+--------+------------------+-----------------+
| 1B flag| 4B length (BE)   | prost BidiChunk |
|  = 0   |  = N             | N bytes         |
+--------+------------------+-----------------+
```

- `flag` must be 0 (reserved for compression); non-zero is a protocol error.
- Maximum frame: 16 MiB; oversized declarations are rejected before allocation.

**Session:**

```
POST /v2/models/asr/bidi   (h2; headers: authorization, x-sequence-id, x-lite-timeout…)
C→S  LPM(BidiChunk{open:{initial_data}})    # first frame MUST be open;
                                            # model/version/sequence_id fields are ignored —
                                            # the URL path and HTTP headers are authoritative
C→S  LPM(BidiChunk{data:{…}}) ×N
C→S  LPM(BidiChunk{close:{}})               # or just end the body (EOF → server sends on_close)
S→C  200, content-type: application/x-lite-bidi
S→C  LPM(BidiChunk{data:{…}}) ×N
S→C  LPM(BidiChunk{close:{}})               # worker Done; or LPM(BidiChunk{error:{message,error_type}}) on failure
```

- The server generates the `stream_id` (`http-bidi-<uuid>`) and echoes it on
  every downstream frame.
- First frame not `open` → 400 (the response is not yet committed, so plain
  HTTP errors work). Auth / readiness / rate-limit failures are likewise plain
  4xx/404/503.
- `open.initial_data` is validated against the request Content-Type **before
  the worker stream opens**: under a JSON content type (missing header, or the
  `application/x-lite-bidi` framing type itself) malformed JSON → 400; a raw
  content type (e.g. `application/octet-stream`) skips validation and the
  bytes reach `on_open` untouched. Empty `initial_data` is always legal (the
  worker maps it to `{}`).
- Ending the request body without a `close` frame still ends worker input
  gracefully (half-close, same semantics as gRPC).

**Requirements and limitations:**

- **h2 only.** HTTP/1.1 gets `426 Upgrade Required` — there is no downgrade.
  Use prior-knowledge h2c (`curl --http2-prior-knowledge`, or
  `reqwest::Client::builder().http2_prior_knowledge()`) or TLS with ALPN
  (the server advertises `h2, http/1.1`). Negotiated h2c upgrade is not
  supported.
- **Start streaming immediately.** The server waits for the first LPM frame
  before committing the 200 (bounded by `server.timeout`). A client that
  buffers the whole request body until the response arrives will deadlock
  into that timeout.
- **Not for browsers.** `fetch` cannot stream request bodies full-duplex;
  use the WebSocket endpoint in web apps.
- **Proxies must not buffer.** nginx's default `proxy_request_buffering on`
  destroys simultaneity — set it `off` (or connect directly).

## Decoupled Streaming

### SSE `POST .../decoupled`

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

### WebSocket `GET .../decoupled-stream`

```
Handshake: GET /v2/models/{m}/decoupled-stream  → 101
           (CORS: browsers send no preflight for WS, so Origin is checked
           at upgrade — `ws_origin_allowed`)
C→S       Text {"prompt": ...}         first frame = request payload (Text = JSON)
C→S       Binary <raw bytes>           ...or a Binary first frame = raw bytes
                                       (frame type decides, same rule as /stream)
S→C       Binary <chunk>               model push ×N
C→S       Text {"type":"cancel"}       optional cancel
C→S       Text {"type":"close"}        cancel alias (same behavior)
S→C       Text {"done":true}           terminal (model close())
S→C       Text {"error":...}           terminal error / protocol error
```

**Control frames (C→S after the first frame):**

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

### Decoupled semantics

- **Route**: `RequestMeta.route = "/predict"`, `Protocol::Sse` / `Protocol::WebSocket`,
  `InferenceContext.route = "/predict"` — identical to gRPC decoupled and
  coupled SSE/WS.
- **Timeout**: overall deadline only when the client specifies `x-lite-timeout`
  (identical to coupled streaming); chunk-idle
  (`server.decoupled_idle_timeout_secs`, default 300s) is always on.
- **Backpressure**: forwarder `mpsc(64)` bounded channel (inherited from
  coupled forwarder).
- **Worker missing `predict_decoupled`**: worker sends Error frame → mapped
  per the existing terminal-error path (worker-side `FailedPrecondition`
  semantics).
- **Canary**: bare (unversioned) requests resolve the version exactly like
  unary — `x-lite-version` pin (when `features.canary_override` is on) →
  weighted routing → active (parity with coupled SSE/WS and gRPC streaming);
  a versioned URL path skips both.
- **Auth / rate-limit**: identical to SSE `infer` / WS `stream` — model
  policies evaluated in the same order (auth first, then rate limit; WS
  errors go as frames, not HTTP status codes).
- **Cancel idempotency**: cancel may fire twice on the same stream (reader +
  main task); the worker ignores duplicate Cancel for unknown/terminated
  streams — safe by construction.

### Comparison with gRPC DecoupledInfer

| Aspect | gRPC `DecoupledInfer` | HTTP SSE | HTTP WS |
|--------|----------------------|----------|---------|
| Worker method | `predict_decoupled` | same | same |
| `StreamOpen.decoupled` | `true` | `true` | `true` |
| Timeout semantics | overall + idle | same | same |
| Cancel | targeted | targeted | targeted |
| Metrics label | `"grpc"` | `"sse"` | `"websocket"` |
| Feature gate | `features.grpc_streaming` | `streaming && sse && decoupled` | `streaming && websocket_streaming && decoupled` |

## Rolling Recycle and In-Flight Streams

Streams bypass the inference queue, so the worker lifecycle interacts with
them explicitly:

- **Budget**: streams count toward `max_requests` — one per stream open, and
  one per ensemble DAG node for ensemble streams (set
  `count_streams_toward_max_requests: false` for the legacy behavior where
  pure-streaming workers never roll-recycle).
- **Drain**: when a slot crosses its budget, the recycle drain waits for its
  in-flight streams to finish, up to `recycle_stream_drain_timeout_secs`
  (default 60s, independent of the batch drain bound). Routing keeps NEW
  streams off the recycling slot in the meantime; a direct pin
  (`x-lite-worker-id`) to a recycling slot is rejected with `503` +
  `Retry-After` / gRPC `Unavailable` + retry-after metadata — transient, so
  the same request may be retried as-is.
- **Negotiated close (grace)**: when the drain timeout elapses with streams
  still in flight, the server first sends every such stream a grace cancel
  (`reason=recycle`, `grace_ms` from `recycle_stream_grace_ms`, default
  2000). The worker flags the model's sender (`sender.closing` on decoupled
  streams) or defers the generator cancel (coupled streams), so a model that
  wraps up and closes within the window ends the stream with a normal
  `Done` — the client never sees an error. Wrap-up output keeps forwarding:
  the routes stay open for the whole window.
- **Forced eviction**: whatever survives the grace window is evicted — every
  remaining stream ends with a terminal error frame (`worker recycling:
  evicting in-flight streams`), counted by
  `liteserver_recycle_streams_evicted_total`. Set
  `recycle_stream_grace_ms: 0` to skip the negotiation and evict
  immediately. Bidi sessions are always cancelled immediately (interactive
  sessions have no self-close path).
- **Server shutdown** applies the same negotiated close once, globally:
  in-flight streams drain naturally for most of the `graceful_timeout`
  window, then every remaining stream gets a grace cancel
  (`reason=shutdown`, `grace_ms` from `server.shutdown_stream_grace_ms`,
  default 2000) just before the drain backstop. Cooperative models end with
  a normal `Done` (counted by `liteserver_shutdown_streams_closed_total`);
  survivors are evicted with a terminal error frame (`server shutting down:
  evicting in-flight streams`, counted by
  `liteserver_shutdown_streams_evicted_total`). The drain window itself is
  observable via `liteserver_draining` (gauge) and
  `liteserver_shutdown_drain_seconds` (histogram).
- **Worker death mid-stream** (recycle, health-check kill, unload, hot
  reload): never a silent EOF. The client always gets a terminal error —
  SSE `data: {"error":"worker exited mid-stream"}`, WS
  `{"error":...}` text frame, gRPC `Unavailable`, h2 bidi LPM Error frame —
  and the close is recorded in the `5xx` family (`worker_eof`), never as a
  clean `2xx` end.
- **Concurrency cap**: `max_concurrent_streams` (per version, default 0 =
  unlimited) rejects over-cap opens with `429` / `ResourceExhausted` +
  `Retry-After`.

**Client guidance**: browser `EventSource` reconnects SSE automatically. WS
and gRPC clients should treat `worker_recycling` / `worker exited
mid-stream` / `Unavailable` as retryable and reconnect with backoff — the
replacement worker is usually up within seconds.

## Observability

- Streaming metrics (`record_stream_open/ttft/tbt/chunk/close`) carry the
  protocol label per transport: `http2` (bidi), `websocket` (`/stream` +
  decoupled WS), `sse` (decoupled SSE), `grpc` — same labels as the coupled
  counterparts.
- Inference callbacks fire with the corresponding `Protocol`; `InferenceRequest`
  fires when the worker stream opens, `InferenceResponse` exactly once on the
  terminal frame.
- Long-running decoupled streams (potentially minutes) that are reclaimed by
  the idle timeout still emit `stream_close`.

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
