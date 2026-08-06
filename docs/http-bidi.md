# HTTP Bidirectional Streaming

lite-server exposes two HTTP transports for bidirectional streaming, both
translated onto the same worker stream protocol the gRPC `BidiStream` RPC uses
(`on_open` / `on_chunk` / `on_close` on the model side — see
[Model Authoring](model-authoring.md)). The model and worker need no
transport-specific code.

| Transport | Endpoint | Framing | Gate |
|-----------|----------|---------|------|
| WebSocket | `GET /v2/models/{m}/stream` (+ `/versions/{v}/stream`) | WS messages (JSON + binary) | `features.streaming && features.websocket_streaming` |
| HTTP/2 | `POST /v2/models/{m}/bidi` (+ `/versions/{v}/bidi`) | LPM frames (protobuf) | `features.streaming && features.http_bidi` (default on) |

`x-sequence-id` is honored on both (worker affinity), as are auth, rate
limits, deadlines (`x-lite-timeout`), and the inference callbacks.

## WebSocket `/stream` bidi frames

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

**First-frame dispatch (0.8.3):** the frame *type* — not any header — decides
how the first frame is interpreted, because browser WebSocket clients cannot
set custom headers on the upgrade request:

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

## HTTP/2 `/bidi` (LPM framing)

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
  the worker stream opens** (0.8.3): under a JSON content type (missing
  header, or the `application/x-lite-bidi` framing type itself) malformed
  JSON → 400; a raw content type (e.g. `application/octet-stream`) skips
  validation and the bytes reach `on_open` untouched. Empty `initial_data`
  is always legal (the worker maps it to `{}`).
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

## Observability

- Streaming metrics (`record_stream_open/ttft/tbt/chunk/close`) carry the
  protocol label `http2` for this endpoint (`websocket` / `sse` / `grpc` for
  the others).
- Inference callbacks fire with protocol `http2`; `InferenceRequest` fires
  when the worker stream opens, `InferenceResponse` exactly once on the
  terminal frame.
