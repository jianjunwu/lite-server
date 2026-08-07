# Architecture

[中文版](zh/architecture.md)

lite-server is a hybrid Rust + Python inference server. The Rust core handles all I/O (HTTP, gRPC, IPC, metrics, file watching), while Python workers handle model inference.

## High-Level Architecture

```
                          ┌─────────────────────────────────┐
                          │          lite-server             │
                          │         (Rust Core)              │
                          │                                  │
  HTTP Request ──────────►│  ┌──────────┐  ┌─────────────┐  │
  gRPC Request ──────────►│  │  HTTP /   │  │  Inference   │  │
                          │  │  gRPC     │  │  Queue       │  │
                          │  │  Server   │──│  (per model  │  │
                          │  └──────────┘  │   version)   │  │
                          │                └──────┬───────┘  │
                          │                       │          │
                          │  ┌──────────┐         │          │
                          │  │  Model    │         │          │
                          │  │  Registry │         │          │
                          │  └──────────┘         │          │
                          │  ┌──────────┐         │          │
                          │  │  Metrics  │         │          │
                          │  │  + Alerts │         │          │
                          │  └──────────┘         │          │
                          └───────────────────────┼──────────┘
                                                  │
                           ZMQ / Protobuf IPC      │
                                                  ▼
                          ┌─────────────────────────────────┐
                          │       Python Workers             │
                          │                                  │
                          │  ┌──────────┐  ┌──────────┐     │
                          │  │ Worker 1 │  │ Worker 2 │ ... │
                          │  │          │  │          │     │
                          │  │ model.py │  │ model.py │     │
                          │  └──────────┘  └──────────┘     │
                          └─────────────────────────────────┘
```

## Request Lifecycle

### Single Request Path

A single inference request follows this path:

```
1. HTTP POST /v2/models/{name}/infer
        │
        ▼
2. axum HTTP handler parses request
        │
        ▼
3. Model Registry looks up active version
        │
        ▼
4. Request enqueued to InferenceQueue
   (least-loaded worker selected)
        │
        ▼
5. Worker picks up request via ZMQ
        │
        ▼
6. Python worker executes callback pipeline:
   before_decode_request() → decode_request() → after_decode_request() → predict() → after_predict() → encode_response() → after_encode_response()
        │
        ▼
7. Response sent back via ZMQ
        │
        ▼
8. Rust core returns HTTP response to client
```

### Batching Path

When `batch()` / `unbatch()` are overridden and `max_batch_size > 1`:

```
1. Multiple independent requests each go through decode_request()
        │
        ▼
2. InferenceQueue collects a batch (up to max_batch_size requests
   or batch_timeout deadline)
        │
        ▼
3. batch(decoded_inputs[]) → merged into a single batched input
        │
        ▼
4. predict(batched_input)
        │
        ▼
5. unbatch(raw_output) → split into list[per_input_output]
        │
        ▼
6. Individual encode_response() → dispatched to each request
```

### Streaming Mode

When `stream: true` and model implements `stream_predict()`:

```
1. HTTP POST /v2/models/{name}/events  (SSE)
   GET  /v2/models/{name}/stream       (WebSocket)
        │
        ▼
2. Worker calls stream_predict() → generator
        │
        ▼
3. Each yielded chunk → SSE event / WebSocket frame
        │
        ▼
4. Stream ends when generator exhausted
```

### Bidirectional Streaming

When model implements `bidi_stream()` (ASR, real-time dialogue, etc.):

```
1. WebSocket connection established
        │
        ▼
2. on_open(initial_data) → initialize session state
        │
        ▼
3. Each client message → on_chunk(chunk, ctx) → optionally return a response frame
        │
        ▼
4. Connection closed or cancelled → on_close() → cleanup resources
```

### Decoupled Streaming (P9-1)

`DecoupledInfer` (gRPC) is a 1:N stream whose channel lifetime the **model**
controls. Unlike `stream_predict` (a generator the worker *pulls* from, ending
when it exhausts), the model receives an async `sender` in
`predict_decoupled(data, sender)` and may **return before the stream is done** —
pushing N responses asynchronously (token-by-token, multiple candidates,
progress) and ending with `await sender.close()`:

```
gRPC DecoupledInfer ──► Rust opens a stream with StreamOpen.decoupled=true
        │
        ▼
Worker: predict_decoupled(data, sender) returns (channel stays open)
        │
        ▼
sender.send(chunk) × N  ──►  DecoupledResponse{is_final=false}
        │
        ▼
sender.close()  ──►  DecoupledResponse{is_final=true}  (terminal)
```

The server reclaims an open channel via `server.decoupled_idle_timeout_secs`
(default 300s; 0 = disabled) or on client disconnect (cancel propagates to the
worker, the sender is invalidated). A model without `predict_decoupled` fails
with `FailedPrecondition`.

> **Backpressure.** The Rust↔worker link is ZMQ `PAIR` with blocking sends — a
> slow worker blocks the sender rather than silently dropping (verified for P9-1).
> The in-process `mpsc(64)` bridge to the gRPC client evicts a stream on a slow
> *end-client* (load shedding) — a pre-existing property shared with all streams,
> owned by the reliability phase (P-FLOW); DecoupledInfer inherits it unchanged.

## Batching Mode

When `max_batch_size > 1`:

```
Request A ──┐
Request B ──┼──► InferenceQueue ──► Batch collects (up to N requests
Request C ──┘    or batch_timeout)  or batch_timeout)
                                      │
                                      ▼
                              predict([A, B, C])
                                      │
                                      ▼
                              Responses dispatched individually
```

### Adaptive Batching

When `adaptive_batching: true`, batch_timeout adjusts dynamically based on queue depth:
the system slides between `batch_timeout` (upper bound under high load) and `min_batch_timeout` (lower bound under low load). A traffic burst sends the current batch immediately rather than waiting for the full timeout, balancing throughput and latency.

### Continuous Batching

Models can implement `prefill()` / `step()` / `has_finished()` hooks to implement continuous batching: sequences dynamically join/leave an ongoing inference, rather than waiting for a batch to be collected. This is suited to LLMs and other variable-length generation scenarios.

## Key Components

### Rust Core (`src/`)

| Component | File(s) | Role |
|-----------|---------|------|
| HTTP Server | `http/` | axum-based HTTP server, routing, request parsing |
| gRPC Server | `grpc/` | tonic-based gRPC server |
| Inference Queue | `inference_queue.rs` | Per-model-version request queue, batch formation, worker dispatch, retry logic |
| Model Registry | `registry/` | Model/version lifecycle, hot reload, load policies |
| Worker Manager | `worker/` | Worker process spawning, health monitoring, outlier detection, lifecycle hooks |
| Worker Protocol | `worker/protocol.rs` | Rust ↔ Python worker message struct definitions |
| Transport | `transport/` | ZMQ IPC between core and workers (UDS on Unix, TCP on Windows) |
| Streaming | `streaming/` | Protobuf streaming request builders (open/chunk/close/cancel) |
| Metrics | `metrics/` | Prometheus metrics, timeline aggregation, alert engine |
| Rate Limiter | `rate_limit.rs` | Token-bucket rate limiting |
| Ensemble | `ensemble.rs` | DAG-based multi-model pipeline orchestration |
| Callback | `callback.rs` | Server and inference lifecycle callbacks (Rust trait) |
| Validation | `validation.rs` | Model name and version identifier format validation |
| Config | `config.rs` | YAML config loading, CLI override application |
| Error | `error.rs` | Unified error type definitions |
| Logging | `logging.rs` | Tracing log/span configuration |
| Proto | `proto.rs` + `proto/` | Protobuf definitions and generated code |
| Server | `server.rs` | Main server lifecycle, graceful shutdown, file watching |

### Python Package (`python/lite_server/`)

| Component | File(s) | Role |
|-----------|---------|------|
| CLI | `cli.py` | Command-line interface (serve, benchmark, init, etc.) |
| LitAPI | `api.py` | Model authoring base class with predict / batch / stream / bidi_stream hooks |
| Callbacks | `callbacks/` | Inference pipeline callbacks (before_decode_request / after_decode_request / after_predict / after_encode_response) + lifecycle hooks (before_setup / after_setup / before_teardown / after_teardown) |
| Context | `context.py` | Request context (RequestContext, RequestMeta) with request_id, client_ip, etc. |
| Pipeline | `pipeline.py` | Data pre/post-processing pipeline |
| Route | `route.py` | `@route` decorator for declaring custom HTTP routes |
| Server Proxy | `server_proxy.py` | Loopback HTTP proxy within worker (reach back into Rust core) |
| Response | `response.py` | Inference response data model |
| Exceptions | `exceptions.py` | Python-side exception definitions |
| Worker | `worker/inference.py` | Worker process that loads and runs models |
| Proto | `proto/` | Python protobuf generated code |
| Analyzer | `analyzer/` | Static model analysis (static, report) |
| Benchmark | `benchmark/` | Load testing & bidi streaming benchmarks |
| Artifact | `artifact/` | Model packing/unpacking (.lma format) |
| Init | `init/` | Project scaffolding templates |

### Python Native Extension (`python/_lite_server/`)

The Rust core compiles to a Python native extension (`_lite_server.abi3.so`). Worker processes use it for zero-copy protobuf message reads, avoiding manual deserialization overhead on the Python side. This is a key optimization on the hot path.

## Process Model

```
lite-server-core (main process)
  ├── HTTP server (tokio, multi-threaded)
  │     ├── Health probes /health /livez /readyz /startupz
  │     ├── Inference routes /v2/models/:name/infer
  │     ├── Versioned routes /v2/models/:name/versions/:version/infer
  │     └── Weighted routing PUT /v2/models/:name/routing
  ├── gRPC server (optional)
  ├── Metrics server
  ├── Rate limiter (token bucket)
  ├── Model Registry
  │     ├── Reconcile task (manages version lifecycle in auto mode)
  │     ├── File watcher (directory events trigger near-real-time reconcile)
  │     └── Inference Queue (per model version)
  └── Worker processes (subprocesses)
        ├── Worker 1 → Python interpreter → model.py
        ├── Worker 2 → Python interpreter → model.py
        └── ...
```

- Each worker is an independent Python subprocess
- Workers communicate with the core via ZMQ PAIR sockets (UDS on Unix, TCP on Windows)
- Workers are automatically restarted on crash
- `max_requests` triggers periodic restart to prevent memory leaks
- Outlier detection ejects unhealthy workers (Envoy-style consecutive error counting)
- Heartbeat probing detects stuck workers and auto-restarts them
- Worker lifecycle hooks (shell commands + HTTP callbacks): `on_ready`, `on_exit`, `on_error`
- Python Callback lifecycle hooks: `before_setup` / `after_setup` / `before_teardown` / `after_teardown` (exception-isolated, failures never propagated)
- Weighted routing enables canary deployments (multi-version traffic splitting)
- Adaptive batching adjusts batch_timeout dynamically based on queue depth

### Worker Selection & Sticky Routing (sequence_id)

By default requests are routed statelessly: unary `Infer` through the per-(model,version) queue to the **least-loaded** worker (skipping ejected ones); streaming/batch connect **directly** to a random non-ejected worker. A request may opt into **cross-request worker affinity** by carrying a `sequence_id` (HTTP header `x-sequence-id`, gRPC `InferRequest`/`StreamInferRequest`/`BidiOpen.sequence_id`):

- The server keeps a per-process `SequenceRegistry` mapping `sequence_id → (model, version, worker_id)`. A hit biases the next same-`sequence_id` request onto that worker when it is still registered and not ejected; a miss/ejection falls back to normal selection. Availability always wins over stickiness — fallback never rejects.
- For the queue path, affinity is resolved at dispatch with live load/health, so an overloaded sticky worker (load beyond `server.balance_abs_threshold` / `balance_rel_threshold`) falls back to power-of-two selection, and a worker going offline redistributes its sequences via rendezvous hashing (smooth rehash — bounded movement, no hotspot). Streaming uses core stickiness only (it has no per-worker load signal).
- Requests **without** a `sequence_id` route exactly as before — the feature is strictly opt-in.

### Envelope Hints (B3): priority / affinity_key / direct_worker_id

Unary infer (HTTP + gRPC) honors three more opt-in scheduling hints, carried as
headers (HTTP) or the proto `headers` map (gRPC):

- **`x-lite-priority: <int>`** — multi-level priority queue (P-FLOW B1); higher
  values dispatch first (ties FIFO). Absent = 0 = plain FIFO.
- **`x-lite-affinity-key: <string>`** — stateless content-affinity routing: the
  key is rendezvous-hashed onto the live workers, so the same key deterministically
  lands on the same worker without any server-side registry (smooth redistribution
  when a worker leaves). `sequence_id` is the special case and wins when both are
  present; unlike `sequence_id` it carries no cross-request registry stickiness
  and no load-threshold fallback (pure hash).
- **`x-lite-worker-id: <u32>`** — direct mode: pin the request to one worker
  index ("gateway citizen" extension — the server does not take over the
  decision). Validated at submit: an out-of-range or currently-ejected worker is
  rejected with `400` / gRPC `InvalidArgument` (a bad pin never silently
  reroutes). If the worker becomes unavailable between submit and dispatch
  (ejection race / retry exclusion), the server warns and falls back to normal
  selection — availability wins over the hint.

All hints are unauthenticated scheduling hints, not an isolation boundary (same
model as `sequence_id`). Consumed on the queue-dispatched paths (unary infer);
batch/stream/bidi dispatch directly to workers and ignore them. The former
`x-lite-expected-cost` reservation was removed unused — a capacity-aware picker
can reintroduce it additively.

> **Security & isolation.** `sequence_id` is an **unauthenticated scheduling hint, not an isolation boundary**. A client can influence where its own requests land (by guessing/reusing a sequence id) but cannot cross model or tenant boundaries — those stay enforced by access control + worker model scope. Error responses never echo internal `worker_id`/registry structure.
>
> **Multi-instance.** The `SequenceRegistry` is **per-process**: under multiple replicas the same `sequence_id` may land on different workers in different instances. Global stickiness requires upstream session affinity (e.g. a gateway sticky cookie); this server provides in-instance affinity only.

## IPC Protocol

Workers communicate with the Rust core using ZeroMQ PAIR sockets with protobuf serialization. On Unix, the transport uses `ipc://` (Unix domain sockets); on Windows, it falls back to `tcp://127.0.0.1:<port>`.

Custom `@route` handlers run inside the model worker and share this channel: an unmatched `/v2/models/<model>/<tail>` HTTP path falls through to a fallback handler, is enqueued to the model's InferenceQueue, and is dispatched to the worker like an inference request. From a route handler, `ctx.server` reaches back into the Rust core over loopback HTTP (`server_proxy.py`) for registry queries or cross-model inference.

## Data Path

### Inference Request

```
HTTP request (JSON/bytes)
    │
    ▼
Rust: parse → Bytes (zero-copy reference)
    │
    ▼
InferenceQueue: Arc<RequestMeta> (no data copy)
    │
    ▼
ZMQ: protobuf serialize → send to worker
    │
    ▼
Python: protobuf deserialize → before_decode_request() → decode_request() → after_decode_request() → [batch()] → predict() → [unbatch()] → after_predict() → encode_response() → after_encode_response()
    │
    ▼
ZMQ: protobuf serialize → send back
    │
    ▼
Rust: protobuf deserialize → HTTP response
```

The hot path avoids unnecessary data copies using `Bytes` (shared buffers) and `Arc<RequestMeta>` (shared metadata). On the Python side, the native extension `_lite_server.abi3.so` enables zero-copy protobuf reads.

### Streaming Request

```
HTTP POST /v2/models/{name}/events  (SSE)
GET  /v2/models/{name}/stream       (WebSocket)
    │
    ▼
Rust: stream_open(stream_id, data) → ZMQ
    │
    ▼
Python: stream_predict() or bidi_stream() → yield chunks
    │
    ▼
Each chunk → ZMQ → Rust → SSE event / WebSocket frame
    │
    ▼
Stream ends → stream_close(stream_id)
```

## Observability Stack

```
Prometheus ◄── /metrics endpoint
    │
    ├── QPS (requests per second)
    ├── Latency (P50/P90/P99)
    ├── Queue depth
    ├── TTFT (time to first token)
    ├── TBT (time between tokens)
    ├── Batch size
    ├── Worker ejections
    └── Active connections

Alert Engine ◄── Built-in rules
    │
    └── Anomaly detection on metric streams

Timeline ◄── Historical sampling (optional)
    │
    └── Per-model metric trends
```

## Hot Reload Flow

Version/file hot-reload is handled by three independent mechanisms:

| Mechanism | Config Location | Responsibility |
|-----------|----------------|----------------|
| `control_mode` | `server.yaml` `orchestration` | **Version** lifecycle — which versions are loaded/unloaded |
| `hot_reload` | `config.yaml` (per-model) | Whether to react to **file** changes |
| `on_file_changed` | `model.py` (model code) | **How** to handle file changes |

They form a pipeline, not alternatives:

```
control_mode                 hot_reload                on_file_changed
    │                            │                          │
    └─→ version enters registry   └─→ file matches pattern    └─→ returns non-None
        (version "ingress")           → sends FILE_CHANGED        → in-process refresh
                                      (file-change "gate")
                                                              returns None / not implemented
                                                               → fallback: restart workers
```

`control_mode` operates at version granularity; `hot_reload` operates at file granularity.
`hot_reload=true` works regardless of `control_mode` — it only cares about files inside already-loaded versions.

lite-server controls model version lifecycle through `orchestration.control_mode`:

| control_mode | Behavior |
|---|---|
| `"explicit"` (default) | Only loads models listed in `load_models`; does not watch for directory changes |
| `"auto"` | A background reconcile task periodically scans the model repo + directory events trigger near-real-time reconciles to auto-load/unload versions |

### Reconcile in auto mode

In `"auto"` mode, the reconcile task is the single authority on version lifecycle:

```
1. Directory events (new version dir / version dir removed) trigger a reconcile
        │
        ▼
2. A coalesce window (reconcile_coalesce_secs, default 2s) merges bursts into one reconcile run
        │
        ▼
3. reconcile_models():
   ├── Auto-unpack .lma artifacts (incremental, by mtime)
   ├── Scan the model repo for available versions
   ├── Compute target version set per load_policy ("all" / "latest" / "explicit")
   ├── Unload versions no longer in the target set
   ├── Load missing target versions
   └── Activate the default version (if configured)
```

### In-Process Hot Refresh for Loaded Versions

For loaded versions with `hot_reload: true`, file changes go through the FILE_CHANGED path:

```
1. File watcher detects file change inside a loaded version's directory
        │
        ▼
2. Check model configuration:
   ├── hot_reload = false → skip
   └── hot_reload = true → continue
        │
        ▼
3. If hot_reload_patterns is configured (default ["*.py"]):
   → only changes matching the pattern trigger a refresh; unmatched files are ignored
        │
        ▼
4. Cooldown check (hot_reload_cooldown_secs, default 3s):
   → repeat events within the cooldown window for the same version are ignored
        │
        ▼
5. FILE_CHANGED is sent to every worker of the version:
   → each worker calls its on_file_changed(changed_files) hook
   → a hook returning non-None = handled (e.g. hot-swap weights, no restart)
        │
        ▼
6. If ALL workers report handled: done — no restart
   Else (no hook / returns None / raises / old worker): default behavior
   → restart all workers for that model version
   → workers re-run setup() with new code
```

> **Removed in 0.7.7**: When `control_mode != "auto"`, new version directories are no longer auto-loaded — they are only logged. Switch to `control_mode: "auto"` or load explicitly via the Admin API.

## Rate Limiting

Token-bucket-based rate limiting (`rate_limit.rs`), supporting:

- Per-key buckets (by client_ip or custom key)
- Configurable RPM (requests per minute) and burst capacity
- Automatic stale-bucket cleanup

> **Client IP & trusted proxies (P-XFF).** The `client_ip` feeding `key: ip`
> buckets is cleansed fail-safe in `client_ip.rs`, anchored to the direct TCP
> peer: an **untrusted** peer's `X-Forwarded-For` / `X-Real-IP` headers are
> ignored (a client cannot forge an IP to bypass per-IP limiting). Only the
> CIDRs in `server.trusted_proxies` (empty by default) are treated as proxies
> whose forwarded chain is walked right-to-left to the first non-trusted hop.
> The same cleansing runs on the gRPC path. The access log records both the
> cleansed `client_ip` and the raw (truncated) `X-Forwarded-For` for
> attribution.

See [configuration.md](./configuration.md) for configuration examples.

## CORS (P-CORS)

A self-written `cors_middleware` (`src/http/cors.rs`) handles CORS — not
`tower-http::cors`, because a per-model policy override must be resolved from the
request path at runtime, which a statically-mounted `CorsLayer` cannot do.

- **Effective policy**: per-model `policies.cors` wins over the global
  `server.cors`; both absent → pass-through (no headers). Admin endpoints are
  skipped (not browser-facing).
- **Origin matching** is exact (after normalization: lowercase scheme/host,
  default port stripped), with opt-in subdomain wildcards (`*.example.com`).
  No reflection, no `null`, no suffix confusion — see the
  [security checklist](./cors-security-checklist.md).
- **Preflight** (`OPTIONS` + `Access-Control-Request-Method`) short-circuits with
  `204` + CORS headers only when the Origin is allowed. The middleware is mounted
  **outside** `access_control` (D21), so a preflight never triggers
  authentication (preflight carries no credentials), and inside `observability`
  so the `204` carries `x-request-id`.
- **Actual requests** get `Access-Control-Allow-Origin` (the matched origin, or
  `*`), `Access-Control-Allow-Credentials` when configured, `Vary: Origin`, and
  `Access-Control-Expose-Headers`. `credentials: true` + `*` is rejected.
- **WebSocket**: browsers send no preflight and do not enforce ACAO on a WS
  handshake, so the middleware cannot stop cross-site WS hijacking. The WS
  upgrade handler independently checks `Origin` against the same engine
  (`ws_origin_allowed`); with no CORS policy configured, WS security relies on
  `access_control` (P7-1) key auth.

## Overload Protection & Cancellation (P-FLOW)

Production services need explicit overload and cancellation semantics, or they
cascade-fail under pressure (§4.0.9). P-FLOW lands these:

- **Global in-flight cap** (`server.max_inflight`): inference requests beyond
  this concurrent count are rejected with `503` / gRPC `Unavailable` +
  `Retry-After`. **Health/admin endpoints are exempt** — probes must stay
  reachable under load. Enforced by the HTTP admission middleware (inside
  observability, classifies the path) and a guard at the top of each gRPC
  inference handler. `0` = unlimited (default, behaviour unchanged). The guard
  spans the call for unary RPCs; for SSE/WS/gRPC streaming it releases on
  stream-open (the same header-semantic as the in-flight accounting middleware).
- **Queue load shedding**: a full per-version queue returns `503` / `Unavailable`
  + `Retry-After` (HTTP header / gRPC metadata). `ResourceExhausted` is reserved
  for rate limiting (P3-1) — overload stays in the 5xx family.
- **Request size cap** (`server.max_request_body_bytes`): oversized bodies return
  `413` (HTTP) / `ResourceExhausted` (gRPC, tonic's fixed mapping). Default
  64 MiB (67,108,864); `null` = platform default (axum 2MB / tonic 4MB).
- **Multi-level priority queue** (B1): each per-version queue is a priority heap
  keyed by the request's `x-lite-priority` header (higher = dispatched first,
  ties FIFO). With the header absent (default 0) the queue is plain FIFO, so
  behaviour is unchanged. **Queue timeout REJECT** (`queue_timeout_secs` +
  `queue_timeout_action: reject`) returns `503` / gRPC `Unavailable` for a
  request that waits past the deadline; `delay` (default) leaves it to
  `request_timeout`.
- **Cancel propagation**: client disconnect on any stream → a fire-and-forget
  `Cancel` (`send_raw`) tells the worker to stop and release resources. Ensemble
  sub-steps share one cancel: a per-layer `JoinSet` means a parent cancel (client
  disconnect, total-budget timeout, or a sibling step error) **aborts every
  in-flight sub-step** rather than leaving workers computing for a dead request.
  Unary disconnect-cancel is intentionally not implemented (unary has no
  stream_id; the worker may finish an already-received request).

See [configuration.md](./configuration.md) for `max_inflight` /
`max_request_body_bytes`.

## Deadline Propagation & Timeout Status (P-DEADLINE)

A single request budget is bound end-to-end instead of letting scattered
timeouts act independently:

- **HTTP**: send `x-lite-timeout: <seconds>` (relative float, e.g. `2.5`).
- **gRPC**: send the standard `grpc-timeout` metadata key.
- Absent both, `server.timeout` is the fallback budget.

The resolved deadline travels to the worker as an absolute UNIX-ns timestamp
(`RequestMeta.deadline_unix_ns`) and the worker checks it cooperatively.
Ensemble DAGs share one parent budget (each sub-step gets parent − elapsed);
streaming enforces **two stages**: an overall deadline plus a chunk-idle
timeout. The chunk-idle timeout is **always on** (reusing
`decoupled_idle_timeout_secs`, default 300s) so a stuck stream is recovered
instead of hanging unbounded — long streams that keep producing chunks are
unaffected; the overall deadline activates only when the client specified one
(default config leaves long streams unbounded by overall deadline). Set
`decoupled_idle_timeout_secs = 0` to disable idle reclaim.

**Status when the budget expires** — read this before writing retry logic:

| Surface | Status | Meaning |
|---|---|---|
| HTTP (unary / batch / stream / ensemble) | `504 Gateway Timeout` | The server-side budget (client-specified, or `server.timeout` fallback) expired while waiting on the worker. |
| gRPC | `DEADLINE_EXCEEDED` | Same, per gRPC convention. |

**Why 504 and not 408:** `408` means "the *client* was too slow to send its
request" — here the request was fully received and the *server* exhausted its
downstream budget, which is the 504 semantic. The pre-existing
`InferenceTimeout → 504` mapping is kept deliberately (changing it would
silently break clients already alerting on 504); the blueprint's original 408
sketch was superseded during implementation. Treat 504 / `DEADLINE_EXCEEDED`
as "budget exhausted": retryable for idempotent requests with backoff, and
size `x-lite-timeout` below your own caller's budget so the deadline
propagates rather than stacks.

Distinct neighbours: queue-wait timeout REJECT → `503` / `Unavailable`
(P-FLOW); rate limiting → `429` / `RESOURCE_EXHAUSTED` (P3-1).

## Callbacks

The Python-side Callback system (`callbacks/`) injects custom logic at key points in the inference pipeline:

```
before_decode_request → [decode_request] → after_decode_request → [predict] → after_predict → [encode_response] → after_encode_response
```

**Data hooks** (`before_decode_request` / `after_decode_request` / `after_predict` / `after_encode_response`):
Receive a `RequestContext`; can mutate data, return a `Response` for early exit, or raise `HTTPException` to reject the request. Sync and async are both supported. In streaming mode, `after_predict` + `after_encode_response` fire once per chunk.

**Lifecycle hooks** (`before_setup` / `after_setup` / `before_teardown` / `after_teardown`):
Run outside the request path, exception-isolated (failures are logged, never propagated).

**Error hook** (`on_error`):
Driven when a request fails; exception-isolated, never masks the original error.

> `middleware.py` has been deprecated since 0.7.0, replaced by the Callback system. Built-in policy callbacks (RequireApiKey / RateLimit / Cors / LogRequests) were retired in 0.7.6 in favor of declarative `policies` in `config.yaml`, enforced by the Rust core.
