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
   on_request() → decode_request() → on_input() → predict() → on_output() → encode_response() → on_response()
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
| Callbacks | `callbacks/` | Inference pipeline callbacks (on_request / on_input / on_output / on_response) + lifecycle hooks (on_before_setup / on_after_setup / on_teardown) |
| Context | `context.py` | Request context (RequestContext, RequestMeta) with request_id, client_ip, etc. |
| Pipeline | `pipeline.py` | Data pre/post-processing pipeline |
| Route | `route.py` | `@route` decorator for declaring custom HTTP routes |
| Server Proxy | `server_proxy.py` | Loopback HTTP proxy within worker (reach back into Rust core) |
| Response | `response.py` | Inference response data model |
| Exceptions | `exceptions.py` | Python-side exception definitions |
| Worker | `worker/inference.py` | Worker process that loads and runs models |
| Proto | `proto/` | Python protobuf generated code |
| Analyzer | `analyzer/` | Performance analysis tools (benchmark, report, static) |
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
- Python Callback lifecycle hooks: `on_before_setup` / `on_after_setup` / `on_teardown` (exception-isolated, failures never propagated)
- Weighted routing enables canary deployments (multi-version traffic splitting)
- Adaptive batching adjusts batch_timeout dynamically based on queue depth

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
Python: protobuf deserialize → on_request() → decode_request() → on_input() → [batch()] → predict() → [unbatch()] → on_output() → encode_response() → on_response()
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

See [configuration.md](./configuration.md) for configuration examples.

## Callbacks

The Python-side Callback system (`callbacks/`) injects custom logic at key points in the inference pipeline:

```
on_request → [decode_request] → on_input → [predict] → on_output → [encode_response] → on_response
```

**Data hooks** (`on_request` / `on_input` / `on_output` / `on_response`):
Receive a `RequestContext`; can mutate data, return a `Response` for early exit, or raise `HTTPException` to reject the request. Sync and async are both supported. In streaming mode, `on_output` + `on_response` fire once per chunk.

**Lifecycle hooks** (`on_before_setup` / `on_after_setup` / `on_teardown`):
Run outside the request path, exception-isolated (failures are logged, never propagated).

**Error hook** (`on_error`):
Driven when a request fails; exception-isolated, never masks the original error.

> `middleware.py` has been deprecated since 0.7.0, replaced by the Callback system. Built-in policy callbacks (RequireApiKey / RateLimit / Cors / LogRequests) were retired in 0.7.6 in favor of declarative `policies` in `config.yaml`, enforced by the Rust core.
