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
6. Python worker executes:
   decode_request() → predict() → encode_response()
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

Models can override `add_sequence()` / `remove_sequence()` to implement continuous batching: sequences dynamically join/leave an ongoing inference, rather than waiting for a batch to be collected. This is suited to LLMs and other variable-length generation scenarios.

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
| Async LitAPI | `api_async.py` | Native async variant of LitAPI base class |
| Callback | `callback.py` | Python-side lifecycle callbacks |
| Context | `context.py` | Request context (RequestContext) with request_id, client_ip, etc. |
| Middleware | `middleware.py` | Request/response middleware pipeline |
| Pipeline | `pipeline.py` | Data pre/post-processing pipeline |
| Route | `route.py` | `@route` decorator for declaring custom HTTP routes |
| Server Proxy | `server_proxy.py` | Loopback HTTP proxy within worker (reach back into Rust core) |
| Request | `request.py` | Inference request data model |
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
  │     ├── File watcher (hot reload)
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
Python: protobuf deserialize → decode_request() → [batch()] → predict() → [unbatch()] → encode_response()
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

```
1. File watcher detects file change in model directory
        │
        ▼
2. Debounce (1 second default)
        │
        ▼
3. Check model configuration:
   ├── hot_reload = false → skip
   └── hot_reload = true → continue
        │
        ▼
4. If hot_reload_patterns is configured:
   → only changes matching the pattern trigger reload
   → unmatched file changes are ignored
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

`on_file_changed` runs synchronously on the worker event loop (same as a
sync `predict`): heavy refresh work blocks inference for its duration, and
refreshing state while requests are in flight is the model author's
responsibility.

## Rate Limiting

Token-bucket-based rate limiting (`rate_limit.rs`), supporting:

- Per-key buckets (by client_ip or custom key)
- Configurable RPM (requests per minute) and burst capacity
- Automatic stale-bucket cleanup

See [configuration.md](./configuration.md) for configuration examples.

## Middleware

Python-side request/response middleware pipeline (`middleware.py`), executed before `decode_request()` and after `encode_response()`. Typical use cases: request logging, authentication/authorization, request/response transformation.
