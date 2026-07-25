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
                          │  │  Server   │──│  (per model) │  │
                          │  └──────────┘  └──────┬───────┘  │
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

### With Batching

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

### With Streaming

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

## Key Components

### Rust Core (`src/`)

| Component | File(s) | Role |
|-----------|---------|------|
| HTTP Server | `http/` | axum-based HTTP server, routing, request parsing |
| gRPC Server | `grpc/` | tonic-based gRPC server |
| Inference Queue | `inference_queue.rs` | Per-model request queue, batch formation, worker dispatch |
| Model Registry | `registry/` | Model/version lifecycle, hot reload, load policies |
| Worker Manager | `worker/` | Worker process spawning, health monitoring, outlier detection |
| Transport | `transport/` | ZMQ IPC between core and workers (UDS on Unix, TCP on Windows) |
| Streaming | `streaming/` | Protobuf streaming request builders (open/chunk/close/cancel) |
| Metrics | `metrics/` | Prometheus metrics, timeline aggregation, alert engine |
| Ensemble | `ensemble.rs` | DAG-based multi-model pipeline orchestration |
| Callback | `callback.rs` | Server and inference lifecycle callbacks (Rust side) |
| Config | `config.rs` | YAML config loading, CLI override application |
| Server | `server.rs` | Main server lifecycle, graceful shutdown, file watching |

### Python Package (`python/`)

| Component | File(s) | Role |
|-----------|---------|------|
| CLI | `cli.py` | Command-line interface (serve, benchmark, init, etc.) |
| LitAPI | `api.py` | Enhanced model authoring interface with hooks |
| Worker | `worker/` | Worker process that loads and runs models |
| Analyzer | `analyzer/` | Performance analysis tools |
| Artifact | `artifact/` | Model packing/unpacking (.lma format) |
| Init | `init/` | Project scaffolding templates |

## Process Model

```
lite-server-core (main process)
  ├── HTTP server (tokio, multi-threaded)
  ├── gRPC server (optional)
  ├── Metrics server
  ├── Model Registry
  │     ├── Watcher thread (per model)
  │     └── Inference Queue (per model)
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

## IPC Protocol

Workers communicate with the Rust core using ZeroMQ PAIR sockets with protobuf serialization. On Unix, the transport uses `ipc://` (Unix domain sockets); on Windows, it falls back to `tcp://127.0.0.1:<port>`. Custom endpoint workers use a separate length-prefixed JSON protocol over UDS (Unix) or TCP (Windows).

## Data Path

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
Python: protobuf deserialize → decode_request() → predict() → encode_response()
    │
    ▼
ZMQ: protobuf serialize → send back
    │
    ▼
Rust: protobuf deserialize → HTTP response
```

The hot path avoids unnecessary data copies using `Bytes` (shared buffers) and `Arc<RequestMeta>` (shared metadata).

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
1. Watcher detects file change in model directory
        │
        ▼
2. Debounce (1 second default)
        │
        ▼
3. If model implements on_file_changed():
   → call on_file_changed(changed_files)
   → model handles its own reload logic
        │
        ▼
4. Else: default behavior
   → restart all workers for that model
   → workers re-run setup() with new code
```
