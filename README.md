# lite-server

High-performance model inference server — Rust core for I/O, Python for inference.

[中文文档](README_zh.md)

## Table of Contents

- [lite-server](#lite-server)
  - [Table of Contents](#table-of-contents)
  - [Why lite-server?](#why-lite-server)
  - [Quick Start](#quick-start)
  - [Features](#features)
    - [Inference Modes](#inference-modes)
    - [Model Management](#model-management)
    - [Custom Routes](#custom-routes)
    - [Worker Resilience](#worker-resilience)
    - [Traffic & Reliability](#traffic--reliability)
    - [Security](#security)
    - [Observability](#observability)
    - [Operations](#operations)
  - [Installation](#installation)
    - [From Wheel (Recommended)](#from-wheel-recommended)
    - [From Source](#from-source)
  - [Examples](#examples)
  - [API Endpoints](#api-endpoints)
  - [Configuration](#configuration)
  - [Documentation](#documentation)
  - [FAQ](#faq)
  - [Multi-Platform](#multi-platform)
  - [Development](#development)
    - [Project Structure](#project-structure)
  - [License](#license)


## Why lite-server?

| | Feature | What it means for you |
|---|---------|----------------------|
| **Fast** | Rust HTTP core (axum/tokio), zero-copy data path, adaptive batching | Higher throughput, lower latency than pure-Python servers |
| **Stable** | Outlier detection, heartbeat, auto-respawn, lifecycle hooks | Self-healing workers that detect hangs and restart automatically |
| **Simple** | `pip install`, write one `model.py`, done | No Docker, no Java, no C++ build tools |
| **Flexible** | Hot reload, multi-version, ensemble DAG | A/B testing, canary deploys, multi-model pipelines in one server |
| **Observable** | Prometheus metrics, timeline, alerts | Know what's happening in production without guessing |
| **Lightweight** | Single binary + Python workers, cross-platform | Runs on a laptop, scales on a server |

## Quick Start

```bash
# 1. Install
pip install miraserver

# 2. Scaffold a project
python -m lite_server init my_project --template empty
cd my_project

# 3. Serve
python -m lite_server serve --config server.yaml

# 4. Test
python test_request.py
# or manually:
curl -X POST http://localhost:8000/v2/models/my_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}
```

Currently only the `empty` template is available. Use `--wizard` for interactive project setup.

## Features

### Inference Modes

- **Standard batching** — group requests into batches for GPU efficiency
- **Continuous batching** — for LLM workloads with prefill/step/has_finished hooks
- **Streaming** — token-by-token output via SSE, WebSocket, or gRPC
- **Decoupled streaming** — 1:N push streams over gRPC (`DecoupledInfer`) whose lifetime the model controls (`predict_decoupled`)
- **Ensemble** — DAG-based multi-model pipelines with parallel execution, over HTTP and gRPC alike

### Model Management

- **Triton-style repository** — `model_name/version/model.py` directory structure
- **Hot reload** — modify model.py, server picks up changes automatically
- **Multi-version** — load, unload, activate, deactivate versions independently
- **Canary routing** — per-version traffic weights + `x-lite-version` request pinning (`canary_override`)
- **Load policies** — `explicit`, `latest`, `all` for version management
- **Model warmup** — dummy inferences run before a version reports Ready (`policies.warmup`), with a `WarmingUp` state machine
- **Model packing** — `.lma` format with SHA256 + HMAC signature
- **Model upload/download** — upload `.lma` artifacts or raw files via HTTP API, with auto-load support

### Custom Routes

- **Decorator routing** — `@route.get("/status")` on `LitAPI` methods, served under `/v2/models/<model>/<tail>`
- **Same channel as inference** — route handlers run in the model worker; no separate process
- **Server context access** — `ctx.server.registry` and cross-model `ctx.server.inference.infer()` from handlers
- **Callback chain** — model-level callbacks (auth, rate limiting, CORS, logging) cover inference and custom routes alike (Rust-side rate limiting/CORS middleware does not yet cover custom routes)

### Worker Resilience

- **Outlier detection** — consecutive errors trigger automatic worker ejection (Envoy-style)
- **Request retry** — failed requests retry on a different worker (up to 3 attempts)
- **Least-loaded routing** — requests go to the worker with fewest inflight tasks
- **Max-requests recycle** — auto-restart workers after N requests, with jitter to prevent thundering herd
- **Heartbeat detection** — periodic ZMQ probes detect hung workers, auto-kill and respawn
- **Lifecycle hooks** — shell commands and HTTP callbacks on worker ready/exit/error for alerting and observability
- **Per-request timeout** — hard timeout prevents stuck requests from blocking the queue

### Traffic & Reliability

- **Overload protection** — global `max_inflight` cap rejects excess inference with `503 + Retry-After` (health/admin stay reachable)
- **Priority queue** — `x-lite-priority` header (higher = dispatched first) and per-model `queue_timeout` with `reject` action
- **Per-request deadlines** — `x-lite-timeout` (HTTP) / `grpc-timeout` bound the wait; expiry returns `504`, propagated across ensemble DAGs
- **Sequence-sticky routing** — `x-sequence-id` pins a client sequence to one worker (`sequence_ttl_secs` / `max_sequences`; soft pin with load-balancing thresholds)

### Security

- **TLS / mTLS** — rustls-based TLS on HTTP and gRPC, client-certificate mTLS, live certificate hot-rotation (file poll + SIGHUP)
- **Endpoint access control** — per-class (admin / inference / health × http / grpc) API-key or loopback-only policies; admin is fail-closed by default; constant-time key comparison
- **Trusted-proxy client IP** — `trusted_proxies` cleansing of `X-Forwarded-For` / `X-Real-IP`; fail-safe default (headers ignored) prevents forged-IP rate-limit bypass
- **CORS + WebSocket Origin gate** — global or per-model CORS (exact-origin matching, `Vary: Origin`); WS handshakes checked at upgrade (403 on mismatched Origin)
- **Admin API auth** — separate admin bind (`grpc.admin_bind`, e.g. UDS), API-key gating, structured audit log for every control-plane mutation

### Observability

- **Prometheus metrics** — QPS, P50/P90/P99 latency, queue depth, TTFT, batch size, worker ejections
- **Custom metrics** — gauge, counter, histogram from model code via `register_metric()` / `report_metric()`
- **OpenTelemetry** — opt-in OTLP/gRPC traces + metrics SDK (cargo `telemetry` feature + `telemetry.enabled`), W3C traceparent bridging to workers
- **Timeline** — historical metric sampling per model
- **Alerts** — built-in alert rules for anomaly detection
- **Structured logging** — tracing-based logs with model/worker context; `lite_server::audit` target for control-plane mutations

### Operations

- **Admin gRPC service** — 11 RPCs (GetInfo, ListModels, Load/Unload/Reload, ActivateVersion, SetRouting, GetModelStats, …) on a separate bind
- **Unix-domain sockets** — HTTP (`server.host: unix:...`) and gRPC (`grpc.host` / `grpc.admin_bind`) with `socket_mode` control
- **KEDA / autoscaler integration** — vLLM-compatible metric namespace (`{ns}:total_queued_requests`, `kv_cache_utilization`) + ScaledObject recipe
- **Graceful shutdown** — drain in-flight requests, 503-drain gate, force-flushed telemetry with a capped drain window

## Installation

### From Wheel (Recommended)

Pre-built wheels for Linux (x86_64 + aarch64), macOS (Apple Silicon), and Windows (x86_64), Python 3.10-3.14:

```bash
pip install miraserver-<version>-cp310-abi3-<platform>.whl
```

### From Source

Requires a recent stable Rust toolchain (CI builds on `stable`) and Python >= 3.10.

```bash
pip install maturin
maturin develop          # dev build
maturin build --release  # release wheel
```

## Web Console (lite-ui)

A web console for lite-server lives in [`ui/`](ui/): multi-instance dashboard,
model lifecycle management (load/unload/activate/canary routing), and an
inference playground. It runs as a standalone Node service that proxies each
lite-server instance — no changes to the server needed. See
[ui/README.md](ui/README.md).

## Examples

See [examples/](examples/) for runnable model repositories:

| # | Example | Description |
|---|---------|-------------|
| 01 | [basic](examples/01_basic/) | Minimal echo model |
| 02 | [batching](examples/02_batching/) | Request batching with adaptive timeout |
| 03 | [streaming](examples/03_streaming/) | Token-by-token streaming via SSE/WebSocket |
| 04 | [multi_version](examples/04_multi_version/) | Two versions with activation switching |
| 05 | [ensemble](examples/05_ensemble/) | DAG-based multi-model pipeline |
| 06 | [custom_route](examples/06_custom_route/) | Custom HTTP routes with `@route` decorator |
| 07 | [custom_params](examples/07_custom_params/) | Config-driven model behavior |
| 08 | [error_handling](examples/08_error_handling/) | Exception-to-HTTP mapping, request timeout, worker ejection |
| 09 | [custom_metrics](examples/09_custom_metrics/) | Custom Prometheus metrics (gauge/counter/histogram) |
| 10 | [async](examples/10_async/) | Asynchronous inference (unified async pipeline) |
| 11 | [logging](examples/11_logging/) | Structured logging at every stage |
| 12 | [continuous_batching](examples/12_continuous_batching/) | LLM continuous batching (prefill/step/has_finished) |
| 13 | [bidi_streaming](examples/13_bidi_streaming/) | Bidirectional streaming for ASR |
| 14 | [lifecycle_hooks](examples/14_lifecycle_hooks/) | Worker lifecycle hooks (shell + HTTP callbacks) |
| 15 | [callbacks](examples/15_callbacks/) | Python callback pipeline (auth, cache, validation, error metrics) |
| 16 | [grpc](examples/16_grpc/) | gRPC inference endpoints (incl. ensemble DAGs) |
| 17 | [config_templates](examples/17_config_templates/) | Config templates, env vars, multi-env server.yaml |
| 18 | [tls_mtls](examples/18_tls_mtls/) | TLS/mTLS + live certificate rotation |
| 19 | [canary](examples/19_canary/) | Canary traffic weights + `x-lite-version` pinning |
| 20 | [overload_control](examples/20_overload_control/) | max_inflight, queue timeouts, priorities, deadlines |
| 21 | [admin_security](examples/21_admin_security/) | Admin gRPC on its own UDS, access control, audit log |
| 22 | [warmup](examples/22_warmup/) | Model warmup + readiness state machine |
| 23 | [advanced_routing](examples/23_advanced_routing/) | sequence_id stickiness + DecoupledInfer 1:N |
| 24 | [proxy_security](examples/24_proxy_security/) | Trusted-proxy client IP, CORS, WebSocket Origin gate |

See [examples/README.md](examples/README.md) for learning path and usage details.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v2/models/{name}/infer` | Inference (active version) |
| POST | `/v2/models/{name}/versions/{v}/infer` | Inference (specific version) |
| POST | `/v2/models/{name}/events` | SSE streaming |
| POST | `/v2/models/{name}/versions/{v}/events` | SSE streaming (specific version) |
| GET | `/v2/models/{name}/stream` | WebSocket streaming |
| GET | `/v2/models/{name}/versions/{v}/stream` | WebSocket streaming (specific version) |
| POST | `/v2/models/{name}/bidi` | HTTP/2 bidirectional streaming (LPM frames; h2 only) |
| POST | `/v2/models/{name}/versions/{v}/bidi` | HTTP/2 bidirectional streaming (specific version) |
| POST | `/v2/models/{name}/decoupled` | SSE decoupled streaming (model-driven) |
| POST | `/v2/models/{name}/versions/{v}/decoupled` | SSE decoupled streaming (specific version) |
| GET | `/v2/models/{name}/decoupled-stream` | WebSocket decoupled streaming (model-driven) |
| GET | `/v2/models/{name}/versions/{v}/decoupled-stream` | WebSocket decoupled streaming (specific version) |
| GET | `/v2/models` | List loaded models |
| GET | `/v2/models/{name}/versions` | Multi-version overview (status / active / weight / workers / loaded_at) |
| GET | `/v2/models/{name}/ready` | Readiness check (active version) |
| GET | `/v2/models/{name}/versions/{v}/ready` | Readiness check (specific version) |
| GET | `/v2/models/{name}/health` | Per-worker health status (routed version) |
| GET | `/v2/models/{name}/versions/{v}/health` | Per-worker health status (specific version) |
| GET | `/v2/models/{name}/compare` | Compare model versions |
| DELETE | `/v2/models/{name}/versions/{v}` | Delete model version |
| POST | `/v2/repository/models/{name}/versions/{v}/load` | Load model version |
| POST | `/v2/repository/models/{name}/unload` | Unload active version |
| POST | `/v2/repository/models/{name}/versions/{v}/unload` | Unload specific version |
| POST | `/v2/repository/index` | Index model repository |
| POST | `/v2/repository/models/{name}/versions/{v}/upload` | Upload model files (.lma or raw) |
| GET | `/v2/repository/models/{name}/versions/{v}/download` | Download model files |
| GET | `/v2/repository/models/{name}/versions/{v}/files` | List version directory contents |
| POST | `/v2/models/{name}/reload` | Hot reload (active version) |
| POST | `/v2/models/{name}/versions/{v}/reload` | Hot reload (specific version) |
| POST | `/v2/models/{name}/versions/{v}/activate` | Activate version (hard cutover) |
| PUT | `/v2/models/{name}/routing` | Set traffic weights atomically (`{"weights":{"v1":90,"v2":10}}`) |
| GET | `/health` | Health summary (JSON: per-version status grouped by model) |
| GET | `/livez` | Liveness probe (always 200) |
| GET | `/readyz` | Readiness probe (503 until a model can serve) |
| GET | `/startupz` | Startup probe (503 while models load) |
| GET | `/info` | Server info |
| GET | `/metrics` | Prometheus metrics |
| GET | `/metrics/timeline` | Historical metric timeline |
| GET | `/metrics/timeline/{name}` | Per-model metric timeline |
| GET | `/metrics/timeline/{name}/versions/{v}` | Per-model-version metric timeline |
| GET | `/metrics/alerts` | Alert rules and status |

**Custom routes** declared with `@route` on `LitAPI` methods are served under `/v2/models/{name}/<tail>`. System tails (`infer`, `events`, `stream`, `ready`, `health`, `reload`, `versions`, `compare`) are reserved and cannot be overridden. `livez`, `readyz`, `startupz` are root-level probes outside the model namespace, so they do not conflict with custom routes.

## Configuration

Minimal `server.yaml`:

```yaml
server:
  http_port: 8000
  host: 0.0.0.0

model_repository:
  path: ./model_repo
```

The full configuration reference (server, model, orchestration, CLI flags) is in
[docs/configuration.md](docs/configuration.md). Per-model config lives in
`model_repo/my_model/1/config.yaml`; the model authoring guide is in
[docs/model-authoring.md](docs/model-authoring.md).

## Documentation

Full documentation set, indexed in [docs/index.md](docs/index.md):

| Doc | Covers |
|-----|--------|
| [Architecture](docs/architecture.md) ([中文](docs/zh/architecture.md)) | System design, request flow, worker model |
| [Configuration](docs/configuration.md) ([中文](docs/zh/configuration.md)) | server / model / orchestration config, TLS, access control, CORS |
| [Model Authoring](docs/model-authoring.md) ([中文](docs/zh/model-authoring.md)) | LitAPI interface, streaming, continuous batching, best practices |
| [CLI Reference](docs/cli.md) ([中文](docs/zh/cli.md)) | All CLI commands and flags |
| [Streaming](docs/streaming.md) | Bidirectional streaming (WS `/stream`, h2 `/bidi`), decoupled streaming (SSE `/decoupled`, WS `/decoupled-stream`) |
| [Protocol Compatibility](docs/protocol.md) | Raw bytes / tensor requests, Triton Binary extension, openai-compact, known deviations from KServe V2 / Triton |
| [Observability](docs/observability.md) | Prometheus metrics reference, OpenTelemetry |
| [Deployment](docs/deployment.md) | Graceful shutdown, rolling updates, KEDA autoscaling |
| [Migration Guide](docs/migration.md) ([中文](docs/zh/migration.md)) | Breaking changes and upgrade paths |
| [Benchmarks](docs/benchmark.md) | Benchmark methodology and results |
| [Comparison](docs/comparison.md) | lite-server vs other serving frameworks |

## FAQ

**Q: How is lite-server different from LitServe?**
lite-server uses a Rust HTTP core (axum/tokio) instead of Python's uvicorn, with the same LitAPI-compatible model code. Under zero-compute echo (pure framework overhead, three sides isomorphic), lite-server matches the native lite-server-core binary (PyO3 embedding adds zero overhead) and sustains ~2.0–2.4× LitServe at c≥16 (single event loop, aligned); under a 1ms-sleep workload all three converge — see [docs/benchmark.md](docs/benchmark.md).

**Q: Do I need Docker?**
No. `pip install` and run directly. Works on Linux, macOS, and Windows.

**Q: Can I use my existing LitAPI code?**
`from lite_server import LitAPI` works for any model with `setup` + `predict`. Since 0.7.0 it's a self-contained base class — no litserve dependency. Async methods are supported natively; just write `async def predict(self, x)`.

**Q: How do I deploy multiple models?**
Put each model in its own directory under `model_repo/` and list them in `server.yaml`. See [examples/05_ensemble](examples/05_ensemble/) for multi-model pipelines.

**Q: How do I switch model versions?**
Use the activate/deactivate API: `POST /v2/models/{name}/versions/{v}/activate`. See [examples/04_multi_version](examples/04_multi_version/).

**Q: What happens if a worker crashes?**
The worker is automatically restarted. In-flight requests are retried on other workers (up to 3 attempts). Outlier detection ejects unhealthy workers. Heartbeat probes detect hung processes and trigger automatic respawn. Lifecycle hooks fire on exit/error for alerting.

## Multi-Platform

Wheels are built by CI for the platforms below. macOS x86_64 (Intel) and
Windows aarch64 are not currently published — build from source there
(requires Rust + libzmq).

| Platform | Architecture | Wheel Tag |
|----------|-------------|-----------|
| Linux | x86_64 | manylinux_2_28_x86_64 |
| Linux | aarch64 | manylinux_2_28_aarch64 |
| macOS | aarch64 (Apple Silicon) | macosx_11_0_arm64 |
| Windows | x86_64 | win_amd64 |

## Development

```bash
cargo build --release
cargo test
cd python && python -m pytest tests/
```

### Project Structure

```
.
├── src/              # Rust core (HTTP, inference queue, worker management, ensemble, gRPC)
├── python/           # Python package (CLI, worker process, LitAPI, artifact packer)
├── tests/            # Rust integration tests
├── examples/         # Example model repositories
├── benchmarks/       # Performance benchmarks
├── docs/             # Documentation (start at docs/index.md)
│   ├── index.md
│   ├── architecture.md
│   ├── configuration.md
│   ├── cli.md
│   ├── model-authoring.md
│   ├── migration.md
│   ├── streaming.md
│   ├── protocol.md
│   ├── observability.md
│   ├── deployment.md
│   ├── benchmark.md
│   ├── comparison.md
│   └── zh/                 # Chinese docs (core set)
└── Cargo.toml        # Rust manifest
└── pyproject.toml    # Python packaging (maturin)
```

## License

MIT
