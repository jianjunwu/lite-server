# lite-server

High-performance model inference server — Rust core for I/O, Python for inference.

[中文文档](README_zh.md)

## Table of Contents

- [lite-server](#lite-server)
  - [Table of Contents](#table-of-contents)
  - [Why lite-server?](#why-lite-server)
  - [Quick Start](#quick-start)
  - [Architecture](#architecture)
  - [Comparison with Other Frameworks](#comparison-with-other-frameworks)
  - [Benchmarks](#benchmarks)
  - [Features](#features)
    - [Inference Modes](#inference-modes)
    - [Model Management](#model-management)
    - [Custom Endpoints](#custom-endpoints)
    - [Worker Resilience](#worker-resilience)
    - [Observability](#observability)
  - [Installation](#installation)
    - [From Wheel (Recommended)](#from-wheel-recommended)
    - [From Source](#from-source)
  - [CLI Commands](#cli-commands)
  - [Examples](#examples)
  - [API Endpoints](#api-endpoints)
  - [Configuration](#configuration)
  - [FAQ](#faq)
  - [Multi-Platform](#multi-platform)
  - [Development](#development)
    - [Project Structure](#project-structure)
  - [TODO](#todo)
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
pip install lite-server

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

Available templates: `empty`, `llm`, `cv-classify`, `cv-detect`, `nlp`. Use `--wizard` for interactive selection.

## Architecture

```
  HTTP/gRPC Request
        │
        ▼
  ┌─────────────────┐     ┌──────────────────┐
  │   Rust Core      │     │  Python Workers   │
  │  (axum/tokio)    │────►│  (subprocesses)   │
  │                  │ZMQ/ │                   │
  │  ┌────────────┐  │Protobuf ┌─────────────┐│
  │  │ Inference  │  │     │  │  model.py   │  │
  │  │ Queue      │  │     │  │  (LitAPI)   │  │
  │  └────────────┘  │     │  └─────────────┘  │
  │  ┌────────────┐  │     └──────────────────┘
  │  │ Metrics &  │  │
  │  │ Alerts     │  │
  │  └────────────┘  │
  └─────────────────┘
```

The Rust core handles all I/O (HTTP, gRPC, IPC, metrics, file watching), while Python workers handle model inference. This separation gives you Rust-level throughput with Python-level simplicity.

See [docs/architecture.md](docs/architecture.md) for details.

## Comparison with Other Frameworks

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| Language | Rust + Python | C++ | Java + Python | Python | Python |
| Install | `pip install` | Docker | Java + Conda | `pip install` | `pip install` |
| Hot Reload | File watcher | No | Limited | No | No |
| Multi-Version | Yes (activate/deactivate) | Yes | Yes | Manual | Manual |
| Ensemble | DAG with parallel layers | Yes | No | Pipeline | Deployment graph |
| Outlier Detection | Envoy-style auto-eject | No | No | No | No |
| Heartbeat + Respawn | ZMQ probe, auto-restart hung workers | No | No | No | No |
| Lifecycle Hooks | Shell + HTTP callbacks | No | No | No | No |
| Streaming | SSE + WebSocket + gRPC | Yes | No | Yes | Yes |
| Min. Overhead | ~15MB | ~500MB | ~200MB | ~50MB | ~100MB |

See [docs/comparison.md](docs/comparison.md) for detailed analysis.

## Benchmarks

> **Note:** The data below is a preliminary placeholder with limited data points. See [docs/benchmark.md](docs/benchmark.md) for context and reproduction steps.

2-worker, 4-concurrency test (1ms CPU mock model):

| Server | Throughput | p99 Latency |
|--------|-----------|-------------|
| lite-server | 1,583 req/s | 11.5 ms |
| LitServe | 531 req/s | 162.6 ms |
| lite-server-core | 1,364 req/s | 11.6 ms |

See [docs/benchmark.md](docs/benchmark.md) for full results and reproduction steps.

## Features

### Inference Modes

- **Standard batching** — group requests into batches for GPU efficiency
- **Continuous batching** — for LLM workloads with prefill/step/has_finished hooks
- **Streaming** — token-by-token output via SSE, WebSocket, or gRPC
- **Ensemble** — DAG-based multi-model pipelines with parallel execution

### Model Management

- **Triton-style repository** — `model_name/version/model.py` directory structure
- **Hot reload** — modify model.py, server picks up changes automatically
- **Multi-version** — load, unload, activate, deactivate versions independently
- **Load policies** — `explicit`, `latest`, `all` for version management
- **Model packing** — `.lma` format with SHA256 + HMAC signature
- **Model upload/download** — upload `.lma` artifacts or raw files via HTTP API, with auto-load support

### Custom Endpoints

- **Directory-based discovery** — place Python files in `endpoints/` for auto-registration
- **Decorator routing** — `@endpoint.get("/status")` for explicit control
- **Server context access** — `server.registry`, `server.infer()`, `server.metrics` from handlers
- **Middleware chain** — per-route or global middleware (auth, rate limiting, CORS)
- **Streaming support** — chunked and SSE responses from custom endpoints

### Worker Resilience

- **Outlier detection** — consecutive errors trigger automatic worker ejection (Envoy-style)
- **Request retry** — failed requests retry on a different worker (up to 3 attempts)
- **Least-loaded routing** — requests go to the worker with fewest inflight tasks
- **Max-requests recycle** — auto-restart workers after N requests, with jitter to prevent thundering herd
- **Heartbeat detection** — periodic ZMQ probes detect hung workers, auto-kill and respawn
- **Lifecycle hooks** — shell commands and HTTP callbacks on worker ready/exit/error for alerting and observability
- **Per-request timeout** — hard timeout prevents stuck requests from blocking the queue

### Observability

- **Prometheus metrics** — QPS, P50/P90/P99 latency, queue depth, TTFT, batch size, worker ejections
- **Custom metrics** — gauge, counter, histogram from model code via `register_metric()` / `report_metric()`
- **Timeline** — historical metric sampling per model
- **Alerts** — built-in alert rules for anomaly detection
- **Structured logging** — tracing-based logs with model/worker context

## Installation

### From Wheel (Recommended)

Pre-built wheels for Linux, macOS, Windows (x86_64 + aarch64), Python 3.10-3.14:

```bash
pip install lite-server-<version>-py3-none-<platform>.whl
```

### From Source

Requires Rust >= 1.70 and Python >= 3.10.

```bash
pip install maturin
maturin develop          # dev build
maturin build --release  # release wheel
```

## CLI Commands

```bash
lite-server serve                     # Start inference server
lite-server serve --config server.yaml
lite-server serve --port 9000 --max-requests 1000 --max-requests-jitter 100
lite-server config-check server.yaml  # Validate config
lite-server benchmark --model my_model
lite-server analyze --model my_model
lite-server pack ./my_model --version 1
lite-server unpack my_model_v1.lma
lite-server init my_project           # Scaffold new project
```

See [docs/cli.md](docs/cli.md) for the full CLI reference.

## Examples

See [examples/](examples/) for runnable model repositories:

| # | Example | Description |
|---|---------|-------------|
| 01 | [basic](examples/01_basic/) | Minimal echo model |
| 02 | [batching](examples/02_batching/) | Request batching with adaptive timeout |
| 03 | [streaming](examples/03_streaming/) | Token-by-token streaming via SSE/WebSocket |
| 04 | [multi_version](examples/04_multi_version/) | Two versions with activation switching |
| 05 | [ensemble](examples/05_ensemble/) | DAG-based multi-model pipeline |
| 06 | [custom_endpoint](examples/06_custom_endpoint/) | Custom HTTP endpoint |
| 07 | [custom_params](examples/07_custom_params/) | Config-driven model behavior |
| 09 | [custom_metrics](examples/09_custom_metrics/) | Custom Prometheus metrics (gauge/counter/histogram) |
| 10 | [async](examples/10_async/) | Asynchronous inference (unified async pipeline) |
| 11 | [logging](examples/11_logging/) | Structured logging at every stage |
| 12 | [continuous_batching](examples/12_continuous_batching/) | LLM continuous batching (prefill/step/has_finished) |
| 13 | [bidi_streaming](examples/13_bidi_streaming/) | Bidirectional streaming for ASR |
| 14 | [lifecycle_hooks](examples/14_lifecycle_hooks/) | Worker lifecycle hooks (shell + HTTP) |
| 15 | [middleware](examples/15_middleware/) | Endpoint middleware (auth, rate limit, CORS) |
| 16 | [grpc](examples/16_grpc/) | gRPC inference endpoints |

See [examples/README.md](examples/README.md) for learning path and usage details.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/chat/completions` | OpenAI-compatible chat completion |
| POST | `/v2/models/{name}/infer` | Inference (active version) |
| POST | `/v2/models/{name}/versions/{v}/infer` | Inference (specific version) |
| POST | `/v2/models/{name}/events` | SSE streaming |
| POST | `/v2/models/{name}/versions/{v}/events` | SSE streaming (specific version) |
| GET | `/v2/models/{name}/stream` | WebSocket streaming |
| GET | `/v2/models/{name}/versions/{v}/stream` | WebSocket streaming (specific version) |
| GET | `/v2/models` | List loaded models |
| GET | `/v2/models/{name}/versions` | List versions |
| GET | `/v2/models/{name}/ready` | Readiness check |
| GET | `/v2/models/{name}/health` | Per-worker health status |
| GET | `/v2/models/{name}/compare` | Compare model versions |
| DELETE | `/v2/models/{name}/versions/{v}` | Delete model version |
| POST | `/v2/repository/models/{name}/load` | Load model |
| POST | `/v2/repository/models/{name}/unload` | Unload model |
| POST | `/v2/repository/index` | Index model repository |
| POST | `/v2/repository/models/{name}/versions/{v}/upload` | Upload model files (.lma or raw) |
| GET | `/v2/repository/models/{name}/versions/{v}/download` | Download model files |
| GET | `/v2/repository/models/{name}/versions/{v}/files` | List version directory contents |
| POST | `/v2/models/{name}/reload` | Hot reload |
| POST | `/v2/models/{name}/versions/{v}/activate` | Activate version |
| GET | `/health` | Health check (overridable by custom endpoints) |
| GET | `/info` | Server info |
| GET | `/metrics` | Prometheus metrics |
| GET | `/metrics/timeline` | Historical metric timeline |
| GET | `/metrics/timeline/{name}` | Per-model metric timeline |
| GET | `/metrics/alerts` | Alert rules and status |

**Custom endpoints** (loaded from `endpoints/` directory or decorator-registered) are registered dynamically alongside built-in routes. System routes (`/info`, `/metrics`, `/v2/models`, `/v2/repository`) cannot be overridden.

## Configuration

Minimal `server.yaml`:

```yaml
server:
  http_port: 8000
  host: 0.0.0.0

model_repository:
  path: ./model_repo

# Optional: custom HTTP endpoints directory
endpoints_dir: ./endpoints
```

Per-model config (`model_repo/my_model/1/config.yaml`):

```yaml
max_batch_size: 8
batch_timeout: 0.01
stream: false
accelerator: cpu
workers_per_device: 1
request_timeout: 30.0

# Worker lifecycle — auto-restart + jitter + heartbeat + hooks
max_requests: 500
max_requests_jitter: 50

heartbeat_interval: 10.0
heartbeat_timeout: 5.0
heartbeat_max_failures: 3

hooks:
  on_ready: 'echo "Worker $WORKER_ID ready"'
  on_error: 'curl -s -X POST http://alerts.internal/worker-error \
    -d "{\"model\":\"$MODEL\",\"reason\":\"$REASON\"}"'
```

See [docs/configuration.md](docs/configuration.md) for the full configuration reference (server, model, orchestration, CLI flags).

See [docs/cli.md](docs/cli.md) for the complete CLI reference.

See [docs/model-authoring.md](docs/model-authoring.md) for the complete model authoring guide (LitAPI interface, streaming, continuous batching, best practices).

## FAQ

**Q: How is lite-server different from LitServe?**
lite-server uses a Rust HTTP core (axum/tokio) instead of Python's uvicorn, giving 3x higher throughput at multi-worker concurrency. Models are written the same way (LitAPI-compatible).

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

| Platform | Architecture | Wheel Tag |
|----------|-------------|-----------|
| Linux | x86_64 | manylinux2014_x86_64 |
| Linux | aarch64 | manylinux2014_aarch64 |
| macOS | x86_64 | macosx_10_12_x86_64 |
| macOS | aarch64 (Apple Silicon) | macosx_11_0_arm64 |
| Windows | x86_64 | win_amd64 |
| Windows | aarch64 | win_arm64 |

## Development

```bash
cargo build --release
cargo test
cd python && python -m pytest tests/
```

### Project Structure

```
.
├── src/              # Rust core (HTTP, inference queue, worker management)
├── python/           # Python package (CLI, worker process, LitAPI)
├── tests/            # Rust integration tests
├── examples/         # Example model repositories
├── benchmarks/       # Performance benchmarks
├── docs/             # Documentation
│   ├── architecture.md
│   ├── benchmark.md
│   ├── cli.md              # CLI reference
│   ├── comparison.md
│   ├── configuration.md
│   ├── model-authoring.md
│   └── zh/                 # Chinese docs
│       ├── architecture.md
│       ├── benchmark.md
│       ├── cli.md
│       ├── comparison.md
│       ├── configuration.md
│       └── model-authoring.md
├── Cargo.toml        # Rust manifest
└── pyproject.toml    # Python packaging (maturin)
```

## TODO

- [ ] OpenAI-compatible endpoint example — reimplement with decorator pattern (`@endpoint.post("/v1/chat/completions")`) replacing the removed `08_openai_compatible`
- [ ] `server.infer()` — wire up so custom endpoint handlers can call loaded LitAPI models

## License

MIT
