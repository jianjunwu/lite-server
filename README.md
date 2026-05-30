# lite-server

High-performance model inference server — Rust core for I/O, Python for inference.

[中文文档](README_zh.md)

## Table of Contents

- [Why lite-server?](#why-lite-server)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Comparison](#comparison)
- [Benchmarks](#benchmarks)
- [Features](#features)
- [Installation](#installation)
- [CLI Commands](#cli-commands)
- [Examples](#examples)
- [API Endpoints](#api-endpoints)
- [Configuration](#configuration)
- [FAQ](#faq)
- [Development](#development)
- [License](#license)


## Why lite-server?

| | Feature | What it means for you |
|---|---------|----------------------|
| **Fast** | Rust HTTP core (axum/tokio), zero-copy data path, adaptive batching | Higher throughput, lower latency than pure-Python servers |
| **Stable** | Outlier detection, request retry, worker auto-recycle | Self-healing workers, no manual babysitting |
| **Simple** | `pip install`, write one `model.py`, done | No Docker, no Java, no C++ build tools |
| **Flexible** | Hot reload, multi-version, ensemble DAG | A/B testing, canary deploys, multi-model pipelines in one server |
| **Observable** | Prometheus metrics, timeline, alerts | Know what's happening in production without guessing |
| **Lightweight** | Single binary + Python workers, cross-platform | Runs on a laptop, scales on a server |

## Quick Start

```bash
# 1. Install
pip install litserve  # lite-server depends on litserve's LitAPI

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
  │                  │ZMQ  │                   │
  │  ┌────────────┐  │/UDS │  ┌─────────────┐  │
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
| Streaming | SSE + WebSocket + gRPC | Yes | No | Yes | Yes |
| Min. Overhead | ~10MB | ~500MB | ~200MB | ~50MB | ~100MB |

See [docs/comparison.md](docs/comparison.md) for detailed analysis.

## Benchmarks

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

### Worker Resilience

- **Outlier detection** — consecutive errors trigger automatic worker ejection (Envoy-style)
- **Request retry** — failed requests retry on a different worker (up to 3 attempts)
- **Least-loaded routing** — requests go to the worker with fewest inflight tasks
- **Max-requests recycle** — auto-restart workers after N requests to prevent memory leaks
- **Per-request timeout** — hard timeout prevents stuck requests from blocking the queue

### Observability

- **Prometheus metrics** — QPS, P50/P90/P99 latency, queue depth, TTFT, batch size, worker ejections
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
python -m lite_server serve                     # Start inference server
python -m lite_server serve --config server.yaml
python -m lite_server serve --port 9000 --workers 4
python -m lite_server config-check server.yaml  # Validate config
python -m lite_server benchmark --model my_model
python -m lite_server analyze --model my_model
python -m lite_server pack ./my_model --version 1
python -m lite_server unpack my_model_v1.lma
python -m lite_server init my_project           # Scaffold new project
```

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

See [examples/README.md](examples/README.md) for learning path and usage details.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v2/models/{name}/infer` | Inference (active version) |
| POST | `/v2/models/{name}/versions/{v}/infer` | Inference (specific version) |
| POST | `/v2/models/{name}/events` | SSE streaming |
| GET | `/v2/models/{name}/stream` | WebSocket streaming |
| GET | `/v2/models` | List loaded models |
| GET | `/v2/models/{name}/versions` | List versions |
| GET | `/v2/models/{name}/ready` | Readiness check |
| POST | `/v2/repository/models/{name}/load` | Load model |
| POST | `/v2/repository/models/{name}/unload` | Unload model |
| POST | `/v2/repository/models/{name}/versions/{v}/upload` | Upload model files (.lma or raw) |
| GET | `/v2/repository/models/{name}/versions/{v}/download` | Download model files |
| GET | `/v2/repository/models/{name}/versions/{v}/files` | List version directory contents |
| POST | `/v2/models/{name}/reload` | Hot reload |
| POST | `/v2/models/{name}/versions/{v}/activate` | Activate version |
| GET | `/health` | Health check |
| GET | `/info` | Server info |
| GET | `/metrics` | Prometheus metrics |

## Configuration

Minimal `server.yaml`:

```yaml
server:
  http_port: 8000
  host: 0.0.0.0
  transport: zmq  # zmq or uds

model_repository:
  path: ./model_repo
```

Per-model config (`model_repo/my_model/1/config.yaml`):

```yaml
max_batch_size: 8
batch_timeout: 0.01
stream: false
accelerator: cpu
workers_per_device: 1
request_timeout: 30.0
```

See [docs/configuration.md](docs/configuration.md) for the full configuration reference (server, model, orchestration, CLI flags).

See [docs/model-authoring.md](docs/model-authoring.md) for the complete model authoring guide (LitAPI interface, streaming, continuous batching, best practices).

## FAQ

**Q: How is lite-server different from LitServe?**
lite-server uses a Rust HTTP core (axum/tokio) instead of Python's uvicorn, giving 3x higher throughput at multi-worker concurrency. Models are written the same way (LitAPI-compatible).

**Q: Do I need Docker?**
No. `pip install` and run directly. Works on Linux, macOS, and Windows.

**Q: Can I use my existing LitAPI code?**
Yes. `from lite_server import LitAPI` is a drop-in replacement for `litserve.LitAPI` with additional hooks (streaming, continuous batching, lifecycle).

**Q: How do I deploy multiple models?**
Put each model in its own directory under `model_repo/` and list them in `orchestration.yaml`. See [examples/05_ensemble](examples/05_ensemble/) for multi-model pipelines.

**Q: How do I switch model versions?**
Use the activate/deactivate API: `POST /v2/models/{name}/versions/{v}/activate`. See [examples/04_multi_version](examples/04_multi_version/).

**Q: What happens if a worker crashes?**
The worker is automatically restarted. In-flight requests are retried on other workers (up to 3 attempts). Outlier detection ejects unhealthy workers.

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
│   ├── comparison.md
│   ├── comparison_zh.md
│   ├── configuration.md
│   ├── model-authoring.md
│   └── model-authoring_zh.md
├── Cargo.toml        # Rust manifest
└── pyproject.toml    # Python packaging (maturin)
```

## License

MIT
