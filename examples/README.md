# Examples

Each example is a self-contained directory with its own model repository, README, and runnable code.

## Prerequisites

```bash
# Install lite-server (from wheel or source)
pip install lite-server-*.whl
# or
pip install litserve && pip install -e .

# Install example dependencies (if any)
pip install pyyaml
```

## Learning Path

Start from example 01 and work your way up. Each example builds on concepts from the previous ones.

### Getting Started

| # | Example | Description | Key Concept |
|---|---------|-------------|-------------|
| 01 | [basic](01_basic/) | Minimal echo model | `LitAPI` lifecycle: setup → decode → predict → encode |
| 02 | [batching](02_batching/) | Request batching | `max_batch_size`, adaptive batching, custom `batch()` / `unbatch()` |
| 03 | [streaming](03_streaming/) | Token-by-token output | `stream_predict()`, SSE, WebSocket |

### Advanced

| # | Example | Description | Key Concept |
|---|---------|-------------|-------------|
| 04 | [multi_version](04_multi_version/) | Version switching | `orchestration.yaml`, activate/deactivate versions at runtime |
| 05 | [ensemble](05_ensemble/) | Multi-model DAG pipeline | Ensemble config, parallel step execution, `$request`/`$step` refs |
| 06 | [custom_endpoint](06_custom_endpoint/) | Custom HTTP routes | `*_endpoint.py` auto-discovery, server context access |

## Running Any Example

```bash
# From the project root
python -m lite_server serve --model-repo examples/<example>/model_repo

# Or with the Rust binary directly
lite-server-core serve --model-repo examples/<example>/model_repo
```

Each example's README has specific test commands.

## Expected Outputs

### 01 Basic

```bash
curl -X POST http://localhost:8000/v2/models/echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}
```

### 02 Batching

```bash
# Send 8 concurrent requests — they'll be batched into one predict() call
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8000/v2/models/batched/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}" &
done
wait
# => {"output": 1, "batch_size": 8}  (batch_size varies by timing)

# Custom batch/unbatch demo — send requests with weights
for i in $(seq 1 4); do
  curl -s -X POST http://localhost:8000/v2/models/custom_batch/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i, \"weight\": 0.5}" &
done
wait
# => {"output": 0.5, "batch_size": 4}
```

### 03 Streaming (SSE)

```bash
curl -N -X POST http://localhost:8000/v2/models/streaming/events \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "hello world test", "max_tokens": 3}'
# => data: {"token": "hello", "index": 0}
# => data: {"token": "world", "index": 1}
# => data: {"token": "test", "index": 2}
```

### 04 Multi-Version

```bash
# Default version (v2: x * 2)
curl -X POST http://localhost:8000/v2/models/multi_version/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
# => {"output": 20, "version": "v2"}

# Switch to v1 (x + 1)
curl -X POST http://localhost:8000/v2/models/multi_version/versions/v1/activate
curl -X POST http://localhost:8000/v2/models/multi_version/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
# => {"output": 11, "version": "v1"}
```

### 05 Ensemble

```bash
curl -X POST http://localhost:8000/v2/models/pipeline/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "preprocessed(hello) -> done"}
```

### 06 Custom Endpoint

```bash
# Standard inference still works
curl -X POST http://localhost:8000/v2/models/echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}

# Custom status endpoint
curl http://localhost:8000/status
# => {"server": "lite-server", "loaded_models_count": 1, "loaded_models": [...]}
```

## More Documentation

- [Model Authoring Guide](../docs/model-authoring.md) — Full `LitAPI` interface reference
- [Configuration Reference](../docs/configuration.md) — All config options with defaults
- [Architecture](../docs/architecture.md) — How lite-server works internally
