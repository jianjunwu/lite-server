# Examples

Each example is a self-contained directory with its own model repository, README, and runnable code.

## Prerequisites

```bash
# Install lite-server (from wheel or source)
pip install lite-server-*.whl
# or
pip install -e .

# Install example dependencies (if any)
pip install pyyaml
```

## Run All Examples (one command)

Verify every example starts and matches its README output — sequentially
(examples share port 8000, so they run one at a time):

```bash
pip install grpcio          # required: example 13 uses gRPC bidi
python run_all.py           # from the examples/ directory
# (or: python examples/run_all.py from the project root)
```

This starts each example's server, waits for readiness, checks the README
behavior, then shuts it down cleanly — printing a `PASS`/`FAIL` summary and
exiting non-zero if any example fails. Run a subset by name:

```bash
python run_all.py 01_basic 03_streaming 13_bidi_streaming
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
| 04 | [multi_version](04_multi_version/) | Version switching | `server.yaml`, activate/deactivate versions at runtime |
| 05 | [ensemble](05_ensemble/) | Multi-model DAG pipeline | Ensemble config, parallel step execution, `$request`/`$step` refs |
| 06 | [custom_route](06_custom_route/) | Custom HTTP routes on a model | `@route` decorator, path params, `ctx.server` registry queries |
| 07 | [custom_params](07_custom_params/) | Config-driven behavior | `self.config`, custom YAML fields |
| 09 | [custom_metrics](09_custom_metrics/) | Custom Prometheus metrics | `register_metric()`, `report_metric()`, gauge/counter/histogram |
| 10 | [async](10_async/) | Asynchronous inference | `async def predict()`, unified async pipeline, mixed sync/async |
| 11 | [logging](11_logging/) | Structured logging at every stage | `self.logger`, per-request tracing, `--log-level` |

### Production

| # | Example | Description | Key Concept |
|---|---------|-------------|-------------|
| 12 | [continuous_batching](12_continuous_batching/) | LLM continuous batching | `prefill()` / `step()` / `has_finished()` hooks |
| 13 | [bidi_streaming](13_bidi_streaming/) | Bidirectional streaming (ASR) | `BidiStreamHandler`, `on_open` / `on_chunk` / `on_close` |
| 14 | [lifecycle_hooks](14_lifecycle_hooks/) | Worker lifecycle hooks | `on_ready` / `on_error` / `on_exit` shell + HTTP callbacks |
| 15 | [middleware](15_middleware/) | Model-level callback chain | `RequireApiKey`, `RateLimit`, `Cors`, `LogRequests` |
| 16 | [grpc](16_grpc/) | gRPC inference endpoints | `grpc_port`, auto-generated gRPC from LitAPI |

## Running Any Example

```bash
# From the project root
cd examples/<example>
python -m lite_server serve --config server.yaml

# Or with the Rust binary directly
cd examples/<example>
lite-server-core serve --config server.yaml
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

### 06 Custom Routes

```bash
# Standard inference still works
curl -X POST http://localhost:8000/v2/models/pets/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}

# Custom routes live under /v2/models/<model>/<tail>
curl http://localhost:8000/v2/models/pets/status
# => {"model_loaded": true, "method": "GET"}

curl http://localhost:8000/v2/models/pets/pets/1
# => {"id": 1, "name": "Fido"}

# ctx.server registry query from a route handler
curl http://localhost:8000/v2/models/pets/models
# => {"loaded": [{"name": "pets", "version": "1", "status": "Ready", ...}]}
```

### 07 Custom Parameters

```bash
# Score above threshold (0.5) -> "positive"
curl -X POST http://localhost:8000/v2/models/threshold/infer \
  -H 'Content-Type: application/json' \
  -d '{"score": 0.8}'
# => {"label": "positive", "score": 0.8, "threshold": 0.5}

# Score below threshold -> "negative"
curl -X POST http://localhost:8000/v2/models/threshold/infer \
  -H 'Content-Type: application/json' \
  -d '{"score": 0.3}'
# => {"label": "negative", "score": 0.3, "threshold": 0.5}
```

### 09 Custom Metrics

```bash
# Send requests to generate metric data
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/v2/models/metrics_demo/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}" &
done
wait

# Check custom metrics in Prometheus output
curl -s http://localhost:8000/metrics | grep demo_
# => lite_server_demo_batch_size{model="metrics_demo"} 1
# => lite_server_demo_predictions_total_total{model="metrics_demo"} 10
# => lite_server_demo_inference_ms_count{model="metrics_demo"} 10
```

### 10 Async

```bash
# Single request
curl -X POST http://localhost:8000/v2/models/async_echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "async_echo: hello"}

# Concurrent requests — async workers handle them without blocking
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/v2/models/async_echo/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": \"msg-$i\"}" &
done
wait
```

### 11 Logging

```bash
# Run with --log-level info to see per-request logs
curl -X POST http://localhost:8000/v2/models/logged_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42, "call_count": 1}
```

### 12 Continuous Batching

```bash
curl -X POST http://localhost:8000/v2/models/cb_llm/infer \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "hello world this is a test"}'
# => {"tokens": ["hello","world","this","is","a"], "text": "hello world this is a"}
```

### 13 Bidirectional Streaming

Bidirectional streaming runs over gRPC (the `/stream` WebSocket path is
server-side only). From the example directory:

```bash
pip install grpcio   # if not already installed
python test_bidi.py
# open  : {"status": "ready", "sample_rate": 16000}
# chunk : {"partial": "hello", "is_final": false}
# chunk : {"partial": "hello world", "is_final": false}
# close : {"final": "hello world", "is_final": true, "buffer": ["hello", "world"]}
```

### 14 Lifecycle Hooks

```bash
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "hello", "count": 1}
# Console shows hook commands executing (echo statements)
```

### 15 Callbacks

```bash
# Inference — requires API key
curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -d '{"input": "hello"}'
# => {"error": {"type": "authentication_error", "message": "missing API key", ...}}  (401)

curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -H "X-API-Key: secret-api-key-123" \
  -d '{"input": "hello"}'
# => {"output": "protected: hello"}

# Custom route — guarded by the same model-level chain
curl http://localhost:8000/v2/models/protected/status
# => 401

curl -H "X-API-Key: secret-api-key-123" http://localhost:8000/v2/models/protected/status
# => {"server": "lite-server", "loaded_models": [{"name": "protected", ...}], ...}
```

### 16 gRPC

```bash
# HTTP still works
curl -X POST http://localhost:8000/v2/models/grpc_echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "grpc_echo: hello"}

# gRPC inference via grpcurl
grpcurl -plaintext \
  -d '{"model_name": "grpc_echo", "input": {"input": "hello"}}' \
  localhost:8001 \
  liteserver.LiteServer/Infer
# => {"output": "grpc_echo: hello"}
```

## More Documentation

- [Model Authoring Guide](../docs/model-authoring.md) — Full `LitAPI` interface reference
- [Configuration Reference](../docs/configuration.md) — All config options with defaults
- [Architecture](../docs/architecture.md) — How lite-server works internally
