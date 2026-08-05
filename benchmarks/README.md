# Benchmarks: lite-server vs LitServe

Performance comparison of inference server architectures using `wrk`.

## Prerequisites

Requires the compiled `lite-server` Python package, `wrk`, and Python dependencies:

```bash
# Build and install lite-server from the project root
maturin build --release
pip install dist/lite_server-*.whl

# Install benchmark dependencies
pip install litserve pyyaml

# Install wrk (macOS)
brew install wrk

# Or wrk on Linux
# sudo apt-get install wrk   # Ubuntu/Debian
# sudo yum install wrk       # RHEL/CentOS
```

## Quick Start

All commands should be run from the **project root**:

```bash
# Quick sanity check (1 config, ~30s)
python benchmarks/scripts/compare.py --lite

# Full comparison with default 1ms sleep model (takes several minutes)
python benchmarks/scripts/compare.py --workers 1 2 4 --concurrency 1 4 16 64 --plot

# Use 10ms sleep model
python benchmarks/scripts/compare.py --model sleep_model --workers 1 2 4 --concurrency 1 4 16 64 --plot
```

## perf-smoke (P-PERF-a, self-contained)

Zero-external-tool smoke of the key paths (HTTP/gRPC unary + SSE) — this is
the §4.0.8 regression baseline harness. A local/manual measurement tool (not
wired into CI; see docs/benchmark.md for why):

```bash
cargo build --release
cargo run --release --example perf_smoke   # → stdout + target/perf-smoke.json
```

Methodology + first baseline: [docs/benchmark.md](../docs/benchmark.md).

## stream-chunk-overhead (self-contained)

Zero-external-tool measurement of per-chunk plumbing overhead on streaming paths —
gRPC bidi ping-pong RTT + SSE chunk intervals + WS chunk intervals. Uses zero-compute
`bidi_echo_model` / `echo_stream_model` so the numbers isolate server-side overhead
(gRPC+ZMQ+worker dispatch / SSE+WS forward loop), not model time:

```bash
cargo build --release
cargo run --release --example stream_chunk_overhead   # → stdout + target/stream-chunk-overhead.json
```

Two bidi modes:
- **Burst** (1000 ping-pong chunks) — pipeline latency floor
- **Paced 40ms** (300 chunks, 25 fps ASR rhythm) — reports how many RTTs exceed the pace (real-time viability)

## Custom Model Repository

Both `run_liteserver.py` and `run_litserve.py` support `--model-repo` to point to a custom model directory.

Expected structure (Triton-style):

```
model_repo/
  {model_name}/
    {version}/
      model.py       # Must contain a LitAPI subclass
      config.yaml    # Optional: max_batch_size, batch_timeout, workers_per_device, etc.
```

The `model.py` must define a `LitAPI` subclass. Example:

```python
from lite_server import LitAPI

class MyAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, inputs):
        import time
        time.sleep(0.01)  # Simulate 10ms compute
        return {"output": inputs}

    def encode_response(self, output):
        return output
```

### Streaming models

For SSE/WS streaming benchmarks, implement `stream_predict`:

```python
from lite_server import LitAPI

class EchoStreamAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request

    def predict(self, inputs):
        return inputs

    def stream_predict(self, inputs):
        n = int(inputs.get("n", 20)) if isinstance(inputs, dict) else 20
        for i in range(n):
            yield {"chunk": i, "n": n}

    def encode_response(self, output):
        return output
```

For gRPC bidi / WS bidi benchmarks, implement `bidi_stream`:

```python
from lite_server import LitAPI, BidiStreamHandler

class BidiEchoHandler(BidiStreamHandler):
    def on_open(self, initial_data, ctx=None):
        return {"status": "ready"}

    def on_chunk(self, chunk, ctx=None):
        return {"echo": chunk}

    def on_close(self, ctx=None):
        return {"final": True}

class BidiEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx=None):
        return request

    async def predict(self, x, ctx=None):
        return {"output": "sync fallback"}

    async def encode_response(self, output, ctx=None):
        return output

    def bidi_stream(self, ctx=None):
        return BidiEchoHandler()
```

Pre-built zero-compute models are in `benchmarks/models/`:
`echo_model`, `echo_stream_model`, `bidi_echo_model`, `sleep_model`, `sleep_1ms_model`.

## Streaming Benchmarks (lite-server CLI)

For high-level streaming metrics (TTFT, ITL, TPOT, tokens/sec, RTF) with LLM/TTS/STT
semantics, use the built-in `lite-server bench` command instead of the wrk scripts
below. wrk only covers HTTP unary — streaming comparison uses the CLI engine:

```bash
lite-server bench --stream --model-type llm --duration 60 --concurrency 8 --url http://127.0.0.1:8000
```

See `lite-server bench --help` and [docs/cli.md](../docs/cli.md) for flags, model types,
and threshold gates.

## Manual Run

### 1. lite-server

```bash
# Terminal 1: built-in 1ms sleep model
python benchmarks/scripts/run_liteserver.py --port 8000 --workers 4

# Terminal 1: custom model repository
python benchmarks/scripts/run_liteserver.py \
  --port 8000 --workers 4 \
  --model-repo /path/to/your/model_repo \
  --model your_model_name

# Terminal 2
wrk -t4 -c64 -d30s --latency -s benchmarks/scripts/wrk_post.lua \
  http://127.0.0.1:8000/v2/models/{model_name}/infer
```

### 2. LitServe

```bash
# Terminal 1: built-in 1ms sleep model
python benchmarks/scripts/run_litserve.py --port 8001 --workers 4

# Terminal 1: custom model repository
python benchmarks/scripts/run_litserve.py \
  --port 8001 --workers 4 \
  --model-repo /path/to/your/model_repo \
  --model your_model_name

# Terminal 2
wrk -t4 -c64 -d30s --latency -s benchmarks/scripts/wrk_post.lua \
  http://127.0.0.1:8001/v2/models/{model_name}/infer
```

## Output

Results are saved to `benchmarks/results/benchmark.csv`:

| workers | concurrency | lite_rps | lite_p50 | lite_p90 | lite_p99 | lit_rps | lit_p50 | lit_p90 | lit_p99 | speedup |
|---------|-------------|----------|----------|----------|----------|---------|---------|---------|---------|---------|

With `--plot`, charts are saved to `benchmarks/results/comparison.png` and `benchmarks/results/scaling.png` (requires matplotlib).

## Test Matrix

Default comparison covers:
- **Workers**: 1, 2, 4 inference workers
- **Concurrency**: 1, 4, 16, 64 concurrent connections
- **Duration**: 30s per config (use `--duration 60` for more stable results)
- **Model**: 1ms `time.sleep()` CPU mock (no GPU/GIL interference)

## Architecture Under Test

| Aspect          | lite-server                          | LitServe                    |
|-----------------|--------------------------------------|-----------------------------|
| HTTP layer      | Rust (axum) + tokio                  | FastAPI + uvicorn           |
| Inference layer | Python workers (LitAPI-compatible)   | Python workers (LitAPI)     |
| IPC             | UDS + bincode                        | MPQueue / ZMQ               |
| Process spawn   | `std::process::Command`              | `mp.spawn`                  |
| HTTP unary      | POST /v2/models/.../infer            | POST /predict               |
| HTTP streaming  | POST /v2/models/.../events (SSE)     | —                           |
| WS streaming    | GET /v2/models/.../stream            | —                           |
| gRPC bidi       | LiteServer.BidiStream                | —                           |
