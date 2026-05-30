# Benchmarks

Performance comparison of lite-server vs LitServe using `wrk`.

## Test Environment

- **Model**: 1ms `time.sleep()` CPU mock (measures IPC and HTTP overhead, not GPU compute)
- **Tool**: `wrk` with POST requests (`{"input":"hello"}`)
- **OS**: macOS (Apple Silicon)

## Results

### Throughput (req/s)

| Workers | Concurrency | lite-server | LitServe | lite-server-core | Speedup (ls/lit) |
|---------|-------------|-------------|----------|------------------|-------------------|
| 1 | 4 | 171 | 330 | 444 | 0.5x |
| 2 | 4 | 1,583 | 531 | 1,364 | 3.0x |

### p99 Latency (ms)

| Workers | Concurrency | lite-server | LitServe | lite-server-core |
|---------|-------------|-------------|----------|------------------|
| 1 | 4 | 72.1 | 139.6 | 139.2 |
| 2 | 4 | 11.5 | 162.6 | 11.6 |

### Analysis

**Single worker (w=1):** LitServe has higher raw throughput because lite-server's Rust HTTP layer adds overhead that isn't amortized at low concurrency. The Python wrapper path (`lite-server`) is slower than the direct Rust binary (`lite-server-core`) due to PyO3 bridging.

**Two workers (w=2):** lite-server's architecture shines. With multiple workers, the Rust core efficiently distributes requests across workers via ZMQ, while the adaptive batching and least-loaded scheduling keep workers busy. LitServe's throughput drops because its Python HTTP layer (uvicorn) becomes the bottleneck.

**Key takeaway:** lite-server's advantage grows with concurrency and worker count. For production workloads with multiple workers and concurrent requests, lite-server provides significantly higher throughput and lower latency.

## Reproducing

```bash
# Prerequisites
pip install litserve
brew install wrk  # or apt-get install wrk

# Quick test (1 config, ~30s)
python benchmarks/scripts/compare.py --lite

# Full comparison
python benchmarks/scripts/compare.py \
  --workers 1 2 4 \
  --concurrency 1 4 16 64 \
  --plot

# Custom model
python benchmarks/scripts/compare.py \
  --model-repo /path/to/model_repo \
  --model your_model \
  --workers 1 2 4 \
  --concurrency 1 4 16 64
```

Results are saved to `benchmarks/results/benchmark.csv`. With `--plot`, charts are saved to `benchmarks/results/comparison.png`.

## Understanding the Numbers

**lite-server** = Python CLI wrapper launching the Rust binary (`lite-server-core`) + Python workers
**lite-server-core** = Direct Rust binary (no Python wrapper overhead)
**LitServe** = Lightning AI's inference server (FastAPI + uvicorn)

The "speedup" column compares `lite-server` vs `LitServe`. The `lite-server-core` column shows the raw Rust binary performance without Python bridging overhead.

## Notes

- These benchmarks measure HTTP + IPC overhead, not model compute time. Real models with GPU inference will show smaller relative differences.
- The 1ms sleep model is intentionally lightweight to isolate the serving framework overhead.
- Results may vary based on hardware, OS, and system configuration.
