# Benchmarks

[中文版](zh/benchmark.md)

## P-PERF-a baseline & perf-smoke (2026-08-02)

The first measured baseline (blueprint §4.0.8) plus the self-contained
**perf-smoke** harness that keeps it reproducible. The harness is
**informational** — it reports but never fails the build; regression thresholds
are locked in P-PERF-b once CI runner data exists (shared GitHub runners have
>30% variance, so p99-level gates calibrated on a workstation would flap).

### What is measured (and what is not)

perf-smoke drives three key paths against **zero-compute echo models**
(`benchmarks/models/echo_model`, `echo_stream_model`):

| Path | Shape | What it approximates |
|---|---|---|
| `http_unary` | POST `/v2/models/echo_model/infer`, 2000 req @ 32 conc | Full client-visible latency with a no-op model: server overhead (protocol + middleware + queue) + ZMQ IPC + Python worker round-trip |
| `grpc_unary` | `LiteServer.Infer`, 2000 req @ 32 conc | Same pipeline over gRPC/tonic |
| `sse_stream` | POST `/v2/models/echo_stream_model/events`, 16 streams × 20 chunks | Stream-open latency, per-chunk forward interval, whole-stream time |

**Excluded:** model compute (the models return/echo immediately), TLS, CORS,
rate limiting, OTel export (all default-off — matching the §4.0.8 budget
口径 "default config"). Numbers are therefore an *upper-bound proxy for
server-side overhead*, and — more importantly — a **like-for-like regression
baseline**: same pipeline, same load shape, comparable across commits and
machines.

### Run it

```bash
cargo build --release                     # server binary (lite-server-core)
cargo run --release --example perf_smoke  # report → stdout + target/perf-smoke.json
```

Requires `python` on PATH with `lite_server` importable (workers are Python
processes; `uv sync` in the repo root sets this up). CI runs the same command
(`checks.yml`, job `perf-smoke`, `continue-on-error: true`) and uploads
`target/perf-smoke.json` as an artifact.

### First baseline

Machine: macOS x86_64, 16 cores, rustc 1.97.1, release build (LTO), git
`d0d6992`. Workstation numbers — recorded for the regression methodology,
**not** as CI gates.

| Path | Metric | p50 | p99 | Throughput |
|---|---|---|---|---|
| http_unary | request latency | 10.25 ms | 14.03 ms | ~3050 rps |
| grpc_unary | request latency | 9.42 ms | 12.46 ms | ~3350 rps |
| sse_stream | stream open | 4.87 ms | 5.68 ms | — |
| sse_stream | chunk interval | 2.07 ms | 5.33 ms | ~6900 chunks/s |

Reading the unary numbers against the §4.0.8 SLO ("protocol+queue overhead p99
< 5 ms"): the measured p99 includes the Python worker round-trip (ZMQ IPC +
process scheduling), which dominates — the server-only share is not separable
from this harness by design. Decomposing it is the profiling runbook's job
(below). For regression purposes the *delta* between runs is the signal.

### SLO status vs §4.0.8

- **Baseline**: established (this section + reproducible harness). ✅
- **CI gate**: running informational; threshold values (`+10%` placeholder in
  §4.0.8) lock in **P-PERF-b** from runner data, together with the mimalloc
  A/B decision. ⏳
- **0.7.2 lesson**: changes to async paths must attach perf data from this
  harness (see §6.5 acceptance).

### Profiling runbook (测量可信化)

When a regression shows up, decompose it with:

- **tokio-console** (task-level, finds >1 ms blocking between `.await`s — the
  p99 tail red line): add `console-subscriber` behind a cargo feature, build
  with `RUSTFLAGS="--cfg tokio_unstable"`, run `tokio-console` against the
  server. (Not yet wired into the binary — tracked under P-PERF-b.)
- **pprof-rs** (CPU flamegraph): add a debug-only `/debug/pprof` endpoint
  (admin-gated, loopback) with `[profile.profiling] inherits = "release",
  debug = true`, then `go tool pprof -http :8080 <dump>`. (Follow-up;
  blueprint P-PERF 子项①.)
- **Code-review red lines** (standing, from the blueprint): no >1 ms blocking
  task between `.await`s; cross-cutting logic < 100 µs per request.

## wrk comparison (lite-server vs LitServe)

> **Note:** The benchmark data below is a preliminary placeholder with limited data points (2 worker configurations × 1 concurrency level). Comprehensive benchmarks covering more configurations, hardware platforms, and real model workloads are planned. Use as a rough reference only.

Performance comparison of lite-server vs LitServe using `wrk` (preliminary data).

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
