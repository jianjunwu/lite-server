# Benchmarks

[中文版](zh/benchmark.md)

## P-PERF-a baseline & perf-smoke (2026-08-02)

The first measured baseline (blueprint §4.0.8) plus the self-contained
**perf-smoke** harness that keeps it reproducible. The harness is a
**local/manual measurement tool** — it is not wired into CI. Shared GitHub
runners have >30% run-to-run variance, so a CI perf gate calibrated on them
would either flap (tight threshold) or catch nothing (loose threshold). Perf
gating is deferred until a low-variance (self-hosted/dedicated) runner exists;
until then regressions are caught by review (async-path changes attach perf
data per §6.5) and manual runs of this harness.

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
processes; `uv sync` in the repo root sets this up).

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
- **CI gate**: **not in CI.** Shared-runner variance (>30%) makes any absolute
  threshold either flap or catch nothing; gating is deferred until a
  low-variance runner exists. The `+10%` placeholder from §4.0.8 is dropped —
  it would have been pure noise.
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

> **Note:** Measured 2026-08-02 on a single workstation (Intel i9-9980HK) — see "Test Environment" below. A single machine × single model workload is not a verdict; use it as a directional reference, not a spec.

Performance comparison of lite-server vs LitServe using `wrk`. Two workloads are
reported: a **1ms sleep mock** (this section — measures worker-bound behavior,
where framework overhead is drowned out) and a **zero-compute echo model**
([below](#framework-overhead-comparison-zero-compute-echo-aligned) — measures
pure framework overhead, where the real differences show).

## Test Environment

- **Model**: 1ms `time.sleep()` CPU mock for the matrix below; zero-compute echo for the framework-overhead section
- **Tool**: `wrk` 4.2.0, POST requests (`{"input":"hello"}`), 30s per config, 4 threads
- **OS**: macOS x86_64, Intel Core i9-9980HK (8P/16L), 32 GB
- **Versions**: lite-server 0.7.8 (git `82b0535`) vs LitServe 0.2.17
- **HTTP-layer threading** (alignment matters for the comparison): LitServe runs
  a single uvicorn process with one asyncio event loop. lite-server /
  lite-server-core run tokio with N worker threads (`--threads N`, default =
  CPU cores). To compare like-for-like, the framework-overhead section pins
  `--threads 1` — one event-loop thread on both sides.

## Results (1ms sleep model)

### Throughput (req/s)

| Workers | Concurrency | lite-server | LitServe | lite-server-core | Speedup (ls/lit) |
|---------|-------------|-------------|----------|------------------|-------------------|
| 1 | 1 | 353 | 379 | 443 | 0.93x |
| 1 | 4 | 634 | 656 | 612 | 0.97x |
| 1 | 16 | 658 | 656 | 695 | 1.00x |
| 1 | 64 | 666 | 658 | 694 | 1.01x |
| 2 | 1 | 335 | 346 | 395 | 0.97x |
| 2 | 4 | 1,203 | 1,333 | 1,218 | 0.90x |
| 2 | 16 | 1,333 | 1,341 | 1,369 | 0.99x |
| 2 | 64 | 1,357 | 1,335 | 1,394 | 1.02x |
| 4 | 1 | 306 | 345 | 383 | 0.89x |
| 4 | 4 | 1,464 | 1,495 | 1,526 | 0.98x |
| 4 | 16 | 2,574 | 2,607 | 2,311 | 0.99x |
| 4 | 64 | 2,617 | 2,601 | 2,425 | 1.01x |

### p99 Latency (ms)

| Workers | Concurrency | lite-server | LitServe | lite-server-core |
|---------|-------------|-------------|----------|------------------|
| 1 | 1 | 4.08 | 5.47 | 2.80 |
| 1 | 4 | 8.54 | 7.23 | 7.46 |
| 1 | 16 | 29.59 | 28.04 | 31.98 |
| 1 | 64 | 144.68 | 149.18 | 141.82 |
| 2 | 1 | 4.07 | 3.59 | 4.59 |
| 2 | 4 | 7.04 | 6.23 | 11.71 |
| 2 | 16 | 21.18 | 25.99 | 21.32 |
| 2 | 64 | 57.37 | 60.57 | 67.80 |
| 4 | 1 | 4.41 | 3.86 | 3.20 |
| 4 | 4 | 6.17 | 3.41 | 3.19 |
| 4 | 16 | 8.17 | 27.35 | 9.01 |
| 4 | 64 | 50.34 | 52.38 | 66.78 |

### Analysis

**The three servers are within ~±10% of each other across the whole matrix** (0.89x–1.02x for `lite-server` vs LitServe, 0.89x–1.17x for `lite-server-core`). This is a different picture from earlier placeholder numbers (w=2/c=4: 1,583 vs 531 rps, "3.0x") — the main driver is LitServe itself: 0.2.17 does ~2.5x the throughput of the older release used for the placeholder data (531 → 1,333 rps at w=2/c=4), while lite-server is within a few percent of its earlier result.

**This parity is a measurement artifact, not a verdict.** The 1ms sleep is the bottleneck itself: one worker tops out at ~660 rps, four workers at ~2,600 rps, identical across all three servers once the sub-millisecond framework overhead is paid. The matrix says "three servers wait on the same 1ms", not "three servers perform equally". Framework differences are invisible under this load — they show up under a zero-compute echo model (next section): there lite-server delivers **2.5x LitServe's throughput** with the same workers and the same single-threaded HTTP layer.

**Key takeaway:** do not benchmark serving frameworks with a sleep model. Use the echo workload below for framework-overhead comparisons; re-validate with real model workloads before making a call.

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

## Framework-overhead comparison (zero-compute echo, aligned)

The echo model returns immediately (`benchmarks/models/echo_model`) — no sleep,
so every microsecond on the wire is framework cost. All servers run **2 workers**
(`workers_per_device: 2`); the HTTP layer is aligned per the threading note in
"Test Environment". 25-30s per config, same `wrk` load.

### Default form (lite-server tokio threads = auto/16, LitServe 1 process)

| Server | c=16 | c=64 |
|---|---|---|
| lite-server | 4,531 rps / p99 8.30 ms | 4,383 rps / p99 39.05 ms |
| lite-server-core | 4,927 rps / p99 28.56 ms | 5,808 rps / p99 23.09 ms |
| LitServe | 2,644 rps / p99 15.23 ms | 2,725 rps / p99 53.36 ms |

### Aligned (single event-loop thread on both sides: `--threads 1`)

| Server | c=16 | c=64 |
|---|---|---|
| lite-server | 6,606 rps / p99 4.94 ms | 6,574 rps / p99 36.65 ms |
| lite-server-core | 4,920 rps / p99 29.21 ms | 6,376 rps / p99 27.34 ms |
| LitServe | 2,568 rps / p99 31.03 ms | 2,679 rps / p99 70.78 ms |

**lite-server is ~2.5x LitServe with identical HTTP-layer resources** (6,606 vs
2,568 rps @ c=16) and keeps a tighter p99 tail at c=16 (4.94 vs 31.03 ms).
`lite-server-core` is on par with the wrapper under this load (per-request
fixed cost ~0.14 ms on both — the wrapper runs the tokio event loop on a
dedicated OS thread, not CPython's main thread).

**Key takeaway:** with HTTP-layer resources and workers aligned, lite-server
outperforms LitServe ~2.5x on framework overhead, and the Python wrapper
matches the raw binary.

## Notes

- These benchmarks measure HTTP + IPC overhead, not model compute time. Real models with GPU inference will show smaller relative differences.
- The 1ms sleep model is intentionally lightweight to isolate the serving framework overhead.
- Results may vary based on hardware, OS, and system configuration.
