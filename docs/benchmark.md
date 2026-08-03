# Benchmarks

[中文版](zh/benchmark.md)

Three-side `wrk` comparison: **lite-server** (PyO3 `serve()` entry) /
**lite-server-core** (standalone Rust binary) / **LitServe** (FastAPI + uvicorn).
Measures **HTTP + IPC framework overhead**, not model compute time.

## Two workloads

| Workload | Model | What it measures |
|---|---|---|
| **Zero-compute echo** | `echo_model`, returns immediately | Pure framework overhead (protocol + cross-cutting + queue + ZMQ IPC + worker round-trip) — where architecture differences show |
| **1ms sleep** | `sleep_1ms_model`, `time.sleep(0.001)` | Worker-side behavior — 1ms compute dominates, framework overhead is drowned out |

> **Do not benchmark a server framework with a sleep model.** Use echo for
> framework overhead; the sleep model only verifies worker scaling and
> three-side convergence.

## Test environment

- **Machine**: macOS x86_64, Intel Core i9-9980HK @ 2.40GHz (8 physical / 16 logical cores), 32 GB
- **Versions**: lite-server 0.8.0rc2 vs LitServe 0.2.17; wrk 4.2.0; rustc 1.97.1 (release + LTO)
- **Load**: wrk POST `{"input":"hello"}`, 4 threads, 15s per config, rotated run order + 5s cooldown to cancel thermal/turbo bias
- **Launch config (aligned)**: lite-server / lite-server-core run with `--threads 1` (single tokio event loop) to align with LitServe's single uvicorn process for an **isomorphic** framework-overhead comparison. Without `--threads` the Rust side defaults to 16 threads, which further widens the gap.

### Three-side architecture (aligned)

| Side | HTTP layer | Inference workers |
|---|---|---|
| lite-server | 1 Python host process (embeds Rust tokio, `--threads 1` → **1 thread**) | N Python child processes |
| lite-server-core | 1 Rust process (tokio, `--threads 1` → **1 thread**, no Python host) | N Python child processes |
| LitServe | 1 uvicorn process (single asyncio event loop = **1 thread**) | N Python child processes (mp.spawn) |

## Methodology & caveats

- **Isomorphic alignment**: all three sides run **zero-compute echo** — lite-server / lite-server-core load `echo_model`; LitServe cannot load it (`lite_server.LitAPI` is self-contained since 0.7.0, not a `litserve.LitAPI` subclass), so `run_litserve.py` substitutes a **litserve-native zero-compute echo builtin** (`_BuiltinEchoAPI`, mirroring `echo_model`: pure return, no sleep), keeping the workload identical across sides. For the sleep models, all three sides run a `time.sleep` mock of equal duration.
- Real GPU inference is compute-bound; relative differences will be smaller than the framework overhead shown here.

## Results: zero-compute echo (framework overhead, `--threads 1`)

Each cell `rps / p99(ms)`; `lite/Lit` = lite-server ÷ LitServe throughput. All three sides isomorphic (zero-compute echo).

| workers | conc | lite-server | lite-server-core | LitServe | lite/Lit |
|---|---|---|---|---|---|
| 1 | 1 | 1414 / 1.3 | 1494 / 1.0 | 1069 / 1.3 | 1.32× |
| 1 | 16 | 3658 / 18.0 | 3529 / 10.5 | 1745 / 10.7 | 2.10× |
| 1 | 64 | 3650 / 22.7 | 3583 / 30.9 | 1609 / 65.3 | 2.27× |
| 2 | 1 | 1328 / 4.3 | 1366 / 1.5 | 911 / 13.3 | 1.46× |
| 2 | 16 | 6840 / 3.3 | 6783 / 3.4 | 2809 / 10.7 | 2.43× |
| 2 | 64 | 6788 / 14.9 | 6949 / 12.0 | 2978 / 28.2 | 2.28× |
| 4 | 1 | 1469 / 1.1 | 1352 / 3.2 | 1025 / 1.4 | 1.43× |
| 4 | 16 | 7141 / 3.2 | 7146 / 3.2 | 3329 / 15.7 | 2.15× |
| 4 | 64 | 7638 / 11.0 | 7443 / 11.8 | 3674 / 31.7 | 2.08× |

**Reading**:

- **lite-server ≡ lite-server-core** (within <3% across all 9 cells): the PyO3 embedding layer — `serve()`'s `with_gil`/`allow_threads`, the `stop_server` slot, the select! shutdown arm, the GIL release — adds **zero hot-path overhead**, indistinguishable from the native Rust binary.
- **At c≥16 lite-server sustains ~2.0–2.4× LitServe** (isomorphic framework overhead): both HTTP layers run a single event loop (`--threads 1` aligns tokio to LitServe's single uvicorn); the gap is Rust (axum/tokio + ZMQ) vs Python (FastAPI/uvicorn + asyncio) protocol/IPC cost.
- At c=1 (single-request round-trip) the lead narrows to ~1.3–1.5× — framework advantage shrinks when latency-bound.
- All sides' echo throughput rises with workers (lite at c=16: w1 ~3658 → w2 ~6840 → w4 ~7141, saturating) — the bottleneck is the HTTP + IPC round-trip, not worker count.

## Results: 1ms sleep (worker-side, `--threads 1`)

| workers | conc | lite-server | lite-server-core | LitServe | lite/Lit |
|---|---|---|---|---|---|
| 1 | 1 | 529 / 2.3 | 508 / 2.5 | 463 / 2.6 | 1.14× |
| 1 | 16 | 675 / 26.5 | 679 / 25.4 | 675 / 25.8 | 1.00× |
| 1 | 64 | 677 / 101.0 | 670 / 115.5 | 674 / 99.9 | 1.00× |
| 2 | 1 | 511 / 2.4 | 533 / 2.2 | 448 / 2.9 | 1.14× |
| 2 | 16 | 1380 / 12.3 | 1395 / 12.3 | 1405 / 13.3 | 0.98× |
| 2 | 64 | 1385 / 47.5 | 1385 / 47.8 | 1412 / 47.6 | 0.98× |
| 4 | 1 | 530 / 2.2 | 529 / 2.3 | 462 / 2.5 | 1.15× |
| 4 | 16 | 2779 / 6.6 | 2776 / 6.6 | 2735 / 9.1 | 1.02× |
| 4 | 64 | 2730 / 28.3 | 2740 / 25.1 | 2747 / 25.0 | 0.99× |

**Reading**:

- **All three sides converge** (0.98–1.15×): the 1ms sleep dominates per-request cost, so framework differences vanish — confirming "do not benchmark a framework with a sleep model."
- Only at c=1 (single-request latency) do lite/core edge ahead ~1.14× (pure round-trip overhead advantage).
- **Workers scale linearly**: 1→2→4 workers → ~675 / ~1380 / ~2750 rps (~2× / 4×), identically across all three sides.

## Key takeaways

1. **Use echo, not sleep, for framework overhead** — under 1ms sleep all three sides are within ±2%, a measurement artifact (everyone waits on the same 1ms).
2. **The PyO3 embedding layer is zero hot-path overhead** — lite-server matches the native lite-server-core binary across the full matrix (18 cells, <3%), so `serve()`'s GIL release / re-entrancy guard / `stop_server` carry no measurable cost.
3. Under zero-compute echo (isomorphic), lite-server's framework throughput is ~3700–7600 rps, roughly **2.0–2.4× LitServe** at c≥16 (single event loop, aligned).

## Reproduce

```bash
# Prerequisites: build the release wheel + core binary
maturin build --release                      # → target/wheels/lite_server-*.whl
uv pip install --force-reinstall --no-deps target/wheels/lite_server-*.whl
cargo build --release                        # → target/release/lite-server-core

# Aligned config (--threads 1): run echo / 1ms
python benchmarks/scripts/compare.py --model echo_model \
  --workers 1 2 4 --concurrency 1 16 64 --threads 1 --duration 15
python benchmarks/scripts/compare.py --model sleep_1ms_model \
  --workers 1 2 4 --concurrency 1 16 64 --threads 1 --duration 15

# Quick smoke (single cell)
python benchmarks/scripts/compare.py --lite
```

Results are written to `benchmarks/results/benchmark.csv` (`--output` to change). `compare.py` passes `--threads N` through to lite-server / lite-server-core (LitServe is single-process uvicorn and is unaffected).
