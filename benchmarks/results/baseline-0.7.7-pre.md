# Baseline 0.7.7-pre

> Generated: 2026-07-29 17:10 | git: `f0fce6b` | model: `echo_model` (echo, zero compute)
> Machine: Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz, 2P/4L cores, 8GB RAM, Darwin 22.6.0
> workers=2 | duration=60s | warmup=3.0s | wrk -t min(4, concurrency)
> tokio threads: default (auto) | metrics off | log-level warning

## Reproduce

```bash
cargo build --release && uv run maturin develop --release
uv run python benchmarks/scripts/baseline.py --workers 2 --duration 60 --concurrency 1 16 64 --model echo_model
```

| mode | concurrency | rps | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | lat_mean (ms) | rss_peak (MiB) | requests | errors |
|------|------------|-----|----------|----------|----------|------------|---------------|----------------|----------|--------|
| core | 1 | 4.8 | 203.211 | 212.621 | 292.086 | 349.434 | 206.275 | 80.6 | 291 | 0 |
| core | 16 | 2235.6 | 6.450 | 10.401 | 77.682 | 192.614 | 8.688 | 87.7 | 134241 | 0 |
| core | 64 | 2279.4 | 25.511 | 37.577 | 95.502 | 217.691 | 29.137 | 91.7 | 136857 | 0 |
| cli | 1 | 4.7 | 203.383 | 235.637 | 323.933 | 537.501 | 213.386 | 106.8 | 281 | 0 |
| cli | 16 | 1825.8 | 7.234 | 19.265 | 155.033 | 208.590 | 13.474 | 109.3 | 109657 | 0 |
| cli | 64 | 2278.7 | 25.412 | 37.216 | 94.862 | 185.215 | 29.167 | 113.7 | 136799 | 0 |
