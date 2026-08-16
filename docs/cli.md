# CLI Reference

[中文版](zh/cli.md)

## Installation

```bash
pip install miraserver
```

## Global Options

```bash
lite-server -v, --version    Show version
lite-server -h, --help       Show help
```

## Subcommands

### `serve` — Start the Inference Server

```bash
lite-server serve [OPTIONS]
```

#### Config File

| Flag | Type | Description |
|------|------|-------------|
| `--config`, `-c` | string | Path to YAML configuration file |

#### Server Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--port` | int | 8000 | HTTP server port |
| `--host` | string | 0.0.0.0 | Bind address. Use `unix:/path/to/sock` for Unix domain sockets |
| `--timeout` | float | 30.0 | Global request timeout in seconds |
| `--log-level` | string | info | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `--log-info-output` | string | — | Separate file for info-level logs |
| `--log-error-output` | string | — | Separate file for error-level logs |
| `--log-rotation` | string | none | Log rotation strategy: `none`, `size`, `daily`, `hourly` |
| `--threads` | int | auto | Number of Tokio worker threads |
| `--graceful-timeout` | float | 30.0 | Max seconds to wait for in-flight requests during shutdown |
| `--keepalive-timeout` | float | 5.0 | HTTP keep-alive timeout in seconds. 0 = disable |

#### Port Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--grpc-port` | int | 8001 | gRPC server port |
| `--metrics-port` | int | 8002 | Prometheus `/metrics` endpoint port |

#### Model Repository

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model-repo` | string | ./model_repo | Model repository directory |

#### Feature Toggles

| Flag | Description |
|------|-------------|
| `--no-metrics` | Disable Prometheus metrics endpoint |
| `--no-grpc` | Disable gRPC server |
| `--no-streaming-metrics` | Disable streaming-specific metrics |

#### Model Defaults (Override All Models)

These flags set global defaults that override per-model `config.yaml` values.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--max-queue-size` | int | 1000 | Max pending requests per worker |
| `--max-requests` | int | 0 | Auto-restart worker after N requests (0 = disabled) |
| `--max-requests-jitter` | int | 0 | Random jitter range for `max_requests` to prevent thundering herd |
| `--request-timeout` | float | 0.0 | Per-request hard timeout in seconds (0 = disabled) |
| `--health-check-interval` | float | 15.0 | Active health check interval in seconds (0 = disabled) |
| `--ejection-error-threshold` | int | 3 | Consecutive errors before a worker is ejected (0 = disabled) |
| `--ejection-timeout` | float | 30.0 | Seconds an ejected worker stays out before auto-recovery |
| `--ejection-max-percent` | int | 50 | Max % of workers ejectable at once (1-100) |
| `--max-retries` | int | 3 | Retry a failed batch on another worker up to N times (0 = disabled) |
| `--startup-timeout` | float | 60.0 | Max seconds to wait for a worker ready handshake |
| `--health-check-timeout` | float | 5.0 | Seconds per health-check probe before timeout |
| `--worker-kill-timeout` | float | 10.0 | Seconds to wait for the OS to reap a killed worker |
| `--hook-http-timeout` | float | 5.0 | Seconds for a worker lifecycle HTTP hook request |

#### Examples

```bash
# Minimal — load models from ./model_repo
lite-server serve

# With config file
lite-server serve --config server.yaml

# Override port and log level
lite-server serve --config server.yaml --port 9090 --log-level debug

# Production — multiple workers, long graceful timeout
lite-server serve --config server.yaml \
  --graceful-timeout 60 \
  --keepalive-timeout 10 \
  --max-requests 1000 \
  --max-requests-jitter 100

# Disable gRPC and metrics
lite-server serve --no-grpc --no-metrics
```

---

### `config-check` — Validate Configuration

```bash
lite-server config-check <CONFIG>
```

Validates a YAML configuration file and reports errors.

```bash
lite-server config-check server.yaml
```

---

### `benchmark` — Run Benchmark

```bash
lite-server benchmark --model <MODEL> [OPTIONS]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--url` | string | http://127.0.0.1:8000 | Server URL |
| `--model` | string | (required) | Model name to benchmark |
| `--version` | string | (latest) | Model version |
| `--concurrency` | int \| start:end:step | 8 | Concurrency level, or sweep range (e.g. `1:16:2` → 1,3,5,…,15) |
| `--duration` | float | 30.0 | Run for N seconds (mutually exclusive with `--requests`) |
| `--requests` | int | — | Run exactly N requests (mutually exclusive with `--duration`) |
| `--warmup-requests` | int | 0 | Warmup requests before measurement; samples discarded (recommended: ~= concurrency) |
| `--grace-period` | float | 30.0 | After the deadline, wait at most N seconds for in-flight requests to drain |
| `--rate` | float | — | Constant arrival rate in req/s (open-loop); eliminates coordinated omission at the load generator |
| `--processes` | int | 1 | Split the client across N OS processes (each its own event loop + connection pool, one core per process). Concurrency/requests are split evenly; `--rate` is divided by N. Raw samples are merged exactly in the parent |
| `--latency-threshold` | float | — | During concurrency sweep, stop early when p99 exceeds MS |
| `--payload` | string | `{"input": 1.0}` | Inline JSON request body |
| `--payload-file` | path | — | JSON file with request body; repeatable, round-robin |
| `--payload-random` | string | — | Randomize id/request_id/uuid per request using TEMPLATE as the base JSON |
| `--export` | path | — | Write authoritative JSON record to PATH (stdout table unchanged) |
| `--max-error-rate` | float | — | Exit 99 if failed/total exceeds R (e.g. 0.01) |
| `--max-p99` | float | — | Exit 99 if p99 latency exceeds MS milliseconds |
| `--stream` | flag | off | Use SSE streaming endpoint `/v2/models/{m}/events` (mutually exclusive with `--bidi`) |
| `--bidi` | flag | off | Bidi session benchmark over WS `/stream` bidi mode; payload must be a JSON array `[open, chunk1, ...]` |
| `--model-type` | llm\|tts\|stt\|generic | llm | Streaming metric interpretation semantics (`generic`: common section only) |
| `--endpoint` | events\|decoupled | events | Streaming endpoint variant (`decoupled` → `/v2/models/{m}/decoupled`, requires `--stream`) |
| `--transport` | sse\|ws\|grpc\|h2 | sse (ws for `--bidi`) | Streaming transport (requires `--stream` for ws/grpc). `ws` → `/stream`\|`/decoupled-stream`; `grpc` → StreamInfer\|DecoupledInfer over an insecure channel to the `--url` host:port; `h2` → `/bidi` (bidi only, h2c prior-knowledge) |
| `--pace` | float | — | Bidi real-time pacing: seconds between chunks (requires `--bidi`; default: lock-step) |
| `--rt-factor` | float | — | Bidi speedup pacing: divide `--pace` by N (requires `--pace`) |
| `--min-sessions` | int | 30 | Bidi: minimum completed sessions before the sample-size warning fires |
| `--cancel-after` | int | — | Cancel each stream after N chunks — client-cancel scenario (requires `--stream`); cancels bucket under `canceled` in `error_kinds` |
| `--read-delay-ms` | float | — | Slow-consumer scenario: sleep MS after each chunk (requires `--stream`) |
| `--goodput` | string | — | SLO expression, e.g. `ttft:500 tpot:50 e2el:2000` (ms; requires `--stream`; `tpot` is llm-only) |
| `--slo-attainment` | float | 0.95 | Exit 99 if SLO attainment is below R (requires `--goodput`) |
| `--tokenizer` | string | — | Client-side exact token counting (local file or hub id; requires `--stream` + `--model-type llm`; needs `pip install miraserver[benchmark]`) |
| `--text-field` | string | text→token | Chunk JSON field holding the text to tokenize (requires `--tokenizer`) |
| `--stream-read-timeout` | float | 300.0 | Seconds between stream chunks before timeout |
| `--max-ttft-ms` | float | — | Exit 99 if TTFT p99 exceeds MS (requires `--stream`) |
| `--max-rtf` | float | — | Exit 99 if RTF p99 exceeds VAL (requires `--stream` + `--model-type tts/stt`) |
| `--header` / `-H` | string | — | Extra request header, repeatable (`-H "Authorization: Bearer x"`); sent on every request of every transport. Names are lowercased (HTTP/2 requirement) — for `--transport grpc` headers become per-call gRPC metadata, for `ws` they become handshake `additional_headers`, for `h2` they are appended to the POST header block |

**Measurement contract** (closed-loop: service-time; open-loop: `--rate`):

- **Closed-loop** (`load_mode: closed-loop`, `latency_basis: service-time`): queueing at the load generator is not measured (inherent to the model).
- **Open-loop** (`load_mode: open-loop`, `latency_basis: service-time`): requests dispatched on a fixed-interval schedule. Achieved dispatch rate (`achieved_rate`) is reported alongside throughput. A warning is emitted when the generator cannot sustain the target rate (schedule misses or semaphore saturation).
- Warmup samples are discarded; in-flight requests at the deadline are drained up to `--grace-period` (completions kept, the rest reported as `dropped_inflight`).
- Throughput = `successful / (last response − first request)` (measured window), percentiles use numpy `linear` interpolation (p50/p90/p95/p99/max reported).
- Insufficient samples (`< max(300, 10 × concurrency)`) and client CPU saturation (>70% of one core) produce explicit warnings in stdout and JSON.
- **Multi-process client** (`--processes N`, default 1): the Python (httpx) client holds the GIL, so a single event loop is limited to one core — beyond its capacity, client CPU saturation inflates recorded latency (the `>70%` warning fires) and caps achieved throughput. `--processes N` splits concurrency (and `--requests`/`--rate`) across N OS processes via unified `spawn`, scaling client capacity by N cores. Raw samples are merged exactly in the parent: percentiles and stream/bidi metrics are recomputed on the union of samples, the window is the union of first/last timestamps, and the sample-size warning is re-checked on the merged count (children suppress their own). The CPU-saturation warning stays per process. Export JSON records `config.processes`.

**Streaming measurement contract** (`--stream`):

- The stream adapter wraps the SSE response as a unary callable; `latency_ms` still reports e2e stream latency at the request level. Per-chunk metrics (TTFT, ITL, TPOT) are in the `"stream"` JSON section.
- Empty chunks (keepalives, `data: [DONE]`) are filtered from TTFT / ITL / chunk count but their bytes are still counted.
- **LLM** (`--model-type llm`, default): token count defaults to `chunk_count` (estimated). When the model emits `token_count` in chunk metadata, the basis becomes `exact`. Mixed (some with, some without) is labeled `mixed` and metrics still compute with a warning.
- **TTS** (`--model-type tts`): RTF = `total_ms / audio_duration_ms` from chunk metadata.
- **STT** (`--model-type stt`): RTF = `total_ms / audio_duration_ms` from the request payload. **Convention**: include `"audio_duration_ms"` (float, milliseconds) in the JSON payload — the CLI extracts it automatically. Requests without `audio_duration_ms` are excluded from RTF calculation.
- **Generic** (`--model-type generic`): common section only (chunks_per_request / TTFT / e2e) — no ITL/tokens/RTF. Intended for decoupled and other non-token streams.
- **Decoupled** (`--endpoint decoupled`): benchmarks `POST /v2/models/{m}/decoupled` (server-push, `is_final` terminated). Same SSE wire format as `/events`; typically paired with `--model-type generic`. Set `decoupled_idle_timeout_secs` large enough that it never fires within the run — idle truncation is client-indistinguishable from a normal close.
- **Transports** (`--transport`): `sse` (default, httpx) · `ws` (websockets; Binary frames = chunks, Text = `{"done":true}`/`{"error":...}` control) · `grpc` (StreamInfer/DecoupledInfer, insecure channel, payload as JSON bytes). `--endpoint` selects the endpoint variant per transport (`events` ↔ `/stream` ↔ StreamInfer). Note: for `grpc`, `--stream-read-timeout` is a whole-RPC deadline (gRPC semantics), not a per-chunk idle budget.
- **Sweep + rate + version**: `--stream` composes with `--concurrency start:end:step`, `--rate`, and `--version` — no extra flags needed.
- **Thresholds**: `--max-ttft-ms` and `--max-rtf` gate on p99; fail-closed (exit 2 when used without `--stream`).

**Bidi session contract** (`--bidi`, WS transport only for now):

- The benchmark unit is a **session**: open → paced chunks → close. The payload is a JSON array — element 0 is the open payload (sent as the Text first frame → `on_open`), the rest are data chunks (each JSON-serialized into a Binary frame → `on_chunk`).
- Session metrics (JSON `"bidi"` section): open latency, close→final latency, session e2e duration, chunks/session, sessions/sec; per-chunk roundtrip percentiles in **lock-step** mode only.
- **Lock-step** (default) requires the model to return a response from every `on_chunk` **and** an `on_open` ready response — sparse-response models must use `--pace` (real-time) or `--pace` + `--rt-factor` (speedup), which do not pair chunks with responses.
- `--stream-read-timeout` is the per-frame idle budget; a session failing it counts as failed. `--max-p99` gates on session e2e duration. Sample-size warning threshold is `--min-sessions` (default 30), not the 300 used for requests.

**Streaming scenarios** (`--stream` only, all transports):

- **Mid-stream error** (E1): point at an error-injecting model (e.g. `examples/03_streaming`'s `stream_errors` with `mode=server_error`) and mix payloads via repeatable `--payload-file`; error frames bucket as `stream` in `error_kinds`, gated by `--max-error-rate`.
- **Client cancel** (E2): `--cancel-after N` aborts each stream after N chunks (connection teardown → server cancels the worker). Canceled streams bucket under `canceled` — not conflated with failures.
- **Slow consumer** (E3): `--read-delay-ms M` sleeps M after each chunk; ITL inflation shows server-side send blocking. Note: kernel buffers absorb small-chunk backpressure — this measures slow-drain behavior, not TCP-level backpressure. e2e includes one trailing delay.

**Goodput / SLO** (`--goodput`, `--stream` only):

- SLO expression: space-separated `key:threshold_ms` terms — `ttft` (time to first token), `tpot` (per-request time per output token, llm only), `e2el` (end-to-end latency). A request **attains** when every named metric is within its threshold (per-request evaluation, not percentiles).
- Output (JSON `stream.goodput`): `attainment` (attained/successful), `goodput_req_per_sec` (= attainment × throughput, vLLM semantics), `attainment_target`.
- Gate: attainment below `--slo-attainment` (default 0.95) → exit 99.
- A record missing a named metric (e.g. zero-chunk stream) counts as a violation.

**Exact token counting** (`--tokenizer`, `--stream --model-type llm` only):

- Loads a `tokenizers` tokenizer from a local file (`Tokenizer.from_file`) or a HuggingFace hub id (`from_pretrained`, needs network). Requires `pip install miraserver[benchmark]`.
- Per chunk, the text field (`--text-field`, default `"text"` then `"token"`) is tokenized client-side and fed into the token metrics — TPOT / tokens-per-sec become exact. Chunks whose meta already carries `token_count` keep the server value (never double-counted); chunks with no text count 0 and produce a warning.
- Token counting adds client CPU; watch the built-in CPU-saturation warning.

```bash
# SLO-gated run: exit 99 if <95% of requests meet ttft/tpot/e2e budgets
lite-server benchmark --model llama --stream --duration 60 \
  --goodput "ttft:500 tpot:50 e2el:2000" --slo-attainment 0.95

# Exact client-side token metrics for a model that can't report token_count
lite-server benchmark --model llama --stream --duration 60 \
  --tokenizer ./tokenizer.json --text-field text
```

```bash
# Client-cancel scenario: cancel every stream after 5 chunks
lite-server benchmark --model llama --stream --cancel-after 5 --requests 100

# Error-mix load: round-robin normal + erroring payloads, gate error rate
lite-server benchmark --model stream_errors --stream --requests 200 \
  --payload-file ok.json --payload-file err.json --max-error-rate 0.6
```

```bash
# Bidi lock-step: per-chunk roundtrip latency (echo-style models)
lite-server benchmark --model asr --bidi --duration 300 --concurrency 4 \
  --payload-file tests/fixtures/asr_session.json   # ["open", chunk1, chunk2, ...]

# Bidi real-time pacing at 25 fps ASR rhythm (320ms per chunk)
lite-server benchmark --model asr --bidi --pace 0.32 --duration 300 \
  --payload-file tests/fixtures/asr_session.json

# Bidi 2x speedup: find the overload knee
lite-server benchmark --model asr --bidi --pace 0.32 --rt-factor 2 --duration 300 \
  --payload-file tests/fixtures/asr_session.json
```

**Exit codes**: `0` pass · `1` execution error (e.g. no requests completed) · `2` argument/payload error · `99` threshold violation.

```bash
# Benchmark my_model for 60 seconds with 16 concurrent requests
lite-server benchmark --model my_model --concurrency 16 --duration 60

# CI smoke: fixed count, warmup, JSON export, error-rate gate
lite-server benchmark --model my_model --requests 200 --concurrency 4 \
  --warmup-requests 4 --max-error-rate 0.01 --export smoke.json

# LLM streaming: SSE endpoint with token-level metrics
lite-server benchmark --model llama --stream --model-type llm --duration 60 --concurrency 16

# TTS streaming: RTF-based evaluation
lite-server benchmark --model xtts --stream --model-type tts --concurrency 4 \
  --payload '{"text": "hello world"}'

# STT streaming: RTF from audio_duration_ms in payload
lite-server benchmark --model whisper --stream --model-type stt --duration 60 \
  --payload '{"audio_duration_ms": 5000}'

# Streaming with latency thresholds (exit 99 on violation)
lite-server benchmark --model llama --stream --requests 100 \
  --max-ttft-ms 200 --max-p99 500

# Decoupled streaming: server-push endpoint with generic metrics
lite-server benchmark --model detector --stream --endpoint decoupled \
  --model-type generic --duration 60 --concurrency 8

# WS transport streaming (websockets)
lite-server benchmark --model llama --stream --transport ws --duration 60

# gRPC transport streaming (StreamInfer over insecure channel)
lite-server benchmark --model llama --stream --transport grpc \
  --url http://127.0.0.1:8001 --duration 60

# Streaming concurrency sweep
lite-server benchmark --model llama --stream --concurrency 1:16:2 --duration 30
```

---

### `analyze` — Model Analyzer

```bash
lite-server analyze --model <MODEL> [OPTIONS]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model-repo` | string | ./model_repo | Model repository path |
| `--model` | string | (required) | Model name to analyze |
| `--version` | string | (latest) | Model version (default resolves latest and warns LS111) |
| `--format` | json\|markdown | json | Output format; markdown is rendered from the same schema v1 data |
| `--output-dir` | string | — | Additionally save report files (json+md) to DIR |
| `--fail-severity` | error\|warning | error | Minimum finding severity that exits 1 |
| `--strict` | flag | false | Shortcut for `--fail-severity warning` |
| `--deep` | flag | false | Resolve statically-unresolvable classes by importing model.py in an isolated subprocess (**executes model code** — opt-in) |
| `--deep-timeout` | float | 30.0 | Seconds before `--deep` import is killed |
| `--interop` | kserve-v2 | — | Run an optional interop profile check (kserve-v2: KServe V2 inference protocol; renamed from `--profile` by protocol-compat batch 3 — `--profile` remains a deprecated alias) |

**Pure static analysis — user code is never executed.** model.py is parsed as
AST (no import side effects), paths are confined to the repository root
(`..`/symlink escapes rejected with exit 2), and config.yaml is validated
through the same Rust serde path as `config-check`.

**Exit codes**: `0` no finding at `--fail-severity` · `1` finding(s) at or above it · `2` analysis itself failed (model/version not found, path escape).

| rule_id | severity | Trigger |
|---------|----------|---------|
| LS001 | error | `predict` not implemented (LitAPI base raises NotImplementedError) |
| LS002 | error | Zero or multiple LitAPI subclasses (most-derived class counts), or non-ensemble model missing model.py |
| LS004 | error | config.yaml failed validation (Rust serde) or is not a mapping |
| LS005 | error | .py file has a syntax error |
| LS101 | warning | `max_batch_size > 1` but neither `batch` nor `unbatch` overridden |
| LS102 | warning | `setup` not overridden (base defaults to pass) |
| LS103 | warning | `stream: true` but `stream_predict` not overridden or not a generator |
| LS104 | warning | requirements.txt line not parseable |
| LS111 | warning | No `--version` given; resolved latest(1) |
| LS112 | warning | `dag.py` declares an `EnsembleDAG` (E9-A) that drifted from config.yaml — or the declaration is missing/unevaluable (non-literal args, imports outside `lite_server(.ensemble)`) or config.yaml has no `ensemble` block. Evaluated via pure AST; the file is never executed |
| LS201 | info | Lifecycle hooks (`teardown`/`on_file_changed`) not overridden |
| LS202 | info | Possible LitAPI subclass with unresolvable base (no silent false negatives) |
| LS203 | warning | `--deep` import failed (timeout, non-zero exit, invalid output, or runtime error) |
| LS204 | info | `--deep` resolved API class at runtime |
| LS205 | info | `--deep` resolved a different API class than AST |
| LS301 | warning | Dynamic code execution: `eval()`/`exec()`/`compile()` |
| LS302 | warning | System call: `os.system()`/`subprocess.*` |
| LS303 | warning | Network call: `socket`/`urllib`/`requests`/`httpx` |
| LS304 | warning | Deserialization: `pickle.load()`/`torch.load()`/`yaml.load()` |
| LS305 | warning | Destructive filesystem: `os.remove()`/`os.unlink()`/`shutil.rmtree()` |
| LS401 | info | KServe V2: `decode_request`/`encode_response` overrides are asymmetric |
| LS402 | info | KServe V2: config.yaml has no `name`/`version` for model metadata endpoint |
| LS403 | info | KServe V2: `stream: true` but `stream_predict` not a generator |
| LS404 | warning | KServe V2: `predict` not implemented; V2 infer will return 500 |

The JSON report is schema v1 (`schema_version: 1`) and is the single
authoritative representation — CI gates and downstream tools should consume
it (via stdout or `--output-dir`), not the markdown rendering.

---

### `profile` — Configuration Space Search

```bash
lite-server profile --model <MODEL> [OPTIONS]
```

Search the configuration space (config points × concurrency) against a
**running** server. Config changes are applied through Admin ReloadModel —
the server re-reads the on-disk config.yaml (validate-then-swap), so the
server process is never restarted; workers are rebuilt per config point.
Resource metrics are generic (CPU/RAM, non-GPU) via the Prometheus
`/metrics` endpoint and local psutil sampling.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model` | string | (required) | Model name |
| `--version` | string | (resolved) | Model version |
| `--repo` | path | ./model_repo | Model repository path (preflight batching detection) |
| `--admin-url` | url | http://127.0.0.1:8000 | Admin endpoint (also the inference URL for trials) |
| `--metrics-url` | url | admin host:8002 | Prometheus /metrics endpoint |
| `--server-pid` | int | — | Server PID for local resource sampling (default: listener lookup by port) |
| `--sweep-knob` | KEY=v1,v2,v3 | — | Swept config key (repeatable). Batch keys require declared batch/unbatch; `workers_per_device` is dropped under `continuous_batching` |
| `--concurrency` | list | 1,2,4,8,16 | Inner-grid concurrency levels (zero reloads between levels) |
| `--search-mode` | grid\|quick | grid | Search strategy: full grid, or quick (single-key hill climb, ~<40% of the points) |
| `--max-trials` | int | 64 | Cross-product cap (grid) / point-measurement cap (quick) |
| `--duration` | float | 30.0 | Trial duration in seconds |
| `--requests` | int | — | Run exactly N requests per trial (mutually exclusive with `--duration`) |
| `--processes` | int | 1 | Split each trial's client across N OS processes; the worker pool is created once and reused across every trial (benchmark passthrough; see `benchmark --processes`) |
| `--export` | dir | — | Write per-trial JSON checkpoints + summary.json + report.md to DIR |
| `--resume` | dir | — | Re-analyze a complete checkpoint with new constraints, or continue an interrupted run (campaign hash must match) |
| `--reload-timeout` | float | 120.0 | Seconds to wait for Ready after ReloadModel |
| `--max-trial-failures` | int | 3 | Consecutive config-point failures before the circuit breaker aborts |
| `--objective` | throughput\|goodput\|sessions_per_sec | throughput | Ranking objective (goodput needs `--goodput`; sessions_per_sec is bidi-only) |
| `--top-n` | int | 3 | Top-N recommendations |
| `--max-p99` | float | — | Constraint: p99 latency budget in ms |
| `--min-throughput` | float | — | Constraint: minimum req/s |
| `--max-error-rate` | float | — | Constraint: max failed/total |
| `--max-ttft-ms` | float | — | Constraint: TTFT p99 budget in ms (streaming) |
| `--max-rtf` | float | — | Constraint: RTF p99 budget (TTS/STT) |
| `--max-session-ms` / `--max-chunk-roundtrip-ms` | float | — | Constraint: bidi session / chunk-roundtrip p99 budgets |
| `--max-rss-mb` | float | — | Constraint: process-tree RSS (local servers only) |
| `--apply-recommendation` | flag | false | Leave the top-1 config applied and reloaded after the run |
| `--dry-run` | flag | false | Print preflight conclusions + effective grid + estimated wall clock; zero side effects |
| `--force` | flag | false | Override the exclusivity guard (foreign traffic pollutes results) |
| `--recover` | flag | false | Restore config.yaml byte-exact from a stale `.profile.backup`, then exit |

**Benchmark passthrough** (streaming/bidi scenarios, plan §2.11):
`--stream`, `--bidi`, `--model-type`, `--endpoint`, `--transport`,
`--payload` / `--payload-file` / `--payload-random`, `--rate`,
`--warmup-requests`, `--header`, `--processes`, `--grace-period`, `--goodput`, `--slo-attainment`,
`--tokenizer`, `--text-field`, `--pace`, `--rt-factor`, `--min-sessions`,
`--cancel-after`, `--read-delay-ms`.

These passthrough flags share benchmark's combination validation (`--pace`
requires `--bidi`, `--stream --transport h2` is rejected, bidi payloads must
be JSON arrays, etc.): an invalid combination exits 2 before any network
contact, exactly as `benchmark` does. `--stream`/`--bidi` and
`--duration`/`--requests` are mutually exclusive.

**Preflight gates (any failure → exit 2):** server reachable + model loaded +
`/metrics` readable; server version ≥ 0.8.4 (the reload_model disk re-read
fix — the tagged v0.8.4-rc0 predates it and is refused; rc1+ and the final
release pass); exclusivity (`liteserver_queue_depth` == 0 AND
`liteserver_in_flight_requests` == 0 across all models, `--force` overrides);
batching declaration state from StaticAnalyzer (AST, zero execution) decides
the swept key set (undeclared/unknown → batch keys dropped; declared → batch
grid from 2, never 1; `continuous_batching` → `workers_per_device` dropped).

**Mechanism:** per config point — atomically rewrite config.yaml (ruamel
round-trip, comments preserved; pyyaml validation net fail-closed; tmp +
os.replace) → Admin ReloadModel → poll Ready (+ ACTIVE_WORKERS == expected)
→ inner concurrency sweep (zero reloads) → next point. On completion the
original config.yaml is restored **byte-exact** and the server reloaded back
to baseline; a failed restore is a profile failure (exit 2). SIGINT →
best-effort restore. A stale `.profile.backup` (SIGKILL residue) blocks the
run until `--recover` or manual cleanup.

**Exit codes**: `0` recommendation(s) · `1` no trial satisfies the constraints
· `2` failure (preflight refusal, grid conflict, restore failure, circuit
breaker, campaign mismatch).

### `pack` — Pack Model into Artifact

```bash
lite-server pack <MODEL_DIR> --version <VERSION> [OPTIONS]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `model_dir` | string | (positional) | Model directory to pack |
| `--version`, `-v` | string | (required) | Version number |
| `--name`, `-n` | string | (auto-inferred) | Model name (default: inferred from directory name) |
| `--output`, `-o` | string | ./artifacts | Output directory |

```bash
lite-server pack model_repo/my_model/1 --version 1 --output ./artifacts
```

---

### `unpack` — Unpack Artifact

```bash
lite-server unpack <ARTIFACT> [OPTIONS]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `artifact` | string | (positional) | Path to .lma artifact file |
| `--to` | string | . | Target directory |
| `--flat` | flag | false | Extract files directly without model name subdirectory |

---

### `init` — Initialize Project

```bash
lite-server init [PROJECT_NAME] [OPTIONS]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `project_name` | string | (positional) | Project directory name (or model name with `--model-only`) |
| `--wizard`, `-w` | flag | false | Interactive wizard mode |
| `--model-only` | flag | false | Generate only `model_repo/<name>/1/` (model.py, callbacks.py, config.yaml, config.yaml.example) — no project shell; fails if the directory exists |

```bash
# Create a new project
lite-server init my-server

# Interactive wizard
lite-server init --wizard

# Add a model to an existing project (no project shell)
lite-server init --model-only my_model
# -> creates model_repo/my_model/1/ — load it via orchestration.load_models
```

---

## Configuration Precedence

Values are resolved in this order (highest priority first):

1. **CLI flags** — always win
2. **Server YAML file** (`--config`)
3. **Built-in defaults**

For model config:

1. **CLI model defaults** (`--max-queue-size`, `--max-requests`, etc.)
2. **Per-model `config.yaml`** (`model_repo/<name>/<version>/config.yaml`)
3. **Built-in defaults**

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Rust tracing filter (advanced) |
