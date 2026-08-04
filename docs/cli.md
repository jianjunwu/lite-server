# CLI Reference

[中文版](zh/cli.md)

## Installation

```bash
pip install lite-server
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
| `--concurrency` | int | 8 | Closed-loop worker coroutines (fixed pool) |
| `--duration` | float | 30.0 | Run for N seconds (mutually exclusive with `--requests`) |
| `--requests` | int | — | Run exactly N requests (mutually exclusive with `--duration`) |
| `--warmup-requests` | int | 0 | Warmup requests before measurement; samples discarded (recommended: ~= concurrency) |
| `--grace-period` | float | 30.0 | After the deadline, wait at most N seconds for in-flight requests to drain |
| `--payload` | string | `{"input": 1.0}` | Inline JSON request body |
| `--payload-file` | path | — | JSON file with request body; repeatable, round-robin |
| `--export` | path | — | Write authoritative JSON record to PATH (stdout table unchanged) |
| `--max-error-rate` | float | — | Exit 99 if failed/total exceeds R (e.g. 0.01) |
| `--max-p99` | float | — | Exit 99 if p99 latency exceeds MS milliseconds |

**Measurement contract** (closed-loop, service-time):

- Latencies are **service-time only**: queueing at the load generator is not measured (inherent to closed-loop; the output labels this as `load_mode: closed-loop`, `latency_basis: service-time`).
- Warmup samples are discarded; in-flight requests at the deadline are drained up to `--grace-period` (completions kept, the rest reported as `dropped_inflight`).
- Throughput = `successful / (last response − first request)` (measured window), percentiles use numpy `linear` interpolation (p50/p90/p95/p99/max reported).
- Insufficient samples (`< max(300, 10 × concurrency)`) and client CPU saturation (>70% of one core) produce explicit warnings in stdout and JSON.

**Exit codes**: `0` pass · `1` execution error (e.g. no requests completed) · `2` argument/payload error · `99` threshold violation.

```bash
# Benchmark my_model for 60 seconds with 16 concurrent requests
lite-server benchmark --model my_model --concurrency 16 --duration 60

# CI smoke: fixed count, warmup, JSON export, error-rate gate
lite-server benchmark --model my_model --requests 200 --concurrency 4 \
  --warmup-requests 4 --max-error-rate 0.01 --export smoke.json
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

**Pure static analysis — user code is never executed.** model.py is parsed as
AST (no import side effects), paths are confined to the repository root
(`..`/symlink escapes rejected with exit 2), and config.yaml is validated
through the same Rust serde path as `config-check`.

**Exit codes**: `0` no finding at `--fail-severity` · `1` finding(s) at or above it · `2` analysis itself failed (model/version not found, path escape).

| rule_id | severity | Trigger |
|---------|----------|---------|
| LS001 | error | `predict` not implemented (LitAPI base raises NotImplementedError) |
| LS002 | error | Zero or multiple LitAPI subclasses (most-derived class counts) |
| LS004 | error | config.yaml failed validation (Rust serde) or is not a mapping |
| LS005 | error | .py file has a syntax error |
| LS101 | warning | `max_batch_size > 1` but neither `batch` nor `unbatch` overridden |
| LS102 | warning | `setup` not overridden (base defaults to pass) |
| LS103 | warning | `stream: true` but `stream_predict` not overridden or not a generator |
| LS104 | warning | requirements.txt line not parseable |
| LS111 | warning | No `--version` given; resolved latest(1) |
| LS201 | info | Lifecycle hooks (`teardown`/`on_file_changed`) not overridden |
| LS202 | info | Possible LitAPI subclass with unresolvable base (no silent false negatives) |

The JSON report is schema v1 (`schema_version: 1`) and is the single
authoritative representation — CI gates and downstream tools should consume
it (via stdout or `--output-dir`), not the markdown rendering.

---

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
