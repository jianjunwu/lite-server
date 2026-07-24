# Configuration Reference

[中文版](zh/configuration.md)

lite-server uses three layers of configuration: **server config** (YAML file or CLI), **model config** (per-model `config.yaml`), and **orchestration config** (`orchestration` section in `server.yaml`). CLI flags override YAML values.

## Server Configuration

Path: `server.yaml` (passed via `--config` or `-c`)

```yaml
server:
  http_port: 8000              # HTTP server port
  grpc_port: 8001              # gRPC server port
  metrics_port: 8002           # Prometheus metrics port
  host: 0.0.0.0                # Bind address (supports unix:/path/to/sock for UDS)
  timeout: 30.0                # Global request timeout (seconds)
  threads: null                # Tokio worker threads (null = auto = CPU cores)
  cache_registry: false        # Cache model registry to disk
  graceful_timeout: 30.0       # Max seconds to wait for in-flight requests during shutdown
  keepalive_timeout: 5.0       # HTTP keep-alive timeout (seconds), 0 = disable

logging:
  level: info                  # Log level: trace, debug, info, warn, error
  info_output: null            # Separate file for info-level logs
  error_output: null           # Separate file for error-level logs
  rotation: none               # none, size, daily, hourly
  max_size: 100                # Max log file size in MB (rotation=size)
  backup_count: 7              # Number of rotated log files to keep
  hostname_in_log_name: false  # Inject system hostname: server.log -> server-<host>.log

grpc:
  enabled: true                # Enable gRPC server
  max_workers: 10              # Max gRPC worker threads

metrics:
  enabled: true                # Enable Prometheus metrics endpoint

model_repository:
  path: ./model_repo           # Path to the model repository directory

endpoints_dir: ./endpoints     # Custom HTTP endpoints directory (optional)
                               # Scanned recursively for *.py files

features:
  timeline: false              # Enable historical metric timeline
  system_overview: true        # (reserved — not yet implemented)
  custom_metrics: false        # (reserved — not yet implemented)
  benchmarks: true             # (reserved — not yet implemented)
  playground: false            # (reserved — not yet implemented)
  alerts: true                 # Enable alert engine
  version_compare: false       # (reserved — not yet implemented)
  streaming: true              # Enable streaming endpoints
  grpc_streaming: true         # Enable gRPC streaming
  sse: true                    # Enable SSE streaming
  websocket_streaming: true    # Enable WebSocket streaming
  streaming_metrics: true      # Enable streaming-specific metrics

model_defaults:                # CLI-level defaults applied to all models
  max_queue_size: null         # Override max_queue_size for all models
  max_requests: null           # Override max_requests for all models
  max_requests_jitter: null    # Override max_requests_jitter for all models
  request_timeout: null        # Override request_timeout for all models
  health_check_interval: null  # Override health_check_interval for all models
```

## Model Configuration

Path: `model_repo/{model_name}/{version}/config.yaml`

All fields are optional. Omitted fields use defaults.

```yaml
# Batching
max_batch_size: 1              # Max requests per batch (1 = no batching)
batch_timeout: 0.0             # Max seconds to wait for batch to fill (0 = no waiting)
adaptive_batching: false       # Dynamically adjust batch timeout based on queue pressure
min_batch_timeout: 0.001       # Minimum batch timeout when adaptive_batching is enabled
adaptive_queue_threshold: 10   # Queue depth threshold for adaptive batching

# Streaming
stream: false                  # Enable streaming output (requires stream_predict in model.py)
bidirectional: false           # Enable bidirectional streaming

# Continuous Batching (LLM)
continuous_batching: false     # Enable continuous batching mode
max_sequence_length: 2048      # Max sequence length for continuous batching

# Worker Management
accelerator: null              # Accelerator type: "cpu", "cuda", "auto" (null = cpu)
devices: null                  # Device assignment (null = auto, or integer like 1)
workers_per_device: null       # Workers per device (null = 1)
max_queue_size: 1000           # Max pending requests per worker
queue_mode: per_worker         # Queue mode: "per_worker" or "shared"
request_timeout: 0.0           # Per-request hard timeout in seconds (0 = disabled)
max_requests: 0                # Auto-restart worker after N requests (0 = disabled)
max_requests_jitter: 0         # Random jitter for max_requests (prevents thundering herd)
health_check_interval: 15.0    # Active health check interval in seconds (0 = disabled)

# Heartbeat (Worker Liveness Detection)
heartbeat_interval: 0.0        # Heartbeat probe interval in seconds (0 = disabled)
heartbeat_timeout: 5.0         # Max seconds to wait for a probe response
heartbeat_max_failures: 3      # Consecutive failures before killing the worker

# Worker Lifecycle Hooks
hooks:
  on_ready: null               # Shell command on worker ready
  on_exit: null                # Shell command on worker exit
  on_error: null               # Shell command on worker error
  on_ready_http: null          # HTTP callback on worker ready
  on_exit_http: null           # HTTP callback on worker exit
  on_error_http: null          # HTTP callback on worker error
  # HTTP hook format:
  # on_ready_http:
  #   url: "http://notify.internal/worker-ready"
  #   method: POST             # GET or POST (default: POST)
  #   body_template: '{"model":"$MODEL","worker":$WORKER_ID}'
  # Available variables: $MODEL, $VERSION, $WORKER_ID, $EXIT_CODE, $REASON

# Hot Reload
hot_reload: false              # Enable file watching for hot reload
hot_reload_patterns:           # Glob patterns to watch
  - "*.py"
hot_reload_interval: 1.0       # Polling interval in seconds
```

## Orchestration Configuration

Path: `server.yaml` (`orchestration` section)

Controls which models and versions to load at startup.

```yaml
control_mode: explicit         # "explicit" (manual) or "auto" (poll for changes)
poll_interval: 5               # Poll interval in seconds (when control_mode=auto)
load_models:                   # List of model names to load
  - my_model
  - another_model
models:                        # Per-model version strategies
  - name: my_model
    load_policy: explicit      # "explicit", "latest", or "all"
    versions_to_load:          # Versions to load (when load_policy=explicit)
      - "1"
      - "2"
    default_version: "2"       # Version to activate by default
    max_loaded_versions: null  # Max versions to keep loaded (null = unlimited)
```

### Load Policies

| Policy | Behavior |
|--------|----------|
| `explicit` | Only load versions listed in `versions_to_load` |
| `latest` | Load only the latest version (highest version number) |
| `all` | Load all available versions |

## CLI Flags

All server config fields can be overridden via CLI flags:

```bash
python -m lite_server serve [flags]
```

| Flag | Description | Overrides |
|------|-------------|-----------|
| `--config`, `-c` | Path to YAML config file | — |
| `--port` | HTTP port | `server.http_port` |
| `--host` | Bind address | `server.host` |
| `--model-repo` | Model repository path | `model_repository.path` |
| `--endpoints-dir` | Custom endpoints directory | `endpoints_dir` |
| `--timeout` | Global request timeout | `server.timeout` |
| `--log-level` | Log level | `logging.level` |
| `--no-metrics` | Disable metrics | `metrics.enabled` |
| `--grpc-port` | gRPC port | `server.grpc_port` |
| `--no-grpc` | Disable gRPC | `grpc.enabled` |
| `--no-streaming-metrics` | Disable streaming metrics | `features.streaming_metrics` |
| `--max-queue-size` | Max queue size for all models | `model_defaults.max_queue_size` |
| `--max-requests` | Auto-restart after N requests | `model_defaults.max_requests` |
| `--max-requests-jitter` | Jitter for max_requests | `model_defaults.max_requests_jitter` |
| `--request-timeout` | Per-request timeout | `model_defaults.request_timeout` |
| `--health-check-interval` | Health check interval | `model_defaults.health_check_interval` |
| `--graceful-timeout` | Graceful shutdown timeout | `server.graceful_timeout` |
| `--keepalive-timeout` | HTTP keep-alive timeout | `server.keepalive_timeout` |

## Precedence

Configuration values are resolved in this order (highest priority first):

1. CLI flags
2. Server YAML file (`--config`)
3. Built-in defaults

For model config, the precedence is:

1. CLI `--max-queue-size`, `--max-requests`, `--max-requests-jitter`, `--request-timeout`, `--health-check-interval` (via `model_defaults`)
2. Per-model `config.yaml`
3. Built-in defaults

## Minimal Config Examples

### Development (single model, no config file)

```bash
python -m lite_server serve --model-repo ./my_models
```

### Production (multiple workers, custom ports)

```yaml
# server.yaml
server:
  http_port: 8080
  host: 0.0.0.0
  graceful_timeout: 60.0
  keepalive_timeout: 10.0

model_repository:
  path: /opt/models

features:
  alerts: true
  streaming: true

logging:
  level: info
  info_output: /var/log/lite-server/server.log
  rotation: size
  max_size: 100
  backup_count: 10
  hostname_in_log_name: false
```

### LLM Serving (streaming + continuous batching)

```yaml
# model_repo/llm/1/config.yaml
stream: true
continuous_batching: true
max_sequence_length: 4096
max_batch_size: 4
batch_timeout: 0.05
workers_per_device: 1
request_timeout: 120.0
```

### Production with Heartbeat and Hooks

```yaml
# model_repo/my_model/1/config.yaml
max_requests: 500
max_requests_jitter: 50

heartbeat_interval: 10.0
heartbeat_timeout: 5.0
heartbeat_max_failures: 3

hooks:
  on_ready: 'echo "Worker $WORKER_ID ready for $MODEL"'
  on_error: 'curl -s -X POST http://alerts.internal/worker-error -d "{\"model\":\"$MODEL\",\"worker\":$WORKER_ID,\"reason\":\"$REASON\"}"'
  on_exit_http:
    url: "http://notify.internal/worker-exit"
    method: POST
    body_template: '{"model":"$MODEL","version":"$VERSION","worker":$WORKER_ID}'
```
