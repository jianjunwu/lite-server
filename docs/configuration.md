# Configuration Reference

[中文版](zh/configuration.md)

lite-server uses three layers of configuration: **server config** (YAML file or CLI), **model config** (per-model `config.yaml`), and **orchestration config** (`orchestration` section in `server.yaml`). CLI flags override YAML values.

## Server Configuration (`server.yaml`)

Path: `server.yaml` (passed via `--config` or `-c`)

```yaml
server:
  http_port: 8000              # HTTP server port
  grpc_port: 8001              # gRPC server port
  metrics_port: 8002           # Prometheus metrics port
  host: 0.0.0.0                # Bind address (supports unix:/path/to/sock for UDS)
  timeout: 30.0                # Global request timeout (seconds)
  threads: null                # Tokio worker threads (null = auto = CPU cores)
  cache_registry: false        # Cache model registry to disk (reserved — not yet implemented)
  graceful_timeout: 30.0       # Max seconds to wait for in-flight requests during shutdown
  keepalive_timeout: 5.0       # HTTP keep-alive timeout (seconds), 0 = disable
  # TLS/mTLS (see "TLS / mTLS" section below) — all optional, off by default
  tls_cert_path: null          # Server certificate chain PEM; requires tls_key_path
  tls_key_path: null           # Server private key PEM; requires tls_cert_path
  mtls_ca_path: null           # Client CA bundle PEM; when set, client certs are REQUIRED (mTLS)
  tls_min_version: null        # "1.2" (default) or "1.3"
  # sequence_id sticky routing (P8-1) — opt-in per request via x-sequence-id /
  # the gRPC sequence_id field; absence leaves routing exactly as before.
  sequence_ttl_secs: 3600.0    # Seconds a sequence_id→worker mapping is kept after last use
  max_sequences: 65536         # Upper bound on tracked sequence_id entries (approx LRU)
  balance_abs_threshold: 2     # B2: abandon a sticky worker once its in-flight exceeds the
                               # least-loaded by this many (SGLang --balance-abs-threshold; 0 = off)
  balance_rel_threshold: 1.5   # B2: relative complement (...* multiplier; 0.0 = off)
  decoupled_idle_timeout_secs: 300.0  # P9-1: idle timeout (s) for a DecoupledInfer stream — no
                               # chunk within this window → server closes + cancels the worker.
                               # 0 = disabled (stream lives until model close / client cancel)
  # P-FLOW overload protection (§4.0.9) — all default-off, behaviour unchanged
  max_inflight: 0             # Global in-flight inference cap. >0 → inference beyond this concurrent
                               # count is rejected 503 / gRPC Unavailable + Retry-After. Health/admin
                               # endpoints are exempt (probes stay reachable). 0 = unlimited.
  max_request_body_bytes: null # Per-request body cap (bytes). Oversized → HTTP 413 / gRPC
                               # ResourceExhausted. null = platform default (axum 2MB / tonic 4MB).
  # P-XFF trusted-proxy client-IP cleansing — fail-safe by default.
  trusted_proxies: []          # CIDRs/IPs of fronting proxies whose X-Forwarded-For /
                               # X-Real-IP are honored. Empty (default) = the direct TCP peer is
                               # always used and client proxy headers are IGNORED (prevents forged-
                               # IP rate-limit bypass). List your gateway/proxy here for its
                               # forwarded client IPs to reach key=ip rate-limiting.

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
  max_workers: 10              # Max gRPC worker threads (reserved — not yet implemented)
  # TLS/mTLS — same semantics as the server.* TLS keys, applied to the gRPC listener
  tls_cert_path: null          # Server certificate chain PEM; requires tls_key_path
  tls_key_path: null           # Server private key PEM; requires tls_cert_path
  mtls_ca_path: null           # Client CA bundle PEM; when set, client certs are REQUIRED (mTLS)
  tls_min_version: null        # "1.2" (default) or "1.3"

metrics:
  enabled: true                # Enable Prometheus metrics endpoint

rate_limit:
  max_buckets: 65536           # Max distinct rate-limit buckets (per IP/route key).
                               # Bounds memory under spoofed-source floods.
                               # 0 = unlimited.

model_repository:
  path: ./model_repo           # Path to the model repository directory

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
  max_retries: null            # Override max_retries for all models
  ejection_error_threshold: null   # Override ejection_error_threshold for all models
  ejection_timeout: null       # Override ejection_timeout for all models
  ejection_max_percent: null   # Override ejection_max_percent for all models
  startup_timeout: null        # Override startup_timeout for all models
  health_check_timeout: null   # Override health_check_timeout for all models
  health_check_kill_threshold: null  # Override health_check_kill_threshold for all models
  worker_kill_timeout: null    # Override worker_kill_timeout for all models
  hook_http_timeout: null      # Override hook_http_timeout for all models

tunables:                      # Server-level knobs (defaults shown; rarely need tuning)
  reconcile_coalesce_secs: 2.0     # Coalesce window: a burst of fs events -> one reconcile
  hot_reload_cooldown_secs: 3.0    # Cooldown between hot reloads per model/version
  watcher_debounce_secs: 2.5       # File watcher debounce window
  file_changed_timeout_secs: 60.0  # Timeout for one worker's FILE_CHANGED round-trip
  worker_stderr_tail_bytes: 65536  # Max stderr bytes retained for crash diagnostics
  worker_stderr_drain_secs: 5.0    # Wait for an exited worker to flush stderr
  unpack_timeout_secs: 120.0       # Upper bound for one .lma unpack invocation
```

## TLS / mTLS

Both listeners (HTTP `server.*` and gRPC `grpc.*`) support TLS and mutual TLS with rustls (pure-Rust, ring provider). TLS is opt-in per listener: set `tls_cert_path` + `tls_key_path` together.

```yaml
server:
  tls_cert_path: /etc/lite/tls/server.crt   # PEM chain (leaf first)
  tls_key_path: /etc/lite/tls/server.key    # PEM private key (chmod 600 recommended)
  mtls_ca_path: /etc/lite/tls/clients-ca.crt # optional: REQUIRE client certificates (mTLS)
  tls_min_version: "1.3"                     # optional; default "1.2"
grpc:
  tls_cert_path: /etc/lite/tls/server.crt   # independent settings from the HTTP listener
  tls_key_path: /etc/lite/tls/server.key
```

Rules enforced at startup:

- `tls_cert_path` / `tls_key_path` must be set **as a pair** — one without the other is a startup error.
- `mtls_ca_path` requires the pair (mTLS without a server certificate is meaningless).
- TLS is **mutually exclusive with UDS** (`unix:` host) — a Unix socket is already peer-credentialed.
- `tls_min_version` accepts `"1.2"` (default) or `"1.3"` only. TLS 1.3 is recommended.
- Invalid PEM, a key that does not match the certificate, or an empty CA bundle fail startup.

**Hot rotation (no restart).** The server watches the PEM files (10-second content poll, plus instant `SIGHUP` on Unix): when the files change — e.g. a cert-manager/Let's Encrypt renewal or a k8s secret-volume symlink swap — new connections use the new certificate, without dropping established connections. A failed reload (corrupt files, key/cert mismatch mid-rotation) keeps serving the previous certificate and logs an error; the next poll retries. The mTLS CA bundle rotates the same way.

**ALPN.** The gRPC listener advertises `h2` only; the HTTP listener advertises `h2` and `http/1.1`, so health probes and simple HTTPS clients keep working.

**mTLS client identity.** The verified client-certificate principal (URI SAN, else DNS SAN, else subject DN, else SHA-256 fingerprint) is recorded in the request context for access logs/audit. Access control does not consume it yet — API-key auth is configured separately per model (`policies.auth`).

**Deliberate policy notes:**

- **No CRL/OCSP revocation checking** — revocation is out of scope; rotate the CA bundle to exclude a compromised client.
- The `metrics_port` listener stays **plaintext and unauthenticated** — bind it to an internal network or loopback. With TLS enabled, the main port's Prometheus/probe/internal clients must use HTTPS (ALPN includes `http/1.1` for simple clients).
- The server warns at startup if the private key file is group/world-readable (`chmod 600` recommended); this is a warning, not a failure, to allow group-based deployments.

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

# Continuous Batching (LLM)
continuous_batching: false     # Enable continuous batching mode

# Worker Management
accelerator: null              # Device type label, passed through to device string (e.g. "cuda:0"); null = cpu
devices: null                  # Device assignment (null = auto, or integer like 1)
workers_per_device: null       # Workers per device (null = 1)
max_queue_size: 1000           # Max pending requests per worker
request_timeout: 0.0           # Per-request hard timeout in seconds (0 = disabled)
# P-FLOW B1 (§4.0.9) — overload queue control. Per request, set the
# `x-lite-priority` header (integer; higher = dispatched first, default 0).
queue_timeout_secs: 0.0        # Max seconds a request may wait in the queue before
                               # queue_timeout_action applies (0 = disabled, default).
queue_timeout_action: delay    # delay (default; let request_timeout govern) | reject
                               # (return 503 / gRPC Unavailable once queue_timeout_secs elapses)
max_requests: 0                # Auto-restart worker after N requests (0 = disabled)
max_requests_jitter: 0         # Random jitter for max_requests (prevents thundering herd)
health_check_interval: 15.0    # Active health check interval in seconds (0 = disabled)

# Worker Resilience
max_retries: 3                 # Retry a failed batch on another worker (0 = disable)
ejection_error_threshold: 3    # Consecutive errors before ejecting a worker (0 = disable)
ejection_timeout: 30.0         # Seconds an ejected worker stays out before auto-recovery
ejection_max_percent: 50       # Max % of workers ejectable at once (1-100)
startup_timeout: 60.0          # Max seconds to wait for a worker "ready" handshake
health_check_timeout: 5.0      # Seconds per health-check probe before timing out
health_check_kill_threshold: 0 # Consecutive probe failures before kill + respawn (0 = never)
worker_kill_timeout: 10.0      # Seconds to wait for the OS to reap a killed worker

# Worker Lifecycle Hooks
hooks:
  on_ready: null               # Shell command on worker ready
  on_exit: null                # Shell command on worker exit
  on_error: null               # Shell command on worker error
  on_ready_http: null          # HTTP callback on worker ready
  on_exit_http: null           # HTTP callback on worker exit
  on_error_http: null          # HTTP callback on worker error
  hook_http_timeout: 5.0       # Seconds for a lifecycle HTTP hook request
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

# Policies (enforced by the Rust server, per model version)
policies:
  auth: { header: "X-API-Key", keys: ["${API_KEYS}"] }  # ${VAR} = env var; empty keys = any non-empty value
  rate_limit: { requests_per_minute: 60, key: ip, burst: 100 }  # key: "route" | "ip"
  cors:
    allow_origins: ["https://example.com"]
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]
  request_log: {}                # Access log: method, path, status, elapsed

# Callbacks (data hooks around the inference pipeline)
callbacks:                     # Callback class paths loaded at worker startup
  - my_package.callbacks.AuditLogger
```

> **Note on `hot_reload` scope:** it restarts (or, with an
> `on_file_changed` hook, refreshes in-process) workers of
> **already-loaded** versions when their files change. With
> `control_mode: "auto"`, adding/removing version directories is handled
> exclusively by the reconcile task. The legacy behavior of auto-**loading**
> brand-new versions under `hot_reload: true` with a non-auto
> `control_mode` was **removed in 0.7.7** — new version directories are
> only logged; load explicitly via the Admin API or switch to
> `control_mode: "auto"`.

## Orchestration Configuration

Path: `server.yaml` (`orchestration` section)

Controls which models and versions to load at startup.

```yaml
control_mode: explicit         # "explicit", "auto" (reconcile repo changes), or "all" (load all models in repo)
poll_interval: 30              # Resync interval in seconds (when control_mode=auto)
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
    weights:                   # Canary traffic weights per version (versions not listed get weight 0)
      "1": 80
      "2": 20
```

### Load Policies

| Policy | Behavior |
|--------|----------|
| `explicit` | Only load versions listed in `versions_to_load` |
| `latest` | Load only the latest version (highest version number) |
| `all` | Load all available versions |

### Auto Mode (Reconcile)

With `control_mode: auto`, a background reconcile task keeps the registry in
sync with the model repository. Version directories appearing or
disappearing on disk trigger a reconcile in near-real-time (via the file
watcher, coalesced over a 2s window); every `poll_interval` seconds
(minimum 1, default 30) a full resync runs as a backstop in case watch
events are lost (e.g. on network filesystems):

- **Managed set**: the models listed in `load_models`. New version
  directories appearing on disk are loaded automatically (per each model's
  `load_policy`); versions removed from disk are unloaded.
- **Declarative semantics**: the orchestration config is the source of truth
  for managed models. Manual `load`/`unload` calls via the Admin API on a
  managed model are reverted on the next reconcile. Models not in
  `load_models` are left untouched.
- **Single authority**: in auto mode the file watcher never loads or
  unloads versions directly — it only restarts workers of already-loaded
  versions (`hot_reload`) and forwards lifecycle events to the reconcile
  task. All policy decisions (`load_policy`, `max_loaded_versions`) happen
  in one place.
- **Static config**: orchestration lives in `server.yaml` and is read once
  at startup; changing it requires a restart.
- **Capacity**: versions beyond a model's `max_loaded_versions` are skipped
  with a warning (no evict/reload thrash).
- For large repositories (>1000 models) increase `poll_interval` to reduce
  resync overhead.

## CLI Flags

All server config fields can be overridden via CLI flags:

```bash
lite-server serve [flags]
```

| Flag | Description | Overrides |
|------|-------------|-----------|
| `--config`, `-c` | Path to YAML config file | — |
| `--port` | HTTP port | `server.http_port` |
| `--host` | Bind address | `server.host` |
| `--model-repo` | Model repository path | `model_repository.path` |
| `--timeout` | Global request timeout | `server.timeout` |
| `--log-level` | Log level | `logging.level` |
| `--log-info-output` | Info log output file | `logging.info_output` |
| `--log-error-output` | Error log output file | `logging.error_output` |
| `--log-rotation` | Log rotation strategy (none/size/daily/hourly) | `logging.rotation` |
| `--no-metrics` | Disable metrics | `metrics.enabled` |
| `--grpc-port` | gRPC port | `server.grpc_port` |
| `--no-grpc` | Disable gRPC | `grpc.enabled` |
| `--no-streaming-metrics` | Disable streaming metrics | `features.streaming_metrics` |
| `--max-queue-size` | Max queue size for all models | `model_defaults.max_queue_size` |
| `--max-requests` | Auto-restart after N requests | `model_defaults.max_requests` |
| `--max-requests-jitter` | Jitter for max_requests | `model_defaults.max_requests_jitter` |
| `--request-timeout` | Per-request timeout | `model_defaults.request_timeout` |
| `--health-check-interval` | Health check interval | `model_defaults.health_check_interval` |
| `--threads` | Tokio worker threads | `server.threads` |
| `--metrics-port` | Metrics port | `server.metrics_port` |
| `--graceful-timeout` | Graceful shutdown timeout | `server.graceful_timeout` |
| `--keepalive-timeout` | HTTP keep-alive timeout | `server.keepalive_timeout` |
| `--ejection-error-threshold` | Errors to eject a worker (0=disable) | `model_defaults.ejection_error_threshold` |
| `--ejection-timeout` | Ejected worker auto-recovery (s) | `model_defaults.ejection_timeout` |
| `--ejection-max-percent` | Max % workers ejectable | `model_defaults.ejection_max_percent` |
| `--max-retries` | Retry a failed batch on another worker | `model_defaults.max_retries` |
| `--startup-timeout` | Worker ready-handshake timeout (s) | `model_defaults.startup_timeout` |
| `--health-check-timeout` | Health probe timeout (s) | `model_defaults.health_check_timeout` |
| `--health-check-kill-threshold` | Probe failures before kill + respawn (0=disable) | `model_defaults.health_check_kill_threshold` |
| `--worker-kill-timeout` | OS reap wait after kill (s) | `model_defaults.worker_kill_timeout` |
| `--hook-http-timeout` | Lifecycle HTTP hook timeout (s) | `model_defaults.hook_http_timeout` |

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
lite-server serve --model-repo ./my_models
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
# model_repo/my_model/1/config.yaml
stream: true
continuous_batching: true
max_batch_size: 4
batch_timeout: 0.05
workers_per_device: 1
request_timeout: 120.0
```

### Production with Health-Check Kill and Hooks

```yaml
# model_repo/my_model/1/config.yaml
max_requests: 500
max_requests_jitter: 50

# Probe every 10s; a worker that fails 3 consecutive probes is killed and respawned
health_check_interval: 10.0
health_check_timeout: 5.0
health_check_kill_threshold: 3

hooks:
  on_ready: 'echo "Worker $WORKER_ID ready for $MODEL"'
  on_error: 'curl -s -X POST http://alerts.internal/worker-error -d "{\"model\":\"$MODEL\",\"worker\":$WORKER_ID,\"reason\":\"$REASON\"}"'
  on_exit_http:
    url: "http://notify.internal/worker-exit"
    method: POST
    body_template: '{"model":"$MODEL","version":"$VERSION","worker":$WORKER_ID}'
```

## Build-Time Performance Options

### CPU target (`target-cpu`)

Rust release builds default to the baseline instruction set (x86-64 / generic
aarch64). Tuning `target-cpu` lets the compiler emit newer instructions
(AVX2, …), but a binary built for a newer CPU **crashes with SIGILL on older
CPUs** — choose per deployment target, never by "fastest on my machine".

Three-tier strategy:

1. **Local development** — set `target-cpu=native` in a machine-local
   `.cargo/config.toml` (do **not** commit it) for maximum dev-machine
   performance.
2. **CI / release builds for a known fleet** — pick the oldest CPU generation
   in the fleet: `x86-64-v2` (SSE4.2/POPCNT, ~2009+) or `x86-64-v3` (AVX2,
   ~2013+), e.g. `RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release`.
3. **Published artifacts (pip wheels)** — leave unset (baseline). Wheels are
   installed on unknown hardware; baseline is the only safe choice.

Apple Silicon / aarch64: `target-cpu` accepts e.g. `apple-m1` for known
targets; the same fleet rule applies.
