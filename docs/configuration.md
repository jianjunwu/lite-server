# Configuration Reference

[中文版](zh/configuration.md)

lite-server uses three layers of configuration: **server config** (YAML file or CLI), **model config** (per-model `config.yaml`), and **orchestration config** (`orchestration` section in `server.yaml`). CLI flags override YAML values.

> **Upgrading from a previous major?** Breaking changes and old→new mappings: [migration.md](migration.md).

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
  cache_registry: false        # Snapshot registry (strategy + active-version pins) to
                               # <repo>/.lite-server-registry.json on shutdown; restore on
                               # startup. Corrupt-file tolerant; delete the file to reset.
  graceful_timeout: 30.0       # Max seconds to wait for in-flight requests during shutdown
  keepalive_timeout: 5.0       # HTTP keep-alive timeout (seconds); idle connections are
                               # reaped after this window (h1 idle reaper + slowloris-header
                               # guard). 0 = disable keep-alive entirely (h1-only; on TLS
                               # the h2 ALPN offer is dropped — h2 has no close semantic)
  stream_keepalive_interval_secs: 30.0  # Server-initiated liveness frames on streams
                               # (WS Ping / SSE `: keepalive` comment); 0 = off. Detects dead
                               # peers on silent streams and keeps NAT/LB state warm
  stream_channel_size: 64      # Per-stream chunk channel depth (worker->server, SSE, gRPC);
                               # a consumer lagging by more truncates the stream. Raise for
                               # burst tolerance (memory ~= size x chunk x concurrent streams)
  request_body_timeout_secs: 0.0  # Idle timeout for request-body reads (slowloris-body
                               # guard); resets as bytes flow, so large uploads are safe.
                               # 0 = off. h2 /bidi request bodies are exempt (idle is legal)
  http2_keepalive_interval_secs: null  # HTTP h2 PING interval (dead-peer detection only);
                               # null = off. Distinct from grpc.http2_keepalive_*
  http2_keepalive_timeout_secs: null   # h2 PING ack timeout (needs the interval set)
  max_connections: 0           # Hard cap on open HTTP connections (TCP+TLS); over-cap
                               # closed at accept. 0 = unlimited (default)
  compression: false           # gzip HTTP responses; SSE excluded, WS unaffected
  socket_mode: 0o666           # chmod for a unix: UDS host. The HTTP UDS also serves
                               # admin, so on multi-tenant hosts set 0o600 (owner-only).
  # TLS/mTLS (see "TLS / mTLS" section below) — all optional, off by default
  tls_cert_path: null          # Server certificate chain PEM; requires tls_key_path
  tls_key_path: null           # Server private key PEM; requires tls_cert_path
  mtls_ca_path: null           # Client CA bundle PEM; when set, client certs are REQUIRED (mTLS)
  tls_min_version: null        # "1.2" (default) or "1.3"
  # sequence_id sticky routing — opt-in per request via x-sequence-id /
  # the gRPC sequence_id field; absence leaves routing exactly as before.
  sequence_ttl_secs: 3600.0    # Seconds a sequence_id→worker mapping is kept after last use
  max_sequences: 65536         # Upper bound on tracked sequence_id entries (approx LRU)
  balance_abs_threshold: 2     # Abandon a sticky worker once its in-flight exceeds the
                               # least-loaded by this many (SGLang --balance-abs-threshold; 0 = off)
  balance_rel_threshold: 1.5   # Relative complement (...* multiplier; 0.0 = off)
  decoupled_idle_timeout_secs: 300.0  # Idle timeout (s) for a DecoupledInfer stream — no
                               # chunk within this window → server closes + cancels the worker.
                               # 0 = disabled (stream lives until model close / client cancel)
  # Overload protection — all default-off, behaviour unchanged
  max_inflight: 0             # Global in-flight inference cap. >0 → inference beyond this concurrent
                               # count is rejected 503 / gRPC Unavailable + Retry-After. Health/admin
                               # endpoints are exempt (probes stay reachable). 0 = unlimited.
  max_request_body_bytes: 67108864 # Per-request body cap (bytes). Oversized → HTTP 413 / gRPC
                               # ResourceExhausted. Default 64 MiB; null = platform default
                               # (axum 2MB / tonic 4MB). Memory budget: value × in-flight requests.
                               # Inference bodies only — artifact uploads are NOT gated by this.
  max_upload_bytes: null       # Model-repository UPLOAD cap (bytes): total per upload request,
                               # enforced mid-stream (HTTP multipart) / per message (gRPC
                               # UploadModel). Oversized → HTTP 413 / gRPC ResourceExhausted.
                               # null (default) = unlimited — artifacts are legitimately GB-scale.
  max_concurrent_streaming_dags: 128  # Global cap on concurrent STREAMING ensemble DAGs. Streaming
                               # steps bypass the queue (no backpressure), so this semaphore is the
                               # memory bound: worst-case residency ≈ value × 64 × max chunk size.
                               # Excess requests are rejected immediately (HTTP 429 / gRPC
                               # ResourceExhausted, no queueing). 0 = unlimited.
  # Trusted-proxy client-IP cleansing — fail-safe by default.
  trusted_proxies: []          # CIDRs/IPs of fronting proxies whose X-Forwarded-For /
                               # X-Real-IP are honored. Empty (default) = the direct TCP peer is
                               # always used and client proxy headers are IGNORED (prevents forged-
                               # IP rate-limit bypass). List your gateway/proxy here for its
                               # forwarded client IPs to reach key=ip rate-limiting.
  # Global CORS policy (applied when no per-model policies.cors matches, and
  # to non-model routes). null (default) = CORS pass-through (no headers attached).
  # Same shape as the per-model policies.cors (allow_origins/methods/headers,
  # expose_headers, allow_credentials, max_age_secs).
  cors: null

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
  host: null                   # gRPC bind host; null = follow server.host ("unix:/path" = UDS)
  # Separate bind for the LiteAdmin service only. UDS recommended — a UDS
  # admin socket is created owner-only (0o600) by default.
  admin_bind: null             # e.g. unix:/var/run/lite-admin.sock or 127.0.0.1:9001
  http2_keepalive_interval_secs: null  # HTTP/2 PING interval; null = disabled
  http2_keepalive_timeout_secs: null   # PING ack timeout (needs the interval set)
  http2_adaptive_window: false         # BDP-adaptive HTTP/2 flow-control window
  http2_max_frame_size: null           # Max HTTP/2 frame payload (bytes); null = tonic default
  response_compression: false          # gzip gRPC responses; inference service only
  reflection: false                    # gRPC server reflection (opt-in): grpcurl/grpcui service discovery; carries the Admin access class (fail-closed to loopback unless access_control admin is configured)
  socket_mode: 0o666                   # chmod for a unix: gRPC UDS
  # TLS/mTLS — same semantics as the server.* TLS keys, applied to the gRPC listener
  tls_cert_path: null          # Server certificate chain PEM; requires tls_key_path
  tls_key_path: null           # Server private key PEM; requires tls_cert_path
  mtls_ca_path: null           # Client CA bundle PEM; when set, client certs are REQUIRED (mTLS)
  tls_min_version: null        # "1.2" (default) or "1.3"

metrics:
  enabled: true                # Run the dedicated Prometheus listener (server.metrics_port,
                               # plaintext — see TLS notes below). Scope note: the main-port
                               # /metrics route is ALWAYS mounted (Admin endpoint class) and is
                               # NOT affected by this switch.
  # GIE/EPP-compatible metric namespace: exposes
  # {namespace}:total_queued_requests / {namespace}:kv_cache_utilization on /metrics
  # (vllm-compatible naming for the Kubernetes LLM-autoscaler ecosystem).
  # Invalid namespaces fail fast at startup.
  metric_namespace: liteserver
  # /metrics/timeline window (per model/version ring buffer)
  timeline_max_points: 30      # data points kept per series (30 × interval = history depth)
  timeline_sample_interval_secs: 10  # sampling interval; also the /metrics/timeline resolution
  # /metrics/timeline p99 sliding window (per model/version latency samples)
  p99_window_max_samples: 1000 # sample cap; high-QPS versions reach it quickly
  p99_window_max_age_secs: 0   # age bound for samples; 0 = off (count-bounded only).
                               # Set it on low-QPS deployments so p99 cannot go stale for hours.

alerts:
  # /alerts evaluation thresholds (features.alerts stays the on/off switch).
  # Defaults shown; the engine evaluates on demand per GET /alerts.
  queue_depth_warning: 100
  queue_depth_critical: 500
  p99_ms_warning: 500
  p99_ms_critical: 2000

rate_limit:
  max_buckets: 65536           # Max distinct rate-limit buckets (per IP/route key).
                               # Bounds memory under spoofed-source floods.
                               # 0 = unlimited.
                               # In-process (per-instance): N replicas → effective
                               # limit = N× configured value; use the upstream
                               # gateway for fleet-wide limits.

model_repository:
  path: ./model_repo           # Path to the model repository directory

features:
  # Breaking (migration M3): honor the x-lite-version canary-pin request
  # header. Default false = the header is IGNORED (clients cannot pin themselves
  # onto canary versions). Enable only in gray/debug environments.
  canary_override: false
  timeline: false              # Mount /metrics/timeline* and run the background sampler
  custom_metrics: false        # Register worker-declared custom metrics (opt-in)
  alerts: true                 # Mount /metrics/alerts
  version_compare: false       # Mount /v2/models/:model_name/compare
  streaming: true              # Master switch for SSE + WebSocket routes
  grpc_streaming: true         # stream_infer / decoupled_infer / bidi_stream RPCs (else Unimplemented)
  sse: true                    # SSE routes (also requires streaming: true)
  websocket_streaming: true    # WebSocket routes (also requires streaming: true)
  http_bidi: true              # h2 /bidi endpoint (also requires streaming: true)
  decoupled: true             # SSE /decoupled + WS /decoupled-stream (also requires streaming + transport toggle)
  streaming_metrics: true      # Streaming lifecycle metrics family (liteserver_stream*/streaming_*).
                               # Gating boundary + exempt metrics: observability.md §Streaming metrics.
                               # Independent of metrics.enabled — that one controls the dedicated
                               # listener, this one controls which series are recorded.

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
  ejection_max_timeout: null   # Override ejection_max_timeout for all models
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

## Access Control

Endpoint classes: `admin` (HTTP `/admin/*` + gRPC LiteAdmin service), `inference`, `health`. `admin` / `inference` are configured per protocol (`http` / `grpc`); `health` takes a single shorthand applied to both.

- **Defaults (fail-closed admin)**: unconfigured `admin` → **loopback only** (UDS counts as loopback); unconfigured `inference` / `health` → public. Breaking change — see [migration.md](migration.md) M4.
- **Modes** (`mode` tag): `public` (explicitly open — the escape hatch) or `key` (API key: `key` = header name; secret from `value` / `value_env` / `value_file`, first present wins, resolved at startup — a missing source fails fast).
- Key comparison is constant-time. Denials: HTTP 401 / gRPC Unauthenticated. The `metrics_port` listener is not covered — scrape Prometheus there.
- **Always combine `key` mode with TLS** (P5-1) — without TLS the API key travels in cleartext and can be intercepted off the wire.
- **Key rotation**: secrets are resolved once at startup. Rotate by updating the secret source (`value_env` / `value_file`) and performing a rolling restart; prefer a secret source over inline `value` so rotation touches the secret store, not the config file.

```yaml
access_control:
  admin:
    http: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }
    grpc: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }
  inference:
    http: { mode: public }       # explicit — same as the default
  health: { mode: public }       # shorthand, applies to http + grpc
```

Per-model `policies.auth` is independent and stacks after this endpoint-level control.

## OpenAI-Compact (`/v1`) Auth

The `openai_compact.auth` gate (openai-compact protocol, stage 6) locks **only the 5 `/v1` endpoints** (`/v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, `/v1/models`, `/v1/models/{model}`). KServe `/v2`, gRPC, custom routes and admin are untouched; **no loopback exemption** (once configured, every `/v1` request must carry the key).

- Same `mode` tag and secret sources as `access_control` (`value` / `value_env` / `value_file`, first present wins, resolved at startup — a missing source fails fast). Single key; rotate via the secret source + rolling restart.
- With the default header `authorization`, both `Authorization: Bearer <key>` (RFC 6750, what the official `openai` SDK sends) and the bare value are accepted, compared in constant time. A custom header name (e.g. `x-api-key`) is full-value comparison only.
- Denials: 401 with an OpenAI-shaped error body (`{"error": {message, type, param, code}}`). Unconfigured → current behavior (public, zero change).
- Independent of, and stacking with, both `access_control` and per-model `policies.auth`.

```yaml
openai_compact:
  auth:
    mode: key
    key: authorization            # OpenAI standard: Authorization: Bearer <key>
    value_env: OPENAI_API_KEY     # or value: "sk-..." / value_file: <path>
```

## CORS

Configured globally via `server.cors` or per model via `policies.cors` (per-model
overrides the global policy; omitting it falls back to `server.cors`, and `null`
defaults to pass-through — no headers attached). CORS is **not**
`tower-http::cors`: per-model policy override requires resolving the model from
the path at request time, which a statically-mounted `CorsLayer` cannot do. The
middleware resolves the effective policy (per-model → global) and applies the
rules below.

```yaml
server:
  cors:
    allow_origins: ["https://example.com"]  # exact match; "*" = any; "*.example.com" = subdomain wildcard
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]
    expose_headers: ["x-request-id", "x-processing-time-ms"]  # response headers visible to JS
    allow_credentials: false     # true → ACAC: true; forbidden with "*"
    max_age_secs: 7200           # preflight cache (s); Chrome caps at 7200
```

Eight security properties are enforced:

1. **Exact Origin match** — `Origin` is matched exactly against the configured
   `allow_origins` after normalization (lowercase scheme/host, default port
   stripped). No fuzzy matching.
2. **No reflection** — `Access-Control-Allow-Origin` is never set to the
   request's raw `Origin` as an echo. It is set only to (a) a configured origin
   that the request matched, or (b) the literal `*`. An unconfigured origin
   gets **no** ACAO.
3. **Reject `null`** — An `Origin: null` header (sandboxed iframes, `file://`,
   data URIs) is treated as no origin — no CORS headers are attached.
4. **No suffix confusion** — `https://evil-example.com` does not match
   `https://example.com`, and `https://a.notexample.com` does not match
   `https://*.example.com`. Subdomain wildcards (`*.example.com`) require a
   leading label (`a.example.com`) and never match the apex (`example.com`).
5. **Credentials + `*` rejected** — When `allow_credentials: true`, a wildcard
   `*` origin is **not** reflected — no ACAO is emitted (browsers forbid
   `Access-Control-Allow-Origin: *` together with
   `Access-Control-Allow-Credentials: true`). Configure explicit origins.
6. **`Vary: Origin` always** — Every CORS-relevant response carries
   `Vary: Origin` (preflight additionally carries
   `Vary: Access-Control-Request-Method` / `-Headers`) so a shared cache does
   not serve a response obtained for one Origin to a different Origin.
7. **Preflight validates method + headers** — A preflight (`OPTIONS` +
   `Access-Control-Request-Method`) attaches CORS headers **only** when the
   Origin is allowed **and** the requested method (`ACRM`) and every requested
   header (`ACRH`) are in the configured `allow_methods` / `allow_headers`. A
   non-qualifying preflight returns 204 with **no** CORS headers.
8. **`max_age` ≤ 7200** — `max_age_secs` defaults to 7200 — Chrome's cap on
   the preflight cache. Values above it are clamped by the browser anyway;
   configure ≤ 7200.

**Layering** — the CORS middleware is mounted **outside** access control: a
preflight `OPTIONS` short-circuits with 204 before authentication runs
(preflight carries no credentials). It is inside observability so the 204
carries `x-request-id`.

**WebSocket** — Browsers send no preflight and do not enforce ACAO on a
WebSocket handshake, so the CORS middleware cannot stop cross-site WebSocket
hijacking (CSWSH). The WS upgrade handler independently checks `Origin` against
the same engine (`ws_origin_allowed`). When no CORS policy is configured, WS
security relies entirely on access-control key authentication.

**Admin endpoints** — Admin-class endpoints are not browser-facing; the CORS
middleware skips them (no ACAO attached). Configure a global `server.cors`
policy only if you need cross-origin admin access.

## Telemetry / OpenTelemetry

Full OpenTelemetry tracing + metrics SDK, exported over **OTLP/gRPC**. Two-level opt-in: a build-time cargo feature (`--features telemetry`) and a runtime switch (`telemetry.enabled`, default `false` → zero overhead). Both off ⇒ no OTel layer, no propagator, no exporter; the server behaves exactly as without OTel. Trace context reaches the Python worker via the existing `RequestMeta.headers` map (W3C `traceparent`/`tracestate`/`baggage`) — the worker reads it to correlate but creates no span (Rust-only; see [observability.md](observability.md)).

```yaml
callbacks:
  timeout_secs: 0.0            # Per-callback execution timeout; 0 = off. Dispatch is
                               # always bounded (64 in-flight; over-cap drops are counted
                               # in liteserver_callback_dispatch_dropped_total)

telemetry:
  enabled: false                       # opt-in. false = no OTel (zero overhead).
  otlp_endpoint: "http://localhost:4317"  # OTLP/gRPC collector (4317).
  protocol: grpc                       # grpc only this period; http = startup fail-fast (reserved, M6).
  sample_ratio: 1.0                    # ParentBased(TraceIdRatioBased(ratio)).
  health_admin_sample_ratio: 0.0       # Per-class ratio for health/admin spans (0 = probes not sampled).
  service_name: "lite-server"
  resource_attributes: {}              # merged with OTEL_RESOURCE_ATTRIBUTES env.
  otlp_headers: {}                     # OTLP auth, e.g. {"Authorization":"Bearer ..."}.
  export_interval_millis: 5000
  max_queue_size: 2048
  metrics_enabled: false               # OTel metrics SDK overlay (C4 exemplars).
  exemplars_enabled: false             # (reserved) exemplar filter — see observability.md.
  # Inbound W3C baggage is untrusted (M6): only allowlisted keys are kept and
  # forwarded to workers. Default [] = drop ALL inbound baggage.
  baggage_allowlist: []                # e.g. ["tenant", "experiment"]
  baggage_max_entries: 16              # Cap on kept baggage entries.
  baggage_max_entry_bytes: 128         # Per-entry key+value byte cap.
```

- **Build**: `cargo build --features telemetry` (and `cargo test --features telemetry` for telemetry tests). The default build does not compile the OTel SDK/exporter.
- **Sampling**: roots sampled at `sample_ratio`; child spans honour the inbound sampled flag. Health/admin roots use the independent `health_admin_sample_ratio` (default `0.0`) so high-frequency probes do not burn collector quota.
- **Exemplars (C4)**: with `metrics_enabled`, an `liteserver.request.duration` histogram is recorded over OTLP/metrics alongside the existing `/metrics`. Note: `opentelemetry_sdk 0.30` stubs exemplar reservoirs; real trace-linked exemplars need an SDK upgrade (tracked). Prometheus exemplar-storage + Grafana complete the metrics→trace link.
- **Shutdown**: traces/metrics are force-flushed with a 5s cap during graceful shutdown.

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
# Overload queue control. Per request, set the
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
ejection_timeout: 30.0         # Base seconds an ejected worker stays out; backoff grows
                               # ×2 per consecutive ejection (half-open probe after each
                               # backoff: one success closes, one failure re-opens longer)
ejection_max_timeout: 300.0    # Cap for the per-worker circuit-breaker backoff (B1)
ejection_max_percent: 50       # Max % of workers ejectable at once (1-100)
startup_timeout: 60.0          # Max seconds to wait for a worker "ready" handshake
health_check_timeout: 5.0      # Seconds per health-check probe before timing out
health_check_kill_threshold: 0 # probe failures before kill+respawn (0=disabled); on reaching it the worker is killed and respawned, reusing its bound ZMQ socket
worker_kill_timeout: 10.0      # Graceful-stop budget: on unload/shutdown the worker receives a
                               # stop message and has this long to run teardown() and exit before
                               # SIGKILL; also the OS reap wait after the kill

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
  cors:                          # Per-model policy (overrides server.cors). Omit = fall back to global.
    allow_origins: ["https://example.com"]  # exact match; "*" = any; "*.example.com" = subdomain wildcard
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]
    expose_headers: ["x-request-id", "x-processing-time-ms"]  # response headers visible to JS
    allow_credentials: false     # true → ACAC: true; forbidden with "*"
    max_age_secs: 7200           # preflight cache (s); Chrome caps at 7200
  request_log: {}                # Access log: method, path, status, elapsed
  warmup:                        # Warm the engine before serving (default off)
    enabled: true                #   false = version goes straight to Ready (no behavior change)
    samples:                     #   dummy inputs, consumed in order — one file per input
                                 #   shape/batch (M7; legacy dummy_input_ref/iterations removed)
      - input_ref: warmup/batch1.json   # dummy request-body JSON, relative to the model dir
        iterations: 3            #   dummy inferences for this sample (default 1)
      - input_ref: warmup/batch8.json   # another shape/batch (default iterations: 1)
    timeout_secs: 30.0           #   per-warmup budget (0 = fall back to request_timeout; 0 there = no bound)

# Callbacks (data hooks around the inference pipeline)
callbacks:                     # Callback class paths loaded at worker startup
  - my_package.callbacks.AuditLogger
```

> **Note on `hot_reload` scope:** it restarts (or, with an
> `on_file_changed` hook, refreshes in-process) workers of
> **already-loaded** versions when their files change. A change to
> `config.yaml` itself bypasses the `on_file_changed` hook and always
> restarts the workers from the on-disk config (`max_batch_size` and other
> constructor parameters cannot be refreshed in-process); a reload that
> fails validation is refused **before** unloading, so the previous workers
> keep serving. With
> `control_mode: "auto"`, adding/removing version directories is handled
> exclusively by the reconcile task. The legacy behavior of auto-**loading**
> brand-new versions under `hot_reload: true` with a non-auto
> `control_mode` was **removed in 0.7.7** — only a **directory Create
> event** logs a WARN that a new version appeared (never auto-loaded);
> editing files inside an existing unloaded version directory is debug-level
> only, so normal development is not noisy. Load explicitly via the Admin
> API or switch to `control_mode: "auto"`.

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
| `--no-metrics` | Disable the dedicated metrics listener (main-port `/metrics` stays mounted) | `metrics.enabled` |
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
| `--ejection-timeout` | Ejected worker base backoff (s) | `model_defaults.ejection_timeout` |
| `--ejection-max-timeout` | Circuit-breaker backoff cap (s) | `model_defaults.ejection_max_timeout` |
| `--ejection-max-percent` | Max % workers ejectable | `model_defaults.ejection_max_percent` |
| `--max-retries` | Retry a failed batch on another worker | `model_defaults.max_retries` |
| `--startup-timeout` | Worker ready-handshake timeout (s) | `model_defaults.startup_timeout` |
| `--health-check-timeout` | Health probe timeout (s) | `model_defaults.health_check_timeout` |
| `--health-check-kill-threshold` | Probe failures before kill + respawn (0=disable) | `model_defaults.health_check_kill_threshold` |
| `--worker-kill-timeout` | Graceful-stop/teardown budget before SIGKILL; also OS reap wait after kill (s) | `model_defaults.worker_kill_timeout` |
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
