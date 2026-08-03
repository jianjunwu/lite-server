"""Generate project files from templates."""

from __future__ import annotations

import importlib.resources
import textwrap
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_template(name: str) -> str:
    """Load a template file from the package."""
    pkg = "lite_server.init.templates"
    try:
        ref = importlib.resources.files(pkg) / name
        return ref.read_text(encoding="utf-8")
    except (FileNotFoundError, AttributeError):
        # Fallback for older Python
        mod = __import__(pkg, fromlist=["__file__"])
        base = Path(mod.__file__).parent
        return (base / name).read_text(encoding="utf-8")


def _render_config_yaml() -> str:
    """Render concise active config for config.yaml."""
    lines = [
        "# Model version configuration",
        "#",
        "# This file controls inference behavior for this specific version.",
        "# All fields are optional. Omitted fields use system defaults.",
        "# Fields shown commented out are at their default value — uncomment",
        "# to override.",
        "#",
        "# TIP: All fields are accessible in model.py via self.config:",
        "#   value = self.config.get('my_custom_param', 'default_value')",
        "",
        "# ===== Inference Behavior =====",
    ]

    lines.extend([
        "",
        "# ===== Queue & Timeout =====",
        "# max_queue_size: 1000       # Max pending requests per worker",
        "request_timeout: 30.0        # 0 = no limit (not recommended for production)",
        "",
        "# ===== Lifecycle =====",
        "# health_check_interval: 15.0  # Active health check interval (0 = disabled)",
        "",
        "# ===== Worker Resilience =====",
        "# max_retries: 3                # retry a failed batch on another worker (0=disable)",
        "# ejection_error_threshold: 3   # consecutive errors to eject a worker (0=disable)",
        "# ejection_timeout: 30.0        # seconds before an ejected worker auto-recovers",
        "# ejection_max_percent: 50      # max % of workers ejected at once",
        "# startup_timeout: 60.0         # max seconds for a worker 'ready' handshake",
        "# health_check_timeout: 5.0     # seconds per health probe before timeout",
        "# worker_kill_timeout: 10.0     # seconds to wait for OS to reap a killed worker",
        "# hook_http_timeout: 5.0        # seconds for a lifecycle HTTP hook request",
        "",
        "# ===== Callbacks =====",
        "# Register callback classes to hook into the inference pipeline.",
        "# Each entry is a class path string (no-arg) or a single-key map",
        "# {path: kwargs} with constructor arguments. Built-ins live in",
        "# lite_server.callbacks (JsonSchemaValidator needs",
        "# `pip install lite-server[validation]`).",
        "# callbacks:",
        "#   - callbacks.AuditLogger",
        "#   - lite_server.callbacks.JsonSchemaValidator:",
        "#       input_schema:",
        "#         type: object",
        "#         required: [prompt]",
        "#         properties:",
        "#           prompt: { type: string, minLength: 1, maxLength: 4096 }",
        "",
        "# ===== Custom Parameters =====",
        "# Add your own parameters below. Access them in model.py via self.config.get('key')",
        "# my_threshold: 0.5",
    ])

    return "\n".join(lines) + "\n"


CONFIG_YAML_EXAMPLE = textwrap.dedent("""\
    # Complete reference for all model config fields.
    # Copy fields you need into config.yaml and uncomment them.

    # ===== Dynamic Batching =====
    # max_batch_size: 1            # Max requests to batch together (1 = disabled)
    # batch_timeout: 0.0           # Max seconds to wait before processing a batch
    # adaptive_batching: false     # Dynamically adjust batch timeout based on queue pressure
    # min_batch_timeout: 0.001     # Minimum batch timeout when adaptive_batching is enabled
    # adaptive_queue_threshold: 10 # Queue depth threshold for adaptive batching

    # ===== Streaming =====
    # stream: false                # Enable streaming output (requires stream_predict in model.py)

    # ===== Continuous Batching (LLM) =====
    # continuous_batching: false   # Enable continuous batching mode

    # ===== Resource Allocation =====
    # accelerator: null            # Accelerator type: cpu, cuda, mps, tpu, auto (null = cpu)
    # devices: null                # Device assignment (null = auto, or integer like 1)
    # workers_per_device: null     # Workers per device (null = 1)

    # ===== Queue & Timeout =====
    # max_queue_size: 1000         # Max pending requests per worker
    # request_timeout: 0.0         # Per-request hard timeout in seconds (0 = disabled)
    # max_requests: 0              # Auto-restart worker after N requests (0 = disabled)
    # max_requests_jitter: 0       # Random jitter for max_requests (prevents thundering herd)
    # health_check_interval: 15.0  # Active health check interval in seconds (0 = disabled)
    # queue_timeout_secs: 0.0      # P-FLOW B1: max queue-wait secs before queue_timeout_action (0 = off)
    # queue_timeout_action: delay  # P-FLOW B1: delay | reject (reject = 503 once queue wait elapses)

    # ===== Worker Resilience =====
    # max_retries: 3                # Retry a failed batch on another worker up to N times (0 = disable)
    # ejection_error_threshold: 3   # Consecutive errors before a worker is ejected (0 = disable)
    # ejection_timeout: 30.0        # Seconds an ejected worker stays out before auto-recovery
    # ejection_max_timeout: 300.0   # Cap for the per-worker circuit-breaker backoff
    # ejection_max_percent: 50      # Max % of workers that may be ejected at once (1-100)
    # startup_timeout: 60.0         # Max seconds to wait for a worker "ready" handshake
    # health_check_timeout: 5.0     # Seconds per health-check probe before timing out
    # health_check_kill_threshold: 0 # probe failures before kill+respawn (0=disabled); respawn reuses the worker's bound ZMQ socket
    # worker_kill_timeout: 10.0     # Seconds to wait for the OS to reap a killed worker
    # hook_http_timeout: 5.0        # Seconds for a worker lifecycle HTTP hook request

    # ===== Worker Lifecycle Hooks =====
    # hooks:
    #   on_ready: 'echo "Worker $WORKER_ID ready"'   # Shell command on worker ready
    #   on_exit: 'echo "Worker $WORKER_ID exited"'   # Shell command on worker exit
    #   on_error: 'echo "Worker $WORKER_ID error"'   # Shell command on worker error
    #   on_ready_http:                                  # HTTP callback on worker ready
    #     url: 'http://notify.internal/worker-ready'
    #     method: POST
    #     body_template: '{"model":"$MODEL","worker":$WORKER_ID}'
    #   on_exit_http:                                   # HTTP callback on worker exit
    #     url: 'http://notify.internal/worker-exit'
    #     method: POST
    #   on_error_http:                                  # HTTP callback on worker error
    #     url: 'http://notify.internal/worker-error'
    #     method: POST

    # ===== Hot Reload =====
    # Watches files of ALREADY-LOADED versions. On a matching change the
    # workers restart — or refresh in-process without a restart when
    # model.py defines on_file_changed (see the example in model.py).
    # hot_reload: false            # Enable file watching for hot reload
    # hot_reload_patterns:         # Glob patterns to watch (e.g., *.py, model_*.yaml)
    #   - "*.py"
    # Scope note: with control_mode=auto (server.yaml), adding/removing
    # version directories is handled by the server's reconcile task, not by
    # hot_reload. Auto-LOADING new version directories under hot_reload with
    # a non-auto control_mode is deprecated — load via the Admin API or use
    # control_mode=auto.

    # ===== Policies (enforced by the Rust server) =====
    # policies:
    #   auth: { header: "X-API-Key", keys: ["${API_KEYS}"] }  # ${VAR} = env var
    #   rate_limit: { requests_per_minute: 60, key: ip, burst: 100 }
    #   cors:
    #     allow_origins: ["https://app.example.com"]  # exact match; no reflection
    #     allow_methods: [POST]
    #     allow_headers: [content-type, authorization, x-request-id]
    #     expose_headers: [x-request-id, x-processing-time-ms, retry-after]
    #     allow_credentials: false   # true forbids ACAO:"*"
    #     max_age_secs: 7200         # Chrome caps at 7200
    #   request_log: {}
    #   warmup:                      # P-WARM: warm the engine before READY
    #     enabled: false             # off = ready immediately (legacy behavior)
    #     samples:                   # dummy inputs, consumed in order (M7 multi-sample)
    #       - input_ref: "warmup_input.json"  # raw /predict body under the model dir
    #         iterations: 1          # dummy inferences for this sample
    #     timeout_secs: 0.0          # 0 = use request_timeout

    # ===== Callbacks (Inference Pipeline) =====
    # List of Callback subclass entries.  Two entry forms:
    #   - "path.to.Callback"           # no-arg constructible
    #   - path.to.Callback: {kwargs}   # single-key map with constructor args
    # Callbacks chain in registration order and are validated at load time
    # (pre-0.7 function-style signatures are rejected).
    #
    # Built-in class: lite_server.callbacks.JsonSchemaValidator validates the
    # request body (on_request, before decode_request) and response body
    # (on_response; unary/batch only) against JSON Schemas — needs
    # `pip install lite-server[validation]`.
    #
    # Hooks beyond the data chain (on_request/on_input/on_output/on_response):
    #   on_stream_close(ctx, reason)       # stream end: "done"|"error"|"cancel";
    #                                      # ctx.stream_stats = {chunks, bytes}
    #   on_batch_input(ctx_list, batched)  # after batch(), before predict()
    #   on_batch_output(ctx_list, outputs) # after unbatch(), per-item outputs
    # ctx helpers: ctx.elapsed_ms(), ctx.deadline_remaining_ms(), ctx.mode,
    # ctx.stage.
    # callbacks:
    #   - "callbacks.AuditLogger"
    #   - lite_server.callbacks.JsonSchemaValidator:
    #       input_schema:
    #         type: object
    #         required: [prompt]
    #         additionalProperties: false
    #         properties:
    #           prompt: { type: string, minLength: 1, maxLength: 4096 }
    #           max_tokens: { type: integer, minimum: 1, maximum: 2048 }

    # ===== Custom Parameters =====
    # Add your own parameters below. Access them in model.py via self.config.get('key')
    # my_model_path: /opt/models/weights.pt
    # my_threshold: 0.5
""")


# ---------------------------------------------------------------------------
# Static file contents
# ---------------------------------------------------------------------------

SERVER_YAML = textwrap.dedent("""\
    # lite-server configuration
    #
    # Every supported parameter is listed below.  Parameters shown commented
    # out are at their default value — uncomment and change as needed.
    #
    # Per-model settings go in model_repo/<name>/<version>/config.yaml

    server:
      host: 0.0.0.0
      http_port: 8000
      grpc_port: 8001
      metrics_port: 8002
      # timeout: 30.0               # Per-request timeout in seconds (P-DEADLINE fallback)
      # threads: null               # Tokio worker threads (null = auto = CPU cores)
      # cache_registry: false       # Snapshot registry (strategy + active pins) on shutdown; restore on startup
      # graceful_timeout: 30.0      # Max seconds for graceful shutdown
      # keepalive_timeout: 5.0      # HTTP keep-alive timeout (0 = disable)
      # compression: false          # gzip HTTP responses (P1-4; SSE excluded)
      # socket_mode: 0o666          # chmod for a unix: UDS host (P4-1); 0o600 on
      #                              # multi-tenant hosts (HTTP UDS also serves admin)
      # trusted_proxies: []         # CIDRs whose XFF/X-Real-IP are honored (P-XFF);
      #                              # empty = use direct peer (fail-safe). Fronting
      #                              # gateway must be listed for client-IP rate limiting.
      # max_inflight: 0             # Global concurrent inference cap (P-FLOW); 0 = unlimited
      # max_request_body_bytes: null # Per-request body cap (P-FLOW); null = platform default
      # sequence_ttl_secs: 3600     # sequence_id affinity TTL (P8-1)
      # max_sequences: 65536        # Max tracked sequence_id entries (P8-1)
      # balance_abs_threshold: 2   # P8-1: in-flight gap to fall back from sticky affinity (0 = off)
      # balance_rel_threshold: 1.5 # P8-1: relative load multiplier (1.5 = +50%; 0.0 = off)
      # decoupled_idle_timeout_secs: 300 # Decoupled stream idle reclaim (P9-1)
      # cors:                       # Global CORS policy (P-CORS); null = pass-through
      #   allow_origins: ["https://app.example.com"]
      #   allow_methods: [GET, POST, PUT, DELETE]
      #   allow_headers: [content-type, authorization, x-request-id, traceparent, baggage]
      #   expose_headers: [x-request-id, x-processing-time-ms, retry-after]
      #   allow_credentials: true
      #   max_age_secs: 7200
      # tls_cert_path: null         # TLS cert (P5-1); set with tls_key_path to enable
      # tls_key_path: null          # TLS key (P5-1)
      # mtls_ca_path: null          # Client CA → require client certs (mTLS, P5-1)
      # tls_min_version: "1.2"      # "1.2" or "1.3" (P5-1)

    grpc:
      enabled: {grpc}
      # max_workers: 10             # Max worker processes per model; 0 = no cap
      # host: null                  # gRPC bind (null = follow server.host); unix:/path = UDS (P4-1)
      # admin_bind: null            # Separate admin+health bind (P7-2); unix:/path forced 0o600
      # socket_mode: 0o666          # chmod for a unix: gRPC UDS (P4-1)
      # response_compression: false # gzip gRPC responses (P1-3)
      # http2_keepalive_interval_secs: null # HTTP/2 keepalive (P1-2); null = off
      # http2_keepalive_timeout_secs: null  # Needs interval to take effect (P1-2)
      # http2_adaptive_window: false        # Adaptive conn window (P1-2)
      # http2_max_frame_size: null          # Max HTTP/2 frame bytes (P1-2)
      # tls_cert_path: null         # gRPC TLS (P5-1); set with tls_key_path
      # tls_key_path: null
      # mtls_ca_path: null
      # tls_min_version: "1.2"

    metrics:
      enabled: {metrics}
      # metric_namespace: liteserver # Prometheus name prefix (P2-1); "vllm" for GIE compat

    # Endpoint access control (P7-1). Unconfigured: admin = loopback fail-closed,
    # inference/health = public. Remote admin needs a key here or grpc.admin_bind.
    # access_control:
    #   admin:
    #     http:
    #       mode: key
    #       key: "x-api-key"
    #       value_env: "ADMIN_TOKEN"
    #     grpc:
    #       mode: key
    #       key: "api-key"
    #       value: "secret-token"
    #   inference:
    #     http:
    #       mode: public
    #   health:
    #     mode: public

    # OpenTelemetry export (P-TRACE). Default off = zero overhead.
    # telemetry:
    #   enabled: false              # opt-in
    #   otlp_endpoint: "http://localhost:4317"
    #   protocol: grpc             # grpc | http (http fails fast if unsupported)
    #   sample_ratio: 1.0
    #   health_admin_sample_ratio: 0.0  # down-sample high-frequency probes
    #   service_name: "lite-server"
    #   otlp_headers:               # OTLP auth headers map (P-TRACE); empty = none
    #     Authorization: "Bearer <token>"
    #   export_interval_millis: 5000 # Batch export interval (ms)
    #   max_queue_size: 2048        # Batch processor queue length
    #   metrics_enabled: false      # OTel metrics SDK + request-duration histogram
    #   exemplars_enabled: false    # trace-based exemplar filter
    #   baggage_allowlist: []       # Inbound baggage key allowlist (empty = drop all)
    #   baggage_max_entries: 16     # Max baggage entries kept after allowlist
    #   baggage_max_entry_bytes: 128 # Max bytes per baggage entry (key+value)

    logging:
      # level: info                 # Log level: trace, debug, info, warn, error
      # info_output: null           # Info log file path (null = stdout)
      # error_output: null          # Error log file path (null = stderr)
      # rotation: none              # Rotation strategy: none, size, daily, hourly
      # max_size: 100               # Max log file size in MB (when rotation=size)
      # backup_count: 7             # Number of rotated log files to keep
      # hostname_in_log_name: false # Include hostname in log filenames

    model_repository:
      path: ./model_repo

    orchestration:
      control_mode: explicit        # explicit | auto
      load_models:
        - {model_name}
      # poll_interval: 30           # Resync backstop in seconds (control_mode=auto);
                                    # directory events trigger near-real-time reconciles
      # models:                     # Per-model strategy overrides (advanced)
      #   - name: my_model
      #     load_policy: explicit   # explicit | all | latest
      #     versions_to_load: ["1"]
      #     default_version: "1"
      #     max_loaded_versions: 3  # Max versions kept loaded (null = unlimited)
      #     weights:               # Initial per-version traffic weights (canary)
      #       "1": 50
      #       "2": 50

    # Global defaults applied to every loaded model.  When set (non-null),
    # these override the corresponding per-model config.yaml values.
    model_defaults:
      # max_queue_size: null          # Override per-model queue depth
      # request_timeout: null         # Override per-model request timeout (seconds)
      # max_requests: null            # Override per-model max requests before recycle
      # max_requests_jitter: null     # Override per-model recycle jitter
      # health_check_interval: null   # Override per-model health check interval
      # max_retries: null             # Override per-model batch retry count
      # ejection_error_threshold: null  # Override per-model ejection threshold
      # ejection_timeout: null        # Override per-model ejection recovery seconds
      # ejection_max_percent: null    # Override per-model max ejectable worker %
      # startup_timeout: null         # Override per-model worker ready handshake timeout
      # health_check_timeout: null    # Override per-model per-probe timeout
      # health_check_kill_threshold: null  # Override per-model probe failures before kill+respawn
      # worker_kill_timeout: null     # Override per-model OS reap wait after kill
      # hook_http_timeout: null       # Override per-model lifecycle HTTP hook timeout

    # Server-level knobs (defaults shown; uncomment to tune).
    # tunables:
    #   reconcile_coalesce_secs: 2.0    # Coalesce window: burst of fs events -> one reconcile
    #   hot_reload_cooldown_secs: 3.0   # Per-model/version cooldown between hot reloads
    #   watcher_debounce_secs: 2.5      # File watcher debounce window
    #   file_changed_timeout_secs: 60.0 # Timeout for one worker's FILE_CHANGED round-trip
    #   worker_stderr_tail_bytes: 65536 # Max stderr bytes retained for crash diagnostics
    #   worker_stderr_drain_secs: 5.0   # Wait for an exited worker to flush stderr
    #   unpack_timeout_secs: 120.0      # Upper bound for one .lma unpack invocation

    # Admin UI and API feature toggles.
    features:
      # timeline: false
      # custom_metrics: false
      alerts: true
      # version_compare: false
      streaming: true
      grpc_streaming: true
      sse: true
      websocket_streaming: true
      streaming_metrics: true
      # canary_override: false    # P5-2: allow x-lite-version pin to bypass canary weights
""")

CALLBACKS_PY = textwrap.dedent('''\
    """Example callbacks for the inference pipeline.

    Since 0.7.0, callbacks replace the old middleware system. Each data hook
    receives a single ``ctx`` (RequestContext) argument and may be sync or async.

    To activate, uncomment the ``callbacks`` key in config.yaml::

        callbacks:
          - callbacks.AuditLogger

    Entries are class-path strings (no-arg) or single-key maps with constructor
    arguments — e.g. the built-in JsonSchemaValidator (needs
    ``pip install lite-server[validation]``)::

        callbacks:
          - lite_server.callbacks.JsonSchemaValidator:
              input_schema:
                type: object
                required: [prompt]
                properties:
                  prompt: { type: string, minLength: 1 }

    You can also declare callbacks directly on your LitAPI class::

        from lite_server import LitAPI
        from callbacks import AuditLogger

        class MyAPI(LitAPI):
            callbacks = (AuditLogger(),)
    """
    from lite_server import Callback


    class AuditLogger(Callback):
        """Logs request method, route, and elapsed time for every request."""

        def on_response(self, ctx):
            print(
                f"[AuditLogger] {ctx.meta.method} {ctx.meta.route} "
                f"→ {ctx.elapsed_ms():.2f}ms"
            )


    class StreamStats(Callback):
        """Logs chunk/byte stats when a streaming request ends.

        ``on_stream_close`` fires once per stream with reason
        ``"done" | "error" | "cancel"``; ``ctx.stream_stats`` carries
        ``{chunks, bytes}`` (uni-stream only).
        """

        def on_stream_close(self, ctx, reason):
            stats = ctx.stream_stats or {}
            print(
                f"[StreamStats] {ctx.meta.request_id} closed={reason} "
                f"chunks={stats.get('chunks', 0)} bytes={stats.get('bytes', 0)}"
            )
''')

TEST_REQUEST_PY = textwrap.dedent('''\
    """Test script for the {model_name} model."""
    import sys

    import requests

    BASE_URL = "http://127.0.0.1:8000"
    URL = BASE_URL + "/v2/models/{model_name}/infer"


    def test_infer():
        payload = {"{input_key}": "hello world"}
        try:
            resp = requests.post(URL, json=payload, timeout=10)
            resp.raise_for_status()
            print("Status:", resp.status_code)
            print("Response:", resp.json())
        except requests.exceptions.ConnectionError:
            print(
                f"Error: Cannot connect to {BASE_URL}."
                " Is the server running?"
            )
            sys.exit(1)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            if e.response is not None:
                print("Body:", e.response.text)
            sys.exit(1)


    def test_health():
        try:
            url = BASE_URL + "/v2/models/{model_name}/ready"
            resp = requests.get(url, timeout=10)
            print("Ready:", resp.status_code)
        except requests.exceptions.ConnectionError:
            print(
                f"Error: Cannot connect to {BASE_URL}."
                " Is the server running?"
            )
            sys.exit(1)


    if __name__ == "__main__":
        test_health()
        test_infer()
''')

README_MD = textwrap.dedent("""\
    # {project_name}

    Generated by lite-server.  All files are ready to run — no extra setup needed.

    ## Quick Start

    ```bash
    # Terminal 1: start the server
    lite-server serve --config server.yaml

    # Terminal 2: send a test request
    python test_request.py
    ```

    ## Project Structure

    ```
    {project_name}/
    ├── server.yaml                # Server configuration (ports, logging, orchestration)
    ├── Dockerfile                 # Container image
    ├── docker-compose.yml         # Local orchestration with healthcheck
    ├── Makefile                   # Common commands
    ├── test_request.py            # Quick test script
    ├── requirements.txt           # Python dependencies
    ├── .gitignore                 # Git ignore rules
    ├── model_repo/
    │   └── {model_name}/
    │       └── 1/
    │           ├── model.py             # LitAPI implementation (async + ctx)
    │           ├── callbacks.py         # Callback examples (auth, logging, rate limit)
    │           ├── config.yaml          # Active model config
    │           └── config.yaml.example  # Full parameter reference
    └── .github/
        └── workflows/
            └── ci.yml             # GitHub Actions CI
    ```

    ## Key Concepts (since 0.7.0)

    - **Async-first pipeline** — `decode_request`, `predict`, and `encode_response`
      support `async def` out of the box.  No separate base class needed.
    - **`ctx` parameter** — declare a `ctx` parameter in any method to access
      request metadata (headers, route, client IP, request ID) and per-request
      state via `ctx.state` / `ctx.respond(...)`.  In batch mode, `batch` /
      `unbatch` / `predict` receive a `list[RequestContext]` aligned with the
      batch items..
    - **Callbacks** — `callbacks.py` shows how to hook into the inference pipeline
      (logging, auth, rate limiting).  Uncomment the `callbacks` key in
      `config.yaml` to activate.

    ## Customizing

    | Goal | Where to look |
    |---|---|
    | Change ports / logging / model list | `server.yaml` |
    | Enable batching or streaming | `config.yaml` (see `config.yaml.example` for all options) |
    | Add auth / rate limiting / logging | `callbacks.py`, then uncomment `callbacks` in `config.yaml` |
    | Use GPU / change device count | `config.yaml` → `accelerator` / `devices` / `workers_per_device` |

    ## Commands

    | Command | Description |
    |---|---|
    | `lite-server serve --config server.yaml` | Start the server |
    | `python test_request.py` | Send a test inference request |
    | `lite-server benchmark --model {model_name}` | Run performance benchmark |
    | `lite-server config-check server.yaml` | Validate configuration |

    ## Learn More

    - [Model Authoring Guide](https://github.com/nic/lite-server/blob/main/docs/model-authoring.md)
    - [Configuration Reference](https://github.com/nic/lite-server/blob/main/docs/configuration.md)
    - [Examples](https://github.com/nic/lite-server/tree/main/examples)
""")

DOCKERFILE = textwrap.dedent("""\
    FROM python:3.11-slim

    WORKDIR /app

    # Install dependencies
    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt

    # Copy model repository and server config
    COPY model_repo ./model_repo
    COPY server.yaml .

    EXPOSE 8000 8001 8002

    CMD ["lite-server", "serve", "--config", "server.yaml"]
""")

MAKEFILE = textwrap.dedent("""\
    .PHONY: install serve test benchmark clean

    install:
    \tpip install -r requirements.txt

    serve:
    \tlite-server serve --config server.yaml

    test:
    \tpython test_request.py

    benchmark:
    \tlite-server benchmark --model {model_name} --duration 30

    clean:
    \trm -rf __pycache__ .pytest_cache *.log
""")

DOCKER_COMPOSE = textwrap.dedent("""\
    services:
      server:
        build: .
        ports:
          - "8000:8000"
          - "8001:8001"
          - "8002:8002"
        volumes:
          - ./model_repo:/app/model_repo
        environment:
          - PYTHONUNBUFFERED=1
          # - RUST_LOG=info         # Server log level
        restart: unless-stopped
        healthcheck:
          test: ["CMD", "curl", "-f", "http://localhost:8000/readyz"]
          interval: 30s
          timeout: 5s
          retries: 3
          start_period: 10s
""")

REQUIREMENTS_TXT = textwrap.dedent("""\
    lite-server
    requests

    # Optional: JSON-Schema validation for the built-in JsonSchemaValidator
    # callback (see config.yaml → callbacks). Uncomment to install:
    # lite-server[validation]

    # Add your model-specific dependencies below
""")

GITIGNORE = textwrap.dedent("""\
    # Python
    __pycache__/
    *.py[cod]
    *$py.class
    *.so
    .Python
    build/
    develop-eggs/
    dist/
    downloads/
    eggs/
    .eggs/
    lib/
    lib64/
    parts/
    sdist/
    var/
    wheels/
    *.egg-info/
    .installed.cfg
    *.egg

    # Virtual environments
    venv/
    ENV/
    env/
    .venv/

    # IDE
    .vscode/
    .idea/
    *.swp
    *.swo
    *~

    # Logs
    *.log
    logs/

    # Testing
    .pytest_cache/
    .coverage
    htmlcov/

    # Model artifacts
    *.pt
    *.pth
    *.onnx
    *.trt
    *.engine
    model_repo/**/checkpoints/
    model_repo/**/weights/
""")

CI_YML = textwrap.dedent("""\
    name: CI

    on:
      push:
        branches: [main]
      pull_request:
        branches: [main]

    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v4
          - uses: actions/setup-python@v5
            with:
              python-version: "3.11"
          - name: Install dependencies
            run: |
              pip install -r requirements.txt
          - name: Validate server config
            run: lite-server config-check server.yaml
          - name: Validate model configs
            run: |
              for cfg in model_repo/**/config.yaml; do
                lite-server config-check "$cfg" || true
              done
          - name: Lint model code
            run: |
              python -m py_compile model_repo/{model_name}/1/model.py
              pip install ruff
              ruff check model_repo/{model_name}/1/model.py
""")

# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


class ProjectGenerator:
    """Generate a new lite-server project."""

    def __init__(
        self,
        project_name: str,
        output_dir: Path | str = ".",
        model_name: str | None = None,
    ):
        self.project_name = project_name
        self.output_dir = Path(output_dir)
        self.model_name = model_name or "my_model"

    def generate(self) -> Path:
        """Create project files and return the project root path."""
        root = self.output_dir / self.project_name
        if root.exists():
            raise FileExistsError(f"Directory already exists: {root}")

        root.mkdir(parents=True)

        model_dir = root / "model_repo" / self.model_name / "1"
        model_dir.mkdir(parents=True)
        self._write_model_files(model_dir)

        # Server config
        (root / "server.yaml").write_text(
            SERVER_YAML.format(
                grpc="true", metrics="true", model_name=self.model_name
            )
        )

        # Test script
        tr = TEST_REQUEST_PY.replace("{model_name}", self.model_name)
        tr = tr.replace("{input_key}", "input")
        (root / "test_request.py").write_text(tr)

        # README
        (root / "README.md").write_text(
            README_MD.format(
                project_name=self.project_name,
                model_name=self.model_name,
            )
        )

        # Dockerfile
        (root / "Dockerfile").write_text(DOCKERFILE)

        # Makefile
        (root / "Makefile").write_text(
            MAKEFILE.format(model_name=self.model_name)
        )

        # Docker Compose
        (root / "docker-compose.yml").write_text(DOCKER_COMPOSE)

        # Requirements
        (root / "requirements.txt").write_text(REQUIREMENTS_TXT)

        # Git ignore
        (root / ".gitignore").write_text(GITIGNORE)

        # CI workflow
        ci_dir = root / ".github" / "workflows"
        ci_dir.mkdir(parents=True)
        (ci_dir / "ci.yml").write_text(
            CI_YML.format(model_name=self.model_name)
        )

        return root

    def _write_model_files(self, model_dir: Path) -> None:
        """Write the model version files (model.py / config.yaml / example / callbacks.py)."""
        model_py = _load_template("empty_model.py")
        config_yaml = _render_config_yaml()

        (model_dir / "model.py").write_text(model_py)
        (model_dir / "config.yaml").write_text(config_yaml)
        (model_dir / "config.yaml.example").write_text(CONFIG_YAML_EXAMPLE)

        # Callbacks example (always generated; uncomment callbacks in config.yaml to activate)
        (model_dir / "callbacks.py").write_text(CALLBACKS_PY)

    def generate_model_only(self) -> Path:
        """Create only the model version directory (no project shell).

        Writes model_repo/<model_name>/1/ under output_dir — use it to add a
        model to an existing project. Raises FileExistsError if the directory
        already exists.
        """
        model_dir = self.output_dir / "model_repo" / self.model_name / "1"
        if model_dir.exists():
            raise FileExistsError(f"Directory already exists: {model_dir}")
        model_dir.mkdir(parents=True)
        self._write_model_files(model_dir)
        return model_dir
