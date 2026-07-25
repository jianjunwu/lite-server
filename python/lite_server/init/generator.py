"""Generate project files from templates."""

from __future__ import annotations

import importlib.resources
import textwrap
from pathlib import Path
from typing import Any


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


def _render_config_yaml(
    batch: bool = False,
    stream: bool = False,
    continuous_batching: bool = False,
    accelerator: str | None = None,
) -> str:
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

    if batch:
        lines.extend([
            "max_batch_size: 8",
            "batch_timeout: 0.01",
        ])
    if stream:
        lines.append("stream: true")
    if continuous_batching:
        lines.extend([
            "continuous_batching: true",
            "max_sequence_length: 2048",
        ])

    if accelerator:
        lines.extend([
            "",
            "# ===== Resource Allocation =====",
            f"accelerator: {accelerator}",
        ])

    lines.extend([
        "",
        "# ===== Queue & Timeout =====",
        "# max_queue_size: 1000       # Max pending requests per worker",
        "# queue_mode: per_worker     # per_worker | shared",
        "request_timeout: 30.0        # 0 = no limit (not recommended for production)",
        "",
        "# ===== Lifecycle =====",
        "# health_check_interval: 15.0  # Active health check interval (0 = disabled)",
        "",
        "# ===== Callbacks =====",
        "# Register callback classes to hook into the inference pipeline.",
        "# Each class must be a lite_server.Callback subclass (no-arg constructible).",
        "# callbacks:",
        "#   - callbacks.AuditLogger",
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
    # bidirectional: false         # Enable bidirectional streaming (WebSocket)

    # ===== Continuous Batching (LLM) =====
    # continuous_batching: false   # Enable continuous batching mode
    # max_sequence_length: 2048    # Max sequence length for continuous batching

    # ===== Resource Allocation =====
    # accelerator: null            # Accelerator type: cpu, cuda, mps, tpu, auto (null = cpu)
    # devices: null                # Device assignment (null = auto, or integer like 1)
    # workers_per_device: null     # Workers per device (null = 1)

    # ===== Queue & Timeout =====
    # max_queue_size: 1000         # Max pending requests per worker
    # queue_mode: per_worker       # Queue mode: per_worker or shared
    # request_timeout: 0.0         # Per-request hard timeout in seconds (0 = disabled)
    # max_requests: 0              # Auto-restart worker after N requests (0 = disabled)
    # max_requests_jitter: 0       # Random jitter for max_requests (prevents thundering herd)
    # health_check_interval: 15.0  # Active health check interval in seconds (0 = disabled)

    # ===== Heartbeat (Worker Liveness) =====
    # heartbeat_interval: 0.0     # Heartbeat probe interval in seconds (0 = disabled)
    # heartbeat_timeout: 5.0      # Max seconds to wait for a probe response
    # heartbeat_max_failures: 3   # Consecutive failures before killing the worker

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
    # hot_reload: false            # Enable file watching for hot reload
    # hot_reload_patterns:         # Glob patterns to watch (e.g., *.py, model_*.yaml)
    #   - "*.py"
    # hot_reload_interval: 1.0     # Polling interval in seconds

    # ===== Callbacks (Inference Pipeline) =====
    # List of fully-qualified Callback subclass paths.  Each class must be
    # no-arg constructible.  Callbacks chain in registration order and are
    # validated at load time (pre-0.7 function-style signatures are rejected).
    # See callbacks.py for an example.
    # callbacks:
    #   - "callbacks.AuditLogger"
    #   - "my_package.callbacks.CustomAuth"

    # ===== Custom Parameters =====
    # Add your own parameters below. Access them in model.py via self.config.get('key')
    # my_model_path: /opt/models/weights.pt
    # my_threshold: 0.5
""")


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

TEMPLATES = {
    "empty": {
        "model_py": _load_template("empty_model.py"),
        "config_yaml": _render_config_yaml(batch=False, stream=False),
    },
}


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
      # timeout: 30.0               # Per-request timeout in seconds
      # threads: null               # Tokio worker threads (null = auto = CPU cores)
      # cache_registry: false       # Cache model/version lookups in HTTP layer
      # graceful_timeout: 30.0      # Max seconds for graceful shutdown
      # keepalive_timeout: 5.0      # HTTP keep-alive timeout (0 = disable)

    grpc:
      enabled: {grpc}
      # max_workers: 10             # Max concurrent gRPC request handlers

    metrics:
      enabled: {metrics}

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
      control_mode: explicit        # explicit | poll | all
      load_models:
        - {model_name}
      # poll_interval: 5            # Seconds between repo scans (control_mode=poll)
      # models:                     # Per-model strategy overrides (advanced)
      #   - name: my_model
      #     load_policy: explicit   # explicit | all | latest
      #     versions_to_load: ["1"]
      #     default_version: "1"
      #     max_loaded_versions: 3  # Max versions kept loaded (null = unlimited)

    # Global defaults applied to every loaded model.  When set (non-null),
    # these override the corresponding per-model config.yaml values.
    model_defaults:
      # max_queue_size: null          # Override per-model queue depth
      # request_timeout: null         # Override per-model request timeout (seconds)
      # max_requests: null            # Override per-model max requests before recycle
      # max_requests_jitter: null     # Override per-model recycle jitter
      # health_check_interval: null   # Override per-model health check interval

    # Admin UI and API feature toggles.
    features:
      # timeline: false
      system_overview: true
      # custom_metrics: false
      benchmarks: true
      # playground: false
      alerts: true
      # version_compare: false
      streaming: true
      grpc_streaming: true
      sse: true
      websocket_streaming: true
      streaming_metrics: true
""")

CALLBACKS_PY = textwrap.dedent('''\
    """Example callbacks for the inference pipeline.

    Since 0.7.0, callbacks replace the old middleware system. Each data hook
    receives a single ``ctx`` (RequestContext) argument and may be sync or async.

    To activate, uncomment the ``callbacks`` key in config.yaml::

        callbacks:
          - callbacks.AuditLogger

    You can also declare callbacks directly on your LitAPI class::

        from lite_server import LitAPI, RequireApiKey

        class MyAPI(LitAPI):
            callbacks = (RequireApiKey(keys=["sk-xxx"]),)
    """
    import time

    from lite_server import Callback


    class AuditLogger(Callback):
        """Logs request method, route, and elapsed time for every request."""

        def on_request(self, ctx):
            ctx.state["_start_ns"] = time.time_ns()

        def on_response(self, ctx):
            start = ctx.state.pop("_start_ns", None)
            if start is None:
                return
            elapsed_ms = (time.time_ns() - start) / 1_000_000
            print(
                f"[AuditLogger] {ctx.meta.method} {ctx.meta.route} "
                f"→ {elapsed_ms:.2f}ms"
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
    """Generate a new lite-server project from a template."""

    def __init__(
        self,
        project_name: str,
        template: str,
        output_dir: Path | str = ".",
        model_name: str | None = None,
        options: dict[str, Any] | None = None,
    ):
        if template not in TEMPLATES:
            avail = ", ".join(TEMPLATES)
            raise ValueError(
                f"Unknown template '{template}'. Available: {avail}"
            )
        self.project_name = project_name
        self.template = template
        self.output_dir = Path(output_dir)
        self.model_name = model_name or "my_model"
        self.options = options or {}

    def generate(self) -> Path:
        """Create project files and return the project root path."""
        root = self.output_dir / self.project_name
        if root.exists():
            raise FileExistsError(f"Directory already exists: {root}")

        root.mkdir(parents=True)

        tmpl = TEMPLATES[self.template]
        model_dir = root / "model_repo" / self.model_name / "1"
        model_dir.mkdir(parents=True)

        # Resolve config from wizard overrides (empty template defaults)
        batch = self.options.get("batch", False)
        stream = self.options.get("stream", False)

        config_yaml = _render_config_yaml(
            batch=batch,
            stream=stream,
        )

        # Model files
        (model_dir / "model.py").write_text(tmpl["model_py"])
        (model_dir / "config.yaml").write_text(config_yaml)
        (model_dir / "config.yaml.example").write_text(CONFIG_YAML_EXAMPLE)

        # Callbacks example (always generated; uncomment callbacks in config.yaml to activate)
        (model_dir / "callbacks.py").write_text(CALLBACKS_PY)

        # Server config
        grpc = str(self.options.get("grpc", True)).lower()
        metrics = str(self.options.get("metrics", True)).lower()
        (root / "server.yaml").write_text(
            SERVER_YAML.format(
                grpc=grpc, metrics=metrics, model_name=self.model_name
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
