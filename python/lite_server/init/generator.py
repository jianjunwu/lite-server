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


def _render_config_yaml(batch: bool, stream: bool, continuous_batching: bool = False) -> str:
    lines = [
        "# Model version configuration",
        "#",
        "# This file controls inference behavior for this specific version.",
        "# Place it alongside model.py in model_repo/<name>/<version>/config.yaml",
        "#",
        "# All fields are optional. Omitted fields use defaults.",
        "#",
        "# TIP: All fields in this file are accessible in model.py via self.config:",
        "#   value = self.config.get('my_custom_param', 'default_value')",
        "",
        "# ===== Dynamic Batching =====",
        "# max_batch_size: 1            # Max requests to batch together (1 = disabled)",
        "# batch_timeout: 0.0           # Max seconds to wait before processing a batch",
        "# adaptive_batching: false     # Dynamically adjust batch timeout based on queue pressure",
        "# min_batch_timeout: 0.001     # Minimum batch timeout when adaptive_batching is enabled",
        "# adaptive_queue_threshold: 10 # Queue depth threshold for adaptive batching",
        "",
        "# ===== Streaming =====",
        "# stream: false                # Enable streaming output (requires stream_predict in model.py)",
        "# bidirectional: false         # Enable bidirectional streaming (WebSocket)",
        "",
        "# ===== Continuous Batching (LLM) =====",
        "# continuous_batching: false   # Enable continuous batching mode",
        "# max_sequence_length: 2048    # Max sequence length for continuous batching",
        "",
        "# ===== Resource Allocation =====",
        "# accelerator: null            # Accelerator type: cpu, cuda, mps, tpu, auto (null = cpu)",
        "# devices: null                # Device assignment (null = auto, or integer like 1)",
        "# workers_per_device: null     # Workers per device (null = 1)",
        "",
        "# ===== Queue & Timeout =====",
        "# max_queue_size: 1000         # Max pending requests per worker",
        "# queue_mode: per_worker       # Queue mode: per_worker or shared",
        "# request_timeout: 0.0         # Per-request hard timeout in seconds (0 = disabled)",
        "# max_requests: 0              # Auto-restart worker after N requests (0 = disabled)",
        "# max_requests_jitter: 0       # Random jitter for max_requests (prevents thundering herd)",
        "# health_check_interval: 15.0  # Active health check interval in seconds (0 = disabled)",
        "",
        "# ===== Heartbeat (Worker Liveness) =====",
        "# heartbeat_interval: 0.0     # Heartbeat probe interval in seconds (0 = disabled)",
        "# heartbeat_timeout: 5.0      # Max seconds to wait for a probe response",
        "# heartbeat_max_failures: 3   # Consecutive failures before killing the worker",
        "",
        "# ===== Worker Lifecycle Hooks =====",
        "# hooks:",
        "#   on_ready: 'echo \"Worker $WORKER_ID ready\"'   # Shell command on worker ready",
        "#   on_exit: 'echo \"Worker $WORKER_ID exited\"'   # Shell command on worker exit",
        "#   on_error: 'echo \"Worker $WORKER_ID error\"'   # Shell command on worker error",
        "#   on_ready_http:                                  # HTTP callback on worker ready",
        "#     url: 'http://notify.internal/worker-ready'",
        "#     method: POST",
        "#     body_template: '{\"model\":\"$MODEL\",\"worker\":$WORKER_ID}'",
        "#   on_exit_http:                                   # HTTP callback on worker exit",
        "#     url: 'http://notify.internal/worker-exit'",
        "#     method: POST",
        "",
        "# ===== Hot Reload =====",
        "# hot_reload: false            # Enable file watching for hot reload",
        "# hot_reload_patterns:         # Glob patterns to watch (e.g., *.py, model_*.yaml)",
        "#   - \"*.py\"",
        "# hot_reload_interval: 1.0     # Polling interval in seconds",
        "",
        "# ===== Custom Parameters =====",
        "# Add your own parameters below. Access them in model.py via self.config.get('key')",
        "# my_model_path: /opt/models/weights.pt",
        "# my_threshold: 0.5",
    ]

    # Add active configuration based on template options
    lines.extend(["", "# ===== Active Configuration =====", ""])

    if batch:
        lines.extend([
            "max_batch_size: 8",
            "batch_timeout: 0.01",
        ])
    else:
        lines.extend([
            "# max_batch_size: 8",
            "# batch_timeout: 0.01",
        ])

    if stream:
        lines.append("stream: true")
    else:
        lines.append("# stream: true")

    if continuous_batching:
        lines.extend([
            "continuous_batching: true",
            "max_sequence_length: 2048",
        ])
    else:
        lines.extend([
            "# continuous_batching: false",
            "# max_sequence_length: 2048",
        ])

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

TEMPLATES = {
    "empty": {
        "model_py": _load_template("empty_model.py"),
        "config_yaml": _render_config_yaml(batch=False, stream=False),
    },
    "llm": {
        "model_py": _load_template("llm_model.py"),
        "config_yaml": _render_config_yaml(batch=False, stream=True, continuous_batching=True),
    },
    "cv-classify": {
        "model_py": _load_template("cv_classify_model.py"),
        "config_yaml": _render_config_yaml(batch=True, stream=False),
    },
    "cv-detect": {
        "model_py": _load_template("cv_detect_model.py"),
        "config_yaml": _render_config_yaml(batch=True, stream=False),
    },
    "nlp": {
        "model_py": _load_template("nlp_model.py"),
        "config_yaml": _render_config_yaml(batch=True, stream=False),
    },
}


# ---------------------------------------------------------------------------
# Static file contents
# ---------------------------------------------------------------------------

SERVER_YAML = textwrap.dedent("""\
    # lite-server configuration
    # Docs: https://github.com/your-org/lite-server/docs
    #
    # Quick reference for common fields:
    #   server.host          - Bind address (0.0.0.0 = all interfaces)
    #   server.http_port     - HTTP REST API port
    #   server.grpc_port     - gRPC service port (requires grpc.enabled=true)
    #   server.metrics_port  - Prometheus /metrics endpoint port
    #   server.timeout       - Per-request timeout in seconds
    #   server.threads       - Tokio worker threads (None = auto = num CPUs)
    #   server.transport     - Worker transport: "mp" (multiprocessing) or "zmq"
    #
    #   model_repository.path - Root directory for model versions
    #
    #   orchestration.control_mode - How to load models:
    #     "explicit" = only load models listed in load_models
    #     "poll"     = auto-detect repo changes
    #     "all"      = load every discovered model on startup
    #
    #   orchestration.load_models  - List of model names to auto-load
    #
    # Per-model settings go in model_repo/<name>/<version>/config.yaml

    server:
      host: 0.0.0.0
      http_port: 8000
      grpc_port: 8001
      metrics_port: 8002
      # timeout: 30.0               # Request timeout in seconds
      # threads: 4                  # Tokio worker threads (None = auto = CPU cores)
      # transport: mp               # mp | zmq

    logging:
      level: info
      # info_output: null           # Info log file path
      # error_output: null          # Error log file path
      # rotation: none              # none, size, daily, hourly
      # max_size: 100               # Max log file size in MB (rotation=size)
      # backup_count: 7             # Number of rotated log files to keep

    grpc:
      enabled: {grpc}

    metrics:
      enabled: {metrics}

    model_repository:
      path: ./model_repo

    orchestration:
      control_mode: explicit
      load_models:
        - {model_name}
""")

TEST_REQUEST_PY = textwrap.dedent('''\
    """Test script for the {model_name} model."""
    import requests

    BASE_URL = "http://127.0.0.1:8000"
    URL = BASE_URL + "/v2/models/{model_name}/infer"


    def test_infer():
        payload = {"{input_key}": "hello world"}
        resp = requests.post(URL, json=payload)
        print("Status:", resp.status_code)
        print("Response:", resp.json())


    def test_health():
        resp = requests.get(BASE_URL + "/v2/models/{model_name}/ready")
        print("Ready:", resp.status_code)


    if __name__ == "__main__":
        test_health()
        test_infer()
''')

README_MD = textwrap.dedent("""\
    # {project_name}

    Lite-server project initialized with `{template}` template.

    ## Quick Start

    ```bash
    # Start the server
    make serve
    # or
    lite-server serve --config server.yaml

    # In another terminal, run the test
    make test
    # or
    python test_request.py
    ```

    ## Project Structure

    ```
    {project_name}/
    ├── server.yaml              # Server configuration
    ├── Dockerfile               # Container image
    ├── docker-compose.yml       # Local orchestration
    ├── Makefile                 # Common commands
    ├── test_request.py          # Quick test script
    ├── requirements.txt         # Python dependencies
    ├── .gitignore               # Git ignore rules
    ├── model_repo/
    │   └── {model_name}/
    │       └── 1/
    │           ├── model.py     # LitAPI implementation
    │           └── config.yaml  # Model config
    │   └── orchestration.yaml   # Model orchestration
    └── .github/
        └── workflows/
            └── ci.yml           # GitHub Actions CI
    ```

    ## Commands

    | Command | Description |
    |---------|-------------|
    | `make serve` | Start the server |
    | `make test` | Send a test request |
    | `make benchmark` | Run benchmark |
    | `make clean` | Clean caches |
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
    .PHONY: serve test benchmark clean

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
          - name: Validate config
            run: lite-server config-check server.yaml
          - name: Lint model code
            run: |
              python -m py_compile model_repo/{model_name}/1/model.py
""")

ORCHESTRATION_YAML = textwrap.dedent("""\
    # Orchestration config: controls which models are loaded and how.
    #
    # control_mode: explicit | poll | all
    #   explicit: only load models listed in load_models
    #   poll:     auto-detect repo changes and load/unload models
    #   all:      load all available models on startup
    control_mode: explicit
    poll_interval: 5

    # List of model names to load on startup
    load_models:
      - {model_name}

    # Per-model loading strategy
    models:
      - name: {model_name}
        load_policy: explicit
        versions_to_load:
          - "1"
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
            raise ValueError(
                f"Unknown template '{template}'. Available: {', '.join(TEMPLATES)}"
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

        # Model files
        (model_dir / "model.py").write_text(tmpl["model_py"])
        (model_dir / "config.yaml").write_text(tmpl["config_yaml"])

        # Server config
        grpc = str(self.options.get("grpc", True)).lower()
        metrics = str(self.options.get("metrics", True)).lower()
        (root / "server.yaml").write_text(
            SERVER_YAML.format(
                grpc=grpc, metrics=metrics, model_name=self.model_name
            )
        )

        # Test script
        input_key = "input"
        if self.template in ("llm",):
            input_key = "prompt"
        elif self.template in ("nlp",):
            input_key = "text"
        elif self.template in ("cv-classify", "cv-detect"):
            input_key = "image"
        (root / "test_request.py").write_text(
            TEST_REQUEST_PY.replace("{model_name}", self.model_name).replace("{input_key}", input_key)
        )

        # README
        (root / "README.md").write_text(
            README_MD.format(
                project_name=self.project_name,
                template=self.template,
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

        # Orchestration config
        (root / "model_repo" / "orchestration.yaml").write_text(
            ORCHESTRATION_YAML.format(model_name=self.model_name)
        )

        return root
