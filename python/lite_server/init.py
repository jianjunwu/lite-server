"""Project scaffolding / init command for lite-server.

Reference: light_server init system (simplified).
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

TEMPLATES = {
    "basic": {
        "model_py": textwrap.dedent('''\
            from litserve import LitAPI


            class MyAPI(LitAPI):
                def setup(self, device):
                    pass

                def decode_request(self, request):
                    return request.get("input", 0)

                def predict(self, x):
                    return {"output": x * 2}

                def encode_response(self, output):
                    return output


            api = MyAPI
        '''),
        "config_yaml": textwrap.dedent('''\
            name: {model_name}
            max_batch_size: 1
            batch_timeout: 0.0
            stream: false
            bidirectional: false
            continuous_batching: false
            accelerator: cpu
            devices: 1
            workers_per_device: 1
            max_queue_size: 1000
            queue_mode: per_worker
        '''),
    },
    "streaming": {
        "model_py": textwrap.dedent('''\
            from litserve import LitAPI


            class MyAPI(LitAPI):
                def setup(self, device):
                    pass

                def decode_request(self, request):
                    return request.get("prompt", "")

                def predict(self, prompt):
                    # Simulated streaming generator
                    for token in ["Hello", " world", "!"]:
                        yield {"token": token}

                def encode_response(self, output):
                    return output


            api = MyAPI
        '''),
        "config_yaml": textwrap.dedent('''\
            name: {model_name}
            max_batch_size: 1
            batch_timeout: 0.0
            stream: true
            bidirectional: false
            continuous_batching: false
            max_sequence_length: 2048
            accelerator: cpu
            devices: 1
            workers_per_device: 1
            max_queue_size: 1000
            queue_mode: per_worker
        '''),
    },
    "batching": {
        "model_py": textwrap.dedent('''\
            from litserve import LitAPI


            class MyAPI(LitAPI):
                def setup(self, device):
                    pass

                def decode_request(self, request):
                    return request.get("input", 0)

                def batch(self, inputs):
                    return inputs

                def predict(self, batch):
                    return [{"output": x * 2} for x in batch]

                def unbatch(self, outputs):
                    return outputs

                def encode_response(self, output):
                    return output


            api = MyAPI
        '''),
        "config_yaml": textwrap.dedent('''\
            name: {model_name}
            max_batch_size: 8
            batch_timeout: 0.01
            stream: false
            bidirectional: false
            continuous_batching: false
            accelerator: cpu
            devices: 1
            workers_per_device: 1
            max_queue_size: 1000
            queue_mode: per_worker
            adaptive_batching: true
            min_batch_timeout: 0.001
            adaptive_queue_threshold: 10
        '''),
    },
}

SERVER_YAML = textwrap.dedent('''\
    server:
      http_port: 8000
      grpc_port: 8001
      metrics_port: 8002
      host: 0.0.0.0
      timeout: 30.0
      log_level: info

    model_repository:
      path: ./model_repo

    orchestration:
      control_mode: explicit
      load_models:
        - {model_name}
''')

TEST_REQUEST_PY = textwrap.dedent('''\
    """Test script for the {model_name} model."""
    import requests

    BASE_URL = "http://127.0.0.1:8000"
    MODEL_NAME = "{model_name}"


    def test_infer():
        url = BASE_URL + "/v2/models/" + MODEL_NAME + "/infer"
        payload = {"input": "hello world"}
        resp = requests.post(url, json=payload)
        print("Status:", resp.status_code)
        print("Response:", resp.json())


    def test_health():
        url = BASE_URL + "/v2/models/" + MODEL_NAME + "/ready"
        resp = requests.get(url)
        print("Ready:", resp.status_code)


    if __name__ == "__main__":
        test_health()
        test_infer()
''')

README_MD = textwrap.dedent('''\
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
''')

DOCKERFILE = textwrap.dedent('''\
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
''')

MAKEFILE = textwrap.dedent('''\
    .PHONY: serve test benchmark clean

    serve:
    \tlite-server serve --config server.yaml

    test:
    \tpython test_request.py

    benchmark:
    \tlite-server benchmark --model {model_name} --duration 30

    clean:
    \trm -rf __pycache__ .pytest_cache *.log
''')

DOCKER_COMPOSE = textwrap.dedent('''\
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
''')

REQUIREMENTS_TXT = textwrap.dedent('''\
    lite-server
    requests

    # Add your model-specific dependencies below
''')

GITIGNORE = textwrap.dedent('''\
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
''')

CI_YML = textwrap.dedent('''\
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
''')

ORCHESTRATION_YAML = textwrap.dedent('''\
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
''')


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
        model_name: Optional[str] = None,
    ):
        if template not in TEMPLATES:
            raise ValueError(f"Unknown template '{template}'. Available: {', '.join(TEMPLATES)}")
        self.project_name = project_name
        self.template = template
        self.output_dir = Path(output_dir)
        self.model_name = model_name or "my_model"

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
        (model_dir / "config.yaml").write_text(tmpl["config_yaml"].format(model_name=self.model_name))

        # Server config
        (root / "server.yaml").write_text(SERVER_YAML.format(model_name=self.model_name))

        # Test script
        (root / "test_request.py").write_text(
            TEST_REQUEST_PY.replace("{model_name}", self.model_name)
        )

        # README
        (root / "README.md").write_text(
            README_MD.format(project_name=self.project_name, template=self.template, model_name=self.model_name)
        )

        # Dockerfile
        (root / "Dockerfile").write_text(DOCKERFILE)

        # Makefile
        (root / "Makefile").write_text(
            MAKEFILE.replace("{model_name}", self.model_name)
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
            CI_YML.replace("{model_name}", self.model_name)
        )

        # Orchestration config
        (root / "model_repo" / "orchestration.yaml").write_text(
            ORCHESTRATION_YAML.replace("{model_name}", self.model_name)
        )

        return root
