"""Start lite-server for benchmarking.

Requires the lite-server Python package to be installed (e.g. via maturin build + pip install).
"""

from __future__ import annotations

import argparse
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib import request

import yaml


def _handle_sigterm(signum, frame):
    """Convert SIGTERM to KeyboardInterrupt so finally block runs."""
    raise KeyboardInterrupt


def _set_workers_in_config(model_repo: Path, model_name: str, workers: int) -> None:
    """Inject workers_per_device into the model's config.yaml."""
    for version_dir in (model_repo / model_name).iterdir():
        if version_dir.is_dir():
            cfg_path = version_dir / "config.yaml"
            cfg: dict = {}
            if cfg_path.exists():
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f) or {}
            cfg["workers_per_device"] = workers
            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(cfg, f, sort_keys=False)


def _write_server_config(model_repo: Path, model_name: str) -> Path:
    """Write a server config YAML with explicit orchestration for auto-load."""
    cfg_path = model_repo / "server.yaml"
    cfg = {
        "orchestration": {
            "control_mode": "explicit",
            "load_models": [model_name],
            "models": [
                {
                    "name": model_name,
                    "load_policy": "all",
                }
            ],
        },
    }
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
    return cfg_path


def wait_for_health(url: str, timeout: float = 30.0) -> bool:
    """Poll /health until HTTP 200."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = request.Request(url, method="GET")
            with request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def wait_for_model_ready(url: str, timeout: float = 30.0) -> bool:
    """Poll /v2/models/{name}/ready until ready=true."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = request.Request(url, method="GET")
            with request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    import json
                    body = json.loads(resp.read().decode())
                    if body.get("ready"):
                        return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _safe_terminate(proc: subprocess.Popen) -> None:
    """Gracefully terminate a subprocess, falling back to kill."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            proc.kill()
            proc.wait(timeout=2)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            pass


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_sigterm)

    parser = argparse.ArgumentParser(description="Run lite-server benchmark target")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--workers", type=int, default=1, help="Inference workers per device")
    parser.add_argument(
        "--model-repo",
        default=None,
        help="Path to model repository directory (default: <script_dir>/models)",
    )
    parser.add_argument(
        "--model",
        default="sleep_1ms_model",
        help="Model name to benchmark",
    )
    parser.add_argument(
        "--duration", type=float, default=30.0, help="Expected benchmark duration (for timeout)"
    )
    parser.add_argument(
        "--core", action="store_true", help="Use lite-server-core binary instead of Python CLI"
    )
    args = parser.parse_args()

    # Resolve model repository
    if args.model_repo:
        model_repo = Path(args.model_repo).resolve()
    else:
        model_repo = Path(__file__).resolve().parent.parent / "models"

    if not model_repo.exists():
        print(f"ERROR: Model repository not found: {model_repo}", file=sys.stderr)
        return 1

    model_dir = model_repo / args.model
    if not model_dir.exists():
        print(f"ERROR: Model not found: {model_dir}", file=sys.stderr)
        return 1

    # Create a temporary copy of model repo to avoid modifying tracked files
    temp_repo = Path(tempfile.mkdtemp(prefix="lite_server_benchmark_"))
    shutil.copytree(model_repo, temp_repo, dirs_exist_ok=True)

    # Update workers_per_device in the temporary config
    _set_workers_in_config(temp_repo, args.model, args.workers)

    # Write server config for auto-load in the temporary repo
    config_path = _write_server_config(temp_repo, args.model)

    if args.core:
        # Look for lite-server-core binary
        cargo_root = Path(__file__).resolve().parent.parent.parent
        core_binary = cargo_root / "target" / "release" / "lite-server-core"
        if not core_binary.exists():
            core_binary = shutil.which("lite-server-core")
            if core_binary is None:
                print(
                    "ERROR: 'lite-server-core' binary not found. "
                    "Run 'cargo build --release' first.",
                    file=sys.stderr,
                )
                return 1
            core_binary = Path(core_binary)
        lite_server = str(core_binary)
        subcmd = "serve"
    else:
        lite_server = shutil.which("lite-server")
        if lite_server is None:
            print(
                "ERROR: 'lite-server' command not found. "
                "Install the package first: maturin build --release && pip install dist/lite_server-*.whl",
                file=sys.stderr,
            )
            return 1
        subcmd = "serve"

    cmd = [
        lite_server,
        subcmd,
        "--config", str(config_path),
        "--port", str(args.port),
        "--host", "127.0.0.1",
        "--model-repo", str(temp_repo),
        "--no-metrics",
        "--log-level", "warning",
        "--timeout", str(args.duration + 10.0),
    ]

    label = "lite-server-core" if args.core else "lite-server"
    print(f"[{label}] Starting on port {args.port} with {args.workers} workers...")
    print(f"  model-repo: {model_repo}")
    print(f"  model: {args.model}")
    print(f"  binary: {lite_server}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for health endpoint
        health_url = f"http://127.0.0.1:{args.port}/health"
        if not wait_for_health(health_url, timeout=60):
            print("ERROR: lite-server failed to start (health check timed out)", file=sys.stderr)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            if stderr:
                print(f"stderr:\n{stderr}", file=sys.stderr)
            _safe_terminate(proc)
            return 1

        # Wait for model ready
        ready_url = f"http://127.0.0.1:{args.port}/v2/models/{args.model}/ready"
        if not wait_for_model_ready(ready_url, timeout=60):
            print("ERROR: lite-server model failed to become ready", file=sys.stderr)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            if stderr:
                print(f"stderr:\n{stderr}", file=sys.stderr)
            _safe_terminate(proc)
            return 1

        print(f"[lite-server] Ready. Model '{args.model}' loaded.")

        # Keep running until interrupted
        proc.wait()
    except KeyboardInterrupt:
        print("\n[lite-server] Shutting down...")
        _safe_terminate(proc)
    finally:
        _safe_terminate(proc)
        # Clean up temporary model repo
        try:
            shutil.rmtree(temp_repo, ignore_errors=True)
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
