"""Start lite-server for benchmarking.

Requires Rust toolchain (cargo) and the lite-server source tree.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib import request

import yaml


def _ensure_cargo_in_path() -> None:
    """Detect cargo and prepend its directory to PATH for subprocess visibility.

    Scans PATH and common rustup/cargo install directories. If found,
    prepends the directory to os.environ['PATH'] so subprocess.Popen
    inherits it automatically.
    """
    path_env = os.environ.get("PATH", "")
    path_dirs = [p for p in path_env.split(os.pathsep) if p]

    # Already in PATH?
    for p in path_dirs:
        if (Path(p) / "cargo").exists():
            return

    # Search common install locations
    home = Path.home()
    candidates = [
        home / ".cargo" / "bin",
        home / ".rustup" / "toolchains" / "stable-x86_64-apple-darwin" / "bin",
        home / ".rustup" / "toolchains" / "stable-aarch64-apple-darwin" / "bin",
        home / ".rustup" / "toolchains" / "stable-x86_64-unknown-linux-gnu" / "bin",
        home / ".rustup" / "toolchains" / "stable-aarch64-unknown-linux-gnu" / "bin",
    ]
    for candidate in candidates:
        if (candidate / "cargo").exists():
            os.environ["PATH"] = str(candidate) + os.pathsep + path_env
            return


def _find_cargo_root() -> Path | None:
    """Find the cargo project root (directory containing Cargo.toml)."""
    script_dir = Path(__file__).resolve().parent
    # Walk up until we find Cargo.toml
    current = script_dir
    while current != current.parent:
        if (current / "Cargo.toml").exists():
            return current
        current = current.parent
    return None


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


def _write_orchestration(model_repo: Path, model_name: str) -> Path:
    """Write orchestration.yaml to model_repo root for auto-load."""
    orch_path = model_repo / "orchestration.yaml"
    orch = {
        "control_mode": "explicit",
        "load_models": [model_name],
        "models": [
            {
                "name": model_name,
                "load_policy": "explicit",
            }
        ],
    }
    with open(orch_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(orch, f, sort_keys=False)
    return orch_path


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


def main() -> int:
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

    # Update workers_per_device in model config
    _set_workers_in_config(model_repo, args.model, args.workers)

    # Write orchestration.yaml for auto-load
    orch_path = _write_orchestration(model_repo, args.model)

    # Ensure cargo is discoverable by subprocess
    _ensure_cargo_in_path()

    # Find cargo root
    cargo_root = _find_cargo_root()
    if cargo_root is None:
        print("ERROR: Cannot find Cargo.toml project root", file=sys.stderr)
        return 1

    cmd = [
        "cargo",
        "run",
        "--release",
        "--",
        "serve",
        "--port", str(args.port),
        "--host", "127.0.0.1",
        "--model-repo", str(model_repo),
        "--no-metrics",
        "--log-level", "warning",
        "--timeout", str(args.duration + 10.0),
    ]

    print(f"[lite-server] Starting on port {args.port} with {args.workers} workers...")
    print(f"  model-repo: {model_repo}")
    print(f"  model: {args.model}")
    print(f"  cargo root: {cargo_root}")

    proc = subprocess.Popen(
        cmd,
        cwd=str(cargo_root),
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
            proc.terminate()
            proc.wait()
            return 1

        # Wait for model ready
        ready_url = f"http://127.0.0.1:{args.port}/v2/models/{args.model}/ready"
        if not wait_for_model_ready(ready_url, timeout=60):
            print("ERROR: lite-server model failed to become ready", file=sys.stderr)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            if stderr:
                print(f"stderr:\n{stderr}", file=sys.stderr)
            proc.terminate()
            proc.wait()
            return 1

        print(f"[lite-server] Ready. Model '{args.model}' loaded.")

        # Keep running until interrupted
        proc.wait()
    except KeyboardInterrupt:
        print("\n[lite-server] Shutting down...")
        proc.terminate()
        proc.wait()
    finally:
        # Clean up orchestration.yaml
        try:
            orch_path.unlink(missing_ok=True)
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
