"""Start LitServe for benchmarking.

Requires litserve to be installed:
    uv pip install litserve
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import litserve as ls


def _find_litapi_class(module):
    """Find the first LitAPI subclass in a module."""
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if (
            isinstance(obj, type)
            and issubclass(obj, ls.LitAPI)
            and obj is not ls.LitAPI
        ):
            return obj
    return None


def _load_model_from_repo(repo_path: Path, target_name: str | None = None):
    """Scan model_repo and load the first LitAPI found.

    Expects structure: repo/model_name/version/model.py
    Returns (model_name, lit_api_instance) or (None, None).

    To make the dynamically-loaded class picklable across spawn,
    we copy the model file into a temporary directory on PYTHONPATH.
    """
    for model_dir in repo_path.iterdir():
        if not model_dir.is_dir():
            continue
        if target_name and model_dir.name != target_name:
            continue
        for version_dir in model_dir.iterdir():
            if not version_dir.is_dir():
                continue
            model_py = version_dir / "model.py"
            if model_py.exists():
                # Copy model.py into a temp package so it is importable by name
                tmp_dir = Path(tempfile.mkdtemp(prefix="litserve_benchmark_"))
                pkg_dir = tmp_dir / model_dir.name
                pkg_dir.mkdir()
                shutil.copy2(model_py, pkg_dir / "__init__.py")

                # Ensure sibling modules are importable
                pythonpath = os.environ.get("PYTHONPATH", "")
                paths = [p for p in pythonpath.split(os.pathsep) if p]
                for p in (str(tmp_dir), str(model_py.parent)):
                    if p not in paths:
                        paths.insert(0, p)
                os.environ["PYTHONPATH"] = os.pathsep.join(paths)

                # Refresh sys.path so the current process can find the module
                for p in (str(tmp_dir), str(model_py.parent)):
                    if p not in sys.path:
                        sys.path.insert(0, p)

                # Import using the package name
                module = importlib.import_module(model_dir.name)
                cls = _find_litapi_class(module)
                if cls:
                    instance = cls()
                    # Align api_path with lite-server convention if still default
                    if getattr(instance, "api_path", "/predict") == "/predict":
                        instance.api_path = f"/v2/models/{model_dir.name}/infer"
                    return model_dir.name, instance
    return None, None


class _BuiltinSleepAPI(ls.LitAPI):
    """Fallback CPU-bound mock model when no model_repo is provided."""

    def __init__(self):
        super().__init__(api_path="/v2/models/sleep_1ms_model/infer")

    def setup(self, device):
        self.device = device

    def decode_request(self, request, **kwargs):
        return request.get("input", "")

    def predict(self, inputs, **kwargs):
        time.sleep(0.001)  # 1ms per request
        if isinstance(inputs, list):
            return [{"output": i, "sleep_ms": 1} for i in inputs]
        return {"output": inputs, "sleep_ms": 1}

    def encode_response(self, output, **kwargs):
        return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LitServe benchmark target")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--workers", type=int, default=1, help="Inference workers")
    parser.add_argument(
        "--model-repo",
        default=None,
        help="Path to model repository directory (default: built-in sleep model)",
    )
    parser.add_argument(
        "--duration", type=float, default=30.0, help="Expected benchmark duration (for timeout)"
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Model name to load (default: auto-detect from model_repo or sleep_1ms_model)",
    )
    args = parser.parse_args()

    model_name = args.model if args.model else "sleep_1ms_model"

    if args.model_repo:
        repo_path = Path(args.model_repo).resolve()
        if not repo_path.exists():
            print(f"ERROR: Model repository not found: {repo_path}", file=sys.stderr)
            return 1
        loaded_name, api = _load_model_from_repo(repo_path, target_name=args.model)
        if api is None:
            print(f"ERROR: No LitAPI subclass found in {repo_path}", file=sys.stderr)
            return 1
        model_name = loaded_name
        print(f"Loaded model '{model_name}' from {repo_path}")
    else:
        api = _BuiltinSleepAPI()
        print("Using built-in 1ms sleep model")

    server = ls.LitServer(
        api,
        accelerator="cpu",
        devices=1,
        workers_per_device=args.workers,
        timeout=args.duration + 10.0,
    )

    print(f"LitServe starting on port {args.port} with {args.workers} workers...")
    try:
        server.run(port=args.port, log_level="warning", generate_client_file=False)
    except KeyboardInterrupt:
        print("\nShutting down LitServe...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
