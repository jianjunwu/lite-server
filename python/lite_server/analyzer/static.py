"""Static model analysis: inspect model.py, config.yaml, requirements.txt."""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from typing import Any

import yaml


class StaticAnalyzer:
    """Analyze a model repository without loading models into memory."""

    def __init__(self, repo_path: Path | str):
        self.repo_path = Path(repo_path)
        if not self.repo_path.exists():
            raise ValueError(f"Repository path does not exist: {self.repo_path}")

    def analyze_model(self, model_name: str) -> dict[str, Any]:
        """Return a dict with static analysis results for a model."""
        if "/" in model_name or "\\" in model_name or model_name in (".", ".."):
            raise ValueError(f"Invalid model name: {model_name!r}")
        model_dir = self.repo_path / model_name
        result: dict[str, Any] = {
            "model_name": model_name,
            "found": False,
            "versions": [],
            "has_model_py": False,
            "has_config": False,
            "has_requirements": False,
            "config": {},
            "methods": [],
            "dependencies": [],
            "warnings": [],
        }

        if not model_dir.exists():
            result["warnings"].append(f"Model directory not found: {model_dir}")
            return result

        result["found"] = True

        # Discover version directories
        version_dirs = [
            d for d in model_dir.iterdir()
            if d.is_dir() and d.name.isdigit()
        ]
        result["versions"] = sorted([d.name for d in version_dirs])

        if not version_dirs:
            result["warnings"].append("No version directories found (expected numeric names like '1')")
            return result

        # Analyze the first version (or default to "1")
        version_dir = version_dirs[0]
        version = version_dir.name
        result["analyzed_version"] = version

        # Parse config.yaml
        config_path = version_dir / "config.yaml"
        if config_path.exists():
            result["has_config"] = True
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                if not isinstance(config, dict):
                    result["warnings"].append("config.yaml is not a mapping, ignoring")
                    config = {}
                result["config"] = config
            except Exception as e:
                result["warnings"].append(f"config.yaml parse error: {e}")

        # Parse model.py
        model_py = version_dir / "model.py"
        if model_py.exists():
            result["has_model_py"] = True
            self._analyze_model_py(model_py, result)
        else:
            result["warnings"].append("model.py not found")

        # Check requirements.txt
        req_file = model_dir / "requirements.txt"
        if req_file.exists():
            result["has_requirements"] = True
            deps = [
                line.strip()
                for line in req_file.read_text().splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            result["dependencies"] = deps

        return result

    @staticmethod
    def _analyze_model_py(model_py: Path, result: dict[str, Any]) -> None:
        """Inspect model.py for LitAPI subclass and required methods."""
        try:
            source = model_py.read_text(encoding="utf-8")
        except Exception as e:
            result["warnings"].append(f"Failed to read model.py: {e}")
            return

        # Try AST analysis first (fast, no import needed)
        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            result["warnings"].append(f"model.py syntax error: {e}")
            return

        api_classes = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                bases = [
                    getattr(base, "id", "")
                    or getattr(getattr(base, "value", None), "id", "")
                    for base in node.bases
                ]
                if "LitAPI" in bases or any("LitAPI" in str(b) for b in bases):
                    api_classes.append(node)

        if not api_classes:
            result["warnings"].append("No LitAPI subclass found in model.py")
            return

        # Use the first LitAPI subclass found
        api_class = api_classes[0]
        result["api_class_name"] = api_class.name

        methods = {node.name for node in api_class.body if isinstance(node, ast.FunctionDef)}
        result["methods"] = sorted(methods)

        required = {"setup", "decode_request", "predict", "encode_response"}
        for method in required:
            if method not in methods:
                result["warnings"].append(f"Missing required method: {method}()")

        # Check for optional but recommended methods
        optional_methods = {
            "batch", "unbatch", "teardown",
            "stream_open", "stream_chunk", "stream_close",
        }
        result["optional_methods"] = sorted(methods & optional_methods)

        # Try full import to validate runtime correctness
        try:
            spec = importlib.util.spec_from_file_location("_analyzed_model", model_py)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
        except Exception as e:
            # Import errors are common (missing deps) — just warn, don't fail
            result["warnings"].append(f"model.py import check: {e}")

    def list_models(self) -> list[str]:
        """Return list of model names in the repository."""
        return sorted([
            d.name for d in self.repo_path.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])
