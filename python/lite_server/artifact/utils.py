"""Utility functions for artifact handling."""

from __future__ import annotations

import hashlib
from pathlib import Path

# Files / directories to ignore when packing
IGNORE_NAMES = {
    "__pycache__",
    ".git",
    ".DS_Store",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "env",
    "ENV",
    ".idea",
    ".vscode",
}


def _should_ignore(path: Path, root: Path) -> bool:
    """Return True if path should be excluded from the artifact."""
    rel = path.relative_to(root)
    for part in rel.parts:
        if part in IGNORE_NAMES or part.endswith(".pyc") or part.endswith(".pyo"):
            return True
    return False


def _sha256_file(path: Path) -> str:
    """Compute SHA256 of a file in streaming mode (memory-efficient)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()
