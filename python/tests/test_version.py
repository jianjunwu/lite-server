"""Test that lite_server.__version__ matches pyproject.toml."""

import sys
from pathlib import Path

import pytest


def _read_pyproject_version():
    """Read version from pyproject.toml in project root."""
    # python/tests/ -> ../../pyproject.toml
    project_root = Path(__file__).resolve().parent.parent.parent
    pyproject = project_root / "pyproject.toml"
    if not pyproject.exists():
        pytest.skip(f"pyproject.toml not found at {pyproject}")
    for line in pyproject.read_text().splitlines():
        if line.strip().startswith("version"):
            # version = "x.y.z"
            return line.split("=")[-1].strip().strip('"').strip("'")
    pytest.skip("version field not found in pyproject.toml")


def test_version_matches_pyproject():
    """__version__ must match pyproject.toml, not be hardcoded.

    Compared PEP 440-normalized: a pre-release has multiple equivalent spellings
    (e.g. ``0.8.0-rc0`` in pyproject normalizes to ``0.8.0rc0`` in the installed
    dist metadata), so literal equality would flake on any pre-release version.
    """
    import lite_server

    expected = _read_pyproject_version()
    try:
        from packaging.version import Version

        assert Version(lite_server.__version__) == Version(expected)
    except ImportError:
        # packaging unavailable: fall back to literal equality.
        assert lite_server.__version__ == expected
