"""Audit tests for benchmarks/scripts — defects found during /audit review.

Each test demonstrates a confirmed defect by FAILING against the current code.
These tests do NOT modify any implementation code.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "benchmarks" / "scripts" / "run_litserve.py"

litserve = pytest.importorskip("litserve", reason="run_litserve.py requires litserve")


@pytest.fixture(scope="module")
def run_litserve():
    """Load benchmarks/scripts/run_litserve.py as a module."""
    spec = importlib.util.spec_from_file_location("run_litserve", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# B1 — LitServe fallback serves at wrong api_path for models not in
#      _BUILTIN_SLEEP_MAP, so wait_for_server never sees a healthy server
# ---------------------------------------------------------------------------

class TestAuditB1_FallbackApiPathMismatch:
    """B1 (P1): unknown model names fall back to a hardcoded api_path that
    does not match the URL compare.py benchmarks against."""

    def test_scope_fallback_api_path_mismatches_requested_model(self, run_litserve):
        """compare.py POSTs to /v2/models/{args.model}/infer.  When the repo
        contains a lite_server-only model whose name is NOT in
        _BUILTIN_SLEEP_MAP (e.g. any custom model), the fallback built-in
        still serves at /v2/models/sleep_1ms_model/infer — wait_for_server
        polls the requested URL, gets 404 for 60s, and LitServe is reported
        as 'failed to start' for every (workers, concurrency) row."""
        requested = "my_custom_model"
        sleep_ms = run_litserve._BUILTIN_SLEEP_MAP.get(requested, 1)
        api = run_litserve._BuiltinSleepAPI(sleep_ms=sleep_ms)

        expected_path = f"/v2/models/{requested}/infer"
        assert api.api_path == expected_path, (
            f"fallback for unknown model {requested!r} serves at {api.api_path!r} "
            f"but compare.py benchmarks {expected_path!r} — all requests 404"
        )
