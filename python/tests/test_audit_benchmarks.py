"""Audit tests for benchmarks/scripts — defects found during /audit review.

B1 (fallback api_path mismatch) is now fixed — the test passes and serves
as regression coverage.
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
# B1 — LitServe fallback must serve at the api_path compare.py benchmarks
#      against: /v2/models/{args.model}/infer
# ---------------------------------------------------------------------------

class TestAuditB1_FallbackApiPathMismatch:
    """B1 (P1, fixed): the fallback built-in must serve at the requested
    model's api_path, so wait_for_server sees a healthy server."""

    def test_fallback_api_path_matches_requested_model(self, run_litserve):
        """compare.py POSTs to /v2/models/{args.model}/infer.  main()'s
        fallback constructs _BuiltinSleepAPI with model_name=args.model, so
        even a model NOT in _BUILTIN_SLEEP_MAP serves at the requested URL."""
        requested = "my_custom_model"
        sleep_ms = run_litserve._BUILTIN_SLEEP_MAP.get(requested, 1)
        api = run_litserve._BuiltinSleepAPI(sleep_ms=sleep_ms, model_name=requested)

        assert api.api_path == f"/v2/models/{requested}/infer"

    def test_builtin_default_paths_unchanged(self, run_litserve):
        """Without an explicit model_name, api_path still derives from
        sleep_ms (default behaviour preserved)."""
        assert run_litserve._BuiltinSleepAPI(sleep_ms=1).api_path == \
            "/v2/models/sleep_1ms_model/infer"
        assert run_litserve._BuiltinSleepAPI(sleep_ms=10).api_path == \
            "/v2/models/sleep_model/infer"
