"""Audit tests for analyze/benchmark P0–P2 implementation (commits 7100943..02b8ece).

Each test demonstrates a confirmed defect by FAILING against the current code.
These tests do NOT modify any implementation code. Findings are numbered A1..A10
and cross-referenced with the audit report.

Plan reference: .claude/analyze-benchmark-optimization.research.md
"""

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from lite_server import cli
from lite_server.benchmark.benchmark import BenchmarkEngine, BenchmarkResult
from lite_server.analyzer.static import StaticAnalyzer

REPO_ROOT = Path(__file__).resolve().parents[2]


def _bench_args(**overrides):
    base = {
        "url": "http://127.0.0.1:8000",
        "model": "m",
        "version": None,
        "concurrency": "1",
        "duration": None,
        "requests": 5,
        "warmup_requests": 0,
        "grace_period": 1.0,
        "payload": None,
        "payload_file": None,
        "payload_random": None,
        "export": None,
        "max_error_rate": None,
        "max_p99": None,
        "rate": None,
        "latency_threshold": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _fake_httpx(status_code=200):
    """Minimal httpx stub (same shape as test_cli.TestBenchmark._fake_httpx)."""
    mock_response = type("Response", (), {"status_code": status_code})()

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, **kwargs):
            return mock_response

    return type("FakeHttpx", (), {
        "AsyncClient": FakeAsyncClient,
        "Limits": type("Limits", (), {"__init__": lambda self, *a, **k: None}),
        "Timeout": type("Timeout", (), {"__init__": lambda self, *a, **k: None}),
        "TimeoutException": type("TimeoutException", (Exception,), {}),
        "ConnectError": type("ConnectError", (Exception,), {}),
        "TransportError": type("TransportError", (Exception,), {}),
    })()


# ---------------------------------------------------------------------------
# A1 — version-directory symlink escapes the path whitelist (P0, security)
# ---------------------------------------------------------------------------

class TestAuditA1_VersionDirSymlinkEscape:
    """A1 (P0): docs promise '..'/symlink escapes are rejected with exit 2, but
    only the MODEL dir is resolve()+checked.  A symlinked VERSION dir
    (model/1 -> /outside) is followed silently, and the outside config.yaml
    content ends up embedded in the JSON report (report.config)."""

    def test_scope_version_dir_symlink_escape_not_rejected(self, tmp_path):
        repo = tmp_path / "repo"
        (repo / "model").mkdir(parents=True)
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "config.yaml").write_text('secret_key: "OUTSIDE-SECRET"')
        (outside / "model.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class M(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        (repo / "model" / "1").symlink_to(outside)

        analyzer = StaticAnalyzer(repo)
        with pytest.raises(ValueError, match="escape"):
            analyzer.analyze_model("model")


# ---------------------------------------------------------------------------
# A2 — missing model.py (non-ensemble) yields no error finding (P1)
# ---------------------------------------------------------------------------

class TestAuditA2_MissingModelPyPasses:
    """A2 (P1): the server scanner SKIPS version dirs without model.py (unless
    the config has an `ensemble` key) — such a model can never be served.
    analyze however records only files.has_model_py=False and emits no
    error-severity finding, so the CI gate passes an unloadable model."""

    def test_control_missing_model_py_no_error_finding(self, tmp_path):
        vdir = tmp_path / "m" / "1"
        vdir.mkdir(parents=True)
        (vdir / "config.yaml").write_text("max_batch_size: 1\n")

        report = StaticAnalyzer(tmp_path).analyze_model("m", version="1")

        assert report.files["has_model_py"] is False
        assert report.exit_code() == 1, (
            "no error finding for a version dir the server will not load: "
            f"findings={[(f.rule_id, f.severity) for f in report.findings]}"
        )


# ---------------------------------------------------------------------------
# A3 — open-loop (--rate) + fixed count (--requests) sends zero requests (P1)
# ---------------------------------------------------------------------------

class TestAuditA3_OpenLoopFixedCount:
    """A3 (P1): engine.run(rate=R, total_requests=N, duration=None) routes to
    _run_open_loop(duration or 0.0) — the deadline is 'now', the dispatch loop
    never runs, and zero requests are sent.  The CLI accepts this flag combo
    (--rate is outside the --duration/--requests mutex group) and then reports
    the misleading 'No requests completed — is the server running?'."""

    @pytest.mark.asyncio
    async def test_data_open_loop_with_total_requests_sends_nothing(self):
        async def target(payload):
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=target, payload={}, concurrency=2,
            total_requests=10, rate=100.0,
        )
        assert result.total_requests == 10, (
            f"open-loop + fixed count sent {result.total_requests}/10 requests"
        )


# ---------------------------------------------------------------------------
# A4 — sweep mode bypasses the threshold gate (and --export) (P1)
# ---------------------------------------------------------------------------

class TestAuditA4_SweepSkipsGate:
    """A4 (P1): _cmd_benchmark returns early for sweep mode, so the
    --max-error-rate/--max-p99 gate (exit 99) and the --export writer are never
    reached.  A CI gate combined with a concurrency sweep is silently
    meaningless — the exact anti-pattern the plan cites k6 for."""

    def test_control_sweep_skips_threshold_gate(self, capsys):
        async def run_one(c):
            return BenchmarkResult(
                total_requests=5, successful=5,
                latencies=[100.0] * 5, window=1.0,
            )

        args = _bench_args(max_p99=1.0)  # p99=100ms >> 1ms → must exit 99
        rc = cli._run_concurrency_sweep(args, [1, 2], 1.0, run_one)
        assert rc == 99, f"sweep returned {rc} despite p99 threshold violation"


# ---------------------------------------------------------------------------
# A5 — sweep where every level completes zero requests exits 0 (P1)
# ---------------------------------------------------------------------------

class TestAuditA5_SweepAllFailExit0:
    """A5 (P1): single-run mode treats 'no requests completed' as exit 1, but
    sweep mode breaks out of the level loop and falls through to `return 0`
    when every level completed zero requests (e.g. server down)."""

    def test_control_sweep_all_levels_fail_returns_success(self, capsys):
        async def run_one(c):
            return BenchmarkResult()  # zero completed requests

        rc = cli._run_concurrency_sweep(_bench_args(), [1, 2], 1.0, run_one)
        assert rc != 0, f"sweep with zero completed requests returned {rc}"


# ---------------------------------------------------------------------------
# A6 — CLI lets engine ValueError escape as a traceback instead of exit 2 (P2)
# ---------------------------------------------------------------------------

class TestAuditA6_CliArgValidationCrash:
    """A6 (P2): the engine validates concurrency/rate and raises ValueError
    (regression-covered by test_audit_analyzer B1/B2), but _cmd_benchmark never
    translates it — the user gets an unhandled traceback instead of the
    documented exit code 2 ('argument/payload error')."""

    def test_data_cli_concurrency_zero_crashes(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "httpx", _fake_httpx())
        rc = cli._cmd_benchmark(_bench_args(concurrency="0"))
        assert rc == 2, f"concurrency=0 returned {rc} (expected clean exit 2)"

    def test_data_cli_rate_zero_crashes(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "httpx", _fake_httpx())
        rc = cli._cmd_benchmark(_bench_args(rate=0.0))
        assert rc == 2, f"rate=0 returned {rc} (expected clean exit 2)"


# ---------------------------------------------------------------------------
# A7 — open-loop with unsustainable target rate stays silent (P1)
# ---------------------------------------------------------------------------

class TestAuditA7_OpenLoopSilentOverload:
    """A7 (P1): when the client cannot sustain --rate (semaphore saturated /
    dispatch behind schedule), requests queue at the generator and the
    measured latencies silently re-introduce coordinated omission.  The
    plan's honesty principle ('测量不充分必须显式 warning') and its
    '计划 vs 实际发出数' requirement produce no warning and no achieved-rate
    field in the report."""

    @pytest.mark.asyncio
    async def test_pure_open_loop_unsustainable_rate_no_warning(self):
        async def slow(payload):
            await asyncio.sleep(0.005)
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=slow, payload={}, concurrency=1,
            duration=0.2, rate=1000.0, grace_period=0.2,
        )
        assert any("rate" in w.lower() for w in result.warnings), (
            f"target 1000 req/s vs ~200 achievable produced no rate warning: "
            f"{result.warnings}"
        )


# ---------------------------------------------------------------------------
# A8 — --deep is doubly broken: generated script is a SyntaxError, and the
#      runtime introspection (if it ever ran) suppresses LS001 (P1)
# ---------------------------------------------------------------------------

class TestAuditA8_DeepBroken:
    """A8 (P1): two stacked defects make --deep (commit f308e7a) unusable.

    A8a: _DEEP_IMPORT_SCRIPT embeds json.dumps(sys.path) — which contains
    double quotes — inside a double-quoted Python string literal.  The
    generated script is ALWAYS a SyntaxError, so every --deep run ends in
    LS203 'deep import failed' (and the report still claims
    executed_user_code=true although model code never ran).

    A8b: behind that, the introspection collects methods via
    getattr(_obj, name) and only filters __isabstractmethod__ — but
    LitAPI.predict is NOT abstract (it raises NotImplementedError when
    called).  The faithful subprocess output for a class overriding only
    `setup` lists predict as implemented, so _merge_deep_results suppresses
    the LS001 error and the gate passes a model that cannot serve.
    """

    def test_data_deep_generated_script_is_syntax_error(self):
        """The script _deep_import_analysis spawns must compile (A8a: fixed by
        passing sys.path JSON via argv instead of string interpolation)."""
        from lite_server.analyzer.static import _DEEP_IMPORT_SCRIPT

        code = _DEEP_IMPORT_SCRIPT.replace(
            "__VERSION_DIR__", "/x/1"
        ).replace(
            "__MODEL_DIR__", "/x"
        ).replace(
            "__MODEL_PY__", "/x/1/model.py"
        )
        compile(code, "<deep>", "exec")  # must succeed after fix

    def test_data_deep_inherited_predict_false_implemented(self, tmp_path):
        """A8b fixed: deep import only reports methods from the class's own
        __dict__ (plus repo-internal intermediate bases), not inherited
        LitAPI defaults.  A class overriding only setup must still raise
        LS001 because predict is not implemented."""
        vdir = tmp_path / "m" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(
            "import lite_server\n"
            "_Base = lite_server.LitAPI\n\n"
            "class MyAPI(_Base):\n"
            "    def setup(self, device): pass\n"
        )
        analyzer = StaticAnalyzer(tmp_path)
        report = analyzer.analyze_model("m", version="1")  # AST-only: LS002

        # Correct post-fix deep import output: only setup is in MyAPI.__dict__
        runtime_result = {"classes": [{
            "name": "MyAPI", "bases": ["LitAPI"],
            "location": {"file": "model.py", "line": 4},
            "methods": ["setup"],
            "stream_predict_is_generator": False,
        }]}
        analyzer._merge_deep_results(report, runtime_result, {})

        assert any(f.rule_id == "LS001" for f in report.findings), (
            "predict not overridden but deep merge reports it implemented: "
            f"{report.methods.get('core_required')}"
        )


# ---------------------------------------------------------------------------
# A9 — non-UTF-8 config.yaml / requirements.txt crash the analysis (P2)
# ---------------------------------------------------------------------------

class TestAuditA9_NonUtf8FilesCrash:
    """A9 (P2): read_text(encoding='utf-8') on config.yaml / requirements.txt
    is only guarded against yaml.YAMLError / InvalidRequirement — a non-UTF-8
    file raises UnicodeDecodeError out of analyze_model, so the CLI dies with
    a traceback instead of an LS004/LS104 finding or a clean exit 2
    ('文件不可解析' per the exit-code protocol)."""

    def test_data_config_non_utf8_crashes(self, tmp_path):
        vdir = tmp_path / "m" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class M(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        (vdir / "config.yaml").write_bytes(b"\xff\xfe\x00bad")

        report = StaticAnalyzer(tmp_path).analyze_model("m", version="1")
        assert any(f.rule_id == "LS004" for f in report.findings)

    def test_data_requirements_non_utf8_crashes(self, tmp_path):
        vdir = tmp_path / "m" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class M(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        (tmp_path / "m" / "requirements.txt").write_bytes(b"\xff\xfe\x00bad")

        report = StaticAnalyzer(tmp_path).analyze_model("m", version="1")
        assert any(f.rule_id == "LS104" for f in report.findings)


# ---------------------------------------------------------------------------
# A10 — benchmark export misses plan-mandated contract fields (P2)
# ---------------------------------------------------------------------------

class TestAuditA10_ExportContractGaps:
    """A10 (P2): plan §4.2.2 defines the JSON export as the authoritative
    record containing a timestamp, stddev in the latency stats, and the
    payload source in the config metadata.  None of the three are written."""

    def _export(self, monkeypatch, tmp_path):
        monkeypatch.setitem(sys.modules, "httpx", _fake_httpx())
        path = tmp_path / "r.json"
        rc = cli._cmd_benchmark(_bench_args(requests=3, export=str(path)))
        assert rc == 0
        return json.loads(path.read_text())

    def test_data_export_has_no_timestamp(self, monkeypatch, tmp_path):
        data = self._export(monkeypatch, tmp_path)
        assert "timestamp" in data or "generated_at" in data

    def test_data_export_latency_has_no_stddev(self, monkeypatch, tmp_path):
        data = self._export(monkeypatch, tmp_path)
        assert "stddev" in data["latency_ms"]

    def test_data_export_config_has_no_payload_source(self, monkeypatch, tmp_path):
        data = self._export(monkeypatch, tmp_path)
        assert "payload" in data["config"]


# ---------------------------------------------------------------------------
# D1 — docs lag the CLI: P2 flags and rule IDs undocumented (P1, docs sync)
# ---------------------------------------------------------------------------

class TestAuditD1_DocsCliSync:
    """D1 (P1): docs/cli.md and docs/zh/cli.md (commit d8181b7) predate the
    P2 feature commits (f308e7a, 02b8ece) and were never updated — the
    following shipped flags/rules are absent from both language versions."""

    @pytest.mark.parametrize("flag", [
        "--rate", "--latency-threshold", "--payload-random",
    ])
    def test_docs_benchmark_flags_documented(self, flag):
        for doc in ("docs/cli.md", "docs/zh/cli.md"):
            text = (REPO_ROOT / doc).read_text(encoding="utf-8")
            assert flag in text, f"{flag} missing from {doc}"

    @pytest.mark.parametrize("flag", [
        "--deep", "--deep-timeout", "--profile",
    ])
    def test_docs_analyze_flags_documented(self, flag):
        for doc in ("docs/cli.md", "docs/zh/cli.md"):
            text = (REPO_ROOT / doc).read_text(encoding="utf-8")
            assert flag in text, f"{flag} missing from {doc}"

    @pytest.mark.parametrize("rule", [
        "LS203", "LS204", "LS301", "LS305", "LS401", "LS404",
    ])
    def test_docs_rule_table_complete(self, rule):
        for doc in ("docs/cli.md", "docs/zh/cli.md"):
            text = (REPO_ROOT / doc).read_text(encoding="utf-8")
            assert rule in text, f"{rule} missing from {doc} rule table"
