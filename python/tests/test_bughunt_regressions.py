"""Regression tests for bugs found in the 2026-10-15 bug hunt.

Every test in this file is written against the *documented / intended*
behaviour and currently FAILS, demonstrating a real defect. Each test
documents the defect it pins in its docstring, so the file doubles as the
evidence bundle for the hunt.

Grouped by subsystem. Nothing here is speculative: each case was traced
through the source before the test was written.
"""

from __future__ import annotations

import os
import stat
import zipfile
from pathlib import Path

import pytest

from lite_server.analyzer.static import StaticAnalyzer
from lite_server.artifact import (
    ArtifactCache,
    ModelPacker,
    ModelUnpacker,
    _load_or_create_key,
)
from lite_server.ensemble import canonical_ensemble_block
from lite_server.profile.checkpoint import TrialRecord, read_trials, write_trials
from lite_server.profile.grid import GridError, GridSpec, default_knobs

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

MODEL_PY = (
    "from lite_server import LitAPI\n"
    "\n"
    "class MyModel(LitAPI):\n"
    "    def setup(self, device): pass\n"
    "    def decode_request(self, request): return request\n"
    "    def predict(self, x): return x\n"
    "    def encode_response(self, output): return output\n"
)


def _make_model_dir(root: Path, marker: str) -> Path:
    """A minimal packable model directory carrying `marker` in model.py."""
    model_dir = root / "my_model"
    vdir = model_dir / "1"
    vdir.mkdir(parents=True, exist_ok=True)
    (vdir / "model.py").write_text(f"# {marker}\n" + MODEL_PY)
    return model_dir


def _rule_ids(report) -> list[str]:
    return [f.rule_id for f in report.findings]


# ---------------------------------------------------------------------------
# artifact layer
# ---------------------------------------------------------------------------


class TestArtifactUnpack:
    def test_unpack_writes_no_entries_absent_from_the_manifest(self, tmp_path):
        """`unpack()` calls `zf.extractall()` with no filter, so zip members
        that are NOT listed in `manifest.json` are still written to disk —
        outside the checksum *and* HMAC signature that `validate()` verified.

        The server itself shells out to `lite_server unpack` for repository
        auto-unpack and for untrusted artifact upload, and the Python worker
        inserts the model dir on `sys.path`, so an injected `1/json.py`
        shadows a stdlib import and executes in the worker.
        """
        model_dir = _make_model_dir(tmp_path / "src", "V1")
        artifact = ModelPacker(model_dir, version="1").pack(tmp_path / "out")

        # Append an entry the manifest does not know about.
        with zipfile.ZipFile(artifact, "a") as zf:
            zf.writestr("1/backdoor.py", "print('injected')\n")

        unpacker = ModelUnpacker(artifact)
        unpacker.validate()  # manifest + checksums are still valid

        out = tmp_path / "out_dir"
        unpacker.unpack(out, prepend_name=False)

        injected = out / "1" / "backdoor.py"
        assert not injected.exists(), (
            "an entry absent from manifest.json was extracted anyway — it "
            "bypasses both the checksum and the signature"
        )


class TestArtifactCache:
    def test_cache_does_not_serve_a_different_artifact_for_a_stale_key(self, tmp_path):
        """The cache index is keyed by `artifact_path:mtime` but the extracted
        directory is keyed by `model_name` alone.

        Extracting artifact B swaps B's content into the same
        `cache/<model_name>` directory that A's index entry still points at,
        so a later lookup of A returns a path that exists — holding B's
        content. Two different artifacts for one model name silently alias.
        """
        root_a = tmp_path / "a"
        root_b = tmp_path / "b"
        artifact_a = ModelPacker(
            _make_model_dir(root_a, "AAA"), version="1"
        ).pack(tmp_path / "out_a")
        artifact_b = ModelPacker(
            _make_model_dir(root_b, "BBB"), version="1"
        ).pack(tmp_path / "out_b")
        assert artifact_a.resolve() != artifact_b.resolve()

        # Force distinct mtimes so the two artifacts get distinct cache keys.
        os.utime(artifact_a, (1_600_000_000, 1_600_000_000))
        os.utime(artifact_b, (1_600_000_100, 1_600_000_100))

        cache = ArtifactCache(tmp_path / "cache")
        path_a = cache.get_or_unpack(artifact_a, "my_model")
        assert "AAA" in (path_a / "1" / "model.py").read_text()

        cache.get_or_unpack(artifact_b, "my_model")

        # The original artifact must still resolve to the original content.
        path_a_again = cache.get_or_unpack(artifact_a, "my_model")
        assert "AAA" in (path_a_again / "1" / "model.py").read_text(), (
            "a cached lookup for artifact A returned artifact B's content — "
            "the index key covers the artifact but the target dir does not"
        )


class TestSigningKey:
    def test_created_signing_key_is_not_group_or_world_readable(self, tmp_path):
        """`_load_or_create_key` writes the HMAC signing key with
        `Path.write_text`, i.e. with whatever the process umask allows
        (0644 by default) and no `chmod`. On a shared host every local user
        can read the key and forge "signed" artifacts.
        """
        old_umask = os.umask(0o022)  # deterministic, restores in finally
        try:
            key_path = tmp_path / "sign.key"
            _load_or_create_key(key_path)
            mode = stat.S_IMODE(key_path.stat().st_mode)
            assert mode == 0o600, (
                f"signing key written with mode {oct(mode)}; key material must "
                "not be readable by group/other"
            )
        finally:
            os.umask(old_umask)


# ---------------------------------------------------------------------------
# OpenAI helpers
# ---------------------------------------------------------------------------


class TestOpenAIChatChunks:
    def test_all_chunks_of_one_completion_share_an_id(self):
        """`build_chat_chunk` mints a fresh `chatcmpl-<uuid>` whenever
        `request_id` is omitted, so the 50 chunks of one completion carry 50
        different ids. The OpenAI streaming contract is that every chunk of a
        stream shares `id` (clients key stream assembly and message identity
        on it).

        The module's own docstring example omits `request_id`.
        """
        from lite_server.helpers.openai import build_chat_chunk

        chunks = [build_chat_chunk(t, model="m") for t in ("a", "b", "c")]
        ids = {c["id"] for c in chunks}
        assert len(ids) == 1, (
            f"one completion produced {len(ids)} different chunk ids: {sorted(ids)}"
        )


# ---------------------------------------------------------------------------
# analyzer
# ---------------------------------------------------------------------------


class TestAnalyzerDottedImport:
    def test_dangerous_call_through_dotted_import_is_still_reported(self, tmp_path):
        """`_build_alias_table` maps `import a.b` to `{"a": "a.b"}`, so the
        first path segment resolves to `a.b` and `a.func()` becomes
        `a.b.func`.

        For `import os.path` that turns `os.system(...)` into
        `os.path.system(...)`, which matches no dangerous-call rule — the
        whole LS3xx security scan goes blind for any model using a dotted
        import, and `import os.path` / `import urllib.request` are common.
        """
        repo = tmp_path / "repo"
        vdir = repo / "dm" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(
            "import os.path\n"
            "\n"
            "from lite_server import LitAPI\n"
            "\n"
            "class MyModel(LitAPI):\n"
            "    def setup(self, device): pass\n"
            "    def decode_request(self, request): pass\n"
            "    def predict(self, x):\n"
            "        os.system('ls')\n"
            "        return x\n"
            "    def encode_response(self, output): return output\n"
        )

        report = StaticAnalyzer(repo).analyze_model("dm")
        assert "LS302" in _rule_ids(report), (
            "os.system() was not flagged when the module was imported as "
            "`import os.path` — the alias table maps `os` to `os.path`"
        )

    def test_pip_options_in_requirements_are_not_reported_as_unparseable(self, tmp_path):
        """`_check_requirements` runs every non-comment line through
        `packaging.Requirement` (PEP 508 only), so pip-specific options that
        `pip install -r` accepts — `-r base.txt`, `--extra-index-url`, `-e .`
        — are reported as LS104 "cannot parse".

        These are warnings, so they flip `--strict` / `--fail-severity
        warning` to a non-zero exit on a perfectly valid requirements file
        (the project's own generated Dockerfile runs `pip install -r`).
        """
        repo = tmp_path / "repo"
        vdir = repo / "rm" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(MODEL_PY)
        (repo / "rm" / "requirements.txt").write_text(
            "-r base.txt\n"
            "--extra-index-url https://example.com/simple\n"
            "-e .\n"
            "torch>=2.0\n"
        )

        report = StaticAnalyzer(repo).analyze_model("rm")
        ls104 = [f for f in report.findings if f.rule_id == "LS104"]
        assert not ls104, (
            "valid pip option lines were reported as unparseable: "
            + "; ".join(f.message for f in ls104)
        )


# ---------------------------------------------------------------------------
# ensemble
# ---------------------------------------------------------------------------


class TestEnsembleCanonicalization:
    def test_malformed_step_compares_unequal_instead_of_raising(self):
        """`canonical_ensemble_block` is documented as "pure normalization —
        no validation (a malformed YAML simply compares unequal and surfaces
        as drift in the analyzer)".

        A structurally malformed `ensemble:` block instead raises out of
        `_canonical_step` (ValueError for a non-mapping step, AttributeError
        for a non-mapping `outputs`), and the analyzer call site has no
        handler — so `lite-server analyze` dies with a traceback instead of
        emitting the intended LS112 drift finding.
        """
        block = canonical_ensemble_block({"steps": [{"outputs": "not-a-mapping"}]})
        assert isinstance(block, dict), (
            "canonical_ensemble_block must normalize-or-differ, not raise, "
            "on a malformed ensemble block"
        )

    def test_dags_form_does_not_silently_drop_forbidden_top_level_fields(self):
        """`EnsembleDAG._block_config` returns only `dags` (+ `dag_selector`)
        in the dags form, dropping top-level `output` / `outputs` / `inputs`
        that the author actually set.

        The server rejects exactly that combination (E8-1: "ensemble.dags
        forbids top-level steps/output/outputs/inputs"), and `__post_init__`
        only rejects `steps` alongside `dags` — so here the forbidden fields
        are removed *before* serialization and the server never sees the
        conflict: the emitted config silently becomes a different model than
        the declaration (declared `$s0` output is ignored, the server falls
        back to the last step).
        """
        from lite_server.ensemble import DagSet, EnsembleDAG, Step

        dag = EnsembleDAG(
            dags={"a": DagSet(steps=[Step(name="s0", model="sub")])},
            output="$s0",  # forbidden in the dags form — must not vanish
        )

        with pytest.raises(ValueError):
            dag.to_config()


# ---------------------------------------------------------------------------
# custom metrics
# ---------------------------------------------------------------------------


class TestRegisterMetric:
    def test_unknown_metric_type_is_rejected_at_registration(self):
        """`register_metric` documents `metric_type` as one of
        `"gauge"`/`"counter"`/`"histogram"` but accepts any string.

        An unknown type is silently dropped by the Python collector and by
        the Rust registrar, so the metric never appears anywhere and nothing
        warns — the author sees a working `register_metric`/`report_metric`
        pair and an empty dashboard.
        """
        from lite_server.api import LitAPI

        api = LitAPI()
        with pytest.raises(ValueError):
            api.register_metric("queue_depth", "counters")  # typo


# ---------------------------------------------------------------------------
# profiler
# ---------------------------------------------------------------------------


class TestWebuiProxyPathHandling:
    def test_dot_segments_cannot_change_which_model_the_acl_judged(self):
        """`webui/proxy.py` decides the model-whitelist and version-ownership
        gates from the **raw** tail (`ownership.parse_model_path(tail)`), then
        builds the upstream URL as `f"{inst.base_url}/{tail}"`, which httpx
        dot-normalizes.

        The path the ACL judged and the path the instance actually receives
        are therefore different paths, so `v2/models/alpha/../beta/infer`
        passes a whitelist check for `alpha` while the instance serves
        `beta`, and `.../versions/1/../2` misses the ownership rule entirely.
        """
        import httpx

        from lite_server.webui.ownership import parse_model_path

        tail = "v2/models/alpha/../beta/infer"
        judged = parse_model_path(tail)  # what the ACL gate evaluated
        forwarded = httpx.URL(f"http://instance/{tail}").path.lstrip("/")
        received = parse_model_path(forwarded)  # what the instance is asked for

        assert judged == received, (
            f"the ACL judged model {judged!r} but the instance is asked for "
            f"{received!r} — the raw tail is judged, the normalized one is sent"
        )


class TestProfileCheckpoint:
    def test_resumed_trials_do_not_overwrite_fresh_engine_indices(self, tmp_path):
        """`write_trials` dedupes by `trial_key` (config point, concurrency)
        but names files by `trial.index`, and a fresh `ProfileEngine` always
        restarts `_trial_index` at 0.

        `cli.py` writes `resumed_trials + result.trials`, so the trials from
        the resumed run and the newly measured ones claim the same
        `trial-000.json` / `trial-001.json` filenames. The resumed trials are
        overwritten, `read_trials` returns fewer records than were run, and
        the next `--resume` re-runs them.
        """
        resumed = [
            TrialRecord(index=0, config_point={"max_batch_size": 2}, concurrency=1, status="ok"),
            TrialRecord(index=1, config_point={"max_batch_size": 2}, concurrency=2, status="ok"),
        ]
        # Same shape a restarted engine produces: indices start at 0 again.
        fresh = [
            TrialRecord(index=0, config_point={"max_batch_size": 2}, concurrency=4, status="ok"),
            TrialRecord(index=1, config_point={"max_batch_size": 2}, concurrency=8, status="ok"),
        ]

        write_trials(tmp_path, resumed + fresh, {})

        assert len(read_trials(tmp_path)) == 4, (
            "trials from the resumed run were overwritten by newly measured "
            "trials that reuse the same index-derived filenames"
        )


class TestProfileGridDefaults:
    def test_shipped_default_grid_fits_the_shipped_default_max_trials(self):
        """The CLI defaults are `--concurrency 1,2,4,8,16` (5 levels) and
        `--max-trials 64`, while the default grid for a batching model is
        `max_batch_size` (4) × `batch_timeout` (3) × `workers_per_device` (3)
        = 36 config points.

        36 × 5 = 180 > 64, so the documented default invocation
        (`lite-server profile --model m`, no sweep flags) raises `GridError`
        before any preflight or trial ever runs.
        """
        knobs = default_knobs(batching_declared=True, continuous_batching=False)
        grid = GridSpec(
            batching_declared=True,
            continuous_batching=False,
            knobs=knobs,
            concurrency=[1, 2, 4, 8, 16],  # --concurrency default
            max_trials=64,  # --max-trials default
        )

        try:
            grid.check_cap()
        except GridError as e:
            pytest.fail(
                f"the shipped defaults cannot run at all ({grid.total_trials()} "
                f"trials vs the {grid.max_trials} cap): {e}"
            )
