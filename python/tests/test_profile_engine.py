"""ProfileEngine: the §2.6 five-step sequence (write → ReloadModel → Ready
→ concurrency sweep → restore), trial-failure policy and circuit breaker,
stale-backup gate, dry-run."""

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from lite_server.profile.checkpoint import campaign_hash
from lite_server.profile.config_writer import has_stale_backup, write_backup
from lite_server.profile.engine import (
    ProfileAbort,
    ProfileEngine,
    ProfileFailure,
)
from lite_server.profile.grid import GridSpec
from lite_server.profile.preflight import PreflightResult

CONFIG_TEMPLATE = """max_batch_size: 1
batch_timeout: 0.0
stream: false
accelerator: cpu
devices: 1
workers_per_device: 1
"""


class AdminFake(httpx.AsyncBaseTransport):
    """In-memory Admin fake: tracks reload calls, configurable ready behavior."""

    def __init__(self, *, ready=True, fail_reload=False, workers=1, ready_after_reloads=0):
        self.ready = ready
        self.fail_reload = fail_reload
        self.workers = workers
        self.ready_after_reloads = ready_after_reloads
        self.reload_count = 0
        self.reload_requests: list[str] = []

    async def handle_async_request(self, request):
        self.reload_requests.append(request.url.path)
        if request.url.path.endswith("/reload"):
            self.reload_count += 1
            if self.fail_reload:
                return httpx.Response(500, text="reload failed")
            return httpx.Response(200, text="{}")
        if "/ready" in request.url.path:
            ok = self.ready or self.reload_count > self.ready_after_reloads
            return httpx.Response(200 if ok else 404, text="{}")
        if request.url.path == "/metrics":
            return httpx.Response(
                200,
                text=f'ACTIVE_WORKERS{{model="m",version="1"}} {self.workers}\n'
                     "liteserver_queue_depth 0\n",
            )
        return httpx.Response(404, text="{}")


def _preflight(continuous_batching=False, expected_workers=1, exclusive=True) -> PreflightResult:
    return PreflightResult(
        version="0.8.4-rc0",
        model="m",
        resolved_version="1",
        model_loaded=True,
        exclusive=exclusive,
        batching_declared=False,
        batching_detection="not_declared",
        continuous_batching=continuous_batching,
        local=True,
        server_pid=123,
        expected_workers=expected_workers,
    )


def _engine(tmp_path: Path, transport: httpx.AsyncBaseTransport, measure=None,
            reload_timeout=30.0, max_trial_failures=3) -> ProfileEngine:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")
    client = httpx.AsyncClient(transport=transport, base_url="http://admin.local")
    grid = GridSpec(batching_declared=False, knobs={}, concurrency=[1, 2])
    campaign = campaign_hash("m", "1", {}, {})
    async def default_measure(c: int) -> dict:
        return {"concurrency": c, "tp": 10.0}
    return ProfileEngine(
        config_path=cfg,
        model="m",
        version="1",
        admin_url="http://127.0.0.1:8000",
        grid=grid,
        preflight=_preflight(),
        measure=measure or default_measure,
        client=client,
        reload_timeout=reload_timeout,
        max_trial_failures=max_trial_failures,
        campaign=campaign,
    )


class TestRunSequence:
    @pytest.mark.asyncio
    async def test_baseline_only_no_reload_and_trials_recorded(self, tmp_path):
        transport = AdminFake()
        engine = _engine(tmp_path, transport)
        result = await engine.run()
        assert len(result.trials) == 2  # baseline × 2 concurrency levels
        assert engine_after(tmp_path, "config.yaml") is not None
        assert result.trials[0].status == "ok"
        assert result.trials[0].metrics["tp"] == 10.0
        assert result.trials[0].config_point == {}
        assert transport.reload_count == 0

    @pytest.mark.asyncio
    async def test_grid_point_edits_config_reloads_and_restores_byte_exact(self, tmp_path):
        transport = AdminFake(workers=2)
        cfg = tmp_path / "config.yaml"
        cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        grid = GridSpec(
            batching_declared=False,
            knobs={"workers_per_device": [1, 2]},
            concurrency=[1],
        )
        measured: list[dict] = []

        async def measure(c: int) -> dict:
            import yaml

            measured.append(yaml.safe_load(cfg.read_text()))
            return {"tp": 1.0}

        client = httpx.AsyncClient(transport=transport, base_url="http://admin.local")
        engine = ProfileEngine(
            config_path=cfg, model="m", version="1",
            admin_url="http://127.0.0.1:8000", grid=grid,
            preflight=_preflight(expected_workers=2), measure=measure,
            client=client, reload_timeout=30.0,
            campaign=campaign_hash("m", "1", grid.knobs, {}),
        )
        result = await engine.run()
        assert result.trials[0].status == "ok"  # baseline
        assert result.trials[1].status == "ok"  # workers_per_device=1
        assert result.trials[2].status == "ok"  # workers_per_device=2
        assert [t.config_point.get("workers_per_device") for t in result.trials] == [None, 1, 2]
        # restored byte-exact
        assert cfg.read_bytes().decode("utf-8") == CONFIG_TEMPLATE
        assert not has_stale_backup(cfg)
        # edits are atomic: what measure sees parses cleanly
        for m in measured:
            assert set(m) >= {"workers_per_device"}

    @pytest.mark.asyncio
    async def test_reload_failure_marks_point_failed_and_continues(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        grid = GridSpec(batching_declared=False, knobs={"workers_per_device": [1, 2]}, concurrency=[1])
        transport = AdminFake(fail_reload=True)
        client = httpx.AsyncClient(transport=transport, base_url="http://admin.local")

        async def measure(c: int) -> dict:
            return {"tp": 1.0}

        engine = ProfileEngine(
            config_path=cfg, model="m", version="1", admin_url="http://127.0.0.1:8000",
            grid=grid, preflight=_preflight(), measure=measure,
            client=client, reload_timeout=5.0,
            campaign=campaign_hash("m", "1", grid.knobs, {}),
        )
        result = await engine.run()
        failed = [t for t in result.trials if t.status == "failed"]
        assert len(failed) == 2, "failed point → all its concurrency trials recorded failed"
        assert all("ReloadModel" in (t.reason or "") for t in failed)
        assert cfg.read_bytes().decode("utf-8") == CONFIG_TEMPLATE, "original config restored even after failure"

    @pytest.mark.asyncio
    async def test_circuit_breaker_aborts_after_max_failures(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        grid = GridSpec(batching_declared=False, knobs={"workers_per_device": [1, 2, 3]}, concurrency=[1])
        client = httpx.AsyncClient(transport=AdminFake(fail_reload=True), base_url="http://admin.local")
        engine = ProfileEngine(
            config_path=cfg, model="m", version="1", admin_url="http://127.0.0.1:8000",
            grid=grid, preflight=_preflight(), measure=lambda c: {"tp": 1.0},
            client=client, reload_timeout=5.0, max_trial_failures=2,
            campaign=campaign_hash("m", "1", grid.knobs, {}),
        )
        with pytest.raises(ProfileAbort, match="circuit breaker"):
            await engine.run()
        assert cfg.read_bytes().decode("utf-8") == CONFIG_TEMPLATE, "original config restored after the breaker"
        assert not has_stale_backup(cfg)

    @pytest.mark.asyncio
    async def test_stale_backup_blocks_run(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        write_backup(cfg, campaign_hash="stale")
        engine = _engine(tmp_path, AdminFake())
        with pytest.raises(ProfileFailure, match="stale"):
            await engine.run()

    @pytest.mark.asyncio
    async def test_measure_exception_marks_trial_failed(self, tmp_path):
        async def bad_measure(c: int) -> dict:
            raise RuntimeError("boom")
        engine = _engine(tmp_path, AdminFake(), measure=bad_measure)
        result = await engine.run()
        assert all(t.status == "failed" for t in result.trials)
        assert all("boom" in (t.reason or "") for t in result.trials)
        assert not has_stale_backup(tmp_path / "config.yaml")

    @pytest.mark.asyncio
    async def test_ready_timeout_marks_point_failed(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        grid = GridSpec(batching_declared=False, knobs={"workers_per_device": [2]}, concurrency=[1])
        # Point reload (1st) never ready → point fails; restore reload (2nd) is ready.
        transport = AdminFake(ready=False, ready_after_reloads=1)
        client = httpx.AsyncClient(transport=transport, base_url="http://admin.local")

        async def measure(c: int) -> dict:
            return {"tp": 1.0}

        engine = ProfileEngine(
            config_path=cfg, model="m", version="1", admin_url="http://127.0.0.1:8000",
            grid=grid, preflight=_preflight(), measure=measure,
            client=client, reload_timeout=1.5,
            campaign=campaign_hash("m", "1", grid.knobs, {}),
        )
        result = await engine.run()
        failed = [t for t in result.trials if t.status == "failed"]
        assert len(failed) == 1 and "Ready timeout" in failed[0].reason
        assert cfg.read_bytes().decode("utf-8") == CONFIG_TEMPLATE

    @pytest.mark.asyncio
    async def test_restore_wait_timeout_is_profile_failure_but_bytes_restored(self, tmp_path):
        """Server never becomes ready: the point fails → the restore path times
        out too → exit 2 per plan (restore verification failure = profile failure);
        the byte restore already ran, the repo is not left modified."""
        cfg = tmp_path / "config.yaml"
        cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        grid = GridSpec(batching_declared=False, knobs={"workers_per_device": [2]}, concurrency=[1])
        transport = AdminFake(ready=False, ready_after_reloads=999)
        client = httpx.AsyncClient(transport=transport, base_url="http://admin.local")

        async def measure(c: int) -> dict:
            return {"tp": 1.0}

        engine = ProfileEngine(
            config_path=cfg, model="m", version="1", admin_url="http://127.0.0.1:8000",
            grid=grid, preflight=_preflight(), measure=measure,
            client=client, reload_timeout=1.0,
            campaign=campaign_hash("m", "1", grid.knobs, {}),
        )
        with pytest.raises(ProfileFailure, match="waiting for Ready timed out"):
            await engine.run()
        assert cfg.read_bytes().decode("utf-8") == CONFIG_TEMPLATE, "byte restore runs first, repo safe"


class TestDryRun:
    def test_dry_run_report_zero_side_effects(self, tmp_path):
        engine = _engine(tmp_path, AdminFake())
        report = engine.dry_run_report()  # type: ignore[call-arg] — estimate has a default
        assert report["grid"]["config_points"] == [{}]
        assert report["grid"]["total_trials"] == 2
        assert report["preflight"]["expected_workers"] == 1
        assert cfg_bytes(tmp_path) == CONFIG_TEMPLATE  # zero side effects
        assert not has_stale_backup(tmp_path / "config.yaml")


def engine_after(tmp_path: Path, name: str) -> Path | None:
    p = tmp_path / name
    return p if p.exists() else None


def cfg_bytes(tmp_path: Path) -> str:
    return (tmp_path / "config.yaml").read_bytes().decode("utf-8")
