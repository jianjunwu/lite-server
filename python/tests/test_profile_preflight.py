"""preflight: version gate, exclusivity guard, batching declaration, local-server detection (plan §2.4)."""

import re
from pathlib import Path

import httpx
import pytest

from lite_server.profile.preflight import (
    PreflightError,
    parse_active_workers,
    parse_queue_depth,
    parse_version,
    run_preflight,
    version_gate_ok,
)

MODEL_PY = """from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"""

BATCH_MODEL_PY = MODEL_PY.replace(
    "def encode_response(self, output):\n        return output",
    "def batch(self, inputs):\n        return inputs\n\n    def unbatch(self, outputs):\n        return outputs\n\n    def encode_response(self, output):\n        return output",
)


class FakeTransport(httpx.AsyncBaseTransport):
    """In-memory fake: mimics the endpoints profile talks to."""

    def __init__(self, *, version="0.8.4-rc1", queue_depth=0.0, ready=True,
                 metrics_extra="", continuous_batching=False):
        self.version = version
        self.queue_depth = queue_depth
        self.ready = ready
        self.metrics_extra = metrics_extra
        self.continuous_batching = continuous_batching
        self.calls: list[str] = []

    async def handle_async_request(self, request):
        self.calls.append(request.url.path)
        path = request.url.path
        if path == "/info":
            body = f'{{"version": "{self.version}"}}'
        elif path == "/metrics":
            workers = 1 if self.continuous_batching else 1
            body = (
                f"liteserver_queue_depth {self.queue_depth}\n"
                f"liteserver_active_workers{{model=\"m\",version=\"1\"}} {workers}\n"
                f"{self.metrics_extra}"
            )
        elif "/ready" in path:
            status = 200 if self.ready else 404
            return httpx.Response(status, text="{}")
        else:
            return httpx.Response(404, text="{}")
        return httpx.Response(200, text=body)


def _make_repo(tmp_path: Path, model_py: str = MODEL_PY, config_extra: str = "") -> Path:
    version_dir = tmp_path / "m" / "1"
    version_dir.mkdir(parents=True)
    (version_dir / "model.py").write_text(model_py, encoding="utf-8")
    (version_dir / "config.yaml").write_text(
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\n"
        f"accelerator: cpu\ndevices: 1\nworkers_per_device: 1\n{config_extra}",
        encoding="utf-8",
    )
    return tmp_path


def _client(transport: FakeTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=transport, base_url="http://admin.local")


class TestVersionGate:
    def test_parse_version(self):
        assert parse_version("0.8.4") == (0, 8, 4)
        assert parse_version("0.8.4-rc0") == (0, 8, 4)
        assert parse_version("1.2.3+meta") == (1, 2, 3)

    def test_rc_of_min_version_rejected_final_passes(self):
        # The tagged v0.8.4-rc0 predates the batch-0 fix and is refused;
        # rc1+ (built on the fixed tree), the final release, and newer pass.
        assert not version_gate_ok("0.8.4-rc0")
        assert version_gate_ok("0.8.4-rc1")
        assert version_gate_ok("0.8.4")
        assert version_gate_ok("0.8.4+build.3")
        assert version_gate_ok("0.9.0")

    def test_prerelease_of_newer_minor_passes(self):
        # 0.8.5-rc0 is built on a tree that already contains the 0.8.4 fix.
        assert version_gate_ok("0.8.5-rc0")

    def test_old_version_fails(self):
        assert not version_gate_ok("0.8.3")
        assert not version_gate_ok("0.7.9")

    def test_garbage_version_fails(self):
        with pytest.raises(PreflightError, match="cannot parse"):
            parse_version("unknown")

    def test_prerelease_of_min_version_rejected(self):
        # AUDIT B1 (data assumption): a pre-release of the minimum version
        # must NOT pass the gate. Semver orders 0.8.4-rc0 BEFORE 0.8.4, and
        # the tagged v0.8.4-rc0 (c7d29c4) predates the batch-0 reload fix
        # (c3b5c30) — git merge-base confirms the tag lacks it. Passing it
        # re-opens plan §0.1: ReloadModel reuses the registry's stale config
        # and every trial silently measures the same old config.
        assert not version_gate_ok("0.8.4-rc0"), (
            "pre-release of the minimum version must be refused — the tagged "
            "v0.8.4-rc0 does not contain the batch-0 disk re-read fix"
        )


class TestMetricParsers:
    def test_queue_depth_parse(self):
        text = "# HELP liteserver_queue_depth ...\nliteserver_queue_depth 3\n"
        assert parse_queue_depth(text) == 3.0
        assert parse_queue_depth("nothing here") is None

    def test_active_workers_parse(self):
        text = 'liteserver_active_workers{model="m",version="1"} 2\n'
        assert parse_active_workers(text) == 2

    def test_queue_depth_other_model_nonzero_is_not_exclusive(self):
        # AUDIT B5 (range assumption): the exclusivity guard means NO queued
        # requests anywhere — foreign traffic on ANOTHER model pollutes every
        # trial just the same (plan §2.4: "他方流量会污染全部结果"). With
        # multiple labeled series, taking only the first line (0) falsely
        # reports the server as exclusive.
        text = (
            'liteserver_queue_depth{model="m",version="1"} 0\n'
            'liteserver_queue_depth{model="other",version="1"} 5\n'
        )
        depth = parse_queue_depth(text)
        assert depth is not None and depth > 0, (
            "any non-zero queue_depth series must defeat exclusivity, not "
            "just the first one"
        )

    def test_active_workers_multi_model_reads_target_series(self):
        # AUDIT B5b (range assumption): the readiness gate compares
        # ACTIVE_WORKERS against the TARGET model's expected worker count
        # (§2.7). With another model's series first, the first-line parse
        # returns the wrong model's workers — the gate then times out on a
        # healthy reload (or passes on a mismatched one).
        text = (
            'liteserver_active_workers{model="other",version="1"} 8\n'
            'liteserver_active_workers{model="m",version="1"} 2\n'
        )
        assert parse_active_workers(text, model="m", version="1") == 2, (
            "the readiness gate needs the target model's series, not the "
            "first one"
        )

    def test_active_workers_real_export_name_parsed(self):
        # AUDIT B10 (data assumption): the real /metrics export name is
        # liteserver_active_workers (prometheus.rs:57), but the parser regex
        # matches an uppercase "ACTIVE_WORKERS" that the server never emits —
        # against a live server every parse returns None and the readiness
        # worker-count gate (§2.7 "exact") is silently skipped. The existing
        # fixtures only passed because they hand-write the wrong name.
        text = 'liteserver_active_workers{model="m",version="1"} 2\n'
        assert parse_active_workers(text, model="m", version="1") == 2


class TestRunPreflight:
    @pytest.mark.asyncio
    async def test_all_gates_pass(self, tmp_path):
        repo = _make_repo(tmp_path)
        async with _client(FakeTransport(version="0.8.4-rc1")) as client:
            result = await run_preflight(
                admin_url="http://127.0.0.1:8000", model="m", version="1",
                repo_path=repo, client=client,
            )
        assert result.version == "0.8.4-rc1"  # rc1+ is built on the fixed tree
        assert result.model_loaded is True
        assert result.exclusive is True
        assert result.batching_declared is False  # this model does not override batch/unbatch
        assert result.batching_detection == "not_declared"
        assert result.continuous_batching is False
        assert result.expected_workers == 1

    @pytest.mark.asyncio
    async def test_old_server_rejected(self, tmp_path):
        repo = _make_repo(tmp_path)
        async with _client(FakeTransport(version="0.8.3")) as client:
            with pytest.raises(PreflightError, match="too old"):
                await run_preflight(
                    admin_url="http://127.0.0.1:8000", model="m", version="1",
                    repo_path=repo, client=client,
                )

    @pytest.mark.asyncio
    async def test_unreachable_server_rejected(self, tmp_path):
        repo = _make_repo(tmp_path)
        transport = httpx.MockTransport(lambda r: httpx.Response(503, text="down"))
        async with httpx.AsyncClient(transport=transport, base_url="http://admin.local") as client:
            with pytest.raises(PreflightError, match="unreachable"):
                await run_preflight(
                    admin_url="http://127.0.0.1:8000", model="m", version="1",
                    repo_path=repo, client=client,
                )

    @pytest.mark.asyncio
    async def test_unloaded_model_rejected(self, tmp_path):
        repo = _make_repo(tmp_path)
        async with _client(FakeTransport(ready=False)) as client:
            with pytest.raises(PreflightError, match="not loaded"):
                await run_preflight(
                    admin_url="http://127.0.0.1:8000", model="m", version="1",
                    repo_path=repo, client=client,
                )

    @pytest.mark.asyncio
    async def test_busy_server_rejected_without_force(self, tmp_path):
        repo = _make_repo(tmp_path)
        async with _client(FakeTransport(queue_depth=7)) as client:
            with pytest.raises(PreflightError, match="not exclusive"):
                await run_preflight(
                    admin_url="http://127.0.0.1:8000", model="m", version="1",
                    repo_path=repo, client=client,
                )

    @pytest.mark.asyncio
    async def test_in_flight_requests_defeat_exclusivity(self, tmp_path):
        # AUDIT B8 (missing feature): plan §2.4 defines the exclusivity guard
        # as "liteserver_queue_depth == 0 AND no in-flight requests" (他方流量
        # 会污染全部结果). The server exports liteserver_in_flight_requests
        # (prometheus.rs:249), but the guard never reads it — a long-running
        # foreign stream holds zero queue depth and passes as "exclusive".
        repo = _make_repo(tmp_path)
        transport = FakeTransport(
            metrics_extra='liteserver_in_flight_requests{model="m",version="1"} 3\n',
        )
        async with _client(transport) as client:
            with pytest.raises(PreflightError, match="not exclusive"):
                await run_preflight(
                    admin_url="http://127.0.0.1:8000", model="m", version="1",
                    repo_path=repo, client=client,
                )

    @pytest.mark.asyncio
    async def test_busy_server_force_overrides(self, tmp_path):
        repo = _make_repo(tmp_path)
        async with _client(FakeTransport(queue_depth=7)) as client:
            result = await run_preflight(
                admin_url="http://127.0.0.1:8000", model="m", version="1",
                repo_path=repo, client=client, force=True,
            )
        assert result.exclusive is False  # result keeps the flag for reporting

    @pytest.mark.asyncio
    async def test_batching_declared_detected(self, tmp_path):
        repo = _make_repo(tmp_path, model_py=BATCH_MODEL_PY)
        async with _client(FakeTransport()) as client:
            result = await run_preflight(
                admin_url="http://127.0.0.1:8000", model="m", version="1",
                repo_path=repo, client=client,
            )
        assert result.batching_declared is True
        assert result.batching_detection == "declared"

    @pytest.mark.asyncio
    async def test_continuous_batching_expected_workers_is_one(self, tmp_path):
        repo = _make_repo(
            tmp_path,
            config_extra="continuous_batching: true\nworkers_per_device: 2\n",
        )
        async with _client(FakeTransport(continuous_batching=True)) as client:
            result = await run_preflight(
                admin_url="http://127.0.0.1:8000", model="m", version="1",
                repo_path=repo, client=client,
            )
        assert result.continuous_batching is True
        assert result.expected_workers == 1, "continuous_batching forces =1"

    @pytest.mark.asyncio
    async def test_corrupt_disk_config_rejected(self, tmp_path):
        version_dir = tmp_path / "m" / "1"
        version_dir.mkdir(parents=True)
        (version_dir / "model.py").write_text(MODEL_PY)
        (version_dir / "config.yaml").write_text("max_batch_size: [unclosed\n")
        async with _client(FakeTransport()) as client:
            with pytest.raises(PreflightError, match="unparseable"):
                await run_preflight(
                    admin_url="http://127.0.0.1:8000", model="m", version="1",
                    repo_path=tmp_path, client=client,
                )

    @pytest.mark.asyncio
    async def test_explicit_server_pid_marks_local(self, tmp_path):
        repo = _make_repo(tmp_path)
        async with _client(FakeTransport()) as client:
            result = await run_preflight(
                admin_url="http://127.0.0.1:8000", model="m", version="1",
                repo_path=repo, client=client, server_pid=12345,
            )
        assert result.local is True
        assert result.server_pid == 12345
