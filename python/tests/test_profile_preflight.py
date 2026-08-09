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

    def __init__(self, *, version="0.8.4-rc0", queue_depth=0.0, ready=True,
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
                f"ACTIVE_WORKERS{{model=\"m\",version=\"1\"}} {workers}\n"
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

    def test_rc_carries_the_fix_passes(self):
        assert version_gate_ok("0.8.4-rc0")
        assert version_gate_ok("0.8.4")
        assert version_gate_ok("0.9.0")

    def test_old_version_fails(self):
        assert not version_gate_ok("0.8.3")
        assert not version_gate_ok("0.7.9")

    def test_garbage_version_fails(self):
        with pytest.raises(PreflightError, match="cannot parse"):
            parse_version("unknown")


class TestMetricParsers:
    def test_queue_depth_parse(self):
        text = "# HELP liteserver_queue_depth ...\nliteserver_queue_depth 3\n"
        assert parse_queue_depth(text) == 3.0
        assert parse_queue_depth("nothing here") is None

    def test_active_workers_parse(self):
        text = 'ACTIVE_WORKERS{model="m",version="1"} 2\n'
        assert parse_active_workers(text) == 2


class TestRunPreflight:
    @pytest.mark.asyncio
    async def test_all_gates_pass(self, tmp_path):
        repo = _make_repo(tmp_path)
        async with _client(FakeTransport(version="0.8.4-rc0")) as client:
            result = await run_preflight(
                admin_url="http://127.0.0.1:8000", model="m", version="1",
                repo_path=repo, client=client,
            )
        assert result.version == "0.8.4-rc0"  # rc0 carries the fix, passes the gate
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
