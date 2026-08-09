"""End-to-end smoke (plan batch 1): a real in-process server + a model whose
predict() returns its constructor max_batch_size. The profile run must
measure values that CHANGE with the swept config — the direct §0.1 mechanism
check: if reload_model reused the registry's stale config, every trial would
serve the same old value and this test fails.

Also covers: config.yaml byte-exact restore, no stale backup left, --dry-run
zero side effects, and the preflight gates against a live server.
"""

import json
import socket
import threading
import time
from pathlib import Path
from urllib import request

import httpx
import pytest

from lite_server import serve, stop_server
from lite_server.profile.checkpoint import campaign_hash
from lite_server.profile.engine import ProfileEngine
from lite_server.profile.grid import GridSpec
from lite_server.profile.preflight import run_preflight

MODEL_PY = """from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def batch(self, inputs):
        return inputs

    def unbatch(self, outputs):
        return outputs

    def predict(self, x):
        # Batching is not always active for a single request; handle both
        # shapes. The constructor arg is the served value under test.
        if isinstance(x, list):
            return [{"max_batch_size": self.max_batch_size} for _ in x]
        return {"max_batch_size": self.max_batch_size}

    def encode_response(self, output):
        return output
"""


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_http(url: str, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _wait_ready(port: int, model: str, version: str, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    url = f"http://127.0.0.1:{port}/v2/models/{model}/versions/{version}/ready"
    while time.time() < deadline:
        try:
            with request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception:
            pass
        time.sleep(0.3)
    raise AssertionError(f"model {model} {version} never became ready")


@pytest.fixture
def live_server(tmp_path):
    """Real in-process server with a max_batch_size-echo model loaded."""
    repo = tmp_path / "model_repo"
    version_dir = repo / "m" / "1"
    version_dir.mkdir(parents=True)
    (version_dir / "model.py").write_text(MODEL_PY, encoding="utf-8")
    (version_dir / "config.yaml").write_text(
        "max_batch_size: 2\nbatch_timeout: 0.0\nstream: false\n"
        "accelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        encoding="utf-8",
    )
    port = _free_port()
    metrics_port = _free_port()
    t = threading.Thread(
        target=serve,
        kwargs=dict(
            port=port,
            host="127.0.0.1",
            model_repo=str(repo),
            metrics_port=metrics_port,
            no_grpc=True,
            graceful_timeout=2.0,
            log_level="warn",
        ),
        daemon=True,
    )
    t.start()
    assert _wait_http(f"http://127.0.0.1:{port}/health"), "server did not start"
    # Explicit Admin load (control_mode default is explicit — deterministic;
    # versioned load lives under /v2/repository/models/...)
    req = request.Request(
        f"http://127.0.0.1:{port}/v2/repository/models/m/versions/1/load",
        method="POST",
    )
    with request.urlopen(req, timeout=30) as resp:
        assert resp.status == 200
    _wait_ready(port, "m", "1")
    yield {
        "repo": repo,
        "port": port,
        "admin_url": f"http://127.0.0.1:{port}",
        "metrics_url": f"http://127.0.0.1:{metrics_port}",
    }
    stop_server()
    t.join(timeout=15)


def _find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = _find_key(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_key(v, key)
            if found is not None:
                return found
    return None


class TestProfileSmoke:
    @pytest.mark.asyncio
    async def test_measured_value_follows_swept_config(self, live_server):
        """§0.1 mechanism check: after write → ReloadModel → Ready, the served
        max_batch_size must be the NEW value (worker rebuilt with constructor
        args from the re-read config)."""
        admin_url = live_server["admin_url"]
        repo = live_server["repo"]
        async with httpx.AsyncClient(base_url=admin_url, timeout=30.0, trust_env=False) as client:
            preflight = await run_preflight(
                admin_url=admin_url, model="m", version="1",
                repo_path=repo, client=client,
                metrics_url=live_server["metrics_url"],
            )
            assert preflight.batching_declared is True
            assert preflight.expected_workers == 1

            grid = GridSpec(
                batching_declared=True,
                knobs={"max_batch_size": [2, 4]},
                concurrency=[1],
            )
            served: list[int] = []

            async def measure(_c: int) -> dict:
                resp = await client.post(
                    f"{admin_url}/v2/models/m/versions/1/infer",
                    json={"input": 1},
                )
                resp.raise_for_status()
                value = _find_key(resp.json(), "max_batch_size")
                served.append(value)
                return {"served_max_batch_size": value}

            engine = ProfileEngine(
                config_path=repo / "m" / "1" / "config.yaml",
                model="m", version="1",
                admin_url=admin_url,
                grid=grid, preflight=preflight, measure=measure,
                client=client, reload_timeout=60.0,
                campaign=campaign_hash("m", "1", grid.knobs, {}),
                metrics_url=live_server["metrics_url"],
            )
            result = await engine.run()

        assert len(served) == 3  # baseline(2) + grid point mbs=2 + mbs=4
        assert served == [2, 2, 4], (
            f"served max_batch_size must follow the swept config (§0.1): {served}"
        )
        assert all(t.status == "ok" for t in result.trials)
        # config.yaml byte-exact restore + no stale backup
        original = (repo / "m" / "1" / "config.yaml").read_bytes()
        assert b"max_batch_size: 2" in original
        assert not (repo / "m" / "1" / "config.yaml.profile.backup").exists()

    @pytest.mark.asyncio
    async def test_dry_run_zero_side_effects(self, live_server):
        """--dry-run: preflight conclusions + grid + estimate, nothing written,
        no reload issued."""
        admin_url = live_server["admin_url"]
        repo = live_server["repo"]
        config_path = repo / "m" / "1" / "config.yaml"
        before = config_path.read_bytes()
        async with httpx.AsyncClient(base_url=admin_url, timeout=30.0, trust_env=False) as client:
            preflight = await run_preflight(
                admin_url=admin_url, model="m", version="1",
                repo_path=repo, client=client,
                metrics_url=live_server["metrics_url"],
            )
            grid = GridSpec(
                batching_declared=True,
                knobs={"max_batch_size": [4, 8]},
                concurrency=[1],
            )

            async def measure(_c: int) -> dict:
                return {"tp": 1.0}

            engine = ProfileEngine(
                config_path=config_path,
                model="m", version="1",
                admin_url=admin_url,
                grid=grid, preflight=preflight, measure=measure,
                client=client, reload_timeout=5.0,
                campaign=campaign_hash("m", "1", grid.knobs, {}),
                metrics_url=live_server["metrics_url"],
            )
            report = engine.dry_run_report()
        assert report["grid"]["total_trials"] == 3  # baseline + 2 config points
        assert config_path.read_bytes() == before, "dry-run must not touch config.yaml"
        assert not (repo / "m" / "1" / "config.yaml.profile.backup").exists()

    @pytest.mark.asyncio
    async def test_preflight_gates_pass_against_live_server(self, live_server):
        admin_url = live_server["admin_url"]
        repo = live_server["repo"]
        async with httpx.AsyncClient(base_url=admin_url, timeout=30.0, trust_env=False) as client:
            preflight = await run_preflight(
                admin_url=admin_url, model="m", version="1",
                repo_path=repo, client=client,
                metrics_url=live_server["metrics_url"],
            )
        assert preflight.model_loaded is True
        assert preflight.exclusive is True
        assert preflight.continuous_batching is False
        assert preflight.expected_workers == 1
