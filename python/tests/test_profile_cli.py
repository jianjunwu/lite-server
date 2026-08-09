"""profile CLI: arg parsing, grid construction from --sweep-knob, exit codes
for preflight/grid failures, --recover. The end-to-end smoke lives in
test_profile_smoke.py (real in-process server)."""

import socket
import threading
import time
from pathlib import Path
from urllib import request

import pytest

from lite_server import serve, stop_server
from lite_server.cli import main

MODEL_PY = """from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"ok": x}

    def encode_response(self, output):
        return output
"""


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def repo(tmp_path) -> Path:
    version_dir = tmp_path / "m" / "1"
    version_dir.mkdir(parents=True)
    (version_dir / "model.py").write_text(MODEL_PY, encoding="utf-8")
    (version_dir / "config.yaml").write_text(
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\n"
        "accelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        encoding="utf-8",
    )
    return tmp_path


class TestProfileCli:
    def test_missing_model_flag_is_usage_error(self, repo):
        with pytest.raises(SystemExit):
            main(["profile", "--repo", str(repo), "--dry-run"])

    def test_preflight_failure_exits_2(self, repo):
        # Nothing listening on the admin port → preflight refuses
        port = _free_port()
        rc = main([
            "profile", "--model", "m", "--repo", str(repo),
            "--admin-url", f"http://127.0.0.1:{port}", "--dry-run",
        ])
        assert rc == 2

    def test_undeclared_batching_swept_key_exits_2(self, repo):
        # Model does not override batch/unbatch → max_batch_size sweep is
        # rejected by the grid (LS101 scenario)
        rc = main([
            "profile", "--model", "m", "--repo", str(repo),
            "--sweep-knob", "max_batch_size=2,4", "--dry-run",
        ])
        assert rc == 2

    def test_quick_search_dry_run_reports_grid(self, repo):
        # Quick search with --dry-run: preflight gate fails first against a
        # dead admin port — exit 2 from the gate, not the mode.
        dead_port = _free_port()
        rc = main([
            "profile", "--model", "m", "--repo", str(repo),
            "--admin-url", f"http://127.0.0.1:{dead_port}",
            "--search-mode", "quick", "--dry-run",
        ])
        assert rc == 2

    def test_quick_search_with_resume_refused(self, repo, tmp_path):
        export = tmp_path / "ckpt"
        export.mkdir()
        (export / "summary.json").write_text("{}")
        rc = main([
            "profile", "--model", "m", "--repo", str(repo),
            "--search-mode", "quick", "--resume", str(export),
        ])
        assert rc == 2

    def test_recover_without_backup_exits_2(self, repo):
        rc = main([
            "profile", "--model", "m", "--repo", str(repo), "--recover",
        ])
        assert rc == 2

    def test_recover_restores_backup_bytes(self, repo, tmp_path):
        from lite_server.profile.config_writer import write_backup

        cfg = tmp_path / "m" / "1" / "config.yaml"
        original = cfg.read_bytes()
        write_backup(cfg, campaign_hash="c")
        cfg.write_text("max_batch_size: 99\n", encoding="utf-8")
        rc = main([
            "profile", "--model", "m", "--repo", str(repo), "--recover",
        ])
        assert rc == 0
        assert cfg.read_bytes() == original
