"""Test that run_liteserver.py properly cleans up on SIGTERM."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib import request

import pytest

RUN_SCRIPT = Path(__file__).resolve().parent.parent.parent / "benchmarks" / "scripts" / "run_liteserver.py"
MODEL_REPO = Path(__file__).resolve().parent.parent.parent / "benchmarks" / "models"


def _wait_for_health(port: int, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = request.Request(f"http://127.0.0.1:{port}/health", method="GET")
            with request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _lite_server_procs() -> list:
    result = subprocess.run(
        ["ps", "aux"],
        capture_output=True,
        text=True,
    )
    procs = []
    for line in result.stdout.splitlines():
        if "lite-server serve" in line and "grep" not in line:
            procs.append(line)
    return procs


class TestBenchmarkLifecycle:
    def test_sigterm_cleans_up_child_process(self):
        """SIGTERM to run_liteserver.py should also terminate lite-server serve."""
        port = 18000  # Use high port to avoid conflicts

        # Count existing lite-server processes before test
        before = _lite_server_procs()
        before_count = len(before)

        proc = subprocess.Popen(
            [
                sys.executable,
                str(RUN_SCRIPT),
                "--port", str(port),
                "--workers", "1",
                "--duration", "60",
                "--model-repo", str(MODEL_REPO),
                "--model", "sleep_1ms_model",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        try:
            # Wait for server to be ready
            assert _wait_for_health(port, timeout=30), "lite-server failed to start"

            # Verify lite-server serve process exists
            during = _lite_server_procs()
            assert len(during) > before_count, "lite-server serve process not found"

            # Send SIGTERM to run_liteserver.py
            proc.send_signal(signal.SIGTERM)

            # Wait for run_liteserver.py to exit
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pytest.fail("run_liteserver.py did not exit after SIGTERM")

            # Wait a bit for cleanup
            time.sleep(1)

            # Verify lite-server serve process is gone
            after = _lite_server_procs()
            after_count = len(after)

            assert after_count == before_count, (
                f"lite-server serve process leaked: "
                f"before={before_count}, after={after_count}\n"
                f"Remaining: {after}"
            )
        finally:
            # Hard kill any remaining processes from this test
            if proc.poll() is None:
                proc.kill()
                proc.wait()

            # Also kill any stray lite-server serve on this port
            for line in _lite_server_procs():
                if f"--port {port}" in line:
                    pid = int(line.split()[1])
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
