"""Test that run_liteserver.py properly cleans up on SIGTERM."""

import socket
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib import request

import pytest

RUN_SCRIPT = Path(__file__).resolve().parent.parent.parent / "benchmarks" / "scripts" / "run_liteserver.py"
MODEL_REPO = Path(__file__).resolve().parent.parent.parent / "benchmarks" / "models"


def _free_port() -> int:
    """Return a port currently unused on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_health(port: int, timeout: float = 60.0) -> bool:
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


def _drain_stream(stream, collector: list):
    """Read lines from *stream* into *collector* in a background thread."""
    for line in iter(stream.readline, b""):
        collector.append(line.decode(errors="replace"))


class TestBenchmarkLifecycle:
    def test_sigterm_cleans_up_child_process(self):
        """SIGTERM to run_liteserver.py should also terminate lite-server serve."""
        port = _free_port()

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

        stderr_lines: list[str] = []
        stderr_drainer = threading.Thread(target=_drain_stream, args=(proc.stderr, stderr_lines), daemon=True)
        stderr_drainer.start()

        try:
            # Wait for server to be ready
            assert _wait_for_health(port), (
                f"lite-server failed to start on port {port}\n"
                f"stderr:\n{''.join(stderr_lines[-50:])}"
            )

            # Verify server is actually responding
            assert _wait_for_health(port, timeout=1), "health endpoint not responding"

            # Send SIGTERM to run_liteserver.py
            proc.send_signal(signal.SIGTERM)

            # Wait for run_liteserver.py to exit
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pytest.fail("run_liteserver.py did not exit after SIGTERM")

            # Wait for cleanup: health endpoint should stop responding
            deadline = time.time() + 5
            still_alive = False
            while time.time() < deadline:
                try:
                    req = request.Request(f"http://127.0.0.1:{port}/health", method="GET")
                    with request.urlopen(req, timeout=1) as resp:
                        if resp.status == 200:
                            time.sleep(0.3)
                            continue
                except Exception:
                    break
            else:
                still_alive = True

            assert not still_alive, (
                f"lite-server still responding on port {port} after SIGTERM"
            )
        finally:
            # Hard kill any remaining processes from this test
            if proc.poll() is None:
                proc.kill()
                proc.wait()
