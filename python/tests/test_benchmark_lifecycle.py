"""Test that run_liteserver.py properly cleans up on SIGTERM."""

import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib import request

import pytest

RUN_SCRIPT = Path(__file__).resolve().parent.parent.parent / "benchmarks" / "scripts" / "run_liteserver.py"
MODEL_REPO = Path(__file__).resolve().parent.parent.parent / "benchmarks" / "models"


def _clean_env() -> dict:
    """Return a copy of os.environ without PYTHONPATH.

    PYTHONPATH=python (set for dev tests) would interfere with the
    child lite-server process, which must use the installed package.
    """
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    return env


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


def _spawn_tree(cmd: list[str], **kwargs) -> subprocess.Popen:
    """Popen in its own process group so teardown can kill the whole tree
    (run_liteserver + serve + workers), not just the direct child."""
    return subprocess.Popen(cmd, start_new_session=True, **kwargs)


def _kill_tree(proc: subprocess.Popen) -> None:
    """Hard-kill proc's entire process group; safe on every exit path.

    A process group outlives its leader, so this also reaps grandchildren
    that survived a leader exit — the failure mode that used to leak orphan
    serve/worker processes whose workers steal sockets and poison every
    later run."""
    if hasattr(os, "killpg"):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    if proc.poll() is None:
        proc.kill()  # Windows fallback + group-less stragglers
    proc.wait()


class TestBenchmarkLifecycle:
    def test_sigterm_cleans_up_child_process(self, tmp_path):
        """SIGTERM to run_liteserver.py should also terminate lite-server serve."""
        port = _free_port()

        # Copy model repo to a temp dir to isolate from concurrent test runs.
        # Use copytree with ignore to exclude __pycache__ which can carry
        # stale bytecode from a different Python version.
        tmp_repo = tmp_path / "models"
        shutil.copytree(MODEL_REPO, tmp_repo, ignore=shutil.ignore_patterns("__pycache__"))

        proc = _spawn_tree(
            [
                sys.executable,
                str(RUN_SCRIPT),
                "--port", str(port),
                "--workers", "1",
                "--duration", "60",
                "--model-repo", str(tmp_repo),
                "--model", "sleep_1ms_model",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=_clean_env(),
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
            # Kill the whole tree: a startup hang or failed assert must not
            # leak orphan servers into later runs.
            _kill_tree(proc)


class TestPortConflictStartup:
    """Multi-platform audit (2026-08-02): a startup port conflict must fail
    fast with an error, not wedge the server forever.

    The Python CLI drops the tokio runtime when `run()` returns the bind
    error, and the drop deadlocks: BlockingPool::shutdown waits for the
    in-flight ZMQ worker-actor blocking task, which exits only when its
    command channel closes — and the abandoned spawned server tasks still
    hold their WorkerManager Arc. The process then lives on with no error,
    no listeners, and orphaned workers. (The lite-server-core binary exits
    via std::process::exit(1), which skips destructors — so it does not hang;
    this test pins the Python CLI path.)
    """

    def test_grpc_port_conflict_fails_fast_not_wedge(self, tmp_path):
        port = _free_port()
        grpc_port = _free_port()

        # Occupy the gRPC port — a second lite-server (or any other service).
        hog = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        hog.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        hog.bind(("127.0.0.1", grpc_port))
        hog.listen(1)

        # Model repo with orchestration auto-load: the worker (and its ZMQ
        # actor) must be up before the gRPC bind error unwinds run().
        tmp_repo = tmp_path / "models"
        shutil.copytree(MODEL_REPO, tmp_repo, ignore=shutil.ignore_patterns("__pycache__"))
        cfg = tmp_repo / "server.yaml"
        cfg.write_text(
            "server:\n"
            f"  host: 127.0.0.1\n  http_port: {port}\n  grpc_port: {grpc_port}\n"
            "  metrics_port: 18299\n  log_level: warn\n"
            "metrics:\n  enabled: false\n"
            "grpc:\n  enabled: true\n"
            "orchestration:\n"
            "  control_mode: explicit\n"
            "  load_models: [sleep_1ms_model]\n"
            "  models:\n"
            "    - name: sleep_1ms_model\n"
            "      load_policy: all\n"
            f"model_repository:\n  path: {tmp_repo}\n"
        )

        proc = _spawn_tree(
            [sys.executable, "-m", "lite_server.cli", "serve", "--config", str(cfg)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=_clean_env(),
        )
        try:
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                # The wedged server leaves its python worker orphaned (no parent
                # to watch it); reap processes of THIS test's repo only, so
                # parallel tests' workers are never touched.
                import psutil

                for p in psutil.process_iter(["cmdline"]):
                    try:
                        cmd = " ".join(p.info["cmdline"] or [])
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                    if str(tmp_repo) in cmd:
                        p.kill()
                hog.close()
                pytest.fail(
                    "server wedged on a gRPC port conflict (Python CLI): the tokio "
                    "Runtime drop deadlocks on the in-flight ZMQ worker actor; it "
                    "must fail fast with a startup error"
                )

            hog.close()
            assert proc.returncode != 0, "server must exit non-zero on a port conflict"
            stderr = proc.stderr.read().decode(errors="replace") if proc.stderr else ""
            assert "Server error" in stderr, f"expected a startup error; got: {stderr[-500:]}"
        finally:
            _kill_tree(proc)
