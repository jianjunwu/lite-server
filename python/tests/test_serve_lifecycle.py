"""Lifecycle tests for the PyO3 ``serve()`` entry point.

Covers three properties of the embedding design:
  * R3 — re-entrancy guard: a second ``serve()`` while one is running fails
    fast, instead of racing on the port (which can wedge the tokio runtime drop
    on the Python CLI path).
  * R2 — programmatic stop: ``stop_server()`` triggers graceful shutdown so a
    blocked ``serve()`` returns; callable from another thread, idempotent.
  * R6 — GIL release: ``serve()`` releases the GIL (``allow_threads``) so other
    Python threads progress while the calling thread is blocked.

These run with no model loaded (control_mode explicit, empty repo) so they are
fast and isolated.
"""

import socket
import threading
import time
from urllib import request

import pytest

from lite_server import serve, stop_server


def _free_port() -> int:
    """Return a port currently unused on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


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


def _serve_kwargs(port: int) -> dict:
    """Minimal serve() kwargs: HTTP only, no models, short graceful timeout."""
    return dict(
        port=port,
        host="127.0.0.1",
        no_metrics=True,
        no_grpc=True,
        graceful_timeout=2.0,
        log_level="warn",
    )


def _start_serve(port: int) -> threading.Thread:
    t = threading.Thread(target=serve, kwargs=_serve_kwargs(port), daemon=True)
    t.start()
    return t


class TestServeReentry:
    """R3: a second serve() while one is running fails fast with a clear error."""

    def test_second_serve_is_rejected(self):
        port1 = _free_port()
        port2 = _free_port()
        t = _start_serve(port1)
        try:
            assert _wait_for_health(port1, 30), "first serve() did not start"
            with pytest.raises(RuntimeError, match="already running"):
                serve(**_serve_kwargs(port2))
        finally:
            stop_server()
            t.join(timeout=15)


class TestStopServer:
    """R2: stop_server() triggers graceful shutdown of a blocked serve()."""

    def test_stop_unblocks_serve_and_is_idempotent(self):
        port = _free_port()
        t = _start_serve(port)
        try:
            assert _wait_for_health(port, 30), "serve() did not start"
            assert stop_server() is True, "stop_server() must signal the running serve()"
            t.join(timeout=10)
            assert not t.is_alive(), "serve() did not return after stop_server()"
            # Idempotent: a second call finds nothing running and does not raise.
            assert stop_server() is False
        finally:
            stop_server()
            t.join(timeout=10)


class TestGilRelease:
    """R6: serve() releases the GIL so a second Python thread makes progress
    while serve() is blocked on its caller thread."""

    def test_serve_releases_gil(self):
        port = _free_port()
        t = _start_serve(port)
        try:
            assert _wait_for_health(port, 30), "serve() did not start"

            progress = []

            def worker():
                # Each append needs the GIL; time.sleep releases it. If serve()
                # held the GIL the whole time, this thread would starve and
                # progress would stay far below 50.
                for i in range(50):
                    progress.append(i)
                    time.sleep(0.005)

            w = threading.Thread(target=worker)
            w.start()
            w.join(timeout=5)  # join BEFORE stop_server, while serve() is still up
            assert not w.is_alive(), "worker thread did not finish in time"
            assert len(progress) == 50, (
                f"second Python thread made no GIL progress (only "
                f"{len(progress)}/50 iterations) — serve() did not release the GIL"
            )
        finally:
            stop_server()
            t.join(timeout=10)
