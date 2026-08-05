"""Audit repro tests for commit 23ee783 (parent watchpoint PID-1 fix).

B1 — 测试缺陷 (FIXED): ``_start_parent_watchpoint_sync`` originally returned
an unstoppable daemon thread armed with ``os._exit``.  Tests installing it
under monkeypatched ``os.getppid``/``os._exit`` leaked the thread past
fixture teardown; the next 1 Hz tick then saw the *real* ppid and fired the
*real* ``os._exit(0)``, killing pytest with exit status 0 (silent green —
demonstrated end-to-end with the project's own xdist addopts).  The fix adds
a ``stop_event`` test hook so the thread can be joined before teardown.
The test below pins the new contract.

B2 — 功能缺陷/可观测性 (FIXED): the sync variant claimed "Same semantics" as
the async ``_start_parent_watchpoint`` but took no logger and emitted nothing
when suiciding on a server-pid mismatch — the async twin logs a warning in
that exact path.  The fix gives the sync variant the same ``log`` parameter
and the same warning messages.  The test below pins the parity.
"""

import logging
import os
import threading
import time

import pytest

from lite_server.worker import inference


class TestSyncWatcherStopEventContract:
    """B1: with ``stop_event`` the watcher thread joins cleanly before
    monkeypatch teardown and never fires ``os._exit`` afterwards."""

    def test_watcher_thread_stops_on_event_and_never_exits_after_teardown(self, monkeypatch):
        # Guard: the stubbed pid must differ from this process's real parent.
        assert os.getppid() != 424242

        monkeypatch.setattr(os, "getppid", lambda: 424242)
        exited = []
        monkeypatch.setattr(os, "_exit", lambda code=0: exited.append(code))

        stop = threading.Event()
        t = inference._start_parent_watchpoint_sync(
            logging.getLogger("t"), server_pid=424242, stop_event=stop,
        )
        assert t.is_alive()

        # Stop and join BEFORE teardown (what TestParentWatchpointSync's
        # watcher_factory fixture now does).
        stop.set()
        t.join(timeout=2)
        assert not t.is_alive()
        assert exited == []

        # Teardown: real getppid/_exit restored.  Spy (raising SystemExit to
        # be leak-safe even on regression) proves no stray thread survives
        # to fire the real os._exit during the rest of the suite.
        monkeypatch.undo()
        real_exit = os._exit
        exit_calls = []

        def _spy(code=0):
            exit_calls.append(code)
            raise SystemExit  # terminates only a hypothetically leaked thread

        os._exit = _spy
        try:
            time.sleep(1.2)  # > one 1 Hz tick
        finally:
            os._exit = real_exit

        assert exit_calls == [], (
            f"watcher thread outlived stop_event+teardown and called "
            f"os._exit{tuple(exit_calls)}"
        )


class TestSyncWatcherMismatchObservability:
    """B2: the sync watcher must log the same warning as its async twin
    before suiciding on a server-pid mismatch."""

    def test_mismatch_exit_logs_warning_like_async_twin(self, monkeypatch, caplog):
        monkeypatch.setattr(os, "getppid", lambda: 1)

        class _ExitSignal(BaseException):
            pass

        def fake_exit(code=0):
            raise _ExitSignal

        monkeypatch.setattr(os, "_exit", fake_exit)

        with caplog.at_level(logging.WARNING):
            with pytest.raises(_ExitSignal):
                inference._start_parent_watchpoint_sync(
                    logging.getLogger("t"), server_pid=9999,
                )

        assert any("server died" in r.getMessage() for r in caplog.records), (
            "sync watcher must log 'server pid %s but parent is %s; server "
            "died during worker init' like the async twin"
        )
