"""Tests for bidi (bidirectional streaming) benchmark — 批次 2.

Phases: datatypes → bidi_metrics → bidi_session orchestrator →
run_bidi engine adapter → ws_bidi_target → CLI.
Design: streaming-benchmark-plan.md §4 + §4.8 修正案.
"""

from __future__ import annotations

import asyncio

import pytest

from lite_server.analyzer.benchmark import (
    BenchmarkEngine,
    BenchmarkResult,
    BidiSessionMetrics,
    BidiSessionRecord,
)


# ── Phase 1: Datatypes ──────────────────────────────────────────────────────

class TestBidiSessionRecord:
    def test_defaults(self):
        r = BidiSessionRecord()
        assert r.open_latency_ms is None
        assert r.chunk_roundtrips_ms == []
        assert r.consumer_chunks == 0
        assert r.producer_chunks == 0
        assert r.close_to_final_ms is None
        assert r.session_duration_ms is None
        assert r.total_bytes_sent == 0
        assert r.total_bytes_recv == 0


class TestBidiSessionMetrics:
    def _sample(self) -> BidiSessionMetrics:
        pct = {"mean": 1.0, "p50": 1.0, "p90": 2.0, "p95": 2.0,
               "p99": 2.0, "min": 0.5, "max": 2.0}
        return BidiSessionMetrics(
            transport="ws",
            pacing_mode="lock_step",
            sessions=10,
            failed_sessions=0,
            open_latency_ms=dict(pct),
            close_to_final_ms=dict(pct),
            session_duration_ms=dict(pct),
            chunks_per_session=dict(pct),
            chunk_roundtrip_ms=dict(pct),
            sessions_per_sec=2.5,
        )

    def test_to_dict_keys(self):
        d = self._sample().to_dict()
        assert d["transport"] == "ws"
        assert d["pacing_mode"] == "lock_step"
        assert d["sessions"] == 10
        assert d["failed_sessions"] == 0
        assert "open_latency_ms" in d
        assert "close_to_final_ms" in d
        assert "session_duration_ms" in d
        assert "chunks_per_session" in d
        assert "chunk_roundtrip_ms" in d
        assert d["sessions_per_sec"] == 2.5

    def test_to_dict_omits_roundtrip_when_none(self):
        m = self._sample()
        m.chunk_roundtrip_ms = None
        d = m.to_dict()
        assert "chunk_roundtrip_ms" not in d

    def test_result_to_dict_bidi_null_by_default(self):
        assert BenchmarkResult().to_dict()["bidi"] is None

    def test_result_to_dict_includes_bidi_section(self):
        result = BenchmarkResult()
        result.bidi_metrics = self._sample()
        d = result.to_dict()
        assert d["bidi"]["transport"] == "ws"
        assert d["stream"] is None  # stream key untouched


# ── Phase 2: compute_bidi_metrics ───────────────────────────────────────────

class TestComputeBidiMetrics:
    def _record(self, **kw) -> BidiSessionRecord:
        base = dict(
            open_latency_ms=50.0,
            chunk_roundtrips_ms=[10.0, 12.0],
            consumer_chunks=3,
            producer_chunks=2,
            close_to_final_ms=30.0,
            session_duration_ms=500.0,
            total_bytes_sent=100,
            total_bytes_recv=300,
        )
        base.update(kw)
        return BidiSessionRecord(**base)

    def test_basic_percentiles(self):
        from lite_server.analyzer.bidi_metrics import compute_bidi_metrics

        records = [self._record(), self._record(session_duration_ms=700.0)]
        m = compute_bidi_metrics(
            records, transport="ws", pacing_mode="lock_step",
            failed_sessions=1, window_secs=4.0,
        )
        assert m.sessions == 2
        assert m.failed_sessions == 1
        assert m.session_duration_ms["mean"] == pytest.approx(600.0)
        assert m.open_latency_ms["mean"] == pytest.approx(50.0)
        assert m.chunks_per_session["mean"] == pytest.approx(3.0)
        assert m.sessions_per_sec == pytest.approx(0.5)

    def test_roundtrip_pooled_across_sessions(self):
        from lite_server.analyzer.bidi_metrics import compute_bidi_metrics

        records = [
            self._record(chunk_roundtrips_ms=[10.0, 20.0]),
            self._record(chunk_roundtrips_ms=[30.0]),
        ]
        m = compute_bidi_metrics(
            records, transport="ws", pacing_mode="lock_step",
        )
        assert m.chunk_roundtrip_ms["mean"] == pytest.approx(20.0)
        assert m.chunk_roundtrip_ms["min"] == pytest.approx(10.0)
        assert m.chunk_roundtrip_ms["max"] == pytest.approx(30.0)

    def test_roundtrip_none_when_no_samples(self):
        """real_time pacing: no per-chunk pairing → no roundtrip section."""
        from lite_server.analyzer.bidi_metrics import compute_bidi_metrics

        records = [self._record(chunk_roundtrips_ms=[])]
        m = compute_bidi_metrics(
            records, transport="ws", pacing_mode="real_time",
        )
        assert m.chunk_roundtrip_ms is None
        assert "chunk_roundtrip_ms" not in m.to_dict()

    def test_none_open_latency_excluded(self):
        """on_open returned None → no ready frame → open_latency excluded."""
        from lite_server.analyzer.bidi_metrics import compute_bidi_metrics

        records = [
            self._record(open_latency_ms=None),
            self._record(open_latency_ms=100.0),
        ]
        m = compute_bidi_metrics(
            records, transport="ws", pacing_mode="lock_step",
        )
        assert m.open_latency_ms["mean"] == pytest.approx(100.0)

    def test_empty_records_zero_structure(self):
        from lite_server.analyzer.bidi_metrics import compute_bidi_metrics

        m = compute_bidi_metrics(
            [], transport="ws", pacing_mode="lock_step",
        )
        assert m.sessions == 0
        assert m.session_duration_ms["mean"] == 0.0
        assert m.chunk_roundtrip_ms is None
        assert m.sessions_per_sec is None

    def test_sessions_per_sec_none_without_window(self):
        from lite_server.analyzer.bidi_metrics import compute_bidi_metrics

        m = compute_bidi_metrics(
            [self._record()], transport="ws", pacing_mode="lock_step",
            window_secs=None,
        )
        assert m.sessions_per_sec is None


# ── Phase 3: bidi_session orchestrator ──────────────────────────────────────

class _FakeIO:
    """Scripted BidiIO: each send enqueues its queued response frames."""

    def __init__(self, *, open_responses=None, chunk_responses=None,
                 close_responses=None, delay=0.001):
        self.open_responses = list(open_responses or [])
        self.chunk_responses = [list(r) for r in (chunk_responses or [])]
        self.close_responses = list(close_responses or [])
        self.delay = delay
        self.sent = []  # (kind, payload, ts_ns)
        self._q = asyncio.Queue()
        self._chunk_idx = 0

    async def _respond(self, frames):
        await asyncio.sleep(self.delay)
        for f in frames:
            self._q.put_nowait(f)

    async def send_open(self, data: bytes):
        import time
        self.sent.append(("open", data, time.perf_counter_ns()))
        await self._respond(self.open_responses)

    async def send_chunk(self, data: bytes):
        import time
        self.sent.append(("chunk", data, time.perf_counter_ns()))
        idx = min(self._chunk_idx, len(self.chunk_responses) - 1)
        self._chunk_idx += 1
        if self.chunk_responses:
            await self._respond(self.chunk_responses[idx])

    async def send_close(self):
        import time
        self.sent.append(("close", None, time.perf_counter_ns()))
        await self._respond(self.close_responses)

    async def recv(self):
        return await self._q.get()


class TestBidiSession:
    @staticmethod
    def _frames():
        from lite_server.analyzer.bidi_session import Data, Done, Error
        return Data, Done, Error

    @pytest.mark.asyncio
    async def test_lock_step_happy_path(self):
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session

        Data, Done, _ = self._frames()
        io = _FakeIO(
            open_responses=[Data(b'{"status": "ready"}')],
            chunk_responses=[[Data(b'"e1"')], [Data(b'"e2"')]],
            close_responses=[Done()],
        )
        rec = await run_bidi_session(
            io, [{"cfg": 1}, "c1", "c2"],
            pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
        )
        assert rec.open_latency_ms is not None and rec.open_latency_ms > 0
        assert rec.producer_chunks == 2
        assert rec.consumer_chunks == 3  # ready + 2 echoes
        assert len(rec.chunk_roundtrips_ms) == 2
        assert all(t > 0 for t in rec.chunk_roundtrips_ms)
        assert rec.close_to_final_ms is not None and rec.close_to_final_ms > 0
        assert rec.session_duration_ms is not None
        assert rec.session_duration_ms > rec.open_latency_ms
        assert rec.total_bytes_sent > 0
        assert rec.total_bytes_recv > 0
        # open frame serialized as JSON bytes of the first script element
        import json
        assert json.loads(io.sent[0][1].decode()) == {"cfg": 1}
        assert io.sent[-1][0] == "close"

    @pytest.mark.asyncio
    async def test_lock_step_requires_open_response(self):
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session
        from lite_server.analyzer.benchmark import RequestStreamError

        io = _FakeIO(open_responses=[])  # on_open returned None
        with pytest.raises(RequestStreamError, match="on_open"):
            await run_bidi_session(
                io, [{"cfg": 1}, "c1"],
                pacing=Pacing(mode="lock_step"), idle_timeout=0.05,
            )

    @pytest.mark.asyncio
    async def test_lock_step_chunk_without_response_fails(self):
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session
        from lite_server.analyzer.benchmark import RequestStreamError

        Data, _, _ = self._frames()
        io = _FakeIO(
            open_responses=[Data(b"ready")],
            chunk_responses=[[Data(b'"e1"')], []],  # 2nd chunk: no response
        )
        with pytest.raises(RequestStreamError, match="response per chunk"):
            await run_bidi_session(
                io, ["o", "c1", "c2"],
                pacing=Pacing(mode="lock_step"), idle_timeout=0.05,
            )

    @pytest.mark.asyncio
    async def test_error_frame_aborts_session(self):
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session
        from lite_server.analyzer.benchmark import RequestStreamError

        Data, _, Error = self._frames()
        io = _FakeIO(
            open_responses=[Data(b"ready")],
            chunk_responses=[[Error("model exploded")]],
        )
        with pytest.raises(RequestStreamError, match="model exploded"):
            await run_bidi_session(
                io, ["o", "c1"],
                pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
            )

    @pytest.mark.asyncio
    async def test_real_time_pacing_no_roundtrips(self):
        import time
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session

        Data, Done, _ = self._frames()
        io = _FakeIO(
            open_responses=[Data(b"ready")],
            chunk_responses=[[Data(b'"e1"')], [Data(b'"e2"')], [Data(b'"e3"')]],
            close_responses=[Done()],
        )
        rec = await run_bidi_session(
            io, ["o", "c1", "c2", "c3"],
            pacing=Pacing(mode="real_time", pace_secs=0.02), idle_timeout=1.0,
        )
        assert rec.producer_chunks == 3
        assert rec.chunk_roundtrips_ms == []  # no pairing in paced mode
        assert rec.consumer_chunks == 4
        assert rec.close_to_final_ms is not None
        # pace respected: chunk send timestamps spaced by ~= pace_secs
        chunk_ts = [ts for kind, _, ts in io.sent if kind == "chunk"]
        gaps = [(b - a) for a, b in zip(chunk_ts, chunk_ts[1:])]
        assert all(g >= 0.015 for g in gaps)

    @pytest.mark.asyncio
    async def test_early_done_stops_production(self):
        """Model closes the session mid-script: producer stops, no close sent."""
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session

        Data, Done, _ = self._frames()
        io = _FakeIO(
            open_responses=[Data(b"ready")],
            chunk_responses=[[Done()], [Data(b'"never"')]],
            close_responses=[],
        )
        rec = await run_bidi_session(
            io, ["o", "c1", "c2"],
            pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
        )
        assert rec.producer_chunks == 1  # stopped after first chunk
        assert rec.close_to_final_ms is None  # close never sent
        assert rec.session_duration_ms is not None
        assert not any(kind == "close" for kind, _, _ in io.sent)

    @pytest.mark.asyncio
    async def test_script_with_only_open(self):
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session

        Data, Done, _ = self._frames()
        io = _FakeIO(open_responses=[Data(b"ready")], close_responses=[Done()])
        rec = await run_bidi_session(
            io, ["open-only"],
            pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
        )
        assert rec.producer_chunks == 0
        assert rec.close_to_final_ms is not None

    @pytest.mark.asyncio
    async def test_empty_script_raises_value_error(self):
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session

        with pytest.raises(ValueError, match="script"):
            await run_bidi_session(io := _FakeIO(), [], pacing=Pacing())

    @pytest.mark.asyncio
    async def test_transport_exception_becomes_stream_error(self):
        from lite_server.analyzer.bidi_session import Pacing, run_bidi_session
        from lite_server.analyzer.benchmark import RequestStreamError

        Data, _, _ = self._frames()
        io = _FakeIO(open_responses=[Data(b"ready")],
                     chunk_responses=[[Data(b'"e1"')]])

        original_recv = io.recv
        calls = [0]

        async def failing_recv():
            calls[0] += 1
            if calls[0] > 2:  # after open-resp + chunk-resp reads
                raise RuntimeError("socket gone")
            return await original_recv()

        io.recv = failing_recv
        # close_responses empty: close wait hits the transport failure
        with pytest.raises(RequestStreamError, match="socket gone"):
            await run_bidi_session(
                io, ["o", "c1"],
                pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
            )


# ── Phase 4: engine run_bidi adapter ────────────────────────────────────────

class TestRunBidi:
    @staticmethod
    def _record(duration_ms=100.0):
        return BidiSessionRecord(
            open_latency_ms=10.0,
            chunk_roundtrips_ms=[5.0, 6.0],
            consumer_chunks=3,
            producer_chunks=2,
            close_to_final_ms=8.0,
            session_duration_ms=duration_ms,
            total_bytes_sent=50,
            total_bytes_recv=120,
        )

    @pytest.mark.asyncio
    async def test_happy_path_attaches_bidi_metrics(self):
        engine = BenchmarkEngine()

        async def session_runner(script):
            await asyncio.sleep(0.001)
            return self._record()

        result = await engine.run_bidi(
            session_runner=session_runner,
            payload=["open", "c1", "c2"],
            concurrency=1,
            total_requests=4,
            transport="ws",
            pacing_mode="lock_step",
            min_sessions=1,
        )
        assert result.successful == 4
        bm = result.bidi_metrics
        assert bm is not None
        assert bm.transport == "ws"
        assert bm.pacing_mode == "lock_step"
        assert bm.sessions == 4
        assert bm.failed_sessions == 0
        assert bm.chunk_roundtrip_ms is not None
        assert bm.sessions_per_sec is not None
        assert result.stream_metrics is None  # stream section untouched

    @pytest.mark.asyncio
    async def test_failed_sessions_counted_not_recorded(self):
        from lite_server.analyzer.benchmark import RequestStreamError

        engine = BenchmarkEngine()
        calls = [0]

        async def session_runner(script):
            calls[0] += 1
            if calls[0] % 2 == 0:
                raise RequestStreamError("session boom")
            return self._record()

        result = await engine.run_bidi(
            session_runner=session_runner,
            payload=["open"],
            concurrency=1,
            total_requests=4,
            min_sessions=1,
        )
        assert result.successful == 2
        assert result.failed == 2
        assert "stream" in result.error_kinds
        assert result.bidi_metrics.sessions == 2
        assert result.bidi_metrics.failed_sessions == 2

    @pytest.mark.asyncio
    async def test_warmup_sessions_not_recorded(self):
        engine = BenchmarkEngine()

        async def session_runner(script):
            return self._record()

        result = await engine.run_bidi(
            session_runner=session_runner,
            payload=["open"],
            concurrency=1,
            total_requests=3,
            warmup_requests=2,
            min_sessions=1,
        )
        assert result.warmup_requests == 2
        assert result.bidi_metrics.sessions == 3  # warmup excluded

    @pytest.mark.asyncio
    async def test_min_sessions_overrides_sample_warning(self):
        """min_samples=30 replaces max(300, 10×concurrency) for bidi (§4.5)."""
        engine = BenchmarkEngine()

        async def session_runner(script):
            return self._record()

        result = await engine.run_bidi(
            session_runner=session_runner,
            payload=["open"],
            concurrency=1,
            total_requests=5,
            min_sessions=30,
        )
        sample_warnings = [w for w in result.warnings if "Sample size" in w]
        assert len(sample_warnings) == 1
        assert "< 30" in sample_warnings[0]
        assert "max(300" not in sample_warnings[0]

    @pytest.mark.asyncio
    async def test_no_warning_when_sessions_meet_min(self):
        engine = BenchmarkEngine()

        async def session_runner(script):
            return self._record()

        result = await engine.run_bidi(
            session_runner=session_runner,
            payload=["open"],
            concurrency=1,
            total_requests=5,
            min_sessions=5,
        )
        assert not any("Sample size" in w for w in result.warnings)

    @pytest.mark.asyncio
    async def test_session_duration_is_e2e_latency(self):
        """Adapter latency = session e2e (§4.8 D1): result percentiles ≈ durations."""
        engine = BenchmarkEngine()

        async def session_runner(script):
            await asyncio.sleep(0.01)
            return self._record(duration_ms=10.0)

        result = await engine.run_bidi(
            session_runner=session_runner,
            payload=["open"],
            concurrency=1,
            total_requests=3,
            min_sessions=1,
        )
        assert result.p50 >= 9.0  # ~10ms sleep per session


# ── Phase 5: ws_bidi_target ─────────────────────────────────────────────────

class TestWsBidiTarget:
    """WS bidi transport: frame mapping onto the bidi IO contract."""

    @staticmethod
    def _fake_ws(messages):
        """Fake websocket: recv pops messages (bytes=Binary, str=Text)."""
        class FakeWS:
            def __init__(self):
                self.sent = []

            async def send(self, data):
                self.sent.append(data)

            async def recv(self):
                if messages:
                    return messages.pop(0)
                raise RuntimeError("closed without done")

        class _Ctx:
            def __init__(self, ws):
                self._ws = ws

            async def __aenter__(self):
                return self._ws

            async def __aexit__(self, *args):
                return False

        ws = FakeWS()
        calls = []

        def connect(url):
            calls.append(url)
            return _Ctx(ws)

        return connect, ws, calls

    @pytest.mark.asyncio
    async def test_frame_mapping_and_session_record(self):
        import json
        from lite_server.analyzer.bidi_session import Pacing
        from lite_server.analyzer.ws_bidi_target import ws_bidi_session

        connect, ws, calls = self._fake_ws([
            b'{"status": "ready"}',       # on_open response (Binary)
            b'"echo1"',                   # chunk 1 response
            b'"echo2"',                   # chunk 2 response
            '{"done": true}',             # terminal
        ])
        from lite_server.analyzer.bidi_session import Pacing as P
        session = ws_bidi_session(
            connect, "ws://x/v2/models/m/stream",
            pacing=P(mode="lock_step"), idle_timeout=1.0,
        )
        rec = await session([{"cfg": 1}, "c1", "c2"])

        assert calls == ["ws://x/v2/models/m/stream"]
        # open = Text frame (str), JSON of script[0]
        assert isinstance(ws.sent[0], str)
        assert json.loads(ws.sent[0]) == {"cfg": 1}
        # chunks = Binary frames (bytes)
        assert ws.sent[1] == b'"c1"'
        assert ws.sent[2] == b'"c2"'
        # close = Text {"type":"close"}
        assert json.loads(ws.sent[3]) == {"type": "close"}
        # record
        assert rec.producer_chunks == 2
        assert rec.consumer_chunks == 3
        assert len(rec.chunk_roundtrips_ms) == 2
        assert rec.close_to_final_ms is not None

    @pytest.mark.asyncio
    async def test_error_frame_fails_session(self):
        from lite_server.analyzer.bidi_session import Pacing
        from lite_server.analyzer.benchmark import RequestStreamError
        from lite_server.analyzer.ws_bidi_target import ws_bidi_session

        connect, _, _ = self._fake_ws([
            b"ready",
            '{"error": "worker died"}',
        ])
        session = ws_bidi_session(
            connect, "ws://x/stream", pacing=Pacing(mode="lock_step"),
            idle_timeout=1.0,
        )
        with pytest.raises(RequestStreamError, match="worker died"):
            await session(["open", "c1"])

    @pytest.mark.asyncio
    async def test_unknown_text_frame_tolerated(self):
        from lite_server.analyzer.bidi_session import Pacing
        from lite_server.analyzer.ws_bidi_target import ws_bidi_session

        connect, _, _ = self._fake_ws([
            b"ready",
            "garbage",              # unknown Text — tolerated, skipped
            b'"echo1"',
            '{"done": true}',
        ])
        session = ws_bidi_session(
            connect, "ws://x/stream", pacing=Pacing(mode="lock_step"),
            idle_timeout=1.0,
        )
        rec = await session(["open", "c1"])
        assert rec.consumer_chunks == 2  # ready + echo1 (garbage not counted)

    @pytest.mark.asyncio
    async def test_close_without_done_fails_session(self):
        from lite_server.analyzer.bidi_session import Pacing
        from lite_server.analyzer.benchmark import RequestStreamError
        from lite_server.analyzer.ws_bidi_target import ws_bidi_session

        connect, _, _ = self._fake_ws([b"ready"])  # then transport error
        session = ws_bidi_session(
            connect, "ws://x/stream", pacing=Pacing(mode="lock_step"),
            idle_timeout=1.0,
        )
        with pytest.raises(RequestStreamError):
            await session(["open", "c1"])


# ── Phase 6: CLI ─────────────────────────────────────────────────────────────

class TestBidiCLI:
    @staticmethod
    def _bidi_args(**overrides):
        base = {
            "url": "http://127.0.0.1:8000",
            "model": "test_model",
            "version": None,
            "concurrency": "1",
            "duration": None,
            "requests": 3,
            "warmup_requests": 0,
            "grace_period": 1.0,
            "payload": '["open", "c1"]',
            "payload_file": None,
            "payload_random": None,
            "export": None,
            "max_error_rate": None,
            "max_p99": None,
            "rate": None,
            "latency_threshold": None,
            "stream": False,
            "model_type": "llm",
            "stream_read_timeout": 300.0,
            "max_ttft_ms": None,
            "max_rtf": None,
            "endpoint": "events",
            "transport": None,
            "bidi": True,
            "pace": None,
            "rt_factor": None,
            "min_sessions": 1,
        }
        base.update(overrides)
        return type("Args", (), base)()

    @staticmethod
    def _fake_ws_tree(messages):
        """Fake websockets package tree with asyncio.client.connect."""
        import sys
        import types

        class FakeWS:
            def __init__(self):
                self.sent = []

            async def send(self, data):
                self.sent.append(data)

            async def recv(self):
                if messages:
                    return messages.pop(0)
                raise RuntimeError("closed without done")

        class _Ctx:
            def __init__(self, ws):
                self._ws = ws

            async def __aenter__(self):
                return self._ws

            async def __aexit__(self, *args):
                return False

        ws = FakeWS()
        calls = []

        def fake_connect(url):
            calls.append(url)
            return _Ctx(ws)

        client_mod = types.ModuleType("websockets.asyncio.client")
        client_mod.connect = fake_connect
        asyncio_mod = types.ModuleType("websockets.asyncio")
        asyncio_mod.client = client_mod
        pkg = types.ModuleType("websockets")
        pkg.asyncio = asyncio_mod

        modules = {
            "websockets": pkg,
            "websockets.asyncio": asyncio_mod,
            "websockets.asyncio.client": client_mod,
        }
        return modules, ws, calls

    def _patch(self, monkeypatch, modules):
        import sys
        for name, mod in modules.items():
            monkeypatch.setitem(sys.modules, name, mod)

    def test_bidi_lock_step_runs_and_renders(self, monkeypatch, capsys):
        """--bidi → WS /stream bidi, lock-step default, bidi section printed."""
        from lite_server import cli

        modules, ws, calls = self._fake_ws_tree([
            b"ready", b'"e1"', '{"done": true}',
        ] * 5)
        self._patch(monkeypatch, modules)

        rc = cli._cmd_benchmark(self._bidi_args(requests=3))
        assert rc == 0
        assert calls[0].endswith("/v2/models/test_model/stream")
        assert calls[0].startswith("ws://")
        out = capsys.readouterr().out
        assert "Bidi Session Metrics" in out
        assert "lock_step" in out

    def test_bidi_default_transport_is_ws(self, monkeypatch, capsys):
        """--bidi without --transport resolves to ws (not sse)."""
        from lite_server import cli

        modules, _, calls = self._fake_ws_tree([b"ready", '{"done": true}'])
        self._patch(monkeypatch, modules)

        rc = cli._cmd_benchmark(self._bidi_args(requests=1,
                                                payload='["open"]'))
        assert rc == 0
        assert calls[0].startswith("ws://")

    def test_bidi_with_stream_mutex_exits_2(self):
        """--bidi + --stream → argparse mutual exclusion exit 2."""
        from lite_server import cli

        with pytest.raises(SystemExit) as exc_info:
            cli.main(["benchmark", "--model", "m", "--requests", "1",
                      "--stream", "--bidi"])
        assert exc_info.value.code == 2

    def test_bidi_transport_sse_exits_2(self):
        """--bidi --transport sse → exit 2 (SSE cannot do bidi)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._bidi_args(transport="sse"))
        assert rc == 2

    def test_bidi_transport_grpc_exits_2(self):
        """--bidi --transport grpc → exit 2 (批次 3)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._bidi_args(transport="grpc"))
        assert rc == 2

    def test_pace_without_bidi_exits_2(self):
        """--pace without --bidi → exit 2 (fail-closed)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._bidi_args(bidi=False, pace=0.1))
        assert rc == 2

    def test_rt_factor_requires_pace(self):
        """--rt-factor without --pace → exit 2."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._bidi_args(rt_factor=2.0))
        assert rc == 2

    def test_bidi_non_list_payload_exits_2(self):
        """--bidi payload must be a JSON array."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._bidi_args(payload='{"input": 1}'))
        assert rc == 2

    def test_bidi_payload_random_exits_2(self):
        """--bidi + --payload-random → exit 2 (dict templates incompatible)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._bidi_args(
            payload=None, payload_random='{"id": "x"}',
        ))
        assert rc == 2

    def test_bidi_export_contains_bidi_section(self, monkeypatch, tmp_path):
        import json
        from lite_server import cli

        modules, _, _ = self._fake_ws_tree([
            b"ready", b'"e1"', '{"done": true}',
        ] * 5)
        self._patch(monkeypatch, modules)
        export_path = tmp_path / "r.json"

        rc = cli._cmd_benchmark(self._bidi_args(
            requests=3, export=str(export_path),
            pace=0.01, rt_factor=2.0,
        ))
        assert rc == 0
        data = json.loads(export_path.read_text())
        assert data["bidi"]["transport"] == "ws"
        assert data["bidi"]["pacing_mode"] == "speedup"
        assert data["config"]["bidi"] is True
        assert data["config"]["pacing_mode"] == "speedup"
        assert data["config"]["min_sessions"] == 1

    def test_bidi_sweep_export_per_level_bidi_section(self, monkeypatch, tmp_path):
        """--bidi + sweep export: each level carries the bidi section."""
        import json
        from lite_server import cli

        modules, _, _ = self._fake_ws_tree([
            b"ready", b'"e1"', '{"done": true}',
        ] * 20)
        self._patch(monkeypatch, modules)
        export_path = tmp_path / "sweep.json"

        rc = cli._cmd_benchmark(self._bidi_args(
            requests=2, concurrency="1:2:1", export=str(export_path),
        ))
        assert rc == 0
        data = json.loads(export_path.read_text())
        assert data["config"]["bidi"] is True
        assert data["config"]["pacing_mode"] == "lock_step"
        assert data["config"]["url"].startswith("ws://")
        assert data["config"]["url"].endswith("/stream")
        assert len(data["levels"]) == 2
        for level in data["levels"]:
            assert level["bidi"]["transport"] == "ws"
            assert level["bidi"]["sessions"] == 2
