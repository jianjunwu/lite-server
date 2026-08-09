#!/usr/bin/env python3
"""One-click runner for every lite-server example.

Sequentially starts each example's server, waits for it to become ready, runs
the README-based behavior check, then shuts it down cleanly.  Prints a
per-example PASS/FAIL report and exits ``0`` only if every example passes.

Examples share a single HTTP port (8000, hard-coded in each ``server.yaml``),
so they run one at a time — never in parallel.

Requirements (in the Python that runs this script):
  * ``lite-server``        — the server itself (``python -m lite_server``)
  * ``grpcio``             — example 13's bidirectional gRPC handshake
  * ``tritonclient``       — example 25's Triton Binary client channel
  * ``numpy``              — example 25's tensor construction

Usage:
    python run_all.py                 # run every example
    python run_all.py 01_basic 03_streaming   # run a subset
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8000"
HOST, PORT = "localhost", 8000
ROOT = os.path.dirname(os.path.abspath(__file__))

USAGE = """Usage: python run_all.py [OPTIONS] [example_dir ...]

Options:
  -v, --verbose      Print detailed intermediate results for each check.
  -t, --timeout N    Readiness wait timeout in seconds (default: 45).
  -h, --help         Show this help message and exit.

Examples:
  python run_all.py                          # run every example
  python run_all.py 01_basic 03_streaming    # run a subset
  python run_all.py -v -t 60 13_bidi_streaming  # verbose, 60s timeout
"""


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def http_json(method, path, body=None, headers=None, timeout=10,
              base=BASE, context=None):
    """Return (status, parsed_body_or_text).  status is None on connection error."""
    url = base + path
    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as r:
            txt = r.read().decode()
            try:
                return r.status, json.loads(txt)
            except json.JSONDecodeError:
                return r.status, txt
    except urllib.error.HTTPError as e:
        txt = e.read().decode()
        try:
            return e.code, json.loads(txt)
        except json.JSONDecodeError:
            return e.code, txt
    except Exception as e:  # noqa: BLE001 — report any transport failure
        return None, f"{type(e).__name__}: {e}"


def wait_ready(model, timeout=45, base=BASE, context=None, headers=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st, body = http_json("GET", f"/v2/models/{model}/ready", timeout=3,
                             base=base, context=context, headers=headers)
        if st == 200 and isinstance(body, dict) and body.get("ready"):
            return True
        time.sleep(0.5)
    return False


def read_sse(path, body, want_lines=4, deadline_s=8.0):
    """POST ``body`` to ``path`` and count ``data:`` lines until ``want_lines``
    or ``deadline_s``.  The SSE connection stays open, so we must bound the
    read ourselves.  Returns the count."""
    conn = http.client.HTTPConnection(HOST, PORT, timeout=deadline_s)
    count = 0
    try:
        conn.request("POST", path, body=json.dumps(body),
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        end = time.time() + deadline_s
        while count < want_lines and time.time() < end:
            try:
                line = resp.readline()
            except Exception:  # socket timeout / closed
                break
            if not line:
                break
            if line.startswith(b"data:"):
                count += 1
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return count


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def run_setup(example):
    """Run example/setup.sh (if present) before the server starts. Used by
    examples that need generated artifacts (e.g. TLS certificates)."""
    setup = os.path.join(ROOT, example, "setup.sh")
    if os.path.exists(setup):
        subprocess.run(["bash", setup], cwd=os.path.join(ROOT, example), check=True)


def start_server(example, env=None):
    # Check for port conflicts before launching.
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    in_use = s.connect_ex(("localhost", PORT)) == 0
    s.close()
    if in_use:
        raise RuntimeError(
            f"Port {PORT} is already in use — a previous server may still be "
            f"running. Kill it first:  pkill -f 'lite_server serve'"
        )
    log_path = os.path.join("/tmp", f"run_all_{example}.log")
    log = open(log_path, "wb")
    # Merge extra env vars into the current environment.
    proc_env = os.environ.copy()
    if env:
        proc_env.update(env)
    proc = subprocess.Popen(
        [sys.executable, "-m", "lite_server", "serve", "--config", "server.yaml"],
        cwd=os.path.join(ROOT, example),
        stdout=log, stderr=subprocess.STDOUT,
        env=proc_env,
        start_new_session=True,  # own process group → clean group kill
    )
    return proc, log_path, log


def stop_server(proc):
    """SIGTERM the server's process group, then SIGKILL if needed, and sweep
    any straggler workers so the next example's port is free."""
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            return
        for _ in range(20):
            if proc.poll() is not None:
                break
            time.sleep(0.3)
        if proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
    subprocess.run(["pkill", "-f", "lite_server serve"], capture_output=True)
    time.sleep(1.0)


# ---------------------------------------------------------------------------
# Per-example checks — each mirrors its README's expected output.
# Returns (ok: bool, detail: str).
# ---------------------------------------------------------------------------

def check_01():
    st, r = http_json("POST", "/v2/models/echo/infer", {"input": 21})
    return st == 200 and r.get("output") == 42, f"HTTP {st} -> {r}"


def check_02():
    results = []

    def one():
        st, r = http_json("POST", "/v2/models/batched/infer", {"input": 1})
        results.append((st, r))

    threads = [threading.Thread(target=one) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Under concurrent load the server batches; predict() then reports the
    # real batch size, so at least one response should see batch_size >= 2.
    batch_sizes = [r.get("batch_size") for st, r in results
                   if st == 200 and isinstance(r, dict)]
    st2, r2 = http_json("POST", "/v2/models/custom_batch/infer", {"input": 3, "weight": 0.5})
    ok = (all(st == 200 for st, _ in results)
          and any(b and b >= 2 for b in batch_sizes)
          and st2 == 200)
    return ok, f"batched sizes={sorted(set(batch_sizes))} | custom_batch HTTP {st2} -> {r2}"


def check_03():
    """Composite check for all 5 streaming models (SSE, WS, decoupled, bidi, errors).

    All models run in one server. Each sub-check returns (ok, detail); the
    composite passes only when every sub-check passes.
    """
    import websockets
    import asyncio

    results = {}

    # --- helpers for streaming checks -----------------------------------------

    def sse_collect(path, body, deadline_s=8.0):
        """POST body to path, collect all SSE events until the stream ends."""
        conn = http.client.HTTPConnection(HOST, PORT, timeout=deadline_s)
        events = []
        try:
            conn.request("POST", path, body=json.dumps(body),
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            end = time.time() + deadline_s
            while time.time() < end:
                try:
                    line = resp.readline()
                except Exception:
                    break
                if not line:
                    break
                s = line.decode("utf-8", errors="replace").strip()
                if s.startswith("data: "):
                    events.append(s[6:])
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return events

    async def ws_collect(path, first_msg, send_frames=None, deadline_s=8.0):
        """Connect to ws://localhost:8000/{path}, send first_msg + optional
        send_frames, collect all text/binary messages. Returns list of parsed
        text messages (dict) or binary payloads (bytes, prefixed 'bin:')."""
        uri = f"ws://{HOST}:{PORT}{path}"
        msgs = []
        try:
            async with websockets.connect(uri, close_timeout=3, proxy=None) as ws:
                # Send first frame (Text or Binary based on type)
                if isinstance(first_msg, bytes):
                    await ws.send(first_msg)
                else:
                    await ws.send(json.dumps(first_msg))
                # Send additional frames
                if send_frames:
                    for f in send_frames:
                        if isinstance(f, bytes):
                            await ws.send(f)
                        else:
                            await ws.send(json.dumps(f))
                # Collect responses until close or deadline
                end = time.time() + deadline_s
                while time.time() < end:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    except asyncio.TimeoutError:
                        break
                    if isinstance(msg, bytes):
                        # Try to decode as JSON text fallback
                        try:
                            msgs.append(json.loads(msg.decode()))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            msgs.append(f"bin:{msg!r}")
                    else:
                        try:
                            msgs.append(json.loads(msg))
                        except json.JSONDecodeError:
                            msgs.append(msg)
        except Exception:
            pass
        return msgs

    async def ws_bidi_collect(path, first_msg, extra_frames=None, deadline_s=8.0):
        """Like ws_collect but sends extra_frames concurrently with recv.
        Extra frames (Binary or Text) are sent with 100ms delays to avoid
        backpressure — the server needs time to drain chunks between frames."""
        uri = f"ws://{HOST}:{PORT}{path}"
        msgs = []
        try:
            async with websockets.connect(uri, close_timeout=3, proxy=None) as ws:
                # Send first frame
                if isinstance(first_msg, bytes):
                    await ws.send(first_msg)
                else:
                    await ws.send(json.dumps(first_msg))
                # Start recv in background
                async def _recv():
                    end = time.time() + deadline_s
                    while time.time() < end:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                        except asyncio.TimeoutError:
                            break
                        if isinstance(msg, bytes):
                            try:
                                msgs.append(json.loads(msg.decode()))
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                msgs.append(f"bin:{msg!r}")
                        else:
                            try:
                                msgs.append(json.loads(msg))
                            except json.JSONDecodeError:
                                msgs.append(msg)
                recv_task = asyncio.create_task(_recv())
                # Send extra frames with small delays
                if extra_frames:
                    for f in extra_frames:
                        await asyncio.sleep(0.1)
                        if isinstance(f, bytes):
                            await ws.send(f)
                        else:
                            await ws.send(json.dumps(f))
                # Wait for recv to finish
                await recv_task
        except Exception:
            pass
        return msgs

    # --- 1) sse_tokens: SSE events, 10 tokens + [DONE] ------------------------

    events = sse_collect("/v2/models/sse_tokens/events",
                         {"prompt": "a b c d e f g h i j", "max_tokens": 10})
    done_count = sum(1 for e in events if e == "[DONE]")
    tokens = [e for e in events if e != "[DONE]"]
    ok1 = len(tokens) >= 10 and done_count == 1
    results["sse_tokens"] = (ok1, f"tokens={len(tokens)} done={done_count}")

    # --- 2) ws_echo: WS coupled stream, first frame → chunks → Done ---------

    async def _ws_echo():
        # Simple case: send first frame, read all responses (no C→S frames needed).
        return await ws_collect("/v2/models/ws_echo/stream", {"count": 3})
    ws_msgs = asyncio.run(_ws_echo())
    has_chunks = any(isinstance(m, dict) and m.get("chunk") is not None for m in ws_msgs)
    has_done = any(isinstance(m, dict) and m.get("done") is True for m in ws_msgs)
    ok2 = has_chunks and has_done
    results["ws_echo"] = (ok2, f"chunks={has_chunks} done={has_done} msgs={len(ws_msgs)}")

    # --- 3) decoupled_push (SSE): push 5 chunks + [DONE] ---------------------

    dc_events = sse_collect("/v2/models/decoupled_push/decoupled",
                            {"message": "hello push", "chunks": 5})
    dc_done = sum(1 for e in dc_events if e == "[DONE]")
    dc_chunks = [e for e in dc_events if e != "[DONE]"]
    ok3 = len(dc_chunks) >= 5 and dc_done == 1
    results["decoupled_sse"] = (ok3, f"chunks={len(dc_chunks)} done={dc_done}")

    # --- 4) decoupled_push (WS): cancel frame cancels worker ------------------

    async def _ws_decoupled_cancel():
        # Concurrent send+recv: start recv task, then send first+chunks+cancel.
        return await ws_bidi_collect(
            "/v2/models/decoupled_push/decoupled-stream",
            first_msg={"message": "cancel test", "chunks": 10},
            extra_frames=[{"type": "cancel"}],
        )
    dc_ws_msgs = asyncio.run(_ws_decoupled_cancel())
    chunk_msgs = [m for m in dc_ws_msgs if isinstance(m, dict) and "chunk_index" in m]
    ok4 = len(chunk_msgs) >= 1  # at least some chunks arrived before cancel
    results["decoupled_ws"] = (ok4, f"chunks_before_cancel={len(chunk_msgs)}")

    # --- 5) bidi_session: on_open → on_chunk×2 → on_close --------------------

    async def _bidi_session():
        return await ws_bidi_collect(
            "/v2/models/bidi_session/stream",
            first_msg={"session_id": "test-123"},
            extra_frames=[
                bytes(json.dumps({"text": "chunk one"}).encode()),
                bytes(json.dumps({"text": "chunk two"}).encode()),
                {"type": "close"},
            ],
        )
    bidi_msgs = asyncio.run(_bidi_session())
    events_by_type = {}
    for m in bidi_msgs:
        if isinstance(m, dict) and "event" in m:
            events_by_type[m["event"]] = m
    has_open = "open" in events_by_type
    has_chunk = "chunk" in events_by_type
    has_close = "close" in events_by_type
    close_total = events_by_type.get("close", {}).get("total_chunks", 0)
    ok5 = has_open and has_chunk and has_close and close_total == 2
    results["bidi_session"] = (
        ok5,
        f"open={has_open} chunk={has_chunk} close={has_close} total={close_total}",
    )

    # --- 6) stream_errors: normal + 3 error modes -----------------------------

    # normal mode
    normal_events = sse_collect("/v2/models/stream_errors/events",
                                {"mode": "normal", "input": "hello"})
    normal_has_done = any(e == "[DONE]" for e in normal_events)
    ok6a = normal_has_done
    if not ok6a:
        results["stream_errors"] = (False, f"normal mode failed: {normal_events}")
        return False, _format_results(results)

    # error modes: each should produce an error frame (not [DONE])
    error_results = {}
    for mode, expect_status in [("bad_request", 400), ("not_found", 404),
                                 ("server_error", 500)]:
        err_events = sse_collect("/v2/models/stream_errors/events",
                                 {"mode": mode, "input": "test"})
        has_error = any("error" in e for e in err_events)
        has_done_err = any(e == "[DONE]" for e in err_events)
        error_results[mode] = has_error and not has_done_err
    ok6 = ok6a and all(error_results.values())
    results["stream_errors"] = (ok6, f"normal={ok6a} errors={error_results}")

    all_ok = all(v[0] for v in results.values())
    return all_ok, _format_results(results)


def _format_results(results):
    """Format sub-check results: 'OK/FAIL key: detail'."""
    parts = []
    for k, (ok, detail) in results.items():
        parts.append(f"{'✓' if ok else '✗'} {k}: {detail}")
    return " | ".join(parts)


def check_04():
    st, r = http_json("POST", "/v2/models/multi_version/infer", {"input": 10})
    ok1 = st == 200 and r.get("output") == 20          # v2 (default)
    http_json("POST", "/v2/models/multi_version/versions/v1/activate")
    time.sleep(1)
    st2, r2 = http_json("POST", "/v2/models/multi_version/infer", {"input": 10})
    ok2 = st2 == 200 and r2.get("output") == 11         # v1 after activate
    return ok1 and ok2, f"default {r} | after v1 activate {r2}"


def check_05():
    st, r = http_json("POST", "/v2/models/pipeline/infer", {"input": "hello"})
    return st == 200 and r.get("output") == "preprocessed(hello) -> done", f"HTTP {st} -> {r}"


def check_06():
    """Custom routes: GET /status, GET /pets/{id} (hit + 404), POST /pets, infer."""
    # 1) GET /status — basic route with ctx.meta
    st, r = http_json("GET", "/v2/models/pets/status")
    if not (st == 200 and isinstance(r, dict) and r.get("model_loaded") is True):
        return False, f"/status: HTTP {st} -> {r}"
    # 2) GET /pets/1 — path param hit
    st, r = http_json("GET", "/v2/models/pets/pets/1")
    if not (st == 200 and isinstance(r, dict) and r.get("name") == "Fido"):
        return False, f"/pets/1: HTTP {st} -> {r}"
    # 3) GET /pets/99 — 404
    st, r = http_json("GET", "/v2/models/pets/pets/99")
    if st != 404:
        return False, f"/pets/99: expected 404, got HTTP {st}"
    # 4) POST /pets — create, returns 201
    st, r = http_json("POST", "/v2/models/pets/pets", {"name": "Buddy"})
    if not (st == 201 and isinstance(r, dict) and r.get("name") == "Buddy"):
        return False, f"POST /pets: HTTP {st} -> {r}"
    # 5) Standard inference still works
    st, r = http_json("POST", "/v2/models/pets/infer", {"input": 5})
    if not (st == 200 and isinstance(r, dict) and r.get("output") == 10):
        return False, f"infer: HTTP {st} -> {r}"
    return True, "status + path params + 404 + POST 201 + infer all OK"


def check_08():
    """Error handling: exercises exception-to-HTTP mapping."""
    path = "/v2/models/error_demo/infer"
    # 1) normal → 200
    st, r = http_json("POST", path, {"input": "hello", "mode": "normal"})
    if not (st == 200 and isinstance(r, dict) and r.get("output") == "ok: hello"):
        return False, f"normal: HTTP {st} -> {r}"
    # 2) bad_request → 400
    st, r = http_json("POST", path, {"input": "", "mode": "bad_request"})
    if st != 400:
        return False, f"bad_request: expected 400, got HTTP {st}"
    # 3) not_found → 404
    st, r = http_json("POST", path, {"input": "x", "mode": "not_found"})
    if st != 404:
        return False, f"not_found: expected 404, got HTTP {st}"
    # 4) server_error → 500
    st, r = http_json("POST", path, {"input": "x", "mode": "server_error"})
    if st != 500:
        return False, f"server_error: expected 500, got HTTP {st}"
    # 5) invalid mode → 400 (caught in decode_request)
    st, r = http_json("POST", path, {"input": "x", "mode": "unknown"})
    if st != 400:
        return False, f"invalid_mode: expected 400, got HTTP {st}"
    return True, "200 + 400 + 404 + 500 + invalid_mode_400 all OK"


def check_07():
    st1, r1 = http_json("POST", "/v2/models/threshold/infer", {"score": 0.8})
    st2, r2 = http_json("POST", "/v2/models/threshold/infer", {"score": 0.3})
    ok = (st1 == 200 and r1.get("label") == "positive"
          and st2 == 200 and r2.get("label") == "negative")
    return ok, f"pos {r1} | neg {r2}"


def check_09():
    res = []

    def one():
        st, _ = http_json("POST", "/v2/models/metrics_demo/infer", {"input": 1})
        res.append(st)

    threads = [threading.Thread(target=one) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    st, txt = http_json("GET", "/metrics")
    has_demo = isinstance(txt, str) and ("demo_batch_size" in txt or "demo_predictions" in txt)
    ok = all(s == 200 for s in res) and has_demo
    return ok, f"infers={res.count(200)}/10 | /metrics has demo_={has_demo}"


def check_10():
    st, r = http_json("POST", "/v2/models/async_echo/infer", {"input": "hello"})
    return st == 200 and r.get("output") == "async_echo: hello", f"HTTP {st} -> {r}"


def check_11():
    st, r = http_json("POST", "/v2/models/logged_model/infer", {"input": 21})
    return st == 200 and r.get("output") == 42, f"HTTP {st} -> {r}"


def check_12():
    st, r = http_json("POST", "/v2/models/cb_llm/infer", {"prompt": "hello world this is a test"})
    ok = st == 200 and isinstance(r, dict) and len(r.get("tokens", [])) >= 1
    return ok, f"HTTP {st} -> {r}"


def check_13():
    # Bidirectional streaming runs over gRPC (the /stream WebSocket path is
    # server-side only).  Open a bidi session, send two chunks, close.
    import grpc.aio
    from lite_server.proto import BidiChunk, BidiData, BidiOpen, BidiClose

    async def run():
        async with grpc.aio.insecure_channel("localhost:8001") as ch:
            bidi = ch.stream_stream(
                "/liteserver.LiteServer/BidiStream",
                request_serializer=BidiChunk.SerializeToString,
                response_deserializer=BidiChunk.FromString,
            )
            call = bidi(timeout=30)

            async def read(timeout=10):
                r = await asyncio.wait_for(call.read(), timeout=timeout)
                if r is grpc.aio.EOF:
                    return None
                if r.WhichOneof("payload") == "data":
                    return json.loads(r.data.data)
                return None

            frames = []
            await call.write(BidiChunk(open=BidiOpen(
                model_name="asr", initial_data=json.dumps({"text": ""}).encode())))
            frames.append(await read())                       # on_open
            for word in ("hello", "world"):
                await call.write(BidiChunk(data=BidiData(
                    data=json.dumps({"text": word}).encode())))
                frames.append(await read())                   # on_chunk
            await call.write(BidiChunk(close=BidiClose()))
            frames.append(await read())                       # on_close
            await call.done_writing()
            return frames

    frames = asyncio.run(run())
    ok = (len(frames) >= 4
          and frames[0].get("status") == "ready"
          and frames[1].get("partial") == "hello"
          and frames[2].get("partial") == "hello world"
          and frames[3].get("final") == "hello world")
    return ok, f"bidi frames={frames}"


def check_14():
    st, r = http_json("POST", "/v2/models/hooked_model/infer", {"input": "hello"})
    ok = st == 200 and isinstance(r, dict) and r.get("output") == "hello"
    return ok, f"HTTP {st} -> {r}"


def check_16():
    st, r = http_json("POST", "/v2/models/grpc_echo/infer", {"input": "hello"})
    return st == 200 and r.get("output") == "grpc_echo: hello", f"HTTP {st} -> {r}"


def check_17():
    """Config templates: env-var auth + env-driven model output."""
    path = "/v2/models/env_demo/infer"
    key = {"X-API-Key": "test-key-17"}
    # Valid request with auth → 200, response echoes env-driven config
    st, r1 = http_json("POST", path, {"input": "world"}, headers=key)
    if not (st == 200 and isinstance(r1, dict) and r1.get("output") == "hello, world"):
        return False, f"valid: HTTP {st} -> {r1}"
    backend = r1.get("backend", "?")
    if backend != "cpu":
        return False, f"expected backend=cpu, got {backend}"
    # No auth header → 401
    st, r2 = http_json("POST", path, {"input": "world"})
    if st != 401:
        return False, f"no-auth: expected 401, got HTTP {st} -> {r2}"
    return True, f"auth OK + backend={backend} + 401 without key"


def check_15():
    path = "/v2/models/callbacks_demo/infer"
    key = {"X-API-Key": "demo-key"}
    valid = {"text": "hello", "note": None}  # schema: text required, note required-but-null
    # No API key -> 401 from ApiKeyAuth.before_decode_request
    st, r = http_json("POST", path, valid)
    if st != 401:
        return False, f"expected 401 without key, got HTTP {st} -> {r}"
    # Empty text -> 400 from the built-in JsonSchemaValidator (minLength: 1)
    st, r = http_json("POST", path, {"text": "", "note": None}, headers=key)
    if st != 400:
        return False, f"expected 400 for empty text, got HTTP {st} -> {r}"
    # Valid request -> 200; the schema-valid body is echoed back in output
    st, r = http_json("POST", path, valid, headers=key)
    if not (st == 200 and isinstance(r, dict) and r.get("output") == valid):
        return False, f"valid request failed: HTTP {st} -> {r}"
    # Same request again -> SimpleCache short-circuits with cached body
    st, r = http_json("POST", path, valid, headers=key)
    ok = st == 200 and isinstance(r, dict) and r.get("cached") is True
    return ok, f"cache hit: HTTP {st} -> {r}"


def tls_ctx():
    """SSL context trusting the example's CA and presenting the client cert
    (mTLS is mandatory in the TLS example — probes need it too)."""
    import ssl
    cert = os.path.join(ROOT, "18_tls_mtls", "certs")
    ctx = ssl.create_default_context(cafile=os.path.join(cert, "ca.crt"))
    ctx.load_cert_chain(os.path.join(cert, "client.crt"),
                        os.path.join(cert, "client.key"))
    return ctx


def check_18():
    """TLS/mTLS: mTLS request succeeds, no-client-cert handshake fails, and
    the server certificate rotates live (SIGHUP) without a restart."""
    import ssl
    import subprocess as sp

    CERT = os.path.join(ROOT, "18_tls_mtls", "certs")
    ctx = ssl.create_default_context(cafile=os.path.join(CERT, "ca.crt"))
    ctx.load_cert_chain(
        os.path.join(CERT, "client.crt"), os.path.join(CERT, "client.key"))

    def https_json(body):
        url = "https://localhost:8000/v2/models/tls_echo/infer"
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as r:
            return r.status, json.loads(r.read().decode())

    # 1) mTLS request works
    try:
        st, r = https_json({"input": 21})
    except Exception as e:
        return False, f"mTLS request failed: {type(e).__name__}: {e}"
    if not (st == 200 and r.get("output") == 42):
        return False, f"mTLS request: HTTP {st} -> {r}"

    # 2) no client cert -> TLS handshake rejected
    bare = ssl.create_default_context(cafile=os.path.join(CERT, "ca.crt"))
    try:
        req = urllib.request.Request(
            "https://localhost:8000/v2/models/tls_echo/infer",
            data=json.dumps({"input": 21}).encode(),
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, context=bare, timeout=10).read()
        return False, "expected handshake failure without client cert, but request succeeded"
    except Exception:
        pass  # handshake rejected — expected

    # 3) rotate the server certificate and SIGHUP -> peer CN flips, no restart
    sp.run(
        ["bash", "-c",
         "openssl req -newkey rsa:2048 -keyout certs/server.key -out /tmp/rot.csr "
         "-nodes -subj '/CN=localhost-rotated' && "
         "openssl x509 -req -in /tmp/rot.csr -CA certs/ca.crt -CAkey certs/ca.key "
         "-CAcreateserial -out certs/server.crt -days 3650 "
         "-extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\\n"
         "basicConstraints=critical,CA:false\\n"
         "keyUsage=critical,digitalSignature,keyEncipherment\\n"
         "extendedKeyUsage=serverAuth\\n') && "
         "rm -f /tmp/rot.csr"],
        cwd=os.path.join(ROOT, "18_tls_mtls"), capture_output=True, check=True)
    try:
        pid = int(sp.check_output(
            ["pgrep", "-f", "lite_server serve"]).split()[-1])
        os.kill(pid, signal.SIGHUP)
    except Exception:
        pass  # fall back to the 10s content poll below

    # 3) assert the peer now presents the rotated certificate (direct TLS
    #    handshake — independent of any HTTP client internals)
    import socket
    rotated = tls_ctx()
    rotated.load_cert_chain(
        os.path.join(CERT, "client.crt"), os.path.join(CERT, "client.key"))
    deadline = time.time() + 12
    cn = None
    while time.time() < deadline:
        try:
            raw = socket.create_connection(("localhost", 8000), timeout=5)
            with rotated.wrap_socket(raw, server_hostname="localhost") as ssock:
                peer = ssock.getpeercert()
                cn = next(v for subj in peer.get("subject", [])
                          for k, v in subj if k == "commonName")
                break
        except Exception:
            time.sleep(0.5)
    if cn != "localhost-rotated":
        return False, f"expected rotated CN=localhost-rotated, got {cn}"
    return True, f"mTLS ok + no-cert rejected + live rotation CN={cn}"


def check_19():
    """Canary: weighted split favors v2, and x-lite-version pins requests."""
    versions = []
    for _ in range(40):
        st, r = http_json("POST", "/v2/models/canary_echo/infer", {"input": 5})
        if st == 200 and isinstance(r, dict):
            versions.append(r.get("version"))
    v1, v2 = versions.count("v1"), versions.count("v2")
    if not (v1 > 0 and v2 > 0 and v2 > v1):
        return False, f"weights not honored (want v2 majority): v1={v1} v2={v2}"
    # x-lite-version pins
    for pin, expect_out in (("v1", 6), ("v2", 10)):
        st, r = http_json("POST", "/v2/models/canary_echo/infer",
                          {"input": 5}, headers={"x-lite-version": pin})
        if not (st == 200 and r.get("version") == pin and r.get("output") == expect_out):
            return False, f"pin {pin}: HTTP {st} -> {r}"
    return True, f"split v1={v1}/40 v2={v2}/40 | pins v1→6 v2→10 ok"


def check_20():
    """Overload: max_inflight rejects excess inference with 503 + Retry-After,
    and x-lite-timeout returns 504 at the deadline."""
    results = []
    retry_afters = []
    barrier = threading.Barrier(6)

    def one():
        barrier.wait()
        url = "http://localhost:8000/v2/models/slow_echo/infer"
        req = urllib.request.Request(url, data=json.dumps({"input": 1}).encode(),
                                     method="POST",
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=15).read()
            results.append(200)
        except urllib.error.HTTPError as e:
            results.append(e.code)
            if e.code == 503:
                retry_afters.append(e.headers.get("Retry-After"))
        except Exception:
            results.append(0)

    threads = [threading.Thread(target=one) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok200 = results.count(200)
    ok503 = results.count(503)
    if ok200 < 2 or ok503 < 1:
        return False, f"expected >=2x200 and >=1x503, got {sorted(results)}"
    if "1" not in retry_afters:
        return False, f"503 responses missing Retry-After: 1, got {retry_afters}"

    # deadline: x-lite-timeout 0.1s on a 0.8s model → 504
    st, r = http_json("POST", "/v2/models/slow_echo/infer", {"input": 1},
                      headers={"x-lite-timeout": "0.1"}, timeout=10)
    if st != 504:
        return False, f"expected 504 from x-lite-timeout, got HTTP {st} -> {r}"
    return True, f"200x{ok200} 503x{ok503} + Retry-After + deadline 504 ok"


def check_21():
    """Admin security: HTTP admin 401 without key / ok with key; gRPC admin on
    its UDS requires a key; a mutation lands in the audit log."""
    import grpc.aio
    from lite_server.proto.liteserver_pb2 import (
        GetInfoRequest, GetInfoResponse,
        ActivateVersionRequest, ActivateVersionResponse,
    )

    # 1) HTTP admin without key -> 401, with key -> ok
    st, _ = http_json("POST", "/v2/models/admin_echo/versions/v1/activate")
    if st != 401:
        return False, f"expected 401 without admin key, got HTTP {st}"
    st, r = http_json("POST", "/v2/models/admin_echo/versions/v1/activate",
                      headers={"x-admin-key": "secret-admin-key"})
    if not (st == 200 and isinstance(r, dict) and r.get("success") is True):
        return False, f"admin activate with key: HTTP {st} -> {r}"

    # 2) gRPC admin over its UDS — no key -> Unauthenticated, key -> ok.
    #    grpc-python sends the socket path as :authority, which tonic rejects
    #    with RST_STREAM/PROTOCOL_ERROR — pin it to localhost (grpcurl's
    #    -authority localhost is the equivalent).
    uds = "unix://" + os.path.join(ROOT, "21_admin_security", "admin.sock")
    async def admin_call(rpc_name, req, resp_type, metadata=None, timeout=10):
        ch = grpc.aio.insecure_channel(
            uds, options=(("grpc.default_authority", "localhost"),))
        try:
            rpc = ch.unary_unary(
                f"/liteserver.Admin/{rpc_name}",
                request_serializer=req.SerializeToString,
                response_deserializer=resp_type.FromString)
            call = rpc(req, timeout=timeout,
                       metadata=metadata or ())
            try:
                resp = await call
                return 0, resp
            except grpc.aio.AioRpcError as e:
                return e.code(), None
        finally:
            await ch.close()

    code, _ = asyncio.run(admin_call("GetInfo", GetInfoRequest(), GetInfoResponse))
    if code != grpc.StatusCode.UNAUTHENTICATED:
        return False, f"expected Unauthenticated on admin gRPC without key, got {code}"
    code, resp = asyncio.run(admin_call(
        "GetInfo", GetInfoRequest(), GetInfoResponse,
        metadata=(("x-admin-key", "secret-admin-key"),)))
    loaded = list(resp.loaded_models) if resp else []
    if code != 0 or "admin_echo/v1" not in loaded:
        return False, f"admin GetInfo with key failed: code={code} loaded={loaded}"

    # 3) a mutation (activate) writes an audit record
    code, _ = asyncio.run(admin_call(
        "ActivateVersion",
        ActivateVersionRequest(model_name="admin_echo", version="v1"),
        ActivateVersionResponse,
        metadata=(("x-admin-key", "secret-admin-key"),)))
    if code != 0:
        return False, f"ActivateVersion failed: {code}"
    time.sleep(1.0)
    audit_log = os.path.join(ROOT, "21_admin_security", "audit.log")
    if not os.path.exists(audit_log):
        return False, "audit.log not created"
    with open(audit_log, encoding="utf-8", errors="replace") as f:
        tail = f.read()
    if "admin control-plane mutation" not in tail or "activate" not in tail:
        return False, f"audit log missing activate record: {tail[-300:]!r}"
    return True, "401-no-key + keyed HTTP admin + UDS gRPC auth + audit record ok"


def check_22():
    """Warmup: the model recorded exactly the configured dummy inferences,
    proving the version warmed up before serving."""
    st, r = http_json("GET", "/v2/models/warmup_echo/stats")
    if not (st == 200 and isinstance(r, dict) and r.get("warmup_count") == 2):
        return False, f"expected warmup_count=2, got HTTP {st} -> {r}"
    # and normal inference still works
    st, r = http_json("POST", "/v2/models/warmup_echo/infer", {"input": 21})
    if not (st == 200 and isinstance(r, dict) and "warmup_count" in r.get("output", {})):
        return False, f"infer after warmup: HTTP {st} -> {r}"
    return True, "warmup_count=2 (iterations honored) + infer ok"


def check_23():
    """Advanced routing: x-sequence-id pins requests to one worker (pid), and
    DecoupledInfer streams 3 chunks + final over gRPC."""
    import grpc.aio
    from lite_server.proto.liteserver_pb2 import (
        DecoupledInferRequest, DecoupledResponse)

    # 1) sticky routing — same sequence id → same worker pid
    pids = []
    for _ in range(5):
        st, r = http_json("POST", "/v2/models/sticky_echo/infer", {"input": 1},
                          headers={"x-sequence-id": "session-42"})
        if st != 200 or "pid" not in r.get("output", {}):
            return False, f"sticky infer: HTTP {st} -> {r}"
        pids.append(r["output"]["pid"])
    if len(set(pids)) != 1:
        return False, f"sequence not sticky: pids={sorted(set(pids))}"

    # 2) DecoupledInfer — model pushes 3 chunks, then closes (server streaming:
    #    single request in, response stream out)
    async def decoupled():
        ch = grpc.aio.insecure_channel("localhost:8001")
        try:
            rpc = ch.unary_stream(
                "/liteserver.LiteServer/DecoupledInfer",
                request_serializer=DecoupledInferRequest.SerializeToString,
                response_deserializer=DecoupledResponse.FromString)
            call = rpc(DecoupledInferRequest(model_name="sticky_echo", data=b"{}"))
            frames = []
            while True:
                r = await asyncio.wait_for(call.read(), timeout=10)
                if r is grpc.aio.EOF:
                    break
                frames.append({"data": r.data, "final": r.is_final})
                if r.is_final:
                    break
            return frames
        finally:
            await ch.close()

    frames = asyncio.run(decoupled())
    if len(frames) != 4:
        return False, f"expected 3 chunks + final, got {len(frames)} frames"
    if not (frames[3]["final"] and frames[0]["final"] is False):
        return False, f"unexpected frame flags: {frames}"
    return True, f"sticky pid={pids[0]} x5 | decoupled 3 chunks + final ok"


def check_24():
    """Proxy/browser security: XFF cleansing, CORS preflight headers, and the
    WebSocket Origin gate (101 for a matching Origin, 403 otherwise)."""
    # 1) client-IP cleansing
    st, r = http_json("POST", "/v2/models/proxy_echo/infer", {"input": 1})
    ip0 = r.get("output", {}).get("client_ip") if isinstance(r, dict) else None
    st, r = http_json("POST", "/v2/models/proxy_echo/infer", {"input": 1},
                      headers={"X-Forwarded-For": "1.2.3.4"})
    ip1 = r.get("output", {}).get("client_ip") if isinstance(r, dict) else None
    st, r = http_json("POST", "/v2/models/proxy_echo/infer", {"input": 1},
                      headers={"X-Forwarded-For": "1.2.3.4, 5.6.7.8"})
    ip2 = r.get("output", {}).get("client_ip") if isinstance(r, dict) else None
    if not (ip0 == "127.0.0.1" and ip1 == "1.2.3.4" and ip2 == "5.6.7.8"):
        return False, f"xff cleansing: no={ip0} one={ip1} chain={ip2}"

    # 2) CORS preflight headers (raw request to read headers; keep the
    #    HTTPMessage object — its get() is case-insensitive)
    def options(origin):
        req = urllib.request.Request(
            "http://localhost:8000/v2/models/proxy_echo/infer", method="OPTIONS",
            headers={"Origin": origin, "Access-Control-Request-Method": "POST"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.headers
        except urllib.error.HTTPError as e:
            return e.code, e.headers

    st_ok, h_ok = options("https://app.example.com")
    st_bad, h_bad = options("https://evil.example.com")
    acao = h_ok.get("Access-Control-Allow-Origin")
    if not (st_ok == 204 and acao == "https://app.example.com"
            and "origin" in h_ok.get("Vary", "").lower()):
        return False, f"preflight ok-origin: HTTP {st_ok} acao={acao} vary={h_ok.get('Vary')}"
    if h_bad.get("Access-Control-Allow-Origin"):
        return False, f"preflight evil-origin got ACAO: {h_bad.get('Access-Control-Allow-Origin')}"

    # 3) WebSocket Origin gate — raw upgrade probe
    def ws_upgrade(origin):
        conn = http.client.HTTPConnection(HOST, PORT, timeout=5)
        try:
            hdrs = {"Connection": "Upgrade", "Upgrade": "websocket",
                    "Sec-WebSocket-Version": "13",
                    "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="}
            if origin is not None:
                hdrs["Origin"] = origin
            conn.request("GET", "/v2/models/proxy_echo/stream", headers=hdrs)
            return conn.getresponse().status
        except Exception:
            return None
        finally:
            conn.close()

    st_ws_ok = ws_upgrade("https://app.example.com")
    st_ws_bad = ws_upgrade("https://evil.example.com")
    if st_ws_ok != 101:
        return False, f"ws matching origin: expected 101, got {st_ws_ok}"
    if st_ws_bad != 403:
        return False, f"ws evil origin: expected 403, got {st_ws_bad}"
    return True, f"xff {ip0}/{ip1}/{ip2} + preflight ok/evil + ws 101/403"


def check_25():
    """KServe V2 dataplane: raw tensor bytes + Triton Binary e2e
    (tritonclient, binary response via binary_data_output)."""
    import numpy as np
    import tritonclient.http as httpclient

    # 1) Raw tensor bytes (octet-stream + x-tensor-* headers) -> JSON summary
    body = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32).tobytes()
    req = urllib.request.Request(
        BASE + "/v2/models/raw_tensor/infer", data=body, method="POST",
        headers={"Content-Type": "application/octet-stream",
                 "x-tensor-dtype": np.dtype("<f4").str, "x-tensor-shape": "4"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            r1 = json.loads(r.read().decode())
    except Exception as e:  # noqa: BLE001 — report any transport failure
        return False, f"raw tensor request failed: {e}"
    if r1.get("sum") != 10.0 or r1.get("shape") != [4]:
        return False, f"raw tensor: {r1}"

    # 2) Triton Binary: two FP32 inputs, binary output negotiated
    try:
        client = httpclient.InferenceServerClient(url="localhost:8000")
        a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        b = np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32)
        ia = httpclient.InferInput("a", [2, 2], "FP32")
        ia.set_data_from_numpy(a)
        ib = httpclient.InferInput("b", [2, 2], "FP32")
        ib.set_data_from_numpy(b)
        out = httpclient.InferRequestedOutput("output0", binary_data=True)
        got = client.infer("binary_sum", [ia, ib], outputs=[out]).as_numpy("output0")
    except Exception as e:  # noqa: BLE001
        return False, f"triton binary failed: {e}"
    if not np.array_equal(got, a + b):
        return False, f"triton binary sum mismatch: {got.tolist()}"
    return True, "raw tensor sum=10.0 + triton binary sum OK"


def check_26():
    """openai-compact: unary chat, SSE stream, embeddings, /v1/models (+404)."""
    # 1) Unary chat completion
    st, r = http_json("POST", "/v1/chat/completions",
                      {"model": "chat",
                       "messages": [{"role": "user", "content": "hi there"}]})
    if st != 200 or not isinstance(r, dict) or r.get("object") != "chat.completion":
        return False, f"unary chat: HTTP {st} -> {r}"
    content = r["choices"][0]["message"]["content"]
    if content != "chat echo: hi there":
        return False, f"unary chat content: {content!r}"

    # 2) Streaming chat -> SSE data: chunks + finish + [DONE]
    def sse_collect(path, body, deadline_s=8.0):
        conn = http.client.HTTPConnection(HOST, PORT, timeout=deadline_s)
        events, status = [], None
        try:
            conn.request("POST", path, body=json.dumps(body),
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            status = resp.status
            end = time.time() + deadline_s
            while time.time() < end:
                try:
                    line = resp.readline()
                except Exception:  # socket timeout / closed
                    break
                if not line:
                    break
                s = line.decode("utf-8", errors="replace").strip()
                if s.startswith("data: "):
                    events.append(s[6:])
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return status, events

    st, events = sse_collect(
        "/v1/chat/completions",
        {"model": "chat", "stream": True,
         "messages": [{"role": "user", "content": "a b c"}]})
    if st != 200 or len(events) < 5 or events[-1] != "[DONE]":
        return False, f"stream: HTTP {st} events={events}"
    try:
        deltas = [json.loads(e)["choices"][0]["delta"].get("content", "")
                  for e in events[:4]]
    except Exception as e:  # noqa: BLE001
        return False, f"stream parse: {e}"
    if deltas[:3] != ["a", "b", "c"]:
        return False, f"stream deltas: {deltas}"

    # 3) Embeddings
    st, r = http_json("POST", "/v1/embeddings", {"model": "embed", "input": "hey"})
    if st != 200 or not isinstance(r, dict) or r.get("object") != "list":
        return False, f"embeddings: HTTP {st} -> {r}"
    if r["data"][0]["embedding"] != [104.0, 101.0, 121.0]:
        return False, f"embedding: {r['data'][0]['embedding']}"

    # 4) Model listing + unknown-model 404
    st, r = http_json("GET", "/v1/models")
    ids = [m["id"] for m in r.get("data", [])] if isinstance(r, dict) else []
    if st != 200 or "chat" not in ids or "embed" not in ids:
        return False, f"/v1/models: HTTP {st} -> {r}"
    st, r = http_json("POST", "/v1/chat/completions",
                      {"model": "nope",
                       "messages": [{"role": "user", "content": "x"}]})
    if st != 404:
        return False, f"unknown model: expected 404, got HTTP {st} -> {r}"
    return True, "unary+stream chat + embeddings + /v1/models OK"


# example dir -> (primary model for readiness, check fn)
SPECS = {
    "01_basic": ("echo", check_01),
    "02_batching": ("batched", check_02),
    "03_streaming": ("sse_tokens", check_03),
    "04_multi_version": ("multi_version", check_04),
    "05_ensemble": ("pipeline", check_05),
    "06_custom_route": ("pets", check_06),
    "07_custom_params": ("threshold", check_07),
    "08_error_handling": ("error_demo", check_08),
    "09_custom_metrics": ("metrics_demo", check_09),
    "10_async": ("async_echo", check_10),
    "11_logging": ("logged_model", check_11),
    "12_continuous_batching": ("cb_llm", check_12),
    "13_bidi_streaming": ("asr", check_13),
    "14_lifecycle_hooks": ("hooked_model", check_14),
    "15_callbacks": ("callbacks_demo", check_15),
    "16_grpc": ("grpc_echo", check_16),
    "17_config_templates": ("env_demo", check_17, {"DEMO_API_KEY": "test-key-17"}),
    "18_tls_mtls": ("tls_echo", check_18, None, "https://localhost:8000", tls_ctx),
    "19_canary": ("canary_echo", check_19),
    "20_overload_control": ("slow_echo", check_20),
    "21_admin_security": ("admin_echo", check_21, None, None, None,
                          {"x-admin-key": "secret-admin-key"}),
    "22_warmup": ("warmup_echo", check_22),
    "23_advanced_routing": ("sticky_echo", check_23),
    "24_proxy_security": ("proxy_echo", check_24),
    "25_kserve_v2": ("raw_tensor", check_25),
    "26_openai_compact": ("chat", check_26),
}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_one(example, timeout=45, verbose=False):
    spec = SPECS[example]
    primary, check = spec[0], spec[1]
    env = spec[2] if len(spec) > 2 else None
    base = spec[3] or BASE if len(spec) > 3 else BASE
    run_setup(example)
    ctx = spec[4]() if len(spec) > 4 and callable(spec[4]) else (spec[4] if len(spec) > 4 else None)
    hdrs = spec[5] if len(spec) > 5 else None
    proc, log_path, log = start_server(example, env=env)
    result = {"example": example, "ready": False, "ok": False, "detail": "", "log": log_path}
    try:
        if verbose:
            print(f"  [startup] waiting up to {timeout}s for model '{primary}' ...", flush=True)
        result["ready"] = wait_ready(primary, timeout=timeout, base=base,
                                     context=ctx, headers=hdrs)
        if not result["ready"]:
            result["detail"] = f"NOT READY within {timeout}s (primary model={primary})"
        else:
            if verbose:
                print(f"  [startup] model ready, running check ...", flush=True)
            ok, detail = check()
            result["ok"] = bool(ok)
            result["detail"] = detail
    finally:
        stop_server(proc)
        log.close()
    return result


def main():
    # Fail fast on missing hard requirements.
    try:
        import grpc.aio  # noqa: F401
    except ImportError:
        print("ERROR: grpcio is required (example 13 uses gRPC bidi).\n"
              "       Install it with:  pip install grpcio")
        sys.exit(2)
    try:
        import lite_server  # noqa: F401
    except ImportError:
        print("ERROR: lite-server is not installed in this Python env.\n"
              "       Install it (pip install -e .) so `python -m lite_server` works.")
        sys.exit(2)

    # Parse CLI flags and positional example names.
    args = sys.argv[1:]
    verbose = False
    timeout = 45
    only = []
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("-h", "--help"):
            print(USAGE)
            sys.exit(0)
        elif a in ("-v", "--verbose"):
            verbose = True
        elif a in ("-t", "--timeout"):
            i += 1
            if i >= len(args):
                print("ERROR: --timeout requires a value", file=sys.stderr)
                sys.exit(2)
            try:
                timeout = int(args[i])
            except ValueError:
                print(f"ERROR: invalid timeout value: {args[i]}", file=sys.stderr)
                sys.exit(2)
            if timeout < 1:
                print("ERROR: timeout must be >= 1", file=sys.stderr)
                sys.exit(2)
        else:
            only.append(a)
        i += 1

    only = only or list(SPECS.keys())
    unknown = [e for e in only if e not in SPECS]
    if unknown:
        print(f"ERROR: unknown example(s): {unknown}\n"
              f"       available: {', '.join(SPECS)}")
        sys.exit(2)

    results = []
    for ex in only:
        print(f"\n===== {ex} =====", flush=True)
        r = run_one(ex, timeout=timeout, verbose=verbose)
        results.append(r)
        status = ("PASS" if r["ready"] and r["ok"]
                  else "READY-FAIL" if not r["ready"] else "CHECK-FAIL")
        print(f"[{status}] ready={r['ready']} ok={r['ok']}", flush=True)
        print(f"  {r['detail']}", flush=True)
        if not (r["ready"] and r["ok"]):
            print(f"  (server log: {r['log']})", flush=True)

    print("\n================ SUMMARY ================")
    width = max(len(e) for e in SPECS)
    all_ok = True
    for r in results:
        status = "PASS" if r["ready"] and r["ok"] else "FAIL"
        all_ok = all_ok and r["ready"] and r["ok"]
        print(f"  {r['example']:<{width}}  {status}")
    passed = sum(1 for r in results if r["ready"] and r["ok"])
    print(f"\n  {passed}/{len(results)} passed")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
