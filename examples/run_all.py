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


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def http_json(method, path, body=None, headers=None, timeout=10):
    """Return (status, parsed_body_or_text).  status is None on connection error."""
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
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


def wait_ready(model, timeout=45):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st, body = http_json("GET", f"/v2/models/{model}/ready", timeout=3)
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

def start_server(example):
    log_path = os.path.join("/tmp", f"run_all_{example}.log")
    log = open(log_path, "wb")
    proc = subprocess.Popen(
        [sys.executable, "-m", "lite_server", "serve", "--config", "server.yaml"],
        cwd=os.path.join(ROOT, example),
        stdout=log, stderr=subprocess.STDOUT,
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
    n = read_sse("/v2/models/streaming/events",
                 {"prompt": "hello world test", "max_tokens": 3}, want_lines=4)
    return n >= 4, f"SSE data-lines={n} (expect >=4: 3 tokens + [DONE])"


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


def check_15():
    # Model-level callback chain guards every route of the model — inference
    # and custom @route handlers alike. No key → 401; valid key → 200.
    key = {"X-API-Key": "secret-api-key-123"}
    st1, _ = http_json("POST", "/v2/models/protected/infer", {"input": "hello"})
    st2, r2 = http_json("POST", "/v2/models/protected/infer", {"input": "hello"}, headers=key)
    st3, _ = http_json("GET", "/v2/models/protected/status")
    st4, r4 = http_json("GET", "/v2/models/protected/status", headers=key)
    ok = (st1 == 401
          and st2 == 200 and isinstance(r2, dict) and r2.get("output") == "protected: hello"
          and st3 == 401
          and st4 == 200 and isinstance(r4, dict))
    return ok, (f"infer no-key HTTP {st1} | infer key HTTP {st2} | "
                f"status no-key HTTP {st3} | status key HTTP {st4}")


def check_16():
    st, r = http_json("POST", "/v2/models/grpc_echo/infer", {"input": "hello"})
    return st == 200 and r.get("output") == "grpc_echo: hello", f"HTTP {st} -> {r}"


# example dir -> (primary model for readiness, check fn)
SPECS = {
    "01_basic": ("echo", check_01),
    "02_batching": ("batched", check_02),
    "03_streaming": ("streaming", check_03),
    "04_multi_version": ("multi_version", check_04),
    "05_ensemble": ("pipeline", check_05),
    "07_custom_params": ("threshold", check_07),
    "09_custom_metrics": ("metrics_demo", check_09),
    "10_async": ("async_echo", check_10),
    "11_logging": ("logged_model", check_11),
    "12_continuous_batching": ("cb_llm", check_12),
    "13_bidi_streaming": ("asr", check_13),
    "14_lifecycle_hooks": ("hooked_model", check_14),
    "15_middleware": ("protected", check_15),
    "16_grpc": ("grpc_echo", check_16),
}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_one(example):
    primary, check = SPECS[example]
    proc, log_path, log = start_server(example)
    result = {"example": example, "ready": False, "ok": False, "detail": "", "log": log_path}
    try:
        result["ready"] = wait_ready(primary, timeout=45)
        if not result["ready"]:
            result["detail"] = f"NOT READY within 45s (primary model={primary})"
        else:
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

    only = sys.argv[1:] or list(SPECS.keys())
    unknown = [e for e in only if e not in SPECS]
    if unknown:
        print(f"ERROR: unknown example(s): {unknown}\n"
              f"       available: {', '.join(SPECS)}")
        sys.exit(2)

    results = []
    for ex in only:
        print(f"\n===== {ex} =====", flush=True)
        r = run_one(ex)
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
