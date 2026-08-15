#!/usr/bin/env python3
"""Connection-count black-box smoke: the server's open HTTP connection gauge
must stay bounded under concurrency and return to baseline when connections
close — on plaintext TCP AND TLS, including the TLS handshake-failure path.

What this verifies (against the current source, not the installed wheel):
  * L4 gauge `liteserver_http_connections{transport="tcp"|"tls"}` — +1 at
    accept, -1 when the connection task ends (prometheus.rs:370, http/mod.rs,
    tls.rs CountedTlsStream). A leak shows up as the gauge never returning to
    baseline.
  * D7 hard cap `server.max_connections` — over-cap connections are refused at
    accept, so the gauge is bounded even under connection floods.
  * TLS handshake-failure path (1b06784): the accept-time cap counter slot is
    returned by `ConnectionCountGuard::Drop` when a handshake fails. Without
    the guard, N failed handshakes would permanently occupy N slots and
    eventually refuse ALL valid connections. This smoke fires garbage
    (non-TLS) connections and asserts a valid request still succeeds.

Usage:
    tools/conn_smoke.py                 # full suite: tcp + tls
    tools/conn_smoke.py --scope tcp     # plaintext only
    tools/conn_smoke.py --scope tls     # TLS only (incl. D7 handshake-failure)
    tools/conn_smoke.py --binary /path/to/lite-server-core

Requires:
    * a built ./target/debug/lite-server-core (auto-builds if missing)
    * a Python interpreter (stdlib only) — the script itself needs nothing
      from the venv; the server spawns no workers (empty model repo)

No model is loaded — the probe is GET /health, which exercises the same
serve_tcp/serve_tls accept loop and connection gauge as real inference.

Exit code: 0 = all assertions passed; 1 = any assertion failed.
"""

import argparse
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOST = "127.0.0.1"
TLS_CERTS = os.path.join(REPO_ROOT, "examples/18_tls_mtls/certs")

# --- tunables -----------------------------------------------------------
TCP_CONC = 32          # concurrent held /health connections, no-cap run
TCP_ROUNDS = 4         # open/close cycles — catches one-per-cycle leaks
TCP_CAP = 8            # max_connections for the cap run
TCP_CAP_CONC = 32      # concurrent connections thrown at the cap
TLS_HOLD = 8           # valid TLS connections held open
TLS_CAP = 10           # max_connections for the TLS cap run
TLS_CAP_CONC = 25      # concurrent valid connections thrown at the TLS cap
TLS_GARBAGE_ROUNDS = 3  # handshake-failure bursts
TLS_GARBAGE_N = 30     # garbage connections per burst
POLL_DEADLINE = 6.0    # seconds to wait for the gauge to settle
SLACK = 2              # gauge-peak tolerance (accept-loop transients)


class SmokeError(Exception):
    pass


def log(msg):
    print(f"[conn_smoke] {msg}", flush=True)


def fail(msg):
    print(f"[conn_smoke] FAIL: {msg}", flush=True)


# No proxy for 127.0.0.1 — macOS system proxies otherwise hijack localhost
# and turn these probes into 502/timeouts. The HTTPS handler skips cert
# verification: the smoke uses the repo's self-signed test certs, and probes
# only care about connection-level reachability.
_SSL_CTX = ssl._create_unverified_context()
OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({}),
    urllib.request.HTTPSHandler(context=_SSL_CTX),
)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def fetch_gauge(metrics_port, transport):
    """Value of liteserver_http_connections{transport=...}; None on scrape
    failure (never a falsy pass)."""
    try:
        with OPENER.open(f"http://{HOST}:{metrics_port}/metrics", timeout=2) as r:
            for line in r:
                t = line.decode()
                if t.startswith(f'liteserver_http_connections{{transport="{transport}"}}'):
                    return int(t.split()[-1])
    except Exception:
        return None
    return 0


def gauge_at_least(mport, transport, bound):
    v = fetch_gauge(mport, transport)
    return v is not None and v >= bound


def gauge_at_most(mport, transport, bound):
    v = fetch_gauge(mport, transport)
    return v is not None and v <= bound


def gauge_is_zero(mport, transport):
    v = fetch_gauge(mport, transport)
    return v == 0


def observed_peak(samples):
    real = [v for v in samples if v is not None]
    return max(real) if real else 0


def poll_until(cond, deadline=POLL_DEADLINE, interval=0.1, label=""):
    end = time.time() + deadline
    while time.time() < end:
        if cond():
            return True
        time.sleep(interval)
    if label:
        fail(f"timed out waiting for {label}")
    return False


def sample_gauge(mport, transport, seconds):
    samples = []
    end = time.time() + seconds
    while time.time() < end:
        samples.append(fetch_gauge(mport, transport))
        time.sleep(0.1)
    return samples


def http_open(port, n):
    """Open n concurrent keep-alive GET /health connections; hold them open.
    Returns (held_sockets, ok_responses, refused)."""
    results = []
    lock = threading.Lock()

    def one():
        try:
            s = socket.create_connection((HOST, port), timeout=5)
            s.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            s.settimeout(3)
            buf = b""
            while b"\r\n\r\n" not in buf:
                c = s.recv(4096)
                if not c:
                    break
                buf += c
            ok = buf.startswith(b"HTTP/1.1 200")
            # Refused connections surface as either RST (OSError above) or a
            # clean FIN before any response — both must count as "not held".
            if not ok:
                s.close()
                with lock:
                    results.append((False, None))
                return
            with lock:
                results.append((ok, s))
        except OSError:
            with lock:
                results.append((False, None))

    _run_threads([threading.Thread(target=one) for _ in range(n)])
    held = [s for _, s in results if s is not None]
    ok = sum(1 for o, _ in results if o)
    return held, ok, n - len(held)


def tls_open(port, n):
    ctx = ssl._create_unverified_context()
    results = []
    lock = threading.Lock()

    def one():
        try:
            raw = socket.create_connection((HOST, port), timeout=5)
            tls = ctx.wrap_socket(raw, server_hostname="localhost")
            tls.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            tls.settimeout(3)
            buf = b""
            while b"\r\n\r\n" not in buf:
                c = tls.recv(4096)
                if not c:
                    break
                buf += c
            ok = buf.startswith(b"HTTP/1.1 200")
            if not ok:
                tls.close()
                with lock:
                    results.append((False, None))
                return
            with lock:
                results.append((ok, tls))
        except OSError:
            with lock:
                results.append((False, None))

    _run_threads([threading.Thread(target=one) for _ in range(n)])
    held = [s for _, s in results if s is not None]
    ok = sum(1 for o, _ in results if o)
    return held, ok, n - len(held)


def fire_garbage(port, n):
    """n concurrent connections that fail the TLS handshake (non-TLS bytes)."""
    lock = threading.Lock()
    fired = [0]

    def one():
        try:
            s = socket.create_connection((HOST, port), timeout=5)
            s.sendall(b"\x16\x03\x01 this-is-not-a-tls-clienthello\r\n\x00\xff")
            s.close()
            with lock:
                fired[0] += 1
        except OSError:
            pass

    _run_threads([threading.Thread(target=one) for _ in range(n)])
    return fired[0]


def _run_threads(ts):
    for t in ts:
        t.start()
    for t in ts:
        t.join()


def _drain(src, dst):
    for chunk in iter(lambda: src.read(4096), b""):
        dst.write(chunk)
    dst.flush()


def spawn_server(binary, cfg_path, workdir, tag):
    """Spawn the server, draining its stdout/stderr to a per-server log file
    (a pipe + reader avoids losing libc-buffered output on SIGKILL, which a
    shared append-only file suffered)."""
    logpath = os.path.join(workdir, f"server_{tag}.log")
    proc = subprocess.Popen(
        [binary, "serve", "-c", cfg_path],
        env=dict(os.environ, **{
            "LITESERVER_PYTHON": os.path.join(REPO_ROOT, ".venv/bin/python"),
            "RUST_LOG": os.environ.get("RUST_LOG", "warn"),
        }),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    threading.Thread(target=_drain, args=(proc.stdout, open(logpath, "wb")),
                     daemon=True).start()
    return proc, logpath


def wait_ready(url, timeout=25):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with OPENER.open(url, timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def stop_server(proc, logpath):
    proc.terminate()  # SIGTERM — graceful drain
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        log("warning: graceful shutdown exceeded 10s; SIGKILL "
            f"(last log: {_log_tail(logpath)})")
        proc.kill()
        proc.wait()


def _log_tail(logpath, n=5):
    try:
        with open(logpath, "rb") as f:
            lines = f.read().decode(errors="replace").splitlines()
        return " | ".join(lines[-n:])
    except OSError:
        return "<no log>"


def lsof_established(pid):
    """Count established TCP sockets for pid via lsof; None if unavailable."""
    try:
        out = subprocess.run(
            ["lsof", "-nP", "-a", "-p", str(pid), "-iTCP", "-sTCP:ESTABLISHED"],
            capture_output=True, text=True, timeout=5,
        ).stdout
        return max(len(out.strip().splitlines()) - 1, 0)  # drop the header
    except (subprocess.SubprocessError, FileNotFoundError):
        return None


def write_config(path, port, metrics_port, grpc_port, cap, empty_repo, tls=False):
    tls_block = (
        "  tls_cert_path: " + os.path.join(TLS_CERTS, "server.crt") + "\n"
        "  tls_key_path: " + os.path.join(TLS_CERTS, "server.key") + "\n"
    ) if tls else ""
    with open(path, "w") as f:
        f.write(
            "server:\n"
            f"  http_port: {port}\n"
            f"  host: {HOST}\n"
            f"  metrics_port: {metrics_port}\n"
            f"  grpc_port: {grpc_port}\n"
            "  keepalive_timeout: 30.0\n"
            f"  max_connections: {cap}\n"
            f"{tls_block}"
            "model_repository:\n"
            f"  path: {empty_repo}\n"
        )


# -------------------------------------------------------------------------
# TCP suite
# -------------------------------------------------------------------------
def run_tcp_suite(binary, workdir, empty_repo):
    log(f"TCP suite: no-cap concurrent={TCP_CONC} rounds={TCP_ROUNDS}, "
        f"cap run cap={TCP_CAP} concurrent={TCP_CAP_CONC}")

    # --- TCP-1: no cap — gauge tracks held conns, returns to baseline, and
    # stays baseline across cycles (no cumulative leak). ---
    port, mport, gport = free_port(), free_port(), free_port()
    cfg = os.path.join(workdir, "tcp_nocap.yaml")
    write_config(cfg, port, mport, gport, cap=0, empty_repo=empty_repo)
    proc, logpath = spawn_server(binary, cfg, workdir, "tcp_nocap")
    if not wait_ready(f"http://{HOST}:{port}/health"):
        stop_server(proc, logpath)
        raise SmokeError(f"TCP server did not become ready (log: {_log_tail(logpath)})")
    base = fetch_gauge(mport, "tcp")
    try:
        for rnd in range(1, TCP_ROUNDS + 1):
            held, ok, refused = http_open(port, TCP_CONC)
            if not poll_until(
                lambda: gauge_at_least(mport, "tcp", TCP_CONC - SLACK),
                label=f"TCP-1 round {rnd} gauge to rise to ~{TCP_CONC}",
            ):
                raise SmokeError(
                    f"TCP-1 round {rnd}: gauge never reached ~{TCP_CONC}; "
                    f"ok={ok} refused={refused} "
                    f"last_gauges={[fetch_gauge(mport, 'tcp') for _ in range(3)]}")
            peak = observed_peak(sample_gauge(mport, "tcp", 2.0))
            if peak > TCP_CONC + SLACK:
                raise SmokeError(
                    f"TCP-1 round {rnd}: gauge peaked at {peak} > {TCP_CONC} held "
                    f"conns + slack {SLACK} — connections not counted 1:1")
            for s in held:
                s.close()
            if not poll_until(
                lambda: gauge_at_most(mport, "tcp", base),
                label=f"TCP-1 round {rnd} gauge back to baseline",
            ):
                raise SmokeError(
                    f"TCP-1 round {rnd}: gauge did not return to baseline {base} "
                    f"after closing {len(held)} connections — connection-count leak")
            log(f"TCP-1 round {rnd}/{TCP_ROUNDS}: peak={peak} -> baseline OK")
        log(f"TCP-1 PASS: gauge tracked {TCP_CONC} conns across {TCP_ROUNDS} "
            f"rounds and returned to baseline each time")
    finally:
        stop_server(proc, logpath)

    # --- TCP-2: hard cap — gauge never exceeds max_connections, over-cap
    # connections refused at accept, returns to baseline. ---
    port, mport, gport = free_port(), free_port(), free_port()
    cfg = os.path.join(workdir, "tcp_cap.yaml")
    write_config(cfg, port, mport, gport, cap=TCP_CAP, empty_repo=empty_repo)
    proc, logpath = spawn_server(binary, cfg, workdir, "tcp_cap")
    if not wait_ready(f"http://{HOST}:{port}/health"):
        stop_server(proc, logpath)
        raise SmokeError(f"TCP cap server did not become ready "
                         f"(log: {_log_tail(logpath)})")
    base = fetch_gauge(mport, "tcp")
    try:
        held, ok, refused = http_open(port, TCP_CAP_CONC)
        peak = observed_peak(sample_gauge(mport, "tcp", 3.0))
        if peak > TCP_CAP + SLACK:
            raise SmokeError(
                f"TCP-2: gauge peaked at {peak} > max_connections={TCP_CAP} + "
                f"slack {SLACK} — hard cap not enforced")
        if peak < TCP_CAP - SLACK:
            raise SmokeError(
                f"TCP-2: gauge never filled the cap (peak={peak} < {TCP_CAP})")
        if refused < TCP_CAP_CONC - TCP_CAP - SLACK:
            raise SmokeError(
                f"TCP-2: expected ~{TCP_CAP_CONC - TCP_CAP} over-cap refusals, "
                f"got {refused} (ok={ok})")
        for s in held:
            s.close()
        if not poll_until(lambda: gauge_at_most(mport, "tcp", base),
                          label="TCP-2 gauge back to baseline"):
            raise SmokeError("TCP-2: gauge did not return to baseline after close")
        log(f"TCP-2 PASS: peak={peak} (cap {TCP_CAP}), over-cap refused={refused}, "
            f"baseline restored")
    finally:
        stop_server(proc, logpath)
        est = lsof_established(proc.pid)
        if est:
            raise SmokeError(f"TCP suite: {est} established TCP connections remain "
                             f"after the suite — leaked file descriptors")


# -------------------------------------------------------------------------
# TLS suite
# -------------------------------------------------------------------------
def run_tls_suite(binary, workdir, empty_repo):
    log(f"TLS suite: valid-hold={TLS_HOLD}, cap={TLS_CAP} vs {TLS_CAP_CONC}, "
        f"garbage {TLS_GARBAGE_ROUNDS}x{TLS_GARBAGE_N}")

    port, mport, gport = free_port(), free_port(), free_port()
    cfg = os.path.join(workdir, "tls.yaml")
    write_config(cfg, port, mport, gport, cap=TLS_CAP, empty_repo=empty_repo,
                 tls=True)
    proc, logpath = spawn_server(binary, cfg, workdir, "tls")
    if not wait_ready(f"https://{HOST}:{port}/health"):
        stop_server(proc, logpath)
        raise SmokeError(f"TLS server did not become ready "
                         f"(log: {_log_tail(logpath)})")
    base = fetch_gauge(mport, "tls")
    try:
        # --- TLS-1: valid TLS connections tracked by the tls gauge ---
        held, ok, refused = tls_open(port, TLS_HOLD)
        if ok < TLS_HOLD:
            raise SmokeError(
                f"TLS-1: only {ok}/{TLS_HOLD} valid TLS connections succeeded")
        if not poll_until(lambda: gauge_at_least(mport, "tls", TLS_HOLD - SLACK),
                          label="TLS-1 gauge to rise to held count"):
            raise SmokeError(
                f"TLS-1: tls gauge never reached ~{TLS_HOLD} while {TLS_HOLD} "
                f"connections held")
        peak = observed_peak(sample_gauge(mport, "tls", 1.5))
        if peak > TLS_HOLD + SLACK:
            raise SmokeError(f"TLS-1: gauge peaked at {peak} > {TLS_HOLD} held")
        for s in held:
            s.close()
        if not poll_until(lambda: gauge_at_most(mport, "tls", base),
                          label="TLS-1 gauge back to baseline"):
            raise SmokeError(
                "TLS-1: tls gauge did not return to baseline after close")
        log(f"TLS-1 PASS: {TLS_HOLD} valid TLS conns tracked, baseline restored")

        # --- TLS-2: hard cap on TLS — peak <= cap, over-cap refused ---
        held, ok, refused = tls_open(port, TLS_CAP_CONC)
        if ok < TLS_CAP - SLACK or ok > TLS_CAP + SLACK:
            raise SmokeError(
                f"TLS-2: expected ~{TLS_CAP} accepted under cap, got ok={ok} "
                f"refused={refused}")
        peak = observed_peak(sample_gauge(mport, "tls", 2.0))
        if peak > TLS_CAP + SLACK:
            raise SmokeError(f"TLS-2: gauge peaked at {peak} > cap {TLS_CAP}")
        for s in held:
            s.close()
        if not poll_until(lambda: gauge_at_most(mport, "tls", base),
                          label="TLS-2 gauge back to baseline"):
            raise SmokeError("TLS-2: tls gauge did not return to baseline")
        log(f"TLS-2 PASS: cap {TLS_CAP} enforced (accepted {ok}, refused {refused}), "
            f"baseline restored")

        # --- TLS-3: D7 handshake-failure path — garbage conns must not
        # permanently occupy cap slots; a valid request still succeeds. ---
        for rnd in range(1, TLS_GARBAGE_ROUNDS + 1):
            fired = fire_garbage(port, TLS_GARBAGE_N)
            time.sleep(0.6)  # let the server process the failed handshakes
            if not gauge_is_zero(mport, "tls"):
                raise SmokeError(
                    f"TLS-3 round {rnd}: tls gauge nonzero after handshake "
                    f"failures (garbage conns must not complete handshakes)")
            # The D7 regression signal: with cap={TLS_CAP}, if the accept-time
            # counter leaked one slot per failed handshake, this probe would
            # be refused. It must return 200.
            held1, ok1, _ = tls_open(port, 1)
            if ok1 != 1:
                raise SmokeError(
                    f"TLS-3 round {rnd}: after {fired} failed handshakes a valid "
                    f"TLS request was refused — ConnectionCountGuard leak "
                    f"(D7, 1b06784)")
            held1[0].close()
            log(f"TLS-3 round {rnd}/{TLS_GARBAGE_ROUNDS}: {fired} handshake "
                f"failures, valid probe 200 OK")
        log(f"TLS-3 PASS: {TLS_GARBAGE_ROUNDS} handshake-failure bursts left the "
            f"cap counter uncontaminated")
    finally:
        stop_server(proc, logpath)
        est = lsof_established(proc.pid)
        if est:
            raise SmokeError(f"TLS suite: {est} established TCP connections remain "
                             f"after the suite — leaked file descriptors")


def main():
    ap = argparse.ArgumentParser(description="Connection-count black-box smoke")
    ap.add_argument("--scope", choices=["tcp", "tls", "all"], default="all",
                    help="suite to run (default: all)")
    ap.add_argument("--binary", default=None,
                    help="path to lite-server-core (default: target/debug, "
                         "auto-built if missing)")
    ap.add_argument("--workdir", default=None,
                    help="keep server configs/logs in DIR instead of a temp dir")
    args = ap.parse_args()

    binary = args.binary or os.path.join(REPO_ROOT, "target/debug/lite-server-core")
    if not os.path.exists(binary):
        log(f"binary not found at {binary}; running cargo build...")
        subprocess.run(["cargo", "build", "--bin", "lite-server-core"],
                       cwd=REPO_ROOT, check=True)
    if not os.path.exists(binary):
        log("ERROR: lite-server-core still missing after build")
        return 1

    if not os.path.exists(os.path.join(TLS_CERTS, "server.crt")):
        log(f"ERROR: TLS certs not found in {TLS_CERTS}")
        return 1

    if args.workdir:
        os.makedirs(args.workdir, exist_ok=True)
        workdir = args.workdir
    else:
        workdir = tempfile.mkdtemp(prefix="conn_smoke_")
    empty_repo = os.path.join(workdir, "empty_model_repo")
    os.makedirs(empty_repo, exist_ok=True)

    failures = []
    try:
        if args.scope in ("tcp", "all"):
            try:
                run_tcp_suite(binary, workdir, empty_repo)
            except SmokeError as e:
                failures.append(str(e))
        if args.scope in ("tls", "all"):
            try:
                run_tls_suite(binary, workdir, empty_repo)
            except SmokeError as e:
                failures.append(str(e))
    except KeyboardInterrupt:
        log("interrupted")
        return 130

    if failures:
        print("\n[conn_smoke] RESULT: FAIL")
        for f in failures:
            print(f"  - {f}")
        log(f"artifacts kept in {workdir}")
        return 1
    print("\n[conn_smoke] RESULT: PASS")
    log(f"artifacts in {workdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
