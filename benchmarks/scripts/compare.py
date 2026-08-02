#!/usr/bin/env python3
"""Benchmark comparison: lite-server vs LitServe using wrk.

Orchestrates server lifecycle, runs wrk for each (workers, concurrency) pair,
and outputs CSV + optional matplotlib charts.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import os
import re
import signal
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from urllib import request


def wait_for_server(url: str, timeout: float = 30.0) -> bool:
    """Poll until server responds with HTTP 200 and valid JSON output."""
    deadline = time.time() + timeout
    data = b'{"input":"hello"}'
    while time.time() < deadline:
        try:
            req = request.Request(
                url,
                method="POST",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    body = resp.read().decode()
                    if '"output"' in body:
                        return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _scrub_escapees(cmd):
    """SIGKILL descendants that escaped the process-group kill.

    LitServe's multiprocessing workers reparent to PID 1 (launchd/init) and
    survive ``killpg``; each pegs ~1 core in a busy spin indefinitely. Measured
    impact on the NEXT server's benchmark: ~6200 → ~4400 rps — i.e. the entire
    spurious "core slower than lite" inversion was two of these orphans stealing
    cores during whichever server ran right after LitServe (rotation only picks
    the victim; cooldown does not kill them). Match survivors by the launched
    script's basename and SIGKILL. Idempotent and safe: teardown-only, and
    lite-server/lite-server-core workers stay in the group so this finds nothing
    for them.
    """
    script = next((a for a in cmd[1:] if isinstance(a, str) and a.endswith(".py")), None)
    if not script:
        return
    pat = os.path.basename(script)
    try:
        subprocess.run(
            ["pkill", "-9", "-f", pat],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
        )
    except FileNotFoundError:
        pass  # pkill unavailable (non-POSIX); group kill still ran


def _terminate_process_group(proc):
    """SIGTERM → SIGKILL the whole process group, best-effort."""
    if proc.poll() is not None:
        return
    pgid = None
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass
    if pgid is not None:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        proc.wait(timeout=2)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        proc.kill()
        proc.wait(timeout=1)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        pass


@contextlib.contextmanager
def _managed_popen(cmd, **kwargs):
    """启动子进程并在退出时级联清理：先 kill 整个进程组，再 scrub 逃逸进程。

    必须先 group-kill 再 scrub：group-kill 负责组内子进程（lite-server-core
    二进制等）；scrub 才负责 LitServe 那些脱离进程组、被 launchd 收养（PPID=1）、
    killpg 够不着的 multiprocessing worker。顺序反了会让 launcher 提前死掉、
    poll() 非 None、跳过 group-kill，把真正的 server 子进程遗弃在端口上。
    """
    proc = subprocess.Popen(cmd, start_new_session=True, **kwargs)
    try:
        yield proc
    finally:
        _terminate_process_group(proc)
        _scrub_escapees(cmd)


def run_wrk(
    wrk_path: str,
    url: str,
    concurrency: int,
    duration: float,
    lua_script: Path,
    threads: int = 4,
) -> dict[str, float]:
    """Run wrk and parse its output."""
    # wrk requires connections >= threads
    threads = min(threads, concurrency)
    cmd = [
        wrk_path,
        f"-t{threads}",
        f"-c{concurrency}",
        f"-d{int(duration)}s",
        "--latency",
        "-s",
        str(lua_script),
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    text = proc.stdout + proc.stderr

    result: dict[str, float] = {}

    # 1. Parse standard wrk output
    m = re.search(r"Requests/sec:\s+([\d.]+)", text)
    if m:
        result["rps"] = float(m.group(1))

    m = re.search(r"(\d+) requests in", text)
    if m:
        result["total"] = int(m.group(1))

    # Latency Distribution: 50% 1.23ms, 75% 2.34ms, 90% 3.45ms, 99% 5.67ms
    for p in [50, 75, 90, 99]:
        pat = rf"{p}%\s+([\d.]+)([a-z]+)"
        m = re.search(pat, text)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            if unit == "us":
                val /= 1000.0
            elif unit == "s":
                val *= 1000.0
            result[f"p{p}"] = val

    # Thread Stats Latency Avg/Stdev/Max
    m = re.search(r"Latency\s+([\d.]+)([a-z]+)\s+([\d.]+)([a-z]+)\s+([\d.]+)([a-z]+)", text)
    if m:
        def _to_ms(v: float, u: str) -> float:
            if u == "us":
                return v / 1000.0
            if u == "s":
                return v * 1000.0
            return v
        result["latency_avg"] = _to_ms(float(m.group(1)), m.group(2))
        result["latency_stdev"] = _to_ms(float(m.group(3)), m.group(4))
        result["latency_max"] = _to_ms(float(m.group(5)), m.group(6))

    # 2. Parse lua summary block (more precise)
    in_summary = False
    for line in text.splitlines():
        if "---BENCH_SUMMARY---" in line:
            in_summary = True
            continue
        if "---END_SUMMARY---" in line:
            in_summary = False
            continue
        if in_summary and "=" in line:
            key, val_str = line.split("=", 1)
            try:
                result[key] = float(val_str) if "." in val_str else int(val_str)
            except ValueError:
                pass

    return result


def plot_results(rows: list[dict], output_dir: Path) -> None:
    """Generate comparison charts."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed, skipping plots")
        return

    # Group by workers for throughput vs concurrency
    workers_set = sorted({r["workers"] for r in rows})

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    ax_rps, ax_lat = axes
    for w in workers_set:
        subset = [r for r in rows if r["workers"] == w]
        concs = [r["concurrency"] for r in subset]
        ax_rps.plot(concs, [r["lite_rps"] for r in subset], "o-", label=f"lite-server w={w}")
        ax_rps.plot(concs, [r["core_rps"] for r in subset], "^-", label=f"lite-server-core w={w}")
        ax_rps.plot(concs, [r["lit_rps"] for r in subset], "s--", label=f"LitServe w={w}")

        ax_lat.plot(concs, [r["lite_p99"] for r in subset], "o-", label=f"lite-server w={w}")
        ax_lat.plot(concs, [r["core_p99"] for r in subset], "^-", label=f"lite-server-core w={w}")
        ax_lat.plot(concs, [r["lit_p99"] for r in subset], "s--", label=f"LitServe w={w}")

    ax_rps.set_xlabel("Concurrency")
    ax_rps.set_ylabel("Throughput (req/s)")
    ax_rps.set_title("Throughput vs Concurrency")
    ax_rps.set_xscale("log", base=2)
    ax_rps.legend()
    ax_rps.grid(True, alpha=0.3)

    ax_lat.set_xlabel("Concurrency")
    ax_lat.set_ylabel("p99 Latency (ms)")
    ax_lat.set_title("p99 Latency vs Concurrency")
    ax_lat.set_xscale("log", base=2)
    ax_lat.legend()
    ax_lat.grid(True, alpha=0.3)

    fig.tight_layout()
    plot_path = output_dir / "comparison.png"
    fig.savefig(plot_path, dpi=150)
    print(f"Plot saved to {plot_path}")

    # Worker scaling plot (fixed concurrency = max concurrency)
    max_conc = max(r["concurrency"] for r in rows)
    scale_rows = [r for r in rows if r["concurrency"] == max_conc]
    if len(scale_rows) > 1:
        fig2, ax2 = plt.subplots(figsize=(7, 5))
        ws = [r["workers"] for r in scale_rows]
        ax2.plot(ws, [r["lite_rps"] for r in scale_rows], "o-", label="lite-server")
        ax2.plot(ws, [r["core_rps"] for r in scale_rows], "^-", label="lite-server-core")
        ax2.plot(ws, [r["lit_rps"] for r in scale_rows], "s-", label="LitServe")
        ax2.set_xlabel("Inference Workers")
        ax2.set_ylabel("Throughput (req/s)")
        ax2.set_title(f"Scaling Efficiency (concurrency={max_conc})")
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        scale_path = output_dir / "scaling.png"
        fig2.savefig(scale_path, dpi=150)
        print(f"Scaling plot saved to {scale_path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare lite-server vs LitServe performance with wrk"
    )
    parser.add_argument("--wrk", default="wrk", help="Path to wrk binary")
    parser.add_argument(
        "--duration", type=float, default=30.0, help="Duration per benchmark (seconds)"
    )
    parser.add_argument(
        "--warmup", type=float, default=2.0, help="Warmup seconds after server becomes ready"
    )
    parser.add_argument(
        "--cooldown",
        type=float,
        default=0.0,
        help="Idle seconds between consecutive server runs. Cancels the "
        "thermal/turbo ordering bias on laptops (whichever server runs "
        "first/cool wins); 15 is a reasonable starting point. 0 = off.",
    )
    parser.add_argument(
        "--workers",
        nargs="+",
        type=int,
        default=[1, 2, 4],
        help="Inference worker counts to test",
    )
    parser.add_argument(
        "--concurrency",
        nargs="+",
        type=int,
        default=[1, 4, 16, 64],
        help="Concurrent connections",
    )
    parser.add_argument(
        "--model-repo",
        default=None,
        help="Path to model repository directory (default: <script_dir>/models)",
    )
    parser.add_argument(
        "--model",
        default="sleep_1ms_model",
        help="Model name to benchmark (default: sleep_1ms_model)",
    )
    parser.add_argument(
        "--output",
        default="../results/benchmark.csv",
        help="Output CSV path (relative to scripts dir)",
    )
    parser.add_argument("--plot", action="store_true", help="Generate matplotlib charts")
    parser.add_argument(
        "--lite",
        action="store_true",
        help="Quick mode: only test highest concurrency with max workers",
    )
    args = parser.parse_args()

    scripts_dir = Path(__file__).resolve().parent

    if args.model_repo:
        model_repo = Path(args.model_repo).resolve()
    else:
        model_repo = scripts_dir.parent / "models"

    if not model_repo.exists():
        print(f"ERROR: Model repository not found: {model_repo}", file=sys.stderr)
        return 1

    lua_script = scripts_dir / "wrk_post.lua"
    output_path = (scripts_dir / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lite_url = f"http://127.0.0.1:8000/v2/models/{args.model}/infer"
    lit_url = f"http://127.0.0.1:8001/v2/models/{args.model}/infer"
    core_url = f"http://127.0.0.1:8002/v2/models/{args.model}/infer"
    lite_script = scripts_dir / "run_liteserver.py"
    lit_script = scripts_dir / "run_litserve.py"

    model_repo_arg = ["--model-repo", str(model_repo)]
    model_arg = ["--model", args.model]

    if args.lite:
        workers_list = [max(args.workers)]
        concurrency_list = [max(args.concurrency)]
    else:
        workers_list = sorted(args.workers)
        concurrency_list = sorted(args.concurrency)

    rows: list[dict] = []

    def _run_server(name: str, port: int, extra_args: list[str] | None = None) -> dict[str, float]:
        """Start one server, run wrk, return parsed metrics.  Returns empty dict on failure."""
        url = f"http://127.0.0.1:{port}/v2/models/{args.model}/infer"
        script = lite_script  # run_liteserver.py for lite-server and lite-server-core
        if name == "LitServe":
            script = lit_script
        cmd = [
            sys.executable,
            str(script),
            "--port", str(port),
            "--workers", str(workers),
            "--duration", str(args.duration + 10),
            *model_repo_arg,
            *model_arg,
        ]
        if extra_args:
            cmd.extend(extra_args)

        print(f"[{name}] Starting...")
        try:
            with _managed_popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ):
                if not wait_for_server(url, timeout=60):
                    print(f"ERROR: {name} failed to start", file=sys.stderr)
                    return {}
                time.sleep(args.warmup)
                print(f"[{name}] wrk -c{conc} -d{int(args.duration)}s ...")
                return run_wrk(args.wrk, url, conc, args.duration, lua_script)
        except Exception as exc:
            print(f"ERROR: {name} exception: {exc}", file=sys.stderr)
            return {}

    # Each entry: (name, port, extra_args). Order is rotated per cell so each
    # server occupies each temporal position equally across the matrix — this
    # cancels the monotonic thermal/turbo-decay bias (on laptops, whichever
    # server runs first/cool wins; later runs are throttled). Pair with
    # --cooldown for idle between runs.
    server_specs = [
        ("lite-server", 8000, None),
        ("LitServe", 8001, None),
        ("lite-server-core", 8002, ["--core"]),
    ]

    cell_index = 0
    for workers in workers_list:
        for conc in concurrency_list:
            banner = f"Workers={workers}, Concurrency={conc}"
            print(f"\n{'=' * 60}")
            print(banner)
            print(f"{'=' * 60}")

            order = deque(server_specs)
            order.rotate(-cell_index)  # different server goes first each cell
            cell_index += 1
            cd = f"  (cooldown {args.cooldown:.0f}s between runs)" if args.cooldown else ""
            print(f"  run order: {' -> '.join(s[0] for s in order)}{cd}")

            res: dict[str, dict[str, float]] = {}
            for name, port, extra in order:
                res[name] = _run_server(name, port, extra_args=extra)
                if args.cooldown:
                    time.sleep(args.cooldown)

            lite_res = res["lite-server"]
            lit_res = res["LitServe"]
            core_res = res["lite-server-core"]

            row = {
                "workers": workers,
                "concurrency": conc,
                "lite_rps": round(lite_res.get("rps", 0), 2),
                "lite_p99": round(lite_res.get("p99", 0), 3),
                "lite_p90": round(lite_res.get("p90", 0), 3),
                "lite_p50": round(lite_res.get("p50", 0), 3),
                "lit_rps": round(lit_res.get("rps", 0), 2),
                "lit_p99": round(lit_res.get("p99", 0), 3),
                "lit_p90": round(lit_res.get("p90", 0), 3),
                "lit_p50": round(lit_res.get("p50", 0), 3),
                "core_rps": round(core_res.get("rps", 0), 2),
                "core_p99": round(core_res.get("p99", 0), 3),
                "core_p90": round(core_res.get("p90", 0), 3),
                "core_p50": round(core_res.get("p50", 0), 3),
            }
            rows.append(row)

            speedup = (
                row["lite_rps"] / row["lit_rps"]
                if row["lit_rps"] > 0
                else float("inf")
            )
            speedup_core = (
                row["core_rps"] / row["lit_rps"]
                if row["lit_rps"] > 0
                else float("inf")
            )
            print(f"  lite-server:      {row['lite_rps']:.1f} req/s  p99={row['lite_p99']:.2f}ms")
            print(f"  lite-server-core: {row['core_rps']:.1f} req/s  p99={row['core_p99']:.2f}ms")
            print(f"  LitServe:         {row['lit_rps']:.1f} req/s  p99={row['lit_p99']:.2f}ms")
            print(f"  lite-server / LitServe      = {speedup:.2f}x")
            print(f"  lite-server-core / LitServe = {speedup_core:.2f}x")

    # Write CSV
    fieldnames = [
        "workers",
        "concurrency",
        "lite_rps",
        "lite_p50",
        "lite_p90",
        "lite_p99",
        "lit_rps",
        "lit_p50",
        "lit_p90",
        "lit_p99",
        "core_rps",
        "core_p50",
        "core_p90",
        "core_p99",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nCSV saved: {output_path}")

    if args.plot:
        plot_results(rows, output_path.parent)

    return 0


if __name__ == "__main__":
    sys.exit(main())
