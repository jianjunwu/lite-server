#!/usr/bin/env python3
"""Performance baseline runner for lite-server (phase-3 of the 0.7.7 plan).

Runs the echo model (zero compute) at fixed concurrency tiers against the
core binary and/or the Python CLI, samples server-process-tree RSS while wrk
is running, and writes a self-contained markdown report with the exact
reproduction commands in its header.

Reuses the wrk parsing and process management from compare.py.
"""

from __future__ import annotations

import argparse
import datetime
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil

from compare import _managed_popen, run_wrk, wait_for_server

SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent.parent

ERROR_KEYS = (
    "errors_connect",
    "errors_read",
    "errors_write",
    "errors_status",
    "errors_timeout",
)


def _sample_rss_mb(root_pid: int) -> float:
    """RSS of the server process tree in MiB (0.0 if already gone)."""
    try:
        root = psutil.Process(root_pid)
    except psutil.NoSuchProcess:
        return 0.0
    total = root.memory_info().rss
    for child in root.children(recursive=True):
        try:
            total += child.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return total / (1024 * 1024)


def _rss_sampler(root_pid: int, stop: threading.Event, samples: list[float], interval: float = 2.0) -> None:
    while not stop.is_set():
        samples.append(_sample_rss_mb(root_pid))
        stop.wait(interval)


def _cpu_brand() -> str:
    if platform.system() == "Darwin":
        try:
            out = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True, check=True,
            )
            return out.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
    return platform.processor() or platform.machine()


def _git_sha() -> str:
    out = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True, cwd=PROJECT_ROOT,
    )
    return out.stdout.strip()


def _machine_line() -> str:
    ram_gb = psutil.virtual_memory().total / (1024**3)
    return (
        f"{_cpu_brand()}, {psutil.cpu_count(logical=False)}P/{psutil.cpu_count(logical=True)}L cores, "
        f"{ram_gb:.0f}GB RAM, {platform.system()} {platform.release()}"
    )


def _run_one(
    mode: str,
    port: int,
    conc: int,
    args: argparse.Namespace,
    lua_script: Path,
) -> dict[str, float]:
    """Start one server, sample RSS during a wrk run, return merged metrics."""
    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "run_liteserver.py"),
        "--port", str(port),
        "--workers", str(args.workers),
        "--duration", str(args.duration + 10),
        "--model-repo", str(SCRIPTS_DIR.parent / "models"),
        "--model", args.model,
    ]
    if mode == "core":
        cmd.append("--core")

    url = f"http://127.0.0.1:{port}/v2/models/{args.model}/infer"
    with _managed_popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as proc:
        if not wait_for_server(url, timeout=60):
            print(f"ERROR: {mode} failed to start", file=sys.stderr)
            return {}
        time.sleep(args.warmup)

        stop = threading.Event()
        samples: list[float] = []
        sampler = threading.Thread(target=_rss_sampler, args=(proc.pid, stop, samples))
        sampler.start()
        try:
            print(f"[{mode}] wrk -c{conc} -d{int(args.duration)}s ...")
            result = run_wrk(args.wrk, url, conc, args.duration, lua_script)
        finally:
            stop.set()
            sampler.join()

    result["rss_peak_mb"] = max(samples, default=0.0)
    result["errors"] = sum(int(result.get(k, 0)) for k in ERROR_KEYS)
    return result


def _render_markdown(args: argparse.Namespace, rows: list[dict]) -> str:
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M %Z").strip()
    repro = (
        "cargo build --release && uv run maturin develop --release\n"
        f"uv run python benchmarks/scripts/baseline.py"
        f" --workers {args.workers} --duration {int(args.duration)}"
        f" --concurrency {' '.join(str(c) for c in args.concurrency)}"
        f" --model {args.model}"
    )
    lines = [
        f"# Baseline {args.label}",
        "",
        f"> Generated: {stamp} | git: `{_git_sha()}` | model: `{args.model}` (echo, zero compute)",
        f"> Machine: {_machine_line()}",
        f"> workers={args.workers} | duration={int(args.duration)}s | warmup={args.warmup}s | wrk -t min(4, concurrency)",
        "> tokio threads: default (auto) | metrics off | log-level warning",
        "",
        "## Reproduce",
        "",
        "```bash",
        repro,
        "```",
        "",
        "| mode | concurrency | rps | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | lat_mean (ms) | rss_peak (MiB) | requests | errors |",
        "|------|------------|-----|----------|----------|----------|------------|---------------|----------------|----------|--------|",
    ]
    for r in rows:
        lines.append(
            f"| {r['mode']} | {r['concurrency']} | {r.get('rps', 0):.1f}"
            f" | {r.get('latency_p50_ms', 0):.3f} | {r.get('latency_p90_ms', 0):.3f}"
            f" | {r.get('latency_p99_ms', 0):.3f} | {r.get('latency_p99.9_ms', 0):.3f}"
            f" | {r.get('latency_mean_ms', 0):.3f} | {r.get('rss_peak_mb', 0):.1f}"
            f" | {int(r.get('requests_total', 0))} | {int(r.get('errors', 0))} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="lite-server performance baseline (echo model)")
    parser.add_argument("--wrk", default="wrk", help="Path to wrk binary")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--warmup", type=float, default=3.0)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 16, 64])
    parser.add_argument("--model", default="echo_model")
    parser.add_argument(
        "--modes", nargs="+", choices=["core", "cli"], default=["core", "cli"],
        help="core = lite-server-core binary; cli = Python CLI (PyO3 in-process)",
    )
    parser.add_argument("--label", default="0.7.7-pre", help="Baseline label for the report title")
    parser.add_argument("--output", default="../results/baseline-0.7.7-pre.md")
    args = parser.parse_args()

    lua_script = SCRIPTS_DIR / "wrk_post.lua"
    output_path = (SCRIPTS_DIR / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    for mode in args.modes:
        port = 8002 if mode == "core" else 8000
        for conc in sorted(args.concurrency):
            print(f"\n{'=' * 60}\nmode={mode}, concurrency={conc}\n{'=' * 60}")
            res = _run_one(mode, port, conc, args, lua_script)
            if not res:
                print(f"ERROR: run failed for mode={mode} conc={conc}", file=sys.stderr)
                return 1
            res["mode"] = mode
            res["concurrency"] = conc
            rows.append(res)
            print(
                f"  {mode}: {res.get('rps', 0):.1f} req/s"
                f"  p99={res.get('latency_p99_ms', 0):.2f}ms"
                f"  rss_peak={res.get('rss_peak_mb', 0):.1f}MiB"
            )

    output_path.write_text(_render_markdown(args, rows), encoding="utf-8")
    print(f"\nReport saved: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
