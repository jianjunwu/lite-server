"""Process-level resource sampling (plan §2.7: CPU/RAM, generic — no GPU).

Only meaningful against a LOCAL server (preflight gate). Sampled per trial
during the measurement window (not warmup); reported as rss_max_mb /
rss_mean_mb / cpu_mean. Cross-platform via psutil (benchmark extra).
"""

from __future__ import annotations

import asyncio
import time


def sample_process_rss_mb(pid: int) -> float | None:
    """RSS of the process tree (server + workers) in MB. None on psutil errors."""
    try:
        import psutil

        p = psutil.Process(pid)
        total = p.memory_info().rss
        for child in p.children(recursive=True):
            total += child.memory_info().rss
        return total / (1024.0 * 1024.0)
    except Exception:  # noqa: BLE001 — psutil.Error, NoSuchProcess, AccessDenied...
        return None


def sample_process_cpu_percent(pid: int) -> float | None:
    try:
        import psutil

        p = psutil.Process(pid)
        total = p.cpu_percent(interval=None)
        for child in p.children(recursive=True):
            total += child.cpu_percent(interval=None)
        return total
    except Exception:  # noqa: BLE001
        return None


async def sample_until_cancelled(pid: int) -> dict:
    """Sample RSS/CPU every 0.5s until the caller cancels the task — the
    sampling window equals the measurement window (warmup excluded, because
    the benchmark's warmup runs inside the same measure call; the sampler
    covers the whole call). Reports rss_max_mb/rss_mean_mb/cpu_mean. Empty
    dict when sampling is unavailable (remote server → n/a)."""
    import psutil

    try:
        psutil.Process(pid)
    except Exception:  # noqa: BLE001
        return {}
    rss_samples: list[float] = []
    cpu_samples: list[float] = []
    first_cpu: float | None = None
    try:
        while True:
            rss = sample_process_rss_mb(pid)
            if rss is not None:
                rss_samples.append(rss)
            cpu = sample_process_cpu_percent(pid)
            if cpu is not None:
                # First psutil cpu_percent() call returns 0.0 (baseline); skip it.
                if first_cpu is None:
                    first_cpu = cpu
                else:
                    cpu_samples.append(cpu)
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        pass
    result: dict = {}
    if rss_samples:
        result["rss_max_mb"] = round(max(rss_samples), 1)
        result["rss_mean_mb"] = round(sum(rss_samples) / len(rss_samples), 1)
    if cpu_samples:
        result["cpu_mean"] = round(sum(cpu_samples) / len(cpu_samples), 1)
    return result
