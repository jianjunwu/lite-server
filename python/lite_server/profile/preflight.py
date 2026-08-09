"""Preflight gates (plan §2.4): hard checks, any failure → exit 2.

- Server reachable + target model/version loaded + Admin reachable
- Server version ≥ the release carrying the batch-0 fix (reload_model disk
  re-read) — an older server reloads from the registry's stale config (§0.1),
  so profiling it measures fake data
- Exclusivity guard: liteserver_queue_depth == 0 (foreign traffic pollutes
  every trial); non-exclusive → refuse (--force overrides)
- Batching declaration state (StaticAnalyzer AST, zero execution) — the
  search-space preflight input
- continuous_batching (on-disk config.yaml) → expected ACTIVE_WORKERS = 1
- Local-server detection + process discovery (psutil; explicit --server-pid
  or look up the listener by port)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

# Minimum server version carrying the batch-0 fix (reload_model disk re-read).
# RC builds carry the same fix (0.8.4-rc0 onward), so an equal version number
# passes regardless of pre-release suffix.
PROFILE_MIN_SERVER_VERSION = (0, 8, 4)

# Default Prometheus metrics port (config.rs server.metrics_port default).
DEFAULT_METRICS_PORT = 8002


def default_metrics_url(admin_url: str) -> str:
    """Derive the /metrics URL from the Admin URL (same host, default metrics
    port). Overridable via --metrics-url (plan decision point 4)."""
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(admin_url)
    return urlunsplit((parts.scheme, f"{parts.hostname}:{DEFAULT_METRICS_PORT}", "", "", ""))

_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$")
# GaugeVec export is labeled: liteserver_queue_depth{model="m",version="1"} 0
_QUEUE_DEPTH_RE = re.compile(r"^liteserver_queue_depth(?:\{[^}]*\})?\s+(-?\d+(?:\.\d+)?)\s*$", re.M)
_ACTIVE_WORKERS_RE = re.compile(r"^ACTIVE_WORKERS\{[^}]*\}\s+(\d+)\s*$", re.M)


class PreflightError(RuntimeError):
    """Preflight failure (→ exit 2). Message is user-facing, with fix hints."""


def parse_version(version: str) -> tuple[int, int, int]:
    m = _VERSION_RE.match(version.strip())
    if not m:
        raise PreflightError(f"cannot parse server version {version!r} — /info response odd?")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def version_gate_ok(version: str) -> bool:
    """Server version ≥ PROFILE_MIN_SERVER_VERSION; equal version number with an
    rc suffix also passes (carries the fix)."""
    return parse_version(version) >= PROFILE_MIN_SERVER_VERSION


def parse_queue_depth(metrics_text: str) -> float | None:
    m = _QUEUE_DEPTH_RE.search(metrics_text)
    return float(m.group(1)) if m else None


def parse_active_workers(metrics_text: str) -> int | None:
    m = _ACTIVE_WORKERS_RE.search(metrics_text)
    return int(m.group(1)) if m else None


@dataclass
class PreflightResult:
    version: str
    model: str
    resolved_version: str
    model_loaded: bool
    exclusive: bool
    batching_declared: bool | None  # None = AST undeterminable → conservatively drop batch keys
    batching_detection: str  # "declared" | "not_declared" | "unknown"
    continuous_batching: bool
    local: bool
    server_pid: int | None
    expected_workers: int
    config: dict[str, Any] = field(default_factory=dict)


async def run_preflight(
    *,
    admin_url: str,
    model: str,
    version: str,
    repo_path: Path,
    client: httpx.AsyncClient,
    force: bool = False,
    server_pid: int | None = None,
    metrics_url: str | None = None,
) -> PreflightResult:
    """Run all hard gates; any failure → PreflightError.

    force=True downgrades the exclusivity guard to a warning (still noted in
    the result); every other gate stays hard.

    metrics_url: /metrics endpoint (plan decision point 4). Defaults to the
    Admin host on the default metrics port; override for remote servers.
    """
    base = admin_url.rstrip("/")

    # 1. Server reachable + /info version
    try:
        info = (await client.get(f"{base}/info", timeout=10.0)).json()
    except (httpx.HTTPError, ValueError) as e:
        raise PreflightError(
            f"Admin endpoint {base} unreachable ({e}). Confirm the server is up "
            f"and the Admin face is reachable (loopback by default)"
        ) from e
    server_version = str(info.get("version", ""))
    if not version_gate_ok(server_version):
        raise PreflightError(
            f"server version {server_version} too old: the reload_model disk "
            f"re-read fix (profile prerequisite) requires "
            f"≥ {'.'.join(map(str, PROFILE_MIN_SERVER_VERSION))}. Old servers "
            f"reload from the registry's stale config — profiling them is fake data"
        )

    # 2. Target model/version loaded (Admin ready check)
    loaded = False
    try:
        resp = await client.get(
            f"{base}/v2/models/{model}/versions/{version}/ready", timeout=10.0
        )
        loaded = resp.status_code == 200
    except httpx.HTTPError:
        loaded = False
    if not loaded:
        raise PreflightError(
            f"model {model} version {version} not loaded (or Admin check failed). "
            f"Load it first: POST {base}/v2/models/{model}/versions/{version}/load"
        )

    # 3. Exclusivity guard: liteserver_queue_depth == 0. The gauge is a
    #    labeled GaugeVec — it only appears in /metrics after the first queued
    #    request creates the label set. Absence therefore means "never queued"
    #    = exclusive; only an unreadable /metrics endpoint is fail-closed.
    metrics_base = (metrics_url or default_metrics_url(base)).rstrip("/")
    exclusive = False
    try:
        metrics_text = (await client.get(f"{metrics_base}/metrics", timeout=10.0)).text
        depth = parse_queue_depth(metrics_text)
        exclusive = depth is None or depth == 0
    except httpx.HTTPError:
        exclusive = False
    if not exclusive and not force:
        raise PreflightError(
            "server not exclusive: liteserver_queue_depth is non-zero or "
            "/metrics is unreadable. Foreign traffic pollutes every trial; "
            "drain requests and retry, or --force to accept explicitly"
        )

    # 4. On-disk config.yaml (declaration state + continuous_batching +
    #    expected worker count)
    config_path = repo_path / model / version / "config.yaml"
    config: dict[str, Any] = {}
    if config_path.exists():
        import yaml as pyyaml

        try:
            config = pyyaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except Exception as e:  # noqa: BLE001
            raise PreflightError(
                f"target config.yaml unparseable: {e} — preflight refuses to "
                f"overwrite a broken config"
            ) from e

    continuous_batching = bool(config.get("continuous_batching", False))
    devices = _devices_count(config)
    workers_per_device = int(config.get("workers_per_device") or 1)
    expected_workers = 1 if continuous_batching else devices * workers_per_device

    # 5. Batching declaration state (StaticAnalyzer AST, zero execution)
    from lite_server.analyzer.static import StaticAnalyzer

    batching_declared: bool | None = None
    try:
        report = StaticAnalyzer(str(repo_path)).analyze_model(model, version)
        batching = report.methods.get("batching", {})
        batch_status = batching.get("batch")
        unbatch_status = batching.get("unbatch")
        if batch_status == "implemented" and unbatch_status == "implemented":
            batching_declared = True
        elif batch_status == "default" or unbatch_status == "default":
            batching_declared = False
        else:
            batching_declared = None
    except (FileNotFoundError, ValueError):
        batching_declared = None  # AST undeterminable → conservative
    detection = (
        "declared" if batching_declared is True
        else "not_declared" if batching_declared is False
        else "unknown"
    )

    # 6. Local-server detection + process discovery (psutil; --server-pid
    #    explicit or listener lookup by port)
    local = False
    resolved_pid = server_pid
    if resolved_pid is None:
        resolved_pid = _find_listener_pid(base)
        local = resolved_pid is not None
    else:
        local = True

    return PreflightResult(
        version=server_version,
        model=model,
        resolved_version=version,
        model_loaded=True,
        exclusive=exclusive,
        batching_declared=batching_declared,
        batching_detection=detection,
        continuous_batching=continuous_batching,
        local=local,
        server_pid=resolved_pid,
        expected_workers=expected_workers,
        config=config,
    )


def _devices_count(config: dict[str, Any]) -> int:
    devices = config.get("devices")
    if isinstance(devices, int):
        return devices
    if isinstance(devices, str):
        return 1  # "auto"
    return 1


def _find_listener_pid(base: str) -> int | None:
    """Look up the process listening on the Admin URL port (local psutil)."""
    try:
        import psutil
        from urllib.parse import urlsplit
    except ImportError:
        return None
    try:
        port = urlsplit(base).port
        host = urlsplit(base).hostname or "127.0.0.1"
    except ValueError:
        return None
    if port is None:
        return None
    try:
        for conn in psutil.net_connections(kind="tcp"):
            if conn.laddr.port == port and conn.status == "LISTEN":
                return conn.pid
    except (psutil.Error, OSError):
        return None
    return None
