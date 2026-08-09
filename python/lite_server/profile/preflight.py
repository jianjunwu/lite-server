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
PROFILE_MIN_SERVER_VERSION = (0, 8, 4)

# Default Prometheus metrics port (config.rs server.metrics_port default).
DEFAULT_METRICS_PORT = 8002


def default_metrics_url(admin_url: str) -> str:
    """Derive the /metrics URL from the Admin URL (same host, default metrics
    port). Overridable via --metrics-url (plan decision point 4)."""
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(admin_url)
    return urlunsplit((parts.scheme, f"{parts.hostname}:{DEFAULT_METRICS_PORT}", "", "", ""))

_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:([-+]).*)?$")
# GaugeVec exports are labeled: liteserver_queue_depth{model="m",version="1"} 0
_QUEUE_DEPTH_RE = re.compile(r"^liteserver_queue_depth(?:\{[^}]*\})?\s+(-?\d+(?:\.\d+)?)\s*$", re.M)
_IN_FLIGHT_RE = re.compile(r"^liteserver_in_flight_requests(?:\{[^}]*\})?\s+(-?\d+(?:\.\d+)?)\s*$", re.M)
# Export names from src/metrics/prometheus.rs (:57 / :286 / :721).
_ACTIVE_WORKERS_RE = re.compile(r"^liteserver_active_workers\{([^}]*)\}\s+(\d+)\s*$", re.M)
_WORKER_SATURATION_RE = re.compile(r"^liteserver_worker_saturation\{([^}]*)\}\s+(-?\d+(?:\.\d+)?)\s*$", re.M)
_BATCH_SIZE_BUCKET_RE = re.compile(r"^liteserver_batch_size_bucket\{([^}]*)\}\s+(-?\d+(?:\.\d+)?)\s*$", re.M)


def _labels(blob: str) -> dict[str, str]:
    return dict(re.findall(r'(\w+)="([^"]*)"', blob))


class PreflightError(RuntimeError):
    """Preflight failure (→ exit 2). Message is user-facing, with fix hints."""


def parse_version(version: str) -> tuple[int, int, int]:
    m = _VERSION_RE.match(version.strip())
    if not m:
        raise PreflightError(f"cannot parse server version {version!r} — /info response odd?")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def version_gate_ok(version: str) -> bool:
    """Server version ≥ PROFILE_MIN_SERVER_VERSION. The tagged v0.8.4-rc0
    predates the batch-0 fix (c3b5c30) and is refused — profiling it
    measures fake data (plan §0.1). rc1+ / the final release / build
    metadata (+) are built on the fixed tree and pass."""
    parsed = parse_version(version)
    if parsed != PROFILE_MIN_SERVER_VERSION:
        return parsed > PROFILE_MIN_SERVER_VERSION
    suffix = version.strip()[len(".".join(map(str, PROFILE_MIN_SERVER_VERSION))):]
    if not suffix or suffix.startswith("+"):
        return True
    m = re.fullmatch(r"-rc(\d+)", suffix)
    return m is not None and int(m.group(1)) >= 1


def parse_queue_depth(metrics_text: str) -> float | None:
    """Max across ALL labeled series — exclusivity means no queued requests
    anywhere; a foreign model's queue pollutes every trial just the same."""
    values = [float(m.group(1)) for m in _QUEUE_DEPTH_RE.finditer(metrics_text)]
    return max(values) if values else None


def parse_in_flight(metrics_text: str) -> float | None:
    """Max in-flight requests across all series (plan §2.4: exclusivity also
    requires no in-flight requests — a long foreign stream holds zero queue
    depth but pollutes every trial, and reload would cut it off)."""
    values = [float(m.group(1)) for m in _IN_FLIGHT_RE.finditer(metrics_text)]
    return max(values) if values else None


def _match_labels(blob: str, model: str | None, version: str | None) -> bool:
    labels = _labels(blob)
    if model is not None and labels.get("model") != model:
        return False
    if version is not None and labels.get("version") != version:
        return False
    return True


def parse_active_workers(metrics_text: str, model: str | None = None,
                         version: str | None = None) -> int | None:
    """liteserver_active_workers for the TARGET model/version — the readiness
    gate (§2.7) must not compare against another model's series. Without
    label arguments: first series (single-model convenience)."""
    for m in _ACTIVE_WORKERS_RE.finditer(metrics_text):
        if model is None and version is None:
            return int(m.group(2))
        if _match_labels(m.group(1), model, version):
            return int(m.group(2))
    return None


def parse_worker_saturation(metrics_text: str, model: str | None = None,
                            version: str | None = None) -> float | None:
    """liteserver_worker_saturation for the target model/version (plan §2.7)."""
    for m in _WORKER_SATURATION_RE.finditer(metrics_text):
        if model is None and version is None:
            return float(m.group(2))
        if _match_labels(m.group(1), model, version):
            return float(m.group(2))
    return None


def parse_batch_size_median(metrics_text: str, model: str | None = None,
                            version: str | None = None) -> float | None:
    """Estimated median of the liteserver_batch_size histogram for the target
    model/version: the first bucket whose cumulative count reaches half of
    the total (plan §2.7 — the only indirect evidence max_batch_size landed)."""
    buckets: list[tuple[float, float]] = []
    for m in _BATCH_SIZE_BUCKET_RE.finditer(metrics_text):
        labels = _labels(m.group(1))
        if model is not None and labels.get("model") != model:
            continue
        if version is not None and labels.get("version") != version:
            continue
        le_raw = labels.get("le")
        if le_raw is None:
            continue
        le = float("inf") if le_raw == "+Inf" else float(le_raw)
        buckets.append((le, float(m.group(2))))
    if not buckets:
        return None
    buckets.sort(key=lambda b: b[0])
    total = buckets[-1][1]  # the +Inf (or largest) bucket holds the total
    if total <= 0:
        return None
    half = total / 2.0
    for le, cum in buckets:
        if cum >= half:
            return le
    return buckets[-1][0]


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

    # 3. Exclusivity guard: liteserver_queue_depth == 0 AND no in-flight
    #    requests (plan §2.4). Both gauges are labeled GaugeVecs — they only
    #    appear after the first request creates the label set, so absence
    #    means "never busy" = exclusive; only an unreadable /metrics endpoint
    #    is fail-closed.
    metrics_base = (metrics_url or default_metrics_url(base)).rstrip("/")
    exclusive = False
    try:
        metrics_text = (await client.get(f"{metrics_base}/metrics", timeout=10.0)).text
        depth = parse_queue_depth(metrics_text)
        in_flight = parse_in_flight(metrics_text)
        exclusive = (depth is None or depth == 0) and \
                    (in_flight is None or in_flight == 0)
    except httpx.HTTPError:
        exclusive = False
    if not exclusive and not force:
        raise PreflightError(
            "server not exclusive: liteserver_queue_depth or "
            "liteserver_in_flight_requests is non-zero (or /metrics is "
            "unreadable). Foreign traffic pollutes every trial; "
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
