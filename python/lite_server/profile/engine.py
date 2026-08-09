"""ProfileEngine: the config-search main loop (plan §2.6).

Per config point (nested-grid outer layer):
  0. Precondition: in-flight requests drained (benchmark's bounded drain +
     the exclusivity guard)
  1. Atomically write the new config.yaml (surgical replacement of swept
     keys only; tmp + os.replace)
  2. Admin ReloadModel — the server re-reads the on-disk config (batch-0
     validate-then-swap); a bad config is refused before unload, old workers
     keep serving
  3. Poll Ready (/v2/models/:m/versions/:v/ready + ACTIVE_WORKERS == expected)
  4. applied-config indirect check → warmup → inner concurrency sweep (zero
     reloads)
  5. Next config point; after all points, restore the original config.yaml →
     ReloadModel → wait Ready. A failed restore is a profile failure (exit 2)
     — the user's repo is never left on a modified config.

Trial failure → recorded failed (with reason) → back to baseline → continue;
consecutive failures ≥ max_trial_failures → circuit breaker aborts (restore
original config then exit 2). SIGINT/SIGTERM → best-effort restore.
"""

from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

from lite_server.profile.checkpoint import TrialRecord
from lite_server.profile.config_writer import (
    ConfigEditError,
    edit_config,
    has_stale_backup,
    restore_backup,
    write_backup,
)
from lite_server.profile.grid import ConfigPoint, GridSpec
from lite_server.profile.preflight import (
    PreflightResult,
    default_metrics_url,
    parse_active_workers,
    parse_queue_depth,
)

READY_POLL_SECS = 0.5


class ProfileAbort(RuntimeError):
    """Circuit breaker / user interrupt → already best-effort restored, exit 2."""


class ProfileFailure(RuntimeError):
    """Whole-run failure (e.g. restore failed) → exit 2."""


# Benchmark runner: async (concurrency) -> metrics dict
MeasureFn = Callable[[int], Awaitable[dict]]


@dataclass
class EstimateInputs:
    reload_secs: float = 10.0  # per config point: write + ReloadModel + wait Ready
    warmup_secs: float = 5.0
    duration_secs: float = 30.0


@dataclass
class ProfileResult:
    trials: list[TrialRecord] = field(default_factory=list)
    campaign: str | None = None
    aborted: bool = False


class ProfileEngine:
    def __init__(
        self,
        *,
        config_path: Path,
        model: str,
        version: str,
        admin_url: str,
        grid: GridSpec,
        preflight: PreflightResult,
        measure: MeasureFn,
        client: httpx.AsyncClient,
        reload_timeout: float = 120.0,
        max_trial_failures: int = 3,
        campaign: str | None = None,
        metrics_url: str | None = None,
        resume_done: set[str] | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        self.model = model
        self.version = version
        self.admin_url = admin_url.rstrip("/")
        self.grid = grid
        self.preflight = preflight
        self.measure = measure
        self.client = client
        self.reload_timeout = reload_timeout
        self.max_trial_failures = max_trial_failures
        self.campaign = campaign
        self.metrics_base = (metrics_url or default_metrics_url(admin_url)).rstrip("/")
        self.resume_done = resume_done or set()
        self._abort = False
        self._orig_sigint: Any = None
        self._orig_sigterm: Any = None
        self._last_point_failure: str | None = None
        self._trial_index = 0
        self._edited = False  # whether this run actually rewrote config.yaml (restore gate)

    # ---- entry point ---------------------------------------------------------

    async def run(self) -> ProfileResult:
        points = self._points_to_run()
        result = ProfileResult(campaign=self.campaign)
        if has_stale_backup(self.config_path):
            raise ProfileFailure(
                "stale .profile.backup found: a previous profile run was "
                "SIGKILLed. Verify config.yaml is still the original, then "
                "remove the backup file manually, or run with --recover to "
                "restore it byte-exact and retry"
            )

        self._install_signal_handlers()
        backup_path = write_backup(self.config_path, self.campaign)
        consecutive_failures = 0
        try:
            for point in points:
                if self._abort:
                    result.aborted = True
                    break
                if point.values:
                    ok = await self._apply_config_point(point)
                else:
                    ok = True  # baseline: current config, zero reloads
                if not ok:
                    # §2.6.3: failed trials stay in the checkpoint (visible, auditable)
                    reason = self._last_point_failure or "config point apply failed"
                    for c in self.grid.concurrency:
                        result.trials.append(
                            self._failed_trial(point, c, reason)
                        )
                    consecutive_failures += 1
                    await self._restore_baseline()
                    if consecutive_failures >= self.max_trial_failures:
                        raise ProfileAbort(
                            f"{consecutive_failures} consecutive config points "
                            f"failed (≥ --max-trial-failures "
                            f"{self.max_trial_failures}), circuit breaker"
                        )
                    continue
                consecutive_failures = 0
                result.trials.extend(await self._sweep_concurrency(point))
        except ProfileAbort as e:
            result.aborted = True
            raise
        finally:
            if self._edited:
                try:
                    await self._restore_baseline()
                except Exception as e:  # noqa: BLE001
                    raise ProfileFailure(f"restore failed: {e}") from e
            backup_path.unlink(missing_ok=True)
            self._restore_signal_handlers()
        return result

    # ---- §2.6 sequence -------------------------------------------------------

    def _points_with_baseline(self) -> list[ConfigPoint]:
        baseline = ConfigPoint(values={})
        rest = [p for p in self.grid.config_points() if p.values]
        return [baseline, *rest]

    def _points_to_run(self) -> list[ConfigPoint]:
        """Resume: drop config points whose (point, concurrency) trials are
        already recorded in the checkpoint (§2.10)."""
        if not self.resume_done:
            return self._points_with_baseline()
        import json as _json

        out = []
        for point in self._points_with_baseline():
            key = _json.dumps(point.values, sort_keys=True)
            done = all(f"{key}@{c}" in self.resume_done for c in self.grid.concurrency)
            if not done:
                out.append(point)
        return out

    async def _apply_config_point(self, point: ConfigPoint) -> bool:
        """Steps 1-3: write config → ReloadModel → wait Ready. True on success."""
        try:
            edit_config(self.config_path, point.values)
            self._edited = True
        except ConfigEditError as e:
            self._log_point_failure(point, f"config.yaml rewrite failed: {e}")
            return False
        try:
            resp = await self.client.post(
                f"{self.admin_url}/v2/models/{self.model}/versions/{self.version}/reload",
                timeout=30.0,
            )
            if resp.status_code != 200:
                self._log_point_failure(
                    point, f"ReloadModel HTTP {resp.status_code}: {resp.text[:200]}"
                )
                return False
        except httpx.HTTPError as e:
            self._log_point_failure(point, f"ReloadModel request failed: {e}")
            return False
        if not await self._wait_ready(self._expected_workers_for(point)):
            self._log_point_failure(point, f"Ready timeout ({self.reload_timeout}s)")
            return False
        return True

    async def _wait_ready(self, expected_workers: int | None = None) -> bool:
        """Poll version ready + ACTIVE_WORKERS == expected (§2.7 indirect check)."""
        expected_workers = expected_workers if expected_workers is not None \
            else self.preflight.expected_workers
        deadline = asyncio.get_running_loop().time() + self.reload_timeout
        while asyncio.get_running_loop().time() < deadline:
            if self._abort:
                return False
            ready = False
            try:
                resp = await self.client.get(
                    f"{self.admin_url}/v2/models/{self.model}/versions/{self.version}/ready",
                    timeout=10.0,
                )
                ready = resp.status_code == 200
            except httpx.HTTPError:
                ready = False
            if ready:
                workers_ok = await self._check_active_workers(expected_workers)
                if workers_ok is not False:  # True, or metrics unreadable (skip)
                    return True
            await asyncio.sleep(READY_POLL_SECS)
        return False

    async def _check_active_workers(self, expected_workers: int | None = None) -> bool | None:
        """None = /metrics unreadable (cannot verify; mechanism is guaranteed
        by the version gate, treat as pass)."""
        try:
            text = (await self.client.get(f"{self.metrics_base}/metrics", timeout=10.0)).text
        except httpx.HTTPError:
            return None
        workers = parse_active_workers(text)
        if workers is None:
            return None
        return workers == (expected_workers if expected_workers is not None
                           else self.preflight.expected_workers)

    def _expected_workers_for(self, point: ConfigPoint) -> int:
        """ACTIVE_WORKERS expectation for a config point (§2.7): the point's
        own workers_per_device × devices — sweeping workers_per_device is the
        whole point, the readiness gate must follow it."""
        if self.preflight.continuous_batching:
            return 1
        wpd = point.values.get("workers_per_device")
        if wpd is None:
            return self.preflight.expected_workers
        devices = self.preflight.config.get("devices")
        return (devices if isinstance(devices, int) else 1) * int(wpd)

    async def _sweep_concurrency(self, point: ConfigPoint) -> list[TrialRecord]:
        records: list[TrialRecord] = []
        for c in self.grid.concurrency:
            if self._abort:
                break
            record = TrialRecord(
                index=self._trial_index,
                config_point=point.to_dict(),
                concurrency=c,
                status="ok",
                batching_declared=self.preflight.batching_declared,
                continuous_batching=self.preflight.continuous_batching,
                token_count_basis=None,
            )
            self._trial_index += 1
            try:
                record.metrics = await self.measure(c)
                record.server_metrics = await self._snapshot_server_metrics()
            except Exception as e:  # noqa: BLE001 — single-trial failure → failed
                record.status = "failed"
                record.reason = f"benchmark failed: {e}"
            records.append(record)
        return records

    async def _snapshot_server_metrics(self) -> dict | None:
        try:
            text = (await self.client.get(f"{self.metrics_base}/metrics", timeout=10.0)).text
        except httpx.HTTPError:
            return None
        return {
            "active_workers": parse_active_workers(text),
            "queue_depth": parse_queue_depth(text),
        }

    async def restore(self) -> None:
        """Public restore-to-baseline (used by quick search / winner retest)."""
        await self._restore_baseline()

    async def measure_point(self, point: ConfigPoint) -> list[TrialRecord]:
        """Apply one config point + run its concurrency sweep, leaving the
        config applied (caller controls restore — used by quick search)."""
        if point.values:
            ok = await self._apply_config_point(point)
            if not ok:
                reason = self._last_point_failure or "config point apply failed"
                return [self._failed_trial(point, c, reason)
                        for c in self.grid.concurrency]
        return await self._sweep_concurrency(point)

    # ---- restore & interrupt -------------------------------------------------

    async def _restore_baseline(self) -> None:
        """Restore the original config.yaml (byte-exact) → ReloadModel → wait Ready."""
        if has_stale_backup(self.config_path):
            restore_backup(self.config_path)
            # Once restored the cluster is back at baseline, so the finally
            # cleanup skips automatically — avoids a double worker rebuild on
            # the circuit-breaker abort path.
            self._edited = False
            # The restore deleted the backup, but later config points in this
            # run may still rewrite the file (failure → baseline → continue).
            # Re-persist the backup so subsequent edits keep an anchor.
            write_backup(self.config_path, self.campaign)
        try:
            await self.client.post(
                f"{self.admin_url}/v2/models/{self.model}/versions/{self.version}/reload",
                timeout=30.0,
            )
        except httpx.HTTPError as e:
            raise ProfileFailure(f"restore ReloadModel failed: {e}") from e
        if not await self._wait_ready():
            raise ProfileFailure("restore: waiting for Ready timed out — server state not restored")

    def _install_signal_handlers(self) -> None:
        try:
            self._orig_sigint = signal.signal(signal.SIGINT, self._on_signal)
            self._orig_sigterm = signal.signal(signal.SIGTERM, self._on_signal)
        except ValueError:
            pass  # not the main thread: fall back on the finally restore path

    def _restore_signal_handlers(self) -> None:
        if self._orig_sigint is not None:
            signal.signal(signal.SIGINT, self._orig_sigint)
        if self._orig_sigterm is not None:
            signal.signal(signal.SIGTERM, self._orig_sigterm)

    def _on_signal(self, signum: int, _frame: Any) -> None:
        self._abort = True  # main loop checks at point boundaries; finally restores

    def _log_point_failure(self, point: ConfigPoint, reason: str) -> None:
        import logging

        self._last_point_failure = reason
        logging.getLogger("lite_server.profile").warning(
            "config point %s failed: %s", point.to_dict(), reason
        )

    def _failed_trial(self, point: ConfigPoint, concurrency: int, reason: str) -> TrialRecord:
        record = TrialRecord(
            index=self._trial_index,
            config_point=point.to_dict(),
            concurrency=concurrency,
            status="failed",
            reason=reason,
            batching_declared=self.preflight.batching_declared,
            continuous_batching=self.preflight.continuous_batching,
        )
        self._trial_index += 1
        return record

    # ---- dry-run (zero side effects) -----------------------------------------

    def dry_run_report(self, estimate: EstimateInputs | None = None) -> dict[str, Any]:
        """Preflight conclusions + effective nested grid + estimated wall clock
        (plan §2.9 --dry-run)."""
        estimate = estimate or EstimateInputs()
        points = self._points_with_baseline()
        n_points = len(points)
        n_conc = len(self.grid.concurrency)
        total_trials = n_points * n_conc
        per_point = (
            estimate.reload_secs + estimate.warmup_secs
            + estimate.duration_secs * n_conc
        )
        return {
            "preflight": {
                "server_version": self.preflight.version,
                "model": self.model,
                "version": self.version,
                "batching_detection": self.preflight.batching_detection,
                "continuous_batching": self.preflight.continuous_batching,
                "expected_workers": self.preflight.expected_workers,
                "exclusive": self.preflight.exclusive,
                "local_server": self.preflight.local,
                "server_pid": self.preflight.server_pid,
            },
            "grid": {
                "config_points": [p.to_dict() for p in points],
                "concurrency": list(self.grid.concurrency),
                "total_trials": total_trials,
            },
            "estimate_wallclock_secs": round(n_points * per_point, 1),
            "campaign": self.campaign,
        }
