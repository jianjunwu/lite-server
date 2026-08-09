"""Atomic config.yaml rewrite and backup/restore (plan §2.6.1/§2.6.2).

Write: ruamel.yaml round-trip preserves comments/key order/quote style; the
dump is re-parsed with pyyaml (core dependency) with swept-key assertions and
a full-dict drift check — any failure restores the original file and raises
(fail-closed). Persist via tmp + os.replace (a half-written file is a parse
failure whose blast radius leaks outward, never allowed).

Restore always writes the backup's ORIGINAL bytes back (byte-exact), never
round-trip.
"""

from __future__ import annotations

import base64
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

BACKUP_SUFFIX = ".profile.backup"


class ConfigEditError(Exception):
    """config.yaml rewrite failed (original file restored or untouched)."""


def _roundtrip_yaml() -> YAML:
    y = YAML(typ="rt")
    y.preserve_quotes = True
    y.width = 4096  # do not reflow keys because of line length
    return y


def _set_nested(mapping: Any, key_path: list[str], value: Any) -> None:
    """Set a value along a dotted path (e.g. ["worker", "max_batch_size"]),
    creating CommentedMap sections as needed."""
    cur = mapping
    for part in key_path[:-1]:
        if not isinstance(cur, dict):
            raise ConfigEditError(f"path {'.'.join(key_path)} has a non-mapping intermediate")
        if part not in cur:
            cur[part] = CommentedMap()
        cur = cur[part]
    cur[key_path[-1]] = value


def render_config(config_path: Path, updates: dict[str, Any]) -> str:
    """Render the would-be config.yaml text for `updates` (round-trip +
    validation net, fail-closed), without touching the file. Used for the
    recommendation diff and by edit_config."""
    config_path = Path(config_path)
    original_text = config_path.read_bytes().decode("utf-8")

    y = _roundtrip_yaml()
    try:
        data = y.load(original_text)
    except Exception as e:  # noqa: BLE001 — unparseable original, refuse to rewrite
        raise ConfigEditError(f"config.yaml unparseable, refusing to rewrite: {e}") from e
    if data is None:
        data = CommentedMap()

    expected = _parse_with_pyyaml(original_text) or {}
    for dotted, value in updates.items():
        _set_nested(data, dotted.split("."), value)
        _set_nested(expected, dotted.split("."), value)

    import io

    buf = io.StringIO()
    y.dump(data, buf)
    dumped = buf.getvalue()

    # Validation net (fail-closed): pyyaml re-parse + full-dict comparison.
    parsed = _parse_with_pyyaml(dumped)
    if parsed is None:
        raise ConfigEditError("post-write pyyaml re-parse failed — refusing to persist")
    if parsed != expected:
        drift = _diff_keys(parsed, expected)
        raise ConfigEditError(f"post-write full-dict comparison drifted: {drift}")
    return dumped


def edit_config(config_path: Path, updates: dict[str, Any]) -> None:
    """Atomically rewrite the swept keys in config.yaml; on failure the
    original file is kept/restored and ConfigEditError is raised.

    updates: dotted key → value, e.g. {"max_batch_size": 4} or
    {"worker.max_batch_size": 4}.
    """
    config_path = Path(config_path)
    rendered = render_config(config_path, updates)

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=config_path.parent, suffix=".tmp", delete=False, encoding="utf-8"
        ) as tf:
            tf.write(rendered)
            tmp_path = Path(tf.name)
        os.replace(tmp_path, config_path)
        tmp_path = None
    except Exception as e:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        raise ConfigEditError(f"config.yaml rewrite failed: {e}") from e


def _parse_with_pyyaml(text: str) -> dict[str, Any] | None:
    import yaml as pyyaml

    try:
        return pyyaml.safe_load(text)
    except Exception:  # noqa: BLE001
        return None


def _diff_keys(parsed: dict, expected: dict) -> str:
    keys = sorted(set(parsed) | set(expected))
    return "; ".join(
        f"{k}: {parsed.get(k)!r} != {expected.get(k)!r}"
        for k in keys
        if parsed.get(k) != expected.get(k)
    )


# ---- backup & restore (§2.6.1) ------------------------------------------------


def backup_path_for(config_path: Path) -> Path:
    return config_path.with_name(config_path.name + BACKUP_SUFFIX)


def write_backup(config_path: Path, campaign_hash: str | None) -> Path:
    """Persist original bytes + campaign meta; enables recovery after SIGKILL."""
    config_path = Path(config_path)
    backup = backup_path_for(config_path)
    meta = {
        "campaign_hash": campaign_hash,
        "original_file": config_path.name,
    }
    payload = {
        "meta": meta,
        "content_b64": base64.b64encode(config_path.read_bytes()).decode("ascii"),
    }
    backup.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return backup


def read_backup(config_path: Path) -> dict[str, Any] | None:
    backup = backup_path_for(config_path)
    if not backup.exists():
        return None
    try:
        payload = json.loads(backup.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if "content_b64" not in payload:
        return None
    return payload


def has_stale_backup(config_path: Path) -> bool:
    return backup_path_for(config_path).exists()


def restore_backup(config_path: Path) -> bytes:
    """Restore the original bytes byte-exact and remove the backup file.
    No backup → ConfigEditError."""
    payload = read_backup(config_path)
    if payload is None:
        raise ConfigEditError(
            f"no {BACKUP_SUFFIX} backup available — refusing to rewrite config.yaml"
        )
    original = base64.b64decode(payload["content_b64"])
    _atomic_write_bytes(Path(config_path), original)
    backup_path_for(config_path).unlink(missing_ok=True)
    return original


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
