"""Cryptographic helpers for artifact signing."""

from __future__ import annotations

import secrets
from pathlib import Path


def _load_or_create_key(key_path: Path) -> bytes:
    """Load signing key from disk or generate a new 32-byte key."""
    if key_path.exists():
        return bytes.fromhex(key_path.read_text().strip())
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key = secrets.token_bytes(32)
    key_path.write_text(key.hex())
    return key
