"""Cryptographic helpers for artifact signing."""

from __future__ import annotations

import os
import secrets
from pathlib import Path


def _load_or_create_key(key_path: Path) -> bytes:
    """Load signing key from disk or generate a new 32-byte key."""
    if key_path.exists():
        _restrict(key_path)
        return bytes.fromhex(key_path.read_text().strip())
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key = secrets.token_bytes(32)
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(key.hex())
    _restrict(key_path)
    return key


def _restrict(key_path: Path) -> None:
    """Owner-only key material. Without this the file gets whatever the
    process umask allows (0644 by default), and on a shared host any local
    user can read the HMAC key and forge "signed" artifacts — which is the
    whole point of signing them. Applied on load too: a key written before
    this guard existed is exactly as forgeable as one written without it."""
    os.chmod(key_path, 0o600)
