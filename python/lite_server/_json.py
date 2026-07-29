"""Unified JSON entry point for hot paths: orjson when available, stdlib json
otherwise. ``dumps`` always returns ``bytes`` (orjson's native return type;
the stdlib fallback encodes).

Strictness differences vs stdlib json when orjson is active (pinned in
tests/test_json.py):
- NaN/Infinity serialize to ``null`` (stdlib emits invalid ``NaN``/``Infinity``
  literals).
- Non-str dict keys raise ``TypeError`` (stdlib coerces them to strings).
- ``loads`` raises ``orjson.JSONDecodeError``, a subclass of
  ``json.JSONDecodeError`` — existing except clauses keep working.
"""

from __future__ import annotations

import json as _stdlib_json
from typing import Any

try:
    import orjson as _orjson

    HAS_ORJSON = True
except ImportError:  # platforms without an orjson wheel
    _orjson = None
    HAS_ORJSON = False


def loads(data: bytes | bytearray | str) -> Any:
    if HAS_ORJSON:
        return _orjson.loads(data)
    return _stdlib_json.loads(data)


def dumps(obj: Any) -> bytes:
    if HAS_ORJSON:
        return _orjson.dumps(obj)
    return _stdlib_json.dumps(obj).encode()
