"""Artifact cache: avoid re-extracting unchanged artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from lite_server.artifact.unpacker import ModelUnpacker


class ArtifactCache:
    """Cache extracted artifacts keyed by artifact path and mtime."""

    def __init__(self, cache_dir: Path | str):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._index_path = self.cache_dir / ".cache_index.json"
        self._index: dict[str, str] = {}
        self._load_index()

    def _load_index(self) -> None:
        if self._index_path.exists():
            try:
                self._index = json.loads(self._index_path.read_text())
            except (json.JSONDecodeError, OSError):
                self._index = {}

    def _save_index(self) -> None:
        self._index_path.write_text(json.dumps(self._index, indent=2))

    def _cache_key(self, artifact_path: Path) -> str:
        mtime = str(artifact_path.stat().st_mtime)
        return f"{artifact_path.resolve()}:{mtime}"

    def _cached_path(self, model_name: str) -> Path:
        return self.cache_dir / model_name

    def get_or_unpack(self, artifact_path: Path | str, model_name: str) -> Path:
        """Return cached extraction path, or extract if missing/outdated."""
        artifact_path = Path(artifact_path)
        cache_key = self._cache_key(artifact_path)
        cached_path = self._cached_path(model_name)

        if cache_key in self._index:
            stored_path = Path(self._index[cache_key])
            if stored_path.exists():
                return stored_path

        # Cache miss or stale: extract
        cached_path.mkdir(parents=True, exist_ok=True)
        unpacker = ModelUnpacker(artifact_path)
        unpacker.unpack(cached_path, prepend_name=False)

        self._index[cache_key] = str(cached_path)
        self._save_index()
        return cached_path

    def invalidate(self, artifact_path: Optional[Path | str] = None) -> None:
        """Remove cache entries. If artifact_path is None, clear all."""
        if artifact_path is None:
            self._index = {}
            for p in self.cache_dir.iterdir():
                if p.is_dir():
                    import shutil
                    shutil.rmtree(p)
        else:
            artifact_path = Path(artifact_path)
            cache_key = self._cache_key(artifact_path)
            cached_path = self._index.pop(cache_key, None)
            if cached_path is not None:
                import shutil
                p = Path(cached_path)
                if p.exists():
                    shutil.rmtree(p)
        self._save_index()
