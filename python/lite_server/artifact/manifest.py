"""Artifact manifest data structures."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass
class FileEntry:
    size: int
    sha256: str


@dataclass
class Manifest:
    manifest_version: str = "1.0"
    name: str = ""
    version: str = ""
    created_at: str = ""
    files: dict[str, FileEntry] = field(default_factory=dict)
    signature: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    def to_canonical_json(self) -> str:
        """Deterministic JSON for signing (sorted keys, no whitespace)."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict) -> Manifest:
        files = {}
        for path, fdata in data.get("files", {}).items():
            files[path] = FileEntry(size=fdata.get("size", 0), sha256=fdata.get("sha256", ""))
        return cls(
            manifest_version=data.get("manifest_version", "1.0"),
            name=data.get("name", ""),
            version=data.get("version", ""),
            created_at=data.get("created_at", ""),
            files=files,
            signature=data.get("signature", ""),
        )
