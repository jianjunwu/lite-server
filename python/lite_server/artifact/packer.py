"""Artifact packer: create .lma files from model directories."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from lite_server.artifact.manifest import FileEntry, Manifest
from lite_server.artifact.utils import _sha256_file, _should_ignore


class ModelPacker:
    """Pack a model directory into a signed .lma artifact."""

    def __init__(self, model_dir: Path | str, version: str):
        self.model_dir = Path(model_dir)
        self.version = version
        self.manifest: Optional[Manifest] = None
        self._artifact_path: Optional[Path] = None
        self._file_paths: list[tuple[str, Path]] = []

    def pack(self, output_dir: Path | str, sign_key: Optional[bytes] = None) -> Path:
        """Create the .lma artifact.

        Args:
            output_dir: Where to write the artifact.
            sign_key: Optional HMAC key. If provided, the manifest is signed.

        Returns:
            Path to the created artifact file.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        name = self.model_dir.name
        artifact_path = output_dir / f"{name}_v{self.version}.lma"

        # Collect files and checksums
        files: dict[str, FileEntry] = {}
        file_data: dict[str, bytes] = {}

        for path in sorted(self.model_dir.rglob("*")):
            if path.is_file() and not _should_ignore(path, self.model_dir):
                rel = path.relative_to(self.model_dir).as_posix()
                files[rel] = FileEntry(size=path.stat().st_size, sha256=_sha256_file(path))
                file_data[rel] = path.read_bytes()

        self.manifest = Manifest(
            name=name,
            version=self.version,
            created_at=datetime.now(timezone.utc).isoformat(),
            files=files,
        )

        # Sign before writing to zip (exclude signature field from payload)
        if sign_key:
            manifest_dict = self.manifest.to_dict()
            manifest_dict.pop("signature", None)
            canonical = json.dumps(manifest_dict, sort_keys=True, separators=(",", ":"))
            sig = hmac.new(sign_key, canonical.encode(), hashlib.sha256)
            self.manifest.signature = sig.hexdigest()

        # Write zip archive
        with zipfile.ZipFile(artifact_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json", self.manifest.to_canonical_json())
            for rel, data in sorted(file_data.items()):
                zf.writestr(rel, data)

        self._artifact_path = artifact_path
        self._file_paths = [(rel, self.model_dir / rel) for rel in file_data]
        return artifact_path
