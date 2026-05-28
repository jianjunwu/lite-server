"""Artifact packaging with manifest, SHA256 checksums, and HMAC signing.

Format: .lma (zip archive)
  - manifest.json  : file listing with SHA256 checksums + optional signature
  - <model files>  : original directory contents

Reference: light_server artifact system (simplified for lite-server).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Files / directories to ignore when packing
IGNORE_NAMES = {
    "__pycache__",
    ".git",
    ".DS_Store",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "env",
    "ENV",
    ".idea",
    ".vscode",
}


def _should_ignore(path: Path, root: Path) -> bool:
    """Return True if path should be excluded from the artifact."""
    rel = path.relative_to(root)
    for part in rel.parts:
        if part in IGNORE_NAMES or part.endswith(".pyc") or part.endswith(".pyo"):
            return True
    return False


def _sha256_file(path: Path) -> str:
    """Compute SHA256 of a file in streaming mode (memory-efficient)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def _load_or_create_key(key_path: Path) -> bytes:
    """Load signing key from disk or generate a new 32-byte key."""
    if key_path.exists():
        return bytes.fromhex(key_path.read_text().strip())
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key = secrets.token_bytes(32)
    key_path.write_text(key.hex())
    return key


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Packer
# ---------------------------------------------------------------------------


class ModelPacker:
    """Pack a model directory into a signed .lma artifact."""

    def __init__(self, model_dir: Path | str, version: str):
        self.model_dir = Path(model_dir)
        self.version = version
        self.manifest: Optional[Manifest] = None

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

        return artifact_path


# ---------------------------------------------------------------------------
# Unpacker
# ---------------------------------------------------------------------------


class ArtifactCorruptedError(ValueError):
    """Raised when artifact checksum validation fails."""


class SignatureInvalidError(ValueError):
    """Raised when signature verification fails."""


class ModelUnpacker:
    """Unpack and validate a .lma artifact."""

    def __init__(self, artifact_path: Path | str):
        self.artifact_path = Path(artifact_path)
        self.manifest: Optional[Manifest] = None

    def validate(self, verify_key: Optional[bytes] = None) -> Manifest:
        """Validate manifest, file checksums, and optional signature.

        Returns the parsed Manifest on success.
        Raises ArtifactCorruptedError or SignatureInvalidError on failure.
        """
        if not self.artifact_path.exists():
            raise FileNotFoundError(f"Artifact not found: {self.artifact_path}")

        with zipfile.ZipFile(self.artifact_path, "r") as zf:
            manifest_raw = zf.read("manifest.json").decode("utf-8")
            manifest = Manifest.from_dict(json.loads(manifest_raw))
            self.manifest = manifest

            # Verify file checksums
            for rel_path, entry in manifest.files.items():
                try:
                    content = zf.read(rel_path)
                except KeyError:
                    raise ArtifactCorruptedError(f"File missing in artifact: {rel_path}")
                actual = hashlib.sha256(content).hexdigest()
                if actual != entry.sha256:
                    raise ArtifactCorruptedError(
                        f"Checksum mismatch for {rel_path}: expected {entry.sha256[:16]}..., got {actual[:16]}..."
                    )

            # Verify signature (recompute HMAC over manifest without signature field)
            if verify_key and manifest.signature:
                manifest_dict = json.loads(manifest_raw)
                manifest_dict.pop("signature", None)
                canonical = json.dumps(manifest_dict, sort_keys=True, separators=(",", ":"))
                expected = hmac.new(verify_key, canonical.encode(), hashlib.sha256).hexdigest()
                if not hmac.compare_digest(expected, manifest.signature):
                    raise SignatureInvalidError("Artifact signature verification failed")

        return manifest

    def unpack(self, target_dir: Path | str, filter_func=None) -> Path:
        """Extract artifact contents to target_dir.

        Args:
            target_dir: Destination directory.
            filter_func: Optional callable(zipinfo) -> bool to filter extracted files.

        Returns:
            Path to the extracted model directory.
        """
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(self.artifact_path, "r") as zf:
            if filter_func:
                for info in zf.infolist():
                    if filter_func(info):
                        zf.extract(info, target_dir)
            else:
                zf.extractall(target_dir)

        return target_dir
