"""Artifact unpacker: validate and extract .lma files."""

from __future__ import annotations

import hashlib
import hmac
import json
import zipfile
from pathlib import Path
from typing import Callable, Optional, Tuple

from lite_server.artifact.manifest import Manifest


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
                        f"Checksum mismatch for {rel_path}: "
                        f"expected {entry.sha256[:16]}..., got {actual[:16]}..."
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

    def unpack(
        self,
        target_dir: Path | str,
        filter_func: Optional[Callable] = None,
        prepend_name: bool = True,
    ) -> Path:
        """Extract artifact contents to target_dir.

        Args:
            target_dir: Destination directory.
            filter_func: Optional callable(zipinfo) -> bool to filter extracted files.
            prepend_name: If True, prepend manifest.name as top-level directory.

        Returns:
            Path to the extracted model directory.
        """
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        if prepend_name:
            manifest = self._ensure_manifest()
            target_dir = target_dir / manifest.name
            target_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(self.artifact_path, "r") as zf:
            if filter_func:
                for info in zf.infolist():
                    if filter_func(info):
                        zf.extract(info, target_dir)
            else:
                zf.extractall(target_dir)

        return target_dir

    def unpack_and_validate(
        self,
        target_dir: Path | str,
        verify_key: Optional[bytes] = None,
        prepend_name: bool = True,
    ) -> Tuple[Manifest, Path]:
        """Extract and validate checksums in a single pass.

        Streams each file from the zip, computes SHA256 on the fly,
        validates against the manifest, and writes to disk — all in one
        read of the zip archive.

        Args:
            target_dir: Destination directory.
            verify_key: Optional HMAC key for signature verification.
            prepend_name: If True, prepend manifest.name as top-level directory.

        Returns:
            Tuple of (validated Manifest, path to extracted model directory).

        Raises:
            ArtifactCorruptedError: If a file is missing or checksum mismatches.
            SignatureInvalidError: If signature verification fails.
        """
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        if not self.artifact_path.exists():
            raise FileNotFoundError(f"Artifact not found: {self.artifact_path}")

        with zipfile.ZipFile(self.artifact_path, "r") as zf:
            manifest_raw = zf.read("manifest.json").decode("utf-8")
            manifest = Manifest.from_dict(json.loads(manifest_raw))
            self.manifest = manifest

            if prepend_name:
                extract_dir = target_dir / manifest.name
            else:
                extract_dir = target_dir
            extract_dir.mkdir(parents=True, exist_ok=True)

            for rel_path, entry in manifest.files.items():
                try:
                    info = zf.getinfo(rel_path)
                except KeyError:
                    raise ArtifactCorruptedError(f"File missing in artifact: {rel_path}")

                # Single pass: stream-read, hash, and write
                h = hashlib.sha256()
                out_path = extract_dir / rel_path
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, open(out_path, "wb") as dst:
                    while chunk := src.read(65536):
                        h.update(chunk)
                        dst.write(chunk)

                actual = h.hexdigest()
                if actual != entry.sha256:
                    raise ArtifactCorruptedError(
                        f"Checksum mismatch for {rel_path}: "
                        f"expected {entry.sha256[:16]}..., got {actual[:16]}..."
                    )

            # Verify signature
            if verify_key and manifest.signature:
                manifest_dict = json.loads(manifest_raw)
                manifest_dict.pop("signature", None)
                canonical = json.dumps(manifest_dict, sort_keys=True, separators=(",", ":"))
                expected = hmac.new(verify_key, canonical.encode(), hashlib.sha256).hexdigest()
                if not hmac.compare_digest(expected, manifest.signature):
                    raise SignatureInvalidError("Artifact signature verification failed")

        return manifest, extract_dir

    def _ensure_manifest(self) -> Manifest:
        """Return cached manifest, loading it from the artifact if needed."""
        if self.manifest is None:
            self.validate()
        return self.manifest  # type: ignore[return-value]
