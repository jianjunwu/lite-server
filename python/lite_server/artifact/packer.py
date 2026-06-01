"""Artifact packer: create .lma files from model directories."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from lite_server.artifact.manifest import FileEntry, Manifest
from lite_server.artifact.utils import _sha256_file, _should_ignore

_VERSION_RE = re.compile(r"^v?\d+(\.\d+){0,2}$")
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_version(version: str) -> str:
    """Validate and normalize version string.

    Accepts: 1, 1.0, 1.0.0, v1, v1.0, v1.0.0
    Returns version with 'v' prefix stripped.
    """
    if not _VERSION_RE.match(version):
        raise ValueError(
            f"invalid version format: '{version}'. "
            "Expected: N, N.N, or N.N.N (e.g., 1, 1.0, 1.0.0)"
        )
    return version.lstrip("v")


def _validate_name(name: str) -> str:
    """Validate model name. Only alphanumeric, hyphens, underscores allowed."""
    if not name:
        raise ValueError("model name cannot be empty")
    if not _NAME_RE.match(name):
        raise ValueError(
            f"invalid model name: '{name}'. "
            "Only alphanumeric, hyphens, and underscores are allowed"
        )
    return name


def _resolve_version_dir(model_dir: Path, version: str) -> Path:
    """Find the version subdirectory under model_dir.

    Tries exact match first (e.g., '1'), then v-prefixed (e.g., 'v1').
    """
    # version is already normalized (v stripped), so try both
    candidate = model_dir / version
    if candidate.is_dir():
        return candidate
    candidate_v = model_dir / f"v{version}"
    if candidate_v.is_dir():
        return candidate_v
    raise ValueError(
        f"version '{version}' not found under {model_dir}. "
        f"Expected directory '{version}' or 'v{version}'"
    )


def _is_version_dir_name(name: str) -> bool:
    """Check if a directory name looks like a version directory (e.g., '1', 'v2')."""
    return bool(re.match(r"^v?\d+$", name))


# Public files that should always be included from the model root
_PUBLIC_FILE_NAMES = {
    "requirements.txt",
    "README.md",
    "readme.md",
}


class ModelPacker:
    """Pack a model directory into a signed .lma artifact.

    Args:
        model_dir: Path to the model directory (model root or version subdir).
        version: Version string (e.g., '1', '1.0', 'v1.0.0').
        name: Optional model name override. Auto-inferred if not provided.
    """

    def __init__(
        self,
        model_dir: Path | str,
        version: str,
        name: Optional[str] = None,
    ):
        self.model_dir = Path(model_dir)
        self.version = _validate_version(version)
        self.manifest: Optional[Manifest] = None
        self._artifact_path: Optional[Path] = None
        self._file_paths: list[tuple[str, Path]] = []

        # Resolve model name
        if name is not None:
            self.name = _validate_name(name)
        elif _is_version_dir_name(self.model_dir.name):
            # model_dir looks like a version subdir (e.g., my_model/1)
            self.name = _validate_name(self.model_dir.parent.name)
        else:
            self.name = _validate_name(self.model_dir.name)

    def pack(self, output_dir: Path | str, sign_key: Optional[bytes] = None) -> Path:
        """Create the .lma artifact.

        Packs only the files from the matching version subdirectory,
        plus public files (requirements.txt, README.md) from model root.

        Args:
            output_dir: Where to write the artifact.
            sign_key: Optional HMAC key. If provided, the manifest is signed.

        Returns:
            Path to the created artifact file.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        artifact_path = output_dir / f"{self.name}_v{self.version}.lma"

        # Determine if model_dir is the model root or a version subdir
        if _is_version_dir_name(self.model_dir.name):
            # model_dir is already a version subdir (e.g., my_model/1)
            version_dir = self.model_dir
            model_root = self.model_dir.parent
        else:
            # model_dir is the model root — resolve version subdir
            version_dir = _resolve_version_dir(self.model_dir, self.version)
            model_root = self.model_dir

        # Collect files from version directory
        files: dict[str, FileEntry] = {}
        rel_paths: list[str] = []

        for path in sorted(version_dir.rglob("*")):
            if path.is_file() and not _should_ignore(path, version_dir):
                rel = path.relative_to(version_dir).as_posix()
                version_dir_name = version_dir.name
                arcname = f"{version_dir_name}/{rel}"
                files[arcname] = FileEntry(size=path.stat().st_size, sha256=_sha256_file(path))
                rel_paths.append(arcname)

        # Include public files from model root
        for pub_name in _PUBLIC_FILE_NAMES:
            pub_path = model_root / pub_name
            if pub_path.is_file():
                files[pub_name] = FileEntry(
                    size=pub_path.stat().st_size,
                    sha256=_sha256_file(pub_path),
                )
                rel_paths.append(pub_name)

        self.manifest = Manifest(
            name=self.name,
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

        # Write zip archive — read files from disk via zf.write()
        with zipfile.ZipFile(artifact_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json", self.manifest.to_canonical_json())
            for arcname in sorted(rel_paths):
                if "/" in arcname:
                    # Version file: reconstruct source path
                    version_dir_name = version_dir.name
                    rel_in_version = arcname[len(version_dir_name) + 1:]
                    src = version_dir / rel_in_version
                else:
                    # Public file from model root
                    src = model_root / arcname
                zf.write(src, arcname=arcname)

        self._artifact_path = artifact_path
        self._file_paths = [(a, model_root) for a in rel_paths]
        return artifact_path
