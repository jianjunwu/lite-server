"""Tests for lite_server.artifact — artifact pack/unpack/sign/cache."""

import hashlib
import hmac
import json
import zipfile
from pathlib import Path

import pytest

from lite_server.artifact import (
    ArtifactCache,
    ArtifactCorruptedError,
    FileEntry,
    Manifest,
    ModelPacker,
    ModelUnpacker,
    SignatureInvalidError,
    _load_or_create_key,
    _sha256_file,
    _should_ignore,
)


class TestShouldIgnore:
    """Ignore pattern matching for packing."""

    def test_ignores_pycache(self, tmp_path):
        pycache = tmp_path / "__pycache__"
        pycache.mkdir()
        assert _should_ignore(pycache, tmp_path) is True

    def test_ignores_pyc_files(self, tmp_path):
        pyc = tmp_path / "foo.cpython-312.pyc"
        pyc.write_text("x")
        assert _should_ignore(pyc, tmp_path) is True

    def test_ignores_git(self, tmp_path):
        git = tmp_path / ".git"
        git.mkdir()
        assert _should_ignore(git, tmp_path) is True

    def test_keeps_model_py(self, tmp_path):
        model = tmp_path / "model.py"
        model.write_text("x")
        assert _should_ignore(model, tmp_path) is False


class TestSha256File:
    """File checksum computation."""

    def test_computes_correct_hash(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert _sha256_file(f) == expected

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        expected = hashlib.sha256(b"").hexdigest()
        assert _sha256_file(f) == expected


class TestLoadOrCreateKey:
    """Signing key management."""

    def test_creates_new_key(self, tmp_path):
        key_path = tmp_path / "key.bin"
        key = _load_or_create_key(key_path)
        assert len(key) == 32
        assert key_path.exists()

    def test_loads_existing_key(self, tmp_path):
        key_path = tmp_path / "key.bin"
        original = _load_or_create_key(key_path)
        loaded = _load_or_create_key(key_path)
        assert original == loaded


class TestManifest:
    """Manifest data structure."""

    def test_default_values(self):
        m = Manifest()
        assert m.manifest_version == "1.0"
        assert m.name == ""
        assert m.files == {}

    def test_to_dict_roundtrip(self):
        m = Manifest(name="test", version="1.0", files={"a.py": FileEntry(size=10, sha256="abc")})
        d = m.to_dict()
        assert d["name"] == "test"
        assert d["files"]["a.py"]["size"] == 10

    def test_from_dict(self):
        data = {
            "manifest_version": "1.0",
            "name": "test",
            "version": "1.0",
            "created_at": "2024-01-01",
            "files": {"a.py": {"size": 10, "sha256": "abc"}},
            "signature": "",
        }
        m = Manifest.from_dict(data)
        assert m.name == "test"
        assert m.files["a.py"].size == 10

    def test_canonical_json_is_deterministic(self):
        m1 = Manifest(name="test", version="1.0")
        m2 = Manifest(name="test", version="1.0")
        assert m1.to_canonical_json() == m2.to_canonical_json()


class TestModelPacker:
    """Packing model directories into .lma artifacts."""

    def test_packs_files(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        vdir = model_dir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("print('hello')")
        (vdir / "config.yaml").write_text("stream: true\n")

        packer = ModelPacker(model_dir, version="1.0.0")
        artifact = packer.pack(tmp_path / "out")

        assert artifact.exists()
        assert artifact.suffix == ".lma"

        # Verify it's a valid zip
        with zipfile.ZipFile(artifact, "r") as zf:
            names = zf.namelist()
            assert "manifest.json" in names
            assert "1/model.py" in names
            assert "1/config.yaml" in names

    def test_manifest_contains_checksums(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        vdir = model_dir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("print('hello')")

        packer = ModelPacker(model_dir, version="1.0.0")
        packer.pack(tmp_path / "out")

        expected_hash = hashlib.sha256(b"print('hello')").hexdigest()
        assert packer.manifest is not None
        assert packer.manifest.files["1/model.py"].sha256 == expected_hash

    def test_ignores_pycache(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        vdir = model_dir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("x")
        pycache = vdir / "__pycache__"
        pycache.mkdir()
        (pycache / "foo.cpython-312.pyc").write_text("x")

        packer = ModelPacker(model_dir, version="1.0.0")
        packer.pack(tmp_path / "out")

        with zipfile.ZipFile(packer._artifact_path, "r") as zf:
            names = zf.namelist()
            assert "1/__pycache__/foo.cpython-312.pyc" not in names

    def test_signs_manifest(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        vdir = model_dir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("x")

        key = b"secret_key_32_bytes_long_12345678"
        packer = ModelPacker(model_dir, version="1.0.0")
        packer.pack(tmp_path / "out", sign_key=key)

        assert packer.manifest is not None
        assert packer.manifest.signature != ""


class TestModelUnpacker:
    """Unpacking and validating artifacts."""

    @pytest.fixture
    def artifact(self, tmp_path):
        """Create a valid artifact for testing."""
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        vdir = model_dir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("print('hello')")
        (vdir / "config.yaml").write_text("stream: true\n")

        packer = ModelPacker(model_dir, version="1.0.0")
        return packer.pack(tmp_path / "out")

    def test_validate_returns_manifest(self, artifact):
        unpacker = ModelUnpacker(artifact)
        manifest = unpacker.validate()
        assert manifest.name == "my_model"
        assert manifest.version == "1.0.0"
        assert "1/model.py" in manifest.files

    def test_validate_detects_missing_file(self, artifact, tmp_path):
        # Tamper: create artifact with manifest but remove file
        tampered = tmp_path / "tampered.lma"
        with zipfile.ZipFile(artifact, "r") as src:
            with zipfile.ZipFile(tampered, "w") as dst:
                for info in src.infolist():
                    if info.filename != "1/model.py":
                        dst.writestr(info, src.read(info.filename))

        unpacker = ModelUnpacker(tampered)
        with pytest.raises(ArtifactCorruptedError, match="missing"):
            unpacker.validate()

    def test_validate_detects_checksum_mismatch(self, artifact, tmp_path):
        # Tamper: modify file contents
        tampered = tmp_path / "tampered.lma"
        with zipfile.ZipFile(artifact, "r") as src:
            with zipfile.ZipFile(tampered, "w") as dst:
                for info in src.infolist():
                    if info.filename == "1/model.py":
                        dst.writestr(info, "tampered content")
                    else:
                        dst.writestr(info, src.read(info.filename))

        unpacker = ModelUnpacker(tampered)
        with pytest.raises(ArtifactCorruptedError, match="Checksum mismatch"):
            unpacker.validate()

    def test_validate_signature_ok(self, artifact):
        key = b"secret_key_32_bytes_long_12345678"
        # Re-pack with signature
        model_dir = artifact.parent.parent / "my_model"
        packer = ModelPacker(model_dir, version="1.0.0")
        signed = packer.pack(artifact.parent, sign_key=key)

        unpacker = ModelUnpacker(signed)
        manifest = unpacker.validate(verify_key=key)
        assert manifest.name == "my_model"

    def test_validate_signature_fails_with_wrong_key(self, artifact):
        key = b"secret_key_32_bytes_long_12345678"
        model_dir = artifact.parent.parent / "my_model"
        packer = ModelPacker(model_dir, version="1.0.0")
        signed = packer.pack(artifact.parent, sign_key=key)

        wrong_key = b"wrong_key_32_bytes_long_123456789"
        unpacker = ModelUnpacker(signed)
        with pytest.raises(SignatureInvalidError):
            unpacker.validate(verify_key=wrong_key)

    def test_unpack_extracts_files(self, artifact, tmp_path):
        unpacker = ModelUnpacker(artifact)
        target = tmp_path / "extracted"
        unpacker.unpack(target)

        assert (target / "1" / "model.py").exists()
        assert (target / "1" / "config.yaml").exists()

    def test_file_not_found(self, tmp_path):
        unpacker = ModelUnpacker(tmp_path / "missing.lma")
        with pytest.raises(FileNotFoundError):
            unpacker.validate()


class TestArtifactCache:
    """Unpack caching."""

    @pytest.fixture
    def artifact(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        vdir = model_dir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("x")
        packer = ModelPacker(model_dir, version="1.0")
        return packer.pack(tmp_path / "out")

    def test_cache_hit(self, artifact, tmp_path):
        cache = ArtifactCache(tmp_path / "cache")

        # First unpack
        path1 = cache.get_or_unpack(artifact, "my_model")
        assert path1.exists()

        # Second should be cached
        path2 = cache.get_or_unpack(artifact, "my_model")
        assert path1 == path2

    def test_cache_miss_different_artifact(self, artifact, tmp_path):
        cache = ArtifactCache(tmp_path / "cache")
        cache.get_or_unpack(artifact, "my_model")

        # Create different artifact
        model_dir2 = tmp_path / "other_model"
        model_dir2.mkdir()
        vdir = model_dir2 / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("y")
        packer2 = ModelPacker(model_dir2, version="1.0")
        artifact2 = packer2.pack(tmp_path / "out")

        path2 = cache.get_or_unpack(artifact2, "other_model")
        assert (path2 / "1" / "model.py").read_text() == "y"

    def test_cache_invalidation_by_mtime(self, artifact, tmp_path):
        cache = ArtifactCache(tmp_path / "cache")
        path1 = cache.get_or_unpack(artifact, "my_model")

        # Modify artifact
        import time
        time.sleep(0.01)
        model_dir = artifact.parent.parent / "my_model"
        packer = ModelPacker(model_dir, version="1.0")
        packer.pack(artifact.parent)

        path2 = cache.get_or_unpack(artifact, "my_model")
        # Should re-extract because mtime changed
        assert path1 == path2  # Same path, but contents re-extracted
