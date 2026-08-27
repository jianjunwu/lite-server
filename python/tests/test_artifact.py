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
from lite_server.artifact.packer import _validate_version, _validate_name, _resolve_version_dir


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


class TestValidateVersion:
    """Version format validation."""

    def test_accepts_bare_number(self):
        assert _validate_version("1") == "1"
        assert _validate_version("123") == "123"

    def test_accepts_semver(self):
        assert _validate_version("1.0") == "1.0"
        assert _validate_version("1.0.0") == "1.0.0"
        assert _validate_version("12.3.4") == "12.3.4"

    def test_accepts_v_prefix_and_strips(self):
        assert _validate_version("v1") == "1"
        assert _validate_version("v1.0") == "1.0"
        assert _validate_version("v1.0.0") == "1.0.0"

    def test_rejects_invalid_formats(self):
        for bad in ["abc", "1.0.0.0", "1_0", "", "v", "1-0", "1.0-beta"]:
            with pytest.raises(ValueError, match="invalid version"):
                _validate_version(bad)


class TestValidateName:
    """Model name validation."""

    def test_accepts_valid_names(self):
        assert _validate_name("my_model") == "my_model"
        assert _validate_name("model-v2") == "model-v2"
        assert _validate_name("Model123") == "Model123"

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="name cannot be empty"):
            _validate_name("")

    def test_rejects_special_chars(self):
        for bad in ["my model", "model/v2", "model@1", "a.b"]:
            with pytest.raises(ValueError, match="invalid model name"):
                _validate_name(bad)


class TestResolveVersionDir:
    """Auto-detect version subdirectory from model dir."""

    def test_finds_numeric_dir(self, tmp_path):
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("x")

        result = _resolve_version_dir(model_dir, "1")
        assert result == vdir

    def test_finds_v_prefixed_dir(self, tmp_path):
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "v1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("x")

        result = _resolve_version_dir(model_dir, "1")
        assert result == vdir

    def test_raises_when_version_dir_missing(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()

        with pytest.raises(ValueError, match="version.*not found"):
            _resolve_version_dir(model_dir, "99")


class TestModelPacker:
    """Packing model directories into .lma artifacts."""

    def test_packs_version_dir_only(self, tmp_path):
        """Pack model_dir with version — should only include that version's files."""
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        v1 = model_dir / "1"
        v1.mkdir()
        (v1 / "model.py").write_text("v1 code")
        v2 = model_dir / "2"
        v2.mkdir()
        (v2 / "model.py").write_text("v2 code")

        packer = ModelPacker(model_dir, version="1")
        artifact = packer.pack(tmp_path / "out")

        with zipfile.ZipFile(artifact, "r") as zf:
            names = zf.namelist()
            assert "1/model.py" in names
            assert "2/model.py" not in names

    def test_packs_version_dir_with_v_prefix(self, tmp_path):
        """Pack when version dirs use v-prefix naming."""
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        v1 = model_dir / "v1"
        v1.mkdir()
        (v1 / "model.py").write_text("v1 code")

        packer = ModelPacker(model_dir, version="1")
        artifact = packer.pack(tmp_path / "out")

        with zipfile.ZipFile(artifact, "r") as zf:
            names = zf.namelist()
            assert "v1/model.py" in names

    def test_includes_public_files_from_model_root(self, tmp_path):
        """Public files like requirements.txt at model root should be included."""
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        (model_dir / "requirements.txt").write_text("torch>=2.0\n")
        v1 = model_dir / "1"
        v1.mkdir()
        (v1 / "model.py").write_text("v1 code")

        packer = ModelPacker(model_dir, version="1")
        artifact = packer.pack(tmp_path / "out")

        with zipfile.ZipFile(artifact, "r") as zf:
            names = zf.namelist()
            assert "requirements.txt" in names
            assert "1/model.py" in names

    def test_infers_name_from_model_dir(self, tmp_path):
        """When model_dir is the model root, name = model_dir.name."""
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("x")

        packer = ModelPacker(model_dir, version="1")
        assert packer.name == "my_model"

    def test_infers_name_from_parent_when_version_dir(self, tmp_path):
        """When model_dir is a version dir (leaf matches version), use parent name."""
        model_dir = tmp_path / "my_model" / "1"
        model_dir.mkdir(parents=True)
        (model_dir / "model.py").write_text("x")

        packer = ModelPacker(model_dir, version="1")
        assert packer.name == "my_model"

    def test_explicit_name_overrides_inference(self, tmp_path):
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("x")

        packer = ModelPacker(model_dir, version="1", name="custom")
        assert packer.name == "custom"
        artifact = packer.pack(tmp_path / "out")
        assert artifact.name == "custom_v1.lma"

    def test_artifact_name_includes_model_name(self, tmp_path):
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("x")

        packer = ModelPacker(model_dir, version="1")
        artifact = packer.pack(tmp_path / "out")
        assert artifact.name == "my_model_v1.lma"

    def test_manifest_name_is_model_name(self, tmp_path):
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("x")

        packer = ModelPacker(model_dir, version="1")
        packer.pack(tmp_path / "out")
        assert packer.manifest.name == "my_model"
        assert packer.manifest.version == "1"

    def test_rejects_invalid_version(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        with pytest.raises(ValueError, match="invalid version"):
            ModelPacker(model_dir, version="abc")

    def test_rejects_invalid_name(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        with pytest.raises(ValueError, match="invalid model name"):
            ModelPacker(model_dir, version="1", name="bad name")

    def test_manifest_contains_checksums(self, tmp_path):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        vdir = model_dir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text("print('hello')")

        packer = ModelPacker(model_dir, version="1")
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

        packer = ModelPacker(model_dir, version="1")
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
        packer = ModelPacker(model_dir, version="1")
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

        packer = ModelPacker(model_dir, version="1")
        return packer.pack(tmp_path / "out")

    def test_validate_returns_manifest(self, artifact):
        unpacker = ModelUnpacker(artifact)
        manifest = unpacker.validate()
        assert manifest.name == "my_model"
        assert manifest.version == "1"
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
        packer = ModelPacker(model_dir, version="1")
        signed = packer.pack(artifact.parent, sign_key=key)

        unpacker = ModelUnpacker(signed)
        manifest = unpacker.validate(verify_key=key)
        assert manifest.name == "my_model"

    def test_validate_signature_fails_with_wrong_key(self, artifact):
        key = b"secret_key_32_bytes_long_12345678"
        model_dir = artifact.parent.parent / "my_model"
        packer = ModelPacker(model_dir, version="1")
        signed = packer.pack(artifact.parent, sign_key=key)

        wrong_key = b"wrong_key_32_bytes_long_123456789"
        unpacker = ModelUnpacker(signed)
        with pytest.raises(SignatureInvalidError):
            unpacker.validate(verify_key=wrong_key)

    def test_unpack_prepends_model_name_dir(self, artifact, tmp_path):
        """Unpack should create target/model_name/ structure."""
        unpacker = ModelUnpacker(artifact)
        target = tmp_path / "extracted"
        model_dir = unpacker.unpack(target)

        # Returns the model directory path
        assert model_dir == target / "my_model"
        assert (target / "my_model" / "1" / "model.py").exists()
        assert (target / "my_model" / "1" / "config.yaml").exists()

    def test_unpack_and_validate_prepends_model_name_dir(self, artifact, tmp_path):
        """Single-pass unpack should also prepend model name dir."""
        unpacker = ModelUnpacker(artifact)
        target = tmp_path / "extracted"
        manifest, model_dir = unpacker.unpack_and_validate(target)

        assert manifest.name == "my_model"
        assert model_dir == target / "my_model"
        assert (target / "my_model" / "1" / "model.py").read_text() == "print('hello')"
        assert (target / "my_model" / "1" / "config.yaml").read_text() == "stream: true\n"

    def test_unpack_and_validate_detects_corruption(self, artifact, tmp_path):
        """Single-pass: corrupted file must be detected during extraction."""
        tampered = tmp_path / "tampered.lma"
        with zipfile.ZipFile(artifact, "r") as src:
            with zipfile.ZipFile(tampered, "w") as dst:
                for info in src.infolist():
                    if info.filename == "1/model.py":
                        dst.writestr(info, "tampered content")
                    else:
                        dst.writestr(info, src.read(info.filename))

        unpacker = ModelUnpacker(tampered)
        target = tmp_path / "extracted"
        with pytest.raises(ArtifactCorruptedError, match="Checksum mismatch"):
            unpacker.unpack_and_validate(target)

    def test_unpack_skip_name_dir(self, artifact, tmp_path):
        """Unpack with prepend_name=False extracts without model name dir."""
        unpacker = ModelUnpacker(artifact)
        target = tmp_path / "extracted"
        model_dir = unpacker.unpack(target, prepend_name=False)

        assert model_dir == target
        assert (target / "1" / "model.py").exists()

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
        packer = ModelPacker(model_dir, version="1")
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
        packer2 = ModelPacker(model_dir2, version="1")
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
        packer = ModelPacker(model_dir, version="1")
        packer.pack(artifact.parent)

        path2 = cache.get_or_unpack(artifact, "my_model")
        # Should re-extract because mtime changed
        assert path1 == path2  # Same path, but contents re-extracted

    def test_invalidate_removes_cached_directory(self, artifact, tmp_path):
        cache = ArtifactCache(tmp_path / "cache")
        cached_path = cache.get_or_unpack(artifact, "my_model")
        assert cached_path.exists()
        assert cache._index  # index should have an entry

        cache.invalidate(artifact)

        # Index entry should be removed
        cache_key = cache._cache_key(artifact)
        assert cache_key not in cache._index
        # Cached directory should be deleted from disk
        assert not cached_path.exists()

    def test_invalidate_unknown_artifact_is_noop(self, artifact, tmp_path):
        cache = ArtifactCache(tmp_path / "cache")
        cache.get_or_unpack(artifact, "my_model")

        unknown = tmp_path / "unknown.lma"
        unknown.write_text("nope")

        # Should not raise
        cache.invalidate(unknown)

    def test_invalidate_all_clears_everything(self, artifact, tmp_path):
        cache = ArtifactCache(tmp_path / "cache")
        path1 = cache.get_or_unpack(artifact, "my_model")
        assert path1.exists()

        cache.invalidate()

        assert not cache._index
        assert not path1.exists()


class TestUnpackAndValidatePathSafety:
    """Audit: manifest-controlled ``rel_path`` must never escape the target dir."""

    def test_rel_path_with_dotdot_does_not_escape_target_dir(self, tmp_path):
        content = b"pwned"
        artifact = tmp_path / "evil.lma"
        target = tmp_path / "target"
        target.mkdir()
        manifest = {
            "manifest_version": "1.0",
            "name": "m",
            "version": "1",
            "files": {
                "../escaped.txt": {"size": len(content),
                                   "sha256": hashlib.sha256(content).hexdigest()},
            },
        }
        with zipfile.ZipFile(artifact, "w") as zf:
            zf.writestr("manifest.json", json.dumps(manifest))
            zf.writestr("../escaped.txt", content)

        unpacker = ModelUnpacker(artifact)
        with pytest.raises(ArtifactCorruptedError):
            unpacker.unpack_and_validate(target, prepend_name=False)

        assert not (tmp_path / "escaped.txt").exists(), \
            "a crafted manifest must not write outside the target directory"


class TestArtifactCacheAudit:
    """Audit: cache re-extraction must not serve stale files from a previous artifact."""

    def test_reextract_into_same_dir_does_not_mix_stale_files(self, tmp_path):
        import os as _os

        cache = ArtifactCache(tmp_path / "cache")
        a = tmp_path / "a.lma"
        with zipfile.ZipFile(a, "w") as zf:
            zf.writestr("a.txt", "A")
        b = tmp_path / "b.lma"
        with zipfile.ZipFile(b, "w") as zf:
            zf.writestr("b.txt", "B")
        _os.utime(b, (b.stat().st_atime, b.stat().st_mtime + 5))

        cache.get_or_unpack(a, "m")
        cache.get_or_unpack(b, "m")

        extracted = sorted(p.name for p in (tmp_path / "cache" / "m").iterdir())
        assert extracted == ["b.txt"], \
            "the cache dir must not serve files from a previous artifact"
