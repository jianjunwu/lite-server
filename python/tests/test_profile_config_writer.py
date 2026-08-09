"""config_writer: round-trip fidelity, fail-closed validation net, atomic
write, backup/restore (plan §2.6.1/§2.6.2)."""

import base64
import json

import pytest

from lite_server.profile.config_writer import (
    ConfigEditError,
    edit_config,
    has_stale_backup,
    read_backup,
    restore_backup,
    write_backup,
)

SAMPLE = """# model config (comments must survive)
max_batch_size: 1          # batch cap
batch_timeout: 0.0
stream: false
accelerator: cpu
devices: 1
workers_per_device: 1
"""


class TestEditConfig:
    def test_swept_key_updated_and_comments_preserved(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(SAMPLE, encoding="utf-8")
        edit_config(cfg, {"max_batch_size": 4})
        text = cfg.read_text(encoding="utf-8")
        assert "# model config (comments must survive)" in text
        assert "# batch cap" in text
        assert "max_batch_size: 4" in text

    def test_key_order_preserved(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(SAMPLE, encoding="utf-8")
        edit_config(cfg, {"max_batch_size": 4})
        keys = [line.split(":")[0] for line in cfg.read_text().splitlines() if line and not line.startswith("#") and ":" in line]
        assert keys == ["max_batch_size", "batch_timeout", "stream", "accelerator", "devices", "workers_per_device"]

    def test_nested_dotted_path_creates_section(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("model: simple\n", encoding="utf-8")
        edit_config(cfg, {"worker.max_batch_size": 8})
        import yaml

        assert yaml.safe_load(cfg.read_text())["worker"]["max_batch_size"] == 8

    def test_multiple_updates_in_one_call(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(SAMPLE, encoding="utf-8")
        edit_config(cfg, {"max_batch_size": 8, "batch_timeout": 0.005})
        import yaml

        data = yaml.safe_load(cfg.read_text())
        assert data["max_batch_size"] == 8
        assert data["batch_timeout"] == 0.005

    def test_unparseable_original_rejected_untouched(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("max_batch_size: [unclosed\n", encoding="utf-8")
        with pytest.raises(ConfigEditError, match="unparseable"):
            edit_config(cfg, {"max_batch_size": 4})
        assert cfg.read_text(encoding="utf-8") == "max_batch_size: [unclosed\n"

    def test_float_roundtrip_no_drift(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("batch_timeout: 0.005\nmax_batch_size: 2\n", encoding="utf-8")
        edit_config(cfg, {"batch_timeout": 0.020})
        import yaml

        assert yaml.safe_load(cfg.read_text())["batch_timeout"] == 0.020


class TestBackupRestore:
    def test_backup_roundtrip_byte_exact(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(SAMPLE, encoding="utf-8")
        write_backup(cfg, campaign_hash="abc123")
        assert has_stale_backup(cfg)
        meta = read_backup(cfg)
        assert meta is not None
        assert meta["meta"]["campaign_hash"] == "abc123"

        cfg.write_text("max_batch_size: 16\n", encoding="utf-8")
        restored = restore_backup(cfg)
        assert restored.decode("utf-8") == SAMPLE
        assert not has_stale_backup(cfg), "backup must be removed after restore"

    def test_restore_without_backup_rejected(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(SAMPLE, encoding="utf-8")
        with pytest.raises(ConfigEditError, match="no .* backup"):
            restore_backup(cfg)

    def test_corrupt_backup_detected_as_missing(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(SAMPLE, encoding="utf-8")
        backup = cfg.with_name(cfg.name + ".profile.backup")
        backup.write_text("not json", encoding="utf-8")
        assert read_backup(cfg) is None
        assert has_stale_backup(cfg)  # file present → still a stale residue to flag

    def test_backup_holds_original_bytes_not_roundtrip(self, tmp_path):
        """Restore must be byte-exact (§2.6.2): the backup holds raw bytes."""
        cfg = tmp_path / "config.yaml"
        raw = SAMPLE + "quoted: 'single-style'\n"
        cfg.write_text(raw, encoding="utf-8")
        write_backup(cfg, campaign_hash=None)
        payload = json.loads(cfg.with_name(cfg.name + ".profile.backup").read_text())
        assert base64.b64decode(payload["content_b64"]).decode("utf-8") == raw
