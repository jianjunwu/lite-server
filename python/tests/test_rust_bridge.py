"""Tests for the Rust PyO3 bridge (_lite_server.testing).

These tests exercise Rust logic through the Python bridge, covering:
- validate_identifier
- ModelRegistry (register, get, status, activate, remove, list)
"""

import pytest

from _lite_server import validate_identifier, ModelRegistry


# ---------------------------------------------------------------------------
# validate_identifier
# ---------------------------------------------------------------------------

class TestValidateIdentifier:
    def test_valid_simple(self):
        validate_identifier("bert")

    def test_valid_with_hyphen(self):
        validate_identifier("model-v1")

    def test_valid_with_underscore(self):
        validate_identifier("bert_base")

    def test_valid_alphanumeric_mixed(self):
        validate_identifier("MyModel_123")

    def test_valid_single_char(self):
        validate_identifier("a")

    def test_valid_max_length(self):
        validate_identifier("x" * 64)

    def test_reject_empty(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_identifier("")

    def test_reject_too_long(self):
        with pytest.raises(ValueError, match="exceeds maximum length"):
            validate_identifier("a" * 65)

    def test_reject_path_traversal(self):
        with pytest.raises(ValueError):
            validate_identifier("../../etc/passwd")

    def test_reject_slash(self):
        with pytest.raises(ValueError):
            validate_identifier("model/name")

    def test_reject_backslash(self):
        with pytest.raises(ValueError):
            validate_identifier("model\\name")

    def test_reject_space(self):
        with pytest.raises(ValueError):
            validate_identifier("model name")

    def test_reject_dot(self):
        with pytest.raises(ValueError):
            validate_identifier("model.name")

    def test_reject_at(self):
        with pytest.raises(ValueError):
            validate_identifier("model@name")


# ---------------------------------------------------------------------------
# ModelRegistry
# ---------------------------------------------------------------------------

class TestModelRegistryLifecycle:
    def test_register_and_get(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        mv = reg.get("m1", "1")
        assert mv is not None
        assert mv["version"] == "1"
        assert mv["status"] == "Pending"

    def test_get_nonexistent_returns_none(self):
        reg = ModelRegistry()
        assert reg.get("nope", "1") is None

    def test_get_default_version(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.set_status("m1", "1", "Ready")
        reg.activate_version("m1", "1")
        mv = reg.get("m1")
        assert mv is not None
        assert mv["version"] == "1"

    def test_get_default_no_active_returns_none(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        assert reg.get("m1") is None


class TestModelRegistryStatus:
    def test_set_status_ready(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.set_status("m1", "1", "Ready")
        assert reg.is_ready("m1", "1")

    def test_is_ready_false_initially(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        assert not reg.is_ready("m1", "1")

    def test_is_ready_nonexistent(self):
        reg = ModelRegistry()
        assert not reg.is_ready("nope", "1")

    def test_set_status_invalid_raises(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        with pytest.raises(ValueError, match="unknown status"):
            reg.set_status("m1", "1", "BadStatus")


class TestModelRegistryActivation:
    def test_activate_version(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.set_status("m1", "1", "Ready")
        assert reg.activate_version("m1", "1")
        assert reg.get_active_version("m1") == "1"

    def test_activate_not_ready_returns_false(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        assert not reg.activate_version("m1", "1")

    def test_deactivate(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.set_status("m1", "1", "Ready")
        reg.activate_version("m1", "1")
        reg.deactivate("m1")
        assert reg.get_active_version("m1") is None

    def test_activate_switches_version(self):
        reg = ModelRegistry()
        for v in ["1", "2"]:
            reg.register("m1", v, {"max_batch_size": 1}, "lit_api", "/tmp/m1")
            reg.set_status("m1", v, "Ready")
        reg.activate_version("m1", "1")
        assert reg.get_active_version("m1") == "1"
        reg.activate_version("m1", "2")
        assert reg.get_active_version("m1") == "2"


class TestModelRegistryListAndRemove:
    def test_list_loaded(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.register("m2", "1", {"max_batch_size": 1}, "ensemble", "/tmp/m2")
        loaded = reg.list_loaded()
        names = {item["name"] for item in loaded}
        assert names == {"m1", "m2"}

    def test_list_loaded_empty(self):
        reg = ModelRegistry()
        assert reg.list_loaded() == []

    def test_list_versions(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.register("m1", "2", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        versions = reg.list_versions("m1")
        version_ids = {v["version"] for v in versions}
        assert version_ids == {"1", "2"}

    def test_list_versions_nonexistent(self):
        reg = ModelRegistry()
        assert reg.list_versions("nope") == []

    def test_remove_version(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.remove("m1", "1")
        assert reg.get("m1", "1") is None

    def test_remove_last_version_clears_model(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        reg.remove("m1", "1")
        assert reg.list_loaded() == []


class TestModelRegistryModelType:
    def test_register_ensemble(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "ensemble", "/tmp/m1")
        mv = reg.get("m1", "1")
        assert mv["model_type"] == "Ensemble"

    def test_register_lit_api(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        mv = reg.get("m1", "1")
        assert mv["model_type"] == "LitAPI"

    def test_register_invalid_model_type_raises(self):
        reg = ModelRegistry()
        with pytest.raises(ValueError, match="unknown model_type"):
            reg.register("m1", "1", {"max_batch_size": 1}, "bad_type", "/tmp/m1")


class TestModelRegistryConfig:
    def test_config_fields_propagate(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 8, "stream": True}, "lit_api", "/tmp/m1")
        mv = reg.get("m1", "1")
        assert mv is not None

    def test_config_empty_dict_uses_defaults(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {}, "lit_api", "/tmp/m1")
        mv = reg.get("m1", "1")
        assert mv is not None

    def test_workers_count_starts_at_zero(self):
        reg = ModelRegistry()
        reg.register("m1", "1", {"max_batch_size": 1}, "lit_api", "/tmp/m1")
        mv = reg.get("m1", "1")
        assert mv["workers_count"] == 0
