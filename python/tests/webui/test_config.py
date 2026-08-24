"""Port of ui/server/test/config.test.ts."""

import json
import os
import stat

import pytest

from lite_server.webui.config import InstanceStore, load_instances


def write_yaml(tmp_path, content: str) -> str:
    path = tmp_path / "instances.yaml"
    path.write_text(content, encoding="utf-8")
    return str(path)


def test_should_load_instances_from_yaml_file(tmp_path):
    path = write_yaml(tmp_path, """
instances:
  - id: local
    name: Local dev
    base_url: http://localhost:8000
  - id: prod
    name: Prod
    base_url: http://10.0.0.11:8000
    admin_key: secret
""")
    instances = load_instances(path, {})
    assert len(instances) == 2
    assert next(i for i in instances if i.id == "prod").admin_key == "secret"


def test_should_resolve_admin_key_from_env_when_admin_key_env_set(tmp_path):
    path = write_yaml(tmp_path, """
instances:
  - id: prod
    name: Prod
    base_url: http://10.0.0.11:8000
    admin_key_env: PROD_KEY
""")
    instances = load_instances(path, {"PROD_KEY": "from-env"})
    assert instances[0].admin_key == "from-env"


def test_should_mark_env_injected_instances_readonly():
    instances = load_instances(
        "/nonexistent/instances.yaml",
        {"LITE_UI_INSTANCES": json.dumps([{"id": "env1", "name": "Env", "base_url": "http://h:1"}])},
    )
    assert instances[0].readonly is True
    assert instances[0].base_url == "http://h:1"


def test_should_throw_on_invalid_id(tmp_path):
    path = write_yaml(tmp_path, """
instances:
  - id: "Bad ID!"
    name: X
    base_url: http://localhost:8000
""")
    with pytest.raises(Exception, match="(?i)id"):
        load_instances(path, {})


def test_should_throw_on_invalid_base_url(tmp_path):
    path = write_yaml(tmp_path, """
instances:
  - id: ok
    name: X
    base_url: ftp://nope
""")
    with pytest.raises(Exception, match="(?i)base_url"):
        load_instances(path, {})


def test_should_throw_on_duplicate_id(tmp_path):
    path = write_yaml(tmp_path, """
instances:
  - { id: a, name: A, base_url: "http://h:1" }
  - { id: a, name: B, base_url: "http://h:2" }
""")
    with pytest.raises(Exception, match="(?i)duplicate"):
        load_instances(path, {})


def test_should_return_empty_registry_when_file_missing():
    assert load_instances("/nonexistent/x.yaml", {}) == []


def test_should_strip_trailing_slash_from_base_url(tmp_path):
    path = write_yaml(tmp_path, """
instances:
  - { id: a, name: A, base_url: "http://h:8000/" }
""")
    instances = load_instances(path, {})
    assert instances[0].base_url == "http://h:8000"


def test_should_persist_instances_yaml_with_owner_only_permissions(tmp_path):
    # The file can hold plaintext admin keys; it must not be world-readable.
    path = write_yaml(tmp_path, "instances: []\n")
    store = InstanceStore(path, {})
    store.create({"id": "a", "name": "A", "base_url": "http://h:1", "admin_key": "secret"})
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600
