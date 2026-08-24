"""Port of ui/server/test/instances-crud.test.ts."""

import json

from lite_server.webui.app import build_app
from lite_server.webui.config import InstanceStore

SEED = """
instances:
  - id: local
    name: Local
    base_url: http://localhost:8000
"""


def seed(tmp_path, content: str = SEED) -> str:
    path = tmp_path / "instances.yaml"
    path.write_text(content, encoding="utf-8")
    return str(path)


def test_should_create_instance_and_persist_to_yaml(tmp_path, client_factory):
    file = seed(tmp_path)
    client = client_factory(build_app(InstanceStore(file, {})))

    res = client.post("/api/instances", json={"id": "gpu-1", "name": "GPU 1", "base_url": "http://10.0.0.2:8000"})
    assert res.status_code == 201
    assert [i["id"] for i in res.json()["instances"]] == ["local", "gpu-1"]

    persisted = open(file, encoding="utf-8").read()
    assert "gpu-1" in persisted
    assert "http://10.0.0.2:8000" in persisted


def test_should_reject_duplicate_id_with_409(tmp_path, client_factory):
    client = client_factory(build_app(InstanceStore(seed(tmp_path), {})))
    res = client.post("/api/instances", json={"id": "local", "name": "Dup", "base_url": "http://h:1"})
    assert res.status_code == 409


def test_should_reject_invalid_payload_with_400(tmp_path, client_factory):
    client = client_factory(build_app(InstanceStore(seed(tmp_path), {})))
    for payload in [
        {"id": "Bad ID", "name": "x", "base_url": "http://h:1"},
        {"id": "ok", "name": "x", "base_url": "ftp://h"},
        {"id": "ok", "name": "x"},
    ]:
        res = client.post("/api/instances", json=payload)
        assert res.status_code == 400, payload


def test_should_update_instance_and_persist(tmp_path, client_factory):
    file = seed(tmp_path)
    client = client_factory(build_app(InstanceStore(file, {})))
    res = client.put("/api/instances/local",
                     json={"name": "Renamed", "base_url": "http://localhost:9000", "admin_key": "k"})
    assert res.status_code == 200
    persisted = open(file, encoding="utf-8").read()
    assert "Renamed" in persisted
    assert "localhost:9000" in persisted
    assert "admin_key" in persisted
    # GET never leaks the key.
    listing = client.get("/api/instances")
    assert '"k"' not in json.dumps(listing.json())
    assert listing.json()["instances"][0]["has_admin_key"] is True


def test_should_delete_instance_and_persist(tmp_path, client_factory):
    file = seed(tmp_path)
    client = client_factory(build_app(InstanceStore(file, {})))
    res = client.delete("/api/instances/local")
    assert res.status_code == 200
    assert len(res.json()["instances"]) == 0
    assert "local" not in open(file, encoding="utf-8").read()


def test_should_return_404_for_unknown_instance_on_update_and_delete(tmp_path, client_factory):
    client = client_factory(build_app(InstanceStore(seed(tmp_path), {})))
    put = client.put("/api/instances/nope", json={"name": "x"})
    delete = client.delete("/api/instances/nope")
    assert put.status_code == 404
    assert delete.status_code == 404


def test_should_reject_mutation_of_readonly_env_instance_with_403(tmp_path, client_factory):
    file = seed(tmp_path)
    store = InstanceStore(file, {
        "LITE_UI_INSTANCES": json.dumps([{"id": "env1", "name": "E", "base_url": "http://h:1"}]),
    })
    client = client_factory(build_app(store))
    put = client.put("/api/instances/env1", json={"name": "x"})
    delete = client.delete("/api/instances/env1")
    assert put.status_code == 403
    assert delete.status_code == 403


def test_should_probe_reachability_when_requested_and_reject_unreachable_with_422(tmp_path, client_factory):
    file = seed(tmp_path)
    client = client_factory(build_app(InstanceStore(file, {})))
    res = client.post("/api/instances?probe=true",
                      json={"id": "dead", "name": "Dead", "base_url": "http://127.0.0.1:1"})
    assert res.status_code == 422
    assert res.json()["error"] == "instance_unreachable"
    # Not saved.
    assert "dead" not in open(file, encoding="utf-8").read()
