"""Audit: the /info whitelist filter must keep granted models.

The Rust instance serves ``loaded_models`` as ``"name/version"`` strings
(src/http/handlers/health.rs), but the BFF filter matches the whole string
against grants stored as bare model names (authdb.py: model_grants PK is
(username, instance_id, model)) — so ``granted("pets/1")`` never matches a
grant on ``"pets"`` and every loaded model is dropped for whitelist users.
"""

from __future__ import annotations

from lite_server.webui.auth import UserStore
from lite_server.webui.ownership import filter_list_response


def test_info_filter_keeps_loaded_models_of_granted_model(tmp_path):
    store = UserStore(str(tmp_path / "auth.db"), {})
    store.set_model_grant("alice", "dev", "pets", "viewer")
    user = {"username": "alice", "role": "viewer"}
    payload = {"server": "lite-server", "loaded_models": ["pets/1", "cats/2"]}

    filtered = filter_list_response(store, "dev", user, "info", payload)

    assert filtered["loaded_models"] == ["pets/1"], (
        "the granted model's 'name/version' entry must survive the filter"
    )


def test_info_filter_drops_models_without_grant(tmp_path):
    store = UserStore(str(tmp_path / "auth.db"), {})
    store.set_model_grant("alice", "dev", "pets", "viewer")
    user = {"username": "alice", "role": "viewer"}
    payload = {"server": "lite-server", "loaded_models": ["pets/1", "cats/2"]}

    filtered = filter_list_response(store, "dev", user, "info", payload)

    assert "cats/2" not in filtered["loaded_models"]
