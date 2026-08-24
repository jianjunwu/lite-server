"""Instance registry: yaml/env loading, validation, atomic write-back."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

import yaml

ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")


@dataclass
class InstanceConfig:
    id: str
    name: str
    base_url: str
    admin_key: str | None = None
    readonly: bool = False


def _normalize(raw: dict, env: Mapping[str, str], source: str, readonly: bool) -> InstanceConfig:
    inst_id = raw.get("id")
    if not isinstance(inst_id, str) or not ID_PATTERN.match(inst_id):
        raise ValueError(
            f"invalid instance id in {source}: {inst_id!r} (must match {ID_PATTERN.pattern})"
        )
    base_url = raw.get("base_url")
    if not isinstance(base_url, str):
        raise ValueError(f'invalid base_url for instance "{inst_id}" in {source}: not a string')
    parts = urlsplit(base_url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(f'invalid base_url for instance "{inst_id}" in {source}: {base_url}')
    origin = f"{parts.scheme}://{parts.netloc}"
    path = "" if parts.path in ("", "/") else parts.path.rstrip("/")

    admin_key = raw.get("admin_key")
    if not (isinstance(admin_key, str) and admin_key):
        env_name = raw.get("admin_key_env")
        admin_key = env.get(env_name) if isinstance(env_name, str) and env_name else None

    name = raw.get("name")
    return InstanceConfig(
        id=inst_id,
        name=name if isinstance(name, str) and name else inst_id,
        base_url=origin + path,
        admin_key=admin_key,
        readonly=readonly,
    )


def load_instances(config_path: str, env: Mapping[str, str]) -> list[InstanceConfig]:
    """Load instances from the yaml file plus LITE_UI_INSTANCES (readonly)."""
    instances: dict[str, InstanceConfig] = {}

    content = None
    try:
        content = Path(config_path).read_text(encoding="utf-8")
    except OSError:
        # Missing file is fine: env-only or empty registry.
        pass
    if content is not None:
        doc = yaml.safe_load(content) or {}
        raw_list = doc.get("instances", [])
        if not isinstance(raw_list, list):
            raise ValueError(f'invalid {config_path}: "instances" must be a list')
        for raw in raw_list:
            inst = _normalize(raw, env, str(config_path), readonly=False)
            if inst.id in instances:
                raise ValueError(f'duplicate instance id "{inst.id}" in {config_path}')
            instances[inst.id] = inst

    env_json = env.get("LITE_UI_INSTANCES")
    if env_json:
        raw_list = json.loads(env_json)
        if not isinstance(raw_list, list):
            raise ValueError("invalid LITE_UI_INSTANCES: must be a JSON array")
        for raw in raw_list:
            inst = _normalize(raw, env, "LITE_UI_INSTANCES", readonly=True)
            if inst.id in instances:
                raise ValueError(f'duplicate instance id "{inst.id}" (env)')
            instances[inst.id] = inst

    return list(instances.values())


class StoreError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class InstanceStore:
    """Mutable instance registry with atomic yaml write-back.

    Env-injected instances stay readonly and are never persisted.
    """

    def __init__(self, config_path: str, env: Mapping[str, str]):
        self._config_path = str(config_path)
        self._env = env
        self._instances = {i.id: i for i in load_instances(config_path, env)}

    def list(self) -> list[InstanceConfig]:
        return list(self._instances.values())

    def get(self, inst_id: str) -> InstanceConfig | None:
        return self._instances.get(inst_id)

    def create(self, raw: dict) -> InstanceConfig:
        inst = self._validate(raw)
        if inst.id in self._instances:
            raise StoreError("duplicate", f'instance id "{inst.id}" already exists')
        self._instances[inst.id] = inst
        self._persist()
        return inst

    def update(self, inst_id: str, patch: dict) -> InstanceConfig:
        existing = self._instances.get(inst_id)
        if existing is None:
            raise StoreError("not_found", f'unknown instance "{inst_id}"')
        if existing.readonly:
            raise StoreError("readonly", f'instance "{inst_id}" is env-managed (readonly)')
        merged = self._validate({
            "id": inst_id,
            "name": patch.get("name") if patch.get("name") is not None else existing.name,
            "base_url": patch.get("base_url") if patch.get("base_url") is not None else existing.base_url,
            # Explicit null clears the key; a missing key keeps the existing one.
            "admin_key": patch["admin_key"] if "admin_key" in patch else existing.admin_key,
        })
        self._instances[inst_id] = merged
        self._persist()
        return merged

    def remove(self, inst_id: str) -> None:
        existing = self._instances.get(inst_id)
        if existing is None:
            raise StoreError("not_found", f'unknown instance "{inst_id}"')
        if existing.readonly:
            raise StoreError("readonly", f'instance "{inst_id}" is env-managed (readonly)')
        del self._instances[inst_id]
        self._persist()

    def _validate(self, raw: dict) -> InstanceConfig:
        try:
            return _normalize(raw, self._env, "api", readonly=False)
        except ValueError as e:
            raise StoreError("invalid", str(e)) from e

    def _persist(self) -> None:
        """Atomic write: temp file + rename. Only file-managed instances persist."""
        doc = {
            "instances": [
                {
                    "id": i.id,
                    "name": i.name,
                    "base_url": i.base_url,
                    **({"admin_key": i.admin_key} if i.admin_key else {}),
                }
                for i in self._instances.values()
                if not i.readonly
            ]
        }
        tmp = f"{self._config_path}.tmp-{os.getpid()}"
        Path(tmp).write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
        os.replace(tmp, self._config_path)
