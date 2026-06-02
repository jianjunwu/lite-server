"""Extended server proxy with full AppState capabilities.

This module provides an enriched ServerProxy that allows endpoints to
interact with the Rust server's core services (registry, inference queue,
metrics) via an internal communication channel.

Note: Full remote-call implementation requires the internal admin gRPC
service on the Rust side. The current version provides the interface;
actual RPCs are stubbed and can be wired up in a follow-up phase.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class RegistryProxy:
    """Proxy for server.registry."""

    def __init__(self, snapshot: dict):
        self._snapshot = snapshot

    def list_loaded(self) -> List[dict]:
        return self._snapshot.get("loaded_models", [])

    def get(self, name: str) -> Optional[dict]:
        for m in self.list_loaded():
            if m.get("name") == name:
                return m
        return None


class MetricsProxy:
    """Proxy for server.metrics (stub)."""

    def query(self, name: str, **labels) -> Optional[float]:
        # TODO: wire up via internal admin channel
        return None


class InferenceProxy:
    """Proxy for server.infer (stub)."""

    async def infer(self, model_name: str, input_data: dict, version: str | None = None) -> Any:
        # TODO: wire up via internal admin channel
        raise NotImplementedError("Remote inference not yet implemented")


class ServerProxy:
    """Full-featured proxy for the Rust server."""

    def __init__(self, snapshot: dict):
        self._snapshot = snapshot
        self._registry = RegistryProxy(snapshot)
        self._metrics = MetricsProxy()
        self._inference = InferenceProxy()

    @property
    def registry(self) -> RegistryProxy:
        return self._registry

    @property
    def config(self) -> dict:
        return self._snapshot.get("config", {})

    @property
    def metrics(self) -> MetricsProxy:
        return self._metrics

    @property
    def inference(self) -> InferenceProxy:
        return self._inference

    async def load_model(self, name: str, version: str) -> None:
        """Trigger model load via the admin API (stub)."""
        raise NotImplementedError("Remote load_model not yet implemented")

    async def unload_model(self, name: str) -> None:
        """Trigger model unload via the admin API (stub)."""
        raise NotImplementedError("Remote unload_model not yet implemented")
