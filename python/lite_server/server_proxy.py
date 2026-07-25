"""Server proxy for @route handlers (``ctx.server``).

Route handlers run inside the model worker and reach the hosting Rust server
over loopback HTTP (the worker receives the server's HTTP address via
``--server-http`` at spawn). Two capabilities:

- ``registry`` — live view of loaded models (``GET /v2/models``)
- ``inference`` — cross-model inference (``POST /v2/models/<m>/infer``)

Remote admin ops (``load_model`` / ``unload_model`` / ``metrics``) stay
unimplemented stubs.

Transport is stdlib ``urllib`` — no extra runtime dependency. Registry
methods are synchronous (safe in sync handlers, which run on a worker
thread); ``inference.infer`` is async and offloads the blocking call with
``asyncio.to_thread``.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

# Registry lookups should resolve fast; inference may legitimately take long
# (LLM generation) — the Rust side still bounds it by server.timeout.
REGISTRY_TIMEOUT_S = 10.0
INFER_TIMEOUT_S = 600.0


class ServerProxyError(RuntimeError):
    """The hosting server returned a non-2xx response or was unreachable."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _request_json(url: str, *, payload: Any = None, timeout: float) -> Any:
    """JSON GET (payload=None) or POST over stdlib urllib.

    Raises :class:`ServerProxyError` on HTTP errors and connection failures —
    never leaks ``urllib`` exception types to handler code.
    """
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data)
    if data is not None:
        req.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:500]
        raise ServerProxyError(
            f"server returned {e.code} for {url}: {body}", status_code=e.code
        ) from e
    except (urllib.error.URLError, OSError) as e:
        raise ServerProxyError(f"server unreachable at {url}: {e}") from e


class RegistryProxy:
    """Proxy for ``server.registry`` — live queries, no caching."""

    def __init__(self, base_url: str):
        self._base_url = base_url

    def list_loaded(self) -> List[dict]:
        """All loaded model versions: ``[{"name", "version", "status",
        "model_type", "workers"}, ...]``."""
        data = _request_json(
            f"{self._base_url}/v2/models", timeout=REGISTRY_TIMEOUT_S)
        return data.get("models", [])

    def get(self, name: str) -> Optional[dict]:
        """First loaded entry for *name*, or ``None``."""
        for m in self.list_loaded():
            if m.get("name") == name:
                return m
        return None


class InferenceProxy:
    """Proxy for cross-model inference from a route handler."""

    def __init__(self, base_url: str, model_name: str, version: str):
        self._base_url = base_url
        self._model_name = model_name
        self._version = version

    async def infer(
        self,
        model_name: str,
        input_data: Any,
        version: str | None = None,
        timeout: float = INFER_TIMEOUT_S,
    ) -> Any:
        """Run inference on another model; returns the model's JSON output.

        Raises ``ValueError`` when the target is the handler's own
        model+version: a route handler occupies its worker, so self-inference
        would deadlock with a single worker. Use a direct method call for
        own-model logic instead.
        """
        if model_name == self._model_name and (
            version is None or version == self._version
        ):
            raise ValueError(
                f"self-inference of {model_name!r} from a route handler would "
                f"deadlock (the handler occupies its worker) — call a "
                f"different model/version or use a direct method call"
            )
        path = f"/v2/models/{model_name}"
        if version is not None:
            path += f"/versions/{version}"
        url = f"{self._base_url}{path}/infer"
        return await asyncio.to_thread(
            _request_json, url, payload=input_data, timeout=timeout)


class MetricsProxy:
    """Proxy for ``server.metrics`` (stub)."""

    def query(self, name: str, **labels) -> Optional[float]:
        # TODO: wire up via internal admin channel
        return None


class ServerProxy:
    """Proxy for the hosting Rust server, scoped to one model version."""

    def __init__(self, base_url: str, model_name: str, version: str):
        self._base_url = base_url.rstrip("/")
        self._model_name = model_name
        self._version = version
        self._registry = RegistryProxy(self._base_url)
        self._metrics = MetricsProxy()
        self._inference = InferenceProxy(self._base_url, model_name, version)

    @classmethod
    def for_model(cls, base_url: str, model_name: str, version: str) -> "ServerProxy":
        return cls(base_url, model_name, version)

    @property
    def registry(self) -> RegistryProxy:
        return self._registry

    @property
    def metrics(self) -> MetricsProxy:
        return self._metrics

    @property
    def inference(self) -> InferenceProxy:
        return self._inference

    def load_model(self, name: str, version: str) -> None:
        """Trigger model load via the admin API (stub)."""
        raise NotImplementedError("Remote load_model not yet implemented")

    def unload_model(self, name: str) -> None:
        """Trigger model unload via the admin API (stub)."""
        raise NotImplementedError("Remote unload_model not yet implemented")
