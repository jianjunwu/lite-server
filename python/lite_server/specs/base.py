"""EndpointSpec - abstract base for protocol-compatible endpoints.

Subclass ``EndpointSpec`` to define a new API compatibility layer
(e.g. Anthropic messages, Ollama generate). Concrete subclasses are
auto-registered in ``_SPEC_REGISTRY`` via ``__init_subclass__`` and
are discovered by the endpoint worker at startup.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

_SPEC_REGISTRY: list[type[EndpointSpec]] = []


class EndpointSpec(ABC):
    """Abstract base for protocol-compatible endpoint specs.

    Subclass, implement the required methods, and the spec is
    automatically registered for discovery by the endpoint worker.
    """

    @classmethod
    @abstractmethod
    def detect(cls, mod) -> list[EndpointSpec]:
        """Discover concrete instances of this spec in a loaded module.

        Called by the endpoint worker when scanning ``endpoints/*.py``
        files. Should inspect the module's namespace and return
        instantiated spec objects for each discovered implementation.

        Returns an empty list if no matching implementations are found.
        """
        ...

    @abstractmethod
    def get_routes(self) -> list[dict[str, object]]:
        """Return route definitions for registration.

        Each dict should have:
        - ``"route"`` (str): URL path
        - ``"methods"`` (list[str]): HTTP methods
        """
        ...

    @abstractmethod
    async def handle(self, request: dict) -> dict:
        """Handle a single request and return a response dict.

        The response dict must contain:
        - ``"request_id"`` (str)
        - ``"status_code"`` (int)
        - ``"headers"`` (dict | None)
        - ``"body"`` (dict)
        """
        ...

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # In Python < 3.13, __abstractmethods__ is not yet populated when
        # __init_subclass__ runs. Register unconditionally; detect() will
        # filter out abstract classes at runtime via __abstractmethods__.
        _SPEC_REGISTRY.append(cls)
