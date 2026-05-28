"""Enhanced LitAPI with lifecycle hooks for lite-server.

Model authors subclass ``lite_server.api.LitAPI`` instead of
``litserve.LitAPI`` to gain access to framework-level hooks
(teardown, on_file_changed, logger).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import litserve as ls

if TYPE_CHECKING:
    from litserve.specs.base import LitSpec


class LitAPI(ls.LitAPI):
    """Drop-in replacement for ``ls.LitAPI`` with lite-server hooks.

    Usage is identical to ``ls.LitAPI``::

        from lite_server import LitAPI

        class MyModel(LitAPI):
            def setup(self, device):
                self.model = load_model()

            def predict(self, x):
                return self.model(x)
    """

    def __init__(
        self,
        max_batch_size: int = 1,
        batch_timeout: float = 0.0,
        api_path: str = "/predict",
        stream: bool = False,
        loop: Any = "auto",
        spec: LitSpec | None = None,
        mcp: Any = None,
        enable_async: bool = False,
    ):
        super().__init__(
            max_batch_size=max_batch_size,
            batch_timeout=batch_timeout,
            api_path=api_path,
            stream=stream,
            loop=loop,
            spec=spec,
            mcp=mcp,
            enable_async=enable_async,
        )
        self.config: dict[str, Any] = {}
        self._logger: logging.Logger | None = None

    @property
    def logger(self) -> logging.Logger:
        """Lazy logger bound to the model class name."""
        if self._logger is None:
            self._logger = logging.getLogger(
                self.__class__.__module__ + "." + self.__class__.__name__
            )
        return self._logger

    def on_file_changed(self, changed_files: list[str]) -> Any:
        """Called when files in the model directory change (hot reload).

        Override to implement custom reload logic for weights, configs,
        vocab files, or any other model artifacts.

        Args:
            changed_files: Absolute paths to files that have changed.

        Returns:
            Any non-None value suppresses the default fallback behavior.
        """
        return None

    def teardown(self) -> None:
        """Called when the model is unloaded.

        Override to release resources (GPU memory, file handles, etc.).
        """
        pass
