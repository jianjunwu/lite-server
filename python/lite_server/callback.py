"""Callback base class and CallbackRunner for lite-server.

Model authors subclass ``Callback`` and override hooks they care about.
Multiple callbacks can be registered; the ``CallbackRunner`` dispatches
to each in order with exception isolation.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lite_server.api import LitAPI, RequestMeta


class Callback:
    """Base class for inference lifecycle callbacks.

    Override any hook to inject custom logic.  All hooks have default
    no-op implementations — you only need to define the ones you use.

    Hooks that accept a data argument may return a modified value to
    transform the data flowing through the pipeline.  Return ``None``
    (or the original value) to pass through unchanged.

    Hook order for a standard request::

        on_before_decode  →  on_after_decode  →  on_before_predict
        →  on_after_predict  →  on_before_encode  →  on_after_encode
    """

    # ---- Setup / Teardown ----

    def on_before_setup(self, config: dict[str, Any], device: str) -> None:
        """Called before ``LitAPI.setup()``.

        Args:
            config: Model configuration dict.
            device: Device string (e.g. ``"cuda:0"``).
        """
        pass

    def on_after_setup(self, lit_api: LitAPI) -> None:
        """Called after ``LitAPI.setup()`` completes successfully.

        Args:
            lit_api: The fully initialized LitAPI instance.
        """
        pass

    def on_teardown(self, lit_api: LitAPI) -> None:
        """Called when the model is unloaded / worker shuts down.

        Args:
            lit_api: The LitAPI instance being torn down.
        """
        pass

    # ---- Decode ----

    def on_before_decode(self, request: Any, meta: RequestMeta) -> Any | None:
        """Called before ``decode_request``.

        Args:
            request: Raw request payload (decoded JSON dict).
            meta: HTTP request metadata.

        Returns:
            Modified request, or None to pass through unchanged.
        """
        pass

    def on_after_decode(self, decoded: Any, meta: RequestMeta) -> Any | None:
        """Called after ``decode_request``, before ``predict``.

        Args:
            decoded: Output of ``decode_request``.
            meta: HTTP request metadata.

        Returns:
            Modified decoded value, or None to pass through unchanged.
        """
        pass

    # ---- Predict ----

    def on_before_predict(self, decoded: Any, meta: RequestMeta) -> Any | None:
        """Called before ``predict``.

        Args:
            decoded: Decoded input ready for inference.
            meta: HTTP request metadata.

        Returns:
            Modified input, or None to pass through unchanged.
        """
        pass

    def on_after_predict(self, output: Any, meta: RequestMeta) -> Any | None:
        """Called after ``predict``, before ``encode_response``.

        Args:
            output: Raw model output.
            meta: HTTP request metadata.

        Returns:
            Modified output, or None to pass through unchanged.
        """
        pass

    # ---- Encode ----

    def on_before_encode(self, output: Any, meta: RequestMeta) -> Any | None:
        """Called before ``encode_response``.

        Args:
            output: Raw model output.
            meta: HTTP request metadata.

        Returns:
            Modified output, or None to pass through unchanged.
        """
        pass

    def on_after_encode(self, encoded: Any, meta: RequestMeta) -> Any | None:
        """Called after ``encode_response``, before sending to client.

        This is the last hook before the response is serialized and sent.
        Equivalent to ``LitAPI.on_response``.

        Args:
            encoded: Output of ``encode_response``.
            meta: HTTP request metadata.

        Returns:
            Modified encoded value, or None to pass through unchanged.
        """
        pass


class CallbackRunner:
    """Dispatches lifecycle events to registered callbacks.

    Features:
    - **Exception isolation**: if one callback raises, subsequent
      callbacks still run.
    - **Data transformation**: callbacks may return a modified value
      to transform data flowing through the pipeline.
    - **Async support**: use ``trigger_async`` for async callbacks.
    - **Backward compatible**: falls back to ``LitAPI.on_request`` /
      ``on_response`` when no callbacks are registered for those hooks.
    """

    _HOOK_NAMES = [
        "on_before_setup", "on_after_setup", "on_teardown",
        "on_before_decode", "on_after_decode",
        "on_before_predict", "on_after_predict",
        "on_before_encode", "on_after_encode",
    ]

    def __init__(self, callbacks: list[Callback] | None = None):
        self._callbacks: list[Callback] = list(callbacks) if callbacks else []
        self._hooked: dict[str, list[Callback]] = {name: [] for name in self._HOOK_NAMES}
        self._logger = logging.getLogger("lite_server.callback")
        for cb in self._callbacks:
            self._index_callback(cb)

    def _index_callback(self, cb: Callback) -> None:
        """Pre-compute which hooks this callback overrides."""
        for name in self._HOOK_NAMES:
            for cls in type(cb).__mro__:
                if cls is Callback:
                    break
                if name in cls.__dict__:
                    self._hooked[name].append(cb)
                    break

    # ---- Registration ----

    def register(self, cb: Callback) -> None:
        """Add a callback to the runner."""
        self._callbacks.append(cb)
        self._index_callback(cb)

    @property
    def callbacks(self) -> list[Callback]:
        return list(self._callbacks)

    # ---- Trigger helpers ----

    def _invoke_hook(self, hook_name: str, current_value: Any, meta: RequestMeta) -> Any:
        """Invoke a sync hook on registered callbacks that override it."""
        for cb in self._hooked.get(hook_name, ()):
            try:
                result = getattr(cb, hook_name)(current_value, meta)
                if result is not None:
                    current_value = result
            except Exception:
                self._logger.warning(
                    "Callback %s.%s failed", type(cb).__name__, hook_name, exc_info=True
                )
        return current_value

    async def _invoke_hook_async(self, hook_name: str, current_value: Any, meta: RequestMeta) -> Any:
        """Invoke a potentially-async hook on registered callbacks that override it."""
        for cb in self._hooked.get(hook_name, ()):
            try:
                hook = getattr(cb, hook_name)
                if inspect.iscoroutinefunction(hook):
                    result = await hook(current_value, meta)
                else:
                    result = hook(current_value, meta)
                if result is not None:
                    current_value = result
            except Exception:
                self._logger.warning(
                    "Callback %s.%s failed", type(cb).__name__, hook_name, exc_info=True
                )
        return current_value

    def _invoke_void(self, hook_name: str, *args: Any) -> None:
        """Invoke a void hook on registered callbacks that override it."""
        for cb in self._hooked.get(hook_name, ()):
            try:
                getattr(cb, hook_name)(*args)
            except Exception:
                self._logger.warning(
                    "Callback %s.%s failed", type(cb).__name__, hook_name, exc_info=True
                )

    async def _invoke_void_async(self, hook_name: str, *args: Any) -> None:
        """Invoke a potentially-async void hook on registered callbacks that override it."""
        for cb in self._hooked.get(hook_name, ()):
            try:
                hook = getattr(cb, hook_name)
                if inspect.iscoroutinefunction(hook):
                    await hook(*args)
                else:
                    hook(*args)
            except Exception:
                self._logger.warning(
                    "Callback %s.%s failed", type(cb).__name__, hook_name, exc_info=True
                )

    # ---- Public trigger API ----

    def trigger(self, hook_name: str, current_value: Any, meta: RequestMeta) -> Any:
        """Trigger a data-transforming hook on all callbacks (sync).

        Args:
            hook_name: Name of the hook method (e.g. ``"on_before_decode"``).
            current_value: Current pipeline value.
            meta: Request metadata.

        Returns:
            The (possibly modified) value after all callbacks have run.
        """
        if not self._callbacks:
            return current_value
        return self._invoke_hook(hook_name, current_value, meta)

    async def trigger_async(self, hook_name: str, current_value: Any, meta: RequestMeta) -> Any:
        """Trigger a data-transforming hook on all callbacks (async-safe).

        Args:
            hook_name: Name of the hook method.
            current_value: Current pipeline value.
            meta: Request metadata.

        Returns:
            The (possibly modified) value after all callbacks have run.
        """
        if not self._callbacks:
            return current_value
        return await self._invoke_hook_async(hook_name, current_value, meta)

    def trigger_void(self, hook_name: str, *args: Any) -> None:
        """Trigger a void hook (no return value) on all callbacks.

        Args:
            hook_name: Name of the hook method (e.g. ``"on_teardown"``).
            *args: Positional arguments to pass to the hook.
        """
        if not self._callbacks:
            return
        self._invoke_void(hook_name, *args)

    async def trigger_void_async(self, hook_name: str, *args: Any) -> None:
        """Trigger a potentially-async void hook on all callbacks."""
        if not self._callbacks:
            return
        await self._invoke_void_async(hook_name, *args)

    def has_callbacks(self) -> bool:
        """Return True if at least one callback is registered."""
        return len(self._callbacks) > 0


def load_callbacks(config: dict[str, Any]) -> CallbackRunner:
    """Load callback instances from model config.

    Expects a ``callbacks`` key in config containing a list of
    fully-qualified class paths::

        callbacks:
          - my_package.callbacks.AuditLogger
          - my_package.callbacks.MetricsCollector

    Each class is instantiated with no arguments and must be a
    subclass of :class:`Callback`.
    """
    runner = CallbackRunner()
    paths = config.get("callbacks", [])
    if not paths:
        return runner

    for path in paths:
        try:
            module_path, class_name = path.rsplit(".", 1)
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            if not issubclass(cls, Callback):
                runner._logger.warning(
                    "%s is not a Callback subclass, skipping", path
                )
                continue
            runner.register(cls())
        except Exception:
            runner._logger.warning(
                "Failed to load callback %s", path, exc_info=True
            )

    return runner
