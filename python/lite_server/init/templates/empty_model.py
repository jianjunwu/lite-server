"""Minimal LitAPI example.

Since 0.7.0, every model runs on the unified async loop — sync and async
methods are adapted automatically.  Use ``async def`` for I/O-bound work
(database lookups, downstream API calls); the pipeline runs sync methods
on a thread executor so they never block the event loop.

Declare a ``ctx`` parameter on ``decode_request``, ``predict``,
``encode_response``, ``batch``, and ``unbatch`` to access per-request
metadata (headers, route, client_ip, request_id) and ``ctx.state``
(per-request scratch dict).  In batch mode, ``batch`` / ``unbatch`` /
``predict`` receive a ``list[RequestContext]`` aligned with the inputs.
"""

from lite_server import LitAPI, RequestContext


class MyAPI(LitAPI):
    def setup(self, device):
        """Load model weights and initialize state (always synchronous)."""
        self.model = lambda x: x * 2

    async def decode_request(self, request, ctx: RequestContext | None = None):
        """Convert HTTP request JSON to model input.

        ``ctx`` provides:
        - ``ctx.meta`` — immutable request metadata (headers, route,
          client_ip, request_id)
        - ``ctx.state`` — per-request scratch dict (safe under concurrency)
        - ``ctx.respond(...)`` — short-circuit the pipeline with an early
          response (auth failures, cache hits, validation errors)
        """
        return request.get("input", 0)

    async def predict(self, x, ctx: RequestContext | None = None):
        """Run inference.  x is a list when batching is enabled.

        ``async def`` is the recommended default since 0.7.0 — ideal for
        I/O-bound work like downstream API calls or database lookups.
        For CPU-bound inference, just use ``def`` and the pipeline runs
        it on a thread executor without blocking the event loop.
        """
        if isinstance(x, list):
            return [self.model(item) for item in x]
        return self.model(x)

    async def encode_response(self, output, ctx: RequestContext | None = None):
        """Convert model output to HTTP response JSON.

        Use ``ctx.respond(output, headers=...)`` to attach custom headers
        or override the status code::

            return ctx.respond(
                {"result": output},
                headers={"X-Request-ID": ctx.meta.request_id},
            )
        """
        return {"result": output}

    # ----- Optional: hot-reload hook (needs hot_reload: true in config.yaml)
    # Uncomment to refresh artifacts IN-PROCESS when watched files change,
    # instead of paying a full worker restart (weights stay loaded).
    #
    # Return any non-None value to mark the change handled; returning None
    # (or raising) makes the server restart the worker as before.  The hook
    # runs synchronously on the worker event loop — heavy refreshes block
    # inference, and refreshing state while requests are in flight is your
    # responsibility.
    #
    # def on_file_changed(self, changed_files: list[str]):
    #     for path in changed_files:
    #         if path.endswith("weights.pt"):
    #             self.model = load_weights(path)
    #     return "handled"
