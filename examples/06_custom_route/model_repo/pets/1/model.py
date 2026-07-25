"""Custom routes example: @route handlers served alongside inference.

Routes are declared with the ``@route`` decorator on LitAPI methods and are
served under ``/v2/models/<model>/<tail>`` over the same channel as inference.
``ctx`` is a :class:`RequestContext`: ``ctx.request`` (parsed JSON body),
``ctx.meta.method`` / ``ctx.meta.query`` / ``ctx.meta.headers``,
``ctx.state["path_params"]`` (path params), and ``ctx.server`` (a ServerProxy
for the hosting server — registry queries and cross-model inference).
"""

from lite_server import LitAPI, route
from lite_server.response import Response


class PetsAPI(LitAPI):
    def setup(self, device):
        self.loaded = True
        self.pets = {1: {"id": 1, "name": "Fido"}, 2: {"id": 2, "name": "Rex"}}

    def predict(self, x):
        return {"output": x["input"] * 2}

    # ---- custom routes (served under /v2/models/pets/<tail>) ----

    @route.get("/status")
    def status(self, ctx):
        return {"model_loaded": self.loaded, "method": ctx.meta.method}

    @route.get("/pets/{pet_id}")
    def get_pet(self, ctx):
        pet_id = int(ctx.state["path_params"]["pet_id"])
        pet = self.pets.get(pet_id)
        if pet is None:
            return Response(content={"error": "pet not found"}, status_code=404)
        return pet

    @route.post("/pets")
    def create_pet(self, ctx):
        body = ctx.request or {}
        pet_id = max(self.pets, default=0) + 1
        self.pets[pet_id] = {"id": pet_id, "name": body.get("name", "unnamed")}
        return Response(content=self.pets[pet_id], status_code=201)

    @route.get("/models")
    def models(self, ctx):
        # ctx.server queries the hosting server over loopback HTTP.
        # Note: infer() back into this same model is rejected (deadlock).
        return {"loaded": ctx.server.registry.list_loaded()}

    @route.get("/ticks")
    def ticks(self, ctx):
        # Streaming route: each yielded item becomes one SSE event
        # (default text/event-stream media type).
        from lite_server.response import StreamingResponse

        async def gen():
            for n in range(3):
                yield {"n": n}

        return StreamingResponse(content=gen())

    @route.get("/request_count")
    def request_count(self, ctx):
        # ctx.server.metrics scrapes the server's /metrics endpoint.
        total = ctx.server.metrics.query(
            "liteserver_requests_total", model="pets", version="1")
        return {"requests": total}
