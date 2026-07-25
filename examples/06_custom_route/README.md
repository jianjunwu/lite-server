# 06 · Custom Routes (`@route`)

Declare extra HTTP endpoints on a model with the `@route` decorator. They are
served under `/v2/models/<model>/<tail>` and dispatched to the model's worker
over the same ZMQ channel as inference (no separate process).

## What this does

`model.py` declares six custom routes on `PetsAPI`:

| Route | Method | Path | Purpose |
|-------|--------|------|---------|
| `status` | GET | `/v2/models/pets/status` | returns model state |
| `get_pet` | GET | `/v2/models/pets/pets/{pet_id}` | path param, 404 when missing |
| `create_pet` | POST | `/v2/models/pets/pets` | JSON body, returns 201 |
| `models` | GET | `/v2/models/pets/models` | `ctx.server` registry query |
| `ticks` | GET | `/v2/models/pets/ticks` | streaming route (SSE) |
| `request_count` | GET | `/v2/models/pets/request_count` | `ctx.server` metrics query |

Handlers receive a `RequestContext`:

- `ctx.request` — parsed JSON body (dict, or `{}` when absent)
- `ctx.meta.method` / `ctx.meta.query` / `ctx.meta.headers` — HTTP metadata
- `ctx.state["path_params"]` — path params extracted from `{name}` segments
- `ctx.server` — a `ServerProxy` for the hosting server:
  `ctx.server.registry.list_loaded()` lists loaded models, and
  `await ctx.server.inference.infer(model, input)` calls *another* model's
  inference (calling back into the same model+version raises `ValueError` —
  the handler occupies its worker, so self-inference would deadlock)
- return a plain value (→ `200 application/json`) or a `Response` (custom
  status / headers / media type)

## Run

```bash
lite-server serve --config server.yaml
```

## Try it

```bash
# custom route
curl http://localhost:8000/v2/models/pets/status
# → {"model_loaded": true, "method": "GET"}

# path params
curl http://localhost:8000/v2/models/pets/pets/1
# → {"id": 1, "name": "Fido"}

curl http://localhost:8000/v2/models/pets/pets/99
# → 404 {"error": "pet not found"}

# POST body
curl -X POST http://localhost:8000/v2/models/pets/pets \
  -H 'content-type: application/json' -d '{"name": "Buddy"}'
# → 201 {"id": 3, "name": "Buddy"}

# ctx.server: live registry of the hosting server
curl http://localhost:8000/v2/models/pets/models
# → {"loaded": [{"name": "pets", "version": "1", "status": "Ready", ...}]}

# streaming route: one SSE event per yielded item
curl -N http://localhost:8000/v2/models/pets/ticks
# → data: {"n": 0}
#   data: {"n": 1}
#   data: {"n": 2}

# ctx.server: metric lookup from the server's /metrics
curl http://localhost:8000/v2/models/pets/request_count
# → {"requests": 5}

# inference still works on the same model
curl -X POST http://localhost:8000/v2/models/pets/infer \
  -H 'content-type: application/json' -d '{"input": 5}'
# → {"output": 10}
```

## Notes

- System routes (`infer`, `events`, `stream`, `ready`, `health`, `reload`,
  `versions`, `compare`) are reserved: declaring `@route` at one of them is
  skipped with a warning at load time — you cannot shadow the inference
  contract.
- Return a `StreamingResponse` to stream chunk by chunk: with the default
  `text/event-stream` media type each chunk becomes one SSE event; any other
  `media_type` passes chunk bytes through verbatim.
- Per-route auth / rate-limit / CORS are out of scope (gateway concern);
  custom routes share the model's global callback chain.
