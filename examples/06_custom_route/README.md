# 06 · Custom Routes (`@route`)

Declare extra HTTP endpoints on a model with the `@route` decorator. They are
served under `/v2/models/<model>/<tail>` and dispatched to the model's worker
over the same ZMQ channel as inference (no separate process).

## What this does

`model.py` declares three custom routes on `PetsAPI`:

| Route | Method | Path | Purpose |
|-------|--------|------|---------|
| `status` | GET | `/v2/models/pets/status` | returns model state |
| `get_pet` | GET | `/v2/models/pets/pets/{pet_id}` | path param, 404 when missing |
| `create_pet` | POST | `/v2/models/pets/pets` | JSON body, returns 201 |

Handlers receive a `RequestContext`:

- `ctx.request` — parsed JSON body (dict, or `{}` when absent)
- `ctx.meta.method` / `ctx.meta.query` / `ctx.meta.headers` — HTTP metadata
- `ctx.state["path_params"]` — path params extracted from `{name}` segments
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
- Per-route auth / rate-limit / CORS are out of scope (gateway concern);
  custom routes share the model's global callback chain.
