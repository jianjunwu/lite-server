# 06 Custom Endpoints

Demonstrates how to add custom HTTP endpoints to the server. A `*_endpoint.py` file in the model repo registers new routes.

## Run

```bash
python -m lite_server serve --model-repo examples/06_custom_endpoint/model_repo
```

## Test

```bash
# Built-in inference endpoint
curl -X POST http://localhost:8000/v2/models/echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}

# Custom status endpoint
curl http://localhost:8000/status
# => {"server": "lite-server", "loaded_models_count": 1, "loaded_models": [...]}
```

## What You Learn

- How to create custom HTTP endpoints alongside inference routes
- How `*_endpoint.py` files are auto-discovered from the model repo
- How to access the server's model registry from a custom endpoint

## How It Works

Any `*_endpoint.py` file in the model repo root is auto-discovered:

```python
# status_endpoint.py

methods = ["GET"]  # HTTP methods to register

def handler(request, server):
    """Called when the endpoint is hit."""
    # `server.registry` gives access to the model registry
    models = server.registry.list_loaded()
    return {"loaded": len(models)}
```

The `methods` list defines which HTTP methods to register. The `handler` function receives the request and server context.
