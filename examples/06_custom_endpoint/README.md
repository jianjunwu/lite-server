# 06 Custom Endpoints

Demonstrates how to add custom HTTP endpoints to the server. Endpoints are defined in the `endpoints/` directory (or any directory specified via `--endpoints-dir`).

## Run

```bash
cd examples/06_custom_endpoint
lite-server serve --config server.yaml
# or: python -m lite_server serve --config server.yaml
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
# => {"server": "lite-server", "loaded_models_count": 1}
```

## What You Learn

- How to create custom HTTP endpoints alongside inference routes
- How endpoint files are auto-discovered from the `endpoints/` directory
- How to access the server's model registry from a custom endpoint

## How It Works

Place Python files in the `endpoints/` directory. They are auto-discovered recursively:

```python
# endpoints/status.py

methods = ["GET"]  # HTTP methods to register

def handler(request, server):
    """Called when the endpoint is hit."""
    # `server.registry` gives access to the model registry
    models = server.registry.list_loaded()
    return {"loaded": len(models)}
```

The `methods` list defines which HTTP methods to register. The `handler` function receives the request and server context.

## Advanced: Decorator-based Routes

You can also use the decorator API for more control:

```python
from lite_server import endpoint

@endpoint.get("/status")
def status(request, server):
    return {"loaded": len(server.registry.list_loaded())}
```

## CLI: Specify Custom Endpoint Directory

```bash
lite-server serve --endpoints-dir ./my-endpoints --config server.yaml
```

Priority: `--endpoints-dir` > `server.yaml endpoints_dir` > `model_repository.path`
