# 04 Multi-Version Management

Demonstrates multi-version model management. Two versions of the same model run simultaneously, and you can switch between them at runtime.

## Run

```bash
cd examples/04_multi_version
python -m lite_server serve --config server.yaml
```

## Test

```bash
# Inference uses the active version (v2 by default, set in server.yaml)
curl -X POST http://localhost:8000/v2/models/multi_version/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
# => {"output": 20, "version": "v2"}  (v2: x * 2)

# Switch active version to v1
curl -X POST http://localhost:8000/v2/models/multi_version/versions/v1/activate

# Now inference uses v1
curl -X POST http://localhost:8000/v2/models/multi_version/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
# => {"output": 11, "version": "v1"}  (v1: x + 1)

# List all loaded versions
curl http://localhost:8000/v2/models/multi_version/versions

# Inference on a specific version (regardless of active)
curl -X POST http://localhost:8000/v2/models/multi_version/versions/v2/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
```

## What You Learn

- How to organize multiple versions under one model name
- How `server.yaml` controls which versions to load
- How to activate/deactivate versions at runtime
- How to target a specific version in inference requests

## Directory Structure

```
model_repo/
  multi_version/
    v1/
      model.py        # Version 1: x + 1
      config.yaml
    v2/
      model.py        # Version 2: x * 2
      config.yaml
server.yaml           # Loads all versions, sets v2 as default
```

## Orchestration Config

```yaml
control_mode: explicit
load_models:
  - multi_version
models:
  - name: multi_version
    load_policy: all        # Load all versions
    default_version: v2     # v2 is active by default
```

### Load Policies

| Policy | Behavior |
|--------|----------|
| `explicit` | Only load versions listed in `versions_to_load` |
| `latest` | Load only the latest version |
| `all` | Load all available versions |
