# 07 Custom Parameters

Demonstrates how to pass custom parameters from `config.yaml` to your model code via `self.config`.

## Key Concept

All fields in `config.yaml` are available in `model.py` through `self.config.get(key, default)`. This lets you tune model behavior without changing code.

## Run

```bash
cd examples/07_custom_params
python -m lite_server serve --config server.yaml
```

## Test

```bash
# Score above threshold (0.5) -> "positive"
curl -X POST http://localhost:8000/v2/models/threshold/infer \
  -H 'Content-Type: application/json' \
  -d '{"score": 0.8}'
# => {"label": "positive", "score": 0.8, "threshold": 0.5}

# Score below threshold -> "negative"
curl -X POST http://localhost:8000/v2/models/threshold/infer \
  -H 'Content-Type: application/json' \
  -d '{"score": 0.3}'
# => {"label": "negative", "score": 0.3, "threshold": 0.5}
```

## What You Learn

- How to access custom `config.yaml` fields via `self.config.get(key, default)`
- How to make model behavior config-driven without code changes
- The pattern: define parameters in YAML, read them in `setup()`
