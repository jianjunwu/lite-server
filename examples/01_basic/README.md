# 01 Basic Model

The simplest lite-server example. A model that returns `input * 2`.

## Run

```bash
# From the project root
python -m lite_server serve --model-repo examples/01_basic/model_repo
```

## Test

```bash
curl -X POST http://localhost:8000/v2/models/echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}
```

## What You Learn

- How to write a `model.py` (subclass `LitAPI`)
- How to define `setup`, `decode_request`, `predict`, `encode_response`
- How `config.yaml` configures the model
- The `model_name/version/` directory structure
