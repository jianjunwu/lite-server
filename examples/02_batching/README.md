# 02 Request Batching

Demonstrates request batching — both with default behavior and custom batch/unbatch functions.

## Run

```bash
python -m lite_server serve --model-repo examples/02_batching/model_repo
```

## Models

### `batched` — Default Batching

Uses the framework's default `batch()` / `unbatch()` (identity passthrough).
The `predict()` method simply receives a list of decoded inputs.

```python
def predict(self, inputs):
    if isinstance(inputs, list):
        return [{"output": x, "batch_size": len(inputs)} for x in inputs]
    return {"output": inputs, "batch_size": 1}
```

Test:

```bash
# Send 8 requests concurrently — batched into one call
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8000/v2/models/batched/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}" &
done
wait

# Each response: {"output": <N>, "batch_size": 8}
```

### `custom_batch` — Custom batch / unbatch

Overrides `batch()` and `unbatch()` to reshape data between individual requests
and the batched `predict()` call.

**Pipeline:** `decode_request -> batch -> predict -> unbatch -> encode_response`

```python
def batch(self, inputs):
    # Pack decoded requests into arrays (mimics tensor stacking)
    return {
        "values": [x["value"] for x in inputs],
        "weights": [x["weight"] for x in inputs],
        "batch_size": len(inputs),
    }

def predict(self, batch):
    # When multiple requests are queued, batch() is called and predict()
    # receives its output.  For a single request, batch() is skipped and
    # predict() gets the decoded request directly — handle both cases.
    if isinstance(batch, dict) and "values" in batch:
        results = [v * w for v, w in zip(batch["values"], batch["weights"])]
        return {"results": results, "batch_size": batch["batch_size"]}
    # Single request — batch() and unbatch() are both skipped,
    # so return the final per-request format directly.
    return {"output": batch["value"] * batch["weight"], "batch_size": 1}

def unbatch(self, output):
    # Splits batch output back into per-request responses
    return [{"output": r, "batch_size": output["batch_size"]}
            for r in output["results"]]
```

Test:

```bash
# Send requests with different weights
for i in $(seq 1 4); do
  curl -s -X POST http://localhost:8000/v2/models/custom_batch/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i, \"weight\": 0.5}" &
done
wait

# Each response: {"output": <N * 0.5>, "batch_size": 4}
```

## What You Learn

- How `max_batch_size` and `batch_timeout` enable automatic batching
- How `predict()` receives a list when batching is active (default path)
- How to override `batch()` to reshape inputs before prediction
- How to override `unbatch()` to split outputs back to per-request responses
- How adaptive batching adjusts timeout based on queue pressure

## Key Config

```yaml
max_batch_size: 8      # Max requests per batch
batch_timeout: 0.01    # Wait up to 10ms to fill a batch
```
