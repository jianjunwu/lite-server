# 02 Request Batching

Demonstrates request batching. Multiple requests arriving within the batch window are grouped and processed together.

## Run

```bash
python -m lite_server serve --model-repo examples/02_batching/model_repo
```

## Test

```bash
# Send 8 requests concurrently — they'll be batched into one call
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8000/v2/models/batched/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}" &
done
wait

# Each response includes "batch_size" showing how many requests were batched
# {"output": 1, "batch_size": 8}
```

## What You Learn

- How to enable batching via `max_batch_size` and `batch_timeout`
- How `predict()` receives a list when batching is active
- How adaptive batching adjusts timeout based on queue pressure

## Key Config

```yaml
max_batch_size: 8      # Max requests per batch
batch_timeout: 0.01    # Wait up to 10ms to fill a batch
```
