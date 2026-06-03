# 12 Continuous Batching

Demonstrates continuous batching for LLM workloads using three hooks: `prefill()`, `step()`, `has_finished()`.

[中文](README_zh.md)

## Key Concept

Continuous batching processes multiple sequences simultaneously by iteratively calling `step()` to generate one token per active sequence. New requests are added mid-generation via `prefill()`, and finished sequences are removed via `has_finished()`.

## Run

```bash
cd examples/12_continuous_batching
python -m lite_server serve --config server.yaml
```

## Test

```bash
# Send a single request — generates tokens one step at a time
curl -X POST http://localhost:8000/v2/models/cb_llm/infer \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "hello world this is a test"}'
# => {"tokens": ["hello","world","this","is","a"], "text": "hello world this is a"}

# Send multiple concurrent requests — they share the same generation loop
for i in 1 2 3; do
  curl -s -X POST http://localhost:8000/v2/models/cb_llm/infer \
    -H 'Content-Type: application/json' \
    -d "{\"prompt\": \"request $i goes here\"}" &
done
wait
```

## What You Learn

- How to implement `prefill()` to initialize a sequence in the batch
- How `step()` generates one token for all active sequences per iteration
- How `has_finished()` signals sequence completion
- Config pattern: `continuous_batching: true` in config.yaml
