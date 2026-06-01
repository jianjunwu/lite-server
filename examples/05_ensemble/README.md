# 05 Ensemble Pipeline

Demonstrates ensemble inference with a DAG pipeline. Multiple models are chained together, and independent steps run in parallel.

## Run

```bash
cd examples/05_ensemble
python -m lite_server serve --config server.yaml
```

## Test

```bash
curl -X POST http://localhost:8000/v2/models/pipeline/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "preprocessed(hello) -> done"}
```

The pipeline: `request` -> `step_a` (preprocess) -> `step_b` (postprocess) -> response.

## What You Learn

- How to define an ensemble DAG in `config.yaml`
- How `$request` and `$step_name` references wire steps together
- How independent steps run in parallel (topological execution)
- That ensemble models don't need a `model.py` — only `config.yaml`

## DAG Config

```yaml
ensemble:
  steps:
    - name: preprocess
      model: step_a          # References step_a model
      version: "1"
      inputs:
        input: "$request.input"  # Takes input from the HTTP request

    - name: postprocess
      model: step_b          # References step_b model
      version: "1"
      inputs:
        input: "$preprocess.output"  # Takes input from preprocess step
```

## Directory Structure

```
model_repo/
  step_a/1/
    model.py        # Preprocessing model
    config.yaml
  step_b/1/
    model.py        # Postprocessing model
    config.yaml
  pipeline/
    config.yaml     # Ensemble DAG definition (no model.py needed)
```

## Parallel Execution

If you have steps that don't depend on each other, they run in parallel:

```yaml
ensemble:
  steps:
    - name: step_a
      model: model_a
      version: "1"
      inputs:
        input: "$request.input"

    - name: step_b
      model: model_b
      version: "1"
      inputs:
        input: "$request.input"  # Both take from request — no dependency

    - name: step_c
      model: model_c
      version: "1"
      inputs:
        a: "$step_a.output"      # Depends on step_a
        b: "$step_b.output"      # Depends on step_b
```

Here, `step_a` and `step_b` run in parallel. `step_c` waits for both.

## Multi-Parameter Parallel Pipeline

A real-world example: **multi-way recall** with 4 independent recall models running in parallel, followed by merge and rank.

### DAG

```
$request.query    --> [bm25_recall] --+
$request.user_id  --> [cf_recall]    --+
$request.image    --> [visual_recall] --+--> [merge] --> [rank] --> response
$request.history  --> [seq_recall]    --+
```

Layer 0: `bm25_recall`, `cf_recall`, `visual_recall`, `seq_recall` all depend only on `$request` — they execute in parallel.

Layer 1: `merge` waits for all 4 recalls to finish.

Layer 2: `rank` waits for merge, outputs the final top-k results.

### Directory Structure

```
model_repo/
  bm25_recall/1/
    model.py
    config.yaml
  cf_recall/1/
    model.py
    config.yaml
  visual_recall/1/
    model.py
    config.yaml
  seq_recall/1/
    model.py
    config.yaml
  merge/1/
    model.py
    config.yaml
  rank/1/
    model.py
    config.yaml
  multi_pipeline/1/
    config.yaml     # Ensemble DAG definition
```

### Config

`multi_pipeline/1/config.yaml`:

```yaml
ensemble:
  steps:
    - name: bm25_recall
      model: bm25_recall
      version: "1"
      inputs:
        query: "$request.query"

    - name: cf_recall
      model: cf_recall
      version: "1"
      inputs:
        user_id: "$request.user_id"

    - name: visual_recall
      model: visual_recall
      version: "1"
      inputs:
        image: "$request.image"

    - name: seq_recall
      model: seq_recall
      version: "1"
      inputs:
        history: "$request.history"

    - name: merge
      model: merge
      version: "1"
      inputs:
        bm25: "$bm25_recall.items"
        cf: "$cf_recall.items"
        visual: "$visual_recall.items"
        seq: "$seq_recall.items"

    - name: rank
      model: rank
      version: "1"
      inputs:
        candidates: "$merge.merged"
        top_k: "$request.top_k"
```

### Test

```bash
curl -X POST http://localhost:8000/v2/models/multi_pipeline/infer \
  -H 'Content-Type: application/json' \
  -d '{"query": "running shoes", "user_id": "u123", "image": "shoe.jpg", "history": ["sneakers", "nike"], "top_k": 5}'
```

Expected output:

```json
{"results": ["bm25_item_0", "bm25_item_1", "bm25_item_2", "cf_item_0", "cf_item_1"]}
```
