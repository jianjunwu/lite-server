# 05 Ensemble Pipeline

Demonstrates ensemble inference with a DAG pipeline. Multiple models are chained together, and independent steps run in parallel.

## Run

```bash
python -m lite_server serve --model-repo examples/05_ensemble/model_repo
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
