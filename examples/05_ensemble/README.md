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
  pipeline/1/
    config.yaml     # Ensemble DAG definition (no model.py needed)
    dag.py          # Optional E9-A Python declaration (declaration only)
  caption_stream/1/
    model.py        # Streaming captioner (stream_predict generator)
    config.yaml
  stream_pipeline/1/
    config.yaml     # Streaming ensemble DAG (tail stream: true)
  mmdemo/1/
    config.yaml     # MIMO named inputs + step.outputs + multi-sink outputs
```

## Python Declaration (E9-A)

The `pipeline` DAG is also declared in Python at `model_repo/pipeline/1/dag.py`:

```python
from lite_server import EnsembleDAG, Step

dag = EnsembleDAG(
    steps=[
        Step(name="preprocess", model="step_a", version="1",
             inputs={"input": "$request.input"}),
        Step(name="postprocess", model="step_b", version="1",
             inputs={"input": "$preprocess.output"}),
    ],
)
```

The declaration **serializes to the equivalent config.yaml**; the server
executes config.yaml — `dag.py` is an authoring surface only and is never
run. Cross-check for drift:

```bash
python -m lite_server analyze --model pipeline --model-repo ./model_repo
# drift → warning LS112
```

## Streaming Ensemble (tail streaming)

`stream_pipeline` chains the unary `step_a` preprocess into a **streaming
tail** (`caption_stream`, `stream: true`). The pre-layer finishes first,
then the DAG returns the tail model's chunk stream:

```bash
curl -N -X POST http://localhost:8000/v2/models/stream_pipeline/events \
  -H 'Content-Type: application/json' \
  -d '{"input": "a picture of a cat"}'
# => SSE events: {"token":"a","index":0} {"token":"picture","index":1} ...
```

Notes:

- The DAG performs **no chunk-content conversion** — the tail model owns
  the chunk format (OpenAI-style chunks require an OpenAI-style tail model).
- A streaming DAG is rejected with 400 on unary endpoints; use the
  streaming endpoints (`/events`, `/generate_stream`, `/v1`, gRPC
  server-streaming, WS/h2/gRPC bidi).
- **Unary steps queue** (bounded by `max_queue_size`); **streaming steps
  bypass the queue** (the direct streaming path). Concurrent streaming
  DAGs are bounded globally by `server.max_concurrent_streaming_dags`
  (default 128 — immediate 429 when exhausted).

## MIMO: named multi-input / multi-output (KServe envelope)

`mmdemo` declares two named request inputs, a `step.outputs` JSON
projection, and multi-sink outputs:

```bash
curl -X POST http://localhost:8000/v2/models/mmdemo/infer \
  -H 'Content-Type: application/json' \
  -d '{
    "inputs": [
      {"name": "text", "data": {"input": "hello"}}
      // system_prompt omitted — optional with default
    ]
  }'
# => {"model_name":"mmdemo","outputs":[
#      {"name":"answer","data":"preprocessed(hello) -> done"},
#      {"name":"echo_text","data":{"input":"hello"}}]}
```

Named inputs use the **KServe V2 envelope wire**: each element matches an
`inputs` declaration by name (`type: json` carries its value in `data`;
`type: binary` elements carry `parameters.binary_data_size` and the raw
bytes ride a binary tail — HTTP uses the `inference-header-content-length`
header, gRPC/WS/h2 use the in-frame LSBE-1 container). Optional inputs
without a `default` make the referencing step conditional (absent → step
skipped → its sink is `null`). Declared-input models accept **only** the
envelope shape (400 otherwise); legacy configs without `inputs` keep the
historical plain-JSON body, byte-identical.

More: [protocol.md](../../docs/protocol.md) (KServe V2),
[model-authoring.md](../../docs/model-authoring.md), and the full
reference in [configuration.md](../../docs/configuration.md).

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
