# Examples

Each example is a self-contained directory with its own model repository, README, and runnable code.

## List

| # | Example | Description | Key Feature |
|---|---------|-------------|-------------|
| 01 | [basic](01_basic/) | Minimal echo model | `LitAPI` basics |
| 02 | [batching](02_batching/) | Request batching | `max_batch_size`, adaptive batching |
| 03 | [streaming](03_streaming/) | Token-by-token output | `stream_predict()`, SSE, WebSocket |
| 04 | [multi_version](04_multi_version/) | Version switching | `orchestration.yaml`, activate/deactivate |
| 05 | [ensemble](05_ensemble/) | Multi-model DAG pipeline | Ensemble config, parallel execution |
| 06 | [custom_endpoint](06_custom_endpoint/) | Custom HTTP routes | `*_endpoint.py` auto-discovery |

## Legacy

| Example | Description |
|---------|-------------|
| [model_repo/test_model](model_repo/test_model/) | Original test model (minimal echo) |
| [model_repo/status_endpoint.py](model_repo/status_endpoint.py) | Custom endpoint example |

## Running Any Example

```bash
# From the project root
python -m lite_server serve --model-repo examples/<example>/model_repo

# Or with the Rust binary
lite-server-core serve --model-repo examples/<example>/model_repo
```

Each example's README has specific test commands.
