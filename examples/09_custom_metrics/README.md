# 09 Custom Metrics

Demonstrates how to collect custom Prometheus metrics (gauge, counter, histogram) from model code using `register_metric()` and `report_metric()`.

## Run

```bash
# From the project root
python -m lite_server serve --model-repo examples/09_custom_metrics/model_repo
```

## Test

```bash
# Send some requests
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/v2/models/metrics_demo/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}"
done
# => {"output": 2}, {"output": 4}, ...

# Check custom metrics in Prometheus output
curl -s http://localhost:8000/metrics | grep demo_
# => lite_server_demo_batch_size{model="metrics_demo"} 1
# => lite_server_demo_predictions_total_total{model="metrics_demo"} 10
# => lite_server_demo_inference_ms_count{model="metrics_demo"} 10
# => lite_server_demo_inference_ms_sum{model="metrics_demo"} ...
```

## What You Learn

- How to pre-register metrics in `setup()` with `register_metric()`
- How to report metric values in `predict()` with `report_metric()`
- How gauge, counter, and histogram metrics appear in `/metrics`
- The pre-registration pattern for zero-allocation hot-path metric reporting
