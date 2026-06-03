# 09 自定义指标

演示如何使用 `register_metric()` 和 `report_metric()` 在模型代码中收集自定义 Prometheus 指标（gauge、counter、histogram）。

[English](README.md)

## 运行

```bash
cd examples/09_custom_metrics
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 发送一些请求
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/v2/models/metrics_demo/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}"
done
# => {"output": 2}, {"output": 4}, ...

# 在 Prometheus 输出中查看自定义指标
curl -s http://localhost:8000/metrics | grep demo_
# => lite_server_demo_batch_size{model="metrics_demo"} 1
# => lite_server_demo_predictions_total_total{model="metrics_demo"} 10
# => lite_server_demo_inference_ms_count{model="metrics_demo"} 10
# => lite_server_demo_inference_ms_sum{model="metrics_demo"} ...
```

## 学习要点

- 如何在 `setup()` 中用 `register_metric()` 预注册指标
- 如何在 `predict()` 中用 `report_metric()` 上报指标值
- gauge、counter、histogram 指标在 `/metrics` 中的展示格式
- 预注册模式实现热路径零分配的指标上报
