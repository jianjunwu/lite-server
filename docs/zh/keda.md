# 使用 KEDA 自动扩缩

[English](../keda.md)

lite-server 暴露的 Prometheus 指标可直接作为 KEDA 扩缩信号。推荐信号是
**`liteserver_queue_depth{model,version}`**——排队等待派发的请求数，它在时延
恶化之前先涨；`liteserver_active_workers` 与派生的 in-flight 数为辅助信号。

**硬约束（设计决策，D35）**：

- **不支持 scale-to-zero**——`minReplicaCount` 必须 ≥ 1。gRPC 流式排空语义与
  HTTP 代理式 scale-to-zero（KEDA-HTTP / Knative）冲突，且冷启动加载模型远
  慢于零→一唤醒的容忍度。如确需 scale-to-zero，应放在上游网关层，而非本服务。
- **慢缩容**——streaming/bidi 连接长存。使用较长的 `cooldownPeriod`，并依靠
  服务端优雅停机（`graceful_timeout`）排空在途流；Pod 的
  `terminationGracePeriodSeconds` 相应调大。

可直接套用的清单：[examples/keda-scaledobject.yaml](../examples/keda-scaledobject.yaml)

```yaml
triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus-operated.monitoring:9090
      metricName: liteserver_queue_depth
      query: sum(liteserver_queue_depth)   # 集群级积压
      threshold: "8"                        # 目标 ≈ 每副本 8 条排队
```

说明：

- 优先用 `sum(liteserver_queue_depth)`（总积压）而非按 Pod 均值——队列是服务
  端的准入缓冲，总量更能预测饱和点。
- `metrics.metric_namespace`（默认 `liteserver`）另暴露 GIE/EPP 兼容名
  （`liteserver:total_queued_requests`、`liteserver:kv_cache_utilization`），
  供需要 vllm 风格命名的工具使用（P2-1，D32）。
- 过激进的**扩容**只会把等待从队列挪到模型加载——`maxReplicaCount` 应结合
  你的硬件上 worker 变 READY 的速度（参考 `liteserver_model_ready`）。
