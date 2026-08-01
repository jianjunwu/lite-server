# Autoscaling with KEDA

[中文版](zh/keda.md)

lite-server exposes Prometheus metrics that KEDA can scale on. The recommended
signal is **`liteserver_queue_depth{model,version}`** — requests waiting in the
dispatch queue; it rises before latency degrades. `liteserver_active_workers` and
the derived in-flight count are secondary signals.

**Hard constraints (design decisions, D35):**

- **No scale-to-zero** — `minReplicaCount` must stay ≥ 1. gRPC streaming drain
  semantics conflict with HTTP-proxy scale-to-zero layers (KEDA-HTTP / Knative),
  and a cold model load is far too slow for a zero→one wakeup. Scale-to-zero, if
  ever needed, belongs to an upstream gateway, not this server.
- **Slow scale-down** — streaming/bidi connections are long-lived. Use a long
  `cooldownPeriod` and rely on the server's graceful shutdown (`graceful_timeout`)
  to drain in-flight streams; set the pod `terminationGracePeriodSeconds`
  accordingly.

Ready-to-apply manifest: [examples/keda-scaledobject.yaml](examples/keda-scaledobject.yaml)

```yaml
triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus-operated.monitoring:9090
      metricName: liteserver_queue_depth
      query: sum(liteserver_queue_depth)   # cluster-wide backlog
      threshold: "8"                        # target ~8 queued requests per replica
```

Notes:

- Prefer `sum(liteserver_queue_depth)` (total backlog) over per-pod averages —
  the queue is the server's admission buffer, and its total predicts saturation.
- `metrics.metric_namespace` (default `liteserver`) additionally exposes
  GIE/EPP-compatible names (`liteserver:total_queued_requests`,
  `liteserver:kv_cache_utilization`) for tooling that expects the vllm-style
  naming (P2-1, D32).
- Scaling *up* too aggressively just moves the wait from the queue to model
  loading — pair `maxReplicaCount` with how fast a worker becomes ready on your
  hardware (see `liteserver_model_ready`).
