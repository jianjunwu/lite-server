# Deployment

- [Graceful Shutdown & Rolling Updates](#graceful-shutdown--rolling-updates)
- [Autoscaling with KEDA](#autoscaling-with-keda)

## Graceful Shutdown & Rolling Updates

lite-server drains in-flight work on `SIGTERM`/`SIGINT` so a rolling update
does not cut requests mid-flight. This doc is the operator-facing companion
to that behavior — the rolling-update 502 is the single most common
gateway/backend failure, and it is almost always a misconfigured drain
window.

### What the server does on SIGTERM

1. **Health fails fast (LB 摘流).** The shared `draining` flag flips on at
   the very start of shutdown:
   - HTTP `/livez`, `/readyz` → `503` (`{"status":"draining"}`).
   - The gRPC `grpc.health.v1` overall service (`""`) → `NOT_SERVING`.
   - New non-probe HTTP requests → `503 server draining` (the draining gate).
   A Kubernetes readiness/liveness probe and a gRPC LB health watcher
   therefore see the server as not-ready **before** connections are torn
   down.
2. **Stop accepting new connections / streams.** HTTP stops `accept()`;
   gRPC sends `GOAWAY` and rejects new streams (tonic `serve_with_shutdown`
   / `serve_with_incoming_shutdown`).
3. **Drain in-flight.** In-flight HTTP requests and gRPC RPCs are allowed to
   finish. A long-lived SSE / gRPC stream runs until it ends naturally or,
   near the end of the drain window, is asked to wrap up via the negotiated
   close (`server.shutdown_stream_grace_ms`, default 2000ms — same protocol
   as the rolling-recycle grace cancel): a cooperative model ends the stream
   with a normal `Done`; whatever survives the grace window is evicted with
   a terminal error frame, never a silently dropped connection. Set
   `shutdown_stream_grace_ms: 0` for the legacy hard-cut at the backstop.
4. **Grace backstop.** After `server.graceful_timeout` (default `30`s),
   still-running server tasks are force-aborted. (An individual request is
   also bound by its own per-request deadline, `server.timeout`, and the
   worker unload grace — so a stuck request never pins the process open
   forever.)

### Recommended Kubernetes manifest

```yaml
spec:
  template:
    spec:
      # terminationGracePeriodSeconds MUST exceed the whole drain:
      #   preStop sleep + server.graceful_timeout + slack
      # default graceful_timeout is 30s → 45 + 30 + ~15 ≈ 90. Round up.
      terminationGracePeriodSeconds: 90
      containers:
        - name: lite-server
          # Give the LB time to observe the failing readyz and stop sending new
          # traffic BEFORE SIGTERM arrives. 5–15s is typical; match your LB's
          # probe interval × polling jitter.
          lifecycle:
            preStop:
              exec:
                command: ["sleep", "10"]
          # readyz is the readiness signal; livez is liveness. Once draining,
          # readyz flips 503 and the kubelet removes the pod from Endpoints —
          # but only on the next probe, which is why preStop sleep matters.
          readinessProbe:
            httpGet: { path: /readyz, port: http }
            periodSeconds: 5
            failureThreshold: 1
          livenessProbe:
            httpGet: { path: /livez, port: http }
            periodSeconds: 10
            failureThreshold: 3
```

#### Grace calculation

```
terminationGracePeriodSeconds  >  preStop_sleep + graceful_timeout + slack
```

- `preStop_sleep` (≈ LB probe interval, 5–15s): time for the LB to notice
  `readyz` failing and stop routing new connections.
- `graceful_timeout` (default 30s): the server's own drain backstop. Set
  `server.graceful_timeout` in `server.yaml` to bound how long an in-flight
  request may keep the pod alive.
- `slack` (≈10–15s): worker unload, OS teardown, slow shutdown of long
  streams.

If you raise `graceful_timeout` for long streams (e.g. 90–300s for long SSE),
raise `terminationGracePeriodSeconds` to match or the kubelet SIGKILLs the pod
mid-drain — the exact 502 this design avoids.

#### PodDisruptionBudget + maxUnavailable

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: lite-server
spec:
  minAvailable: 1           # or maxUnavailable: 1
  selector:
    matchLabels: { app: lite-server }
```

Pair with `maxUnavailable: 0` (or `1`) in the Deployment rollout so voluntary
disruptions never take the last replica. With `maxUnavailable: 0` the rollout
surges a new pod first and only drains an old one once the new pod is `ready` —
no capacity gap.

### Do NOT use sticky sessions

Sticky sessions (session affinity, consistent-hash LB routing) are incompatible
with rolling updates: any pod add/remove reshuffles the hash ring and breaks
every "sticky" affinity at once, producing a thundering herd of reconnects.
lite-server is stateless per request — route each request independently and put
any session state in an external store (Redis, DB). Long-lived SSE/gRPC streams
are client→pod affined for their duration only; when that pod drains, the stream
ends with a close frame / GOAWAY and the client reconnects to a healthy pod.

## Autoscaling with KEDA

lite-server exposes Prometheus metrics that KEDA can scale on. The recommended
signal is **`liteserver_queue_depth{model,version}`** — requests waiting in the
dispatch queue; it rises before latency degrades. `liteserver_active_workers`
and the derived in-flight count are secondary signals.

**Hard constraints (design decisions):**

- **No scale-to-zero** — `minReplicaCount` must stay ≥ 1. gRPC streaming
  drain semantics conflict with HTTP-proxy scale-to-zero layers
  (KEDA-HTTP / Knative), and a cold model load is far too slow for a
  zero→one wakeup. Scale-to-zero, if ever needed, belongs to an upstream
  gateway, not this server.
- **Slow scale-down** — streaming/bidi connections are long-lived. Use a
  long `cooldownPeriod` and rely on the server's graceful shutdown
  (`graceful_timeout`) to drain in-flight streams; set the pod
  `terminationGracePeriodSeconds` accordingly.

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
  the queue is the server's admission buffer, and its total predicts
  saturation.
- `metrics.metric_namespace` (default `liteserver`) additionally exposes
  GIE/EPP-compatible names (`liteserver:total_queued_requests`,
  `liteserver:kv_cache_utilization`) for tooling that expects the vllm-style
  naming.
- Scaling *up* too aggressively just moves the wait from the queue to model
  loading — pair `maxReplicaCount` with how fast a worker becomes ready on
  your hardware (see `liteserver_model_ready`).
