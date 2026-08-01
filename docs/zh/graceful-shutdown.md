# 优雅关闭与滚动更新

lite-server 在 `SIGTERM`/`SIGINT` 时排空在途请求（P4-2），滚动更新不会截断处理中的请求。本文是面向运维的行为说明——滚动更新时的 502 是最常见的网关/后端故障，而它几乎总是排空窗口配置不当造成的。

## SIGTERM 时服务器做什么

1. **健康检查快速失败（LB 摘流）。** 共享的 `draining` 标志在关闭一开始就翻转：
   - HTTP `/livez`、`/readyz` → `503`（`{"status":"draining"}`）。
   - gRPC `grpc.health.v1` 整体服务（`""`）→ `NOT_SERVING`。
   - 新的非探针 HTTP 请求 → `503 server draining`（draining 门）。
   因此 Kubernetes 就绪/存活探针和 gRPC LB 健康 watcher 会在连接被拆除**之前**看到服务器不可用。
2. **停止接受新连接/流。** HTTP 停止 `accept()`；gRPC 发送 `GOAWAY` 并拒绝新流（tonic `serve_with_shutdown` / `serve_with_incoming_shutdown`）。
3. **排空在途请求。** 允许在途 HTTP 请求和 gRPC RPC 完成。长连接 SSE / gRPC 流运行到自然结束或宽限期耗尽。
4. **兜底强制结束。** `server.graceful_timeout`（默认 `30` 秒）之后，仍在运行的服务器任务被强制中止。（单个请求也受自身 per-request deadline `server.timeout` 和 worker 卸载宽限约束——卡死的请求不会把进程永久钉住。）

## 推荐的 Kubernetes manifest

```yaml
spec:
  template:
    spec:
      # terminationGracePeriodSeconds 必须大于整个排空时间：
      #   preStop sleep + server.graceful_timeout + 余量
      # 默认 graceful_timeout 是 30s → 45 + 30 + ~15 ≈ 90。向上取整。
      terminationGracePeriodSeconds: 90
      containers:
        - name: lite-server
          # 给 LB 时间观察 readyz 失败并在 SIGTERM 到达前停止转发新流量。
          # 通常 5–15s；匹配你的 LB 探针间隔 × 轮询抖动。
          lifecycle:
            preStop:
              exec:
                command: ["sleep", "10"]
          # readyz 是就绪信号；livez 是存活信号。一旦 draining，
          # readyz 翻转为 503，kubelet 把 pod 从 Endpoints 摘除——
          # 但只在下一个探针周期生效，这正是 preStop sleep 的意义。
          readinessProbe:
            httpGet: { path: /readyz, port: http }
            periodSeconds: 5
            failureThreshold: 1
          livenessProbe:
            httpGet: { path: /livez, port: http }
            periodSeconds: 10
            failureThreshold: 3
```

### 宽限计算

```
terminationGracePeriodSeconds  >  preStop_sleep + graceful_timeout + slack
```

- `preStop_sleep`（≈ LB 探针间隔，5–15s）：LB 注意到 `readyz` 失败、停止路由新连接所需的时间。
- `graceful_timeout`（默认 30s）：服务器自身的排空兜底。在 `server.yaml` 里设 `server.graceful_timeout` 约束在途请求最多让 pod 存活多久。
- `slack`（≈10–15s）：worker 卸载、OS 收尾、长流缓慢关闭。

如果为长流调大 `graceful_timeout`（例如长 SSE 用 90–300s），必须同步调大 `terminationGracePeriodSeconds`，否则 kubelet 会在排空中途 SIGKILL pod——正是这套设计要避免的 502。

### PodDisruptionBudget + maxUnavailable

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: lite-server
spec:
  minAvailable: 1           # 或 maxUnavailable: 1
  selector:
    matchLabels: { app: lite-server }
```

配合 Deployment 滚动发布中的 `maxUnavailable: 0`（或 `1`），自愿干扰永远不会打掉最后一个副本。`maxUnavailable: 0` 时滚动发布先拉起新 pod，只有新 pod `ready` 后才排空旧 pod——无容量缺口。

## 不要使用粘性会话

粘性会话（session affinity、一致性哈希 LB 路由）与滚动更新不兼容：任何 pod 增删都会重排哈希环并一次性打破所有"粘性"亲和，产生重连惊群。lite-server 每个请求无状态——独立路由每个请求，把会话状态放进外部存储（Redis、DB）。长连接 SSE/gRPC 流只在存续期内客户端→pod 亲和；该 pod 排空时，流以 close frame / GOAWAY 结束，客户端重连到健康 pod。
