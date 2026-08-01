# 20. 过载保护（P-FLOW / P-DEADLINE）

保护服务器免受过载冲击：全局**在途上限**拒绝超额推理（`503 + Retry-After`），按模型的**排队超时**拒绝久候请求，客户端可用 deadline 头**限制自己的等待时间**。

[English](README.md)

## 本示例演示

- `server.max_inflight: 2` —— 同时最多 2 个推理在途。超限请求立即返回 `503` 并带 `Retry-After: 1` 头（health/admin 端点不受影响——负载下探活仍可用）。默认 `0` = 不限制。
- `queue_timeout_secs` + `queue_timeout_action: reject`（按模型）——排队超过 1 秒的请求返回 `503 (queue_full)` + `Retry-After`。
- `x-lite-timeout` —— 客户端自定义相对 deadline（秒，浮点）。服务器到点停止等待并返回 `504 Gateway Timeout`。gRPC 侧用标准 `grpc-timeout` metadata 达到同样效果。
- `x-lite-priority` —— 整数请求头（越大越先调度，默认 0），用于优先级队列（下方命令演示）。

## 目录结构

```
model_repo/
  slow_echo/1/    — 每次推理 0.8 秒，单 worker
server.yaml       — max_inflight: 2
```

## 运行

```bash
lite-server serve --config server.yaml
```

## 验证

```bash
# 1. 6 个并发请求打满 max_inflight（只有 2 个槽位）：
for i in $(seq 1 6); do
  curl -s -o /dev/null -w "%{http_code} " -X POST \
    http://localhost:8000/v2/models/slow_echo/infer \
    -H 'Content-Type: application/json' -d '{"input": 1}' &
done; wait; echo
# => 200 200 503 503 503 503   （503 都带 Retry-After: 1）

# 2. 显式查看 503 + Retry-After 头：
curl -s -D - -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
sleep 0.05
curl -s -D - -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
sleep 0.05
curl -s -D - -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}'
wait
# 第三个请求 => HTTP/1.1 503 Service Unavailable
#              retry-after: 1

# 3. 用 deadline 限制自己的等待（模型要 0.8 秒）：
curl -s -w "\nHTTP %{http_code}\n" -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -H 'x-lite-timeout: 0.1' \
  -d '{"input": 1}'
# => HTTP 504 Gateway Timeout（立即返回，不等 0.8 秒）

# 4. 优先级队列：两个排队请求，x-lite-priority 高的先调度。
#    先发 2 个慢请求占满槽位，再排一个低优先级和一个高优先级：
curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
sleep 0.05
( time curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
    -H 'Content-Type: application/json' -H 'x-lite-priority: 0' -d '{"input": 1}' ) 2>&1 &
( time curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
    -H 'Content-Type: application/json' -H 'x-lite-priority: 5' -d '{"input": 1}' ) 2>&1 &
wait
# priority=5 的请求先完成，priority=0 的后完成。
```

## 说明

- `max_inflight` 是全局的（跨模型）。只给单个模型设上限请单独部署（或调 worker 数）。
- 排队超时演示需要 `max_inflight: 0`（不限制）才能让超量请求真正排队——`config.yaml` 里的按模型配置已就绪，去掉 `server.max_inflight` 再试即可。
- 不带 `x-lite-timeout` 且 `server.timeout: 0` 时，卡死的 worker 等待没有上限——生产环境请设置服务器级 `timeout`。
