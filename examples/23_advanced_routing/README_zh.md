# 23. 高级路由（P8-1 / P9-1）

**序列粘性路由**把客户端序列钉到同一个 worker；**DecoupledInfer** 给模型提供 gRPC 上的 1:N 推送流，流生命周期由模型自己控制。

[English](README.md)

## 本示例演示

- `x-sequence-id` 请求头（P8-1）——共享同一序列 id 的请求全部路由到**同一个 worker**（响应携带 worker `pid` 可验证）。`server.sequence_ttl_secs` / `max_sequences` 限制粘性映射规模。不带该头 = 路由与之前完全一致（最少负载）。
- `predict_decoupled(data, sender)`（P9-1）——与 `stream_predict`（worker 拉取的生成器）不同，模型拿到一个推送 `sender`，可以**在流结束前就返回**：异步推送 N 个 chunk，最后 `await sender.close()` 收尾。服务器在空闲超时（`decoupled_idle_timeout_secs`）或客户端断开时回收流。

## 目录结构

```
model_repo/
  sticky_echo/v1/
    model.py       — 每次请求回显 pid；predict_decoupled 推送 3 个 chunk
    config.yaml    — 3 个 worker，让粘性可观察
server.yaml        — sequence_ttl_secs
```

## 运行

```bash
lite-server serve --config server.yaml
```

## 验证

```bash
# 1. 序列粘性——相同 x-sequence-id 的 5 个请求命中同一 worker（pid 不变）：
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/v2/models/sticky_echo/infer \
    -H 'Content-Type: application/json' -H 'x-sequence-id: session-42' \
    -d '{"input": 1}'
  echo
done
# => {"output": {"echo": 1, "pid": 12345}} × 5   （每次 pid 相同）

# 2. 不同序列可能落在不同 worker：
curl -s -X POST http://localhost:8000/v2/models/sticky_echo/infer \
  -H 'Content-Type: application/json' -H 'x-sequence-id: session-1' -d '{"input": 1}'; echo
curl -s -X POST http://localhost:8000/v2/models/sticky_echo/infer \
  -H 'Content-Type: application/json' -H 'x-sequence-id: session-2' -d '{"input": 1}'; echo

# 3. gRPC 上的 DecoupledInfer——模型推送 3 个 chunk 后关闭
#    （Python 客户端见 run_all.py 的 check_23）：
#    chunk 0 → {"chunk": 0, "echo": 1, "pid": ...}
#    chunk 1 → {"chunk": 1, ...}
#    chunk 2 → {"chunk": 2, ...}
#    final   → is_final=true（流由模型关闭）
```

## 说明

- `sequence_ttl_secs` 是软钉：被钉 worker 过载（超过 `balance_abs_threshold` / `balance_rel_threshold`）时会放弃粘性，序列重新钉到最少负载的 worker。
- DecoupledInfer 仅走 gRPC；序列 id 也流过 `BidiStream`/`StreamInfer` 实现粘性流式。
