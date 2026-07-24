# 10 异步模型

演示统一异步流水线：任何方法都可以是 `async def` — 不需要单独的基类（0.7.0 起 `AsyncLitAPI` 已移除，所有模型运行在同一个 asyncio 事件循环上）。

[English](README.md)

## 运行

```bash
cd examples/10_async
python -m lite_server serve --config server.yaml
```

## 测试

```bash
curl -X POST http://localhost:8000/v2/models/async_echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "async_echo: hello"}
```

发送并发请求观察异步 worker 无阻塞处理：

```bash
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/v2/models/async_echo/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": \"msg-$i\"}" &
done
wait
```

## 学习要点

- 继承 `LitAPI` 并把 `predict()` 写成 `async def` — 仅此而已
- `decode_request` / `encode_response` / 钩子各自可以是同步或异步 — worker 在加载时自动适配
- `setup()` 始终保持同步
- 使用 `asyncio.sleep` 或 `await` 进行 I/O 密集型操作以保持事件循环响应
