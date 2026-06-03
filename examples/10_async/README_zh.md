# 10 异步模型

演示 `AsyncLitAPI`，用于涉及异步 I/O 的推理流水线（如远程 API 调用、异步模型库）。

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

- 继承 `AsyncLitAPI` 而非 `LitAPI`
- `predict()` 必须是 `async def`
- `decode_request` / `encode_response` / 钩子可以是同步或异步 — worker 自动适配
- `max_batch_size` 强制为 1（异步不支持批处理）
- 使用 `asyncio.sleep` 或 `await` 进行 I/O 密集型操作以保持事件循环响应
