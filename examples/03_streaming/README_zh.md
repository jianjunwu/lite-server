# 03 流式输出

演示服务端流式输出。每个请求产生多个 chunk，通过 SSE 或 WebSocket 发送，实现实时的逐 token 输出。

[English](README.md)

## 运行

```bash
cd examples/03_streaming
python -m lite_server serve --config server.yaml
```

## 测试

### SSE 流式

```bash
curl -N -X POST http://localhost:8000/v2/models/streaming/events \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "hello world test example", "max_tokens": 4}'
```

每个 chunk 作为 SSE 事件到达：
```
data: {"token": "hello", "index": 0}
data: {"token": "world", "index": 1}
data: {"token": "test", "index": 2}
data: {"token": "example", "index": 3}
```

### WebSocket 流式

```bash
# 安装 websocat: brew install websocat
echo '{"prompt": "hello world test", "max_tokens": 3}' | \
  websocat ws://localhost:8000/v2/models/streaming/stream
```

## 学习要点

- 如何实现 `stream_predict()` 进行流式输出
- 如何通过 config.yaml 中的 `stream: true` 启用流式
- SSE（`/events`）和 WebSocket（`/stream`）端点的区别
- `predict()` 作为非流式回退

## 关键配置

```yaml
stream: true
```

## 关键代码

```python
def stream_predict(self, request):
    """逐 token 生成输出。"""
    words = request.get("prompt", "").split()
    for i, word in enumerate(words[:request.get("max_tokens", 5)]):
        time.sleep(0.05)  # 模拟延迟
        yield {"token": word, "index": i}
```
