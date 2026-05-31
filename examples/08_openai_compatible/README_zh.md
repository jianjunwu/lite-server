# OpenAI 兼容端点

演示使用 `OpenAIEndpoint` 基类创建兼容 `/v1/chat/completions` 的端点。

## 核心概念

- `OpenAIEndpoint` 基类 — 零样板代码创建 OpenAI 兼容 API
- 自动注册 `/v1/chat/completions` 路由
- 通过 `stream_predict()` 支持流式输出
- 兼容 OpenAI 客户端库

## 模型结构

```
model_repo/
  openai_chat/
    1/
      model.py      # ChatModel(OpenAIEndpoint)
      config.yaml   # stream: true
```

## 运行

```bash
# 从项目根目录
python -m lite_server serve --model-repo examples/08_openai_compatible/model_repo
```

## 测试

### 非流式请求

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "demo-chat",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

预期响应：

```json
{
  "id": "chatcmpl-xxxxxxxx",
  "object": "chat.completion",
  "created": 1234567890,
  "model": "demo-chat",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "Echo: Hello!"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": 0, "completion_tokens": 0}
}
```

### 流式请求

```bash
curl -N -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "demo-chat",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

预期输出（SSE 格式）：

```
data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"E"},"index":0}]}

data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"c"},"index":0}]}

...

data: {"choices":[{"delta":{},"index":0,"finish_reason":"stop"}]}

data: [DONE]
```

### 使用 OpenAI Python 客户端

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="not-needed")

# 非流式
response = client.chat.completions.create(
    model="demo-chat",
    messages=[{"role": "user", "content": "Hello!"}]
)
print(response.choices[0].message.content)

# 流式
stream = client.chat.completions.create(
    model="demo-chat",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)
for chunk in stream:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```
