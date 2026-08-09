# 26 openai-compact

演示根路径 `/v1` 的 **OpenAI 兼容紧凑子集** — chat 补全(unary + SSE 流式)、
embeddings、模型列表 — 由 LitAPI 模型通过 `lite_server.helpers.openai`
在 worker 侧翻译提供。

[English](README.md)

## 核心概念

server 对 `/v1` **薄透传**(仅解析 `model`/`stream` 两字段用于路由与
SSE 帧封装);**翻译层在 worker 侧**:chat 请求解析与
completion/chunk/embeddings 构造全部位于 `lite_server/helpers/openai.py`
(`parse_chat_request`、`build_chat_response`、`build_chat_chunk`、
`build_embeddings_response`)。chat → 张量是模型语义,server 无法通用翻译。

| 端点 | 模型 | 行为 |
|---|---|---|
| `POST /v1/chat/completions` | `chat` | `stream: false` 时 unary JSON;`stream: true` 时 SSE(`data: {json}` + `data: [DONE]`) |
| `POST /v1/embeddings` | `embed` | 确定性伪 embedding,OpenAI `list` 形状 |
| `GET /v1/models` | — | 已注册模型名列表 |
| `GET /v1/models/{model}` | — | 单模型对象;不存在 → 404 |

## 运行

```bash
cd examples/26_openai_compact
python -m lite_server serve --config server.yaml
```

## 测试

### Chat 补全(unary)

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "chat", "messages": [{"role": "user", "content": "hi there"}]}'
# => {"id": "...", "object": "chat.completion", "model": "chat",
#     "choices": [{"index": 0, "message": {"role": "assistant",
#       "content": "chat echo: hi there"}, "finish_reason": "stop"}]}
```

### Chat 补全(SSE 流式)

```bash
curl -N -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "chat", "stream": true, "messages": [{"role": "user", "content": "a b c"}]}'
# => data: {"id": "...", "object": "chat.completion.chunk", "choices": [{"delta": {"content": "a"}}]}
# => data: {... "delta": {"content": "b"} ...}
# => data: {... "delta": {"content": "c"} ...}
# => data: {... "delta": {"content": ""}, "finish_reason": "stop"}
# => data: [DONE]
```

### Embeddings

```bash
curl -X POST http://localhost:8000/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model": "embed", "input": "hey"}'
# => {"object": "list", "data": [{"object": "embedding", "index": 0,
#     "embedding": [104.0, 101.0, 121.0]}], "model": "embed", ...}
```

### 模型列表

```bash
curl http://localhost:8000/v1/models
# => {"object": "list", "data": [{"id": "chat", ...}, {"id": "embed", ...}]}

curl http://localhost:8000/v1/models/nope
# => 404 model_not_found
```

### 官方 `openai` 客户端(可选)

官方 Python 客户端只需改 base_url 即可指向本 server:

```python
import openai

client = openai.OpenAI(base_url="http://localhost:8000/v1", api_key="unused")
resp = client.chat.completions.create(
    model="chat",
    messages=[{"role": "user", "content": "hi there"}],
)
print(resp.choices[0].message.content)  # chat echo: hi there
```
