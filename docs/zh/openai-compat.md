# openai-compact（0.9.x，批次 5）

OpenAI API 兼容紧凑子集，5 端点（根 `/v1`，v5 J5 裁定）：

| 端点 | 说明 |
|---|---|
| `POST /v1/chat/completions` | chat 补全（unary JSON 或 `stream: true` SSE） |
| `POST /v1/completions` | 文本补全（透传 worker） |
| `POST /v1/embeddings` | 嵌入（透传 worker） |
| `GET /v1/models` | 注册模型名列表 |
| `GET /v1/models/{model}` | 单模型对象；不存在 → 404（OpenAI 形状） |

**翻译层在 worker 侧**（J6/C19）：server 对 /v1 薄透传——body 最小解析仅
`model`/`stream` 两字段用于路由与分流 + SSE 帧编码（`data: {json}` +
`data: [DONE]`）；chat 请求解析 / completion·chunk·embeddings 构造全部在
worker 侧 helper [`lite_server/helpers/openai.py`](../../python/lite_server/helpers/openai.py)
（chat→tensor 是模型语义，server 无法通用翻译）。`/v1/rerank` 不做（非
OpenAI API，见 [known-deviations.md](known-deviations.md)）。

## Worker 集成范式

```python
from lite_server import LitAPI
from lite_server.helpers.openai import (
    parse_chat_request, build_chat_response, build_chat_chunk,
)


class ChatModel(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return parse_chat_request(request)   # {messages, model, stream, ...}

    def predict(self, x, ctx):               # stream: false → unary JSON
        reply = my_generate(x["messages"])
        return build_chat_response(reply, model=x["model"],
                                   request_id=ctx.meta.request_id)

    async def stream_predict(self, x, ctx):  # stream: true → SSE 逐 chunk
        for token in my_generate_stream(x["messages"]):
            yield build_chat_chunk(token, model=x["model"])
```

- **`stream: true`** → SSE（`data: {json}` 逐 chunk + `data: [DONE]`）；HTTP
  状态码由首个 SSE 响应固定，流中途错误携带在后续 `data: {"error": {...}}`
  事件内（OpenAI SSE 惯例）。
- **`stream: false`** → unary JSON（`chat.completion` 形状）。
- 错误码映射（OpenAI 语义）：400 invalid_request_error（缺 `model`/畸形
  body）、404 model_not_found（模型不存在）、429/503 沿用既有语义。
- 二进制（WS/h2 bidi 自有通道）与 OpenAI SSE 互不干扰。
- `model` 字段在 body（非路径）→ access log 归 inference 族、target 用路径、
  无 per-model access log（v5 裁定）。
