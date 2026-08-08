# openai-compact (0.9.x, batch 5)

OpenAI API-compatible compact subset, 5 endpoints (root `/v1`, v5 J5 ruling):

| Endpoint | Description |
|---|---|
| `POST /v1/chat/completions` | Chat completion (unary JSON or `stream: true` SSE) |
| `POST /v1/completions` | Text completion (thin-forwarded to the worker) |
| `POST /v1/embeddings` | Embeddings (thin-forwarded to the worker) |
| `GET /v1/models` | Registered model names |
| `GET /v1/models/{model}` | Single model object; missing → 404 (OpenAI shape) |

**The translation layer lives on the worker side** (J6/C19): the server
thin-forwards /v1 — the body is parsed only for the `model`/`stream` fields
(routing + demux) plus SSE frame encoding (`data: {json}` + `data: [DONE]`);
chat request parsing / completion·chunk·embeddings construction live in the
worker-side helper [`lite_server/helpers/openai.py`](../python/lite_server/helpers/openai.py)
(chat→tensor is model semantics; the server cannot translate generically).
`/v1/rerank` is not implemented (not an OpenAI API — see
[known-deviations.md](known-deviations.md)).

## Worker integration paradigm

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

    async def stream_predict(self, x, ctx):  # stream: true → SSE per chunk
        for token in my_generate_stream(x["messages"]):
            yield build_chat_chunk(token, model=x["model"])
```

- **`stream: true`** → SSE (`data: {json}` per chunk + `data: [DONE]`); the
  HTTP status is fixed by the first SSE response and mid-stream errors arrive
  in later `data: {"error": {...}}` events (OpenAI SSE convention).
- **`stream: false`** → unary JSON (`chat.completion` shape).
- Error mapping (OpenAI semantics): 400 invalid_request_error (missing
  `model` / malformed body), 404 model_not_found (model does not exist),
  429/503 follow existing semantics.
- Binary (the WS/h2 bidi own channels) and OpenAI SSE do not interfere.
- `model` lives in the body (not the path) → access log classifies /v1 as
  inference with the path as target; no per-model access log (v5 ruling).
