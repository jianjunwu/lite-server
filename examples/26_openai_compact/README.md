# 26 openai-compact

Demonstrates the **OpenAI-compatible compact subset** under `/v1` — chat
completions (unary + SSE streaming), embeddings, and model listing — served
by LitAPI models whose workers translate via
`lite_server.helpers.openai`.

[中文](README_zh.md)

## Key Concept

The server **thin-forwards** `/v1` (it parses only `model`/`stream` for
routing and SSE framing); the translation layer lives on the **worker side**:
chat request parsing and completion/chunk/embeddings construction happen in
`lite_server/helpers/openai.py` (`parse_chat_request`,
`build_chat_response`, `build_chat_chunk`, `build_embeddings_response`).
Chat → tensor is model semantics — the server cannot translate it generically.

| Endpoint | Model | Behavior |
|---|---|---|
| `POST /v1/chat/completions` | `chat` | unary JSON when `stream: false`; SSE (`data: {json}` + `data: [DONE]`) when `stream: true` |
| `POST /v1/embeddings` | `embed` | deterministic pseudo-embedding, OpenAI `list` shape |
| `GET /v1/models` | — | registered model names |
| `GET /v1/models/{model}` | — | single model object; missing → 404 |

## Run

```bash
cd examples/26_openai_compact
python -m lite_server serve --config server.yaml
```

## Test

### Chat completion (unary)

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "chat", "messages": [{"role": "user", "content": "hi there"}]}'
# => {"id": "...", "object": "chat.completion", "model": "chat",
#     "choices": [{"index": 0, "message": {"role": "assistant",
#       "content": "chat echo: hi there"}, "finish_reason": "stop"}]}
```

### Chat completion (SSE streaming)

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

### Model listing

```bash
curl http://localhost:8000/v1/models
# => {"object": "list", "data": [{"id": "chat", ...}, {"id": "embed", ...}]}

curl http://localhost:8000/v1/models/nope
# => 404 model_not_found
```

### Official `openai` client (optional)

Point the official Python client at the server — only the base URL changes:

```python
import openai

client = openai.OpenAI(base_url="http://localhost:8000/v1", api_key="unused")
resp = client.chat.completions.create(
    model="chat",
    messages=[{"role": "user", "content": "hi there"}],
)
print(resp.choices[0].message.content)  # chat echo: hi there
```
