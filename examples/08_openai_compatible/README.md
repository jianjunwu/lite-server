# OpenAI-Compatible Endpoint

Demonstrates using `OpenAIEndpoint` base class to create a `/v1/chat/completions` compatible endpoint.

## Key Concepts

- `OpenAIEndpoint` base class — zero boilerplate for OpenAI-compatible APIs
- Auto-registers `/v1/chat/completions` route
- Streaming support via `stream_predict()`
- Compatible with OpenAI client libraries

## Model Structure

```
model_repo/
  openai_chat/
    1/
      model.py      # ChatModel(OpenAIEndpoint)
      config.yaml   # stream: true
```

## Running

```bash
# From project root
python -m lite_server serve --model-repo examples/08_openai_compatible/model_repo
```

## Testing

### Non-streaming request

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "demo-chat",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

Expected response:

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

### Streaming request

```bash
curl -N -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "demo-chat",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

Expected output (SSE format):

```
data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"E"},"index":0}]}

data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"c"},"index":0}]}

...

data: {"choices":[{"delta":{},"index":0,"finish_reason":"stop"}]}

data: [DONE]
```

### Using OpenAI Python client

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="not-needed")

# Non-streaming
response = client.chat.completions.create(
    model="demo-chat",
    messages=[{"role": "user", "content": "Hello!"}]
)
print(response.choices[0].message.content)

# Streaming
stream = client.chat.completions.create(
    model="demo-chat",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)
for chunk in stream:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```
