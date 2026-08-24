# 27 vLLM Backend

Serves [yifan02/qwen3-0.6b-tool-distill](https://huggingface.co/yifan02/qwen3-0.6b-tool-distill) through a single `AsyncLLMEngine`: streaming, non-streaming, and Qwen3-style tool calling via the Hermes parser.

[中文](README_zh.md)

## Key Concept

vLLM's internal scheduler (PagedAttention) owns batching, so lite-server continuous batching is disabled. The model class only adapts vLLM to the pipeline:

- **Lazy engine init** — `AsyncLLMEngine` needs a running event loop, so it is created on the first `predict()` (triggered by warmup, before the version reports `Ready`); `setup()` only stores config and sets `CUDA_VISIBLE_DEVICES` before any vLLM/CUDA import.
- **One inference entry** — `stream_predict()` covers both modes; `predict()` is a thin non-streaming wrapper.
- **Delta slicing** — each vLLM yield carries the *cumulative* text; the model slices per-chunk deltas itself (`text[sent:]`).
- **Disconnect = abort** — on client disconnect the framework cancels the consuming task; the model catches `CancelledError` and calls `engine.abort(request_id)` so GPU compute stops at the next scheduler step.

## Prerequisites

```bash
pip install vllm   # plus a CUDA GPU; the model downloads from HF on first run
```

No GPU? The bundled [Dockerfile](Dockerfile) runs the full stack on CPU in a
Linux container (works on macOS/Windows hosts where vLLM has no native build).
**Docker Desktop VM memory must be ≥ 6 GB** — vLLM's CPU memory utilization
(0.6) targets ~4.7 GB and refuses to start on smaller VMs.

```bash
# one-time: build the vLLM CPU base image, then this image
docker build -t lite-vllm-example .
docker run --rm -p 8000:8000 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  lite-vllm-example
```

> First start takes ~2 min on CPU (engine init). During that window the
> health checker may circuit-break the worker (`worker ejected` /
> `Degraded` in the logs) — this is expected and self-heals; wait for
> `Activated version 1 for qwen3_tool` before sending traffic.

This example is excluded from `run_all.py` (requires a GPU or the Docker CPU image, plus a model download).

## Run

```bash
cd examples/27_vllm
python -m lite_server serve --config server.yaml
```

## Test

```bash
# Non-streaming chat with tool calling
curl -X POST http://localhost:8000/v2/models/qwen3_tool/infer \
  -H 'Content-Type: application/json' \
  -d '{
    "messages": [{"role": "user", "content": "What is the weather in Paris?"}],
    "tools": [{
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Get current weather for a city",
        "parameters": {
          "type": "object",
          "properties": {"city": {"type": "string"}},
          "required": ["city"]
        }
      }
    }],
    "max_tokens": 256
  }'
# => {"text": "<tool_call>{\"name\": \"get_weather\", ...}</tool_call>",
#     "usage": {...},
#     "tool_calls": [{"function": {"name": "get_weather", "arguments": {...}}, ...}]}

# Streaming (SSE): one event per token delta, tool_calls on the final chunk
curl -N -X POST http://localhost:8000/v2/models/qwen3_tool/events \
  -H 'Content-Type: application/json' \
  -d '{"messages": [{"role": "user", "content": "hi"}], "stream": true, "max_tokens": 64}'

# Plain prompt (no chat template)
curl -X POST http://localhost:8000/v2/models/qwen3_tool/infer \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "The capital of France is", "max_tokens": 16}'
```

## OpenAI-Compatible Endpoints

The same model also speaks the OpenAI wire format on `/v1` (the server
thin-forwards; the model detects the OpenAI body by its `model` field and
responds via `lite_server.helpers.openai` shapes):

```bash
# Unary chat completion
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "qwen3_tool", "messages": [{"role": "user", "content": "hi"}]}'
# => {"object": "chat.completion", "choices": [{"message": {"role": "assistant", "content": ...}}]}

# SSE streaming
curl -N -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "qwen3_tool", "messages": [{"role": "user", "content": "hi"}], "stream": true}'
# => data: {"object": "chat.completion.chunk", "choices": [{"delta": {"content": ...}}]}  per token

# Tool calling: parsed calls land in message.tool_calls (unary); SSE streams
# them as incremental delta.tool_calls fragments (name first, then argument
# pieces) via vLLM's extract_tool_calls_streaming — the raw <tool_call>
# markup never leaks into content
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "qwen3_tool", "messages": [{"role": "user", "content": "weather in Paris?"}],
       "tools": [{"type": "function", "function": {"name": "get_weather", "description": "Get weather",
       "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}}]}'

# Model listing
curl http://localhost:8000/v1/models
```

Notes: unary responses include `usage`; SSE starts with a `delta.role =
"assistant"` chunk; `max_completion_tokens` is honored; omitted
`temperature` defaults to 1.0 (OpenAI semantics). `/v1/embeddings` is
**not** covered — this is a chat-only model.

## What You Learn

- Lazy-init pattern for engines that require a running event loop (`asyncio.Lock` + double check)
- Why `CUDA_VISIBLE_DEVICES` must be set in `setup()` before any vLLM import, and why TP uses `visible_devices` instead of `--device`
- vLLM `generate()` yields cumulative `RequestOutput` — slice deltas for true per-token streaming
- Tool-call parsing timing: non-streaming parses the final text; streaming parses on the last chunk (`finish_reason` set)
- Cooperative cancellation: `engine.abort(request_id)` on `CancelledError`/`GeneratorExit`
- Dual protocol: `/v2` custom shapes vs `/v1` OpenAI shapes, told apart by the request body's `model` field (`ctx.meta.route` is `"/predict"` for both — the server normalizes it)
