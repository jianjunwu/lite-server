# 27 vLLM 后端

通过单个 `AsyncLLMEngine` serving [yifan02/qwen3-0.6b-tool-distill](https://huggingface.co/yifan02/qwen3-0.6b-tool-distill)：流式、非流式，以及基于 Hermes parser 的 Qwen3 风格 tool calling。

[English](README.md)

## 核心概念

批处理由 vLLM 内部 scheduler(PagedAttention）接管，因此禁用 lite-server 连续批处理。模型类只做 vLLM 到管线的适配：

- **引擎懒初始化** — `AsyncLLMEngine` 需要在运行中的 event loop 内创建，因此延迟到首次 `predict()`（由 warmup 在版本进入 `Ready` 前触发）;`setup()` 只存配置，并在任何 vLLM/CUDA import 之前设置 `CUDA_VISIBLE_DEVICES`。
- **单一推理入口** — `stream_predict()` 同时覆盖流式与非流式，`predict()` 是非流式的薄代理。
- **增量切片** — vLLM 每次 yield 的是*累积*文本，模型自行切出每帧 delta(`text[sent:]`)。
- **断连即 abort** — 客户端断连时框架取消消费任务；模型捕获 `CancelledError` 并调用 `engine.abort(request_id)`,GPU 在下一个调度步停止计算。

## 前置条件

```bash
pip install vllm   # 需要 CUDA GPU；首次运行会从 HF 下载模型
```

没有 GPU？附带的 [Dockerfile](Dockerfile) 可以在 Linux 容器里用 CPU 跑全栈（适用于 vLLM 没有原生构建的 macOS/Windows 宿主机）。**Docker Desktop 虚拟机内存需 ≥ 6 GB**——vLLM CPU 内存利用率（0.6）目标 ~4.7 GB，更小的 VM 会拒绝启动。

```bash
# 一次性：先构建 vLLM CPU 基础镜像，再构建本镜像
docker build -t lite-vllm-example .
docker run --rm -p 8000:8000 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  lite-vllm-example
```

> CPU 首次启动约需 2 分钟（引擎初始化）。期间健康检查可能熔断
> worker（日志出现 `worker ejected` / `Degraded`)——属预期且会
> 自愈，看到 `Activated version 1 for qwen3_tool` 后再发流量。

本 example 不在 `run_all.py` 中（需要 GPU 或 Docker CPU 镜像，以及模型下载）。

## 运行

```bash
cd examples/27_vllm
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 非流式 chat + tool calling
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

# 流式（SSE）：每个事件一个 token 增量，最后一帧附带 tool_calls
curl -N -X POST http://localhost:8000/v2/models/qwen3_tool/events \
  -H 'Content-Type: application/json' \
  -d '{"messages": [{"role": "user", "content": "hi"}], "stream": true, "max_tokens": 64}'

# 纯 prompt（不走 chat template）
curl -X POST http://localhost:8000/v2/models/qwen3_tool/infer \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "The capital of France is", "max_tokens": 16}'
```

## OpenAI 兼容端点

同一模型在 `/v1` 下同时讲 OpenAI 报文（服务端薄转发；模型靠请求体里的 `model` 字段识别 OpenAI 流量，经 `lite_server.helpers.openai` 构造响应形状）:

```bash
# 非流式 chat completion
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "qwen3_tool", "messages": [{"role": "user", "content": "hi"}]}'
# => {"object": "chat.completion", "choices": [{"message": {"role": "assistant", "content": ...}}]}

# SSE 流式
curl -N -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "qwen3_tool", "messages": [{"role": "user", "content": "hi"}], "stream": true}'
# => data: {"object": "chat.completion.chunk", "choices": [{"delta": {"content": ...}}]}  逐 token

# Tool calling：解析结果在 message.tool_calls（非流式）;SSE 以增量
# delta.tool_calls 片段流出（先 name 后 arguments 碎片），经 vLLM
# extract_tool_calls_streaming——原始 <tool_call> 标记不会混入 content
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "qwen3_tool", "messages": [{"role": "user", "content": "weather in Paris?"}],
       "tools": [{"type": "function", "function": {"name": "get_weather", "description": "Get weather",
       "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}}]}'

# 模型列表
curl http://localhost:8000/v1/models
```

说明：非流式响应带 `usage`;SSE 首帧为 `delta.role = "assistant"`；支持
`max_completion_tokens`；省略 `temperature` 时默认 1.0(OpenAI 语义）。
`/v1/embeddings` **不覆盖**——这是纯 chat 模型。

## 学习要点

- 需要运行中 event loop 的引擎如何做懒初始化（`asyncio.Lock` + 双重检查）
- 为什么 `CUDA_VISIBLE_DEVICES` 必须在 `setup()` 内、任何 vLLM import 之前设置；TP 为何用 `visible_devices` 而非 `--device`
- vLLM `generate()` yield 的是累积 `RequestOutput`——真正的逐 token 流式需要自行切 delta
- tool_call 解析时机：非流式解析最终文本；流式在最后一帧（`finish_reason` 非空）解析
- 协作式取消：在 `CancelledError`/`GeneratorExit` 中调用 `engine.abort(request_id)`
- 双协议：`/v2` 自定义形状 vs `/v1` OpenAI 形状，靠请求体 `model` 字段区分（`ctx.meta.route` 两种流量都是 `"/predict"`，服务端做了归一化）
