"""vLLM-backed chat model with streaming and tool calling.

A single AsyncLLMEngine (lazy-created on the first request) serves all
traffic; vLLM's internal scheduler does the batching, so lite-server
continuous batching stays disabled. ``stream_predict()`` is the only
inference entry point and covers both streaming and non-streaming requests.
"""

import asyncio
import os

from lite_server import LitAPI, RequestContext
from lite_server.helpers.openai import (
    build_chat_chunk,
    build_chat_response,
    parse_chat_request,
)
from transformers import AutoTokenizer

# NOTE: all vllm imports stay function-level on purpose — the worker imports
# this module BEFORE setup() runs, and setup() sets CUDA_VISIBLE_DEVICES,
# which must precede any vLLM/CUDA import (see the plan's §3 timing rule).


class Qwen3ToolAPI(LitAPI):
    def setup(self, device):
        # Must be set before any vLLM/CUDA import.
        os.environ["CUDA_VISIBLE_DEVICES"] = str(self.config["visible_devices"])

        # Store config only — the engine is created lazily on first predict,
        # when a running event loop exists (setup runs before asyncio.run).
        self._engine_config = {
            "model": self.config["model_path"],
            "tensor_parallel_size": self.config.get("tensor_parallel_size", 1),
            "max_model_len": self.config.get("max_model_len"),
            "gpu_memory_utilization": self.config.get("gpu_memory_utilization", 0.90),
            "dtype": self.config.get("dtype", "auto"),
            "trust_remote_code": self.config.get("trust_remote_code", False),
            # Skip torch.compile: on CPU the Inductor pass dominates startup
            # (tens of minutes) while buying little at example scale.
            "enforce_eager": self.config.get("enforce_eager", True),
        }
        # "hermes" parses Qwen3-style <tool_call> blocks; None disables parsing.
        self._tool_parser_name = self.config.get("tool_parser")
        self._engine = None
        self._tokenizer = None
        self._init_lock = asyncio.Lock()

    async def _get_engine(self):
        if self._engine is None:
            async with self._init_lock:
                if self._engine is None:
                    from vllm.engine.arg_utils import AsyncEngineArgs
                    from vllm.engine.async_llm_engine import AsyncLLMEngine

                    engine_args = AsyncEngineArgs(**self._engine_config)
                    self._engine = AsyncLLMEngine.from_engine_args(engine_args)
        return self._engine

    async def decode_request(self, request, ctx: RequestContext | None = None):
        # /v1/chat/completions thin-forwards here with the OpenAI body, which
        # always carries "model" (the server routes on it); /v2 bodies in this
        # example never do. ctx.meta.route is "/predict" for both, so the body
        # shape is the signal: OpenAI request → OpenAI wire format out.
        if "model" in request:
            x = parse_chat_request(request)
            x["openai"] = True
            # Newer OpenAI field spelling; SamplingParams only knows max_tokens.
            if "max_completion_tokens" in x and "max_tokens" not in x:
                x["max_tokens"] = x["max_completion_tokens"]
            return x
        return {
            "prompt": request.get("prompt", ""),
            "messages": request.get("messages"),
            "tools": request.get("tools"),
            "stream": request.get("stream", False),
            "max_tokens": request.get("max_tokens", 256),
            "temperature": request.get("temperature", 0.0),
            "top_p": request.get("top_p", 1.0),
            "stop": request.get("stop"),
        }

    async def predict(self, x, ctx: RequestContext | None = None):
        """Thin wrapper: non-streaming is the stream_predict() else-branch."""
        result = None
        async for out in self.stream_predict({**x, "stream": False}, ctx):
            result = out
        return result

    async def stream_predict(self, inputs, ctx: RequestContext | None = None):
        from vllm import SamplingParams

        engine = await self._get_engine()
        # request_id keys vLLM's abort; must be unique per stream.
        request_id = ctx.meta.request_id
        openai_mode = bool(inputs.get("openai"))
        model_name = inputs.get("model", "qwen3_tool")
        params = SamplingParams(
            max_tokens=inputs.get("max_tokens", 256),
            # OpenAI semantics default temperature to 1.0; /v2 keeps 0.0.
            temperature=inputs.get("temperature", 1.0 if openai_mode else 0.0),
            top_p=inputs.get("top_p", 1.0),
            stop=inputs.get("stop"),
        )

        # Build the prompt: chat template when messages/tools are present.
        messages = inputs.get("messages")
        tools = inputs.get("tools")
        do_parse = bool(tools and self._tool_parser_name)
        if messages or do_parse:
            if self._tokenizer is None:
                # Load directly rather than reaching into engine internals —
                # V0's engine.engine.tokenizer is gone on the V1 AsyncLLM.
                self._tokenizer = AutoTokenizer.from_pretrained(self.config["model_path"])
        if messages:
            prompt = self._tokenizer.apply_chat_template(
                messages, tools=tools or None,
                add_generation_prompt=True, tokenize=False,
            )
        else:
            prompt = inputs.get("prompt", "")

        try:
            if inputs.get("stream", False):
                # Each vLLM yield carries the CUMULATIVE text; slice the delta.
                sent = 0
                prev_text = ""
                prev_token_ids: list[int] = []
                if openai_mode:
                    # OpenAI convention: the first chunk carries the role.
                    first = build_chat_chunk(
                        "", model=model_name, request_id=request_id)
                    first["choices"][0]["delta"] = {"role": "assistant"}
                    yield first
                # Incremental tool parsing for the OpenAI stream: the parser
                # holds per-stream state, so each request gets a fresh one.
                stream_parser = (
                    self._new_tool_parser() if (openai_mode and do_parse) else None
                )
                chat_req = self._chat_request(tools) if stream_parser is not None else None
                async for output in engine.generate(prompt, params, request_id=request_id):
                    text = output.outputs[0].text
                    delta, sent = text[sent:], len(text)
                    finish = output.outputs[0].finish_reason
                    if stream_parser is not None:
                        token_ids = list(output.outputs[0].token_ids)
                        dm = stream_parser.extract_tool_calls_streaming(
                            prev_text, text, delta,
                            prev_token_ids, token_ids,
                            token_ids[len(prev_token_ids):],
                            chat_req)
                        prev_text, prev_token_ids = text, token_ids
                        if dm is None and finish is None:
                            continue  # inside a tool call, no new fragments yet
                        chunk = build_chat_chunk(
                            "", model=model_name,
                            request_id=request_id, finish_reason=finish)
                        chunk["choices"][0]["delta"] = (
                            dm.model_dump(mode="json", exclude_none=True) if dm else {})
                        yield chunk
                        continue
                    # Tool calls parse from completed text only — the
                    # last frame's cumulative text IS the full output.
                    tool_calls = None
                    if finish is not None and do_parse:
                        tool_calls = self._parse_tool_calls(text, tools) or None
                    if openai_mode:
                        chunk = build_chat_chunk(
                            delta, model=model_name,
                            request_id=request_id, finish_reason=finish)
                        if tool_calls:
                            chunk["choices"][0]["delta"]["tool_calls"] = tool_calls
                    else:
                        chunk = {"token": delta}
                        if finish is not None:
                            chunk["finish_reason"] = finish
                            if tool_calls:
                                chunk["tool_calls"] = tool_calls
                    yield chunk
            else:
                # Non-streaming: the last cumulative output is the result.
                final = None
                async for output in engine.generate(prompt, params, request_id=request_id):
                    final = output
                text = final.outputs[0].text
                tool_calls = self._parse_tool_calls(text, tools) or None if do_parse else None
                if openai_mode:
                    response = build_chat_response(
                        text, model=model_name, request_id=request_id,
                        finish_reason=final.outputs[0].finish_reason or "stop")
                    response["usage"] = {
                        "prompt_tokens": len(final.prompt_token_ids),
                        "completion_tokens": len(final.outputs[0].token_ids),
                        "total_tokens": len(final.prompt_token_ids) + len(final.outputs[0].token_ids),
                    }
                    if tool_calls:
                        response["choices"][0]["message"]["tool_calls"] = tool_calls
                else:
                    response = {
                        "text": text,
                        "usage": {
                            "prompt_tokens": len(final.prompt_token_ids),
                            "completion_tokens": len(final.outputs[0].token_ids),
                        },
                    }
                    if tool_calls:
                        response["tool_calls"] = tool_calls
                yield response
        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnect: the framework cancels the consuming task.
            # Abort explicitly so generation stops at the next scheduler step
            # on any vLLM version (idempotent).
            abort = engine.abort(request_id)
            if asyncio.iscoroutine(abort):
                await abort
            raise

    def _new_tool_parser(self):
        """Fresh parser instance — streaming use requires one per request
        (it holds per-stream parse state); one-shot use may share."""
        if self._tool_parser_name != "hermes":
            raise ValueError(f"unsupported tool_parser: {self._tool_parser_name}")
        # vLLM 0.26 layout: vllm.tool_parsers (was entrypoints.openai.tool_parsers).
        from vllm.tool_parsers.hermes_tool_parser import Hermes2ProToolParser
        return Hermes2ProToolParser(self._tokenizer)

    def _chat_request(self, tools=None):
        """Request object the vLLM parsers require. Built per call with the
        real tools attached — regex-based parsers barely read it today, but
        future parser versions may validate call names against request.tools."""
        from vllm.entrypoints.openai.chat_completion.protocol import ChatCompletionRequest
        return ChatCompletionRequest(model="n/a", messages=[], tools=tools or None)

    def _parse_tool_calls(self, text, tools=None):
        """Parse tool calls from completed text via the configured vLLM parser."""
        info = self._new_tool_parser().extract_tool_calls(text, self._chat_request(tools))
        if not info.tools_called:
            return None
        return [tc.model_dump(mode="json") for tc in info.tool_calls]

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
