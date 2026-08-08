"""OpenAI 兼容 worker 翻译层(阶段 6,批次 5,J6/C19)。

翻译层在 **worker 侧**(chat→tensor 是模型语义,server 无法通用翻译):
server 薄透传(body 最小解析仅 model/stream 用于路由与分流 + SSE 帧编码),
worker 作者在本 helper 上解析 OpenAI 请求并构造 OpenAI 响应(unary +
SSE chunk + embeddings)。纯函数,与 [`kserve.py`](../kserve.py) 同族。

用法范式(worker 侧):

    from lite_server.helpers.openai import (
        parse_chat_request, build_chat_response, build_chat_chunk)

    class ChatModel(LitAPI):
        def decode_request(self, request):
            return parse_chat_request(request)      # {messages, model, stream, ...}

        def predict(self, x):                        # stream: false
            reply = generate(x["messages"])
            return build_chat_response(reply, model=x["model"],
                                       request_id=ctx.meta.request_id)

        async def stream_predict(self, x, ctx):      # stream: true
            for token in generate_stream(x["messages"]):
                yield build_chat_chunk(token, model=x["model"])
"""

from __future__ import annotations

import time
import uuid

from lite_server.exceptions import HTTPException


def _request_id(request_id: str | None) -> str:
    return request_id or f"chatcmpl-{uuid.uuid4().hex[:24]}"


def parse_chat_request(request: dict) -> dict:
    """解析 `/v1/chat/completions` 请求体 → 规范化 dict。

    返回 `{model, messages, stream, temperature?, max_tokens?, ...}`(原样
    透传其他字段,模型语义归模型)。缺 `messages` 或非列表 → 400。
    """
    if not isinstance(request, dict):
        raise HTTPException(
            400, "request body must be a JSON object",
            error_type="invalid_request_error", code="invalid_request_error",
        )
    messages = request.get("messages")
    if not isinstance(messages, list):
        raise HTTPException(
            400, "messages must be a list of chat messages",
            error_type="invalid_request_error", code="invalid_request_error",
        )
    out = dict(request)
    out["messages"] = messages
    out.setdefault("stream", False)
    return out


def build_chat_response(
    content: str,
    *,
    model: str,
    request_id: str | None = None,
    finish_reason: str = "stop",
) -> dict:
    """unary chat 响应:`chat.completion` 形状。"""
    return {
        "id": _request_id(request_id),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": finish_reason,
        }],
    }


def build_chat_chunk(
    delta: str,
    *,
    model: str,
    request_id: str | None = None,
    finish_reason: str | None = None,
) -> dict:
    """SSE chunk:`chat.completion.chunk` 形状(`data: {json}` 逐 chunk,
    server 追加 `data: [DONE]`)。"""
    return {
        "id": _request_id(request_id),
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "delta": {"content": delta},
            "finish_reason": finish_reason,
        }],
    }


def build_completions_response(
    text: str,
    *,
    model: str,
    request_id: str | None = None,
    finish_reason: str = "stop",
) -> dict:
    """`/v1/completions` unary 响应:`text_completion` 形状。"""
    return {
        "id": _request_id(request_id),
        "object": "text_completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "text": text,
            "finish_reason": finish_reason,
        }],
    }


def build_embeddings_response(
    embedding,
    *,
    model: str,
    request_id: str | None = None,
) -> dict:
    """`/v1/embeddings` 响应:`{object: list, data: [{object: embedding, ...}],
    model}` 形状。`embedding` 接受 list 或 numpy 数组。"""
    if hasattr(embedding, "tolist"):
        embedding = embedding.tolist()
    return {
        "object": "list",
        "data": [{
            "object": "embedding",
            "index": 0,
            "embedding": embedding,
        }],
        "model": model,
        "usage": {
            "prompt_tokens": 0,
            "total_tokens": 0,
        },
        **({"id": request_id} if request_id else {}),
    }
