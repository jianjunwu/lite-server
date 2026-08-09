"""openai-compact chat model: /v1/chat/completions (unary + SSE).

The server thin-forwards /v1 (routing + demux only); the worker-side helpers
translate OpenAI requests into model semantics and build the OpenAI response
shapes — parse_chat_request / build_chat_response / build_chat_chunk.
"""

from lite_server import LitAPI
from lite_server.helpers.openai import (
    build_chat_chunk,
    build_chat_response,
    parse_chat_request,
)


class ChatAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return parse_chat_request(request)  # {model, messages, stream, ...}

    def predict(self, x, ctx):
        # stream: false -> unary JSON (chat.completion shape)
        prompt = x["messages"][-1]["content"]
        reply = f"chat echo: {prompt}"
        return build_chat_response(reply, model=x["model"],
                                   request_id=ctx.meta.request_id)

    async def stream_predict(self, x, ctx):
        # stream: true -> SSE (data: {json} per chunk + server appends data: [DONE])
        prompt = x["messages"][-1]["content"]
        for word in prompt.split():
            yield build_chat_chunk(word, model=x["model"])
        yield build_chat_chunk("", model=x["model"], finish_reason="stop")
