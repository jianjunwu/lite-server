"""OpenAI 兼容 worker 翻译层 helper 测试(阶段 6,批次 5,J6/C19)。

parse_chat_request 请求解析 + build_chat_response/chunk/completions/
embeddings 构造——纯函数,worker 侧翻译层(与 kserve.py 同族)。
"""

import numpy as np
import pytest

from lite_server.exceptions import HTTPException
from lite_server.helpers.openai import (
    build_chat_chunk,
    build_chat_response,
    build_completions_response,
    build_embeddings_response,
    parse_chat_request,
)


class TestParseChatRequest:
    def test_parses_messages_and_model(self):
        req = {
            "model": "my-chat",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": False,
            "temperature": 0.7,
        }
        out = parse_chat_request(req)
        assert out["model"] == "my-chat"
        assert out["messages"] == [{"role": "user", "content": "hi"}]
        assert out["stream"] is False
        assert out["temperature"] == 0.7

    def test_stream_defaults_false(self):
        out = parse_chat_request({"model": "m", "messages": []})
        assert out["stream"] is False

    def test_missing_messages_400(self):
        with pytest.raises(HTTPException) as ei:
            parse_chat_request({"model": "m"})
        assert ei.value.status_code == 400

    def test_messages_not_a_list_400(self):
        with pytest.raises(HTTPException) as ei:
            parse_chat_request({"model": "m", "messages": "not-a-list"})
        assert ei.value.status_code == 400


class TestBuildChatResponse:
    def test_unary_chat_shape(self):
        resp = build_chat_response("hello", model="m1", request_id="r1")
        assert resp["id"] == "r1"
        assert resp["object"] == "chat.completion"
        assert resp["model"] == "m1"
        assert isinstance(resp["created"], int)
        assert resp["choices"][0]["message"] == {"role": "assistant", "content": "hello"}
        assert resp["choices"][0]["finish_reason"] == "stop"

    def test_no_request_id_generates_one(self):
        resp = build_chat_response("hi", model="m1")
        assert isinstance(resp["id"], str) and len(resp["id"]) > 0


class TestBuildChatChunk:
    def test_sse_chunk_shape(self):
        chunk = build_chat_chunk("hel", model="m1", request_id="r1")
        assert chunk["object"] == "chat.completion.chunk"
        assert chunk["model"] == "m1"
        assert chunk["choices"][0]["delta"] == {"content": "hel"}
        assert chunk["choices"][0]["finish_reason"] is None

    def test_finish_reason(self):
        chunk = build_chat_chunk("", model="m1", finish_reason="stop")
        assert chunk["choices"][0]["finish_reason"] == "stop"


class TestBuildCompletions:
    def test_text_completion_shape(self):
        resp = build_completions_response("hello", model="m1", request_id="r1")
        assert resp["object"] == "text_completion"
        assert resp["choices"][0]["text"] == "hello"
        assert resp["choices"][0]["finish_reason"] == "stop"


class TestBuildEmbeddings:
    def test_embeddings_shape(self):
        resp = build_embeddings_response([0.1, 0.2], model="m1", request_id="r1")
        assert resp["object"] == "list"
        assert resp["data"][0]["object"] == "embedding"
        assert resp["data"][0]["embedding"] == [0.1, 0.2]
        assert resp["model"] == "m1"

    def test_embeddings_from_ndarray(self):
        resp = build_embeddings_response(np.array([0.5, 0.25]), model="m1")
        assert resp["data"][0]["embedding"] == [0.5, 0.25]
