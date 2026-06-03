# 13 双向流式通信

演示双向流式通信（如语音识别 ASR、实时对话）。

[English](README.md)

## 核心概念

`bidi_stream()` 返回一个 `BidiStreamHandler`，包含三个钩子：
- `on_open(initial_data)` — 流打开时调用，返回初始响应
- `on_chunk(chunk)` — 每次收到客户端消息时调用，返回可选响应
- `on_close()` — 流关闭时调用，返回最终结果

## 运行

```bash
cd examples/13_bidi_streaming
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 通过 WebSocket 进行双向流通信
websocat ws://localhost:8000/v2/models/asr/stream

# 发送消息（在 WebSocket 中输入）：
> {"text": "hello"}
< {"partial": "hello", "is_final": false}
> {"text": "world"}
< {"partial": "hello world", "is_final": false}
> {"text": "test"}
< {"partial": "hello world test", "is_final": false}
# 关闭连接以触发 on_close()
< {"final": "hello world test", "is_final": true, "buffer": ["hello", "world", "test"]}
```

## 学习要点

- 如何实现 `BidiStreamHandler` 的 `on_open`、`on_chunk`、`on_close`
- 如何在 `bidi_stream()` 中返回 handler 实例
- 配置模式：在 config.yaml 中设置 `bidirectional: true` 和 `stream: true`
