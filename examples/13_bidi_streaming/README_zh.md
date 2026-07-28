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

双向流通过 **gRPC**（端口 8001）传输 —— `/stream` 的 WebSocket 路径仅支持单向流。自带的客户端会建立一个会话并触发全部三个钩子：

```bash
pip install grpcio          # 如未安装
python test_bidi.py
```

预期输出（每个钩子一帧 —— `on_open`、每次 `on_chunk`、`on_close`）：

```
open  : {"status": "ready", "sample_rate": 16000}
chunk : {"partial": "hello", "is_final": false}
chunk : {"partial": "hello world", "is_final": false}
chunk : {"partial": "hello world test", "is_final": false}
close : {"final": "hello world test", "is_final": true, "buffer": ["hello", "world", "test"]}
```

## 学习要点

- 如何实现 `BidiStreamHandler` 的 `on_open`、`on_chunk`、`on_close`
- 如何在 `bidi_stream()` 中返回 handler 实例
- bidi 会话由 `bidi_stream()` 方法自动检测，无需配置项
