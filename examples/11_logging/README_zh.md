# 11 结构化日志

演示推理声明周期各阶段的结构化日志。

[English](README.md)

## 运行

```bash
cd examples/11_logging
# 默认日志级别为 "warn"；使用 "info" 或 "debug" 查看更多
python -m lite_server serve --config server.yaml --log-level info
```

## 测试

```bash
curl -X POST http://localhost:8000/v2/models/logged_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42, "call_count": 1}
```

## 日志级别

| 级别 | 你会看到什么 |
|------|-------------|
| `warn`（默认） | 仅警告和错误 |
| `info` | + setup 消息、每个请求的摘要 |
| `debug` | + 每个阶段的详细输入/输出 |

## 学习要点

- `self.logger` 在每个 `LitAPI` 方法中都可用
- 使用 `.debug()` / `.info()` / `.warning()` / `.error()` 按需记录
- 通过 `--log-level`（或 `log_level` 配置字段）控制详细程度
- 在 `before_decode_request` / `after_encode_response` 中记录请求元数据（客户端 IP、请求 ID、路由）
