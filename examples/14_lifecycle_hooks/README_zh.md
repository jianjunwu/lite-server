# 14 生命周期钩子

演示 Worker 生命周期钩子：在 worker 生命周期的关键事件上触发 shell 命令和 HTTP 回调。

[English](README.md)

## 核心概念

**Worker 生命周期钩子**（`hooks:` 在 config.yaml 中）：`on_ready`、`on_error`、`on_exit` 事件的 shell 命令和 HTTP 回调。shell 命令模板中可使用 `$MODEL`、`$WORKER_ID`、`$DEVICE`、`$REASON`、`$EXIT_CODE` 等环境变量。

如需在 Python 中拦截推理请求管线（`Callback` 子类），见 [15_callbacks](../15_callbacks/)。

## 运行

```bash
cd examples/14_lifecycle_hooks
python -m lite_server serve --config server.yaml
```

启动服务器后，观察控制台输出，可以看到：

- `on_ready`：每个 worker 完成 `setup()` 后触发
- `on_exit`：worker 停止时触发
- `on_error`：worker 崩溃时触发

## 测试

```bash
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "hello", "count": 1}

# 多次请求 — 计数递增
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "world"}'
# => {"output": "world", "count": 2}
```

## 学习要点

- 如何配置 shell 命令钩子：`hooks.on_ready`、`hooks.on_error`、`hooks.on_exit`
- 如何配置 HTTP 回调钩子：`hooks.on_ready_http`、`hooks.on_error_http`
- 可用的模板变量：`$MODEL`、`$WORKER_ID`、`$DEVICE`、`$REASON`、`$EXIT_CODE`、`$EXIT_SIGNAL`
