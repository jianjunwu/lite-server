# 14 生命周期钩子

演示 Worker 生命周期钩子和新的 Callback 推理请求拦截系统。

[English](README.md)

## 核心概念

**Worker 生命周期钩子**（`hooks:` 在 config.yaml 中）：`on_ready`、`on_error`、`on_exit` 事件的 shell 命令和 HTTP 回调。shell 命令模板中可使用 `$MODEL`、`$WORKER_ID`、`$DEVICE`、`$REASON`、`$EXIT_CODE` 等环境变量。

**推理 Callback**（`callbacks:` 在 config.yaml 中）：Python `Callback` 子类，拦截推理请求管线。Callback 可组合、可跨模型复用，并具有自动异常隔离。

模型目录中的 `callbacks.py` 提供了两个示例 callback：
- `AuditLogger`：记录请求时序和每次推理调用的日志
- `ResponseEnricher`：为每个响应添加请求元数据（`_meta` 字段）

注意：Callback 中的异常会因异常隔离机制被有意吞掉。Callback 应用于数据转换或产生副作用 — 使用 `LitAPI.on_request()` 来拒绝请求。

## 运行

```bash
cd examples/14_lifecycle_hooks
python -m lite_server serve --config server.yaml
```

启动服务器后，观察控制台输出，可以看到：

- `on_ready`：每个 worker 完成 `setup()` 后触发
- `[AuditLogger]`：记录每个请求的元数据和推理延迟
- `on_exit`：worker 停止时触发
- `on_error`：worker 崩溃时触发

## 测试

```bash
# 每个响应包含 ResponseEnricher 添加的 _meta 字段
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "hello", "count": 1, "_meta": {"request_id": "...", ...}}

# 多次请求 — 计数递增
curl -X POST http://localhost:8000/v2/models/hooked_model/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "world"}'
# => {"output": "world", "count": 2, "_meta": {"request_id": "...", ...}}
```

## 学习要点

- 如何配置 shell 命令钩子：`hooks.on_ready`、`hooks.on_error`、`hooks.on_exit`
- 如何配置 HTTP 回调钩子：`hooks.on_ready_http`、`hooks.on_error_http`
- 可用的模板变量：`$MODEL`、`$WORKER_ID`、`$DEVICE`、`$REASON`、`$EXIT_CODE`、`$EXIT_SIGNAL`
- 如何编写和注册 `Callback` 子类来拦截推理管线
- 如何通过 config.yaml 中的 `callbacks:` 实现 callback 链式调用与异常隔离
- 9 个可用的 callback 钩子：`on_before_setup`、`on_after_setup`、`on_teardown`、`on_before_decode`、`on_after_decode`、`on_before_predict`、`on_after_predict`、`on_before_encode`、`on_after_encode`
