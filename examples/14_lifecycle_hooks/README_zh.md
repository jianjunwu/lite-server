# 14 生命周期钩子

演示 Worker 生命周期钩子和 Callback 推理请求拦截系统。

[English](README.md)

## 核心概念

**Worker 生命周期钩子**（`hooks:` 在 config.yaml 中）：`on_ready`、`on_error`、`on_exit` 事件的 shell 命令和 HTTP 回调。shell 命令模板中可使用 `$MODEL`、`$WORKER_ID`、`$DEVICE`、`$REASON`、`$EXIT_CODE` 等环境变量。

**推理 Callback**（`callbacks:` 在 config.yaml 中）：Python `Callback` 子类，拦截推理请求管线。Callback 可组合、可跨模型复用。

模型目录中的 `callbacks.py` 提供了两个示例 callback：
- `AuditLogger`：记录请求时序和每次推理调用的日志
- `ResponseEnricher`：为每个响应添加请求元数据（`_meta` 字段）

0.7.0 起，callback 的数据钩子接收单个 `ctx`（RequestContext）参数。每个请求的临时数据放在 `ctx.state`（不要放在 `self` 属性上——它在并发请求间共享）。钩子可以抛出 `HTTPException` 做参数校验并拒绝请求，也可以用 `ctx.respond(...)` 短路返回——数据钩子的异常**不会**被吞掉。生命周期钩子（`on_before_setup` / `on_after_setup` / `on_teardown`）仍保持异常隔离（失败只记日志，不传播）。

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
- callback 钩子点：`on_request` → decode → `on_input` → predict → `on_output` → encode → `on_response`，以及生命周期钩子 `on_before_setup`、`on_after_setup`、`on_teardown`
