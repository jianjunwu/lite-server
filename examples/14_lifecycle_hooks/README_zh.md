# 14 生命周期钩子

演示 Worker 生命周期钩子：`on_ready`、`on_error`、`on_exit` 事件的 shell 命令和 HTTP 回调。

[English](README.md)

## 核心概念

lite-server 可在 worker 状态变化时执行 shell 命令或 HTTP 请求，用于告警、日志和外部监控集成。shell 命令模板中可使用 `$MODEL`、`$WORKER_ID`、`$DEVICE`、`$REASON`、`$EXIT_CODE` 等环境变量。

## 运行

```bash
cd examples/14_lifecycle_hooks
python -m lite_server serve --config server.yaml
```

启动服务器后，观察控制台输出，可以看到钩子命令的执行：
- `on_ready`：每个 worker 完成 `setup()` 后触发
- `on_exit`：worker 停止时触发
- `on_error`：worker 崩溃时触发

## 测试

```bash
# 模型与钩子并行正常工作
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
- 钩子是 fire-and-forget 模式，非阻塞
