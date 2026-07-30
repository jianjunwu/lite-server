# 08 · 错误处理与鲁棒性

演示框架的异常→HTTP 映射、请求超时和 `on_error` 回调——出错了会怎样。

[English](README.md)

## 核心概念

- **框架异常**（`BadRequestError`、`NotFoundError` 等）映射到 HTTP 状态码并生成机器可读的错误响应体。
- **未处理异常**（如 `RuntimeError`）变成 500 Internal Server Error——worker 会捕获它们，单个错误请求不会导致进程崩溃。
- **`request_timeout`**——每个请求的硬性超时（秒）。0 = 禁用。超时的请求被终止并返回超时错误。
- **`on_error` 回调**——任何钩子或阶段抛出异常时执行。它是*异常隔离*的：失败的 `on_error` 只记日志，绝不掩盖原始错误。用于收集错误遥测。
- **Worker 驱逐**——连续 `ejection_error_threshold` 次错误后 worker 被驱逐 `ejection_timeout` 秒，然后自动恢复，防止中毒 worker 持续消耗 CPU。

## 模型：`ErrorDemoAPI`

接受 `{"input": "...", "mode": "<mode>"}`。模式：

| 模式 | 行为 | HTTP 状态 |
|------|------|-----------|
| `normal` | 正常返回 | 200 |
| `bad_request` | `raise BadRequestError(...)` | 400 |
| `not_found` | `raise NotFoundError(...)` | 404 |
| `server_error` | `raise RuntimeError(...)` | 500 |
| `slow` | `await asyncio.sleep(5)` — 超过 2s 的 `request_timeout` | 超时错误 |

无效模式也会通过 `BadRequestError` 返回 400。

## 运行

```bash
cd examples/08_error_handling
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 正常 — 200
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello", "mode": "normal"}' | python -m json.tool

# 错误请求 — 400
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "", "mode": "bad_request"}' | python -m json.tool

# 未找到 — 404
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "missing-key", "mode": "not_found"}' | python -m json.tool

# 服务器错误 — 500
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "boom", "mode": "server_error"}' | python -m json.tool

# 慢请求 — 2s 后超时
curl -s -w "\nHTTP %{http_code}\n" -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "", "mode": "slow"}'

# 无效模式 — 也是 400
curl -s -X POST http://localhost:8000/v2/models/error_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "", "mode": "unknown"}' | python -m json.tool
```

查看服务器控制台中的 `[ErrorMetrics]` 日志行，统计每类错误的计数。连续两次 `server_error` 后 worker 被驱逐，30s 后自动恢复。

## 学到了什么

- 哪种异常类映射到哪个 HTTP 状态码
- `request_timeout` 如何保护服务不被慢请求拖垮
- `on_error` 如何在不掩盖原始错误的情况下提供错误遥测
- Worker 驱逐 → 自动恢复的生命周期
- 未处理异常不会导致 worker 进程崩溃
