# 15 Callbacks

详细演示 Python `Callback` 系统：全部钩子点、两种注册方式、请求拒绝、短路早返和每请求状态。

[English](README.md)

## 核心概念

Callback 在模型三个阶段的上下游提供四个数据钩子点，可观察并改写推理管线：

```
on_request → decode_request → on_input → predict
→ on_output → encode_response → on_response
```

此外还有 `on_error`（请求失败时）和三个生命周期钩子（`on_before_setup` / `on_after_setup` / `on_teardown`）。

本示例注册了五个 callback 加一个内置类 —— 见 `model_repo/callbacks_demo/1/callbacks.py` 和 `config.yaml`：

| Callback | 钩子 | 演示点 |
|----------|------|--------|
| `ApiKeyAuth` | `on_request` | 拒绝请求：抛 `UnauthorizedError` → 401 |
| `RequestTimer` | `on_request`、`on_response` | 用 `ctx.state` 存每请求状态（并发安全） |
| `SimpleCache` | `on_request`、`on_output` | `ctx.respond(...)` 短路早返、自定义响应头 |
| `JsonSchemaValidator`（内置） | `on_input` | config.yaml 声明式 schema 校验，零 Python 代码（需 `pip install lite-server[validation]`） |
| `ErrorMetrics` | `on_error` | 异常隔离的错误钩子 |
| `LifecycleTracer` | setup/teardown | `on_before_setup` / `on_after_setup` / `on_teardown` |

### 两种注册方式

- **`LitAPI.callbacks` 类属性**（在 `model.py` 中）：优先级更高，支持构造参数 —— callback 需要配置时用它。先于 config.yaml 中的 callback 执行。
- **config.yaml 的 `callbacks:`**：每条目是全限定类路径（无参）或**单键 map** `{path: kwargs}`（传构造参数）——内置的 `JsonSchemaValidator` 就在这里用 `input_schema` 声明式配置。追加在类属性之后。

本例中 `ApiKeyAuth` 通过类属性注册（接受合法 key 列表作为构造参数，保证鉴权先于缓存执行），其余通过 config.yaml 注册。

### 需要了解的语义

- 数据钩子接收单个 `ctx`（`RequestContext`）参数，同步异步均可。可原地修改 `ctx.request` / `ctx.input` / `ctx.output` / `ctx.response`，或返回替换值。
- 每请求的临时数据放在 `ctx.state` —— 不要放在 `self` 属性上（在并发请求间共享）。
- 数据钩子的异常**不会**被吞掉：抛 `HTTPException`（或 `BadRequestError` / `UnauthorizedError` 等子类）即以对应状态码拒绝请求，返回机器可读的错误体。
- `ctx.respond(body, status_code=..., headers=...)` 短路管线 —— 后续阶段和钩子全部跳过。
- `on_error` 和生命周期钩子是异常隔离的：失败只记日志，不传播。
- 流式模式下，`on_output` / `on_response` 对每个 chunk 各调一次，`on_error` 对每个失败的 chunk 调一次。
- 日志用 `logging` 模块，不要 `print()` —— stdout 承载 worker 启动握手协议，往里写（比如在 `on_before_setup` 中）会导致 worker 启动失败。
- 生产环境的鉴权/限流/CORS 应使用 config.yaml 中声明式的 `policies:` 配置 —— 这里的 `ApiKeyAuth` 仅用于教学演示钩子机制。

## 运行

```bash
cd examples/15_callbacks
pip install lite-server[validation]   # schema 校验 extra
python -m lite_server serve --config server.yaml
```

启动时可以看到 worker 加载模型打印 `[LifecycleTracer] before setup` / `setup done`。

## 测试

`config.yaml` 中的 schema（`input_schema`）校验整个请求体：`text`（必填、非空字符串）、`note`（必填、但允许 `null`）、`max_tokens` / `temperature`（可选、有边界）、`messages`（可选的对象列表 `{role, content}`），并拒绝未知字段。400 错误里的 `param` 是指向出错位置的 JSON Pointer。

```bash
# 无 API key -> ApiKeyAuth.on_request 返回 401
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"text": "hello", "note": null}'
# => HTTP 401 {"error": {"type": "authentication_error", "message": "missing or invalid X-API-Key header"}}

# 缺必填字段（text）-> 内置 JsonSchemaValidator 返回 400；
# predict() 不会执行
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"note": null}'
# => HTTP 400 ... "'text' is a required property", "param": "body"
# （缺字段错误的 JSON Pointer 为空，param 指向根）

# 空字符串（text 有 minLength: 1）
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "", "note": null}'
# => HTTP 400 ... "'' should be non-empty", "param": "body/text"

# 数值越界（temperature 有 maximum: 2.0）
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "temperature": 3.0}'
# => HTTP 400 ... "3.0 is greater than the maximum of 2.0", "param": "body/temperature"

# 列表元素里的 enum 非法 -> 指针一路指进数组
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "messages": [{"role": "admin", "content": "hi"}]}'
# => HTTP 400 ... "'admin' is not one of ['user', 'assistant', 'system']",
#    "param": "body/messages/0/role"

# 未知字段（additionalProperties: false）
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "extra": 1}'
# => HTTP 400 ... "Additional properties are not allowed ('extra' was unexpected)",
#    "param": "body"

# 正常请求 -> 200；note 必填但允许为 null。[RequestTimer] 每请求打印耗时
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "max_tokens": 128, "temperature": 0.7,
       "messages": [{"role": "user", "content": "hi"}]}'
# => HTTP 200 {"output": {"text": "hello", "note": null, "max_tokens": 128, ...}}

# 相同请求再来一次 -> 缓存命中：X-Cache 响应头 + "cached": true，
# predict() 不会执行
curl -i -X POST http://localhost:8000/v2/models/callbacks_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: demo-key' \
  -d '{"text": "hello", "note": null, "max_tokens": 128, "temperature": 0.7,
       "messages": [{"role": "user", "content": "hi"}]}'
# => HTTP 200, X-Cache: hit, {"output": {...}, "cached": true}
```

> 注意：上面几个失败的 curl 要慢慢发——服务端对连续被拒请求会驱逐 worker（30s 退避），期间 infer 返回临时的 `503 model_not_ready`，会自动恢复。

合法 key 默认为 `demo-key`，可用 `DEMO_API_KEYS=key1,key2` 覆盖。

关闭服务（Ctrl+C）时可以看到 `[LifecycleTracer] model unloading, teardown`。

## 学习要点

- 全部 callback 钩子点及顺序：`on_request` → decode → `on_input` → predict → `on_output` → encode → `on_response`，外加 `on_error` 和生命周期钩子
- 两种注册方式及适用场景（带构造参数的 `LitAPI.callbacks` vs config.yaml 的 `callbacks:`，字符串与单键 map 条目）
- 在数据钩子中抛 `HTTPException` 子类拒绝请求（401/400）
- 用内置 `JsonSchemaValidator` 声明式校验请求输入
- 用 `ctx.respond(...)` 短路管线并附加自定义响应头
- 用 `ctx.state` 携带每请求数据
