# 15 回调（认证 / 限流 / CORS / 日志）

演示统一**回调(callback)** API：在 `LitAPI` 类上声明一次模型级回调链——认证、限流、CORS、请求日志——作用于该模型的所有路由。

[English](README.md)

> 0.7.0 把"中间件(middleware)"层改名为**回调**,每个 handler 改为接收单一 `RequestContext`(`ctx`)参数。完整对照见 `docs/migration-0.7.md`。

## 核心概念

`callbacks` 类属性把一串回调挂到整个模型上。该链对**标准推理**(`/v2/models/protected/infer`)和**自定义 `@route` 路由**(`/v2/models/protected/status`)同样生效——没有按路由单独挂链的用法。回调可短路请求(认证拒绝)、修改上下文、或追加响应头。

```python
class ProtectedAPI(LitAPI):
    callbacks = (
        RequireApiKey(header="X-API-Key", keys=VALID_KEYS),
        RateLimit(requests_per_minute=10),
        Cors(allow_origins=["*"]),
        LogRequests(),
    )
```

四个内置回调:

| 回调 | 作用 |
|---|---|
| `RequireApiKey(header=..., keys=[...])` | 缺少/错误 API key 时拒绝(401) |
| `RateLimit(requests_per_minute=N, key="route"\|"ip", burst=...)` | 超限时拒绝(429),带 `Retry-After` |
| `Cors(allow_origins=[...])` | 给每个响应(含错误响应)附加 CORS 头,并应答 OPTIONS 预检 |
| `LogRequests()` | 记录每次请求/响应及错误 |

handler 接收单一 `ctx`(`RequestContext`):读 `ctx.request`(body)、`ctx.meta`(`headers`/`query`/`method`/`route`/`request_id`)、`ctx.server`(server proxy)、`ctx.state`(逐请求 dict);返回 dict(序列化为 JSON body),或调 `ctx.respond(...)` / 返回 `Response` 获得完全控制。

> **适用范围说明:** `RateLimit` 和 `Cors` 由 Rust HTTP 层在推理路由上执行;在自定义 `@route` 路由上,Python 侧钩子(认证、日志)会运行,但 Rust 托管的策略目前不生效。

## 运行

```bash
cd examples/15_middleware
python -m lite_server serve --config server.yaml
```

若改过 Rust 侧,先重建扩展:`maturin develop`。

## 试一下

```bash
# 推理 —— 需要 X-API-Key
curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -d '{"input": "hello"}'
# => HTTP 401
#    {"error":{"type":"authentication_error","message":"missing API key","code":null,"param":"X-API-Key"}}

curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: secret-api-key-123' \
  -d '{"input": "hello"}'
# => {"output": "protected: hello"}

# 自定义路由 /status —— 同一条回调链守护
curl http://localhost:8000/v2/models/protected/status
# => HTTP 401

curl -H 'X-API-Key: secret-api-key-123' http://localhost:8000/v2/models/protected/status
# => HTTP 200
#    {"server":"lite-server","loaded_models":[{"name":"protected","version":"1",...}],
#     "request_id":"..."}

# 推理路由的 CORS 预检 —— HTTP 层应答(204 + 头)
curl -i -X OPTIONS -H 'Origin: http://app.example' -H 'Access-Control-Request-Method: POST' \
  http://localhost:8000/v2/models/protected/infer
# => HTTP 204   access-control-allow-origin: *

# 推理路由限流 —— 10 req/min(burst 15);快速连发约 15 次后 429
for i in $(seq 1 20); do
  curl -s -o /dev/null -w '%{http_code} ' -H 'X-API-Key: secret-api-key-123' \
    -H 'Content-Type: application/json' -X POST -d '{"input":"x"}' \
    http://localhost:8000/v2/models/protected/infer
done
echo
# => 200 200 200 200 200 200 200 200 200 200 200 200 200 200 200 429 429 429 429 429
```

## 学习要点

- 通过 `callbacks` 类属性声明模型级回调链
- 一次声明同时覆盖推理和自定义 `@route` 路由
- 单 `ctx` 参数的 handler 合同(`ctx.request`、`ctx.meta`、`ctx.server`)
- 四个内置回调各自保证的行为
- 策略在 HTTP 层执行:CORS 覆盖每个推理响应(含 401/429 错误响应)、OPTIONS 预检、限流均在到达模型代码之前完成
