# 15 回调（认证 / 限流 / CORS / 日志）

演示自定义端点上的统一**回调(callback)** API：认证、限流、CORS、请求日志,通过 `callbacks=[...]` 逐路由组合。

[English](README.md)

> 0.7.0 把"中间件(middleware)"层改名为**回调**,每个端点 handler 改为接收单一 `RequestContext`(`ctx`)参数。完整对照见 `docs/migration-0.7.md`。

## 核心概念

自定义端点为路由挂一串回调。每个回调在该路由的每次请求上按注册顺序执行:可短路请求(认证拒绝)、修改上下文、或追加响应头。

四个内置回调:

| 回调 | 作用 |
|---|---|
| `RequireApiKey(header=..., keys=[...])` | 缺少/错误 API key 时拒绝(401) |
| `RateLimit(requests_per_minute=N, key="route"\|"ip", burst=...)` | 超限时拒绝(429),带 `Retry-After` |
| `Cors(allow_origins=[...])` | 给每个响应(含错误响应)附加 CORS 头,并应答 OPTIONS 预检 |
| `LogRequests()` | 记录每次请求/响应及错误 |

handler 接收单一 `ctx`(`RequestContext`):读 `ctx.request`(body)、`ctx.meta`(`headers`/`query`/`method`/`route`/`request_id`)、`ctx.server`(server proxy)、`ctx.state`(逐请求 dict);返回 dict(序列化为 JSON body),或调 `ctx.respond(...)` / 返回 `Response` 获得完全控制。

## 运行

```bash
cd examples/15_middleware
python -m lite_server serve --config server.yaml
```

若改过 Rust 侧,先重建扩展:`maturin develop`。

## 试一下

```bash
# 标准推理 —— 已加载的 `protected` 模型(无回调)
curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' -d '{"input": "hello"}'
# => {"output": "protected: hello"}

# 公开端点 —— 仅 CORS,无认证
curl http://localhost:8000/public
# => {"message": "this endpoint is public", "request_id": "..."}

# 受保护 /status —— 需要 X-API-Key
curl http://localhost:8000/status
# => HTTP 401
#    {"error":{"type":"authentication_error","message":"missing API key","code":null,"param":"X-API-Key"}}

curl -H 'X-API-Key: secret-api-key-123' http://localhost:8000/status
# => HTTP 200
#    {"server":"lite-server","loaded_models":[{"name":"protected","version":"1"}],
#     "request_id":"...","endpoint":"status (callback-protected)"}

# CORS 预检 —— HTTP 层应答(204 + 头)
curl -i -X OPTIONS -H 'Origin: http://app.example' -H 'Access-Control-Request-Method: GET' \
  http://localhost:8000/status
# => HTTP 204   access-control-allow-origin: *

# 限流 —— 该端点 10 req/min(burst 15);快速连发约 15 次后 429
for i in $(seq 1 20); do
  curl -s -o /dev/null -w '%{http_code} ' -H 'X-API-Key: secret-api-key-123' http://localhost:8000/status
done
echo
# => 200 200 200 200 200 200 200 200 200 200 200 200 200 200 429 429 429 429 429 429
```

## 学习要点

- 通过 `callbacks=[...]` 逐路由组合回调
- 单 `ctx` 参数的 handler 合同(`ctx.request`、`ctx.meta`、`ctx.server`)
- 四个内置回调各自保证的行为
- 策略在 HTTP 层执行:CORS 覆盖每个响应(含 401/429 错误响应)、OPTIONS 预检、限流均在到达 handler 之前完成
