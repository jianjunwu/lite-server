# 15 中间件

演示自定义端点中间件：认证、限流、CORS 和请求日志。

[English](README.md)

## 核心概念

自定义端点支持中间件链。通过 `middleware` 参数在路由上堆叠多个中间件装饰器，组合认证、限流、CORS 和日志功能。

可用中间件：
- `require_api_key` — 验证 `X-API-Key` 请求头
- `rate_limit` — 令牌桶限流器（可配置每分钟请求数）
- `cors` — 添加 CORS 响应头
- `log_requests` — 记录请求/响应耗时

## 运行

```bash
cd examples/15_middleware
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 标准推理仍正常工作（无中间件）
curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "protected: hello"}

# 公开端点 — 仅 CORS，无需认证
curl http://localhost:8000/public
# => {"message": "this endpoint is public"}

# 受保护的 status 端点 — 需要 X-API-Key
curl http://localhost:8000/status
# => {"error": "unauthorized"}  (401)

curl -H "X-API-Key: secret-api-key-123" http://localhost:8000/status
# => {"server": "lite-server", "loaded_models": ["protected"], ...}

# 限流测试 — 快速发送请求，第 11 次被拒绝
for i in $(seq 1 11); do
  curl -s -H "X-API-Key: secret-api-key-123" http://localhost:8000/status
  echo
done
# => {"error": "rate limit exceeded"}  (429，超过 10 次后)
```

## 学习要点

- 如何通过 `middleware` 参数在自定义端点上堆叠中间件
- 如何用 `require_api_key` 为路由添加认证
- 如何用 `rate_limit` 保护端点免遭滥用
- 如何用 `cors` 处理跨域请求
- 如何用 `log_requests` 添加请求/响应日志
- 中间件是逐路由的 — 不同端点可设置不同策略
