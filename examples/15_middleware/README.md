# 15 Middleware

Demonstrates custom endpoint middleware: authentication, rate limiting, CORS, and request logging.

[中文](README_zh.md)

## Key Concept

Custom endpoints support middleware chains. Stack multiple middleware decorators on a route to compose auth, rate limiting, CORS, and logging. Middleware is configured per-route via the `middleware` parameter.

Available middleware:
- `require_api_key` — validates `X-API-Key` header
- `rate_limit` — token bucket rate limiter (configurable requests/min)
- `cors` — attaches CORS headers
- `log_requests` — logs request/response timing

## Run

```bash
cd examples/15_middleware
python -m lite_server serve --config server.yaml
```

## Test

```bash
# Standard inference still works (no middleware)
curl -X POST http://localhost:8000/v2/models/protected/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "protected: hello"}

# Public endpoint — only CORS, works without auth
curl http://localhost:8000/public
# => {"message": "this endpoint is public"}

# Protected status endpoint — requires X-API-Key
curl http://localhost:8000/status
# => {"error": "unauthorized"}  (401)

curl -H "X-API-Key: secret-api-key-123" http://localhost:8000/status
# => {"server": "lite-server", "loaded_models": ["protected"], ...}

# Rate limit test — send requests rapidly, 11th request blocked
for i in $(seq 1 11); do
  curl -s -H "X-API-Key: secret-api-key-123" http://localhost:8000/status
  echo
done
# => {"error": "rate limit exceeded"}  (429, after 10 requests)
```

## What You Learn

- How to stack middleware on custom endpoints via `middleware` parameter
- How `require_api_key` adds authentication to routes
- How `rate_limit` protects endpoints from abuse
- How `cors` handles cross-origin requests
- How `log_requests` adds request/response logging
- Middleware is per-route — different endpoints can have different policies
