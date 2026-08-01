# 24. 代理与浏览器安全（P-XFF / P-CORS）

受信代理的**客户端 IP 清洗**与 **CORS**——包括 WebSocket 握手 Origin 校验（浏览器对 WS 不发预检，所以检查发生在升级时）。

[English](README.md)

## 本示例演示

- `server.trusted_proxies`（P-XFF）——受信前置代理集合，其 `X-Forwarded-For` / `X-Real-IP` 才会被采纳。**fail-safe 默认**：空 → 一律使用直连 TCP peer、忽略代理头，客户端无法伪造 IP 绕过 `key: ip` 限流。这里把 `127.0.0.1` 设为受信代理，转发 IP 生效：
  - 无头 → `127.0.0.1`（peer）
  - `X-Forwarded-For: 1.2.3.4` → `1.2.3.4`
  - `X-Forwarded-For: 1.2.3.4, 5.6.7.8` → `5.6.7.8`（最右的非受信跳）
- `policies.cors`（P-CORS）——精确 Origin 匹配（不回显原始 `Origin`、拒绝 `null`、恒带 `Vary: Origin`）。未配置 CORS = 完全不附加 CORS 头。
- **WS Origin 校验**——配置了 CORS 策略后，WebSocket 升级请求的 `Origin` 不在 `allow_origins` 内会被 **403** 拒绝；不带 `Origin` 的非浏览器客户端放行。

## 目录结构

```
model_repo/
  proxy_echo/v1/
    model.py       — 回显清洗后的 client_ip；stream_predict 提供 WS 端点
    config.yaml    — stream: true + 按模型 CORS 策略
server.yaml        — trusted_proxies: ["127.0.0.1"]
```

## 运行

```bash
lite-server serve --config server.yaml
```

## 验证

```bash
# 1. 客户端 IP 清洗：
curl -s -X POST http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}'
# => {"output": {"echo": 1, "client_ip": "127.0.0.1"}}
curl -s -X POST http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Content-Type: application/json' -H 'X-Forwarded-For: 1.2.3.4' -d '{"input": 1}'
# => ... "client_ip": "1.2.3.4"        （peer 受信 → 头被采纳）
curl -s -X POST http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Content-Type: application/json' -H 'X-Forwarded-For: 1.2.3.4, 5.6.7.8' -d '{"input": 1}'
# => ... "client_ip": "5.6.7.8"        （最右的非受信跳）

# 2. CORS 预检（匹配的 Origin）：
curl -s -D - -o /dev/null -X OPTIONS http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Origin: https://app.example.com' \
  -H 'Access-Control-Request-Method: POST' \
  -H 'Access-Control-Request-Headers: content-type'
# => 204 + Access-Control-Allow-Origin: https://app.example.com
#       + Access-Control-Allow-Methods: GET, POST + Vary: Origin

# 3. CORS 预检（未配置的 Origin → 无任何 CORS 头）：
curl -s -D - -o /dev/null -X OPTIONS http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Origin: https://evil.example.com' -H 'Access-Control-Request-Method: POST'
# => 完全没有 Access-Control-Allow-* 头

# 4. WebSocket Origin 校验（原始升级探测见 run_all.py 的 check_24）：
#    Origin: https://app.example.com  → 101 Switching Protocols
#    Origin: https://evil.example.com → 403（浏览器无法劫持 WS）
```

## 说明

- `key: ip` 限流使用清洗后的 IP——`trusted_proxies` 配置正确时，伪造 IP 绕过即被关闭。
- CORS 策略也可以在 `server.cors` 全局配置；按模型策略覆盖全局。
