# 24. Proxy & Browser Security (P-XFF / P-CORS)

Trusted-proxy **client-IP cleansing** and **CORS** — including the WebSocket
handshake Origin gate (browsers send no preflight for WS, so the check happens
at upgrade).

[中文版](README_zh.md)

## What this example shows

- `server.trusted_proxies` (P-XFF) — the set of fronting proxies whose
  `X-Forwarded-For` / `X-Real-IP` are honored. **Fail-safe default**: empty →
  the direct TCP peer is always used and proxy headers are IGNORED, so a
  client cannot forge a fake IP to bypass `key: ip` rate limiting.
  Here `127.0.0.1` is a trusted proxy, so forwarded IPs are honored:
  - no header → `127.0.0.1` (peer)
  - `X-Forwarded-For: 1.2.3.4` → `1.2.3.4`
  - `X-Forwarded-For: 1.2.3.4, 5.6.7.8` → `5.6.7.8` (right-most non-trusted hop)
- `policies.cors` (P-CORS) — exact-origin matching (no reflection of raw
  `Origin`, `null` rejected, `Vary: Origin` always). Unconfigured CORS →
  no CORS headers at all.
- **WS Origin gate** — with a CORS policy in place, a WebSocket upgrade whose
  `Origin` does not match `allow_origins` is rejected with **403**; a
  non-browser client without `Origin` passes.

## Layout

```
model_repo/
  proxy_echo/v1/
    model.py       — echoes the cleansed client_ip; stream_predict for WS
    config.yaml    — stream: true + per-model CORS policy
server.yaml        — trusted_proxies: ["127.0.0.1"]
```

## Running

```bash
lite-server serve --config server.yaml
```

## Verify

```bash
# 1. Client IP cleansing:
curl -s -X POST http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}'
# => {"output": {"echo": 1, "client_ip": "127.0.0.1"}}
curl -s -X POST http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Content-Type: application/json' -H 'X-Forwarded-For: 1.2.3.4' -d '{"input": 1}'
# => ... "client_ip": "1.2.3.4"        (peer is trusted → header honored)
curl -s -X POST http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Content-Type: application/json' -H 'X-Forwarded-For: 1.2.3.4, 5.6.7.8' -d '{"input": 1}'
# => ... "client_ip": "5.6.7.8"        (right-most non-trusted hop)

# 2. CORS preflight (matching origin):
curl -s -D - -o /dev/null -X OPTIONS http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Origin: https://app.example.com' \
  -H 'Access-Control-Request-Method: POST' \
  -H 'Access-Control-Request-Headers: content-type'
# => 204 + Access-Control-Allow-Origin: https://app.example.com
#       + Access-Control-Allow-Methods: GET, POST + Vary: Origin

# 3. CORS preflight (unconfigured origin → no CORS headers):
curl -s -D - -o /dev/null -X OPTIONS http://localhost:8000/v2/models/proxy_echo/infer \
  -H 'Origin: https://evil.example.com' -H 'Access-Control-Request-Method: POST'
# => no Access-Control-Allow-* headers at all

# 4. WebSocket Origin gate (see run_all.py check_24 for the raw-upgrade probe):
#    Origin: https://app.example.com  → 101 Switching Protocols
#    Origin: https://evil.example.com → 403 (no browser can hijack the WS)
```

## Notes

- Rate limiting with `key: ip` uses the cleansed IP, so the forged-IP bypass
  is closed exactly when `trusted_proxies` is configured correctly.
- The CORS policy can also be set globally under `server.cors`; a per-model
  policy overrides it.
