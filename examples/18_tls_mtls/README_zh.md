# 18. TLS / mTLS（P5-1）

通过 **HTTPS + 强制客户端证书（双向 TLS）** 提供服务，并演示**证书热轮换**——无需重启服务器。

[English](README.md)

## 本示例演示

- `server.tls_cert_path` / `tls_key_path` —— HTTP 监听启用 TLS。
- `server.mtls_ca_path` —— 一旦设置，客户端**必须**出示由该 CA 签发的证书（mTLS）。没有客户端证书的握手会被 TLS 层直接拒绝。
- `tls_min_version: "1.3"` —— 仅接受 TLS 1.3。
- **热轮换** —— 服务器监听 PEM 文件变化（10 秒内容轮询，Unix 下 SIGHUP 立即重载）。替换文件后新连接立即使用新证书；轮换失败（文件损坏、密钥不匹配）时继续使用旧证书。

## 目录结构

```
certs/          — 由 setup.sh 生成（CA、服务器、客户端密钥对）
model_repo/
  tls_echo/1/   — echo 模型（输入 × 2）
server.yaml     — 已启用 TLS + mTLS
```

## 运行

```bash
# 生成证书（幂等）：
bash setup.sh

# 启动服务器：
lite-server serve --config server.yaml
```

## 验证

```bash
# 1. mTLS 请求——必须携带客户端证书：
curl --cacert certs/ca.crt --cert certs/client.crt --key certs/client.key \
     -s -X POST https://localhost:8000/v2/models/tls_echo/infer \
     -H 'Content-Type: application/json' -d '{"input": 21}'
# => {"output": 42}

# 2. 不带客户端证书，TLS 握手直接失败：
curl --cacert certs/ca.crt -s -X POST https://localhost:8000/v2/models/tls_echo/infer \
     -H 'Content-Type: application/json' -d '{"input": 21}'
# => curl: (56) ... (no client cert presented)

# 3. 不重启服务器轮换服务器证书：
openssl req -newkey rsa:2048 -keyout certs/server.key -out /tmp/rot.csr \
    -nodes -subj "/CN=localhost-rotated"
openssl x509 -req -in /tmp/rot.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -out certs/server.crt -days 3650 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
kill -HUP <server-pid>   # 可选：立即重载（否则 ≤10 秒轮询）
rm /tmp/rot.csr
# 新连接将出示轮换后的证书（可用 openssl s_client 检查对端 CN）
```

## 说明

- 客户端身份（证书 subject）会写入请求上下文，用于访问日志/审计。mTLS 之上叠加 API-Key 认证请用 `policies.auth`——见示例 17。
- gRPC 监听在 `grpc.*` 下有独立的 TLS 配置。
- TLS 与 UDS（`unix:` host）互斥。
