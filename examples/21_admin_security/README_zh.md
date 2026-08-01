# 21. Admin API 与安全（P6 / P7）

**Admin gRPC 服务**（11 个 RPC：GetInfo、ListModels、ListVersions、ModelReady、ModelHealth、LoadModel、UnloadModel、ReloadModel、ActivateVersion、SetRouting、GetModelStats）运行在**独立绑定**上，两条 admin 通道都要求 **API key**，控制面变更会写**结构化审计日志**。

[English](README.md)

## 本示例演示

- `grpc.admin_bind: unix:./admin.sock`（P7-2）——`LiteAdmin` 服务不暴露在公网 gRPC 端口上，只监听自己的 Unix socket（默认仅属主 `0o600`）。
- `access_control.admin`（P7-1）——HTTP admin 路径（`/v2/models/.../activate`、`.../routing` 等）和 gRPC admin 服务都要 API key。未配置时 admin 默认**仅限回环**（fail-closed）；key 比较为恒定时间。
- **审计日志**（P6-2）——每次 admin 变更（load / unload / reload / activate / set_routing）都写一条结构化记录，含 `action / model / version / request_id / client_ip / principal`，落在 `lite_server::audit` tracing target（`logging.info_output`）。

## 目录结构

```
model_repo/
  admin_echo/1/    — echo 模型
server.yaml        — admin_bind UDS + access_control key + 审计日志文件
```

## 运行

```bash
lite-server serve --config server.yaml
```

## 验证

```bash
# 1. 不带 key 的 HTTP admin → 401：
curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST http://localhost:8000/v2/models/admin_echo/versions/v1/activate
# => 401

# 2. 带 key 的 HTTP admin → 成功：
curl -s -X POST http://localhost:8000/v2/models/admin_echo/versions/v1/activate \
  -H 'x-admin-key: secret-admin-key'
# => {"success": true, ...}

# 3. 走 UDS 的 gRPC admin，不带 key → Unauthenticated：
grpcurl -unix -plaintext -d '{}' \
  -import-path /path/to/lite_server/proto -proto liteserver.proto \
  /tmp/.../admin.sock liteserver.Admin/GetInfo
# => ERROR: Code=Unauthenticated

# 4. 走 UDS 的 gRPC admin，带 key → 成功（Python 客户端见 run_all.py 的
#    check_21；GetInfo 列出 loaded_models，ActivateVersion 做变更）。

# 5. 审计轨迹——变更后日志文件里有记录：
grep "admin control-plane mutation" audit.log
# => ... action=activate model=admin_echo version=Some("v1") ... client_ip=...
```

## 说明

- 推理通道不受影响：`:8000`/`:8001` 上的模型推理不需要 key。`access_control.inference` / `health` 可用来同样锁住这两类端点（见 docs/configuration.md）。
- `metrics_port` 监听有意不纳入访问控制——Prometheus 抓取用它。
- 密钥可以用 `value_env` / `value_file` 代替内联 `value`（启动时解析，缺失即 fail-fast）。
