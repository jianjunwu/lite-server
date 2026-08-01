# 19. 金丝雀路由（P5-2）

按**权重**在两个模型版本间分流流量，并用 `x-lite-version` 请求头把单个请求钉到指定版本。

[English](README.md)

## 本示例演示

- `orchestration.models[].weights` —— 版本 `v1` 承接 20% 请求，`v2` 承接 80%。未列出的版本权重为 0。
- `features.canary_override: true` —— 尊重 **`x-lite-version`** 请求头，客户端可以把请求钉到指定版本（用于 A/B 测试或灰度验证）。
- **默认是 `canary_override: false`** —— 该头被忽略，客户端不能自行钉到金丝雀版本。仅在灰度/调试环境开启（配置变更见 [migration M3](../docs/migration.md)）。

## 目录结构

```
model_repo/
  canary_echo/v1/   — 旧行为（输入 + 1），权重 20
  canary_echo/v2/   — 新行为（输入 * 2），权重 80
server.yaml         — 权重 + 开启 canary_override
```

## 运行

```bash
lite-server serve --config server.yaml
```

## 验证

```bash
# 1. 流量按权重分流（多跑几次——约 20% 命中 v1）：
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/v2/models/canary_echo/infer \
       -H 'Content-Type: application/json' -d '{"input": 5}'
  echo
done
# => {"output": 6,  "version": "v1"}  （10 次中约 2 次）
# => {"output": 10, "version": "v2"}  （10 次中约 8 次）

# 2. 显式钉到 v1：
curl -s -X POST http://localhost:8000/v2/models/canary_echo/infer \
     -H 'Content-Type: application/json' -H 'x-lite-version: v1' \
     -d '{"input": 5}'
# => {"output": 6, "version": "v1"}

# 3. 钉到 v2：
curl -s -X POST http://localhost:8000/v2/models/canary_echo/infer \
     -H 'Content-Type: application/json' -H 'x-lite-version: v2' \
     -d '{"input": 5}'
# => {"output": 10, "version": "v2"}
```

## 说明

- 权重在 `server.yaml` 中设置、启动时读取一次。运行时改分流用 admin 的 `SetRouting` RPC（见示例 21）。
- 同样的金丝雀机制覆盖 gRPC（`Infer` 携带 `x-lite-version` metadata key）。
