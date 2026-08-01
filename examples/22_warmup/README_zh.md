# 22. 模型预热（P-WARM）

服务前先跑**假推理**：版本在预热完成前保持 `WarmingUp` 状态（`/ready` = false），完成后翻转为 `Ready`。

[English](README.md)

## 本示例演示

- `policies.warmup.enabled: true` —— 版本走 `WarmingUp` → `Ready` 两段状态，而不是直接 `Ready`。
- `iterations: 2` —— 执行两次假推理（本模型每次 sleep 0.5 秒，因此就绪被推迟约 1 秒）。
- `dummy_input_ref: warmup/input.json` —— 假请求体（相对模型目录）；这里是 `{"input": 42}`。
- `timeout_secs` —— 单次预热的预算；预热失败会把版本标记为 `Failed`（带 `last_failure`），而不是冷启动服务。

## 目录结构

```
model_repo/
  warmup_echo/v1/
    model.py          — 统计假推理调用（input 42），暴露 /stats 路由
    warmup/input.json — 假请求体
server.yaml           — 加载模型（预热策略在 config.yaml 里）
```

## 运行

```bash
lite-server serve --config server.yaml
```

## 验证

```bash
# 1. 观察就绪状态翻转 false → true（约 1 秒预热）：
for i in $(seq 1 20); do
  curl -s http://localhost:8000/v2/models/warmup_echo/ready; echo
  sleep 0.2
done
# => {"ready": false, ...} × 约 5 次   （WarmingUp——尚未就绪）
# => {"ready": true,  ...}             （预热完成）

# 2. 模型看到恰好配置次数的假推理：
curl -s http://localhost:8000/v2/models/warmup_echo/stats
# => {"warmup_count": 2}

# 3. 正常推理不受影响：
curl -s -X POST http://localhost:8000/v2/models/warmup_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 21}'
# => {"output": {"warmup_count": 2}}
```

## 说明

- `enabled: false`（默认）时版本直接 `Ready`——行为不变、无预热开销。
- 预热失败（异常/超时）会把版本标记为 `Failed`；registry 记录 `last_failure`，重载成功前服务不可用。
