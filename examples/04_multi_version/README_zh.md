# 04 多版本管理

演示多版本模型管理。同一模型的两个版本同时运行，可在运行时切换。

[English](README.md)

## 运行

```bash
cd examples/04_multi_version
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 推理使用当前活跃版本（默认为 v2，在 server.yaml 中指定）
curl -X POST http://localhost:8000/v2/models/multi_version/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
# => {"output": 20, "version": "v2"}  (v2: x * 2)

# 切换到 v1
curl -X POST http://localhost:8000/v2/models/multi_version/versions/v1/activate

# 现在推理使用 v1
curl -X POST http://localhost:8000/v2/models/multi_version/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
# => {"output": 11, "version": "v1"}  (v1: x + 1)

# 列出所有已加载的版本
curl http://localhost:8000/v2/models/multi_version/versions

# 指定版本的推理（不受活跃版本影响）
curl -X POST http://localhost:8000/v2/models/multi_version/versions/v2/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 10}'
```

## 学习要点

- 如何在同一个模型名下组织多个版本
- `server.yaml` 如何控制加载哪些版本
- 如何在运行时激活/停用版本
- 如何指定特定版本进行推理

## 目录结构

```
model_repo/
  multi_version/
    v1/
      model.py        # 版本 1: x + 1
      config.yaml
    v2/
      model.py        # 版本 2: x * 2
      config.yaml
server.yaml           # 加载所有版本，设置 v2 为默认版本
```

## 编排配置

```yaml
control_mode: explicit
load_models:
  - multi_version
models:
  - name: multi_version
    load_policy: all        # 加载所有版本
    default_version: v2     # v2 默认活跃
```

### 加载策略

| 策略 | 行为 |
|------|------|
| `explicit` | 仅加载 `versions_to_load` 中列出的版本 |
| `latest` | 仅加载最新版本 |
| `all` | 加载所有可用版本 |
