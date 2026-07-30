# 17 · 配置模板与环境变量

将配置从代码中抽离的模式——环境变量、自定义 YAML 字段和多环境 server 配置。

[English](README.md)

## 核心概念

三种互补的配置外化模式：

| 模式 | 机制 | 适用场景 |
|------|------|----------|
| `os.environ` 在 `setup()` 中 | 标准 Python | 密钥、后端 URL、功能开关——永远不放 YAML |
| `self.config` 自定义 YAML | `config.yaml` 任意键 | 不想动代码就能调整的可调参数 |
| `${VAR}` 在 auth keys 中 | 框架原生展开 | API 密钥——fail-closed：未设置变量=加载失败 |

再加上多环境 `server.yaml` 覆写（dev vs staging vs prod）。

## 模型：`EnvDemoAPI`

在 `setup()` 中读取三种配置源：

- `DEMO_BACKEND` 环境变量（默认 `"cpu"`）——使用哪个后端
- `DEMO_LOG_VERBOSE` 环境变量（默认 `"0"`）——是否记录每次预测
- `self.config["greeting"]` / `self.config["version_label"]`——自定义 YAML 字段

每次响应都回显当前配置，方便查看实际值。

## 运行

```bash
cd examples/17_config_templates

# Dev — 本地接口, 端口 8000, debug 日志
DEMO_API_KEY=dev-secret python -m lite_server serve --config server.yaml

# Prod — 端口 8080 + gRPC 9001, warn 日志, 绑定所有接口
DEMO_API_KEY=prod-secret DEMO_BACKEND=gpu \
  python -m lite_server serve --config server.prod.yaml
```

> **${DEMO_API_KEY} 是必填的**——config 在 `policies.auth.keys` 中引用了 `${DEMO_API_KEY}`。如果变量未设置，模型**加载失败**（fail-closed）。没有默认值或回退。

## 测试

```bash
# Dev (端口 8000) — 仅本地，用 X-API-Key 鉴权
curl -s -X POST http://127.0.0.1:8000/v2/models/env_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: dev-secret' \
  -d '{"input": "world"}'

# 无鉴权头 → 401
curl -s -X POST http://127.0.0.1:8000/v2/models/env_demo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "world"}'

# Prod (端口 8080) — 绑定所有接口
curl -s -X POST http://localhost:8080/v2/models/env_demo/infer \
  -H 'Content-Type: application/json' -H 'X-API-Key: prod-secret' \
  -d '{"input": "production"}'

# Prod 中使用 gRPC (端口 9001)
grpcurl -plaintext -H 'x-api-key: prod-secret' \
  -d '{"model_name": "env_demo", "input": {"input": "grpc"}}' \
  localhost:9001 liteserver.LiteServer/Infer
```

## 学到了什么

- 三种配置层：环境变量（密钥）、YAML（可调参数）、CLI --config（环境切换）
- `policies.auth.keys` 中的 `${VAR}` 展开——fail-closed 设计
- 如何组织多环境 `server.yaml` 覆写
- 如何通过模型输出来验证当前配置
