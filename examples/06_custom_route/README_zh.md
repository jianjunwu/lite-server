# 06 · 自定义路由（`@route`）

用 `@route` 装饰器为模型声明额外的 HTTP 端点。这些端点挂在
`/v2/models/<model>/<tail>` 下，通过和推理相同的 ZMQ 通道分发到模型
worker（不需要独立进程）。

## 功能

`model.py` 在 `PetsAPI` 上声明了三个自定义路由：

| 路由 | 方法 | 路径 | 用途 |
|------|------|------|------|
| `status` | GET | `/v2/models/pets/status` | 返回模型状态 |
| `get_pet` | GET | `/v2/models/pets/pets/{pet_id}` | 路径参数，缺失时 404 |
| `create_pet` | POST | `/v2/models/pets/pets` | JSON body，返回 201 |

handler 接收一个 `RequestContext`：

- `ctx.request` — 解析后的 JSON body（dict，缺失时为 `{}`）
- `ctx.meta.method` / `ctx.meta.query` / `ctx.meta.headers` — HTTP 元数据
- `ctx.state["path_params"]` — 从 `{name}` 段提取的路径参数
- 返回普通值（→ `200 application/json`）或 `Response`（自定义
  status / headers / media type）

## 运行

```bash
lite-server serve --config server.yaml
```

## 试一试

```bash
# 自定义路由
curl http://localhost:8000/v2/models/pets/status
# → {"model_loaded": true, "method": "GET"}

# 路径参数
curl http://localhost:8000/v2/models/pets/pets/1
# → {"id": 1, "name": "Fido"}

curl http://localhost:8000/v2/models/pets/pets/99
# → 404 {"error": "pet not found"}

# POST body
curl -X POST http://localhost:8000/v2/models/pets/pets \
  -H 'content-type: application/json' -d '{"name": "Buddy"}'
# → 201 {"id": 3, "name": "Buddy"}

# 同一个模型上推理仍然可用
curl -X POST http://localhost:8000/v2/models/pets/infer \
  -H 'content-type: application/json' -d '{"input": 5}'
# → {"output": 10}
```

## 说明

- 系统路由（`infer`、`events`、`stream`、`ready`、`health`、`reload`、
  `versions`、`compare`）为保留项：在这些路径上声明 `@route` 会在加载时
  被跳过并告警 —— 无法覆盖推理契约。
- 路由级 auth / 限流 / CORS 不在范围内（网关层职责）；自定义路由共享模型
  的全局 callback 链。
