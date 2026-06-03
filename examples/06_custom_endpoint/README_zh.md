# 06 自定义端点

演示如何向服务器添加自定义 HTTP 端点。端点定义在 `endpoints/` 目录中（或通过 `--endpoints-dir` 指定任何目录）。

[English](README.md)

## 运行

```bash
cd examples/06_custom_endpoint
lite-server serve --config server.yaml
# 或：python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 内置推理端点
curl -X POST http://localhost:8000/v2/models/echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}

# 自定义 status 端点
curl http://localhost:8000/status
# => {"server": "lite-server", "loaded_models_count": 1}
```

## 学习要点

- 如何创建与推理路由并行的自定义 HTTP 端点
- 端点文件如何从 `endpoints/` 目录自动发现
- 如何从自定义端点访问服务器的模型注册表

## 工作原理

将 Python 文件放入 `endpoints/` 目录，它们会递归自动发现：

```python
# endpoints/status.py

methods = ["GET"]  # 要注册的 HTTP 方法

def handler(request, server):
    """端点被访问时调用。"""
    # `server.registry` 提供对模型注册表的访问
    models = server.registry.list_loaded()
    return {"loaded": len(models)}
```

`methods` 列表定义要注册的 HTTP 方法。`handler` 函数接收请求和服务器上下文。

## 高级：装饰器风格路由

你也可以使用装饰器 API 获得更多控制：

```python
from lite_server import endpoint

@endpoint.get("/status")
def status(request, server):
    return {"loaded": len(server.registry.list_loaded())}
```

## CLI：指定自定义端点目录

```bash
lite-server serve --endpoints-dir ./my-endpoints --config server.yaml
```

优先级：`--endpoints-dir` > `server.yaml endpoints_dir` > `model_repository.path`
