# 01 基础模型

lite-server 最简示例。模型返回 `input * 2`。

[English](README.md)

## 运行

```bash
cd examples/01_basic
python -m lite_server serve --config server.yaml
```

## 测试

```bash
curl -X POST http://localhost:8000/v2/models/echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": 21}'
# => {"output": 42}
```

## 学习要点

- 如何编写 `model.py`（继承 `LitAPI`）
- 如何定义 `setup`、`decode_request`、`predict`、`encode_response`
- `config.yaml` 如何配置模型
- `model_name/version/` 的目录结构
