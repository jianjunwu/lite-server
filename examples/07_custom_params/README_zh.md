# 07 自定义参数

演示如何通过 `self.config` 将 `config.yaml` 中的自定义参数传递到模型代码。

[English](README.md)

## 核心概念

`config.yaml` 中的所有字段都可以通过 `self.config.get(key, default)` 在 `model.py` 中访问。这使你无需修改代码即可调整模型行为。

## 运行

```bash
# 从项目根目录
python -m lite_server serve --model-repo examples/07_custom_params/model_repo
```

## 测试

```bash
# 分数高于阈值 (0.5) -> "positive"
curl -X POST http://localhost:8000/v2/models/threshold/infer \
  -H 'Content-Type: application/json' \
  -d '{"score": 0.8}'
# => {"label": "positive", "score": 0.8, "threshold": 0.5}

# 分数低于阈值 -> "negative"
curl -X POST http://localhost:8000/v2/models/threshold/infer \
  -H 'Content-Type: application/json' \
  -d '{"score": 0.3}'
# => {"label": "negative", "score": 0.3, "threshold": 0.5}
```

## 学习要点

- 如何通过 `self.config.get(key, default)` 访问自定义 `config.yaml` 字段
- 如何使模型行为由配置驱动，无需修改代码
- 模式：在 YAML 中定义参数，在 `setup()` 中读取
