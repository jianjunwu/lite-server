# 05 集成流水线

演示 DAG 集成推理流水线。多个模型串联，独立步骤并行执行。

[English](README.md)

## 运行

```bash
cd examples/05_ensemble
python -m lite_server serve --config server.yaml
```

## 测试

```bash
curl -X POST http://localhost:8000/v2/models/pipeline/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "preprocessed(hello) -> done"}
```

流水线：`request` -> `step_a`（预处理） -> `step_b`（后处理） -> response。

## 学习要点

- 如何在 `config.yaml` 中定义集成 DAG
- `$request` 和 `$step_name` 引用如何连接各步骤
- 独立步骤如何并行执行（拓扑执行）
- 集成模型不需要 `model.py` — 只需 `config.yaml`

## DAG 配置

```yaml
ensemble:
  steps:
    - name: preprocess
      model: step_a          # 引用 step_a 模型
      version: "1"
      inputs:
        input: "$request.input"  # 从 HTTP 请求中获取输入

    - name: postprocess
      model: step_b          # 引用 step_b 模型
      version: "1"
      inputs:
        input: "$preprocess.output"  # 从 preprocess 步骤获取输入
```

## 目录结构

```
model_repo/
  step_a/1/
    model.py        # 预处理模型
    config.yaml
  step_b/1/
    model.py        # 后处理模型
    config.yaml
  pipeline/
    config.yaml     # 集成 DAG 定义（不需要 model.py）
```

## 并行执行

互不依赖的步骤会并行执行：

```yaml
ensemble:
  steps:
    - name: step_a
      model: model_a
      version: "1"
      inputs:
        input: "$request.input"
    - name: step_b
      model: model_b
      version: "1"
      inputs:
        input: "$request.input"  # 都从 request 获取 — 无依赖

    - name: step_c
      model: model_c
      version: "1"
      inputs:
        a: "$step_a.output"      # 依赖 step_a
        b: "$step_b.output"      # 依赖 step_b
```

此处 `step_a` 和 `step_b` 并行执行，`step_c` 等待两者完成。

## 多参数并行流水线

真实场景示例：**多路召回** — 4 个独立召回模型并行执行，然后合并和排序。

### DAG

```
$request.query    --> [bm25_recall] --+
$request.user_id  --> [cf_recall]    --+
$request.image    --> [visual_recall] --+--> [merge] --> [rank] --> response
$request.history  --> [seq_recall]    --+
```

第 0 层：`bm25_recall`、`cf_recall`、`visual_recall`、`seq_recall` 都只依赖 `$request` — 并行执行。

第 1 层：`merge` 等待 4 个召回全部完成。

第 2 层：`rank` 等待 merge，输出最终 top-k 结果。

### 测试

```bash
curl -X POST http://localhost:8000/v2/models/multi_pipeline/infer \
  -H 'Content-Type: application/json' \
  -d '{"query": "running shoes", "user_id": "u123", "image": "shoe.jpg", "history": ["sneakers", "nike"], "top_k": 5}'
```

预期输出：

```json
{"results": ["bm25_item_0", "bm25_item_1", "bm25_item_2", "cf_item_0", "cf_item_1"]}
```
