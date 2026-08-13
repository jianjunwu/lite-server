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
  pipeline/1/
    config.yaml     # 集成 DAG 定义（不需要 model.py）
    dag.py          # 可选 E9-A Python 声明（仅声明，不执行）
  caption_stream/1/
    model.py        # 流式 caption（stream_predict 生成器）
    config.yaml
  stream_pipeline/1/
    config.yaml     # 流式集成 DAG（末步 stream: true）
  mmdemo/1/
    config.yaml     # MIMO 命名输入 + step.outputs + 多 sink 输出
```

## Python 声明（E9-A）

`pipeline` 的 DAG 同时以 Python 声明在 `model_repo/pipeline/1/dag.py`：

```python
from lite_server import EnsembleDAG, Step

dag = EnsembleDAG(
    steps=[
        Step(name="preprocess", model="step_a", version="1",
             inputs={"input": "$request.input"}),
        Step(name="postprocess", model="step_b", version="1",
             inputs={"input": "$preprocess.output"}),
    ],
)
```

声明**序列化为等价 config.yaml**；服务端执行的是 config.yaml——`dag.py`
只是编写面，绝不执行。漂移检查：

```bash
python -m lite_server analyze --model pipeline --model-repo ./model_repo
# 漂移 → 警告 LS112
```

## 流式集成（末步流式）

`stream_pipeline` 将 unary 的 `step_a` 预处理接上**流式末步**
（`caption_stream`，`stream: true`）。预层先完成，随后 DAG 返回末步模型的
chunk 流：

```bash
curl -N -X POST http://localhost:8000/v2/models/stream_pipeline/events \
  -H 'Content-Type: application/json' \
  -d '{"input": "a picture of a cat"}'
# => SSE 事件：{"token":"a","index":0} {"token":"picture","index":1} ...
```

要点：

- DAG **不做 chunk 内容转换**——chunk 格式由末步模型决定（OpenAI 风格
  chunk 需要末步模型本身产出 OpenAI 兼容 chunk）。
- 含流式 step 的 DAG 在 unary 端点返回 400；请用流式端点（`/events`、
  `/generate_stream`、`/v1`、gRPC server-streaming、WS/h2/gRPC bidi）。
- **unary 步骤走队列**（受 `max_queue_size` 限制）；**流式步骤绕过队列**
  （直连流式路径）。并发流式 DAG 数由全局旋钮
  `server.max_concurrent_streaming_dags` 限制（默认 128，耗尽立即 429）。

## MIMO：命名多输入/多输出（KServe 信封）

`mmdemo` 声明两个命名输入、一个 `step.outputs` JSON 投影与多 sink 输出：

```bash
curl -X POST http://localhost:8000/v2/models/mmdemo/infer \
  -H 'Content-Type: application/json' \
  -d '{
    "inputs": [
      {"name": "text", "data": {"input": "hello"}}
      // system_prompt 省略——有 default 的可选输入
    ]
  }'
# => {"model_name":"mmdemo","outputs":[
#      {"name":"answer","data":"preprocessed(hello) -> done"},
#      {"name":"echo_text","data":{"input":"hello"}}]}
```

命名输入使用 **KServe V2 信封 wire**：每个元素按 `name` 匹配 `inputs`
声明（`type: json` 的值放 `data`；`type: binary` 的元素带
`parameters.binary_data_size`，原始字节走二进制尾——HTTP 用
`inference-header-content-length` 标头，gRPC/WS/h2 用帧内 LSBE-1 容器）。
无 `default` 的可选输入缺席时，引用它的 step 成为条件 step（跳过，其
sink 为 `null`）。声明 inputs 的模型**只接受**信封形态（否则 400）；未声明
`inputs` 的旧 config 保持历史 JSON body，字节级不变。

更多：[protocol.md](../../docs/protocol.md)（KServe V2）、
[model-authoring.md](../../docs/model-authoring.md) 与
[configuration.md](../../docs/configuration.md)。

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
