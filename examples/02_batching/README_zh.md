# 02 请求批处理

演示请求批处理 — 包括默认行为和自定义 batch/unbatch 函数。

[English](README.md)

## 运行

```bash
cd examples/02_batching
python -m lite_server serve --config server.yaml
```

## 模型

### `batched` — 默认批处理

使用框架默认的 `batch()` / `unbatch()`（透传）。`predict()` 直接接收一个解码后的输入列表。

```python
def predict(self, inputs):
    if isinstance(inputs, list):
        return [{"output": x, "batch_size": len(inputs)} for x in inputs]
    return {"output": inputs, "batch_size": 1}
```

测试：

```bash
# 并发发送 8 个请求 — 合并为一次批处理调用
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8000/v2/models/batched/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}" &
done
wait

# 每个响应：{"output": <N>, "batch_size": 8}
```

### `custom_batch` — 自定义 batch / unbatch

重写 `batch()` 和 `unbatch()` 以在单个请求和批处理 `predict()` 之间重塑数据。

**流程：** `decode_request -> batch -> predict -> unbatch -> encode_response`

测试：

```bash
# 发送带不同权重的请求
for i in $(seq 1 4); do
  curl -s -X POST http://localhost:8000/v2/models/custom_batch/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i, \"weight\": 0.5}" &
done
wait

# 每个响应：{"output": <N * 0.5>, "batch_size": 4}
```

### `ctx_batch` — 批处理阶段访问每请求上下文

`batch`、`unbatch`、`predict` 可以声明 `ctx` 来接收与批内各项**按位置对齐**的 `list[RequestContext]` —— 用于日志、追踪或分组，无需把数据塞进解码后的输入里搬运。

```python
def batch(self, inputs, ctx):
    for c in ctx:                                    # ctx[i] <-> inputs[i]
        self.logger.info("batching request_id=%s", c.meta.request_id)
    return inputs

def predict(self, batched, ctx):
    return [{"output": v * 2, "request_id": c.meta.request_id}
            for v, c in zip(batched, ctx)]           # predict 阶段也能拿到逐项 ctx

def unbatch(self, output, ctx):
    return list(output)
```

`ctx[i]` 始终与 `inputs[i]` 对齐 —— 不要重排输入，否则结果会写回错误的请求。

测试：

```bash
for i in $(seq 1 4); do
  curl -s -X POST http://localhost:8000/v2/models/ctx_batch/infer \
    -H 'Content-Type: application/json' \
    -d "{\"input\": $i}" &
done
wait

# 每个响应：{"output": <N * 2>, "request_id": "<服务端分配的 id>"}
```

## 学习要点

- `max_batch_size` 和 `batch_timeout` 如何启用自动批处理
- 批处理激活时 `predict()` 接收列表（默认路径）
- 如何重写 `batch()` 在预测前重塑输入
- 如何重写 `unbatch()` 将输出拆分回单个响应
- `batch` / `unbatch` / `predict` 如何声明 `ctx` 访问每请求上下文
- 自适应批处理如何根据队列压力调整超时

## 关键配置

```yaml
max_batch_size: 8      # 每批最大请求数
batch_timeout: 0.01    # 最多等待 10ms 来填满批次
```
