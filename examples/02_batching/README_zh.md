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

## 学习要点

- `max_batch_size` 和 `batch_timeout` 如何启用自动批处理
- 批处理激活时 `predict()` 接收列表（默认路径）
- 如何重写 `batch()` 在预测前重塑输入
- 如何重写 `unbatch()` 将输出拆分回单个响应
- 自适应批处理如何根据队列压力调整超时

## 关键配置

```yaml
max_batch_size: 8      # 每批最大请求数
batch_timeout: 0.01    # 最多等待 10ms 来填满批次
```
