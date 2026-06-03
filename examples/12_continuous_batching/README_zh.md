# 12 连续批处理

演示 LLM 场景下的连续批处理（continuous batching）：`prefill()`、`step()`、`has_finished()` 三个钩子。

[English](README.md)

## 核心概念

连续批处理同时处理多个序列，每次调用 `step()` 为每个活跃序列生成一个 token。新请求通过 `prefill()` 中途加入，完成的序列通过 `has_finished()` 移除。

## 运行

```bash
cd examples/12_continuous_batching
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# 单请求 — 逐步生成 token
curl -X POST http://localhost:8000/v2/models/cb_llm/infer \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "hello world this is a test"}'
# => {"tokens": ["hello","world","this","is","a"], "text": "hello world this is a"}

# 多个并发请求 — 共享同一个生成循环
for i in 1 2 3; do
  curl -s -X POST http://localhost:8000/v2/models/cb_llm/infer \
    -H 'Content-Type: application/json' \
    -d "{\"prompt\": \"request $i goes here\"}" &
done
wait
```

## 学习要点

- 如何实现 `prefill()` 为批处理队列添加新序列
- 如何实现 `step()` 每次迭代为所有活跃序列生成一个 token
- 如何实现 `has_finished()` 判断序列是否完成
- 配置模式：在 config.yaml 中设置 `continuous_batching: true`
