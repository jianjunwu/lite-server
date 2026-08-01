# 16 gRPC

演示 gRPC 推理端点。同一套模型代码同时支持 HTTP 和 gRPC。

[English](README.md)

## 核心概念

lite-server 从你的 LitAPI 模型自动生成 gRPC 端点。无需编写 proto 文件或服务定义 — 只需在 `server.yaml` 中启用 gRPC 并配置端口。`Infer`、`BatchInfer`、`StreamInfer`、`BidiStream` 等 RPC 直接映射到你的模型方法。

## 运行

```bash
cd examples/16_grpc
python -m lite_server serve --config server.yaml
```

## 测试

```bash
# HTTP 推理仍正常工作
curl -X POST http://localhost:8000/v2/models/grpc_echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "grpc_echo: hello"}

# gRPC 推理 — 使用 grpcurl 或 gRPC 客户端
grpcurl -plaintext \
  -d '{"model_name": "grpc_echo", "input": {"input": "hello"}}' \
  localhost:8001 \
  liteserver.LiteServer/Infer
# => {"output": "grpc_echo: hello"}

# 指定版本的 gRPC 推理
grpcurl -plaintext \
  -d '{"model_name": "grpc_echo", "model_version": "1", "input": {"input": "test"}}' \
  localhost:8001 \
  liteserver.LiteServer/Infer
# => {"output": "grpc_echo: test"}
```

## gRPC + Ensemble DAG（P-ENSEMBLE-GRPC）

对 **ensemble** 模型发起 gRPC `Infer` 调用时，整个 DAG 在服务器端执行——客户端只发一个请求、收最终输出，图的扇出/汇合都在服务器内部完成，与 HTTP 路径完全一致。

```bash
# 示例 05 的 pipeline 模型走 gRPC——一个请求，DAG 全执行：
grpcurl -plaintext \
  -d '{"model_name": "pipeline", "input": {"input": "hello"}}' \
  localhost:8001 \
  liteserver.LiteServer/Infer
# => {"output": "preprocessed(hello) -> done"}
```

说明：

- 用 `input` 作为请求体（对应模型的 `decode_request`），`model_name` 填 DAG 入口模型。`version` 字段保持可选。
- `deadline_unix_ns` 传播、金丝雀路由（`x-lite-version` metadata）与准入控制（`max_inflight`）对 gRPC ensemble 调用同样生效。
- Ensemble DAG 定义在入口模型的 `config.yaml` 中——见[示例 05](../05_ensemble/)。

## 学习要点

- 如何通过 `server.grpc_port` 和 `grpc.enabled` 启用 gRPC
- 同一 `model.py` 无需任何改动，即可同时支持 HTTP 和 gRPC
- gRPC 服务名：`liteserver.LiteServer`
- 可用 RPC：`Infer`、`BatchInfer`、`StreamInfer`、`BidiStream`
- 如何使用 `grpcurl` 测试 gRPC 端点
