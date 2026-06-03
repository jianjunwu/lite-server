# 16 gRPC

Demonstrates gRPC inference endpoints. The same model code serves both HTTP and gRPC.

[中文](README_zh.md)

## Key Concept

lite-server auto-generates gRPC endpoints from your LitAPI model. No proto file or service definition needed — just enable gRPC in `server.yaml` and configure a port. The `infer`, `batch_infer`, `stream_infer`, and `bidi_stream` RPCs map directly to your model's methods.

## Run

```bash
cd examples/16_grpc
python -m lite_server serve --config server.yaml
```

## Test

```bash
# HTTP inference still works
curl -X POST http://localhost:8000/v2/models/grpc_echo/infer \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
# => {"output": "grpc_echo: hello"}

# gRPC inference — use grpcurl or a gRPC client
grpcurl -plaintext \
  -d '{"model_name": "grpc_echo", "input": {"input": "hello"}}' \
  localhost:8001 \
  liteserver.LiteServer/Infer
# => {"output": "grpc_echo: hello"}

# gRPC with specific version
grpcurl -plaintext \
  -d '{"model_name": "grpc_echo", "model_version": "1", "input": {"input": "test"}}' \
  localhost:8001 \
  liteserver.LiteServer/Infer
# => {"output": "grpc_echo: test"}
```

## What You Learn

- How to enable gRPC via `server.grpc_port` and `grpc.enabled`
- The same `model.py` works for both HTTP and gRPC — no code changes needed
- gRPC service name: `liteserver.LiteServer`
- Available RPCs: `Infer`, `BatchInfer`, `StreamInfer`, `BidiStream`
- How to use `grpcurl` for testing gRPC endpoints
