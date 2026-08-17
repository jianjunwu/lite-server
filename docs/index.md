# Documentation

lite-server is a high-performance model inference server — Rust core for I/O,
Python for inference. Start with the [README](../README.md) ([中文](../README_zh.md))
for the quick start; this index maps the full documentation set.

## Getting Started

| Doc | Covers |
|---|---|
| [README](../README.md) | Quick start, feature overview, installation, API endpoint map |
| [examples/](../examples/) | Runnable model repositories, one concept each |
| [Model Authoring](model-authoring.md) ([中文](zh/model-authoring.md)) | LitAPI interface, streaming, continuous batching, best practices |

## Using lite-server

| Doc | Covers |
|---|---|
| [Configuration](configuration.md) ([中文](zh/configuration.md)) | `server.yaml` / model config / orchestration, TLS, access control, CORS |
| [CLI Reference](cli.md) ([中文](zh/cli.md)) | All commands and flags |
| [Architecture](architecture.md) ([中文](zh/architecture.md)) | System design, request lifecycle, worker model |

## Protocols

| Doc | Covers |
|---|---|
| [Streaming](streaming.md) | Bidirectional streaming (WS `/stream`, h2 `/bidi`) and decoupled streaming (SSE `/decoupled`, WS `/decoupled-stream`) |
| [Protocol Compatibility](protocol.md) | Raw bytes / tensor requests, Triton Binary extension, openai-compact, known deviations from KServe V2 / Triton |
| [Modality Transport](modality-transport.md) ([中文](zh/modality-transport.md)) | Per-payload transport & compression: codec-layer audio, gzip request/response, gRPC |

## Deployment & Operations

| Doc | Covers |
|---|---|
| [Deployment](deployment.md) | Graceful shutdown, rolling updates, KEDA autoscaling |
| [Observability](observability.md) | Prometheus metrics reference, OpenTelemetry |
| [Migration](migration.md) ([中文](zh/migration.md)) | Breaking changes and upgrade paths |

## Evaluation

| Doc | Covers |
|---|---|
| [Benchmarks](benchmark.md) | Methodology and results |
| [Comparison](comparison.md) | lite-server vs other serving frameworks |
