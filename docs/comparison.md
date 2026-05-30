# Framework Comparison: lite-server vs Other Inference Servers

[中文版](comparison_zh.md)

## TL;DR

- **lite-server** — High-performance, lightweight, Rust+Python hybrid. Best for teams wanting production-grade features without infrastructure overhead.
- **Triton** — NVIDIA's enterprise solution. Best for large GPU clusters with dedicated MLOps teams.
- **TorchServe** — PyTorch's official serving framework. Best when deeply invested in the PyTorch ecosystem.
- **BentoML** — General-purpose model serving. Best for packaging and deploying models across platforms.
- **Ray Serve** — Distributed inference framework. Best for complex multi-model pipelines at scale.

## Detailed Comparison

### Architecture

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| HTTP core | Rust (axum/tokio) | C++ (custom) | Java (Netty) | Python (FastAPI) | Python (uvicorn) |
| Inference layer | Python subprocess | C++ plugins | Python subprocess | Python | Python Actor |
| IPC mechanism | ZMQ/UDS | Shared memory | TorchServe protocol | HTTP | Ray object store |
| Process model | Per-worker subprocess | Single process, backends | Java + Python workers | Per-deployment | Distributed actors |

### Installation & Dependencies

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| Install command | `pip install` | Docker only | Java + pip | `pip install` | `pip install` |
| Min. dependencies | Python 3.10+ | CUDA, Docker | Java 11+, Python | Python 3.8+ | Python 3.8+ |
| Container required | No | Yes (strongly recommended) | No | No | No |
| Binary size | ~15MB | ~1GB | ~500MB | ~50MB | ~200MB |

### Model Management

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| Model format | Python class (LitAPI) | Framework-specific | MAR archive | Python class | Python class |
| Version control | Multi-version with activate/deactivate | Multi-version | Multi-version | Manual | Manual |
| Hot reload | File watcher with debounce | No (requires restart) | Limited (model control API) | No | No |
| Load policies | explicit, latest, all | explicit, polling | explicit | N/A | N/A |
| Ensemble/DAG | DAG with parallel layers | Yes (model ensemble) | No | Pipeline | Deployment graph |
| Model packing | .lma with SHA256+HMAC | No | MAR format | Bento | No |

### Performance & Scalability

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| Batching | Adaptive + static | Dynamic batching | Dynamic batching | Manual | Manual |
| Continuous batching | Yes (LLM hooks) | Yes | No | No | No |
| Worker scheduling | Least-loaded + outlier-aware | Round-robin | Round-robin | Configurable | Actor-based |
| Zero-copy data path | Bytes + Arc | Shared memory | No | No | No |
| Streaming | SSE + WebSocket + gRPC | gRPC streaming | No | SSE | Streaming |

### Resilience

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| Outlier detection | Envoy-style auto-eject | No | No | No | No |
| Request retry | Up to 3 retries on different workers | No | No | No | Configurable |
| Worker recycling | max_requests auto-restart | No | No | No | No |
| Per-request timeout | Yes (configurable) | Yes | Yes | Yes | Yes |
| Health checks | Deep (worker + model status) | Basic | Basic | Basic | Basic |

### Observability

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| Metrics | Prometheus (13+ metrics) | Prometheus | Prometheus | Prometheus | Prometheus |
| Timeline | Built-in historical sampling | No | No | No | No |
| Alerts | Built-in alert engine | No | No | No | No |
| Logging | tracing-based structured logs | Custom | log4j | Python logging | Python logging |

### Platform Support

| Aspect | lite-server | Triton | TorchServe | BentoML | Ray Serve |
|--------|------------|--------|------------|---------|-----------|
| Linux x86_64 | Yes | Yes | Yes | Yes | Yes |
| Linux aarch64 | Yes | Yes | Yes | Yes | Yes |
| macOS | Yes | No | Yes | Yes | Yes |
| Windows | Yes | No | No | Yes | No |
| Python versions | 3.10 - 3.14 | 3.8 - 3.10 | 3.8 - 3.11 | 3.8+ | 3.8+ |

## When to Choose lite-server

**Good fit:**
- You want high performance without Docker/Java/C++ infrastructure
- Your team writes Python but wants Rust-level HTTP throughput
- You need hot reload for rapid iteration during development
- You want built-in outlier detection and self-healing workers
- You're deploying multiple model versions with A/B testing
- You need ensemble pipelines (preprocessing -> model -> postprocessing)
- You're on macOS or Windows for development

**Consider alternatives:**
- **Triton** — If you have a large NVIDIA GPU cluster and need TensorRT/ONNX optimization at the kernel level
- **TorchServe** — If your entire stack is PyTorch and you need tight integration with TorchServe's model archiving
- **Ray Serve** — If you need complex multi-node distributed inference across a Ray cluster

## Performance Notes

lite-server's performance advantage comes from:

1. **Rust HTTP layer** — axum/tokio handles I/O without Python's GIL bottleneck
2. **Zero-copy data path** — `Bytes` shared buffers and `Arc<RequestMeta>` avoid data copying in the hot path
3. **DashMap lock-free concurrency** — model registry and pending response map use concurrent hash maps instead of mutexes
4. **Adaptive batching** — dynamically adjusts batch timeout based on queue pressure, dispatching immediately under high load
5. **ZMQ/UDS IPC** — Unix domain sockets avoid TCP overhead for local worker communication

See [benchmark.md](benchmark.md) for measured performance data.
