# OpenTelemetry 可观测性（P-TRACE）

lite-server Rust 核心的全量 OpenTelemetry trace + metrics SDK，经 **OTLP/gRPC** 导出。这实现了蓝图支柱 C（"全链路可观测"）——W3C traceparent 跨 gateway→server→worker 边界传播、tracing spans 桥接到 OTel、以及 exemplar-ready 的 metrics 覆盖层。

## Rust-only 边界（D8）

Trace 上下文经现有的 `RequestMeta.headers` map（`traceparent` / `tracestate` / `baggage`）到达 Python worker。worker **读取**头以关联日志/trace_id，但**不创建 span**。分布式追踪因此止于 Rust 边界；进入 worker 的全链路追踪是后续工作（需要 Python 侧埋点）。无需改动 protobuf——传播复用现有 headers map。

## 两级 opt-in

1. **构建期**：cargo feature `telemetry` 门控 OTel SDK/exporter/bridge crates（`opentelemetry_sdk`、`opentelemetry-otlp`、`tracing-opentelemetry`）。默认构建**不编译**它们。
   ```sh
   cargo build --features telemetry
   cargo test  --features telemetry   # 运行 telemetry 测试
   ```
2. **运行期**：`telemetry.enabled: false`（默认）。为 `false` 时 subscriber 不挂 OTel layer、不设全局 propagator、`extract`/`inject` 均为 no-op——服务器行为与无 OTel 完全一致。设 `telemetry.enabled: true` 并把 `otlp_endpoint` 指向 collector 即启用。

## 传播模型（D21 单一来源）

- **HTTP**：最外层 `observability_middleware` 只 extract 一次入站父上下文，暂存（`OtelParentContext`），并创建一个关联到该父上下文的 `http.server` span（字段：`http.request.method`、`url.path`、`http.response.status_code`）。`context_middleware` 把暂存读入 `RequestContext.trace_cx`。其它层不再重复 extract。
- **gRPC**：pre-call interceptor 把父上下文 extract 进 `RequestContext.trace_cx`；每个 handler 的 `inference` span 关联到它（`telemetry::link_parent`）。
- **Rust→worker**：在每一处构造 `RequestMeta` 的地方，把当前活动 span 的上下文注入 `headers`（`telemetry::inject`），因此 worker 的请求是 server/step span 的子级（覆盖客户端提供的任何 `traceparent`）。
- **ensemble（防断裂）**：`execute_step` 为每个子步骤构建关联到当前 trace 的 `ensemble.step` 子 span 并注入子步骤的 `headers`——否则每个 ensemble step span 都会与父请求 trace 失联。

## W3C 不变量

- 无效的 `traceparent`（全零 trace id / 非法 hex）被丢弃——请求自己开一条新 trace（W3C 铁律）。见 `extract_discards_invalid_traceparent`。
- `tracestate` 透传；`baggage` 按蓝图入站 allowlist 指导做清洗。

## 采样与关闭

- 采样：`ParentBased(TraceIdRatioBased(sample_ratio))`——根 span 按 `sample_ratio` 采样，子 span 尊重入站 sampled 标志。（按类别的 health/admin 降采样通过 `health_admin_sample_ratio` 独立生效。）
- 关闭：优雅关闭时，traces 和 metrics 在阻塞线程上 `force_flush` + shutdown，带 **5s 上限**——慢/不可达的 collector 无法拖住排空窗口。0.30 的 `BatchSpanProcessor`/`PeriodicReader` 跑在独立线程上，与 tokio runtime 解耦（避免 opentelemetry-rust #2715 的 `force_flush` 死锁）。

## Metrics SDK 与 exemplars（C4）

`telemetry.metrics_enabled: true` 时，OTel metrics SDK（MeterProvider + OTLP/gRPC MetricExporter + PeriodicReader）叠加在现有 Prometheus `/metrics` 管线之上。每个请求结束时记录 `liteserver.request.duration` 直方图（带 status-family 属性）。

> **Exemplar 注意事项（2026-08-01）**：`opentelemetry_sdk 0.30.0` 把 exemplar reservoir 留空（`exemplars: vec![]`）——该版本**不**发出真实 trace 关联的 exemplars。span 内记录管线的接线是正确的、exemplar-ready；真正发出 exemplars 需要升级 OTel SDK（已跟踪为后续项）。metrics→trace 关联在 collector 侧通过 Prometheus exemplar-storage + Grafana 完成。`exemplars_enabled` 为未来 SDK 预留。

## GenAI 语义约定（A5 / D34）

`gen_ai.*` span 属性名集中在 `src/telemetry/genai_attrs.rs`（一个文件）。截至 2026-07，OTel GenAI semconv 仍处于 **Development** 阶段（2026-06 移入独立仓库、无版本化发布、2025-08 有大规模改名），因此我们**不**钉死具体字段——未来稳定版是一处文件编辑的事。6–12 个月后重新评估（蓝图 §2.2 观察清单）。

## 版本（research-verified, 2026-08-01）

| crate | 版本 | 说明 |
|---|---|---|
| `opentelemetry` (core) | 0.30 | `trace` + `metrics` features；常驻依赖（不含 SDK/exporter）。 |
| `opentelemetry_sdk` | 0.30 | `rt-tokio`/`trace`/`metrics`；feature 门控。 |
| `opentelemetry-otlp` | 0.30 | `grpc-tonic`；引入 `tonic ^0.13`。 |
| `tracing-opentelemetry` | 0.31 | 目标 `opentelemetry 0.30`（0.30 发布钉的是 0.29）。 |
| `tonic` 全家 | 0.13 | 0.12→0.13 升级，与 `opentelemetry-otlp` 统一为单一 tonic 版本（解决 §6.3 多版本风险）。 |

## 快速开始

```sh
# 1. 跑一个 collector + 后端（如 otel-collector → Jaeger/Tempo），监听 :4317。

# 2. 带 feature 构建。
cargo build --release --features telemetry

# 3. 配置（server.yaml）。
#    telemetry:
#      enabled: true
#      otlp_endpoint: "http://collector:4317"
#      metrics_enabled: true   # 可选 OTLP/metrics 覆盖层

# 4. 带 traceparent 发一个请求；在 Jaeger/Tempo 里观察 `http.server` → `inference`
#    span 链，按 trace_id 与服务器日志关联。
```
