# Metrics 参考

Prometheus 端点:`GET /metrics`。OTel 叠加层:`telemetry.metrics_enabled`
(见 [otel-observability.md](otel-observability.md))。

流式请求级指标由 observability-gaps 工作引入(0.8.3)。本文档记录新增指标、
label 语义、桶变更与语义说明(D8/S3)——label 白名单的权威评审记录
(蓝图 §6.5 约束 10)。

## 请求级指标

| 指标 | Labels | 说明 |
|---|---|---|
| `liteserver_requests_total` | `model, version, status` | **不**受 `features.streaming_metrics` 门控。HTTP 流式(SSE/WS/h2-bidi)现记录 close 点与早期拒绝(D7)。客户端断开的流保持 `2xx`,另由独立 cancel counter 区分(D1)。 |
| `liteserver_request_duration_seconds` | `model, version` | 桶追加 `30/60/120`(S7/D8)——分钟级流时长不再落 `+Inf`。**语义(D8):** 流式计入后,该直方图混合 unary 与流式 e2e 时长,长流主导尾部;`/metrics/timeline` p99 与 Admin `GetModelStats.avg_duration_ms` 同样混入。 |

## 流式指标

下列所有 `liteserver_stream_*` 指标均随 `features.streaming_metrics` 门控(D9)。

| 指标 | Labels | 说明 |
|---|---|---|
| `liteserver_streaming_connections` | `model, version, protocol` | 既有 gauge(`protocol`:`sse`/`websocket`/`http2`/`grpc`)。 |
| `liteserver_streaming_ttft_seconds` | `model, version, protocol` | 桶追加 `5/10/30/60`——冷启动 TTFT > 2.5 s 不再落 `+Inf`。 |
| `liteserver_streaming_tbt_seconds` | `model, version, protocol` | 桶追加 `1/2.5/5`——慢解码 chunk 间隔不再落 `+Inf`。 |
| `liteserver_streaming_chunks_total` | `model, version, protocol` | 既有 counter。 |
| `liteserver_stream_cancelled_total` | `model, version, protocol` | 客户端中断的流(S2)。断开保持 `requests_total{2xx}`,由此 counter 承担区分(D1)。 |
| `liteserver_stream_errors_total` | `model, version, stream_kind, kind` | 流错误(S4)。`kind` 为封闭枚举:`worker_error`/`deadline`/`idle`/`protocol`/`panic`(panic 仅 WS writer 可达);`cancel`/`done`/`worker_eof` 不计。 |
| `liteserver_stream_duration_seconds` | `model, version, stream_kind` | 流 open→close 时长(S6)。桶 `0.1/0.5/1/2.5/5/10/30/60/120/300`。 |
| `liteserver_stream_output_bytes_total` | `model, version, stream_kind` | 输出 chunk 字节累加(S6),chunk 处累加、close 时上报。 |

### `protocol` vs `stream_kind`(S5/D2)

新指标(S4/S5/S6)携带 `stream_kind` label——6 值封闭枚举:
`sse` / `ws` / `http2` / `grpc_stream` / `grpc_bidi` / `grpc_decoupled`。
既有 `protocol` label 值**不变**(D2——保护既有查询)。`stream_kind` 与
`kind` label 均为封闭枚举,符合蓝图 §6.5 约束 10(label 白名单评审记录——
与 `liteserver_worker_inference_total` 的 `worker_id` label 同一先例)。
close 日志的 `reason` 字段是**日志字段而非指标 label**,无需评审。

## OTel 镜像(G2)

`liteserver.request.duration` 为既有;流式镜像为新增:

| OTel 指标 | Attribute | 镜像对象 |
|---|---|---|
| `liteserver.stream.ttft` | `protocol` | `streaming_ttft_seconds` |
| `liteserver.stream.tbt` | `protocol` | `streaming_tbt_seconds` |
| `liteserver.stream.duration` | `stream_kind` | `stream_duration_seconds` |
| `liteserver.stream.chunks` | `protocol` | `streaming_chunks_total` |

双重门控(D9):OTel 流式镜像仅在 `features.streaming_metrics` **与**
`telemetry.metrics_enabled` 同开时发出(调用点在 Prometheus `record_stream_*`
函数内,继承前者;后者关闭时 OTel meter 为 no-op,零开销)。

## access log 语义(G4)

`access_log_middleware` 计量到 handler 响应为止——对 SSE/WS 流即**首字节时间**
(header),而非流时长。流时长经 `liteserver_stream_duration_seconds` 与结构化
流生命周期日志可见(`stream opened`;`stream closed`,携带 `StreamCloseReason`
枚举的 `reason` 及 per-stream `chunks`/`output_bytes`/`duration_secs`;
`stream ended with error`;`stream cancelled by client`)。流式的 access-log
时延应读作首字节时间。

## `tokens_generated` 语义(S3)

Python worker 在 Done 帧 `Metrics` 中上报 `tokens_generated`——**近似口径**,
等于 per-stream 输出 chunk 数(worker 侧无 tokenizer;精确计数为后续项)。
它作为参数传入 `collect_metrics`,**不走** per-worker 共享的 `_metric_values`
通道,并发流不会交叉误归。unary 路径不填(无 chunk 概念)。零 chunk 流报 0,
Rust 侧 `> 0` 守卫使 `lite_server_tokens_generated_total` 不落盘。

`prefill_ms` / `decode_ms` 本期**不填**(S3 收窄):prefill 与 Rust 侧 TTFT
口径不同源(会看到两个对不上的 TTFT),decode 含下游背压(ZMQ HWM 阻塞会
撑大 worker 侧窗口)。两者与精确 token 计数一并移入 tokenizer 后续项。
