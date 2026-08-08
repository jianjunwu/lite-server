# Metrics Reference

Prometheus endpoint: `GET /metrics`. OTel overlay: `telemetry.metrics_enabled`
(see [otel-observability.md](otel-observability.md)).

Streaming request-level metrics were added by the observability-gaps work
(0.8.3). This document records the new metrics, the label semantics, the
bucket changes, and the semantic notes (D8/S3) — the authoritative review
record for the label whitelist (蓝图 §6.5 约束 10).

## Request-level metrics

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_requests_total` | `model, version, status` | **Not** gated by `features.streaming_metrics`. HTTP streaming (SSE/WS/h2-bidi) now records close-point and early-rejection (D7). Disconnected streams keep `2xx` + a separate cancel counter (D1). |
| `liteserver_request_duration_seconds` | `model, version` | Buckets extended with `30/60/120` (S7/D8) — minute-scale stream durations no longer fall into `+Inf`. **Semantics (D8):** with streaming counted, this histogram mixes unary and stream e2e durations; long streams dominate the tail. Same for `/metrics/timeline` p99 and Admin `GetModelStats.avg_duration_ms`. |

## Streaming metrics

All `liteserver_stream_*` metrics below are gated by
`features.streaming_metrics` (D9), except `liteserver_requests_total`.

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_streaming_connections` | `model, version, protocol` | Existing gauge (`protocol`: `sse`/`websocket`/`http2`/`grpc`). |
| `liteserver_streaming_ttft_seconds` | `model, version, protocol` | Buckets extended with `5/10/30/60` — cold-start TTFT > 2.5 s no longer lands in `+Inf`. |
| `liteserver_streaming_tbt_seconds` | `model, version, protocol` | Buckets extended with `1/2.5/5` — slow decode gaps no longer land in `+Inf`. |
| `liteserver_streaming_chunks_total` | `model, version, protocol` | Existing counter. |
| `liteserver_stream_cancelled_total` | `model, version, protocol` | Client-interrupted streams (S2). Disconnect keeps `requests_total{2xx}`; this counter carries the distinction (D1). |
| `liteserver_stream_errors_total` | `model, version, stream_kind, kind` | Stream errors (S4). `kind` is a closed enum: `worker_error`/`deadline`/`idle`/`protocol`/`panic` (panic reachable via WS writer only). `cancel`/`done`/`worker_eof` do not count. |
| `liteserver_stream_duration_seconds` | `model, version, stream_kind` | Stream open→close duration (S6). Buckets `0.1/0.5/1/2.5/5/10/30/60/120/300`. |
| `liteserver_stream_output_bytes_total` | `model, version, stream_kind` | Σ output chunk bytes (S6), accumulated per chunk, reported at close. |

### `protocol` vs `stream_kind` (S5/D2)

New metrics (S4/S5/S6) carry the `stream_kind` label — a closed 6-value enum:
`sse` / `ws` / `http2` / `grpc_stream` / `grpc_bidi` / `grpc_decoupled`.
Existing `protocol` label values are **unchanged** (D2 — preserves existing
queries). The `stream_kind` and `kind` labels are closed enums per
蓝图 §6.5 约束 10 (label whitelist review record — same precedent as the
`worker_id` label on `liteserver_worker_inference_total`). The close-log
`reason` field is a **log field, not a metric label**, and needs no review.

## OTel mirror (G2)

`liteserver.request.duration` existed already; streaming mirrors are added:

| OTel metric | Attribute | Mirrors |
|---|---|---|
| `liteserver.stream.ttft` | `protocol` | `streaming_ttft_seconds` |
| `liteserver.stream.tbt` | `protocol` | `streaming_tbt_seconds` |
| `liteserver.stream.duration` | `stream_kind` | `stream_duration_seconds` |
| `liteserver.stream.chunks` | `protocol` | `streaming_chunks_total` |

Double gating (D9): an OTel streaming mirror is emitted only when
`features.streaming_metrics` **and** `telemetry.metrics_enabled` are both on
(the call sites live inside the Prometheus `record_stream_*` functions, which
inherit the former; the OTel meter is a no-op when the latter is off).

## Access log semantics (G4)

`access_log_middleware` measures time to the handler response — for SSE/WS
streams that is **first-byte time** (headers), not stream duration. Stream
durations are visible via `liteserver_stream_duration_seconds` and the
structured stream lifecycle logs (`stream opened` / `stream closed` with
`reason` from the `StreamCloseReason` enum plus per-stream
`chunks`/`output_bytes`/`duration_secs`, `stream ended with error`,
`stream cancelled by client`). Access-log latency for streaming should be
read as first-byte time.

## `tokens_generated` semantics (S3)

Python workers report `tokens_generated` in the Done-frame `Metrics` —
an **approximation** equal to the per-stream output chunk count (no worker-side
tokenizer; exact counting is a follow-up item). It is carried as a parameter
to `collect_metrics`, **not** through the shared per-worker `_metric_values`
channel, so concurrent streams cannot cross-contaminate. Unary paths do not
fill it (no chunk concept). Zero-chunk streams report 0 and the Rust
`> 0` guard keeps `lite_server_tokens_generated_total` unexposed.

`prefill_ms` / `decode_ms` are **not** filled by the worker in this release
(S3 收窄): prefill's TTFT口径 differs from the Rust-side TTFT (two
conflicting numbers), and decode includes downstream backpressure
(ZMQ HWM blocking inflates the worker-side window). Both move to the
tokenizer follow-up item together with exact token counting.
