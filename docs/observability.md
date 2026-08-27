# Observability

Prometheus endpoint: `GET /metrics` (with an optional OpenTelemetry overlay
when `telemetry.metrics_enabled` is on). This document covers the metrics
reference and the OpenTelemetry integration.

- [Metrics Reference](#metrics-reference)
- [OpenTelemetry](#opentelemetry)

## Metrics Reference

Streaming request-level metrics were added in 0.8.3. This section records
the metrics, the label semantics, the bucket changes, and the semantic notes
— the authoritative review record for the label whitelist.

### Request-level metrics

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_requests_total` | `model, version, status` | **Not** gated by `features.streaming_metrics`. HTTP streaming (SSE/WS/h2-bidi) now records close-point and early-rejection. Disconnected streams keep `2xx` + a separate cancel counter. |
| `liteserver_request_duration_seconds` | `model, version` | Buckets extended with `30/60/120` — minute-scale stream durations no longer fall into `+Inf`. **Semantics:** with streaming counted, this histogram mixes unary and stream e2e durations; long streams dominate the tail. Same for `/metrics/timeline` p99 and Admin `GetModelStats.avg_duration_ms`. |
| `lite_server_http_request_body_bytes` | `content_type, route` | HTTP request body size histogram. `content_type` = `json` \| `raw` \| `triton_binary` — `triton_binary` covers Triton Binary Tensor Data Extension requests (`Inference-Header-Content-Length` > 0). `route` = matched-path pattern. Buckets 1 KB–256 MB. |
| `liteserver_http_connections` | `transport` | Open HTTP connections (`transport`: `tcp`/`tls`/`uds`). Connection-level resource signal — idle keep-alive connections, TLS handshakes, and slowloris holds are visible here, not in request metrics. Not gated. |
| `liteserver_callback_dispatch_dropped_total` | `reason` | Callback dispatches dropped by the concurrency cap (`reason="concurrency"`, 64 in-flight) or cut by `callbacks.timeout_secs` (`reason="timeout"`). Callbacks are fire-and-forget; this counter makes the loss visible. Not gated. |

### Process & build info

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_info` | `version` | Build info; value always 1. Set at startup. |
| `liteserver_process_resident_memory_bytes` | — | RSS of the server process. |
| `liteserver_process_virtual_memory_bytes` | — | Virtual memory of the server process. |
| `liteserver_process_cpu_seconds_total` | — | Cumulative CPU time (seconds, across cores) — monotonic counter. |
| `liteserver_process_start_time_seconds` | — | Process start time (Unix epoch seconds); uptime = `time() - this`. |
| `liteserver_process_threads` | — | Thread count (populated on Linux/Android only; 0 elsewhere). |

All process metrics refresh at scrape time inside `/metrics` (no background
task). Unary early rejections (404/503/401/429/400 before queue dispatch)
count toward `liteserver_requests_total`, as do ensemble unary top-level
requests (one request = one count; sub-model steps are not counted).

### Worker memory

Worker processes are separate Python processes with their own PIDs; they are
sampled in the same scrape-time refresh as the server process (registered at
spawn/respawn, dropped at unload).

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_worker_resident_memory_bytes` | `model, version, worker_id` | RSS of a single worker process. |
| `liteserver_worker_virtual_memory_bytes` | `model, version, worker_id` | Virtual memory of a single worker process. |
| `liteserver_workers_resident_memory_bytes` | `model, version` | RSS summed over the version's live workers — per-version alerting without a PromQL `sum`. |

A worker that dies without an unload (crash, eject, forced kill) has its
series removed on the next scrape rather than frozen at its last value; PID
reuse is detected via the process start time. The aggregate only sums live
workers.

### Warmup metrics

Added in 0.9.0. Both families are recorded once per terminal warmup run —
the load-time run and each respawn re-warm (`warmup.respawn`) alike — and are
purged on version unload.

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_model_warmup_duration_seconds` | `model, version` | Whole-run wall time (all samples × iterations × worker shares), observed on success AND failure — a failed run's wall time is diagnostic too. |
| `liteserver_model_warmup_total` | `model, version, status` | `status` ∈ `success` \| `failure` \| `timeout` (closed enum; `timeout` covers both the per-iteration and the `total_timeout_secs` budget). |

Warmup traffic is synthetic: the batch collector never mixes warmup items
with live requests in one batch, and all-warmup batches count toward none of
the real-traffic series — `liteserver_inference_duration_seconds`,
`liteserver_batch_size`, `liteserver_worker_inference_total`,
`liteserver_queue_wait_seconds`, and `liteserver_retries_total` all stay pure
real-traffic signals. Warmup inferences also do not consume the per-worker
`max_requests` recycle budget (worker-scope warmup multiplies the unit
count by the worker count; counting it could recycle a worker immediately
after every load). Rolling recycles surface as
`liteserver_worker_respawns_total{reason="rolling_recycle"}` plus a transient
per-worker ejection. Respawn re-warm failures additionally count
`liteserver_worker_respawn_failures_total{reason="warmup"}`.

### Streaming metrics

The detailed stream-lifecycle metrics below are gated by
`features.streaming_metrics` (default on; CLI `--no-streaming-metrics`),
**except** `liteserver_stream_rejected_total`. The `/metrics/timeline`
`active_streams` field derives from `liteserver_streaming_connections`, so it
reads 0 while the gate is off.

Three stream-adjacent signals stay **ungated** by design — cheap server-level
accounting you want most precisely when the expensive per-chunk metrics are
off:

| Metric | Why exempt |
|---|---|
| `liteserver_requests_total` / `liteserver_request_duration_seconds` | Every stream still counts as one request, at close-point or pre-open rejection; gating these would blind request-level SLOs. |
| `liteserver_stream_rejected_total` | Rejection accounting parallels `requests_total` — the reject path is exactly what matters when lifecycle detail is off. |
| `liteserver_http_connections` | Connection-level resource signal (idle keep-alive, TLS handshakes, slowloris), orthogonal to per-stream detail. |

The stream-close **lifecycle log** (`stream closed`, carrying the close
`reason` — plus error/cancel warnings) is likewise ungated: it is the
debugging channel that remains when the metrics are off. The `stream opened`
log, by contrast, fires only inside the gate.

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_streaming_connections` | `model, version, protocol` | Existing gauge (`protocol`: `sse`/`websocket`/`http2`/`grpc`). |
| `liteserver_streaming_ttft_seconds` | `model, version, protocol` | Buckets extended with `5/10/30/60` — cold-start TTFT > 2.5 s no longer lands in `+Inf`. |
| `liteserver_streaming_tbt_seconds` | `model, version, protocol` | Buckets extended with `1/2.5/5` — slow decode gaps no longer land in `+Inf`. |
| `liteserver_streaming_chunks_total` | `model, version, protocol` | Existing counter. |
| `liteserver_stream_cancelled_total` | `model, version, protocol` | Client-interrupted streams. Disconnect keeps `requests_total{2xx}`; this counter carries the distinction. |
| `liteserver_stream_errors_total` | `model, version, stream_kind, kind` | Stream errors. `kind` is a closed enum: `worker_error`/`deadline`/`idle`/`protocol`/`panic` (panic reachable via WS writer only). `cancel`/`done`/`worker_eof` do not count. |
| `liteserver_stream_duration_seconds` | `model, version, stream_kind` | Stream open→close duration. Buckets `0.1/0.5/1/2.5/5/10/30/60/120/300`. |
| `liteserver_stream_output_bytes_total` | `model, version, stream_kind` | Σ output chunk bytes, accumulated per chunk, reported at close. |
| `liteserver_stream_rejected_total` | `model, version, reason` | Pre-open stream rejections. `reason` is a closed enum: `concurrency_limit` (P10 DAG cap — capacity signal) / `early_reject` (resolve/auth/rate-limit/not-ready/first-frame). Not gated on `streaming_metrics`. |

#### `protocol` vs `stream_kind`

New streaming metrics carry the `stream_kind` label — a closed 6-value enum:
`sse` / `ws` / `http2` / `grpc_stream` / `grpc_bidi` / `grpc_decoupled`.
Existing `protocol` label values are **unchanged** — existing queries
preserve their meaning. The `stream_kind` and `kind` labels are closed
enums, reviewed together with the `worker_id` label on
`liteserver_worker_inference_total`. The close-log `reason` field is a
**log field, not a metric label**.

### Ensemble metrics

Ensemble DAG metrics (0.9.0 ensemble streaming/enhancement batch). Entry
metrics (`stream_open`/`stream_ttft`/`stream_tbt`/`stream_terminal` and
`requests_total`) report the **ensemble model's name** — the client view,
identical labels to unary requests.

| Metric | Labels | Notes |
|---|---|---|
| `liteserver_ensemble_step_latency_seconds` | `ensemble, step, model, version, depth` | Per-step latency in ensemble DAG. `version` records the actual version only for **explicit** versions; `latest`/omitted normalize to `"latest"` so active-version drift cannot grow the label set (model × step × version). `depth` = nesting depth (E1). For streaming steps the latency spans open→close. Buckets extended with `10/30/60` — cold-start sub-models / slow steps > 5 s no longer land in `+Inf`. |
| `ensemble_streaming_active` | — | Gauge: concurrent streaming ensemble DAGs (the P10 semaphore in use). **Global, no labels** — same scope as the semaphore; per-model visibility comes from the streaming metrics above. |
| `ensemble_autoload_wait_seconds` | — | Histogram: sub-model autoload wait duration for ensemble DAG steps (P6 cold-start tracking). |
| `ensemble_pipeline_chain_depth` | — | Histogram: streaming steps on a pipeline chain (§4.2). |
| `ensemble_pipeline_channel_saturation_seconds` | — | Histogram: cumulative time a pipeline chain inter-hop channel was full (backpressure). |
| `ensemble_bidi_aggregate_bytes` | — | Histogram: bytes aggregated for a bidi ensemble request (D17 upstream aggregation). |
| `ensemble_bidi_aggregate_seconds` | — | Histogram: elapsed time aggregating a bidi ensemble request. |

P10 rejection semantics: when `server.max_concurrent_streaming_dags` is
exhausted the request is rejected **immediately** (429 — no queueing); the
rejection is recorded via `record_stream_rejected` at the ensemble dispatch
(all transports) and surfaces through `liteserver_requests_total` (status
label `4xx`), the `ensemble_streaming_active` gauge, and the dedicated
counter `liteserver_stream_rejected_total{model, version, reason}` —
`reason="concurrency_limit"` for the P10 cap (a capacity signal,
distinguishable from client-error 4xx), `reason="early_reject"` for S1b
pre-open rejections (resolve/auth/rate-limit/not-ready/first-frame).

### OTel mirror

`liteserver.request.duration` existed already; streaming mirrors are added:

| OTel metric | Attribute | Mirrors |
|---|---|---|
| `liteserver.stream.ttft` | `protocol` | `streaming_ttft_seconds` |
| `liteserver.stream.tbt` | `protocol` | `streaming_tbt_seconds` |
| `liteserver.stream.duration` | `stream_kind` | `stream_duration_seconds` |
| `liteserver.stream.chunks` | `protocol` | `streaming_chunks_total` |

Double gating: an OTel streaming mirror is emitted only when
`features.streaming_metrics` **and** `telemetry.metrics_enabled` are both on
(the call sites live inside the Prometheus `record_stream_*` functions, which
inherit the former; the OTel meter is a no-op when the latter is off).

### OTel export health (Prometheus)

The telemetry pipeline observes itself via three Prometheus counters (always
registered; stay at 0 when `telemetry.enabled` is false):

| Metric | Meaning |
|---|---|
| `liteserver_otel_spans_ended_total` | Spans that entered the export pipeline (processor `on_end`). |
| `liteserver_otel_spans_exported_total` | Spans successfully exported to the collector (per-batch). |
| `liteserver_otel_export_failures_total` | Failed OTLP export batches. |

`ended − exported` approximates dropped spans (the BatchSpanProcessor's
full-queue drop is not directly observable). All request spans also carry
`trace_id`/`span_id` fields when telemetry is enabled, so plain-text logs
inside a span scope correlate with traces automatically.

### Access log semantics

`access_log_middleware` measures time to the handler response — for SSE/WS
streams that is **first-byte time** (headers), not stream duration. Stream
durations are visible via `liteserver_stream_duration_seconds` and the
structured stream lifecycle logs (`stream opened` / `stream closed` with
`reason` from the `StreamCloseReason` enum plus per-stream
`chunks`/`output_bytes`/`duration_secs`, `stream ended with error`,
`stream cancelled by client`). Access-log latency for streaming should be
read as first-byte time.

### Control-plane audit (D27)

Every control-plane **mutation** emits a structured audit record (HTTP admin
handlers and gRPC Admin, same record shape). Read-only endpoints
(`ListModels`, `GetInfo`, health) do not audit.

| Field | Meaning |
|---|---|
| `action` | `load` / `unload` / `reload` / `delete` / `activate` / `set_routing` |
| `model`, `version` | The target model (version `None` for `set_routing`) |
| `request_id` | Correlated request id (UUID-v4 fallback on the gRPC Admin side) |
| `client_ip` | Peer address (XFF-cleansed when behind trusted proxies) |
| `principal` | mTLS client principal, when TLS mutual auth is configured |
| `key_fingerprint` | SHA-256 hex prefix (12 chars) of the configured API key — distinguishes pre-/post-rotation keys in the log **without** writing the secret; `None` for public / loopback / unconfigured policies |
| `details` | Before/after values where applicable, e.g. `weights {"1": 70} -> {"2": 100}`, `previous_active=Some("1") -> 2`; failures are audited too (`activate` not-ready) |

Records go to the dedicated log target `lite_server::audit` at `info` level
(no extra configuration). EnvFilter uses the **underscore** form:

```sh
RUST_LOG=lite_server::audit=info
```

### Timeline endpoint

`GET /metrics/timeline` (all loaded versions), `GET /metrics/timeline/{model}`
(active version), and `GET /metrics/timeline/{model}/versions/{version}`
return the in-memory ring buffer sampled every
`metrics.timeline_sample_interval_secs` (default 10s) with up to
`metrics.timeline_max_points` (default 30) points per model/version. Sampling
reads the existing Prometheus registry only — no extra recording points.

Each entry carries:

| Field | Source | Notes |
|---|---|---|
| `timestamp` | sample time | Unix seconds. |
| `qps` | `liteserver_requests_total` delta | All status families. |
| `p99_ms` | in-process latency window | Sliding window (`p99_window_*` knobs). |
| `queue_depth` | `liteserver_queue_depth` | |
| `active_workers` | `liteserver_active_workers` | |
| `active_streams` | `liteserver_streaming_connections` | Summed across `protocol`; 0 while `features.streaming_metrics` is off. |
| `in_flight` | `liteserver_in_flight_requests` | Queued + processing. |
| `worker_saturation` | `liteserver_worker_saturation` | Hottest worker's concurrent batches. |
| `ttft_p99_ms` | `liteserver_streaming_ttft_seconds` | Bucket-interpolated, merged across `protocol`. Process-lifetime histogram, **not** a sliding window; 0 without streaming traffic. |
| `tbt_p99_ms` | `liteserver_streaming_tbt_seconds` | Same derivation as `ttft_p99_ms`. |
| `stream_bytes_per_s` | `liteserver_stream_output_bytes_total` delta | Summed across `stream_kind`, rated over the sample window. |
| `tokens_per_s` | `lite_server_tokens_generated_total` delta | `null` until the model reports tokens via the worker callback channel. |
| `rss_mb` | `liteserver_workers_resident_memory_bytes` | Live-worker RSS sum for the version, MiB. |
| `cpu_percent` | `liteserver_process_cpu_seconds_total` delta | Process-wide, cumulative across cores — may exceed 100. The sampler refreshes process metrics itself, so no scraper is required. |
| `retries_per_s` | `liteserver_retries_total` delta | Rated over the sample window. |
| `ejections_per_s` | `liteserver_worker_ejections_total` delta | Rated over the sample window. |

Instances older than this schema omit the new fields entirely; clients must
treat absent fields as "not supported by this instance version".

**Downsampling**: `?step=N` (integer ≥ 1) keeps every Nth point anchored at
the latest sample, so the freshest point is always included. `step=0` is
rejected with 400. Example: with a 24h window (1440 points), `?step=5`
returns ~288 points covering the full window.

**Response headers**:

| Header | Meaning |
|---|---|
| `X-Timeline-Coverage` | Retention window in seconds (`timeline_max_points` × `timeline_sample_interval_secs`) — the longest range a client can honestly display. |
| `X-Timeline-Interval` | Point spacing in seconds; clients use it to convert a desired time range into a `step`. |

**Retention window**: the defaults (30 × 10s = 5 minutes) suit live
debugging. For a 24h window at 1-minute resolution:

```yaml
metrics:
  timeline_max_points: 1440        # 24h at 60s spacing
  timeline_sample_interval_secs: 60
```

Memory cost is bounded by `timeline_max_points` × loaded versions; the ring
is per model/version and reaped on unload.

### `tokens_generated` semantics

Python workers report `tokens_generated` in the Done-frame `Metrics` —
an **approximation** equal to the per-stream output chunk count (no worker-side
tokenizer; exact counting is a follow-up item). It is carried as a parameter
to `collect_metrics`, **not** through the shared per-worker `_metric_values`
channel, so concurrent streams cannot cross-contaminate. Unary paths do not
fill it (no chunk concept). Zero-chunk streams report 0 and the Rust
`> 0` guard keeps `lite_server_tokens_generated_total` unexposed.

`prefill_ms` / `decode_ms` are **not** filled by the worker in this release:
prefill's TTFT口径 differs from the Rust-side TTFT (two conflicting numbers),
and decode includes downstream backpressure (ZMQ HWM blocking inflates the
worker-side window). Both move to the tokenizer follow-up item together with
exact token counting.

### Accelerator metrics (vendor-neutral)

Added in the M4 admin-enhancement round. The core links **no vendor SDK** —
model code reads its own stack (pynvml, torch.mlu, torch_npu, …) and reports
through the same worker `Metrics` piggyback channel as `tokens_generated`,
via `lit_api.report_accelerator_metrics(device, accel, ...)` (all value
fields optional; omitted fields stay absent). Readings are device-scoped,
latest-per-device: the worker buffers one reading per `(device, accel)` pair
and attaches it to the next response's `Metrics.accelerator`, so an idle
model simply reports nothing until traffic resumes.

| Metric | Labels | Notes |
|---|---|---|
| `lite_server_accelerator_utilization_percent` | `device, accel` | Compute utilization 0-100. |
| `lite_server_accelerator_memory_used_bytes` | `device, accel` | Device memory in use. |
| `lite_server_accelerator_memory_total_bytes` | `device, accel` | Device memory capacity. |
| `lite_server_accelerator_temperature_celsius` | `device, accel` | Device temperature. |

Label whitelist: `device` (slot id, e.g. `"0"`/`"cuda:0"`) and `accel`
(vendor tag: `cuda`/`mlu`/`npu`/…, bounded in practice) are admitted for
these families only; both values are length-capped (64 chars) and the
distinct `(device, accel)` pair count is capped at 64 — pairs beyond the cap
are dropped (one-shot warning), never created as series. Because the
families carry no `model`/`version` labels they are not part of the
version-unload purge (a device may be shared by several versions); the pair
cap is the bounding mechanism.

`GET /metrics/accelerator` (feature `features.accelerator_metrics`, default
on — fixed families with capped labels and no background sampler, unlike the
opt-in worker-named `custom_metrics`) returns the latest reading per device
as a JSON array:
`[{device, accel, utilization_percent, memory_used_bytes, memory_total_bytes, temperature_celsius, updated_at}]`.
Unreported fields are `null`; `updated_at` (epoch seconds) exposes
staleness. With no reports the array is empty (`[]`) — clients should show a
"model has not reported" empty state, not an error. With the feature off the
route is unmounted (404).

`kv_cache_utilization` is deliberately untouched: device memory is **not** a
KV-cache proxy, and the GIE gauge stays NaN until a model reports KV
utilization explicitly.

## OpenTelemetry

Full OpenTelemetry tracing + metrics SDK, exported over **OTLP/gRPC**, for
the Rust core of lite-server. W3C traceparent propagates across the
gateway→server→worker boundary, tracing spans are bridged to OTel, and an
exemplar-ready metrics overlay is provided.

### Rust-only boundary

Trace context reaches the Python worker via the existing `RequestMeta.headers`
map (`traceparent` / `tracestate` / `baggage`). The worker **reads** the
header to correlate logs/trace_id but **creates no span**. Distributed tracing
therefore stops at the Rust boundary; end-to-end tracing into the worker is a
follow-up (requires Python-side instrumentation). No protobuf change is
needed — propagation rides the existing headers map.

### Two-level opt-in

1. **Build-time**: the cargo feature `telemetry` gates the OTel SDK/exporter/bridge crates (`opentelemetry_sdk`, `opentelemetry-otlp`, `tracing-opentelemetry`). The default build does **not** compile them.
   ```sh
   cargo build --features telemetry
   cargo test  --features telemetry   # runs the telemetry tests
   ```
2. **Runtime**: `telemetry.enabled: false` (default). When `false`, no OTel layer is attached to the subscriber, no global propagator is set, and `extract`/`inject` are no-ops — the server behaves byte-for-byte as without OTel. Set `telemetry.enabled: true` and point `otlp_endpoint` at a collector to enable.

### Propagation model

- **HTTP**: the outermost `observability_middleware` extracts the inbound parent context once, stashes it (`OtelParentContext`), and creates an `http.server` span (fields: `http.request.method`, `url.path`, `http.response.status_code`) linked to that parent. `context_middleware` reads the stash into `RequestContext.trace_cx`. No other layer re-extracts.
- **gRPC**: the pre-call interceptor extracts the parent into `RequestContext.trace_cx`; each handler's `inference` span links to it.
- **Rust→worker**: at every `RequestMeta` construction site the active span's context is injected into `headers`, so the worker's request is a child of the server/step span (overwriting any client-supplied `traceparent`).
- **ensemble (anti-breakage)**: `execute_step` builds a child `ensemble.step` span linked to the current trace and injects into the sub-step `headers` — without this, every ensemble step span would be orphaned from the parent request trace.

### W3C invariants

- An invalid `traceparent` (all-zero trace id / bad hex) is discarded — the request restarts its own trace (W3C rule). See `extract_discards_invalid_traceparent`.
- `tracestate` is passed through; `baggage` is sanitized per the inbound-allowlist guidance (`telemetry.baggage_allowlist`).

### Sampling & shutdown

- Sampling: `ParentBased(TraceIdRatioBased(sample_ratio))` — roots sampled at `sample_ratio`, child spans honour the inbound sampled flag. Health/admin roots use the independent `health_admin_sample_ratio` (default `0.0`) so high-frequency probes do not burn collector quota (see configuration.md §Telemetry).
- Shutdown: on graceful shutdown, traces and metrics are `force_flush`ed + shut down on a blocking thread with a **5s cap**, so a slow/unreachable collector cannot stall the drain window. The 0.30 `BatchSpanProcessor`/`PeriodicReader` run on dedicated threads, decoupling them from the tokio runtime (avoids the `force_flush` deadlock in opentelemetry-rust #2715).

### Metrics SDK & exemplars

With `telemetry.metrics_enabled: true`, an OTel metrics SDK (MeterProvider + OTLP/gRPC MetricExporter + PeriodicReader) overlays the existing Prometheus `/metrics` pipeline. A `liteserver.request.duration` histogram (status-family attribute) is recorded at each request's end.

> **Exemplar caveat (2026-08-01)**: `opentelemetry_sdk 0.30.0` stubs exemplar reservoirs (`exemplars: vec![]`) — real trace-linked exemplars are **not** emitted on this version. The recording-within-span plumbing is correct and exemplar-ready; emitting exemplars requires upgrading the OTel SDK (tracked follow-up). The metrics→trace link is completed on the collector side via Prometheus exemplar-storage + Grafana. `exemplars_enabled` is reserved for that future SDK.

### GenAI semantic conventions

`gen_ai.*` span-attribute names are centralized in `src/telemetry/genai_attrs.rs` (one file). The OTel GenAI semconv is still **Development** as of 2026-07 (moved to a separate repo 2026-06, no versioned release, large rename 2025-08), so we do **not** pin specific fields — a future stable release is a one-file edit. Re-evaluate in 6–12 months.

### Versions (research-verified, 2026-08-01)

| crate | version | note |
|---|---|---|
| `opentelemetry` (core) | 0.30 | `trace` + `metrics` features; constant dependency (no SDK/exporter). |
| `opentelemetry_sdk` | 0.30 | `rt-tokio`/`trace`/`metrics`; feature-gated. |
| `opentelemetry-otlp` | 0.30 | `grpc-tonic`; pulls `tonic ^0.13`. |
| `tracing-opentelemetry` | 0.31 | targets `opentelemetry 0.30` (the 0.30 release pins 0.29). |
| `tonic` family | 0.13 | upgraded 0.12→0.13 to unify a single tonic version with `opentelemetry-otlp`. |

### Quick start

```sh
# 1. Run a collector + backend (e.g. otel-collector → Jaeger/Tempo) listening on :4317.

# 2. Build with the feature.
cargo build --release --features telemetry

# 3. Configure (server.yaml).
#    telemetry:
#      enabled: true
#      otlp_endpoint: "http://collector:4317"
#      metrics_enabled: true   # optional OTLP/metrics overlay

# 4. Send a request with a traceparent; observe the `http.server` → `inference`
#    span chain in Jaeger/Tempo, correlated by trace_id with server logs.
```
