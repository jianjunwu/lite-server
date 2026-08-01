# OpenTelemetry Observability (P-TRACE)

Full OpenTelemetry tracing + metrics SDK, exported over **OTLP/gRPC**, for the Rust core of lite-server. This realizes blueprint pillar C ("全链路可观测") — W3C traceparent propagation across the gateway→server→worker boundary, tracing spans bridged to OTel, and an exemplar-ready metrics overlay.

## Rust-only boundary (D8)

Trace context reaches the Python worker via the existing `RequestMeta.headers` map (`traceparent` / `tracestate` / `baggage`). The worker **reads** the header to correlate logs/trace_id but **creates no span**. Distributed tracing therefore stops at the Rust boundary; end-to-end tracing into the worker is a follow-up (requires Python-side instrumentation). No protobuf change is needed — propagation rides the existing headers map.

## Two-level opt-in

1. **Build-time**: the cargo feature `telemetry` gates the OTel SDK/exporter/bridge crates (`opentelemetry_sdk`, `opentelemetry-otlp`, `tracing-opentelemetry`). The default build does **not** compile them.
   ```sh
   cargo build --features telemetry
   cargo test  --features telemetry   # runs the telemetry tests
   ```
2. **Runtime**: `telemetry.enabled: false` (default). When `false`, no OTel layer is attached to the subscriber, no global propagator is set, and `extract`/`inject` are no-ops — the server behaves byte-for-byte as without OTel. Set `telemetry.enabled: true` and point `otlp_endpoint` at a collector to enable.

## Propagation model (D21 single-source)

- **HTTP**: the outermost `observability_middleware` extracts the inbound parent context once, stashes it (`OtelParentContext`), and creates an `http.server` span (fields: `http.request.method`, `url.path`, `http.response.status_code`) linked to that parent. `context_middleware` reads the stash into `RequestContext.trace_cx`. No other layer re-extracts.
- **gRPC**: the pre-call interceptor extracts the parent into `RequestContext.trace_cx`; each handler's `inference` span links to it (`telemetry::link_parent`).
- **Rust→worker**: at every `RequestMeta` construction site the active span's context is injected into `headers` (`telemetry::inject`), so the worker's request is a child of the server/step span (overwriting any client-supplied `traceparent`).
- **ensemble (防断裂)**: `execute_step` builds a child `ensemble.step` span linked to the current trace and injects into the sub-step `headers` — without this, every ensemble step span would be orphaned from the parent request trace.

## W3C invariants

- An invalid `traceparent` (all-zero trace id / bad hex) is discarded — the request restarts its own trace (W3C 铁律). See `extract_discards_invalid_traceparent`.
- `tracestate` is passed through; `baggage` is sanitized per the blueprint's inbound-allowlist guidance.

## Sampling & shutdown

- Sampling: `ParentBased(TraceIdRatioBased(sample_ratio))` — roots sampled at `sample_ratio`, child spans honour the inbound sampled flag. (Per-class health/admin down-sampling via `health_admin_sample_ratio` is parsed but not yet wired.)
- Shutdown: on graceful shutdown, traces and metrics are `force_flush`ed + shut down on a blocking thread with a **5s cap**, so a slow/unreachable collector cannot stall the drain window. The 0.30 `BatchSpanProcessor`/`PeriodicReader` run on dedicated threads, decoupling them from the tokio runtime (avoids the `force_flush` deadlock in opentelemetry-rust #2715).

## Metrics SDK & exemplars (C4)

With `telemetry.metrics_enabled: true`, an OTel metrics SDK (MeterProvider + OTLP/gRPC MetricExporter + PeriodicReader) overlays the existing Prometheus `/metrics` pipeline. A `liteserver.request.duration` histogram (status-family attribute) is recorded at each request's end.

> **Exemplar caveat (2026-08-01)**: `opentelemetry_sdk 0.30.0` stubs exemplar reservoirs (`exemplars: vec![]`) — real trace-linked exemplars are **not** emitted on this version. The recording-within-span plumbing is correct and exemplar-ready; emitting exemplars requires upgrading the OTel SDK (tracked follow-up). The metrics→trace link is completed on the collector side via Prometheus exemplar-storage + Grafana. `exemplars_enabled` is reserved for that future SDK.

## GenAI semantic conventions (A5 / D34)

`gen_ai.*` span-attribute names are centralized in `src/telemetry/genai_attrs.rs` (one file). The OTel GenAI semconv is still **Development** as of 2026-07 (moved to a separate repo 2026-06, no versioned release, large rename 2025-08), so we do **not** pin specific fields — a future stable release is a one-file edit. Re-evaluate in 6–12 months (blueprint §2.2 watch list).

## Versions (research-verified, 2026-08-01)

| crate | version | note |
|---|---|---|
| `opentelemetry` (core) | 0.30 | `trace` + `metrics` features; constant dependency (no SDK/exporter). |
| `opentelemetry_sdk` | 0.30 | `rt-tokio`/`trace`/`metrics`; feature-gated. |
| `opentelemetry-otlp` | 0.30 | `grpc-tonic`; pulls `tonic ^0.13`. |
| `tracing-opentelemetry` | 0.31 | targets `opentelemetry 0.30` (the 0.30 release pins 0.29). |
| `tonic` family | 0.13 | upgraded 0.12→0.13 to unify a single tonic version with `opentelemetry-otlp` (resolves the §6.3 multi-version risk). |

## Quick start

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
