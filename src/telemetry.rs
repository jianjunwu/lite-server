//! P-TRACE (蓝图 §4.3): 全量 OpenTelemetry——分布式追踪。
//!
//! **Rust-only (D8)**：trace 到 Rust 边界止。traceparent/tracestate/baggage 经
//! 既有 `RequestMeta.headers` map 透传到 Python worker（worker 读 header 关联
//! trace_id，但不创 span）；proto 零改动。
//!
//! **两级 opt-in**：① cargo `telemetry` feature（编译期门控 SDK/exporter/layer，
//! §6.7）；② 运行时 `telemetry.enabled`（默认 false，零开销）。两级都关时，
//! [`extract`]/[`inject`] 走全局默认 no-op propagator（空 Context / 不写 header），
//! 行为与无 OTel 逐字节一致。
//!
//! **D21 单一提取不变式**：HTTP [`extract`]（observability 最外）/ gRPC
//! [`extract_grpc`]（interceptor）各一次提取 OTel parent context；HTTP 侧 stash
//! 入 `OtelParentContext` extension 供 `context_middleware` 读 `trace_cx`，禁止二次 extract。
//!
//! **Rust→worker 注入**：[`inject`] 在每个 `RequestMeta` 构建点把当前 span 的
//! trace context 写入 `headers` map（worker 以 server/step span 为 parent）。
//!
//! **ensemble 防断裂**：`ensemble::execute_step` 建 child span + [`inject`] 子 step
//! headers（蓝图 §4.3 P-TRACE「ensemble 接线」）。
//!
//! **停机 flush**：[`shutdown`] 在 server 优雅停机窗口（HTTP/gRPC drain 后）显式调，
//! `force_flush`+`shutdown` 包 `spawn_blocking` + 超时上限——规避 BSP+Tokio Drop
//! 死锁（opentelemetry-rust #2715），并防拖住停机。

pub mod genai_attrs;

use crate::config::TelemetryConfig;
use opentelemetry::Context;
use std::collections::HashMap;

/// A boxed `tracing` subscriber layer keyed to `Registry`, so `logging::init` can
/// attach the OTel layer conditionally without naming the layer's generic tracer
/// type (which is cfg-gated out of existence when the feature is off).
pub type BoxedLayer =
    Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static>;

// ===========================================================================
// Always-available extract / inject (opentelemetry core + global propagator).
// No-op when the `telemetry` feature is off OR `enabled=false`: the default
// global propagator injects/extracts nothing.
// ===========================================================================

/// W3C traceparent/tracestate/baggage carrier adapter over an HTTP `HeaderMap`.
struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl<'a> opentelemetry::propagation::Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    // The installed propagators (W3C tracecontext + baggage) read fixed keys via
    // `get`; they never enumerate, so the default `keys()` discovery surface is
    // unused. Returning the header names keeps the contract honest for HTTP.
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Carrier adapter over a tonic gRPC `MetadataMap`.
struct MetadataExtractor<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> opentelemetry::propagation::Extractor for MetadataExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    // tonic 0.13's `KeyRef` does not expose `&str`, and the propagators never
    // enumerate keys (fixed key lookups via `get`), so discovery is unused here.
    fn keys(&self) -> Vec<&str> {
        Vec::new()
    }
}

/// Carrier adapter over a `RequestMeta.headers` map (Rust→worker injection).
struct HeadersInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> opentelemetry::propagation::Injector for HeadersInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

/// Extract the inbound OTel parent context from HTTP headers (D21 single-source).
/// Returns an empty `Context` when no propagator is configured (telemetry off).
pub fn extract(headers: &axum::http::HeaderMap) -> Context {
    opentelemetry::global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(headers)))
}

/// Extract the inbound OTel parent context from gRPC metadata.
pub fn extract_grpc(metadata: &tonic::metadata::MetadataMap) -> Context {
    opentelemetry::global::get_text_map_propagator(|p| p.extract(&MetadataExtractor(metadata)))
}

/// Inject the **current** trace context into `RequestMeta.headers` so the Python
/// worker can correlate (it reads `traceparent`, creates no span — D8). Overwrites
/// any client-supplied propagation headers with the active server/step span, giving
/// the worker the correct parent. No-op when telemetry is off (no propagator).
pub fn inject(headers: &mut HashMap<String, String>) {
    let cx = Context::current();
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&cx, &mut HeadersInjector(headers));
    });
}

/// C4 (OTel metrics SDK): record a request-duration observation on the OTel
/// histogram (status-family attribute). Recorded within the request's span path
/// so that — once the SDK populates exemplar reservoirs — the data point carries a
/// trace_id (metrics→trace jump). Overlay on the prometheus pipeline; no-op when
/// telemetry is off (the global meter is a no-op).
static REQUEST_DURATION: once_cell::sync::OnceCell<opentelemetry::metrics::Histogram<f64>> =
    once_cell::sync::OnceCell::new();

pub fn record_request_duration(status_family: &str, seconds: f64) {
    use opentelemetry::KeyValue;
    let histogram = REQUEST_DURATION.get_or_init(|| {
        opentelemetry::global::meter("lite-server")
            .f64_histogram("liteserver.request.duration")
            .with_unit("s")
            .build()
    });
    histogram.record(
        seconds,
        &[KeyValue::new("status", status_family.to_string())],
    );
}

/// Link a handler `info_span!` to the extracted OTel parent context (so the
/// server-side span is a child of the inbound trace). No-op stub when the
/// `telemetry` feature is off (the span stays a plain tracing span).
#[cfg(not(feature = "telemetry"))]
pub fn link_parent(_span: &tracing::Span, _parent: &Context) {}

#[cfg(feature = "telemetry")]
pub fn link_parent(span: &tracing::Span, parent: &Context) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    span.set_parent(parent.clone());
}

// ===========================================================================
// init / shutdown — feature-gated. When the feature is off, init returns None
// and shutdown is a no-op, so the call sites compile unchanged.
// ===========================================================================

#[cfg(not(feature = "telemetry"))]
pub fn init(_cfg: &TelemetryConfig) -> Option<BoxedLayer> {
    None
}

#[cfg(not(feature = "telemetry"))]
pub async fn shutdown() {}

#[cfg(feature = "telemetry")]
mod otel {
    //! Concrete OTel SDK wiring (cfg-gated). Kept in a submodule so the
    //! always-available extract/inject API above compiles without the SDK.

    use super::*;
    use crate::config::TelemetryProtocol;
    use once_cell::sync::OnceCell;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;
    use std::time::Duration;

    /// Held in a static so [`super::shutdown`] can force_flush + shutdown without
    /// threading the provider through every call site.
    static TRACER_PROVIDER: OnceCell<SdkTracerProvider> = OnceCell::new();

    /// C4 (OTel metrics SDK): held for force_flush/shutdown at graceful stop.
    static METER_PROVIDER: OnceCell<opentelemetry_sdk::metrics::SdkMeterProvider> = OnceCell::new();

    /// OTLP auth metadata (评审低#17) built once from `telemetry.otlp_headers`.
    fn build_metadata(cfg: &TelemetryConfig) -> tonic::metadata::MetadataMap {
        let mut metadata = tonic::metadata::MetadataMap::new();
        for (k, v) in &cfg.otlp_headers {
            if let (Ok(name), Ok(val)) = (
                tonic::metadata::MetadataKey::from_bytes(k.as_bytes()),
                v.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>(),
            ) {
                metadata.append(name, val);
            }
        }
        metadata
    }

    /// Build the OTel `Resource` (service.name + version + config attrs + the
    /// `OTEL_RESOURCE_ATTRIBUTES` env, which `Resource::builder` merges).
    fn build_resource(cfg: &TelemetryConfig) -> Resource {
        let mut builder = Resource::builder().with_service_name(cfg.service_name.clone());
        builder = builder.with_attribute(KeyValue::new(
            "service.version",
            env!("CARGO_PKG_VERSION").to_string(),
        ));
        for (k, v) in &cfg.resource_attributes {
            builder = builder.with_attribute(KeyValue::new(k.clone(), v.clone()));
        }
        builder.build()
    }

    pub fn init(cfg: &TelemetryConfig) -> Option<BoxedLayer> {
        if !cfg.enabled {
            return None;
        }
        if cfg.protocol == TelemetryProtocol::Http {
            // OTLP/HTTP exporter is not wired this period (tonic 0.13 unified, gRPC
            // is the supported path). Fail loud at startup rather than silently.
            tracing::warn!(
                "telemetry.protocol=http is not implemented; set protocol: grpc (default)"
            );
            return None;
        }

        let resource = build_resource(cfg);

        // ---- traces: OTLP/gRPC exporter → BatchSpanProcessor (dedicated thread) ----
        // opentelemetry_sdk 0.30: BatchSpanProcessor runs on its own dedicated
        // thread (no user-runtime param), so building the provider needs no tokio
        // context — and the BSP is decoupled from our runtime (no #2715 deadlock).
        use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
        let exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&cfg.otlp_endpoint)
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .with_metadata(build_metadata(cfg))
            .build()
        {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("telemetry: failed to build OTLP span exporter: {e}");
                return None;
            }
        };

        let batch_config = BatchConfigBuilder::default()
            .with_max_queue_size(cfg.max_queue_size)
            .with_scheduled_delay(Duration::from_millis(cfg.export_interval_millis))
            .build();
        let bsp = BatchSpanProcessor::builder(exporter)
            .with_batch_config(batch_config)
            .build();

        // ParentBased(root=TraceIdRatioBased(sample_ratio)): honour the inbound
        // sampled flag, and down-sample roots. (Per-class health/admin down-sampling
        // via health_admin_sample_ratio is a follow-up — the field is parsed today.)
        let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(cfg.sample_ratio)));

        let provider = SdkTracerProvider::builder()
            .with_span_processor(bsp)
            .with_sampler(sampler)
            .with_resource(resource.clone())
            .build();

        use opentelemetry::trace::TracerProvider;
        let tracer = provider.tracer("lite-server");
        let _ = TRACER_PROVIDER.set(provider);

        // ---- global propagator: W3C tracecontext + baggage (D21 single-source) ----
        use opentelemetry::propagation::TextMapCompositePropagator;
        use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
        let propagator = TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]);
        opentelemetry::global::set_text_map_propagator(propagator);

        let layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // ---- C4: OTel metrics SDK (overlay) — exemplar-ready plumbing ----
        // OTLP/gRPC MetricExporter → PeriodicReader (own thread) → MeterProvider.
        // The request-duration histogram is recorded within the active span (see
        // [`super::record_request_duration`]), which is the exemplar capture point.
        // NOTE: opentelemetry_sdk 0.30.0 stubs exemplar reservoirs (`exemplars: vec![]`);
        // real exemplar emission needs an SDK upgrade (tracked follow-up, 蓝图 C4).
        if cfg.metrics_enabled {
            match opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&cfg.otlp_endpoint)
                .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                .with_metadata(build_metadata(cfg))
                .build()
            {
                Ok(metric_exporter) => {
                    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
                        .with_interval(Duration::from_millis(cfg.export_interval_millis))
                        .build();
                    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                        .with_reader(reader)
                        .with_resource(resource.clone())
                        .build();
                    opentelemetry::global::set_meter_provider(meter_provider.clone());
                    let _ = METER_PROVIDER.set(meter_provider);
                    if cfg.exemplars_enabled {
                        tracing::info!(
                            "telemetry: exemplars requested — opentelemetry_sdk 0.30 stubs \
                             exemplar reservoirs; upgrade the SDK to emit trace-linked exemplars"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("telemetry: failed to build OTLP metric exporter: {e}");
                }
            }
        }

        Some(Box::new(layer))
    }

    pub async fn shutdown() {
        // force_flush + shutdown on a blocking thread, capped by a timeout. The
        // provider is held in a static; clone (Arc-backed) so the closure is 'static.
        // spawn_blocking avoids occupying a runtime worker while the BSP drains, and
        // the timeout bounds the graceful-shutdown window (蓝图 §4.3 force_flush 带超时).
        if let Some(provider) = TRACER_PROVIDER.get().cloned() {
            let flush = tokio::task::spawn_blocking(move || {
                let _ = provider.force_flush();
            });
            let _ = tokio::time::timeout(Duration::from_secs(5), flush).await;
        }
        // Final shutdown releases the exporter; also bounded.
        if let Some(provider) = TRACER_PROVIDER.get().cloned() {
            let stop = tokio::task::spawn_blocking(move || {
                let _ = provider.shutdown();
            });
            let _ = tokio::time::timeout(Duration::from_secs(5), stop).await;
        }
        // C4: flush + shut down the OTel metrics provider (own-thread reader).
        if let Some(provider) = METER_PROVIDER.get().cloned() {
            let stop = tokio::task::spawn_blocking(move || {
                let _ = provider.force_flush();
                let _ = provider.shutdown();
            });
            let _ = tokio::time::timeout(Duration::from_secs(5), stop).await;
        }
    }
}

#[cfg(feature = "telemetry")]
pub use otel::{init, shutdown};

#[cfg(test)]
mod tests {
    use super::*;

    // ===== inject / extract: always-available, no-op without a propagator =====

    #[test]
    fn inject_is_noop_without_propagator() {
        // No global propagator set in default tests → inject writes nothing.
        let mut headers = HashMap::from([("x-keep".to_string(), "v".to_string())]);
        inject(&mut headers);
        assert_eq!(headers.get("traceparent"), None);
        assert_eq!(headers.get("x-keep"), Some(&"v".to_string()));
    }

    #[test]
    fn extract_returns_empty_context_without_propagator() {
        use opentelemetry::trace::TraceContextExt;
        let mut h = axum::http::HeaderMap::new();
        h.insert("traceparent", "00-..".parse().unwrap());
        let cx = extract(&h);
        // No propagator → no active span extracted.
        assert!(!cx.span().span_context().is_valid());
    }

    #[test]
    fn extract_grpc_returns_empty_context_without_propagator() {
        use opentelemetry::trace::TraceContextExt;
        let md = tonic::metadata::MetadataMap::new();
        let cx = extract_grpc(&md);
        assert!(!cx.span().span_context().is_valid());
    }

    /// When the W3C tracecontext propagator IS installed (feature on + enabled),
    /// inject/extract round-trip a traceparent and the worker headers carry it.
    #[cfg(feature = "telemetry")]
    #[test]
    fn inject_extract_roundtrip_with_propagator() {
        use opentelemetry::propagation::TextMapCompositePropagator;
        use opentelemetry::trace::{Tracer, TracerProvider, TraceContextExt};
        use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
        // Install the composite propagator for this test (process-global).
        opentelemetry::global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));

        // Build a real span as the "current" context, inject into headers.
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("test");
        let _guard = opentelemetry::Context::current_with_span(tracer.start("root")).attach();

        let mut headers = HashMap::new();
        inject(&mut headers);
        let tp = headers
            .get("traceparent")
            .expect("traceparent injected when propagator installed")
            .clone();
        assert!(tp.starts_with("00-"), "W3C traceparent format, got {tp}");

        // Extract round-trips the same trace id.
        let mut inbound = axum::http::HeaderMap::new();
        inbound.insert("traceparent", tp.parse().unwrap());
        let extracted = extract(&inbound);
        assert!(
            extracted.span().span_context().is_valid(),
            "extracted context carries a valid span"
        );
    }

    /// W3C 铁律 (蓝图 §4.3): an invalid traceparent (all-zero trace id / bad hex)
    /// MUST be discarded — the request restarts its own trace rather than joining
    /// a bogus one.
    #[cfg(feature = "telemetry")]
    #[test]
    fn extract_discards_invalid_traceparent() {
        use opentelemetry::propagation::TextMapCompositePropagator;
        use opentelemetry::trace::TraceContextExt;
        use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
        opentelemetry::global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));

        // All-zero trace id is the W3C "invalid" sentinel → must not produce a span.
        let mut inbound = axum::http::HeaderMap::new();
        inbound.insert(
            "traceparent",
            "00-00000000000000000000000000000000-3a2c0d6f1f4e8b7a-01"
                .parse()
                .unwrap(),
        );
        let extracted = extract(&inbound);
        assert!(
            !extracted.span().span_context().is_valid(),
            "all-zero trace id must be discarded (no valid span extracted)"
        );
    }
}
