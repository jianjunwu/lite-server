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
//! **入站 baggage 清洗（§4.0.7 评审 2.2）**：入站 baggage 不受信——extract 出口
//! 按 `telemetry.baggage_allowlist`（默认空 = 全拒，拓扑②默认不透传）+ 条目数/
//! 单条目字节上限清洗，未过白名单的 baggage 不进 Context、不会注入 worker。
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
/// Inbound baggage is scrubbed per the installed [`BaggagePolicy`] (§4.0.7).
pub fn extract(headers: &axum::http::HeaderMap) -> Context {
    let cx = opentelemetry::global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(headers)));
    scrub_baggage_with(cx, baggage_policy())
}

/// Extract the inbound OTel parent context from gRPC metadata.
/// Inbound baggage is scrubbed per the installed [`BaggagePolicy`] (§4.0.7).
pub fn extract_grpc(metadata: &tonic::metadata::MetadataMap) -> Context {
    let cx = opentelemetry::global::get_text_map_propagator(|p| p.extract(&MetadataExtractor(metadata)));
    scrub_baggage_with(cx, baggage_policy())
}

// ===========================================================================
// 入站 baggage 清洗（蓝图 §4.0.7 评审 2.2）：入站 baggage 不受信——key 白名单
// + 单条目字节上限 + 总条目数上限。默认全拒（拓扑②默认不透传 baggage 到
// worker）；策略由 `otel::init`（feature 开 且 enabled=true）按 `telemetry
// .baggage_*` 安装，与 propagator 同生命周期。
// ===========================================================================

/// 入站 baggage 清洗策略（见上）。
#[derive(Debug, Clone)]
struct BaggagePolicy {
    /// key 白名单（精确匹配）；空 = 全拒。
    allowlist: Vec<String>,
    /// 最大保留条目数（白名单命中后按序截断）。
    max_entries: usize,
    /// 单条目（key+value）字节上限，超限条目丢弃。
    max_entry_bytes: usize,
}

impl Default for BaggagePolicy {
    /// 未安装策略时的兜底：全拒（deny-all）。
    fn default() -> Self {
        Self { allowlist: Vec::new(), max_entries: 16, max_entry_bytes: 128 }
    }
}

static BAGGAGE_POLICY: once_cell::sync::OnceCell<BaggagePolicy> =
    once_cell::sync::OnceCell::new();

/// 当前策略引用：init 已安装用安装的，否则默认全拒（feature 关/单测直调）。
fn baggage_policy() -> &'static BaggagePolicy {
    BAGGAGE_POLICY.get_or_init(BaggagePolicy::default)
}

/// 按策略清洗 Context 内 baggage（纯函数，无 SDK propagator 也可单测）。
/// 只保留白名单条目，按序截断条目数、丢弃超限条目；空 baggage 快速返回。
fn scrub_baggage_with(cx: Context, policy: &BaggagePolicy) -> Context {
    use opentelemetry::baggage::BaggageExt;
    let bag = cx.baggage();
    if bag.is_empty() {
        return cx;
    }
    let mut kept = opentelemetry::baggage::Baggage::default();
    for (key, (value, _meta)) in bag.iter() {
        if kept.len() >= policy.max_entries {
            break;
        }
        if !policy.allowlist.iter().any(|k| k == key.as_str()) {
            continue;
        }
        if key.as_str().len() + value.as_str().len() > policy.max_entry_bytes {
            continue;
        }
        kept.insert(key.clone(), value.clone());
    }
    cx.with_baggage(kept)
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

    /// 按端点类别独立采样（蓝图 §4.3 评审 2.2）：health/admin 探活高频 span 用
    /// `health_admin_sample_ratio`（默认 0，防探活刷 collector 配额），其余端点
    /// 用 `sample_ratio`。依据 span 创建时的 `endpoint.class` 属性分流（HTTP
    /// `http.server` span 按路径 stamp；无属性 → 默认比率——gRPC handler span
    /// 全为 inference 类，不 stamp）。根判定委托 `TraceIdRatioBased`；外层
    /// `ParentBased` 保持父采样位语义。
    #[derive(Debug, Clone)]
    pub(super) struct PerClassSampler {
        pub(super) default_ratio: f64,
        pub(super) health_admin_ratio: f64,
    }

    impl opentelemetry_sdk::trace::ShouldSample for PerClassSampler {
        fn should_sample(
            &self,
            parent_context: Option<&Context>,
            trace_id: opentelemetry::trace::TraceId,
            name: &str,
            span_kind: &opentelemetry::trace::SpanKind,
            attributes: &[KeyValue],
            links: &[opentelemetry::trace::Link],
        ) -> opentelemetry::trace::SamplingResult {
            let ratio = match attributes.iter().find(|kv| kv.key.as_str() == "endpoint.class") {
                Some(kv) if matches!(&*kv.value.as_str(), "health" | "admin") => {
                    self.health_admin_ratio
                }
                _ => self.default_ratio,
            };
            opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(ratio).should_sample(
                parent_context,
                trace_id,
                name,
                span_kind,
                attributes,
                links,
            )
        }
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

        // ParentBased(root=PerClassSampler)：honour 入站采样位；根 span 按
        // endpoint.class 分流——health/admin 用 health_admin_sample_ratio
        // （默认 0，防探活刷 collector 配额，评审 2.2），其余用 sample_ratio。
        let sampler = Sampler::ParentBased(Box::new(PerClassSampler {
            default_ratio: cfg.sample_ratio,
            health_admin_ratio: cfg.health_admin_sample_ratio,
        }));

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

        // 入站 baggage 清洗策略与 propagator 同生命周期安装（§4.0.7 评审 2.2）。
        // set 失败（已初始化）不影响——策略幂等，单测进程内可能已 pin 默认值。
        let _ = super::BAGGAGE_POLICY.set(super::BaggagePolicy {
            allowlist: cfg.baggage_allowlist.clone(),
            max_entries: cfg.baggage_max_entries,
            max_entry_bytes: cfg.baggage_max_entry_bytes,
        });

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

    // ===== 入站 baggage 清洗（§4.0.7 评审 2.2，scrub_baggage_with 纯函数）=====

    fn cx_with_baggage(items: &[(&str, &str)]) -> Context {
        use opentelemetry::baggage::{Baggage, BaggageExt};
        let mut bag = Baggage::default();
        for (k, v) in items {
            bag.insert(k.to_string(), v.to_string());
        }
        Context::current_with_baggage(bag)
    }

    fn baggage_keys(cx: &Context) -> Vec<String> {
        use opentelemetry::baggage::BaggageExt;
        cx.baggage().iter().map(|(k, _)| k.to_string()).collect()
    }

    #[test]
    fn scrub_default_policy_denies_all() {
        // 默认策略（空白名单）全拒——拓扑②默认不透传。
        let cx = cx_with_baggage(&[("tenant", "acme"), ("user_id", "u1")]);
        assert!(baggage_keys(&scrub_baggage_with(cx, &BaggagePolicy::default())).is_empty());
    }

    #[test]
    fn scrub_allowlist_keeps_only_listed_keys() {
        let policy = BaggagePolicy { allowlist: vec!["tenant".to_string()], ..Default::default() };
        let cx = cx_with_baggage(&[("tenant", "acme"), ("internal", "top-secret")]);
        assert_eq!(baggage_keys(&scrub_baggage_with(cx, &policy)), ["tenant"]);
    }

    #[test]
    fn scrub_caps_kept_entries() {
        let policy = BaggagePolicy {
            allowlist: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            max_entries: 2,
            ..Default::default()
        };
        let cx = cx_with_baggage(&[("a", "1"), ("b", "2"), ("c", "3")]);
        assert_eq!(baggage_keys(&scrub_baggage_with(cx, &policy)).len(), 2);
    }

    #[test]
    fn scrub_drops_oversized_entries() {
        let policy = BaggagePolicy {
            allowlist: vec!["a".to_string(), "b".to_string()],
            max_entry_bytes: 4, // key+value ≤ 4
            ..Default::default()
        };
        let cx = cx_with_baggage(&[("a", "toolongvalue"), ("b", "xy")]);
        assert_eq!(baggage_keys(&scrub_baggage_with(cx, &policy)), ["b"]);
    }

    // ===== PerClassSampler：health/admin 独立采样（蓝图 §4.3 评审 2.2）=====

    /// 接线护栏：sampler 只能看到 span **创建时**的属性——HTTP 根 span
    /// （http.server）必须在创建时按路径 stamp `endpoint.class`（gRPC handler
    /// 全为 inference 类，走默认比率分支，无需 stamp）。
    #[test]
    fn http_server_span_stamps_endpoint_class() {
        let src = include_str!("http/mod.rs");
        let boundary = src.find("#[cfg(test)]").unwrap_or(src.len());
        assert!(
            src[..boundary].contains("\"endpoint.class\" = crate::access_control::classify_http_path"),
            "B4: http.server span 必须在创建时 stamp endpoint.class（分类采样依据）"
        );
    }

    #[cfg(feature = "telemetry")]
    fn sample_decision(
        sampler: &super::otel::PerClassSampler,
        attrs: &[opentelemetry::KeyValue],
    ) -> opentelemetry::trace::SamplingDecision {
        use opentelemetry_sdk::trace::ShouldSample;
        sampler
            .should_sample(
                None,
                opentelemetry::trace::TraceId::from_bytes([42u8; 16]),
                "span",
                &opentelemetry::trace::SpanKind::Server,
                attrs,
                &[],
            )
            .decision
    }

    #[cfg(feature = "telemetry")]
    #[test]
    fn health_admin_spans_use_independent_ratio() {
        // default=1.0（全采）+ health_admin=0.0 → health/admin 全丢、其余全采。
        let sampler =
            super::otel::PerClassSampler { default_ratio: 1.0, health_admin_ratio: 0.0 };
        use opentelemetry::trace::SamplingDecision::*;
        let class = |c: &str| vec![opentelemetry::KeyValue::new("endpoint.class", c.to_string())];
        assert_eq!(sample_decision(&sampler, &class("health")), Drop);
        assert_eq!(sample_decision(&sampler, &class("admin")), Drop);
        assert_eq!(sample_decision(&sampler, &class("inference")), RecordAndSample);
        // 无 endpoint.class 属性 → 默认比率。
        assert_eq!(sample_decision(&sampler, &[]), RecordAndSample);
    }

    #[cfg(feature = "telemetry")]
    #[test]
    fn health_admin_ratio_is_independent_of_default_ratio() {
        // 反向：default=0.0 + health_admin=1.0 → health 采、inference 丢。
        let sampler =
            super::otel::PerClassSampler { default_ratio: 0.0, health_admin_ratio: 1.0 };
        use opentelemetry::trace::SamplingDecision::*;
        let class = |c: &str| vec![opentelemetry::KeyValue::new("endpoint.class", c.to_string())];
        assert_eq!(sample_decision(&sampler, &class("health")), RecordAndSample);
        assert_eq!(sample_decision(&sampler, &class("inference")), Drop);
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

    /// 蓝图 §4.0.7 评审 2.2：入站 baggage 不受信——经 BaggagePropagator 提取后
    /// 必须过白名单清洗；默认策略（空白名单）全拒，任何条目不得进入 Context。
    #[cfg(feature = "telemetry")]
    #[test]
    fn inbound_baggage_is_not_allowlisted() {
        use opentelemetry::baggage::BaggageExt;
        use opentelemetry::propagation::TextMapCompositePropagator;
        use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
        opentelemetry::global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));

        let mut inbound = axum::http::HeaderMap::new();
        inbound
            .insert("baggage", "tenant=acme;user_id=u1;internal=top-secret".parse().unwrap());
        let cx = extract(&inbound);
        let items: Vec<String> = cx.baggage().iter().map(|(k, _)| k.to_string()).collect();
        assert!(
            items.is_empty(),
            "蓝图 §4.0.7: 未过 allowlist 的入站 baggage 必须被清洗（空 allowlist 下全拒），\
             实际原样透传 {items:?}"
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
