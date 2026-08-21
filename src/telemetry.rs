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
/// 仅 feature 开启的 `inject` 使用（无 feature 时 inject 为 no-op）。
#[cfg(feature = "telemetry")]
struct HeadersInjector<'a>(&'a mut HashMap<String, String>);

#[cfg(feature = "telemetry")]
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
///
/// 对账修复（e2e 举证）：优先取**当前 tracing span** 的 OTel context——生产
/// 路径中 instrumented inference span 的 context；仅在其无效时回退线程 attach
/// 的 `Context::current()`（单元测试/特殊场景）。原实现只用后者，而生产从不
/// attach → 注入静默空转（HTTP 仅因原始 header 透传而"碰巧正确"）。
#[cfg(not(feature = "telemetry"))]
pub fn inject(_headers: &mut HashMap<String, String>) {}

#[cfg(feature = "telemetry")]
pub fn inject(headers: &mut HashMap<String, String>) {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let span_cx = tracing::Span::current().context();
    let cx = if span_cx.span().span_context().is_valid() {
        span_cx
    } else {
        Context::current()
    };
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
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    span.set_parent(parent.clone());
    // 对账 A5（蓝图 §4.3「日志关联」）：OTel layer 活跃时把 trace_id/span_id
    // 落成 span 字段——纯文本 fmt 日志在 span 作用域内自动带出。未启用/未
    // 采样时 span_context 非法，跳过（字段保持 Empty）。
    let cx = span.context();
    let otel_span = cx.span();
    let sc = otel_span.span_context();
    if sc.is_valid() {
        span.record("trace_id", sc.trace_id().to_string().as_str());
        span.record("span_id", sc.span_id().to_string().as_str());
    }
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
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;
    use std::time::Duration;

    /// Held in a static so [`super::shutdown`] can force_flush + shutdown without
    /// threading the provider through every call site.
    ///
    /// B3 (leak-gap-audit-0821): RwLock<Option<>>, NOT OnceCell. A
    /// serve/stop/serve cycle must install a FRESH provider after the old
    /// one was shut down — the OnceCell swallowed the second `set` (error
    /// discarded with `let _ =`), leaving the just-built provider to die in
    /// Drop while every span of the second server run went into the dead
    /// one: total silent telemetry loss after any restart.
    pub(super) static TRACER_PROVIDER: std::sync::RwLock<Option<SdkTracerProvider>> =
        std::sync::RwLock::new(None);

    /// C4 (OTel metrics SDK): held for force_flush/shutdown at graceful stop.
    pub(super) static METER_PROVIDER: std::sync::RwLock<
        Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    > = std::sync::RwLock::new(None);

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

    /// 对账 A5（蓝图 §4.3 评审 2.2「dropped-spans 计数 + 导出失败计数桥接
    /// /metrics」）：装饰 OTLP exporter——成功按 batch 大小计 exported、失败
    /// 计 failures。BSP 队列满丢弃不可直接观测，以 ended−exported 差值逼近。
    /// 泛型化 inner 以便测试注入 fake。
    #[derive(Debug)]
    pub struct CountingExporter<E> {
        pub inner: E,
    }

    impl<E: opentelemetry_sdk::trace::SpanExporter> opentelemetry_sdk::trace::SpanExporter
        for CountingExporter<E>
    {
        fn export(
            &self,
            batch: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
            let n = batch.len() as u64;
            let fut = self.inner.export(batch);
            async move {
                let result = fut.await;
                match &result {
                    Ok(()) => crate::metrics::prometheus::OTEL_SPANS_EXPORTED_TOTAL.inc_by(n),
                    Err(_) => crate::metrics::prometheus::OTEL_EXPORT_FAILURES_TOTAL.inc(),
                }
                result
            }
        }

        fn shutdown(&mut self) -> opentelemetry_sdk::error::OTelSdkResult {
            self.inner.shutdown()
        }

        fn set_resource(&mut self, resource: &Resource) {
            self.inner.set_resource(resource);
        }
    }

    /// 配对的 processor 装饰：on_end 计 spans_ended（进入导出管线的总量）。
    #[derive(Debug)]
    pub struct CountingProcessor<P> {
        pub inner: P,
    }

    impl<P: opentelemetry_sdk::trace::SpanProcessor> opentelemetry_sdk::trace::SpanProcessor
        for CountingProcessor<P>
    {
        fn on_start(&self, span: &mut opentelemetry_sdk::trace::Span, cx: &Context) {
            self.inner.on_start(span, cx);
        }
        fn on_end(&self, span: opentelemetry_sdk::trace::SpanData) {
            crate::metrics::prometheus::OTEL_SPANS_ENDED_TOTAL.inc();
            self.inner.on_end(span);
        }
        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            self.inner.force_flush()
        }
        fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
            self.inner.shutdown_with_timeout(timeout)
        }
        fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            self.inner.shutdown()
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
        let bsp = BatchSpanProcessor::builder(CountingExporter { inner: exporter })
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
            .with_span_processor(CountingProcessor { inner: bsp })
            .with_sampler(sampler)
            .with_resource(resource.clone())
            .build();

        use opentelemetry::trace::TracerProvider;
        let tracer = provider.tracer("lite-server");
        *TRACER_PROVIDER.write().unwrap_or_else(|e| e.into_inner()) = Some(provider);

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
                    *METER_PROVIDER.write().unwrap_or_else(|e| e.into_inner()) =
                        Some(meter_provider);
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
        // B3: take the provider OUT — the slot must be empty so a later
        // re-init installs a fresh one (OnceCell semantics kept the corpse).
        let tracer_provider = TRACER_PROVIDER
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(provider) = tracer_provider.clone() {
            let flush = tokio::task::spawn_blocking(move || {
                let _ = provider.force_flush();
            });
            let _ = tokio::time::timeout(Duration::from_secs(5), flush).await;
        }
        // Final shutdown releases the exporter; also bounded.
        if let Some(provider) = tracer_provider {
            let stop = tokio::task::spawn_blocking(move || {
                let _ = provider.shutdown();
            });
            let _ = tokio::time::timeout(Duration::from_secs(5), stop).await;
        }
        // C4: flush + shut down the OTel metrics provider (own-thread reader).
        let meter_provider = METER_PROVIDER
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(provider) = meter_provider {
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

/// G2 (批次 4):OTel 流式镜像记录——no-op 安全(无 SDK / telemetry 关闭时
/// 全局 meter 是 no-op,调用零开销)。与 record_request_duration 同模式。
static STREAM_TTFT: once_cell::sync::OnceCell<opentelemetry::metrics::Histogram<f64>> =
    once_cell::sync::OnceCell::new();
static STREAM_TBT: once_cell::sync::OnceCell<opentelemetry::metrics::Histogram<f64>> =
    once_cell::sync::OnceCell::new();
static STREAM_DURATION: once_cell::sync::OnceCell<opentelemetry::metrics::Histogram<f64>> =
    once_cell::sync::OnceCell::new();
static STREAM_CHUNKS: once_cell::sync::OnceCell<opentelemetry::metrics::Counter<u64>> =
    once_cell::sync::OnceCell::new();

pub fn record_stream_ttft(protocol: &str, seconds: f64) {
    use opentelemetry::KeyValue;
    let histogram = STREAM_TTFT.get_or_init(|| {
        opentelemetry::global::meter("lite-server")
            .f64_histogram("liteserver.stream.ttft")
            .with_unit("s")
            .build()
    });
    histogram.record(seconds, &[KeyValue::new("protocol", protocol.to_string())]);
}

pub fn record_stream_tbt(protocol: &str, seconds: f64) {
    use opentelemetry::KeyValue;
    let histogram = STREAM_TBT.get_or_init(|| {
        opentelemetry::global::meter("lite-server")
            .f64_histogram("liteserver.stream.tbt")
            .with_unit("s")
            .build()
    });
    histogram.record(seconds, &[KeyValue::new("protocol", protocol.to_string())]);
}

/// stream_kind 是 S5 的 6 值封闭枚举(与 Prometheus 侧一致)。
pub fn record_stream_duration(stream_kind: &str, seconds: f64) {
    use opentelemetry::KeyValue;
    let histogram = STREAM_DURATION.get_or_init(|| {
        opentelemetry::global::meter("lite-server")
            .f64_histogram("liteserver.stream.duration")
            .with_unit("s")
            .build()
    });
    histogram.record(seconds, &[KeyValue::new("stream_kind", stream_kind.to_string())]);
}

pub fn record_stream_chunks(protocol: &str, count: u64) {
    use opentelemetry::KeyValue;
    let counter = STREAM_CHUNKS.get_or_init(|| {
        opentelemetry::global::meter("lite-server")
            .u64_counter("liteserver.stream.chunks")
            .build()
    });
    counter.add(count, &[KeyValue::new("protocol", protocol.to_string())]);
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== G2 (批次 4):OTel 流式镜像 no-op 安全 =====

    /// 无 SDK / telemetry 关闭时全局 meter 是 no-op——四个镜像记录调用
    /// 不 panic、不产生副作用,零开销。
    #[test]
    fn record_stream_metrics_noop_safe() {
        record_stream_ttft("sse", 0.05);
        record_stream_tbt("sse", 0.01);
        record_stream_duration("sse", 1.5);
        record_stream_chunks("sse", 3);
    }

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

    /// B3 (leak-gap-audit-0821): a serve/stop/serve cycle must be able to
    /// install a FRESH provider. With the old OnceCell storage, shutdown()
    /// left the dead provider in the slot and the second init's `set` was
    /// silently discarded — the entire second server run exported nothing.
    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn reinit_after_shutdown_installs_a_fresh_provider() {
        let cfg = crate::config::TelemetryConfig {
            enabled: true,
            // Never actually dialed: the exporter builds lazily; spans just
            // fail to export in the background.
            otlp_endpoint: "http://127.0.0.1:1".to_string(),
            metrics_enabled: true,
            ..Default::default()
        };
        let _layer1 = super::otel::init(&cfg);
        assert!(
            super::otel::TRACER_PROVIDER
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .is_some(),
            "first init installs the tracer provider"
        );
        super::otel::shutdown().await;
        assert!(
            super::otel::TRACER_PROVIDER
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .is_none(),
            "shutdown must TAKE the provider out — a corpse in the slot is \
             what silently kills the second server run's telemetry"
        );
        assert!(
            super::otel::METER_PROVIDER
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .is_none(),
            "same for the meter provider"
        );
        let _layer2 = super::otel::init(&cfg);
        assert!(
            super::otel::TRACER_PROVIDER
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .is_some(),
            "the second init installs a FRESH provider"
        );
        // Leave no live provider behind for other tests.
        super::otel::shutdown().await;
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

#[cfg(all(test, feature = "telemetry"))]
mod otel_health_tests {
    //! 对账 A5：导出健康计数（CountingExporter/CountingProcessor）+ span
    //! trace_id/span_id 字段（日志关联）。
    use super::otel::{CountingExporter, CountingProcessor};
    use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
    use opentelemetry_sdk::trace::{SpanData, SpanExporter, SpanProcessor};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct FakeExporter {
        fail: bool,
    }

    impl SpanExporter for FakeExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            if self.fail {
                Err(OTelSdkError::InternalFailure(format!("boom ({})", batch.len())))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Debug)]
    struct NoopProcessor;
    impl SpanProcessor for NoopProcessor {
        fn on_start(&self, _span: &mut opentelemetry_sdk::trace::Span, _cx: &opentelemetry::Context) {}
        fn on_end(&self, _span: SpanData) {}
        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> OTelSdkResult {
            Ok(())
        }
    }

    fn fake_span_data(name: &'static str) -> SpanData {
        SpanData {
            span_context: opentelemetry::trace::SpanContext::empty_context(),
            parent_span_id: opentelemetry::trace::SpanId::INVALID,
            span_kind: opentelemetry::trace::SpanKind::Server,
            name: name.into(),
            start_time: std::time::SystemTime::now(),
            end_time: std::time::SystemTime::now(),
            attributes: vec![],
            dropped_attributes_count: 0,
            events: Default::default(),
            links: Default::default(),
            status: opentelemetry::trace::Status::Unset,
            instrumentation_scope: opentelemetry::InstrumentationScope::default(),
        }
    }

    #[tokio::test]
    async fn counting_exporter_counts_success_by_batch_and_failures() {
        let fails0 = crate::metrics::prometheus::OTEL_EXPORT_FAILURES_TOTAL.get();
        let ok0 = crate::metrics::prometheus::OTEL_SPANS_EXPORTED_TOTAL.get();

        let good = CountingExporter { inner: FakeExporter { fail: false } };
        SpanExporter::export(&good, vec![fake_span_data("a"), fake_span_data("b")])
            .await
            .expect("ok exporter");
        let bad = CountingExporter { inner: FakeExporter { fail: true } };
        let _ = SpanExporter::export(&bad, vec![fake_span_data("c")]).await;

        assert_eq!(crate::metrics::prometheus::OTEL_SPANS_EXPORTED_TOTAL.get(), ok0 + 2, "成功按 batch 大小计数");
        assert_eq!(crate::metrics::prometheus::OTEL_EXPORT_FAILURES_TOTAL.get(), fails0 + 1, "失败按批次计数");
    }

    #[test]
    fn counting_processor_counts_ended_spans() {
        let ended0 = crate::metrics::prometheus::OTEL_SPANS_ENDED_TOTAL.get();
        let p = CountingProcessor { inner: NoopProcessor };
        p.on_end(fake_span_data("x"));
        p.on_end(fake_span_data("y"));
        assert_eq!(crate::metrics::prometheus::OTEL_SPANS_ENDED_TOTAL.get(), ended0 + 2);
    }

    /// link_parent 在 OTel layer 活跃时把 trace_id/span_id record 到 span
    /// 字段上（fmt 日志据此自动带出）。
    #[test]
    fn link_parent_records_trace_ids_when_layer_active() {
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Debug)]
        struct NoopExporter;
        impl SpanExporter for NoopExporter {
            async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
                Ok(())
            }
        }

        #[derive(Default)]
        struct Rec {
            recorded: Vec<(String, String)>,
        }
        struct RecLayer(Arc<Mutex<Rec>>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RecLayer {
            fn on_record(
                &self,
                _span: &tracing::span::Id,
                values: &tracing::span::Record<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                struct V<'a>(&'a mut Vec<(String, String)>);
                impl tracing::field::Visit for V<'_> {
                    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                        self.0.push((f.name().to_string(), format!("{v:?}")));
                    }
                    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                        self.0.push((f.name().to_string(), v.to_string()));
                    }
                }
                let mut v = V(&mut self.0.lock().unwrap().recorded);
                values.record(&mut v);
            }
        }

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(NoopExporter)
            .build();
        use opentelemetry::trace::TracerProvider;
        let tracer = provider.tracer("test");
        let rec: Arc<Mutex<Rec>> = Default::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .with(RecLayer(rec.clone())),
        );
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&dispatch);
            tracing::callsite::rebuild_interest_cache();
            let span = tracing::info_span!(
                "a5_test_span",
                trace_id = tracing::field::Empty,
                span_id = tracing::field::Empty,
            );
            super::link_parent(&span, &opentelemetry::Context::new());
            let _enter = span.enter();
        });
        handle.join().unwrap();

        let rec = rec.lock().unwrap();
        let get = |n: &str| rec.recorded.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone());
        let trace_id = get("trace_id").expect("link_parent 须 record trace_id");
        assert_eq!(trace_id.trim_matches('"').len(), 32, "trace_id 为 32 hex: {trace_id}");
        let span_id = get("span_id").expect("link_parent 须 record span_id");
        assert_eq!(span_id.trim_matches('"').len(), 16, "span_id 为 16 hex: {span_id}");
    }

    /// 无 OTel layer 时 link_parent 不 record（字段保持 Empty，不 panic）。
    #[test]
    fn link_parent_without_layer_is_noop() {
        let span = tracing::info_span!(
            "a5_noop_span",
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        );
        super::link_parent(&span, &opentelemetry::Context::new());
        // 无断言——不 panic 即通过（invalid span_context 跳过 record）。
    }
}

/// §6.7 解析面 property 测试（proptest）：入站 baggage 清洗的安全不变式——
/// 白名单外零泄漏、条数/字节上限、值不篡改、幂等。不依赖 SDK（纯函数）。
#[cfg(test)]
mod prop_tests {
    use super::*;
    use opentelemetry::baggage::{Baggage, BaggageExt};
    use proptest::prelude::*;
    use std::collections::HashMap as StdMap;

    /// 低基数 key（碰撞频繁，白名单命中/未命中都覆盖）+ 少量任意串。
    fn arb_key() -> impl Strategy<Value = String> {
        prop_oneof![
            4 => "[a-c]{1,4}",
            1 => "\\PC{0,8}",
        ]
    }

    fn arb_entries() -> impl Strategy<Value = Vec<(String, String)>> {
        prop::collection::vec((arb_key(), "\\PC{0,24}"), 0..12)
    }

    fn cx_of(entries: &[(String, String)]) -> Context {
        let mut bag = Baggage::default();
        for (k, v) in entries {
            bag.insert(k.clone(), v.clone());
        }
        Context::current_with_baggage(bag)
    }

    fn map_of(cx: &Context) -> StdMap<String, String> {
        cx.baggage().iter().map(|(k, (v, _))| (k.to_string(), v.as_str().to_string())).collect()
    }

    proptest! {
        /// 清洗不变式全集：保留 key ⊆ 白名单；条数 ≤ max_entries；单条 ≤ max_entry_bytes；
        /// 保留值 = 该 key 最终输入值（不篡改）；保留条数 = min(符合条件条数, max_entries)。
        #[test]
        fn scrub_invariants(
            entries in arb_entries(),
            allow_flags in prop::collection::vec(any::<bool>(), 0..12),
            extra_allow in prop::collection::vec("[a-c]{1,4}", 0..3),
            max_entries in 0usize..8,
            max_entry_bytes in 0usize..48,
        ) {
            // 白名单 = 输入 key 的策略驱动子集 + 额外 key（覆盖命中与未命中）
            let mut allowlist: Vec<String> = entries
                .iter()
                .zip(&allow_flags)
                .filter(|(_, f)| **f)
                .map(|((k, _), _)| k.clone())
                .collect();
            allowlist.extend(extra_allow);
            let policy =
                BaggagePolicy { allowlist: allowlist.clone(), max_entries, max_entry_bytes };
            // Baggage 自身会拒收非法 key（如空 key）——以实际进入 Baggage 的内容为
            // 输入基准，不变式刻画"清洗输出 vs 清洗输入"。
            let cx = cx_of(&entries);
            let input = map_of(&cx);
            let kept = map_of(&scrub_baggage_with(cx, &policy));

            for (k, v) in &kept {
                prop_assert!(allowlist.contains(k), "key {:?} escaped the allowlist", k);
                prop_assert!(k.len() + v.len() <= max_entry_bytes, "oversize entry kept");
                prop_assert_eq!(Some(v), input.get(k), "value tampered");
            }
            let eligible = input
                .iter()
                .filter(|(k, v)| allowlist.contains(*k) && k.len() + v.len() <= max_entry_bytes)
                .count();
            prop_assert_eq!(kept.len(), eligible.min(max_entries));
        }

        /// 幂等：清洗输出再清洗内容不变（保留条目天然全过策略）。
        #[test]
        fn scrub_idempotent(
            entries in arb_entries(),
            max_entries in 0usize..8,
            max_entry_bytes in 0usize..48,
        ) {
            let policy = BaggagePolicy {
                allowlist: entries.iter().map(|(k, _)| k.clone()).collect(),
                max_entries,
                max_entry_bytes,
            };
            let once = scrub_baggage_with(cx_of(&entries), &policy);
            let twice = scrub_baggage_with(once.clone(), &policy);
            prop_assert_eq!(map_of(&once), map_of(&twice));
        }

        /// 默认策略（空白名单）对任意输入全拒——拓扑②默认不透传。
        #[test]
        fn default_policy_denies_everything(entries in arb_entries()) {
            prop_assert!(
                map_of(&scrub_baggage_with(cx_of(&entries), &BaggagePolicy::default()))
                    .is_empty()
            );
        }
    }
}
