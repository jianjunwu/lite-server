use lazy_static::lazy_static;
use prometheus::{
    Counter, CounterVec, GaugeVec, HistogramOpts, HistogramVec, Registry, TextEncoder, Encoder,
};
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::Mutex;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Request metrics
    pub static ref REQUESTS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "lightserver_requests_total",
            "Total HTTP requests"
        ),
        &["model", "version", "status"]
    ).unwrap();

    pub static ref REQUEST_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "lightserver_request_duration_seconds",
            "End-to-end request latency"
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["model", "version"]
    ).unwrap();

    // Queue metrics
    pub static ref QUEUE_DEPTH: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "lightserver_queue_depth",
            "Current request queue size"
        ),
        &["model", "version"]
    ).unwrap();

    // Model metrics
    pub static ref MODEL_LOAD_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "lightserver_model_load_total",
            "Model load/unload events"
        ),
        &["model", "version", "action", "status"]
    ).unwrap();

    pub static ref VERSION_SWITCHES_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "lightserver_version_switches_total",
            "Active version changes"
        ),
        &["model"]
    ).unwrap();

    pub static ref ACTIVE_WORKERS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "lightserver_active_workers",
            "Number of alive inference workers"
        ),
        &["model", "version"]
    ).unwrap();

    // Ensemble metrics
    pub static ref ENSEMBLE_STEP_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "lightserver_ensemble_step_latency_seconds",
            "Per-step latency in ensemble DAG"
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        &["ensemble", "step", "model"]
    ).unwrap();

    // Streaming metrics
    pub static ref STREAMING_CONNECTIONS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "lightserver_streaming_connections",
            "Active bidirectional streaming connections"
        ),
        &["model", "version", "protocol"]
    ).unwrap();

    pub static ref STREAMING_TTFT: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "lightserver_streaming_ttft_seconds",
            "Time to first token in streaming"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]),
        &["model", "version", "protocol"]
    ).unwrap();

    pub static ref STREAMING_TBT: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "lightserver_streaming_tbt_seconds",
            "Time between tokens in streaming"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5]),
        &["model", "version", "protocol"]
    ).unwrap();

    pub static ref STREAMING_CHUNKS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "lightserver_streaming_chunks_total",
            "Total streaming output chunks"
        ),
        &["model", "version", "protocol"]
    ).unwrap();
}

// Custom metrics reported by Python workers
lazy_static! {
    static ref CUSTOM_COUNTERS: Mutex<HashMap<String, CounterVec>> = Mutex::new(HashMap::new());
    static ref CUSTOM_GAUGES: Mutex<HashMap<String, GaugeVec>> = Mutex::new(HashMap::new());
    static ref CUSTOM_HISTOGRAMS: Mutex<HashMap<String, HistogramVec>> = Mutex::new(HashMap::new());
}

pub fn register_metrics() -> Result<(), prometheus::Error> {
    REGISTRY.register(Box::new(REQUESTS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(REQUEST_DURATION.clone()))?;
    REGISTRY.register(Box::new(QUEUE_DEPTH.clone()))?;
    REGISTRY.register(Box::new(MODEL_LOAD_TOTAL.clone()))?;
    REGISTRY.register(Box::new(VERSION_SWITCHES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(ACTIVE_WORKERS.clone()))?;
    REGISTRY.register(Box::new(ENSEMBLE_STEP_LATENCY.clone()))?;
    REGISTRY.register(Box::new(STREAMING_CONNECTIONS.clone()))?;
    REGISTRY.register(Box::new(STREAMING_TTFT.clone()))?;
    REGISTRY.register(Box::new(STREAMING_TBT.clone()))?;
    REGISTRY.register(Box::new(STREAMING_CHUNKS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(INFERENCE_DURATION.clone()))?;
    REGISTRY.register(Box::new(BATCH_SIZE.clone()))?;
    Ok(())
}

pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = String::new();
    encoder.encode_utf8(&metric_families, &mut buffer).unwrap();
    buffer
}

// ===== Request metrics =====

pub fn record_request_start(model: &str, version: &str) {
    // Light-server uses explicit inc_queue_depth in queue manager
    // Kept for backward compat during transition
}

pub fn record_request_end(model: &str, version: &str, status: &str, duration_secs: f64) {
    REQUESTS_TOTAL.with_label_values(&[model, version, status]).inc();
    REQUEST_DURATION.with_label_values(&[model, version]).observe(duration_secs);
    super::aggregator::TIMELINE.record_latency(model, version, duration_secs);
}

// ===== Queue metrics =====

pub fn inc_queue_depth(model: &str, version: &str) {
    QUEUE_DEPTH.with_label_values(&[model, version]).inc();
}

pub fn dec_queue_depth(model: &str, version: &str) {
    QUEUE_DEPTH.with_label_values(&[model, version]).dec();
}

// ===== Model metrics =====

pub fn record_model_load(model: &str, version: &str, success: bool) {
    let status = if success { "success" } else { "fail" };
    MODEL_LOAD_TOTAL.with_label_values(&[model, version, "load", status]).inc();
}

pub fn record_model_unload(model: &str, version: &str) {
    MODEL_LOAD_TOTAL.with_label_values(&[model, version, "unload", "success"]).inc();
}

pub fn set_active_workers(model: &str, version: &str, count: f64) {
    ACTIVE_WORKERS.with_label_values(&[model, version]).set(count);
}

pub fn record_version_switch(model: &str) {
    VERSION_SWITCHES_TOTAL.with_label_values(&[model]).inc();
}

// ===== Ensemble metrics =====

pub fn record_ensemble_step_latency(ensemble: &str, step: &str, model: &str, latency_secs: f64) {
    ENSEMBLE_STEP_LATENCY.with_label_values(&[ensemble, step, model]).observe(latency_secs);
}

// ===== Streaming metrics =====

pub fn record_stream_open(model: &str, version: &str, protocol: &str) {
    STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).inc();
}

pub fn record_stream_chunk(model: &str, version: &str, protocol: &str) {
    STREAMING_CHUNKS_TOTAL.with_label_values(&[model, version, protocol]).inc();
}

pub fn record_stream_close(model: &str, version: &str, protocol: &str) {
    STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).dec();
}

// Pre-defined worker metrics (light-server compatible)
lazy_static! {
    pub static ref INFERENCE_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "lightserver_inference_duration_seconds",
            "Time inside predict()"
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["model"]
    ).unwrap();

    pub static ref BATCH_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "lightserver_batch_size",
            "Actual batch size processed"
        ).buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0]),
        &["model"]
    ).unwrap();
}

// ===== Custom metrics from Python workers =====

/// Record metrics reported by a Python worker.
pub fn record_worker_metrics(model: &str, metrics: &Option<Vec<crate::worker::protocol::WorkerMetric>>) {
    if let Some(metrics) = metrics {
        for m in metrics {
            let labels = m.labels.as_ref();
            match m.metric_type.as_str() {
                "histogram" => {
                    match m.name.as_str() {
                        "lightserver_inference_duration_seconds" => {
                            INFERENCE_DURATION.with_label_values(&[model]).observe(m.value);
                        }
                        "lightserver_batch_size" => {
                            BATCH_SIZE.with_label_values(&[model]).observe(m.value);
                        }
                        _ => {
                            // Dynamic histogram registration
                            let key = format!("{}:{:?}", m.name, labels);
                            let mut guard = CUSTOM_HISTOGRAMS.lock().unwrap();
                            let hist = guard.entry(key.clone()).or_insert_with(|| {
                                let label_names: Vec<&str> = labels
                                    .map(|l| l.keys().map(|s| s.as_str()).collect())
                                    .unwrap_or_default();
                                let h = HistogramVec::new(
                                    HistogramOpts::new(&m.name, "Worker-reported histogram")
                                        .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
                                    &label_names,
                                ).unwrap();
                                let _ = REGISTRY.register(Box::new(h.clone()));
                                h
                            });
                            let label_values: Vec<&str> = labels
                                .map(|l| l.values().map(|s| s.as_str()).collect())
                                .unwrap_or_default();
                            let _ = hist.with_label_values(&label_values).observe(m.value);
                        }
                    }
                }
                "counter" => {
                    let key = format!("{}:{:?}", m.name, labels);
                    let mut guard = CUSTOM_COUNTERS.lock().unwrap();
                    let counter = guard.entry(key.clone()).or_insert_with(|| {
                        let label_names: Vec<&str> = labels
                            .map(|l| l.keys().map(|s| s.as_str()).collect())
                            .unwrap_or_default();
                        let c = CounterVec::new(
                            prometheus::Opts::new(&m.name, "Worker-reported counter"),
                            &label_names,
                        ).unwrap();
                        let _ = REGISTRY.register(Box::new(c.clone()));
                        c
                    });
                    let label_values: Vec<&str> = labels
                        .map(|l| l.values().map(|s| s.as_str()).collect())
                        .unwrap_or_default();
                    let _ = counter.with_label_values(&label_values).inc_by(m.value);
                }
                "gauge" => {
                    let key = format!("{}:{:?}", m.name, labels);
                    let mut guard = CUSTOM_GAUGES.lock().unwrap();
                    let gauge = guard.entry(key.clone()).or_insert_with(|| {
                        let label_names: Vec<&str> = labels
                            .map(|l| l.keys().map(|s| s.as_str()).collect())
                            .unwrap_or_default();
                        let g = GaugeVec::new(
                            prometheus::Opts::new(&m.name, "Worker-reported gauge"),
                            &label_names,
                        ).unwrap();
                        let _ = REGISTRY.register(Box::new(g.clone()));
                        g
                    });
                    let label_values: Vec<&str> = labels
                        .map(|l| l.values().map(|s| s.as_str()).collect())
                        .unwrap_or_default();
                    let _ = gauge.with_label_values(&label_values).set(m.value);
                }
                _ => {}
            }
        }
    }
}
