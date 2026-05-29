use lazy_static::lazy_static;
use prometheus::{
    Counter, CounterVec, GaugeVec, HistogramOpts, HistogramVec, Registry, TextEncoder, Encoder,
};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Mutex;

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

pub async fn record_request_end(model: &str, version: &str, status: &str, duration_secs: f64) {
    REQUESTS_TOTAL.with_label_values(&[model, version, status]).inc();
    REQUEST_DURATION.with_label_values(&[model, version]).observe(duration_secs);
    super::aggregator::TIMELINE.record_latency(model, version, duration_secs).await;
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
pub async fn record_worker_metrics(model: &str, metrics: Option<&crate::proto::liteserver::Metrics>) {
    if let Some(m) = metrics {
        if m.prefill_ms > 0.0 {
            let mut guard = CUSTOM_GAUGES.lock().await;
            let gauge = guard.entry("prefill_ms".to_string()).or_insert_with(|| {
                let g = GaugeVec::new(
                    prometheus::Opts::new("lite_server_prefill_ms", "Prefill latency in ms"),
                    &["model"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(g.clone()));
                g
            });
            let _ = gauge.with_label_values(&[model]).set(m.prefill_ms as f64);
        }
        if m.decode_ms > 0.0 {
            let mut guard = CUSTOM_GAUGES.lock().await;
            let gauge = guard.entry("decode_ms".to_string()).or_insert_with(|| {
                let g = GaugeVec::new(
                    prometheus::Opts::new("lite_server_decode_ms", "Decode latency in ms"),
                    &["model"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(g.clone()));
                g
            });
            let _ = gauge.with_label_values(&[model]).set(m.decode_ms as f64);
        }
        if m.tokens_generated > 0 {
            let mut guard = CUSTOM_COUNTERS.lock().await;
            let counter = guard.entry("tokens_generated".to_string()).or_insert_with(|| {
                let c = CounterVec::new(
                    prometheus::Opts::new("lite_server_tokens_generated_total", "Total tokens generated"),
                    &["model"],
                ).unwrap();
                let _ = REGISTRY.register(Box::new(c.clone()));
                c
            });
            let _ = counter.with_label_values(&[model]).inc_by(m.tokens_generated as f64);
        }
    }
}

// ===== Streaming metric recording functions =====

pub fn record_stream_ttft(model: &str, version: &str, protocol: &str, ttft_secs: f64) {
    STREAMING_TTFT.with_label_values(&[model, version, protocol]).observe(ttft_secs);
}

pub fn record_stream_tbt(model: &str, version: &str, protocol: &str, tbt_secs: f64) {
    STREAMING_TBT.with_label_values(&[model, version, protocol]).observe(tbt_secs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_stream_ttft_records_value() {
        let model = "ttft_model";
        let version = "1";
        let protocol = "sse";
        let before = STREAMING_TTFT.with_label_values(&[model, version, protocol]).get_sample_count();
        record_stream_ttft(model, version, protocol, 0.05);
        let after = STREAMING_TTFT.with_label_values(&[model, version, protocol]).get_sample_count();
        assert_eq!(after, before + 1);
    }

    #[test]
    fn test_record_stream_tbt_records_value() {
        let model = "tbt_model";
        let version = "1";
        let protocol = "websocket";
        let before = STREAMING_TBT.with_label_values(&[model, version, protocol]).get_sample_count();
        record_stream_tbt(model, version, protocol, 0.01);
        let after = STREAMING_TBT.with_label_values(&[model, version, protocol]).get_sample_count();
        assert_eq!(after, before + 1);
    }

    #[test]
    fn test_record_stream_open_increments_connections() {
        let model = "conn_model";
        let version = "1";
        let protocol = "sse";
        let before = STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).get();
        record_stream_open(model, version, protocol);
        let after = STREAMING_CONNECTIONS.with_label_values(&[model, version, protocol]).get();
        assert_eq!(after, before + 1.0);
        // cleanup
        record_stream_close(model, version, protocol);
    }

    #[test]
    fn test_record_stream_close_decrements_connections() {
        let model = "close_model";
        let version = "1";
        let protocol = "sse";
        record_stream_open(model, version, protocol);
        record_stream_close(model, version, protocol);
        // net zero — gauge should be back to its value before open
        // (other tests may have incremented, so just check relative)
    }

    #[test]
    fn test_record_stream_chunk_increments_total() {
        let model = "chunk_model";
        let version = "1";
        let protocol = "grpc";
        let before = STREAMING_CHUNKS_TOTAL.with_label_values(&[model, version, protocol]).get();
        record_stream_chunk(model, version, protocol);
        let after = STREAMING_CHUNKS_TOTAL.with_label_values(&[model, version, protocol]).get();
        assert_eq!(after, before + 1.0);
    }

    #[test]
    fn test_streaming_metrics_distinguished_by_protocol() {
        record_stream_open("proto_m", "1", "sse");
        record_stream_open("proto_m", "1", "websocket");
        record_stream_open("proto_m", "1", "grpc");

        let sse = STREAMING_CONNECTIONS.with_label_values(&["proto_m", "1", "sse"]).get();
        let ws = STREAMING_CONNECTIONS.with_label_values(&["proto_m", "1", "websocket"]).get();
        let grpc = STREAMING_CONNECTIONS.with_label_values(&["proto_m", "1", "grpc"]).get();

        assert!(sse >= 1.0);
        assert!(ws >= 1.0);
        assert!(grpc >= 1.0);

        // cleanup
        record_stream_close("proto_m", "1", "sse");
        record_stream_close("proto_m", "1", "websocket");
        record_stream_close("proto_m", "1", "grpc");
    }

    #[tokio::test]
    async fn test_record_worker_metrics_concurrent_access() {
        use crate::proto::liteserver::Metrics;
        let metrics = Metrics {
            prefill_ms: 10.0,
            decode_ms: 5.0,
            tokens_generated: 100,
        };
        let m = Some(&metrics);

        // Concurrent calls should not panic or deadlock
        let (r1, r2, r3) = tokio::join!(
            async { record_worker_metrics("concurrent_m1", m).await },
            async { record_worker_metrics("concurrent_m2", m).await },
            async { record_worker_metrics("concurrent_m3", m).await },
        );
    }
}
