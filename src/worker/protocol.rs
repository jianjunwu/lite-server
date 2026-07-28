use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub uid: String,
    pub payload: RequestPayload,
}

/// A metric reported by a Python worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetric {
    pub name: String,
    pub value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<HashMap<String, String>>,
    #[serde(rename = "type")]
    pub metric_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchItem {
    pub uid: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RequestPayload {
    #[serde(rename = "INFER")]
    Infer { data: serde_json::Value },
    #[serde(rename = "BATCH_INFER")]
    BatchInfer { items: Vec<BatchItem> },
    #[serde(rename = "STREAM_OPEN")]
    StreamOpen { stream_id: String },
    #[serde(rename = "STREAM_CHUNK")]
    StreamChunk { stream_id: String, chunk: serde_json::Value },
    #[serde(rename = "STREAM_CLOSE")]
    StreamClose { stream_id: String },
    #[serde(rename = "STREAM_CANCEL")]
    StreamCancel { stream_id: String },
    #[serde(rename = "FILE_CHANGED")]
    FileChanged { paths: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub uid: String,
    pub data: Option<serde_json::Value>,
    pub status: ResponseStatus,
    pub worker_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Vec<WorkerMetric>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseStatus {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ResponseStatus {
    pub fn ok() -> Self {
        Self {
            code: "Ok".to_string(),
            message: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            code: "Error".to_string(),
            message: Some(message.into()),
        }
    }

    pub fn streaming() -> Self {
        Self {
            code: "Streaming".to_string(),
            message: None,
        }
    }

    pub fn finish_streaming() -> Self {
        Self {
            code: "FinishStreaming".to_string(),
            message: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInferenceResponse {
    #[serde(rename = "type")]
    pub response_type: String,
    pub items: Vec<BatchResponseItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Vec<WorkerMetric>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponseItem {
    pub uid: String,
    pub data: Option<serde_json::Value>,
    pub status: ResponseStatus,
    pub worker_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSpec {
    pub name: String,
    pub metric_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStartup {
    pub status: String,
    pub worker_id: u32,
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_specs: Option<Vec<MetricSpec>>,
    /// Custom `@route` declarations emitted by the Python worker at handshake
    /// (phase 2). Empty list when the model declares no routes.
    #[serde(default)]
    pub custom_routes: Vec<RouteDecl>,
}

// ===== Route declarations =====
// Route declaration shared between the Rust HTTP layer and the Python
// model-worker route integration (phase 2).

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteDecl {
    pub route: String,
    pub methods: Vec<String>,
}

/// Reserved first-segment leaves under `/v2/models/:model_name/` — registered
/// as exact axum routes (see `src/http/routes.rs`). A `@route` whose tail
/// starts with one of these would shadow a system route, so `is_reserved_route`
/// gates ingestion (warn + skip). Source of truth: the `.route(...)` calls in
/// `create_routes`.
pub const SYSTEM_ROUTE_LEAVES: &[&str] = &[
    "compare", "versions", "ready", "health", "reload", "infer", "events", "stream",
];

/// True if a declared route tail (e.g. "/status", "/infer", "/pets/{id}")
/// collides with a system-reserved leaf. Matches on the first path segment.
pub fn is_reserved_route(route: &str) -> bool {
    let first = route.trim_start_matches('/').split('/').next().unwrap_or("");
    !first.is_empty() && SYSTEM_ROUTE_LEAVES.contains(&first)
}

/// Convert Python `{param}` placeholders (from route decorators) to axum
/// `:param` route syntax. Shared by route registration and route-policy
/// keying so both sides agree on the same route string (C10 dedup).
pub fn convert_path_params(route: &str) -> String {
    let mut result = String::with_capacity(route.len());
    let mut chars = route.chars();
    while let Some(c) = chars.next() {
        if c == '{' {
            result.push(':');
            for c2 in chars.by_ref() {
                if c2 == '}' {
                    break;
                }
                result.push(c2);
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_batch_infer_serde() {
        let req = InferenceRequest {
            uid: "batch-1".to_string(),
            payload: RequestPayload::BatchInfer {
                items: vec![
                    BatchItem {
                        uid: "u1".to_string(),
                        data: json!({"input": 5}),
                    },
                    BatchItem {
                        uid: "u2".to_string(),
                        data: json!({"input": 7}),
                    },
                ],
            },
        };
        let json_str = serde_json::to_string(&req).unwrap();
        assert!(json_str.contains("BATCH_INFER"));
        assert!(json_str.contains("u1"));
        assert!(json_str.contains("u2"));

        let decoded: InferenceRequest = serde_json::from_str(&json_str).unwrap();
        match decoded.payload {
            RequestPayload::BatchInfer { items } => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].uid, "u1");
            }
            _ => panic!("expected BatchInfer"),
        }
    }

    #[test]
    fn test_batch_response_serde() {
        let resp = BatchInferenceResponse {
            response_type: "BATCH_RESPONSE".to_string(),
            items: vec![
                BatchResponseItem {
                    uid: "u1".to_string(),
                    data: Some(json!({"output": 10})),
                    status: ResponseStatus::ok(),
                    worker_id: 0,
                },
                BatchResponseItem {
                    uid: "u2".to_string(),
                    data: None,
                    status: ResponseStatus::error("boom"),
                    worker_id: 1,
                },
            ],
            metrics: None,
        };
        let json_str = serde_json::to_string(&resp).unwrap();
        assert!(json_str.contains("BATCH_RESPONSE"));

        let decoded: BatchInferenceResponse = serde_json::from_str(&json_str).unwrap();
        assert_eq!(decoded.items.len(), 2);
        assert_eq!(decoded.items[0].status.code, "Ok");
        assert_eq!(decoded.items[1].status.code, "Error");
    }

    #[test]
    fn test_infer_payload_serde() {
        let req = InferenceRequest {
            uid: "req-1".to_string(),
            payload: RequestPayload::Infer {
                data: json!({"input": 3}),
            },
        };
        let json_str = serde_json::to_string(&req).unwrap();
        let decoded: InferenceRequest = serde_json::from_str(&json_str).unwrap();
        match decoded.payload {
            RequestPayload::Infer { data } => {
                assert_eq!(data, json!({"input": 3}));
            }
            _ => panic!("expected Infer"),
        }
    }

    #[test]
    fn test_worker_startup_with_metric_specs() {
        let json = r#"{
            "status": "ready",
            "worker_id": 0,
            "metric_specs": [
                {"name": "cache_hit_rate", "metric_type": "gauge"},
                {"name": "errors", "metric_type": "counter"}
            ]
        }"#;
        let startup: WorkerStartup = serde_json::from_str(json).unwrap();
        assert_eq!(startup.status, "ready");
        assert_eq!(startup.worker_id, 0);
        let specs = startup.metric_specs.unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "cache_hit_rate");
        assert_eq!(specs[0].metric_type, "gauge");
        assert_eq!(specs[1].name, "errors");
        assert_eq!(specs[1].metric_type, "counter");
    }

    #[test]
    fn test_worker_startup_without_metric_specs() {
        let json = r#"{"status": "ready", "worker_id": 1}"#;
        let startup: WorkerStartup = serde_json::from_str(json).unwrap();
        assert_eq!(startup.status, "ready");
        assert_eq!(startup.worker_id, 1);
        assert!(startup.metric_specs.is_none());
    }

    // ===== convert_path_params (C10) =====

    #[test]
    fn test_convert_path_params() {
        assert_eq!(convert_path_params("/pets"), "/pets");
        assert_eq!(convert_path_params("/pets/{id}"), "/pets/:id");
        assert_eq!(
            convert_path_params("/pets/{id}/owner/{oid}"),
            "/pets/:id/owner/:oid"
        );
    }

    // ===== system-route guard + custom_routes handshake (phase 2) =====

    #[test]
    fn test_is_reserved_route_reserved_leaves() {
        for leaf in ["infer", "events", "stream", "versions", "ready", "health", "reload", "compare"] {
            assert!(is_reserved_route(&format!("/{leaf}")), "{leaf} should be reserved");
            assert!(is_reserved_route(leaf), "{leaf} (no slash) should be reserved");
        }
        // server-level health probes live at root, not under the model
        // namespace, so they are NOT reserved here (B3)
        for leaf in ["livez", "readyz", "startupz"] {
            assert!(!is_reserved_route(&format!("/{leaf}")), "{leaf} should not be reserved");
        }
        // a reserved first segment with deeper tail is still reserved
        assert!(is_reserved_route("/versions/v2/status"));
        assert!(is_reserved_route("/infer/sub"));
    }

    #[test]
    fn test_is_reserved_route_custom_routes_pass() {
        assert!(!is_reserved_route("/status"));
        assert!(!is_reserved_route("/pets/{id}"));
        assert!(!is_reserved_route("/pets/123"));
        assert!(!is_reserved_route("/custom-infer"));
        assert!(!is_reserved_route("")); // empty → not reserved (no first segment)
    }

    #[test]
    fn test_worker_startup_custom_routes_roundtrip() {
        let json = r#"{
            "status": "ready",
            "worker_id": 0,
            "custom_routes": [
                {"route": "/status", "methods": ["GET"]},
                {"route": "/pets/{id}", "methods": ["GET", "POST"]}
            ]
        }"#;
        let startup: WorkerStartup = serde_json::from_str(json).unwrap();
        assert_eq!(startup.custom_routes.len(), 2);
        assert_eq!(startup.custom_routes[0].route, "/status");
        assert_eq!(startup.custom_routes[0].methods, vec!["GET".to_string()]);
        assert_eq!(
            startup.custom_routes[1].methods,
            vec!["GET".to_string(), "POST".to_string()]
        );
    }

    #[test]
    fn test_worker_startup_custom_routes_default_empty() {
        let json = r#"{"status": "ready", "worker_id": 0}"#;
        let startup: WorkerStartup = serde_json::from_str(json).unwrap();
        assert!(startup.custom_routes.is_empty());
    }

    #[test]
    fn test_route_decl_ignores_unknown_fields() {
        // Slimmed RouteDecl has no rate_limit/cors; serde ignores unknown
        // fields (no deny_unknown_fields), so a stale emitter that still adds
        // them will not break the handshake.
        let json = r#"{"route":"/x","methods":["GET"],"rate_limit":{"requests_per_minute":10}}"#;
        let decl: RouteDecl = serde_json::from_str(json).unwrap();
        assert_eq!(decl.route, "/x");
        assert_eq!(decl.methods, vec!["GET".to_string()]);
    }

    // ===== B3: SYSTEM_ROUTE_LEAVES must not over-guard root-level probes =====

    /// Regression for B3: `livez`, `readyz`, `startupz` are root-level HTTP
    /// probes (`/livez`, ...). A `@route.get("/livez")` declaration creates
    /// a route at `/v2/models/:m/livez` — a different path that does not
    /// shadow the root probe, so it must be allowed.
    ///
    /// `infer` / `events` / `stream` ARE exact routes under
    /// `/v2/models/:m/`, so they stay reserved.
    #[test]
    fn root_probes_are_not_reserved_in_model_namespace() {
        for probe in &["livez", "readyz", "startupz"] {
            assert!(
                !SYSTEM_ROUTE_LEAVES.contains(probe),
                "{probe} lives at root /{probe}, not under /v2/models/:m/{probe}"
            );
            assert!(
                !is_reserved_route(probe),
                "@route.get(\"/{probe}\") must be allowed at /v2/models/:m/{probe}"
            );
        }

        // SANITY: "infer" / "events" / "stream" ARE model-namespace leaves
        // and MUST stay reserved.
        for leaf in &["infer", "events", "stream"] {
            assert!(
                SYSTEM_ROUTE_LEAVES.contains(leaf),
                "{leaf} must be reserved (it is a model-namespace leaf)"
            );
        }
    }
}
