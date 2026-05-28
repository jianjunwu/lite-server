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
pub struct WorkerStartup {
    pub status: String,
    pub worker_id: u32,
    pub message: Option<String>,
}

// ===== Endpoint Protocol =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointRequest {
    pub request_id: String,
    pub route: String,
    pub method: String,
    pub headers: HashMap<String, String>,
    pub query: HashMap<String, String>,
    pub body: Option<serde_json::Value>,
    pub server_state: ServerSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSnapshot {
    pub loaded_models: Vec<serde_json::Value>,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointResponse {
    pub request_id: String,
    pub status_code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    pub body: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointStartup {
    pub status: String,
    pub routes: Vec<EndpointRoute>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointRoute {
    pub route: String,
    pub methods: Vec<String>,
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
}
