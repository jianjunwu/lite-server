use crate::error::AppError;
use serde_json::{json, Value};

use super::*;

// ===== §4.3/D17: bidi upstream aggregation =====

/// Bidi upstream aggregation for ensemble DAGs (D17): all-Binary frames →
/// byte concat (content-type from the first frame); all-Json frames → JSON
/// array (a single frame = the value itself, historical single-frame
/// semantics); mixed kinds → 400 (no clean boundary/Content-Type semantics).
/// The cumulative byte cap is `max_request_body_bytes` (D3) — exceeded →
/// PayloadTooLarge (413/ResourceExhausted).
pub struct BidiAggregator {
    max_bytes: usize,
    total_bytes: usize,
    state: BidiAggState,
}

enum BidiAggState {
    Empty,
    Binary {
        parts: Vec<bytes::Bytes>,
        content_type: String,
    },
    Json {
        items: Vec<serde_json::Value>,
    },
}

impl BidiAggregator {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            state: BidiAggState::Empty,
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Push one data frame. `is_json` is the frame's declared kind (WS frame
    /// type / content-type header). The first frame fixes the kind; any
    /// later frame of the other kind → 400. Cap enforcement (D3) happens
    /// here so the aggregation can never exceed `max_request_body_bytes`.
    pub fn push(
        &mut self,
        data: bytes::Bytes,
        is_json: bool,
        content_type: Option<&str>,
    ) -> Result<(), AppError> {
        self.total_bytes += data.len();
        if self.total_bytes > self.max_bytes {
            return Err(AppError::PayloadTooLarge {
                max_size: self.max_bytes,
                actual_size: Some(self.total_bytes as u64),
            });
        }
        match &mut self.state {
            BidiAggState::Empty => {
                if is_json {
                    let v: Value = serde_json::from_slice(&data).map_err(|e| {
                        AppError::InvalidRequestBody(format!(
                            "bidi JSON frame is not valid JSON: {e}"
                        ))
                    })?;
                    self.state = BidiAggState::Json { items: vec![v] };
                } else {
                    self.state = BidiAggState::Binary {
                        parts: vec![data],
                        content_type: content_type
                            .unwrap_or("application/octet-stream")
                            .to_string(),
                    };
                }
            }
            BidiAggState::Json { items } => {
                if !is_json {
                    return Err(AppError::InvalidRequestBody(
                        "bidi stream mixes JSON and binary frames; \
                         aggregation requires a single kind (D17)"
                            .to_string(),
                    ));
                }
                let v: Value = serde_json::from_slice(&data).map_err(|e| {
                    AppError::InvalidRequestBody(format!("bidi JSON frame is not valid JSON: {e}"))
                })?;
                items.push(v);
            }
            BidiAggState::Binary { parts, .. } => {
                if is_json {
                    return Err(AppError::InvalidRequestBody(
                        "bidi stream mixes JSON and binary frames; \
                         aggregation requires a single kind (D17)"
                            .to_string(),
                    ));
                }
                parts.push(data);
            }
        }
        Ok(())
    }

    /// Trigger time (D33): produce the aggregated root input. Single Json
    /// frame → the value itself (not wrapped); multiple → `[f0, f1, ...]`;
    /// Binary → concatenated bytes with the first frame's content-type.
    pub fn finish(self) -> Result<EnsembleValue, AppError> {
        match self.state {
            BidiAggState::Empty => Ok(EnsembleValue::Json(json!({}))),
            BidiAggState::Json { items } => {
                if items.len() == 1 {
                    Ok(EnsembleValue::Json(items.into_iter().next().unwrap()))
                } else {
                    Ok(EnsembleValue::Json(Value::Array(items)))
                }
            }
            BidiAggState::Binary { parts, content_type } => {
                let total: usize = parts.iter().map(|p| p.len()).sum();
                let mut buf = Vec::with_capacity(total);
                for p in parts {
                    buf.extend_from_slice(&p);
                }
                Ok(EnsembleValue::Binary(bytes::Bytes::from(buf), content_type, None, None))
            }
        }
    }
}

