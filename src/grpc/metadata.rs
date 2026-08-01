//! Response metadata: request_id/processing-time echo headers (P2-2),
//! per-request metric recording (P2-1), and custom-header injection with
//! hop-by-hop filtering.

use super::error::grpc_code_to_status_family;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::{Response, Status};

/// P2-3：从入站 metadata 取 request_id（span 字段用；与 interceptor 同源）。
pub(super) fn metadata_request_id(metadata: &MetadataMap) -> String {
    metadata
        .get("x-client-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_default()
}

/// P2-2：将 `x-request-id` + `x-processing-time-ms` 注入响应/错误 metadata
///（对齐 HTTP observability_middleware 错误路径回显）。interceptor 是 pre-call
/// 无法改响应（评审 1.12），故回显在 handler 出口统一完成。
pub(super) fn echo_grpc_response_headers<T>(
    result: Result<Response<T>, Status>,
    request_id: &str,
    start: Instant,
) -> Result<Response<T>, Status> {
    let elapsed = start.elapsed();
    match result {
        Ok(mut response) => {
            inject_echo_headers(response.metadata_mut(), request_id, elapsed);
            Ok(response)
        }
        Err(mut status) => {
            inject_echo_headers(status.metadata_mut(), request_id, elapsed);
            Err(status)
        }
    }
}

/// 注入回显 header（request_id 缺省/非法则跳过该项；processing-time 恒定注入）。
fn inject_echo_headers(metadata: &mut MetadataMap, request_id: &str, elapsed: Duration) {
    if !request_id.is_empty() {
        if let Ok(v) = MetadataValue::try_from(request_id) {
            metadata.insert("x-request-id", v);
        }
    }
    if let Ok(v) = MetadataValue::try_from(elapsed.as_millis().to_string().as_str()) {
        metadata.insert("x-processing-time-ms", v);
    }
}

/// P2-1：unary 请求指标统一记录点（成功 "2xx"；错误按 `grpc_code_to_status_family`）。
pub(super) fn record_grpc_request_end<T>(
    model: &str,
    version: &str,
    start: Instant,
    result: &Result<T, Status>,
) {
    let family = match result {
        Ok(_) => "2xx",
        Err(s) => grpc_code_to_status_family(s.code()),
    };
    crate::metrics::prometheus::record_request_end(model, version, family, start.elapsed().as_secs_f64());
}

/// Headers that must not be set by user code (RFC 7230 §6.1 hop-by-hop headers
/// and other transport headers managed by the server).
const BLOCKED_RESPONSE_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "transfer-encoding",
    "content-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
];

/// Inject custom response headers into tonic gRPC metadata,
/// blocking hop-by-hop and transport headers.
pub(super) fn inject_grpc_metadata(
    metadata: &mut MetadataMap,
    headers: &HashMap<String, String>,
) {
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if BLOCKED_RESPONSE_HEADERS.contains(&lower.as_str()) {
            continue;
        }
        if let Ok(mk) = MetadataKey::from_bytes(k.as_bytes()) {
            if let Ok(mv) = MetadataValue::try_from(v.as_str()) {
                metadata.insert(mk, mv);
            }
        }
    }
}
