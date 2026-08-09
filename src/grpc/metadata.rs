//! Response metadata: request_id/processing-time echo headers (P2-2),
//! per-request metric recording (P2-1), and custom-header injection with
//! hop-by-hop filtering.

use super::error::grpc_code_to_status_family;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::{Response, Status};

/// P2-3：从入站 metadata 取 request_id（span 字段用；与 interceptor 同源）。
/// 提取入站 `x-client-request-id`（空→空串）。**套用与 interceptor 相同的
/// `is_valid_request_id` 校验**（P-MW 审计修复：非法值——超长/非 ASCII——
/// 返回空串，由下游 UUID 兜底；否则 span 上的 request_id 会与校验后回显/
/// RequestMeta 的值分叉）。优先消费 RequestContext.request_id，本函数仅为
/// 无 interceptor 场景的回退。
pub(super) fn metadata_request_id(metadata: &MetadataMap) -> String {
    metadata
        .get("x-client-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| crate::validation::is_valid_request_id(s))
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

/// P2-2 parity（对账修复）：admission 等 guard 的早期拒绝发生在 handler 出口
/// 的 echo 包装之前——把回显前移到 guard 处，拒绝响应同样携带
/// `x-request-id`/`x-processing-time-ms`（对齐 HTTP observability 最外层恒回显）。
/// request_id 优先取 interceptor 校验填充的 RequestContext（含 UUID 兜底）。
pub(super) fn echo_early_rejection<T>(mut status: Status, request: &tonic::Request<T>) -> Status {
    let rid = request
        .extensions()
        .get::<crate::request_context::RequestContext>()
        .map(|rc| rc.request_id.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| metadata_request_id(request.metadata()));
    inject_echo_headers(status.metadata_mut(), &rid, std::time::Duration::from_millis(0));
    status
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

#[cfg(test)]
mod audit_tests {
    //! /audit 举证（P-MW 面，蓝图 §4.0.5）：span 的 request_id 来源
    //! `metadata_request_id` 是对入站 metadata 的重复提取，且不套用
    //! `is_valid_request_id` 校验——与 interceptor→finalize 链路（回显
    //! x-request-id / RequestMeta.request_id 的来源）读到的值不一致。
    use super::metadata_request_id;
    use crate::grpc::interceptor::finalize_context;
    use std::collections::HashMap;
    use tonic::metadata::{MetadataKey, MetadataMap};

    #[test]
    fn test_audit_data_span_request_id_diverges_from_validated_context() {
        // 513 字符的 x-client-request-id：is_valid_request_id 拒收（>512）。
        // interceptor 校验 → 空 → finalize 生成 UUID（回显/RequestMeta 读到 UUID）；
        // metadata_request_id 只滤空串 → span 读到 513 字符的非法原值。
        let oversized = "x".repeat(513);
        let mut md = MetadataMap::new();
        md.insert(
            MetadataKey::from_bytes(b"x-client-request-id").unwrap(),
            oversized.parse().unwrap(),
        );
        let finalized = finalize_context(None, &md, &HashMap::new(), None, &[]);
        // 前提确认：finalize 链路确实拒绝了非法值（生成 UUID）。
        assert!(uuid::Uuid::parse_str(&finalized.request_id).is_ok());
        assert_ne!(finalized.request_id, oversized);

        let span_rid = metadata_request_id(&md);
        assert!(
            span_rid.is_empty() || crate::validation::is_valid_request_id(&span_rid),
            "P-MW §4.0.5（observability 读到同一 request_id / 无重复提取）：span 的 \
             request_id 来源必须与 interceptor 同源同校验；当前 span 读到被拒收的 \
             513 字符非法值，与回显的 UUID 不一致（HTTP 侧 span 读 context，无此分叉）"
        );
    }
}
