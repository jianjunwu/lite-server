//! 协议层 seam(D11 / 蓝图 §4.0.11)。
//!
//! 唯一全协议共性 = HTTP 错误体:语义协议(`ApiProtocol`)决定错误 wire 形状,
//! render 分派在此处。本层只依赖 std + axum + serde_json,单向依赖:核心 →
//! protocol(protocol/ 之外禁止拼 wire JSON;SSE/WS 流中途错误帧是传输帧格式,
//! 豁免)。
//!
//! P2.0(批次 0,protocol-compat-plan.md):纯迁移——枚举 + render 双 arm +
//! `CanonicalError` 边界,尚无 Kserve 产出(detect 在 P2.1)。门禁 = 字节快照 +
//! 既有全套测试不改一行全绿。

pub mod detect;
pub mod kserve;
pub mod openai;
#[cfg(test)]
mod tests;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;

/// 语义协议:决定 HTTP 错误体形状。注意与 `RequestContext.protocol`
/// (http/grpc/sse 传输协议)区分——本层是**语义协议**。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiProtocol {
    /// 现状 OpenAI 风格错误体(byte-identical 基准)。
    Legacy,
    /// KServe V2 dataplane 扁平错误体 `{"error": "<message>"}`。
    /// 由 detect::t1_prefilter / t2_kserve_envelope 产出(P2.1,批次 2)。
    Kserve,
    /// openai-compact(阶段 6,批次 5):/v1 路径的语义协议。错误体与
    /// Legacy 同形状(复用 openai.rs renderer,无新 renderer)。
    OpenaiCompact,
}

/// 与协议无关的错误值表——wire 输出的唯一来源。值由
/// [`crate::error::AppError::into_canonical`] 按现状映射原样迁移
/// (P2.0 零行为变化)。
pub struct CanonicalError {
    pub status: StatusCode,
    /// OpenAI 风格 type 字段(如 `not_found_error`);Kserve 形状不使用。
    pub error_type: String,
    /// 机器码(如 `model_not_found`)。Option:ModelError 未提供 code 时保留
    /// None,wire 输出 `"code": null`(现状字节,快照门禁)。
    pub code: Option<String>,
    /// 客户端可见消息(已 sanitize;ModelError 例外 = 模型作者显式消息)。
    pub message: String,
    pub param: Option<String>,
    /// 协议无关的附加字段(现状:PayloadTooLarge 的 max_size/actual_size),
    /// 由各协议 renderer 决定是否合入。
    pub extra: Option<serde_json::Value>,
    /// 透传响应 header(模型作者 headers / retry-after)。
    pub headers: Option<HashMap<String, String>>,
    /// ModelError:日志级别 info(模型主动拒绝,非服务器故障),消息不 sanitize。
    pub from_model: bool,
    /// 日志用内部 detail(含内部信息,绝不进 wire)。
    pub log_detail: String,
}

/// 协议路由挂载 seam(D11 P2.2):create_routes 在 fallback 前一行调用。
/// 阶段 2 为空表——no-op(G17 回归锁定)。openai-compact(批次 5)= 1 handler
/// 模块 + 挂载表一行;挂载形状(状态类型/fold)届时按 handler 需要定
/// (D11:将来要 gate 则条目改 `fn(&FeaturesConfig, Router)`)。
pub(crate) fn mount<S: Clone + Send + Sync + 'static>(router: axum::Router<S>) -> axum::Router<S> {
    router
}

/// 按协议渲染错误响应:C7 日志 + 状态 + 协议体 + header 透传。
pub fn render(err: CanonicalError, protocol: ApiProtocol) -> Response {
    // C7:client errors(4xx,含 429)log at info——饱和的限流器不刷 error
    // 日志;仅服务器故障(5xx)算运营错误;model error 是模型主动拒绝,
    // log at info。从 error.rs 的 wire 分支移入(P2.0)。
    if err.from_model {
        tracing::info!(
            status = %err.status.as_u16(),
            error_type = %err.error_type,
            code = ?err.code,
            detail = %err.log_detail,
            "model error"
        );
    } else {
        // 非 ModelError 恒有 code(Option 仅模型路径可能 None),unwrap_or 仅为
        // 类型占位。
        let code = err.code.as_deref().unwrap_or("");
        if err.status.is_server_error() {
            tracing::error!(
                error_type = %err.error_type,
                code = %code,
                detail = %err.log_detail,
                "request error"
            );
        } else {
            tracing::info!(
                error_type = %err.error_type,
                code = %code,
                detail = %err.log_detail,
                "request error"
            );
        }
    }

    let body = match protocol {
        ApiProtocol::Legacy => openai::render_body(&err),
        ApiProtocol::Kserve => kserve::render_body(&err),
        ApiProtocol::OpenaiCompact => openai::render_body(&err),
    };
    let mut response = (err.status, body).into_response();

    // 模型作者 header(Retry-After 等)与队列/限流 retry-after 统一经此注入
    // (skip hop-by-hop / 库管理 header,与现状行为一致)。
    if let Some(hdrs) = err.headers {
        inject_response_headers_into(response.headers_mut(), &hdrs);
    }
    response
}

/// 必须由 HTTP 库/传输层管理的 header,用户代码不得覆盖
/// (RFC 7230 §6.1 hop-by-hop)。自 http/handlers 迁入(P2.0:render 需要)。
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

/// Inject response headers into an existing `HeaderMap` in place — the
/// error-response path builds via `into_response()` (no Builder), so it needs
/// this mutating variant. Skips hop-by-hop / library-managed headers and
/// silently drops headers with invalid names/values.
pub(crate) fn inject_response_headers_into(
    map: &mut axum::http::HeaderMap,
    headers: &std::collections::HashMap<String, String>,
) {
    for (k, v) in headers {
        if BLOCKED_RESPONSE_HEADERS.contains(&k.to_ascii_lowercase().as_str()) {
            continue;
        }
        if let (Ok(name), Ok(val)) = (
            k.parse::<axum::http::HeaderName>(),
            axum::http::HeaderValue::from_str(v),
        ) {
            map.insert(name, val);
        }
    }
}

/// Builder variant for the response path (worker output headers) — re-exported
/// by http/handlers so existing call sites (inference.rs / custom_routes.rs)
/// are unchanged.
pub(crate) fn inject_response_headers(
    builder: axum::http::response::Builder,
    headers: &HashMap<String, String>,
) -> axum::http::response::Builder {
    let mut builder = builder;
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if !BLOCKED_RESPONSE_HEADERS.contains(&lower.as_str()) {
            builder = builder.header(k.as_str(), v.as_str());
        }
    }
    builder
}
