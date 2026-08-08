//! 统一请求上下文 `RequestContext`（蓝图 §4.0.1，分层模型 T1 层）。
//!
//! 由前置 middleware（HTTP `context_middleware`）/ interceptor（gRPC，见
//! `grpc/interceptor.rs`）**一次填充** request extensions，各消费方
//! （observability 回显、rate-limit、access_log、worker `RequestMeta`、
//! callback）从 context 读取，消除 request_id/client_ip 的重复提取（D19）。
//!
//! **T1 不可变**：`RequestContext` 一经填充即只读；post-decode 阶段补充的
//! model/version 属 T2，是独立 extensions 类型，不回写 T1。
//!
//! **OTel 单一提取（评审 1.11，D21 不变式）**：observability 最外提取 OTel
//! parent context 并 stash 入 extensions（`OtelParentContext`），
//! `context_middleware` 读取该 stash 填充 `trace_cx`；任何层禁止二次
//! propagator extract。P-TRACE 未落地前无写入方，`trace_cx` 为空 `Context`。

use crate::callback::Protocol;
use crate::client_ip::{extract_client_ip, merge_xff, TrustedNetworks};
use crate::http::RequestId;
use axum::extract::{ConnectInfo, FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::Context;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

/// T1 请求级上下文：一次填充、处处消费。
///
/// 注：`client_ip` 本期为原始字符串（与既有提取语义逐字节一致：XFF 整串
/// 透传、无来源时为空串）。蓝图 §4.0.1 目标类型是 `IpAddr`，但那依赖
/// P-XFF 的受信代理清洗（取首个不受信 hop）——P-XFF 落地时再把字段收紧为
/// `IpAddr`，本期改动必须行为不变。
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// 请求 ID（HTTP header / gRPC metadata 提取一次，或 UUID v4 兜底）。
    pub request_id: String,
    /// 客户端 IP（P-XFF 清洗后应为 `IpAddr`，见上注）。
    pub client_ip: String,
    /// OTel parent context（P-TRACE：读 observability 的 stash；未启用 OTel
    /// 时为空 `Context`）。
    pub trace_cx: Context,
    /// 入站协议（复用 callback.rs 的 `Protocol`）。
    pub protocol: Protocol,
    /// mTLS 客户端身份（P5-1：TLS acceptor 经 extension 注入；未启用 TLS 或
    /// 单向 TLS 时为 None）。本期仅供访问日志/审计，access_control 不消费。
    pub principal: Option<String>,
    /// 语义协议 T1 预筛值(D11 P2.1,批次 2):middleware 填充,extractor 期
    /// 拒绝与 handler 错误按此渲染错误体。区别于 `protocol`(http/grpc/sse
    /// **传输**协议)——本字段是**语义**协议。gRPC interceptor 置 None。
    pub api_protocol: Option<crate::protocol::ApiProtocol>,
}

/// observability 提取的 OTel parent context 的 stash 槽位（D21 单一提取
/// 不变式：observability 写、context_middleware 读，禁止二次 extract）。
/// P-TRACE 落地前无写入方，`context_middleware` 读到缺省即空 `Context`。
#[derive(Clone, Debug)]
pub(crate) struct OtelParentContext(pub Context);

/// HTTP client_ip 清洗（P-XFF）：把 header + TCP peer 经
/// [`crate::client_ip::extract_client_ip`] 的 fail-safe 受信代理算法清洗成
/// 单个 IP 字符串。`trusted` 由 `context_middleware` 的状态注入（生产栈）；
/// 无锚点（UDS 无 `ConnectInfo`）时保留既有 header 优先语义。无任何来源 → 空串。
///
/// 重复 `X-Forwarded-For` 头按 RFC 出现序合并（`merge_xff`），再传入清洗。
pub(crate) fn http_client_ip(
    headers: &HeaderMap,
    peer: Option<SocketAddr>,
    trusted: &[ipnet::IpNet],
) -> String {
    let xff = header_all(headers, "x-forwarded-for");
    let x_real_ip = header_first(headers, "x-real-ip");
    extract_client_ip(xff.as_deref(), x_real_ip.as_deref(), peer.map(|p| p.ip()), trusted)
        .map(|ip| ip.to_string())
        .unwrap_or_default()
}

/// Collect every value of a header (RFC appearance order), comma-joined. Used
/// for `X-Forwarded-For` where multiple hops/proxies each append a header.
fn header_all(headers: &HeaderMap, name: &str) -> Option<String> {
    merge_xff(headers.get_all(name).iter().filter_map(|v| v.to_str().ok()))
}

fn header_first(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

impl RequestContext {
    /// HTTP 侧填充规则（middleware 主路径与 extractor 兜底路径共用同一规则，
    /// 保证 handler 在无 middleware 的单测里也能拿到一致的 context）：
    /// - request_id：observability stash 的 `RequestId` extension 优先，否则
    ///   UUID v4（生产栈 observability 恒在最外，stash 恒存在）；
    /// - client_ip：`http_client_ip`（P-XFF 清洗：header + peer > 空串）；
    /// - trace_cx：observability stash 的 `OtelParentContext`，缺省为空
    ///   `Context`（P-TRACE 前恒为空）。
    ///
    /// `trusted` 由 middleware 状态注入（生产）；extractor/兜底路径传 `&[]`
    /// （fail-safe：直连 peer 优先，忽略客户端 header）。
    pub(crate) fn from_http_parts(parts: &Parts, trusted: &[ipnet::IpNet]) -> Self {
        let request_id = parts
            .extensions
            .get::<RequestId>()
            .map(|r| r.0.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ci| ci.0);
        let client_ip = http_client_ip(&parts.headers, peer, trusted);
        let trace_cx = parts
            .extensions
            .get::<OtelParentContext>()
            .map(|c| c.0.clone())
            .unwrap_or_default();
        // P5-1: the TLS acceptor stamps the verified mTLS client principal
        // (absent on plaintext and one-way TLS).
        let principal = parts
            .extensions
            .get::<crate::tls::TlsClientPrincipal>()
            .and_then(|p| p.0.clone());
        // D11 P2.1:语义协议 T1 预筛(header 强信号,零成本)。
        let api_protocol = crate::protocol::detect::t1_prefilter(parts.uri.path(), &parts.headers);
        Self {
            request_id,
            client_ip,
            trace_cx,
            protocol: Protocol::Http,
            principal,
            api_protocol,
        }
    }
}

/// HTTP `context_middleware`（蓝图 §4.0.2）：紧随 observability、在各消费者
/// 前（D21），一次填充 `RequestContext` 入 request extensions。状态携带 P-XFF
/// 的受信代理 CIDR 列表（`server.trusted_proxies`）；未配 → 空 → fail-safe
/// 用直连 peer、忽略客户端 XFF/X-Real-IP。
pub async fn context_middleware(
    State(trusted): State<Arc<TrustedNetworks>>,
    request: Request,
    next: Next,
) -> Response {
    let (mut parts, body) = request.into_parts();
    let cx = RequestContext::from_http_parts(&parts, &trusted);
    parts.extensions.insert(cx);
    next.run(Request::from_parts(parts, body)).await
}

/// Handler 侧 extractor：优先读 middleware 填充的 context；无 middleware
/// （直挂 handler 的单测）时按同一规则现场合成，保证 handler 不 panic。
#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for RequestContext {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<RequestContext>()
            .cloned()
            .unwrap_or_else(|| Self::from_http_parts(parts, &[])))
    }
}

/// P8-1 (B3): request envelope hints — additive scheduling hints an upstream
/// gateway/orchestrator MAY attach. Consumption state (2026-08-02):
/// - `priority` → B1 多级优先级队列（P-FLOW 已消费）；
/// - `affinity_key` → 内容亲和路由：无 `sequence_id` 时作 rendezvous 哈希 key
///   （`sequence_id` 是其特例，两者同时在时 `sequence_id` 优先）；
/// - `direct_worker_id` → 直连钉住：提交时校验（不存在/已剔除 → 400/
///   InvalidArgument），dispatch 优先于一切挑选。
///
/// `expected_cost`（容量感知 picker 的预留字段）已移除——定义即债务；容量
/// 感知 picker 落地时以 additive header 重新引入（§2.2 观察名单）。
///
/// All fields are unauthenticated hints, not an isolation boundary — a client
/// can influence its own routing/landing but cannot cross model or tenant
/// boundaries (those stay enforced by `access_control` + worker model scope).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RequestHints {
    /// Scheduling priority (lower = higher priority). Consumed by the B1
    /// multi-level priority queue (P-FLOW).
    pub priority: Option<i32>,
    /// Generic content-affinity key. `sequence_id` is its special case; when
    /// both are present the explicit `sequence_id` wins.
    pub affinity_key: Option<String>,
    /// Direct-mode: caller names a specific `worker_id` ("gateway citizen"
    /// extension — the server does not take over the decision). Validated at
    /// submit (unknown/ejected worker → 400 / InvalidArgument).
    pub direct_worker_id: Option<u32>,
}

impl RequestHints {
    /// Parse envelope hints off inbound HTTP headers. Unknown / malformed
    /// values are silently dropped (a hint is best-effort).
    pub fn from_http(headers: &HeaderMap) -> Self {
        Self {
            priority: header_str(headers, "x-lite-priority").and_then(|s| s.parse().ok()),
            affinity_key: header_str(headers, "x-lite-affinity-key").map(|s| s.to_string()),
            direct_worker_id: header_str(headers, "x-lite-worker-id").and_then(|s| s.parse().ok()),
        }
    }

    /// Parse envelope hints off a gRPC `headers` map (lower-cased keys).
    pub fn from_grpc(headers: &std::collections::HashMap<String, String>) -> Self {
        let get = |k: &str| headers.get(k).map(|s| s.as_str());
        Self {
            priority: get("x-lite-priority").and_then(|s| s.parse().ok()),
            affinity_key: get("x-lite-affinity-key").filter(|s| !s.is_empty()).map(|s| s.to_string()),
            direct_worker_id: get("x-lite-worker-id").and_then(|s| s.parse().ok()),
        }
    }

    /// `true` when no hint was supplied (the common case) — lets callers skip
    /// the debug log and any future consumption work cheaply.
    pub fn is_empty(&self) -> bool {
        self.priority.is_none()
            && self.affinity_key.is_none()
            && self.direct_worker_id.is_none()
    }
}

fn header_str<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|v| v.to_str().ok()).filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    // ===== P8-1 (B3) RequestHints parsing =====

    #[test]
    fn request_hints_parse_from_http_headers() {
        let h = headers_with(&[
            ("x-lite-priority", "5"),
            ("x-lite-affinity-key", "sess-42"),
            ("x-lite-worker-id", "3"),
        ]);
        let hints = RequestHints::from_http(&h);
        assert_eq!(hints.priority, Some(5));
        assert_eq!(hints.affinity_key.as_deref(), Some("sess-42"));
        assert_eq!(hints.direct_worker_id, Some(3));
        assert!(!hints.is_empty());
    }

    #[test]
    fn request_hints_expected_cost_removed() {
        // expected_cost 已移除（定义即债务）——旧 header 静默忽略，不进 hints。
        let h = headers_with(&[("x-lite-expected-cost", "1.25")]);
        assert!(RequestHints::from_http(&h).is_empty());
    }

    #[test]
    fn request_hints_empty_when_absent_or_malformed() {
        let h = headers_with(&[("x-lite-priority", "not-a-number"), ("x-lite-affinity-key", "")]);
        let hints = RequestHints::from_http(&h);
        assert!(hints.is_empty(), "malformed/empty values drop to None");
    }

    #[test]
    fn request_hints_parse_from_grpc_map() {
        let mut m = std::collections::HashMap::new();
        m.insert("x-lite-priority".to_string(), "2".to_string());
        m.insert("x-lite-worker-id".to_string(), "1".to_string());
        let hints = RequestHints::from_grpc(&m);
        assert_eq!(hints.priority, Some(2));
        assert_eq!(hints.direct_worker_id, Some(1));
    }

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    // ===== http_client_ip 优先级（收敛自原 handlers::extract_client_ip +
    //       peer_ip_fallback 的组合语义） =====

    #[test]
    fn http_client_ip_prefers_x_forwarded_for() {
        // 无 peer（UDS）+ 空 trusted：保留既有 header 优先语义。
        let h = headers_with(&[("x-forwarded-for", "10.0.0.1"), ("x-real-ip", "10.0.0.2")]);
        assert_eq!(http_client_ip(&h, None, &[]), "10.0.0.1");
    }

    #[test]
    fn http_client_ip_uses_x_real_ip_when_no_xff() {
        let h = headers_with(&[("x-real-ip", "10.0.0.2")]);
        assert_eq!(http_client_ip(&h, None, &[]), "10.0.0.2");
    }

    #[test]
    fn http_client_ip_empty_xff_falls_through() {
        let h = headers_with(&[("x-forwarded-for", ""), ("x-real-ip", "10.0.0.2")]);
        assert_eq!(http_client_ip(&h, None, &[]), "10.0.0.2");
    }

    #[test]
    fn http_client_ip_falls_back_to_peer_for_direct_connections() {
        // 直连（无代理头）：TCP peer 即客户端。
        let peer: SocketAddr = "203.0.113.7:5000".parse().unwrap();
        assert_eq!(http_client_ip(&HeaderMap::new(), Some(peer), &[]), "203.0.113.7");
    }

    #[test]
    fn http_client_ip_untrusted_peer_ignores_forged_xff() {
        // P-XFF fail-safe：非受信 peer 携带 XFF → 忽略 XFF，用 peer
        // （修复前会信 XFF，可伪造绕过 key=ip 限流）。
        let h = headers_with(&[("x-forwarded-for", "10.0.0.99")]);
        let peer: SocketAddr = "203.0.113.7:5000".parse().unwrap();
        assert_eq!(http_client_ip(&h, Some(peer), &[]), "203.0.113.7");
    }

    #[test]
    fn http_client_ip_trusted_proxy_honors_forwarded_client() {
        // 受信代理（10.0.0.0/8）转发的 XFF → 清洗出真实客户端。
        let h = headers_with(&[("x-forwarded-for", "203.0.113.55")]);
        let peer: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let trusted = vec!["10.0.0.0/8".parse().unwrap()];
        assert_eq!(http_client_ip(&h, Some(peer), &trusted), "203.0.113.55");
    }

    #[test]
    fn http_client_ip_empty_when_no_source() {
        assert_eq!(http_client_ip(&HeaderMap::new(), None, &[]), "");
    }

    // ===== context_middleware：一次填充 =====

    #[tokio::test]
    async fn context_middleware_fills_request_context() {
        async fn handler(cx: RequestContext) -> String {
            format!("{}|{}|{}", cx.request_id, cx.client_ip, cx.protocol)
        }
        let app = axum::Router::new()
            .route("/t", axum::routing::get(handler))
            .layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(crate::client_ip::TrustedNetworks::new()),
                context_middleware,
            ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/t")
                    .header("x-forwarded-for", "10.0.0.9")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        let mut parts = text.split('|');
        let request_id = parts.next().unwrap();
        assert_eq!(request_id.len(), 36, "no RequestId stash → UUID v4, got {request_id}");
        assert_eq!(parts.next(), Some("10.0.0.9"));
        assert_eq!(parts.next(), Some("http"));
    }

    // ===== P5-1: mTLS principal 传播 =====

    #[tokio::test]
    async fn context_middleware_propagates_tls_principal() {
        async fn handler(cx: RequestContext) -> String {
            cx.principal.unwrap_or_default()
        }
        let app = axum::Router::new()
            .route("/t", axum::routing::get(handler))
            .layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(crate::client_ip::TrustedNetworks::new()),
                context_middleware,
            ));

        let mut request = Request::builder().uri("/t").body(Body::empty()).unwrap();
        request
            .extensions_mut()
            .insert(crate::tls::TlsClientPrincipal(Some("spiffe://ns/svc".to_string())));
        let response = app.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "spiffe://ns/svc");
    }

    #[tokio::test]
    async fn context_middleware_principal_none_without_tls_extension() {
        async fn handler(cx: RequestContext) -> String {
            format!("{}", cx.principal.is_none())
        }
        let app = axum::Router::new()
            .route("/t", axum::routing::get(handler))
            .layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(crate::client_ip::TrustedNetworks::new()),
                context_middleware,
            ));

        let request = Request::builder().uri("/t").body(Body::empty()).unwrap();
        let response = app.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "true");
    }

    #[tokio::test]
    async fn context_middleware_reads_observability_request_id_stash() {
        // 模拟 observability 在前 stash RequestId（生产栈中 observability 最外）。
        async fn stash_middleware(mut request: Request, next: Next) -> Response {
            request
                .extensions_mut()
                .insert(RequestId("stash-id-001".to_string()));
            next.run(request).await
        }
        async fn handler(cx: RequestContext) -> String {
            cx.request_id
        }
        // stash 在外（后挂），context_middleware 在内（先挂）——与生产栈同序。
        let app = axum::Router::new()
            .route("/t", axum::routing::get(handler))
            .layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(crate::client_ip::TrustedNetworks::new()),
                context_middleware,
            ))
            .layer(axum::middleware::from_fn(stash_middleware));

        let response = app
            .oneshot(Request::builder().uri("/t").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "stash-id-001");
    }

    #[tokio::test]
    async fn context_middleware_reads_otel_stash_into_trace_cx() {
        // D21 单一提取：context_middleware 读 observability 的 OTel stash。
        async fn otel_stash_middleware(mut request: Request, next: Next) -> Response {
            let cx = Context::new().with_value(42u32);
            request.extensions_mut().insert(OtelParentContext(cx));
            next.run(request).await
        }
        async fn handler(cx: RequestContext) -> String {
            cx.trace_cx
                .get::<u32>()
                .map(|v| v.to_string())
                .unwrap_or_default()
        }
        let app = axum::Router::new()
            .route("/t", axum::routing::get(handler))
            .layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(crate::client_ip::TrustedNetworks::new()),
                context_middleware,
            ))
            .layer(axum::middleware::from_fn(otel_stash_middleware));

        let response = app
            .oneshot(Request::builder().uri("/t").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "42");
    }

    #[tokio::test]
    async fn context_trace_cx_empty_without_otel_stash() {
        // P-TRACE 前无写入方：trace_cx 为空 Context。
        async fn handler(cx: RequestContext) -> String {
            format!("{}", cx.trace_cx.get::<u32>().is_none())
        }
        let app = axum::Router::new()
            .route("/t", axum::routing::get(handler))
            .layer(axum::middleware::from_fn_with_state(
                std::sync::Arc::new(crate::client_ip::TrustedNetworks::new()),
                context_middleware,
            ));

        let response = app
            .oneshot(Request::builder().uri("/t").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "true");
    }

    // ===== extractor 兜底：无 middleware 时 handler 仍能拿到一致 context =====

    #[tokio::test]
    async fn extractor_synthesizes_context_without_middleware() {
        async fn handler(cx: RequestContext) -> String {
            assert!(cx.principal.is_none());
            format!("{}|{}", cx.request_id.len(), cx.client_ip)
        }
        let app = axum::Router::new().route("/t", axum::routing::get(handler));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/t")
                    .header("x-forwarded-for", "10.0.0.5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "36|10.0.0.5");
    }

    // ===== 护栏：HTTP handler 不再各自 inline 提取 request_id/client_ip =====

    #[test]
    fn http_handlers_do_not_extract_inline() {
        for (name, src) in [
            ("inference.rs", include_str!("http/handlers/inference.rs")),
            ("stream.rs", include_str!("http/handlers/stream.rs")),
            ("custom_routes.rs", include_str!("http/handlers/custom_routes.rs")),
            ("handlers/mod.rs", include_str!("http/handlers/mod.rs")),
        ] {
            let boundary = src.find("#[cfg(test)]").unwrap_or(src.len());
            let prod = &src[..boundary];
            assert!(
                !prod.contains("extract_client_ip"),
                "P-MW: {name} must read RequestContext, not extract_client_ip inline"
            );
            assert!(
                !prod.contains("extract_request_id"),
                "P-MW: {name} must read RequestContext, not extract_request_id inline"
            );
        }
    }
}
