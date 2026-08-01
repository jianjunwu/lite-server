//! gRPC interceptor：pre-decode 一次填充 `RequestContext`（蓝图 §4.0.3 / D20）。
//!
//! **职责边界（评审 1.12）**：tonic `.interceptor()` 是 pre-call 语义——只做
//! 提取+填充，无法修改响应、无法拦截 Status；回显（P2-2）/错误日志（P1-1）/
//! 错误路径注入仍留 handler。
//!
//! **pre/post-decode 切分（D20）**：interceptor 在 decode 前执行，只见
//! transport metadata（HTTP/2 headers）与 peer 地址；protobuf body 里的
//! `headers` map（REST→gRPC bridge）此时不可见，其 fallback 由 handler 在
//! post-decode 经 `finalize_context` 一次完成（T1 extension 实例不被回写，
//! handler 得到的是 finalize 后的副本）。

use crate::access_control::{AccessControl, EndpointClass};
use crate::callback::Protocol;
use crate::client_ip::{extract_client_ip, merge_xff, TrustedNetworks};
use crate::request_context::RequestContext;
use opentelemetry::Context;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};
use uuid::Uuid;

impl RequestContext {
    /// gRPC pre-decode 填充：仅从 transport metadata 提取。
    /// `request_id` / `client_ip` 为空串表示 metadata 未提供，留待
    /// `finalize_context` 应用 proto `headers` map fallback / peer 兜底 /
    /// UUID 生成——interceptor 看不到 proto body，不能提前定案。
    ///
    /// client_ip 经 P-XFF fail-safe 清洗（`extract_client_ip`）：peer 锚定，
    /// 受信代理才信任 metadata XFF/X-Real-IP。重复 XFF 头按出现序合并。
    pub(crate) fn from_grpc_metadata(
        metadata: &MetadataMap,
        _remote_addr: Option<SocketAddr>,
        trusted: &[ipnet::IpNet],
    ) -> Self {
        let request_id = metadata
            .get("x-client-request-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| crate::validation::is_valid_request_id(s))
            .map(String::from)
            .unwrap_or_default();
        let xff = metadata_xff(metadata, "x-forwarded-for");
        let x_real_ip = metadata_first(metadata, "x-real-ip");
        let client_ip = extract_client_ip(
            xff.as_deref(),
            x_real_ip.as_deref(),
            _remote_addr.map(|a| a.ip()),
            trusted,
        )
        .map(|ip| ip.to_string())
        .unwrap_or_default();
        Self {
            request_id,
            client_ip,
            trace_cx: Context::new(), // P-TRACE 前为空 Context 占位
            protocol: Protocol::Grpc,
            principal: None, // P5-1: context_interceptor 随后按 TlsConnectInfo 填充
        }
    }
}

/// Merge every `x-forwarded-for` metadata value (RFC appearance order).
fn metadata_xff(metadata: &MetadataMap, key: &str) -> Option<String> {
    merge_xff(metadata.get_all(key).iter().filter_map(|v| v.to_str().ok()))
}

fn metadata_first(metadata: &MetadataMap, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// P7-1 production interceptor for one tonic service: fills `RequestContext`
/// (pre-call, same as `context_interceptor`) AND enforces endpoint-class access
/// control for the service's class. Mounted per-service with the matching class
/// (LiteServer=Inference, Admin=Admin, health=Health) — the class is known at
/// mount time, so no per-RPC path parsing is needed. Denied → `Unauthenticated`.
/// loopback comes from the transport peer (UDS has none → treated as loopback).
pub fn service_interceptor(
    access_control: Arc<AccessControl>,
    class: EndpointClass,
    trusted: Arc<TrustedNetworks>,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Send + Sync + Clone + 'static {
    move |mut request| {
        let remote = request.remote_addr();
        let mut cx = RequestContext::from_grpc_metadata(request.metadata(), remote, &trusted);
        cx.principal = tls_principal(request.extensions());
        let is_loopback = remote.map(|a| a.ip().is_loopback()).unwrap_or(true);
        if !access_control.check(class, Protocol::Grpc, request.metadata(), is_loopback) {
            return Err(Status::unauthenticated("access denied"));
        }
        request.extensions_mut().insert(cx);
        Ok(request)
    }
}

/// tonic `.interceptor()` 入口（挂载矩阵 §4.0.3：LiteServer 与 health 服务
/// 都挂）：pre-call 提取 + 填充 `RequestContext` 入 request extensions。
/// 对 unary / server-streaming / bidi 语义透明（每次 RPC 调用前执行一次，
/// 不触碰消息流）。P-XFF：trusted 取空（fail-safe）——生产栈用
/// `service_interceptor` 注入真实 trusted。
pub fn context_interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
    let mut cx =
        RequestContext::from_grpc_metadata(request.metadata(), request.remote_addr(), &[]);
    // P5-1: TLS 连接经 tonic 的 TlsConnectInfo 携带 mTLS 客户端证书 → T1
    // principal（明文 TCP/UDS 恒为 None）。
    cx.principal = tls_principal(request.extensions());
    request.extensions_mut().insert(cx);
    Ok(request)
}

/// 从 transport extensions 提取 mTLS 客户端 principal：自定义 TLS incoming
/// （`tls::tls_incoming`）产出 `TlsStream<TcpStream>`，tonic 的 blanket
/// `Connected` impl 据此把 `TlsConnectInfo<TcpConnectInfo>`（含 peer certs）
/// 放进 extensions。
fn tls_principal(extensions: &tonic::Extensions) -> Option<String> {
    let info = extensions.get::<tonic::transport::server::TlsConnectInfo<
        tonic::transport::server::TcpConnectInfo,
    >>()?;
    let certs = info.peer_certs()?;
    certs.first().map(crate::tls::principal_from_cert)
}

/// post-decode finalize：proto `headers` map 的 fallback 只有 decode 后可见。
/// - `cx` 为 None（interceptor 未运行，如直调 handler 的测试）时先按
///   interceptor 同一规则从 metadata 合成；
/// - request_id：metadata 已定案则保留，否则 proto headers 有效值，再否则
///   UUID v4；
/// - client_ip：metadata 已定案则保留，否则 proto headers 经 P-XFF 清洗
///   （`extract_client_ip`），再否则 transport peer 地址，最后空串。
pub(crate) fn finalize_context(
    cx: Option<RequestContext>,
    metadata: &MetadataMap,
    proto_headers: &HashMap<String, String>,
    remote_addr: Option<SocketAddr>,
    trusted: &[ipnet::IpNet],
) -> RequestContext {
    let mut cx =
        cx.unwrap_or_else(|| RequestContext::from_grpc_metadata(metadata, remote_addr, trusted));
    if cx.request_id.is_empty() {
        cx.request_id = proto_headers
            .get("x-client-request-id")
            .filter(|s| crate::validation::is_valid_request_id(s))
            .cloned()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
    }
    if cx.client_ip.is_empty() {
        let xff = proto_headers.get("x-forwarded-for").map(String::as_str);
        let x_real_ip = proto_headers.get("x-real-ip").map(String::as_str);
        cx.client_ip = extract_client_ip(xff, x_real_ip, remote_addr.map(|a| a.ip()), trusted)
            .map(|ip| ip.to_string())
            .unwrap_or_default();
    }
    cx
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataKey;

    fn md_with(pairs: &[(&str, &str)]) -> MetadataMap {
        let mut md = MetadataMap::new();
        for (k, v) in pairs {
            md.insert(MetadataKey::from_bytes(k.as_bytes()).unwrap(), v.parse().unwrap());
        }
        md
    }

    fn headers_with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn addr(ip: &str) -> Option<SocketAddr> {
        Some(format!("{ip}:5000").parse().unwrap())
    }

    // ===== interceptor：一次填充入 extensions =====

    #[test]
    fn interceptor_fills_request_context_extensions() {
        let mut request = Request::new(());
        *request.metadata_mut() = md_with(&[
            ("x-client-request-id", "from-metadata"),
            ("x-forwarded-for", "10.0.0.1"),
        ]);

        let request = context_interceptor(request).expect("interceptor must not reject");

        let cx = request
            .extensions()
            .get::<RequestContext>()
            .expect("RequestContext must be in extensions");
        assert_eq!(cx.request_id, "from-metadata");
        assert_eq!(cx.client_ip, "10.0.0.1");
        assert_eq!(cx.protocol, Protocol::Grpc);
        assert!(cx.principal.is_none());
        assert!(cx.trace_cx.get::<u32>().is_none(), "P-TRACE 前 trace_cx 为空 Context");
    }

    #[test]
    fn interceptor_passes_request_through_untouched() {
        // 流式语义护栏：interceptor 只在 extensions 里加 context，不改
        // metadata / message（pre-call 透明）。
        let mut request = Request::new(());
        *request.metadata_mut() = md_with(&[("x-api-key", "sk-a")]);
        let request = context_interceptor(request).unwrap();
        assert_eq!(
            request.metadata().get("x-api-key").and_then(|v| v.to_str().ok()),
            Some("sk-a")
        );
        assert_eq!(*request.get_ref(), ());
    }

    #[test]
    fn interceptor_leaves_fields_empty_when_metadata_absent() {
        // 不提前生成 UUID / 不套用 peer：proto headers fallback 只有在
        // 字段留空时才可能生效（post-decode finalize 的职责）。
        let request = context_interceptor(Request::new(())).unwrap();
        let cx = request.extensions().get::<RequestContext>().unwrap();
        assert_eq!(cx.request_id, "");
        assert_eq!(cx.client_ip, "");
    }

    // ===== from_grpc_metadata：metadata 侧提取（原 extract_request_id /
    //       extract_client_ip 的 metadata 分支，测试随迁） =====

    #[test]
    fn metadata_request_id_valid_value() {
        let md = md_with(&[("x-client-request-id", "from-metadata")]);
        assert_eq!(
            RequestContext::from_grpc_metadata(&md, None, &[]).request_id,
            "from-metadata"
        );
    }

    #[test]
    fn metadata_request_id_skips_invalid_value() {
        let md = md_with(&[("x-client-request-id", &"x".repeat(513))]);
        assert_eq!(RequestContext::from_grpc_metadata(&md, None, &[]).request_id, "");
    }

    #[test]
    fn metadata_client_ip_prefers_xff_over_real_ip() {
        // 无 peer（UDS）+ 空 trusted：保留 header 优先语义。
        let md = md_with(&[("x-forwarded-for", "10.0.0.1"), ("x-real-ip", "10.0.0.2")]);
        assert_eq!(RequestContext::from_grpc_metadata(&md, None, &[]).client_ip, "10.0.0.1");
    }

    #[test]
    fn metadata_client_ip_skips_empty_xff() {
        let md = md_with(&[("x-forwarded-for", ""), ("x-real-ip", "10.0.0.2")]);
        assert_eq!(RequestContext::from_grpc_metadata(&md, None, &[]).client_ip, "10.0.0.2");
    }

    // ===== P-XFF（gRPC 侧）：peer 锚定 fail-safe =====

    #[test]
    fn metadata_client_ip_untrusted_peer_ignores_forged_xff() {
        // 非受信 TCP peer 携带 XFF → 忽略 XFF，用 peer（防伪造）。
        let md = md_with(&[("x-forwarded-for", "10.0.0.99")]);
        assert_eq!(
            RequestContext::from_grpc_metadata(&md, addr("203.0.113.7"), &[]).client_ip,
            "203.0.113.7"
        );
    }

    #[test]
    fn metadata_client_ip_trusted_proxy_honors_forwarded_client() {
        let md = md_with(&[("x-forwarded-for", "203.0.113.55")]);
        let trusted = vec!["10.0.0.0/8".parse().unwrap()];
        assert_eq!(
            RequestContext::from_grpc_metadata(&md, addr("10.0.0.1"), &trusted).client_ip,
            "203.0.113.55"
        );
    }

    // ===== finalize_context：post-decode fallback（原 extract_* 的 proto
    //       headers / peer / UUID 分支，测试随迁） =====

    #[test]
    fn finalize_keeps_metadata_request_id_over_proto_headers() {
        let md = md_with(&[("x-client-request-id", "from-metadata")]);
        let proto = headers_with(&[("x-client-request-id", "from-headers")]);
        let cx = finalize_context(None, &md, &proto, None, &[]);
        assert_eq!(cx.request_id, "from-metadata");
    }

    #[test]
    fn finalize_falls_back_to_proto_headers_request_id() {
        let proto = headers_with(&[("x-client-request-id", "from-headers")]);
        let cx = finalize_context(None, &MetadataMap::new(), &proto, None, &[]);
        assert_eq!(cx.request_id, "from-headers");
    }

    #[test]
    fn finalize_skips_invalid_metadata_value_then_uses_proto() {
        let md = md_with(&[("x-client-request-id", &"x".repeat(513))]);
        let proto = headers_with(&[("x-client-request-id", "from-headers")]);
        let cx = finalize_context(None, &md, &proto, None, &[]);
        assert_eq!(cx.request_id, "from-headers");
    }

    #[test]
    fn finalize_generates_uuid_when_request_id_absent() {
        let cx = finalize_context(None, &MetadataMap::new(), &HashMap::new(), None, &[]);
        assert!(Uuid::parse_str(&cx.request_id).is_ok(), "expected UUID, got {}", cx.request_id);
    }

    #[test]
    fn finalize_keeps_metadata_client_ip_over_proto_headers() {
        // 无 peer（UDS）→ metadata XFF 生效（fail-safe 不触发）；metadata 命中
        // 后 finalize 不覆盖。proto XFF 10.0.0.3 被忽略（cx 已非空）。
        let md = md_with(&[("x-forwarded-for", "10.0.0.1")]);
        let proto = headers_with(&[("x-forwarded-for", "10.0.0.3")]);
        let cx = finalize_context(None, &md, &proto, None, &[]);
        assert_eq!(cx.client_ip, "10.0.0.1");
    }

    #[test]
    fn finalize_uses_metadata_real_ip_when_no_xff() {
        let md = md_with(&[("x-real-ip", "10.0.0.2")]);
        let cx = finalize_context(None, &md, &HashMap::new(), None, &[]);
        assert_eq!(cx.client_ip, "10.0.0.2");
    }

    #[test]
    fn finalize_falls_back_to_proto_headers_client_ip() {
        // 无 peer（UDS）+ metadata 缺 → proto headers XFF 生效。
        let proto = headers_with(&[("x-forwarded-for", "10.0.0.3")]);
        let cx = finalize_context(None, &MetadataMap::new(), &proto, None, &[]);
        assert_eq!(cx.client_ip, "10.0.0.3");
    }

    #[test]
    fn finalize_falls_back_to_remote_addr() {
        // 无 header → peer（非受信直连 peer 即客户端）。
        let cx = finalize_context(None, &MetadataMap::new(), &HashMap::new(), addr("192.168.1.1"), &[]);
        assert_eq!(cx.client_ip, "192.168.1.1");
    }

    #[test]
    fn finalize_client_ip_empty_when_no_source() {
        let cx = finalize_context(None, &MetadataMap::new(), &HashMap::new(), None, &[]);
        assert_eq!(cx.client_ip, "");
    }

    #[test]
    fn finalize_untrusted_peer_ignores_proto_xff() {
        // P-XFF: 非受信 peer + proto headers XFF → 忽略 XFF，用 peer。
        let proto = headers_with(&[("x-forwarded-for", "10.0.0.99")]);
        let cx = finalize_context(None, &MetadataMap::new(), &proto, addr("192.168.1.1"), &[]);
        assert_eq!(cx.client_ip, "192.168.1.1");
    }

    #[test]
    fn finalize_respects_interceptor_filled_context() {
        // interceptor 已填充（生产路径）：finalize 不覆盖已定案字段。request
        // 无 remote_addr（UDS）→ metadata 缺 XFF 时 client_ip 留空，finalize
        // 用 proto headers XFF（无 peer → header 优先）。
        let md = md_with(&[("x-client-request-id", "from-metadata")]);
        let request = context_interceptor({
            let mut r = Request::new(());
            *r.metadata_mut() = md.clone();
            r
        })
        .unwrap();
        let prefilled = request.extensions().get::<RequestContext>().cloned();
        let proto = headers_with(&[
            ("x-client-request-id", "from-headers"),
            ("x-forwarded-for", "10.0.0.3"),
        ]);
        let cx = finalize_context(prefilled, &md, &proto, None, &[]);
        assert_eq!(cx.request_id, "from-metadata");
        assert_eq!(cx.client_ip, "10.0.0.3", "metadata 未给 client_ip 时 proto headers 生效");
    }

    // ===== 护栏：gRPC handler 不再各自 inline 提取 request_id/client_ip =====

    #[test]
    fn grpc_handlers_do_not_extract_inline() {
        let src = include_str!("mod.rs");
        let boundary = src.find("#[cfg(test)]").unwrap_or(src.len());
        let prod = &src[..boundary];
        assert!(
            !prod.contains("extract_request_id"),
            "P-MW: grpc/mod.rs handlers must read RequestContext (interceptor::finalize_context), not extract_request_id inline"
        );
        assert!(
            !prod.contains("extract_client_ip"),
            "P-MW: grpc/mod.rs handlers must read RequestContext (interceptor::finalize_context), not extract_client_ip inline"
        );
    }
}
