//! P-CORS 混合 CORS（全局 + per-model，自写中间件，蓝图 §4.3 P-CORS）。
//!
//! 不用 `tower-http::cors`：per-model 覆盖需运行时按 `:model_name` 取动态策略，
//! CorsLayer 启动时静态挂全 Router 做不到。本中间件 `cors_middleware` 在请求
//! 时解析 model，取生效策略（per-model 存在则用之，否则全局，皆无→直通）。
//!
//! **安全清单（评审 2.2，8 项）**：① Origin 精确匹配；② 禁反射（无 ACAO=请求
//! Origin 的回声，仅命中配置的精确值或字面 `*`）；③ 禁 null；④ 禁后缀混淆
//! （`attacker.com` 不命中 `example.com`）；⑤ credentials + `*` 拒绝；
//! ⑥ `Vary: Origin` 始终；⑦ 预检校验 method/headers 全在清单；⑧ max_age ≤ 7200
//! （Chrome 上限，配置值浏览器自钳）。WS 握手不经浏览器预检/ACAO 强制
//! （评审 1.3），故 WS upgrade 处独立调同一 Origin 引擎校验。
//!
//! **layer 顺序（D21）**：observability(外) → … → CORS → access_control → … →
//! handler。CORS 在 access_control 外，故预检（OPTIONS）不触发鉴权。

use crate::access_control::{classify_http_path, EndpointClass};
use crate::config::CorsPolicy;
use crate::http::routes::access_log_target;
use crate::http::state::AppState;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;

/// 解析后的规范化 Origin（scheme/host 小写、去默认端口）。用于精确匹配。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedOrigin {
    scheme: String,
    host: String,
    port: Option<u16>,
}

impl NormalizedOrigin {
    fn canonical(&self) -> String {
        match self.port {
            Some(p) => format!("{}://{}:{}", self.scheme, self.host, p),
            None => format!("{}://{}", self.scheme, self.host),
        }
    }
}

/// 规范化一个 Origin 串：`https://Example.com:443/` → `https://example.com`。
/// 大小写不敏感 scheme/host、去默认端口（http:80 / https:443）、去尾斜杠与
/// path/query。非法（无 `://`、空 host）→ None。
pub(crate) fn normalize_origin(raw: &str) -> Option<NormalizedOrigin> {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
        return None; // 禁 null Origin
    }
    let (scheme, rest) = raw.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme.is_empty()
        || !scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
    {
        return None;
    }
    // Origin 携带 path/query 时取 authority 部分（防御性，规范上不应有）。
    let authority = rest.split(['/', '?', '#']).next()?;
    let (host, port) = split_host_port(authority)?;
    if host.is_empty() {
        return None;
    }
    let host = host.to_ascii_lowercase();
    let port = match (scheme.as_str(), port) {
        ("https", Some(443)) | ("http", Some(80)) => None,
        (_, p) => p,
    };
    Some(NormalizedOrigin { scheme, host, port })
}

/// `host` / `host:port` / `[ipv6]:port` → (host, Option<port>)。
fn split_host_port(authority: &str) -> Option<(String, Option<u16>)> {
    if let Some(close) = authority.find(']') {
        // `[ipv6]` or `[ipv6]:port`
        if !authority.starts_with('[') {
            return None;
        }
        let host = authority[..=close].to_string();
        let port = authority[close + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok());
        return Some((host, port));
    }
    match authority.rsplit_once(':') {
        Some((host, port_str)) if !host.is_empty() => match port_str.parse::<u16>() {
            Ok(p) => Some((host.to_string(), Some(p))),
            Err(_) => Some((authority.to_string(), None)),
        },
        _ => Some((authority.to_string(), None)),
    }
}

/// 命中后写入 `Access-Control-Allow-Origin` 的值：字面 `*`（通配）或精确 Origin。
#[derive(Debug, Clone, PartialEq, Eq)]
enum AcaoValue {
    Star,
    Origin(String),
}

/// 解析生效 ACAO 值：优先精确/子域匹配（回写规范化 Origin），其次字面 `*`。
/// 不命中 → None（调用方据此不附 ACAO）。credentials 与 `*` 的冲突在调用方收口。
fn resolve_acao(origin: &NormalizedOrigin, allow_origins: &[String]) -> Option<AcaoValue> {
    for entry in allow_origins {
        let entry = entry.trim();
        if entry.is_empty() || entry == "*" {
            continue;
        }
        if matches_specific(entry, origin) {
            return Some(AcaoValue::Origin(origin.canonical()));
        }
    }
    if allow_origins.iter().any(|e| e.trim() == "*") {
        return Some(AcaoValue::Star);
    }
    None
}

/// 精确匹配（规范化后比较）或子域通配（`*.example.com` / `https://*.example.com`）。
fn matches_specific(pattern: &str, origin: &NormalizedOrigin) -> bool {
    // 子域通配：`[scheme://]*.suffix[:port]`
    if pattern.contains("*.") {
        if let Some(w) = parse_wildcard(pattern) {
            return w.matches(origin);
        }
    }
    matches!(normalize_origin(pattern), Some(n) if n == *origin)
}

struct WildcardOrigin {
    /// `*` = 任意 scheme（裸 `*.host`）；否则精确 scheme。
    scheme: String,
    /// `.example.com`（保留前导点，host 必须以此结尾且更长）。
    suffix: String,
    port: Option<u16>,
}

fn parse_wildcard(pattern: &str) -> Option<WildcardOrigin> {
    let (scheme, rest) = match pattern.split_once("://") {
        Some((s, r)) => (s.to_ascii_lowercase(), r),
        None => ("*".to_string(), pattern),
    };
    let (hostpat, port) = split_host_port(rest)?;
    let suffix = hostpat.strip_prefix("*.")?;
    // suffix 不能为空、不能含另一个 `*`。
    if suffix.is_empty() || suffix.contains('*') {
        return None;
    }
    Some(WildcardOrigin {
        scheme,
        suffix: format!(".{}", suffix.to_ascii_lowercase()),
        port,
    })
}

impl WildcardOrigin {
    fn matches(&self, o: &NormalizedOrigin) -> bool {
        if self.scheme != "*" && self.scheme != o.scheme {
            return false;
        }
        if self.port != o.port {
            return false;
        }
        // host 须以 suffix 结尾且更长相邻（apex `example.com` 不被 `*.example.com` 命中）。
        o.host.ends_with(&self.suffix) && o.host.len() > self.suffix.len()
    }
}

/// 生效策略：per-model（model 路径）> 全局。皆无 → None（直通）。
fn effective_policy(state: &AppState, path: &str) -> Option<Arc<CorsPolicy>> {
    if let Some((model, version)) = access_log_target(path) {
        let per_model = match version {
            Some(v) => state.registry.cors_policy_for(model, v),
            None => state.registry.active_cors_policy(model),
        };
        if per_model.is_some() {
            return per_model;
        }
    }
    state.config.server.cors.clone().map(Arc::new)
}

/// 把 `Vary: <value>` 追加到响应头（CORS 多次 append vary 值）。
fn vary(headers: &mut HeaderMap, value: &str) {
    if let Ok(v) = HeaderValue::from_str(value) {
        headers.append(HeaderName::from_static("vary"), v);
    }
}

/// 写 ACAO +（按策略）ACAC。返回是否附了 ACAO（用于决定是否继续附其它头）。
fn apply_acao(headers: &mut HeaderMap, acao: &AcaoValue, credentials: bool) -> bool {
    // 评审 2.2 安全清单⑤：credentials + `*` 拒绝（不附 ACAO）。
    if credentials && matches!(acao, AcaoValue::Star) {
        return false;
    }
    let val = match acao {
        AcaoValue::Star => "*",
        AcaoValue::Origin(s) => s.as_str(),
    };
    if let Ok(v) = HeaderValue::from_str(val) {
        headers.insert(HeaderName::from_static("access-control-allow-origin"), v);
    } else {
        return false;
    }
    if credentials {
        headers.insert(
            HeaderName::from_static("access-control-allow-credentials"),
            HeaderValue::from_static("true"),
        );
    }
    true
}

/// 混合 CORS 中间件（蓝图 P-CORS）。挂在 observability 内、access_control 外。
pub async fn cors_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    // 评审 2.2 / 低#16：admin 端点不面向浏览器，默认不附 ACAO。
    if classify_http_path(&path) == EndpointClass::Admin {
        return next.run(request).await;
    }
    let policy = match effective_policy(&state, &path) {
        Some(p) => p,
        None => return next.run(request).await, // 无策略 → 直通
    };

    let origin = request
        .headers()
        .get("origin")
        .and_then(|v| v.to_str().ok());
    // 蓝图 P-CORS ④：策略存在时 Vary: Origin 始终附加——无/非法 Origin 的
    // 响应也可能被共享缓存再服务给带 Origin 的请求（缓存正确性）。
    let Some(origin_raw) = origin else {
        let mut response = next.run(request).await;
        vary(response.headers_mut(), "origin"); // 同源/非浏览器 → 不附 CORS，仍附 Vary
        return response;
    };
    let Some(norm) = normalize_origin(origin_raw) else {
        let mut response = next.run(request).await;
        vary(response.headers_mut(), "origin"); // 非法/null Origin → 不附，仍附 Vary
        return response;
    };
    let acao = resolve_acao(&norm, &policy.allow_origins);

    // 预检：OPTIONS + Access-Control-Request-Method。
    let is_preflight = request.method() == Method::OPTIONS
        && request.headers().contains_key("access-control-request-method");
    if is_preflight {
        let req_method = request
            .headers()
            .get("access-control-request-method")
            .and_then(|v| v.to_str().ok());
        let req_headers = request
            .headers()
            .get("access-control-request-headers")
            .and_then(|v| v.to_str().ok());
        return preflight_response(&policy, acao.as_ref(), req_method, req_headers);
    }

    // 实际请求：透传给下游，回程附 ACAO/ACAC/Vary/Expose。
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    vary(headers, "origin");
    if let Some(acao) = acao {
        if apply_acao(headers, &acao, policy.allow_credentials)
            && !policy.expose_headers.is_empty()
        {
            if let Ok(v) = HeaderValue::from_str(&policy.expose_headers.join(", ")) {
                headers.insert(
                    HeaderName::from_static("access-control-expose-headers"),
                    v,
                );
            }
        }
    }
    response
}

/// 构造预检响应（评审 2.2）：始终 204；仅当 Origin 命中 且请求 method/headers
/// 全在清单内才附 ACAO/ACAM/ACAH/ACAC/Max-Age（蓝图 P-CORS ⑥）。清单含 `*`
/// 视为全放行；method/headers 比对大小写归一。
fn preflight_response(
    policy: &CorsPolicy,
    acao: Option<&AcaoValue>,
    req_method: Option<&str>,
    req_headers: Option<&str>,
) -> Response {
    let mut builder = Response::builder().status(StatusCode::NO_CONTENT);
    let headers = builder.headers_mut().unwrap();
    vary(headers, "origin");
    vary(headers, "access-control-request-method");
    vary(headers, "access-control-request-headers");

    // 蓝图 ⑥：请求的 method 与 headers 全在清单内才附 CORS 头。
    let method_ok = req_method.is_some_and(|m| {
        let m = m.trim();
        policy
            .allow_methods
            .iter()
            .any(|am| am == "*" || am.eq_ignore_ascii_case(m))
    });
    let headers_ok = req_headers.is_none_or(|h| {
        h.split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .all(|h| policy.allow_headers.iter().any(|ah| ah == "*" || ah.eq_ignore_ascii_case(h)))
    });

    if let Some(acao) = acao {
        if method_ok && headers_ok && apply_acao(headers, acao, policy.allow_credentials) {
            if let Ok(v) = HeaderValue::from_str(&policy.allow_methods.join(", ")) {
                headers.insert(
                    HeaderName::from_static("access-control-allow-methods"),
                    v,
                );
            }
            if let Ok(v) = HeaderValue::from_str(&policy.allow_headers.join(", ")) {
                headers.insert(
                    HeaderName::from_static("access-control-allow-headers"),
                    v,
                );
            }
            if let Ok(v) = HeaderValue::from_str(&policy.max_age_secs.to_string()) {
                headers.insert(HeaderName::from_static("access-control-max-age"), v);
            }
        }
    }
    builder.body(Body::empty()).unwrap()
}

/// P-CORS（评审 1.3）：WS 握手 Origin 校验。浏览器对 WS 不发预检/不强制 ACAO，
/// CORS 中间件不保护 WS，故 upgrade 处独立调同一 Origin 引擎。
/// 未配置 CORS（per-model + global 皆无）→ 放行（WS 仅靠 access_control 鉴权）。
/// 配置了 CORS 但 Origin 不在白名单 → 拒绝（CSWSH 防护）。
///
/// `version` 为路径版本（版本路由 Some，裸路由 None）——与 CORS 中间件一致按
/// path 解析，避免 upgrade 前做异步版本解析。
pub(crate) fn ws_origin_allowed(
    state: &AppState,
    model: &str,
    version: Option<&str>,
    headers: &HeaderMap,
) -> bool {
    let policy = version
        .and_then(|v| state.registry.cors_policy_for(model, v))
        .or_else(|| state.registry.active_cors_policy(model))
        .or_else(|| state.config.server.cors.clone().map(Arc::new));
    let Some(policy) = policy else {
        return true; // 无 CORS 配置 → WS 靠 access_control
    };
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return true; // 无 Origin（非浏览器客户端）→ 放行
    };
    let Some(norm) = normalize_origin(origin) else {
        return false; // 配置了 CORS 但 Origin 非法/null → 拒绝
    };
    resolve_acao(&norm, &policy.allow_origins).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(s: &str) -> NormalizedOrigin {
        normalize_origin(s).unwrap()
    }

    // ===== normalize_origin =====

    #[test]
    fn normalize_lowercases_scheme_host() {
        assert_eq!(norm("HTTPS://Example.COM/").canonical(), "https://example.com");
    }

    #[test]
    fn normalize_strips_default_port() {
        assert_eq!(norm("https://example.com:443").canonical(), "https://example.com");
        assert_eq!(norm("http://example.com:80").canonical(), "http://example.com");
        assert_eq!(norm("https://example.com:8443").canonical(), "https://example.com:8443");
    }

    #[test]
    fn normalize_rejects_null_and_malformed() {
        assert!(normalize_origin("null").is_none());
        assert!(normalize_origin("").is_none());
        assert!(normalize_origin("example.com").is_none()); // no scheme
        assert!(normalize_origin("://example.com").is_none());
    }

    // ===== resolve_acao：精确 / 通配 / 后缀混淆 =====

    #[test]
    fn exact_match_returns_origin() {
        let o = norm("https://app.example.com");
        let r = resolve_acao(&o, &["https://app.example.com".into()]);
        assert_eq!(r, Some(AcaoValue::Origin("https://app.example.com".into())));
    }

    #[test]
    fn attacker_suffix_does_not_match() {
        // 安全清单④：后缀混淆——`https://evil.example.com` 不被 `evil-example.com` 命中，
        // 也不应误匹配一个无关 origin。
        let o = norm("https://evil-example.com");
        assert_eq!(resolve_acao(&o, &["https://example.com".into()]), None);
    }

    #[test]
    fn subdomain_wildcard_matches_subdomain_not_apex() {
        let allow = &["https://*.example.com".into()];
        assert!(resolve_acao(&norm("https://a.example.com"), allow).is_some());
        assert!(resolve_acao(&norm("https://a.b.example.com"), allow).is_some());
        // apex 不命中（*.example.com 不含 example.com 本身）
        assert!(resolve_acao(&norm("https://example.com"), allow).is_none());
        // 不同 host 后缀不命中
        assert!(resolve_acao(&norm("https://a.notexample.com"), allow).is_none());
    }

    #[test]
    fn wildcard_scheme_matches_any_scheme() {
        let allow = &["*.example.com".into()];
        assert!(resolve_acao(&norm("http://a.example.com"), allow).is_some());
        assert!(resolve_acao(&norm("https://a.example.com"), allow).is_some());
    }

    #[test]
    fn star_matches_any_origin_as_literal_star() {
        let o = norm("https://anything.com");
        assert_eq!(resolve_acao(&o, &["*".into()]), Some(AcaoValue::Star));
    }

    // ===== apply_acao：credentials + * 拒绝 =====

    #[test]
    fn credentials_with_star_rejected_no_acao() {
        // 安全清单⑤：credentials=true + `*` → 不附 ACAO。
        let mut h = HeaderMap::new();
        assert!(!apply_acao(&mut h, &AcaoValue::Star, true));
        assert!(h.get("access-control-allow-origin").is_none());
    }

    #[test]
    fn credentials_with_star_ok_when_no_credentials() {
        let mut h = HeaderMap::new();
        assert!(apply_acao(&mut h, &AcaoValue::Star, false));
        assert_eq!(h.get("access-control-allow-origin").unwrap(), "*");
        assert!(h.get("access-control-allow-credentials").is_none());
    }

    #[test]
    fn credentials_emits_acac_for_specific_origin() {
        let mut h = HeaderMap::new();
        assert!(apply_acao(
            &mut h,
            &AcaoValue::Origin("https://app.example.com".into()),
            true
        ));
        assert_eq!(h.get("access-control-allow-origin").unwrap(), "https://app.example.com");
        assert_eq!(h.get("access-control-allow-credentials").unwrap(), "true");
    }

    // ===== preflight_response：method/headers 全在清单才附头 =====

    #[test]
    fn preflight_attaches_headers_when_origin_allowed() {
        let policy = CorsPolicy {
            allow_origins: vec!["https://app.example.com".into()],
            allow_methods: vec!["POST".into()],
            allow_headers: vec!["content-type".into()],
            allow_credentials: true,
            max_age_secs: 600,
            ..Default::default()
        };
        let acao = Some(AcaoValue::Origin("https://app.example.com".into()));
        let resp = preflight_response(&policy, acao.as_ref(), Some("POST"), Some("content-type"));
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let h = resp.headers();
        assert_eq!(h.get("access-control-allow-origin").unwrap(), "https://app.example.com");
        assert_eq!(h.get("access-control-allow-methods").unwrap(), "POST");
        assert_eq!(h.get("access-control-allow-headers").unwrap(), "content-type");
        assert_eq!(h.get("access-control-allow-credentials").unwrap(), "true");
        assert_eq!(h.get("access-control-max-age").unwrap(), "600");
        // Vary 携带 origin / acrm / acrh
        let vary = h.get_all("vary").iter().map(|v| v.to_str().unwrap()).collect::<Vec<_>>();
        assert!(vary.contains(&"origin"));
        assert!(vary.contains(&"access-control-request-method"));
    }

    #[test]
    fn preflight_no_acao_when_origin_not_allowed() {
        let policy = CorsPolicy {
            allow_origins: vec!["https://app.example.com".into()],
            ..Default::default()
        };
        let resp = preflight_response(&policy, None, Some("POST"), None);
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(resp.headers().get("access-control-allow-origin").is_none());
        assert!(resp.headers().get("access-control-allow-methods").is_none());
    }

    #[test]
    fn preflight_no_acao_when_request_headers_not_allowed() {
        // 蓝图 ⑥：ACRH 含清单外 header → 不附 CORS 头（method 命中也不够）。
        let policy = CorsPolicy {
            allow_origins: vec!["https://app.example.com".into()],
            allow_methods: vec!["POST".into()],
            allow_headers: vec!["content-type".into()],
            ..Default::default()
        };
        let acao = Some(AcaoValue::Origin("https://app.example.com".into()));
        let resp = preflight_response(
            &policy,
            acao.as_ref(),
            Some("POST"),
            Some("content-type, x-internal"),
        );
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(resp.headers().get("access-control-allow-origin").is_none());
    }
}

/// §6.7 解析面 property 测试（proptest）：Origin 规范化 / 通配匹配 / ACAO 禁反射 /
/// 预检 header 列表解析的安全不变式。
#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    /// 任意 Origin-ish 串：形态良好的 URL、合法 Origin、全任意串混合。
    fn arb_origin_string() -> impl Strategy<Value = String> {
        prop_oneof![
            3 => "[a-zA-Z0-9.+\\-]{0,8}://[a-zA-Z0-9.\\-]{0,15}(:[0-9]{1,5})?(/[a-z]*)?",
            2 => "(http|https|HTTP)://[a-z]{3,10}\\.(com|example)(:[0-9]{1,5})?",
            1 => "\\PC{0,32}",
        ]
    }

    /// 结构良好的规范化 Origin（host 必含点，端口 2–4 位恒为合法 u16）。
    fn arb_origin() -> impl Strategy<Value = NormalizedOrigin> {
        "(http|https)://[a-z]{1,8}(\\.[a-z]{1,8}){1,2}(:[0-9]{2,4})?"
            .prop_map(|s| normalize_origin(&s).unwrap())
    }

    /// 清单条目：精确 Origin / 子域通配 / 字面 * / 垃圾混合。
    fn arb_allow_entry() -> impl Strategy<Value = String> {
        prop_oneof![
            3 => "(http|https)://[a-z]{1,6}(\\.[a-z]{1,6}){1,2}",
            2 => "(http|https)://\\*\\.[a-z]{1,6}\\.[a-z]{1,6}",
            1 => Just("*".to_string()),
            1 => "[a-z ]{0,6}",
        ]
    }

    proptest! {
        /// 规范化绝不 panic 且幂等：canonical 再规范化得到同一结果。
        #[test]
        fn normalize_idempotent_and_never_panics(s in arb_origin_string()) {
            if let Some(n) = normalize_origin(&s) {
                let canon = n.canonical();
                prop_assert_eq!(
                    normalize_origin(&canon),
                    Some(n),
                    "not idempotent: {:?} -> {}",
                    s,
                    canon
                );
            }
        }

        /// 安全清单③：null Origin（任意大小写、带空白）一律拒绝。
        #[test]
        fn null_origin_always_rejected(
            null_s in prop::sample::select(vec![
                "null".to_string(), "NULL".to_string(), "Null".to_string(), "nUlL".to_string(),
            ]),
            pad in "[ \t]{0,2}",
        ) {
            let padded = format!("{pad}{null_s}{pad}");
            prop_assert!(normalize_origin(&padded).is_none());
        }

        /// 无 "://" 的裸 host 串一律拒绝（不会与配置的 Origin 误相等）。
        #[test]
        fn bare_host_without_scheme_rejected(s in "[a-zA-Z0-9.\\-]{0,20}") {
            prop_assert!(normalize_origin(&s).is_none());
        }

        /// 安全清单④：通配匹配 ⟺ host 以 `.{suffix}` 结尾且严格更长——
        /// apex 永不命中，后缀混淆（`notexample.com`）永不命中。
        #[test]
        fn wildcard_match_iff_suffix(
            suffix in "[a-z]{2,6}\\.[a-z]{2,6}",
            host in "[a-z]{1,8}(\\.[a-z]{1,8}){0,3}",
        ) {
            let w = parse_wildcard(&format!("*.{suffix}")).unwrap();
            let o = normalize_origin(&format!("http://{host}")).unwrap();
            let expected =
                host.ends_with(&format!(".{suffix}")) && host.len() > suffix.len() + 1;
            prop_assert_eq!(w.matches(&o), expected, "host={} suffix={}", host, suffix);
        }

        /// wildcard 解析结构不变式：suffix 以 . 开头、内部无 *、非空。
        #[test]
        fn wildcard_parse_invariants(s in arb_origin_string()) {
            if let Some(w) = parse_wildcard(&s) {
                prop_assert!(w.suffix.starts_with('.'));
                prop_assert!(w.suffix.len() > 1 && !w.suffix[1..].contains('*'));
            }
        }

        /// 禁反射：ACAO 只能是字面 *（清单含 * 时）或请求 Origin 的 canonical
        /// （且确有清单项命中）——绝不回写清单外的任意串。
        #[test]
        fn acao_only_star_or_matched_canonical(
            o in arb_origin(),
            allow in prop::collection::vec(arb_allow_entry(), 0..5),
        ) {
            match resolve_acao(&o, &allow) {
                None => {}
                Some(AcaoValue::Star) => {
                    prop_assert!(allow.iter().any(|e| e.trim() == "*"))
                }
                Some(AcaoValue::Origin(s)) => {
                    prop_assert_eq!(s, o.canonical());
                    prop_assert!(
                        allow.iter().any(|e| {
                            let e = e.trim();
                            !e.is_empty() && e != "*" && matches_specific(e, &o)
                        }),
                        "ACAO emitted without any matching entry"
                    );
                }
            }
        }

        /// 精确匹配 ⟺ 规范化相等（大小写/默认端口差异不影响语义相等）。
        #[test]
        fn exact_match_iff_normalized_equal(
            o in arb_origin(),
            pat in "(HTTP|https|Https)://[A-Za-z]{1,8}(\\.[a-z]{1,8}){1,2}(:[0-9]{2,4})?",
        ) {
            let m = matches_specific(&pat, &o);
            prop_assert_eq!(m, normalize_origin(&pat).is_some_and(|n| n == o));
        }

        /// header 列表解析（§6.7）：method 与 Origin 均命中时，附 ACAO ⟺
        /// ACRH 每个非空 header 都在清单内（大小写归一）或清单含 *。
        #[test]
        fn preflight_acao_iff_all_request_headers_allowed(
            acrh in prop::option::of(
                prop::collection::vec("[A-Za-z0-9\\- ]{0,12}", 0..5).prop_map(|v| v.join(", "))
            ),
            allow_headers in prop::collection::vec(
                prop_oneof![3 => "[a-z0-9\\-]{1,8}", 1 => Just("\\*".to_string())],
                0..4,
            ),
        ) {
            let policy = CorsPolicy {
                allow_origins: vec!["https://app.example.com".into()],
                allow_methods: vec!["POST".into()],
                allow_headers: allow_headers.clone(),
                ..Default::default()
            };
            let acao = Some(AcaoValue::Origin("https://app.example.com".into()));
            let resp =
                preflight_response(&policy, acao.as_ref(), Some("POST"), acrh.as_deref());
            let attached = resp.headers().contains_key("access-control-allow-origin");
            let expected_ok = acrh.as_deref().is_none_or(|h| {
                h.split(',')
                    .map(str::trim)
                    .filter(|h| !h.is_empty())
                    .all(|h| {
                        allow_headers.iter().any(|ah| ah == "*" || ah.eq_ignore_ascii_case(h))
                    })
            });
            prop_assert_eq!(
                attached, expected_ok, "acrh={:?} allow={:?}", acrh, allow_headers
            );
        }
    }
}
