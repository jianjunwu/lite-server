use crate::error::AppError;
use crate::http::state::AppState;
use axum::{extract::Query, response::Json};
use axum::http::header::HeaderMap;
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use axum::response::Response;
use std::sync::Arc;
use tracing::warn;

/// Map an HTTP status code to its Prometheus label family.
///
/// ``Status.code`` in the worker protocol describes *execution* state
/// (Ok = pipeline completed normally, Error = exception), while
/// ``SingleResponse.status_code`` carries the HTTP status.  This
/// function bridges the two so early 4xx / 5xx responses are recorded
/// under the correct Prometheus label rather than hardcoded "2xx".
fn status_family(status_code: i32) -> &'static str {
    match status_code / 100 {
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "2xx", // 0 (unset) counts as success
    }
}

/// JSON body extractor that converts axum's plain-text `JsonRejection`
/// into a standardized `AppError::InvalidRequestBody` response.
pub struct ApiJson<T>(pub T);

#[axum::async_trait]
impl<S, T> axum::extract::FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    Json<T>: axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
{
    type Rejection = AppError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(ApiJson(value)),
            Err(rejection) => Err(AppError::InvalidRequestBody(rejection.body_text())),
        }
    }
}

/// Query extractor that converts axum's plain-text `QueryRejection`
/// into a standardized `AppError::InvalidQueryParam` response.
pub struct ApiQuery<T>(pub T);

#[axum::async_trait]
impl<S, T> axum::extract::FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    Query<T>: axum::extract::FromRequestParts<S, Rejection = axum::extract::rejection::QueryRejection>,
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(ApiQuery(value)),
            Err(rejection) => Err(AppError::InvalidQueryParam(rejection.body_text())),
        }
    }
}

pub mod health;
pub use health::*;
pub mod admin;
pub use admin::*;
pub mod inference;
pub use inference::*;
pub mod custom_routes;
pub use custom_routes::*;
pub mod stream;
pub use stream::*;
pub mod files;
pub use files::*;

#[cfg(test)]
mod tests;


/// Pure decision for the peer-IP fallback layer: return the IP string to inject
/// as `x-real-ip`, or `None`. Injection is skipped when a proxy header is
/// already present (never overwrite a client/proxy-supplied value) or when
/// there is no peer (the unix-socket path carries no `ConnectInfo`).
pub(crate) fn peer_ip_fallback_target(
    headers: &HeaderMap,
    peer: Option<std::net::SocketAddr>,
) -> Option<String> {
    let has_proxy = headers.contains_key("x-forwarded-for")
        || headers.contains_key("x-real-ip");
    if has_proxy {
        return None;
    }
    peer.map(|p| p.ip().to_string())
}

/// Middleware: inject the TCP peer IP as a fallback `x-real-ip` for direct
/// (non-proxied) connections so client_ip is never empty for them
/// (the 0.7.x regression). A no-op when `ConnectInfo` is unavailable (unix
/// socket) or when a proxy header is already present. P-MW: the read side
/// converged on `RequestContext::client_ip` (`request_context::http_client_ip`,
/// same header > peer precedence); this layer remains so the injected header
/// also reaches `RequestMeta.headers` verbatim for workers.
pub(crate) async fn peer_ip_fallback(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let peer = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0);
    if let Some(ip) = peer_ip_fallback_target(req.headers(), peer) {
        if let Ok(val) = axum::http::HeaderValue::from_str(&ip) {
            req.headers_mut().insert("x-real-ip", val);
        }
    }
    next.run(req).await
}

/// Acquire a rate-limit token for a model. Shared by unary infer, SSE, and WS.
///
/// API-key check for models declaring `policies.auth`. An empty `keys` list
/// accepts any non-empty value (mirrors the retired Python RequireApiKey).
fn enforce_auth(
    auth: Option<&crate::config::AuthPolicy>,
    headers: &HeaderMap,
) -> Result<(), AppError> {
    let Some(auth) = auth else {
        return Ok(());
    };
    let value = headers
        .get(&auth.header)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if value.is_empty() {
        return Err(AppError::Unauthorized(format!(
            "missing API key (header: {})",
            auth.header
        )));
    }
    if !auth.keys.is_empty() && !crate::access_control::ct_contains(&auth.keys, value) {
        return Err(AppError::Unauthorized(format!(
            "invalid API key (header: {})",
            auth.header
        )));
    }
    Ok(())
}

/// `scope` derives from the policy key: `"ip"` → client IP from the
/// `RequestContext` (filled once by `context_middleware`), otherwise the
/// constant `"/predict"` route scope so all inference paths for
/// a model share one bucket. Returns `RateLimitExceeded` (429 + Retry-After)
/// when the bucket is empty.
async fn enforce_rate_limit(
    state: &Arc<AppState>,
    rl: Option<&crate::config::RateLimitPolicy>,
    model_name: &str,
    client_ip: &str,
) -> Result<(), AppError> {
    let Some(rl) = rl else {
        return Ok(());
    };
    let scope = match rl.key.as_str() {
        "ip" => client_ip.to_string(),
        _ => "/predict".to_string(),
    };
    // Empty IP scope (unix-socket path with no peer, or a proxy that sent no
    // client headers) collapses every request into one shared bucket — surface
    // it once so per-IP limiting silently degrading to a global cap is visible.
    if rl.key == "ip" && scope.is_empty() {
        warn!(
            model = %model_name,
            "rate-limit key=ip resolved to empty scope; all requests share one bucket"
        );
    }
    let burst = rl.burst.unwrap_or(rl.requests_per_minute * 1.5);
    let key = format!("{}:{}", model_name, scope);
    match state
        .rate_limiter
        .acquire(&key, rl.requests_per_minute, burst)
    {
        crate::rate_limit::AcquireResult::Rejected { retry_after_secs } => {
            Err(AppError::RateLimitExceeded { retry_after_secs })
        }
        crate::rate_limit::AcquireResult::Allowed => Ok(()),
    }
}

/// Headers that must NOT be overridden by user code — managed by the HTTP
/// library or carry hop-by-hop semantics (RFC 7230 §6.1).
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

fn inject_response_headers(
    builder: axum::http::response::Builder,
    headers: &std::collections::HashMap<String, String>,
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

/// Inject response headers into an existing `HeaderMap` in place — the
/// error-response path builds via `into_response()` (no Builder), so it needs
/// this mutating variant. Skips hop-by-hop / library-managed headers and
/// silently drops headers with invalid names/values.
pub(crate) fn inject_response_headers_into(
    map: &mut HeaderMap,
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




#[cfg(test)]
mod extractor_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn read_error_body(response: Response) -> Value {
        let body_bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        serde_json::from_slice(&body_bytes).unwrap()
    }

    #[tokio::test]
    async fn test_api_json_valid_body_passes() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input": 1}"#)).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_api_json_malformed_body_standardized_error() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .header("content-type", "application/json")
                .body(Body::from("{not json")).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_request_body");
        assert_eq!(body["error"]["param"], Value::Null);
    }

    #[tokio::test]
    async fn test_api_json_missing_content_type_standardized_error() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .body(Body::from(r#"{"input": 1}"#)).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["code"], "invalid_request_body");
    }

    #[tokio::test]
    async fn test_api_query_invalid_param_standardized_error() {
        #[derive(serde::Deserialize)]
        struct TestQuery { #[allow(dead_code)] flag: bool }

        let app = axum::Router::new().route("/t", axum::routing::get(
            |ApiQuery(q): ApiQuery<TestQuery>| async move { q.flag.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t?flag=notabool").body(Body::empty()).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_query_param");
        assert_eq!(body["error"]["param"], Value::Null);
    }
}

#[cfg(test)]
mod status_family_tests {
    use super::status_family;

    #[test]
    fn test_status_family_mapping() {
        assert_eq!(status_family(0), "2xx");     // unset → success
        assert_eq!(status_family(200), "2xx");
        assert_eq!(status_family(201), "2xx");
        assert_eq!(status_family(302), "3xx");
        assert_eq!(status_family(400), "4xx");
        assert_eq!(status_family(404), "4xx");
        assert_eq!(status_family(500), "5xx");
        assert_eq!(status_family(503), "5xx");
    }
}


#[cfg(test)]
mod client_ip_tests {
    use super::*;
    use std::net::SocketAddr;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn addr(ip: &str) -> Option<SocketAddr> {
        Some(format!("{ip}:1234").parse().unwrap())
    }

    // --- extract_client_ip: header-based resolution moved to
    //     `request_context::http_client_ip` (P-MW); its tests moved with it. ---

    // --- peer_ip_fallback_target: should the layer inject the peer IP? ---

    #[test]
    fn fallback_injects_peer_when_no_proxy_header() {
        // Direct connection (no reverse proxy): the TCP peer IS the client.
        // This is the 0.7.x regression — client_ip was empty for direct HTTP.
        assert_eq!(
            peer_ip_fallback_target(&headers(&[]), addr("203.0.113.7")),
            Some("203.0.113.7".to_string())
        );
    }

    #[test]
    fn fallback_skips_when_xff_present() {
        // A (spoofable) proxy header wins; never overwrite it with the peer IP.
        let h = headers(&[("x-forwarded-for", "10.0.0.99")]);
        assert_eq!(peer_ip_fallback_target(&h, addr("203.0.113.7")), None);
    }

    #[test]
    fn fallback_skips_when_real_ip_present() {
        let h = headers(&[("x-real-ip", "10.0.0.2")]);
        assert_eq!(peer_ip_fallback_target(&h, addr("203.0.113.7")), None);
    }

    #[test]
    fn fallback_skips_when_no_peer() {
        // Unix-socket path: no ConnectInfo → nothing to inject.
        assert_eq!(peer_ip_fallback_target(&headers(&[]), None), None);
    }
}

#[cfg(test)]
mod auth_policy_tests {
    use super::*;
    use crate::config::AuthPolicy;

    fn policy(keys: &[&str]) -> AuthPolicy {
        AuthPolicy {
            header: "X-API-Key".to_string(),
            keys: keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    fn headers_with(key: &str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            axum::http::HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[test]
    fn test_no_policy_passes() {
        assert!(enforce_auth(None, &HeaderMap::new()).is_ok());
    }

    #[test]
    fn test_missing_header_rejected() {
        let err = enforce_auth(Some(&policy(&["sk-a"])), &HeaderMap::new()).unwrap_err();
        assert!(matches!(err, AppError::Unauthorized(_)));
        assert!(err.to_string().contains("missing API key"));
    }

    #[test]
    fn test_wrong_key_rejected() {
        let h = headers_with("x-api-key", "sk-wrong");
        let err = enforce_auth(Some(&policy(&["sk-a"])), &h).unwrap_err();
        assert!(err.to_string().contains("invalid API key"));
    }

    #[test]
    fn test_correct_key_passes() {
        let h = headers_with("X-API-Key", "sk-a");
        assert!(enforce_auth(Some(&policy(&["sk-a", "sk-b"])), &h).is_ok());
    }

    #[test]
    fn test_empty_keys_accepts_any_nonempty() {
        let h = headers_with("x-api-key", "anything");
        assert!(enforce_auth(Some(&policy(&[])), &h).is_ok());
        assert!(enforce_auth(Some(&policy(&[])), &HeaderMap::new()).is_err());
    }

    #[test]
    fn test_custom_header_name_case_insensitive() {
        let p = AuthPolicy {
            header: "Authorization".to_string(),
            keys: vec!["tok".to_string()],
        };
        let h = headers_with("authorization", "tok");
        assert!(enforce_auth(Some(&p), &h).is_ok());
    }
}
