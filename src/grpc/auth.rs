//! Per-model policy enforcement at the handler layer (P-MW D6: the
//! interceptor runs pre-decode and cannot know the model name): API-key auth
//! and rate limiting, mirroring the HTTP layer.

use super::error::err;
use std::collections::HashMap;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::Status;
use tracing::warn;

/// API-key enforcement mirroring the HTTP layer's `enforce_auth`: transport
/// metadata first (idiomatic gRPC), then the protobuf `headers` map
/// (REST→gRPC bridges). An empty `keys` list accepts any non-empty value.
pub(super) fn enforce_auth_grpc(
    auth: Option<&crate::config::AuthPolicy>,
    metadata: &MetadataMap,
    headers: &HashMap<String, String>,
) -> Result<(), Status> {
    let Some(auth) = auth else {
        return Ok(());
    };
    let value = metadata
        .get(&auth.header)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            // Proto headers map is a plain HashMap — match case-insensitively
            // per HTTP header semantics.
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&auth.header))
                .map(|(_, v)| v.clone())
        })
        .unwrap_or_default();
    if value.is_empty() {
        return Err(err(Status::unauthenticated(format!(
            "missing API key (header: {})",
            auth.header
        ))));
    }
    if !auth.keys.is_empty() && !crate::access_control::ct_contains(&auth.keys, &value) {
        return Err(err(Status::unauthenticated(format!(
            "invalid API key (header: {})",
            auth.header
        ))));
    }
    Ok(())
}

/// gRPC 限流（P3-1，对齐 HTTP `enforce_rate_limit`）：policy 来自
/// `ModelVersion.policies.rate_limit`；`key=="ip"` 用清洗后 client_ip，否则
/// `/predict` 路由 scope（同模型所有推理共享一桶）。超限 → ResourceExhausted
/// （**专给限流**，落 4xx）+ `retry-after` metadata（§4.0.9 收口；queue-full/
/// 过载用 Unavailable）。无 policy 不限；`rpm<=0` fail-closed（RateLimiter 内置）。
/// 限流放 handler：interceptor 在 decode 前取不到 model 名（D6）。
pub(super) fn enforce_grpc_rate_limit(
    rate_limiter: &crate::rate_limit::RateLimiter,
    rl: Option<&crate::config::RateLimitPolicy>,
    model_name: &str,
    client_ip: &str,
) -> Result<(), Status> {
    let Some(rl) = rl else {
        return Ok(());
    };
    let scope = match rl.key.as_str() {
        "ip" => client_ip.to_string(),
        _ => "/predict".to_string(),
    };
    if rl.key == "ip" && scope.is_empty() {
        warn!(
            model = %model_name,
            "rate-limit key=ip resolved to empty scope; all requests share one bucket"
        );
    }
    let burst = rl.burst.unwrap_or(rl.requests_per_minute * 1.5);
    let key = format!("{}:{}", model_name, scope);
    match rate_limiter.acquire(&key, rl.requests_per_minute, burst) {
        crate::rate_limit::AcquireResult::Allowed => Ok(()),
        crate::rate_limit::AcquireResult::Rejected { retry_after_secs } => {
            let mut status = Status::resource_exhausted(format!(
                "rate limit exceeded for {} (retry in {}s)",
                model_name, retry_after_secs
            ));
            // retry-after 经 metadata 回传（对齐 HTTP Retry-After header）。
            if let Ok(v) = MetadataValue::try_from(retry_after_secs.to_string().as_str()) {
                status.metadata_mut().insert("retry-after", v);
            }
            Err(err(status))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthPolicy, RateLimitPolicy};

    // ===== auth policy tests =====

    fn policy(keys: &[&str]) -> AuthPolicy {
        AuthPolicy {
            header: "x-api-key".to_string(),
            keys: keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    #[test]
    fn test_metadata_key_passes() {
        let mut md = MetadataMap::new();
        md.insert("x-api-key", "sk-a".parse().unwrap());
        assert!(enforce_auth_grpc(Some(&policy(&["sk-a"])), &md, &HashMap::new()).is_ok());
    }

    #[test]
    fn test_proto_headers_fallback_passes() {
        let headers = HashMap::from([("X-API-Key".to_string(), "sk-a".to_string())]);
        assert!(
            enforce_auth_grpc(Some(&policy(&["sk-a"])), &MetadataMap::new(), &headers).is_ok()
        );
    }

    #[test]
    fn test_missing_key_unauthenticated() {
        let err = enforce_auth_grpc(Some(&policy(&["sk-a"])), &MetadataMap::new(), &HashMap::new())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_wrong_key_unauthenticated() {
        let mut md = MetadataMap::new();
        md.insert("x-api-key", "nope".parse().unwrap());
        let err = enforce_auth_grpc(Some(&policy(&["sk-a"])), &md, &HashMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_empty_keys_accepts_any_nonempty() {
        let mut md = MetadataMap::new();
        md.insert("x-api-key", "anything".parse().unwrap());
        assert!(enforce_auth_grpc(Some(&policy(&[])), &md, &HashMap::new()).is_ok());
    }

    // ===== P3-1: gRPC 限流 =====

    fn rl(rpm: f64, key: &str, burst: Option<f64>) -> RateLimitPolicy {
        RateLimitPolicy {
            requests_per_minute: rpm,
            key: key.to_string(),
            burst,
        }
    }

    /// 无 policy 不限（直通 Ok）。
    #[test]
    fn rate_limit_no_policy_is_unlimited() {
        let limiter = crate::rate_limit::RateLimiter::default();
        assert!(enforce_grpc_rate_limit(&limiter, None, "m", "1.2.3.4").is_ok());
    }

    /// 超限返 ResourceExhausted + retry-after metadata（§4.0.9：ResourceExhausted
    /// 专给限流，落 4xx；queue-full/过载才是 Unavailable）。
    #[test]
    fn rate_limit_over_limit_returns_resource_exhausted_with_retry_after() {
        let limiter = crate::rate_limit::RateLimiter::default();
        let policy = rl(1.0, "ip", None); // 极低配额
        // 首个请求耗尽配额，第二个被拒。
        let _ = enforce_grpc_rate_limit(&limiter, Some(&policy), "rlm", "9.9.9.9");
        let err = enforce_grpc_rate_limit(&limiter, Some(&policy), "rlm", "9.9.9.9")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(
            err.metadata().get("retry-after").is_some(),
            "retry-after metadata must be present: {:?}",
            err.metadata()
        );
    }

    /// key="ip"：不同 IP 各自独立桶（互不影响）。
    #[test]
    fn rate_limit_key_ip_separates_buckets_by_ip() {
        let limiter = crate::rate_limit::RateLimiter::default();
        let policy = rl(1.0, "ip", None);
        let _ = enforce_grpc_rate_limit(&limiter, Some(&policy), "m", "10.0.0.1");
        // 不同 IP 不受 10.0.0.1 的桶耗尽影响。
        assert!(enforce_grpc_rate_limit(&limiter, Some(&policy), "m", "10.0.0.2").is_ok());
    }

    /// rpm<=0 fail-closed（RateLimiter 内置：rate=0 直接拒，retry_after 兜底）。
    #[test]
    fn rate_limit_zero_rpm_fails_closed() {
        let limiter = crate::rate_limit::RateLimiter::default();
        let policy = rl(0.0, "route", None);
        let err = enforce_grpc_rate_limit(&limiter, Some(&policy), "m", "1.2.3.4").unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }
}
