//! 协议层 seam 测试(P2.0 纯迁移门禁,批次 0;P2.1 检测,P2.2 路由 seam,
//! 批次 2)。
//!
//! 门禁 = 既有全套测试不改一行全绿 + 快照基准:
//! `test_protocol_openai_renderer_byte_identical*`(精确字节快照)、
//! `test_canonical_error_values_preserved`、`test_protocol_openai_renderer_log_levels_c7`。

use super::detect;
use super::{ApiProtocol, render};
use crate::error::{AppError, ModelErrorData, ProtocolError};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

/// render 一个 AppError → (status, headers, body bytes)。协议感知路径走
/// `into_canonical`,与生产 `ProtocolError::into_response` 同一管线。
async fn render_canonical(err: AppError, protocol: ApiProtocol) -> (StatusCode, HeaderMap, Vec<u8>) {
    let resp = render(err.into_canonical(), protocol);
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), 4096)
        .await
        .unwrap()
        .to_vec();
    (status, headers, body)
}

fn json_headers() -> HeaderMap {
    let mut m = HeaderMap::new();
    m.insert("content-type", "application/json".parse().unwrap());
    m
}

// ===== test_protocol_openai_renderer_byte_identical(P2.0 快照门禁) =====

/// 全部常规变体的精确字节快照(status/content-type/body,无多余 header)。
/// 快照取自 0.8.3 现状 `AppError::into_response` 输出(P2.0 零行为变化)。
#[tokio::test]
async fn test_protocol_openai_renderer_byte_identical() {
    let cases: Vec<(AppError, StatusCode, &str)> = vec![
        (
            AppError::ModelNotFound("bert".to_string()),
            StatusCode::NOT_FOUND,
            r#"{"error":{"code":"model_not_found","message":"model not found","param":null,"type":"not_found_error"}}"#,
        ),
        (
            AppError::ModelNotReady("m".to_string()),
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":{"code":"model_not_ready","message":"model not ready","param":null,"type":"model_not_ready"}}"#,
        ),
        (
            AppError::VersionNotFound("a".to_string(), "b".to_string()),
            StatusCode::NOT_FOUND,
            r#"{"error":{"code":"version_not_found","message":"model version not found","param":null,"type":"not_found_error"}}"#,
        ),
        (
            AppError::VersionAlreadyLoaded("a".to_string(), "b".to_string()),
            StatusCode::CONFLICT,
            r#"{"error":{"code":"version_already_loaded","message":"model version already loaded","param":null,"type":"conflict_error"}}"#,
        ),
        (
            AppError::InferenceTimeout("x".to_string()),
            StatusCode::GATEWAY_TIMEOUT,
            r#"{"error":{"code":"timeout","message":"inference timeout","param":null,"type":"server_error"}}"#,
        ),
        (
            AppError::WorkerCrashed("boom".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"internal_error","message":"service temporarily unavailable","param":null,"type":"server_error"}}"#,
        ),
        (
            AppError::Validation("v".to_string()),
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"invalid_parameter_value","message":"validation error","param":null,"type":"invalid_request_error"}}"#,
        ),
        (
            AppError::Config("c".to_string()),
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"invalid_configuration","message":"invalid configuration","param":null,"type":"invalid_request_error"}}"#,
        ),
        (
            AppError::Transport("t".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"internal_error","message":"transport error","param":null,"type":"server_error"}}"#,
        ),
        (
            AppError::Python("p".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"internal_error","message":"internal server error","param":null,"type":"server_error"}}"#,
        ),
        (
            AppError::Io(std::io::Error::other("io")),
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"internal_error","message":"internal server error","param":null,"type":"server_error"}}"#,
        ),
        (
            AppError::Serialization(serde_json::from_str::<serde_json::Value>("x").unwrap_err()),
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"parse_error","message":"serialization error","param":null,"type":"invalid_request_error"}}"#,
        ),
        (
            AppError::FrameTooLarge,
            StatusCode::PAYLOAD_TOO_LARGE,
            r#"{"error":{"code":"content_size_limit_exceeded","message":"message too large","param":null,"type":"invalid_request_error"}}"#,
        ),
        (
            AppError::Internal("i".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"internal_error","message":"internal server error","param":null,"type":"server_error"}}"#,
        ),
        (
            AppError::RouteNotFound,
            StatusCode::NOT_FOUND,
            r#"{"error":{"code":"route_not_found","message":"route not found","param":null,"type":"not_found_error"}}"#,
        ),
        (
            AppError::MethodNotAllowed,
            StatusCode::METHOD_NOT_ALLOWED,
            r#"{"error":{"code":"method_not_allowed","message":"method not allowed","param":null,"type":"method_not_allowed"}}"#,
        ),
        (
            AppError::InvalidRequestBody(
                "Failed to parse the request body as JSON: expected value at line 1 column 1".to_string(),
            ),
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"invalid_request_body","message":"Failed to parse the request body as JSON: expected value at line 1 column 1","param":null,"type":"invalid_request_error"}}"#,
        ),
        (
            AppError::InvalidQueryParam(
                "Failed to deserialize query string: invalid type".to_string(),
            ),
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"invalid_query_param","message":"Failed to deserialize query string: invalid type","param":null,"type":"invalid_request_error"}}"#,
        ),
        (
            AppError::UnsupportedMediaType("gzip".to_string()),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            r#"{"error":{"code":"unsupported_media_type","message":"gzip","param":null,"type":"invalid_request_error"}}"#,
        ),
        (
            AppError::Unauthorized("missing api key".to_string()),
            StatusCode::UNAUTHORIZED,
            r#"{"error":{"code":"unauthorized","message":"missing api key","param":null,"type":"authentication_error"}}"#,
        ),
    ];
    for (err, expected_status, expected_body) in cases {
        let (status, headers, body) = render_canonical(err, ApiProtocol::Legacy).await;
        assert_eq!(status, expected_status, "status for {expected_body}");
        assert_eq!(headers, json_headers(), "headers for {expected_body}");
        assert_eq!(String::from_utf8(body).unwrap(), expected_body);
    }
}

/// ModelError 快照:code 为 None 时 wire 输出 null(Option 保留,字节不变);
/// 模型作者 header 逐字透传(非 hop-by-hop)。
#[tokio::test]
async fn test_protocol_openai_renderer_byte_identical_model_error() {
    // code: None → wire "code": null
    let err = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 400,
        error_type: "INVALID_INPUT".to_string(),
        detail: "input must be non-negative".to_string(),
        code: None,
        param: Some("temperature".to_string()),
        headers: None,
    }));
    let (status, headers, body) = render_canonical(err, ApiProtocol::Legacy).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers, json_headers());
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":{"code":null,"message":"input must be non-negative","param":"temperature","type":"INVALID_INPUT"}}"#
    );

    // code: Some + 模型 header(retry-after/x-trace)透传
    let err = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 503,
        error_type: "model_error".to_string(),
        detail: "overloaded".to_string(),
        code: Some("overloaded_now".to_string()),
        param: None,
        headers: Some(std::collections::HashMap::from([
            ("retry-after".to_string(), "5".to_string()),
            ("x-trace".to_string(), "abc".to_string()),
        ])),
    }));
    let (status, headers, body) = render_canonical(err, ApiProtocol::Legacy).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers.get("retry-after").unwrap(), "5");
    assert_eq!(headers.get("x-trace").unwrap(), "abc");
    assert_eq!(headers.get("content-type").unwrap(), "application/json");
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":{"code":"overloaded_now","message":"overloaded","param":null,"type":"model_error"}}"#
    );
}

/// PayloadTooLarge 快照:413 + extra{max_size, actual_size} 合入 error 对象;
/// actual_size 缺失时 wire 输出 null(现状行为)。
#[tokio::test]
async fn test_protocol_openai_renderer_byte_identical_413() {
    let err = AppError::PayloadTooLarge {
        max_size: 1048576,
        actual_size: Some(2097152),
    };
    let (status, headers, body) = render_canonical(err, ApiProtocol::Legacy).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers, json_headers());
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":{"actual_size":2097152,"code":"payload_too_large","max_size":1048576,"message":"request body too large: 2097152 bytes exceeds the 1048576 bytes limit","param":null,"type":"invalid_request_error"}}"#
    );

    let err = AppError::PayloadTooLarge {
        max_size: 1048576,
        actual_size: None,
    };
    let (status, _, body) = render_canonical(err, ApiProtocol::Legacy).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":{"actual_size":null,"code":"payload_too_large","max_size":1048576,"message":"request body exceeds the 1048576 bytes limit","param":null,"type":"invalid_request_error"}}"#
    );
}

/// QueueFull / RateLimitExceeded 快照:retry-after header + 字节不变。
#[tokio::test]
async fn test_protocol_openai_renderer_byte_identical_retry_after() {
    let err = AppError::QueueFull("q".to_string());
    let (status, headers, body) = render_canonical(err, ApiProtocol::Legacy).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers.get("retry-after").unwrap(), "1");
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":{"code":"queue_full","message":"queue full","param":null,"type":"queue_full"}}"#
    );

    let err = AppError::RateLimitExceeded { retry_after_secs: 5 };
    let (status, headers, body) = render_canonical(err, ApiProtocol::Legacy).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers.get("retry-after").unwrap(), "5");
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":{"code":"rate_limit_exceeded","message":"rate limit exceeded","param":null,"type":"rate_limit_exceeded"}}"#
    );
}

// ===== test_canonical_error_values_preserved(P2.0 门禁) =====

/// canonical 字段 == 现 code/status/message/type 语义(逐变体)。
#[test]
fn test_canonical_error_values_preserved() {
    // 常规变体:status/type/code/message/param 与现状映射一致;headers 仅
    // QueueFull/RateLimitExceeded 有 retry-after;from_model 仅 ModelError。
    let c = AppError::ModelNotFound("bert".into()).into_canonical();
    assert_eq!(c.status, StatusCode::NOT_FOUND);
    assert_eq!(c.error_type, "not_found_error");
    assert_eq!(c.code.as_deref(), Some("model_not_found"));
    assert_eq!(c.message, "model not found");
    assert_eq!(c.param, None);
    assert_eq!(c.extra, None);
    assert_eq!(c.headers, None);
    assert!(!c.from_model);

    let c = AppError::WorkerCrashed("boom".into()).into_canonical();
    assert_eq!(c.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(c.error_type, "server_error");
    assert_eq!(c.code.as_deref(), Some("internal_error"));
    assert_eq!(c.message, "service temporarily unavailable");
    assert!(!c.from_model);

    let c = AppError::Validation("v".into()).into_canonical();
    assert_eq!(c.status, StatusCode::BAD_REQUEST);
    assert_eq!(c.error_type, "invalid_request_error");
    assert_eq!(c.code.as_deref(), Some("invalid_parameter_value"));

    let c = AppError::RateLimitExceeded { retry_after_secs: 5 }.into_canonical();
    assert_eq!(c.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(c.error_type, "rate_limit_exceeded");
    assert_eq!(c.headers.as_ref().unwrap()["retry-after"], "5");

    let c = AppError::QueueFull("q".into()).into_canonical();
    assert_eq!(c.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(c.headers.as_ref().unwrap()["retry-after"], "1");

    // PayloadTooLarge:413 + extra{max_size, actual_size}
    let c = AppError::PayloadTooLarge {
        max_size: 100,
        actual_size: Some(250),
    }
    .into_canonical();
    assert_eq!(c.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(c.error_type, "invalid_request_error");
    assert_eq!(c.code.as_deref(), Some("payload_too_large"));
    assert_eq!(
        c.extra.as_ref().unwrap(),
        &serde_json::json!({"max_size": 100, "actual_size": 250})
    );
    assert_eq!(c.message, "request body too large: 250 bytes exceeds the 100 bytes limit");

    // ModelError:status/type/code 来自模型数据;code 保留 Option(None → wire null)
    let c = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 503,
        error_type: "model_error".into(),
        detail: "overloaded".into(),
        code: Some("overloaded_now".into()),
        param: None,
        headers: Some(std::collections::HashMap::from([(
            "retry-after".into(),
            "5".into(),
        )])),
    }))
    .into_canonical();
    assert_eq!(c.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(c.error_type, "model_error");
    assert_eq!(c.code.as_deref(), Some("overloaded_now"));
    assert_eq!(c.message, "overloaded");
    assert!(c.from_model);
    assert_eq!(c.headers.as_ref().unwrap()["retry-after"], "5");
    assert_eq!(c.log_detail, "overloaded");

    let c = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 400,
        error_type: "INVALID_INPUT".into(),
        detail: "bad".into(),
        code: None,
        param: Some("temperature".into()),
        headers: None,
    }))
    .into_canonical();
    assert_eq!(c.code, None, "模型未提供 code 时保留 None(wire 输出 null)");
    assert_eq!(c.param.as_deref(), Some("temperature"));
}

// ===== test_protocol_openai_renderer_log_levels_c7(P2.0 门禁) =====

use crate::test_tracing::event_counts;

/// C7:4xx(含 429)log at info;5xx log at error;model error log at info。
/// 与 error.rs 既有 LevelCounter 测试断言一致(渲染路径迁移后行为不变)。
#[tokio::test]
async fn test_protocol_openai_renderer_log_levels_c7() {
    let c = event_counts(|| {
        let _ = render(
            AppError::RateLimitExceeded { retry_after_secs: 1 }.into_canonical(),
            ApiProtocol::Legacy,
        );
    });
    assert_eq!(c.error, 0, "429 must not log at error");
    assert_eq!(c.info, 1, "429 should log at info");

    let c = event_counts(|| {
        let _ = render(
            AppError::InvalidRequestBody("bad".into()).into_canonical(),
            ApiProtocol::Legacy,
        );
    });
    assert_eq!(c.error, 0);
    assert_eq!(c.info, 1);

    let c = event_counts(|| {
        let _ = render(
            AppError::WorkerCrashed("boom".into()).into_canonical(),
            ApiProtocol::Legacy,
        );
    });
    assert_eq!(c.error, 1, "5xx must still log at error");
    assert_eq!(c.info, 0);

    // model error:非服务器故障,log at info
    let c = event_counts(|| {
        let _ = render(
            AppError::ModelError(Box::new(ModelErrorData {
                status_code: 503,
                error_type: "model_error".into(),
                detail: "overloaded".into(),
                code: None,
                param: None,
                headers: None,
            }))
            .into_canonical(),
            ApiProtocol::Legacy,
        );
    });
    assert_eq!(c.error, 0, "model error must not log at error");
    assert_eq!(c.info, 1);
}

// ===== KServe renderer + 分派矩阵 =====

/// Kserve 扁平错误体:`{"error": "<message>"}`;extra 丢弃;状态码语义不变。
#[tokio::test]
async fn test_kserve_renderer_flat() {
    // generic
    let err = AppError::ModelNotFound("bert".into());
    let (status, body) = render_kserve_flat(err).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(String::from_utf8(body).unwrap(), r#"{"error":"model not found"}"#);

    // model error(message = 模型 detail,不 sanitize)
    let err = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 400,
        error_type: "INVALID_INPUT".into(),
        detail: "input must be non-negative".into(),
        code: Some("invalid_input".into()),
        param: None,
        headers: None,
    }));
    let (status, body) = render_kserve_flat(err).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":"input must be non-negative"}"#
    );

    // 413:extra 丢弃,message 取 canonical.message
    let err = AppError::PayloadTooLarge {
        max_size: 1048576,
        actual_size: Some(2097152),
    };
    let (status, body) = render_kserve_flat(err).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"error":"request body too large: 2097152 bytes exceeds the 1048576 bytes limit"}"#
    );
}

async fn render_kserve_flat(err: AppError) -> (StatusCode, Vec<u8>) {
    let (status, headers, body) = render_canonical(err, ApiProtocol::Kserve).await;
    assert_eq!(headers.get("content-type").unwrap(), "application/json");
    (status, body)
}

/// 同一 CanonicalError 按 ApiProtocol 分派:Legacy→OpenAI 形状、Kserve→扁平;
/// 状态码跨协议一致;错误码语义(canonical.code)不变。
#[tokio::test]
async fn test_render_dispatch_matrix() {
    let makers: Vec<fn() -> AppError> = vec![
        || AppError::ModelNotFound("m".into()),
        || AppError::WorkerCrashed("boom".into()),
        || AppError::PayloadTooLarge { max_size: 10, actual_size: None },
        || AppError::RateLimitExceeded { retry_after_secs: 3 },
        || AppError::ModelError(Box::new(ModelErrorData {
            status_code: 422,
            error_type: "INVALID_INPUT".into(),
            detail: "bad".into(),
            code: None,
            param: None,
            headers: None,
        })),
    ];
    for make in makers {
        let (legacy_status, _, legacy_body) =
            render_canonical(make(), ApiProtocol::Legacy).await;
        let (kserve_status, _, kserve_body) =
            render_canonical(make(), ApiProtocol::Kserve).await;
        assert_eq!(kserve_status, legacy_status, "状态码跨协议一致");
        // Legacy 形状:error 对象;Kserve 形状:error 字符串
        let legacy: serde_json::Value = serde_json::from_slice(&legacy_body).unwrap();
        let kserve: serde_json::Value = serde_json::from_slice(&kserve_body).unwrap();
        assert!(legacy["error"].is_object(), "Legacy 形状 = error 对象");
        assert!(kserve["error"].is_string(), "Kserve 形状 = error 字符串");
    }
}

// ===== ProtocolError 边界 =====

/// `From<AppError>` 默认 Legacy(既有调用点零改动)。
#[test]
fn test_protocol_error_from_apperror_defaults_legacy() {
    let pe: ProtocolError = AppError::ModelNotFound("x".into()).into();
    assert_eq!(pe.protocol, ApiProtocol::Legacy);
}

/// ProtocolError::into_response 走 render 管线,与 AppError shim 逐字节一致。
#[tokio::test]
async fn test_protocol_error_matches_apperror_shim() {
    let shim_body = axum::body::to_bytes(
        AppError::ModelNotFound("bert".into()).into_response().into_body(),
        4096,
    )
    .await
    .unwrap()
    .to_vec();
    let pe: ProtocolError = AppError::ModelNotFound("bert".into()).into();
    let pe_body = axum::body::to_bytes(pe.into_response().into_body(), 4096)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(shim_body, pe_body);
}

/// CanonicalError 快照中 status/error_type/code/message 是 wire 输出的唯一来源
/// (protocol/ 之外零 wire JSON 拼装的静态锚点:assert 既有 shim 与 render 一致)。
#[test]
fn test_canonical_error_fields_are_stable() {
    let c = AppError::InvalidRequestBody("bad".into()).into_canonical();
    assert_eq!(c.status, StatusCode::BAD_REQUEST);
    assert_eq!(c.error_type, "invalid_request_error");
    assert_eq!(c.code.as_deref(), Some("invalid_request_body"));
    assert_eq!(c.message, "bad");
    assert_eq!(c.param, None);
    assert_eq!(c.extra, None);
    assert_eq!(c.headers, None);
    assert!(!c.from_model);
    assert!(c.log_detail.contains("invalid request body"));
}

// ===== P2.1 检测(detect) =====

#[test]
fn test_t1_prefilter_ihcl_header() {
    // header 存在(含 "0")→ Kserve;缺失 → None(C9:header 是强信号)
    use axum::http::HeaderMap;
    let mut h = HeaderMap::new();
    h.insert("inference-header-content-length", "546".parse().unwrap());
    assert_eq!(detect::t1_prefilter("/v2/models/m/infer", &h), Some(ApiProtocol::Kserve));

    let mut h = HeaderMap::new();
    h.insert("inference-header-content-length", "0".parse().unwrap());
    assert_eq!(detect::t1_prefilter("/v2/models/m/infer", &h), Some(ApiProtocol::Kserve));

    assert_eq!(detect::t1_prefilter("/v2/models/m/infer", &HeaderMap::new()), None);
}

#[test]
fn test_t1_prefilter_other_paths() {
    // 无 IHCL header 的其他路径 → None(信封主判在 T2)
    assert_eq!(detect::t1_prefilter("/health", &axum::http::HeaderMap::new()), None);
}

#[test]
fn test_t2_envelope_double_condition() {
    // 命中:完整信封
    let body = br#"{"id":"r1","inputs":[{"name":"a","shape":[2],"datatype":"FP32","data":[1,2]}]}"#;
    assert!(detect::t2_kserve_envelope(body));

    // 不命中:缺 inputs / 空 inputs / 缺 name/shape/datatype / 非对象 input / 非 JSON
    assert!(!detect::t2_kserve_envelope(br#"{"id":"r1"}"#));
    assert!(!detect::t2_kserve_envelope(br#"{"id":"r1","inputs":[]}"#));
    assert!(!detect::t2_kserve_envelope(
        br#"{"id":"r1","inputs":[{"name":"a","shape":[2]}]}"#,
    ));
    assert!(!detect::t2_kserve_envelope(
        br#"{"id":"r1","inputs":[{"name":"a","datatype":"FP32"}]}"#,
    ));
    assert!(!detect::t2_kserve_envelope(
        br#"{"id":"r1","inputs":[{"shape":[2],"datatype":"FP32"}]}"#,
    ));
    assert!(!detect::t2_kserve_envelope(br#"{"id":"r1","inputs":[42]}"#));
    assert!(!detect::t2_kserve_envelope(b"not-json"));
}

#[test]
fn test_detect_resolve_precedence() {
    use axum::http::HeaderMap;
    // T1 有 → T1(强信号,不等 T2;Triton 二进制/Raw 路径零 T2 成本)
    let envelope = br#"{"inputs":[{"name":"a","shape":[2],"datatype":"FP32","data":[1,2]}]}"#;
    assert_eq!(detect::resolve(None, envelope), ApiProtocol::Kserve);
    // T1 无 + 信封 → Kserve
    let mut h = HeaderMap::new();
    h.insert("inference-header-content-length", "10".parse().unwrap());
    assert_eq!(detect::resolve(Some(ApiProtocol::Kserve), b"not-envelope"), ApiProtocol::Kserve);
    // 否则 Legacy
    assert_eq!(detect::resolve(None, br#"{"x":1}"#), ApiProtocol::Legacy);
    assert_eq!(detect::resolve(None, b"not-json"), ApiProtocol::Legacy);
}

// ===== P2.2 路由 seam(mount no-op,G17) =====

/// mount 后既有路由零变化(阶段 2 ROUTE_MODULES 为空表);未匹配路径
/// 仍走 router 级 fallback(404)。
#[tokio::test]
async fn test_protocol_mount_noop() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let app = axum::Router::new().route(
        "/health",
        axum::routing::get(|| async { "ok" }),
    );
    let mounted = super::mount(app.clone());

    let resp = mounted
        .clone()
        .oneshot(
            Request::builder().uri("/health").body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(body, "ok", "既有路由行为必须零变化");

    let resp = mounted
        .oneshot(Request::builder().uri("/nope").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "未匹配路径仍 404");

    // 未挂载的对照:行为一致(no-op 语义)
    let resp = app
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
