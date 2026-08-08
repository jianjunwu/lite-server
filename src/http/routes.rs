use crate::http::handlers::*;
use crate::http::state::AppState;
use axum::{
    Router,
    extract::{Request, State},
    middleware::Next,
    response::Response,
    routing::{delete, get, post, put},
};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Resolve the (model, version) an inference-path request targets, or None
/// for non-model paths and model-scoped admin leaves (ready/health/routing/
/// activate/compare/reload/versions CRUD). Custom @route tails resolve to the
/// active version — the fallback handler does the real matching.
pub(crate) fn access_log_target(path: &str) -> Option<(&str, Option<&str>)> {
    let rest = path.strip_prefix("/v2/models/")?;
    let mut segs = rest.split('/');
    let model = segs.next()?;
    if model.is_empty() {
        return None;
    }
    let segs: Vec<&str> = segs.collect();
    match segs.as_slice() {
        [] => None,
        ["infer" | "events" | "stream" | "bidi" | "decoupled" | "decoupled-stream" | "generate" | "generate_stream"] => Some((model, None)),
        ["versions", v, "infer" | "events" | "stream" | "bidi" | "decoupled" | "decoupled-stream" | "generate" | "generate_stream"] if !v.is_empty() => Some((model, Some(v))),
        ["versions", ..] => None,
        // B1 audit fix: bare `reload` is a registered state-changing ADMIN route
        // (POST /v2/models/:m/reload → reload_model_handler, no enforce_auth).
        // It MUST be in this exclusion list, else it falls to the custom-@route
        // `_` arm → Some → classify_http_path returns Inference → unconfigured
        // inference is public → remote unauthenticated reload (DoS). The
        // versioned form is already Admin via the `["versions", ..]` arm.
        ["ready" | "health" | "routing" | "activate" | "compare" | "reload"] => None,
        // Anything else under /v2/models/{m}/ is a custom @route tail.
        _ => Some((model, None)),
    }
}

/// Per-model access log (policies.request_log): logs method, path, status and
/// elapsed time for inference-path requests — including rejections, mirroring
/// the retired Python LogRequests callback.
async fn access_log_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let Some((model, version)) = access_log_target(request.uri().path()) else {
        return next.run(request).await;
    };
    let resolved = match version {
        Some(v) => Some(v.to_string()),
        None => state.registry.get_active_version(model),
    };
    let enabled = resolved
        .as_deref()
        .and_then(|v| state.registry.get(model, Some(v)))
        .map(|mv| mv.policies.request_log.is_some())
        .unwrap_or(false);
    if !enabled {
        return next.run(request).await;
    }
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let model = model.to_string();
    // P-XFF (评审 1.2): log the cleansed client_ip (the rate-limit key source)
    // alongside the raw XFF (truncated) so a mis-attributed limit / forged-XFF
    // attempt is traceable back to its origin hop.
    let client_ip = request
        .extensions()
        .get::<crate::request_context::RequestContext>()
        .map(|cx| cx.client_ip.clone())
        .unwrap_or_default();
    let raw_xff = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().chars().take(64).collect::<String>());
    let start = Instant::now();
    let response = next.run(request).await;
    info!(
        model = %model,
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        client_ip = %client_ip,
        xff = ?raw_xff,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "access"
    );
    response
}

pub fn create_routes(shared: Arc<AppState>) -> Router {
    let mut router = Router::new();

    // Built-in health probes (phase 3) — fixed HTTP-layer endpoints, not
    // overridable by @route declarations.
    router = router
        .route("/health", get(health_handler))
        .route("/livez", get(livez_handler))
        .route("/readyz", get(readyz_handler))
        .route("/startupz", get(startupz_handler));

    router = router
        // Info
        .route("/info", get(info_handler))
        // Metrics
        .route("/metrics", get(metrics_handler))
        // KServe V2 管理面(阶段 3,批次 3,D8):server metadata / health 别名 /
        // 规范形状模型元数据 / bare load alias(G14)。注册前这些路径落
        // route_fallback(404),注册后行为变化——G17 回归测试锁定。
        .route("/v2", get(v2_server_metadata_handler))
        .route("/v2/health/live", get(livez_handler))
        .route("/v2/health/ready", get(readyz_handler))
        .route("/v2/models/:model_name", get(model_metadata_handler))
        .route("/v2/models/:model_name/versions/:version", get(model_metadata_version_handler))
        // Admin: list models
        .route("/v2/models", get(list_models_handler))
        // Admin: list versions
        .route("/v2/models/:model_name/versions", get(list_versions_handler))
        // Admin: model ready check
        .route("/v2/models/:model_name/ready", get(model_ready_handler))
        .route("/v2/models/:model_name/versions/:version/ready", get(model_ready_version_handler))
        // Admin: model health (per-worker status)
        .route("/v2/models/:model_name/health", get(model_health_handler))
        .route("/v2/models/:model_name/versions/:version/health", get(model_health_version_handler))
        // Admin: repository index
        .route("/v2/repository/index", post(repository_index_handler))
        // Admin: load (bare aliases the active version since G14/批次 3;
        // §4.4 删除理由——静默默认 version 1——由 alias 解析 active 取代,
        // 不恢复静默默认)
        .route("/v2/repository/models/:model_name/load", post(bare_load_model_handler))
        .route("/v2/repository/models/:model_name/versions/:version/load", post(load_model_handler))
        // Admin: unload (bare = active version; versioned = explicit)
        .route("/v2/repository/models/:model_name/unload", post(unload_model_handler))
        .route("/v2/repository/models/:model_name/versions/:version/unload", post(unload_version_handler))
        // Admin: reload (bare = active version; versioned = explicit)
        .route("/v2/models/:model_name/reload", post(reload_model_handler))
        .route("/v2/models/:model_name/versions/:version/reload", post(reload_version_handler))
        // Admin: upload/download/list files
        .route("/v2/repository/models/:model_name/versions/:version/upload", post(upload_model_handler))
        .route("/v2/repository/models/:model_name/versions/:version/download", get(download_model_handler))
        .route("/v2/repository/models/:model_name/versions/:version/files", get(list_files_handler))
        // Admin: delete version
        .route("/v2/models/:model_name/versions/:version", delete(delete_version_handler))
        // Admin: activate version
        .route("/v2/models/:model_name/versions/:version/activate", post(activate_version_handler))
        // Admin: weighted/canary routing weights (§4.3)
        .route("/v2/models/:model_name/routing", put(set_routing_handler))
        // Inference (P-CORS: preflight handled by cors_middleware, no per-route .options())
        .route("/v2/models/:model_name/infer", post(infer_handler))
        .route("/v2/models/:model_name/versions/:version/infer", post(infer_version_handler))
        // 批次 4:/generate unary 别名,无 gate(J3:unary 即 infer 别名)
        .route("/v2/models/:model_name/generate", post(generate_handler))
        .route("/v2/models/:model_name/versions/:version/generate", post(generate_version_handler));

    // Feature-gated routes: mounted only when their toggle is on, so a disabled
    // feature 404s at the router. Handlers are untouched — tests that build
    // their own Router against a handler keep working regardless of the toggle.
    let features = &shared.config.features;
    if features.timeline {
        router = router
            .route("/metrics/timeline", get(timeline_handler))
            .route("/metrics/timeline/:model_name", get(timeline_model_handler))
            .route("/metrics/timeline/:model_name/versions/:version", get(timeline_model_version_handler));
    }
    if features.alerts {
        router = router.route("/metrics/alerts", get(alerts_handler));
    }
    if features.version_compare {
        router = router.route("/v2/models/:model_name/compare", get(compare_versions_handler));
    }
    // `streaming` is the master switch for the two streaming transports; each
    // transport also has its own toggle, so e.g. sse=false unmounts SSE while
    // WS keeps flowing (as long as streaming + websocket_streaming are on).
    if features.streaming && features.sse {
        router = router
            .route("/v2/models/:model_name/events", post(sse_infer_handler))
            .route("/v2/models/:model_name/versions/:version/events", post(sse_infer_version_handler))
            // 批次 4:/generate_stream 随同一开关族(J3,与 /events 同批挂载)
            .route("/v2/models/:model_name/generate_stream", post(generate_stream_handler))
            .route("/v2/models/:model_name/versions/:version/generate_stream", post(generate_stream_version_handler));
    }
    if features.streaming && features.sse && features.decoupled {
        router = router
            .route("/v2/models/:model_name/decoupled", post(sse_decoupled_handler))
            .route(
                "/v2/models/:model_name/versions/:version/decoupled",
                post(sse_decoupled_version_handler),
            );
    }
    if features.streaming && features.websocket_streaming {
        router = router
            .route("/v2/models/:model_name/stream", get(ws_stream_handler))
            .route("/v2/models/:model_name/versions/:version/stream", get(ws_stream_version_handler));
    }
    if features.streaming && features.websocket_streaming && features.decoupled {
        router = router
            .route(
                "/v2/models/:model_name/decoupled-stream",
                get(ws_decoupled_handler),
            )
            .route(
                "/v2/models/:model_name/versions/:version/decoupled-stream",
                get(ws_decoupled_version_handler),
            );
    }
    // D3: h2 bidi endpoints — gated on streaming + http_bidi (default true).
    if features.streaming && features.http_bidi {
        router = router
            .route("/v2/models/:model_name/bidi", post(h2_bidi_handler))
            .route("/v2/models/:model_name/versions/:version/bidi", post(h2_bidi_version_handler));
    }
    // Custom @route dispatch (phase 2) is handled by the fallback handler
    // (see http::route_fallback): exact system leaves above are matched by
    // axum; any other `/v2/models/:m/<tail>` path falls through to it and is
    // matched against the model version's declared routes. (A catch-all
    // `/{*tail}` route is rejected by matchit because `:model_name` already
    // has deeper registered children.)

    // D11 P2.2:协议路由模块经 mount 注册(阶段 2 空表,no-op;openai-compact
    // 批次 5 在此挂载)。须在 with_state 之前 merge——Router 此时无状态,
    // 挂载路由与既有系统路由共享 fallback。
    router = crate::protocol::mount(router);

    // Standardized error bodies for unmatched routes (404) and
    // unmatched methods (405) — axum defaults are empty/plain-text.
    router = router
        .fallback(crate::http::route_fallback)
        .method_not_allowed_fallback(crate::http::method_not_allowed_fallback);

    // 批次 5(openai-compact):/v1 路由注册(with_state 之前,与协议 seam
    // mount 同点;handler 的 State 由外层 with_state 统一绑定)。
    router = crate::http::handlers::openai_compact::mount(router);

    router
        .layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            access_log_middleware,
        ))
        .with_state(shared)
}

#[cfg(test)]
mod tests {
    use super::access_log_target;

    #[test]
    fn test_access_log_target_inference_endpoints() {
        assert_eq!(access_log_target("/v2/models/m/infer"), Some(("m", None)));
        assert_eq!(access_log_target("/v2/models/m/events"), Some(("m", None)));
        assert_eq!(access_log_target("/v2/models/m/stream"), Some(("m", None)));
        assert_eq!(access_log_target("/v2/models/m/bidi"), Some(("m", None)));
        assert_eq!(access_log_target("/v2/models/m/decoupled"), Some(("m", None)));
        assert_eq!(access_log_target("/v2/models/m/decoupled-stream"), Some(("m", None)));
        assert_eq!(
            access_log_target("/v2/models/m/versions/2/infer"),
            Some(("m", Some("2")))
        );
        assert_eq!(
            access_log_target("/v2/models/m/versions/2/events"),
            Some(("m", Some("2")))
        );
        assert_eq!(
            access_log_target("/v2/models/m/versions/2/bidi"),
            Some(("m", Some("2")))
        );
        assert_eq!(
            access_log_target("/v2/models/m/versions/2/decoupled"),
            Some(("m", Some("2")))
        );
        assert_eq!(
            access_log_target("/v2/models/m/versions/2/decoupled-stream"),
            Some(("m", Some("2")))
        );
    }

    #[test]
    fn test_access_log_target_skips_admin_and_non_model_paths() {
        assert_eq!(access_log_target("/v2/models/m/ready"), None);
        assert_eq!(access_log_target("/v2/models/m/health"), None);
        assert_eq!(access_log_target("/v2/models/m/routing"), None);
        assert_eq!(access_log_target("/v2/models/m/versions"), None);
        assert_eq!(access_log_target("/v2/models/m/versions/2/ready"), None);
        assert_eq!(access_log_target("/v2/repository/index"), None);
        assert_eq!(access_log_target("/health"), None);
        assert_eq!(access_log_target("/v2/models/"), None);
        // 批次 3 新增路由:全部 admin 类,不落 inference 日志
        assert_eq!(access_log_target("/v2"), None);
        assert_eq!(access_log_target("/v2/health/live"), None);
        assert_eq!(access_log_target("/v2/health/ready"), None);
        assert_eq!(access_log_target("/v2/models/m"), None); // 模型元数据
        assert_eq!(access_log_target("/v2/models/m/versions/2"), None); // versioned 元数据
        assert_eq!(access_log_target("/v2/repository/models/m/load"), None); // bare load
        // 批次 4:generate 归 inference 族(同 /events 流式日志)
        assert_eq!(access_log_target("/v2/models/m/generate"), Some(("m", None)));
        assert_eq!(access_log_target("/v2/models/m/generate_stream"), Some(("m", None)));
        assert_eq!(
            access_log_target("/v2/models/m/versions/2/generate_stream"),
            Some(("m", Some("2")))
        );
    }

    #[test]
    fn test_access_log_target_custom_route_tails() {
        // Custom @route declarations fall through to the fallback handler;
        // they belong to the model pipeline and are logged (active version).
        assert_eq!(access_log_target("/v2/models/m/summarize"), Some(("m", None)));
        assert_eq!(access_log_target("/v2/models/m/a/b"), Some(("m", None)));
    }
}
