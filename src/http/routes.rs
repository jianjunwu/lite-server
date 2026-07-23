use crate::error::AppError;
use crate::http::handlers::*;
use crate::http::state::AppState;
use crate::worker::protocol::EndpointRoute;
use axum::{
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;

/// System routes that cannot be overridden by custom endpoints.
/// Note: "/health" is intentionally excluded — custom health endpoints are allowed.
const SYSTEM_ROUTES: &[&str] = &[
    "/info",
    "/metrics",
    "/metrics/timeline",
    "/metrics/alerts",
    "/v2/models",
    "/v2/repository",
    "/v2/repository/index",
    "/v2/repository/models",
];

fn is_system_route(route: &str) -> bool {
    SYSTEM_ROUTES.iter().any(|sys| {
        route == *sys || route.starts_with(&format!("{}/", sys))
    })
}

/// Validate that no custom endpoint conflicts with system routes.
pub fn validate_endpoint_routes(endpoint_routes: &[EndpointRoute]) -> Result<(), AppError> {
    for ep in endpoint_routes {
        if is_system_route(&ep.route) {
            return Err(AppError::Config(format!(
                "custom endpoint '{}' conflicts with a system route",
                ep.route
            )));
        }
    }
    Ok(())
}

pub fn create_routes(state: AppState, endpoint_routes: Vec<EndpointRoute>) -> Router {
    // Validate no custom endpoint conflicts with system routes
    if let Err(e) = validate_endpoint_routes(&endpoint_routes) {
        // Log and continue, filtering out conflicting routes
        tracing::warn!("{}", e);
    }

    let has_health_endpoint = endpoint_routes.iter().any(|r| r.route == "/health");

    let mut router = Router::new();

    // Default health (skip if overridden by custom endpoint)
    if !has_health_endpoint {
        router = router.route("/health", get(health_handler));
    }

    router = router
        // Info
        .route("/info", get(info_handler))
        // Metrics
        .route("/metrics", get(metrics_handler))
        // Timeline & Alerts
        .route("/metrics/timeline", get(timeline_handler))
        .route("/metrics/timeline/:model_name", get(timeline_model_handler))
        .route("/metrics/alerts", get(alerts_handler))
        // Version compare
        .route("/v2/models/:model_name/compare", get(compare_versions_handler))
        // Admin: list models
        .route("/v2/models", get(list_models_handler))
        // Admin: list versions
        .route("/v2/models/:model_name/versions", get(list_versions_handler))
        // Admin: model ready check
        .route("/v2/models/:model_name/ready", get(model_ready_handler))
        // Admin: model health (per-worker status)
        .route("/v2/models/:model_name/health", get(model_health_handler))
        // Admin: repository index
        .route("/v2/repository/index", post(repository_index_handler))
        // Admin: load
        .route("/v2/repository/models/:model_name/load", post(load_model_handler))
        // Admin: unload
        .route("/v2/repository/models/:model_name/unload", post(unload_model_handler))
        // Admin: reload
        .route("/v2/models/:model_name/reload", post(reload_model_handler))
        // Admin: upload/download/list files
        .route("/v2/repository/models/:model_name/versions/:version/upload", post(upload_model_handler))
        .route("/v2/repository/models/:model_name/versions/:version/download", get(download_model_handler))
        .route("/v2/repository/models/:model_name/versions/:version/files", get(list_files_handler))
        // Admin: delete version
        .route("/v2/models/:model_name/versions/:version", delete(delete_version_handler))
        // Admin: activate version
        .route("/v2/models/:model_name/versions/:version/activate", post(activate_version_handler))
        // Inference
        .route("/v2/models/:model_name/infer", post(infer_handler))
        .route("/v2/models/:model_name/versions/:version/infer", post(infer_version_handler))
        // SSE streaming
        .route("/v2/models/:model_name/events", post(sse_infer_handler))
        .route("/v2/models/:model_name/versions/:version/events", post(sse_infer_version_handler))
        // WebSocket streaming
        .route("/v2/models/:model_name/stream", get(ws_stream_handler))
        .route("/v2/models/:model_name/versions/:version/stream", get(ws_stream_version_handler));

    /// Convert `{param}` placeholders (from Python decorators) to axum `:param`.
    fn convert_path_params(route: &str) -> String {
        let mut result = String::with_capacity(route.len());
        let mut chars = route.chars();
        while let Some(c) = chars.next() {
            if c == '{' {
                result.push(':');
                for c2 in chars.by_ref() {
                    if c2 == '}' {
                        break;
                    }
                    result.push(c2);
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    // Register custom endpoint routes
    for ep in endpoint_routes {
        for method in ep.methods {
            let route = convert_path_params(&ep.route);
            let method_upper = method.to_uppercase();
            router = match method_upper.as_str() {
                "GET" => router.route(&route, get(custom_endpoint_handler)),
                "POST" => router.route(&route, post(custom_endpoint_handler)),
                "DELETE" => router.route(&route, delete(custom_endpoint_handler)),
                _ => router.route(&route, get(custom_endpoint_handler)),
            };
        }
    }

    // Standardized error bodies for unmatched routes (404) and
    // unmatched methods (405) — axum defaults are empty/plain-text.
    router = router
        .fallback(crate::http::route_fallback)
        .method_not_allowed_fallback(crate::http::method_not_allowed_fallback);

    router.with_state(Arc::new(state))
}
