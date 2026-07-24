use crate::http::handlers::*;
use crate::http::state::AppState;
use crate::worker::protocol::EndpointRoute;
use axum::{
    routing::{delete, get, head, options, patch, post, put},
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

/// Drop custom endpoints whose route collides with a system route. axum
/// panics when the same path+method is registered twice, so a conflicting
/// endpoint would crash startup; warn and skip it instead.
fn filter_system_route_conflicts(routes: Vec<EndpointRoute>) -> Vec<EndpointRoute> {
    routes
        .into_iter()
        .filter(|ep| {
            if is_system_route(&ep.route) {
                tracing::warn!(
                    "custom endpoint '{}' conflicts with a system route — skipped",
                    ep.route
                );
                false
            } else {
                true
            }
        })
        .collect()
}

pub fn create_routes(state: AppState, endpoint_routes: Vec<EndpointRoute>) -> Router {
    let endpoint_routes = filter_system_route_conflicts(endpoint_routes);

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
        .route("/v2/models/:model_name/infer", post(infer_handler).options(inference_options_handler))
        .route("/v2/models/:model_name/versions/:version/infer", post(infer_version_handler).options(inference_options_handler))
        // SSE streaming
        .route("/v2/models/:model_name/events", post(sse_infer_handler).options(inference_options_handler))
        .route("/v2/models/:model_name/versions/:version/events", post(sse_infer_version_handler).options(inference_options_handler))
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
        let route = convert_path_params(&ep.route);
        for method in &ep.methods {
            let method_upper = method.to_uppercase();
            router = match method_upper.as_str() {
                "GET" => router.route(&route, get(custom_endpoint_handler)),
                "POST" => router.route(&route, post(custom_endpoint_handler)),
                "PUT" => router.route(&route, put(custom_endpoint_handler)),
                "PATCH" => router.route(&route, patch(custom_endpoint_handler)),
                "DELETE" => router.route(&route, delete(custom_endpoint_handler)),
                "HEAD" => router.route(&route, head(custom_endpoint_handler)),
                "OPTIONS" => router.route(&route, options(custom_endpoint_handler)),
                other => {
                    tracing::warn!(
                        route = %route,
                        method = %other,
                        "unsupported endpoint method — skipped"
                    );
                    router
                }
            };
        }
        // OPTIONS preflight is answered at the Rust layer with a RUNTIME
        // policy lookup, so an endpoint restart that changes a Cors
        // declaration takes effect without re-registering routes. Register
        // OPTIONS for every endpoint (the handler returns 405 when no Cors
        // policy is declared), unless the endpoint declares OPTIONS itself.
        let declares_options = ep
            .methods
            .iter()
            .any(|m| m.eq_ignore_ascii_case("OPTIONS"));
        if !declares_options {
            router = router.route(&route, options(endpoint_options_handler));
        }
    }

    // Standardized error bodies for unmatched routes (404) and
    // unmatched methods (405) — axum defaults are empty/plain-text.
    router = router
        .fallback(crate::http::route_fallback)
        .method_not_allowed_fallback(crate::http::method_not_allowed_fallback);

    router.with_state(Arc::new(state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::protocol::EndpointRoute;

    fn ep(route: &str) -> EndpointRoute {
        EndpointRoute {
            route: route.to_string(),
            methods: vec!["GET".to_string()],
            rate_limit: None,
            cors: None,
        }
    }

    #[test]
    fn test_filter_system_route_conflicts() {
        let routes = vec![
            ep("/v2/models"),       // exact system route
            ep("/v2/models/index"), // system-route prefix
            ep("/ok"),              // custom, kept
            ep("/health"),          // not a system route — kept
        ];
        let kept = filter_system_route_conflicts(routes);
        let kept_routes: Vec<String> = kept.into_iter().map(|r| r.route).collect();
        assert_eq!(kept_routes, vec!["/ok".to_string(), "/health".to_string()]);
    }
}
