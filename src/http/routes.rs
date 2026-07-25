use crate::http::handlers::*;
use crate::http::state::AppState;
use axum::{
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;

pub fn create_routes(state: AppState) -> Router {
    let mut router = Router::new();

    router = router.route("/health", get(health_handler));

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
    // Custom @route dispatch (phase 2) is handled by the fallback handler
    // (see http::route_fallback): exact system leaves above are matched by
    // axum; any other `/v2/models/:m/<tail>` path falls through to it and is
    // matched against the model version's declared routes. (A catch-all
    // `/{*tail}` route is rejected by matchit because `:model_name` already
    // has deeper registered children.)

    // Standardized error bodies for unmatched routes (404) and
    // unmatched methods (405) — axum defaults are empty/plain-text.
    router = router
        .fallback(crate::http::route_fallback)
        .method_not_allowed_fallback(crate::http::method_not_allowed_fallback);

    router.with_state(Arc::new(state))
}
