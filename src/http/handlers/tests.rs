use super::inference::resolve_version;
use super::*;
use crate::http::state::AppState;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod version_routing_tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::registry::types::ModelType;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use axum::http::{HeaderName, HeaderValue, Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        test_state_with(Config::default())
    }

    /// P5-2 (蓝图 §4.4): features.canary_override=true 的 state —— x-lite-version
    /// pin 仅在开关开时生效。
    fn test_state_canary() -> Arc<AppState> {
        let mut config = Config::default();
        config.features.canary_override = true;
        test_state_with(config)
    }

    fn test_state_with(config: Config) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            config,
            std::path::PathBuf::new(),
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    fn register_ready(state: &AppState, model: &str, versions: &[&str]) {
        for v in versions {
            state
                .registry
                .register(model, v, Default::default(), ModelType::LitAPI, std::path::PathBuf::new())
                .unwrap();
            state.registry.mark_ready(model, v).unwrap();
        }
    }

    /// C3 (P4-2): /readyz and /livez must flip to 503 the moment the draining
    /// flag is set, so the LB摘流 at the start of graceful shutdown.
    #[tokio::test]
    async fn readyz_and_livez_go_503_when_draining() {
        use std::sync::atomic::Ordering;
        let state = test_state();
        register_ready(&state, "m", &["1"]);
        // Not draining → 200.
        assert_eq!(
            readyz_handler(axum::extract::State(state.clone()))
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            livez_handler(axum::extract::State(state.clone()))
                .await
                .status(),
            StatusCode::OK
        );
        // Draining → 503.
        state.draining.store(true, Ordering::Relaxed);
        assert_eq!(
            readyz_handler(axum::extract::State(state.clone()))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            livez_handler(axum::extract::State(state.clone()))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    fn pinned_header(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-lite-version"),
            HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[tokio::test]
    async fn explicit_version_wins_over_header_and_weights() {
        let state = test_state_canary();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        let (v, pin) = resolve_version(&state, "m", Some("1".into()), &pinned_header("2"))
            .await
            .unwrap();
        assert_eq!(v, "1");
        assert_eq!(pin, None, "explicit URL version must not honor the pin");
    }

    #[tokio::test]
    async fn header_pin_wins_over_weights() {
        let state = test_state_canary();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        let (v, pin) = resolve_version(&state, "m", None, &pinned_header("1"))
            .await
            .unwrap();
        assert_eq!(v, "1");
        assert_eq!(pin.as_deref(), Some("1"), "honored pin must be returned");
    }

    // ===== P5-2: features.canary_override 开关门控（蓝图 §4.4, D16）=====

    #[tokio::test]
    async fn switch_off_ignores_pin_and_uses_weights() {
        // canary_override 默认 false：pin header 被忽略，权重路由决定版本。
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        let (v, pin) = resolve_version(&state, "m", None, &pinned_header("1"))
            .await
            .unwrap();
        assert_eq!(v, "2", "switch off → pin ignored, weights decide");
        assert_eq!(pin, None, "ignored pin must not be returned");
    }

    #[tokio::test]
    async fn switch_off_ignores_invalid_pin_without_error() {
        // 开关关时 pin 完全不参与解析——非法值也不校验、不报 400。
        let state = test_state();
        register_ready(&state, "m", &["1"]);
        state.registry.activate_version("m", "1").unwrap();
        let (v, pin) = resolve_version(&state, "m", None, &pinned_header("a/b"))
            .await
            .unwrap();
        assert_eq!(v, "1");
        assert_eq!(pin, None, "ignored pin must not be returned");
    }

    #[tokio::test]
    async fn switch_on_pin_to_unknown_version_is_not_found() {
        // 开关开 + pin 指向未注册版本 → 404（蓝图 §4.4：版本不存在→NotFound）。
        let state = test_state_canary();
        register_ready(&state, "m", &["1"]);
        state.registry.activate_version("m", "1").unwrap();
        let err = resolve_version(&state, "m", None, &pinned_header("2"))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::ModelNotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn weights_pick_when_no_explicit_or_header() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        let (v, pin) = resolve_version(&state, "m", None, &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(v, "2");
        assert_eq!(pin, None);
    }

    #[tokio::test]
    async fn active_fallback_when_no_weights() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();
        let (v, pin) = resolve_version(&state, "m", None, &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(v, "1");
        assert_eq!(pin, None);
    }

    #[tokio::test]
    async fn no_active_no_weights_is_not_found() {
        let state = test_state();
        register_ready(&state, "m", &["1"]);
        let err = resolve_version(&state, "m", None, &HeaderMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::ModelNotFound(_)), "got {err:?}");
    }

    // ===== B4: x-lite-version header must pass validate_version =====

    /// Regression for B4: the header pin used to reach downstream lookups
    /// unvalidated, while versioned URL paths are guarded by
    /// `validate_version`. Invalid header values are rejected (400), same
    /// as invalid path versions. (Values with control chars can't appear
    /// in a HeaderValue at all, so only representable cases are tested.)
    /// P5-2: 校验仅在 features.canary_override=true 时进行（开关关→pin 整体忽略）。
    #[tokio::test]
    async fn invalid_header_pin_is_rejected() {
        let state = test_state_canary();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();

        for bad in ["a/b", "a b", "a..b", ".hidden", "trailing.", &"x".repeat(65)] {
            let err = resolve_version(&state, "m", None, &pinned_header(bad))
                .await
                .unwrap_err();
            assert!(
                matches!(err, AppError::Validation(_)),
                "header pin {bad:?} must be rejected, got {err:?}"
            );
        }

        // Sanity: a valid pin still resolves.
        let (v, pin) = resolve_version(&state, "m", None, &pinned_header("2"))
            .await
            .unwrap();
        assert_eq!(v, "2");
        assert_eq!(pin.as_deref(), Some("2"));
    }

    // ===== PUT /v2/models/:m/routing (§4.3) =====

    fn routing_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v2/models/:model_name/routing", axum::routing::put(set_routing_handler))
            .with_state(state)
    }

    async fn put_routing(app: Router, model: &str, body: &str) -> axum::response::Response {
        app.oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/models/{}/routing", model))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn put_routing_sets_weights_atomically() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);

        let resp = put_routing(routing_router(state.clone()), "m", r#"{"weights":{"1":90,"2":10}}"#).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 90);
        assert_eq!(state.registry.get("m", Some("2")).unwrap().weight, 10);

        // Atomic full-set: unlisted versions are zeroed.
        let resp = put_routing(routing_router(state.clone()), "m", r#"{"weights":{"2":50}}"#).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 0);
        assert_eq!(state.registry.get("m", Some("2")).unwrap().weight, 50);
    }

    #[tokio::test]
    async fn put_routing_unknown_version_is_400_and_untouched() {
        let state = test_state();
        register_ready(&state, "m", &["1"]);

        let resp = put_routing(routing_router(state.clone()), "m", r#"{"weights":{"nope":100}}"#).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 0);

        // Unknown model → 404.
        let resp = put_routing(routing_router(state.clone()), "nope", r#"{"weights":{}}"#).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn activate_hard_switches_weights() {
        // Explicit activate = hard cutover (§4.3): target gets weight 100,
        // every other version 0.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions/:version/activate",
                axum::routing::post(activate_version_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/models/m/versions/2/activate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.registry.get_active_version("m").as_deref(), Some("2"));
        assert_eq!(state.registry.get("m", Some("1")).unwrap().weight, 0);
        assert_eq!(state.registry.get("m", Some("2")).unwrap().weight, 100);
    }

    // ===== §4.4: bare vs versioned resolution =====

    #[tokio::test]
    async fn bare_unload_targets_active_not_weighted_pick() {
        // Admin ops on the bare path always target the active version (§4.4
        // decision) — never the routing pick, even at 100% weight.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/unload",
                axum::routing::post(unload_model_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/m/unload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(state.registry.get("m", Some("1")).is_none(), "active v1 must be unloaded");
        assert!(state.registry.get("m", Some("2")).is_some(), "weighted v2 must be untouched");
    }

    #[tokio::test]
    async fn versioned_unload_targets_explicit_version() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/versions/:version/unload",
                axum::routing::post(unload_version_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/m/versions/2/unload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(state.registry.get("m", Some("1")).is_some(), "active v1 untouched");
        assert!(state.registry.get("m", Some("2")).is_none(), "explicit v2 unloaded");
    }

    #[tokio::test]
    async fn bare_health_uses_routing_pick() {
        // Traffic-facing bare endpoints resolve via routing (§4.3) — unlike
        // admin ops.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state
            .registry
            .set_weights("m", &HashMap::from([("2".into(), 100u32)]))
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();

        let app = Router::new()
            .route("/v2/models/:model_name/health", axum::routing::get(model_health_handler))
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/v2/models/m/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["version"], "2", "bare health must follow the routing pick");
    }

    #[tokio::test]
    async fn versioned_ready_reports_explicit_version() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "2").unwrap();

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions/:version/ready",
                axum::routing::get(model_ready_version_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/v2/models/m/versions/1/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["version"], "1");
        assert_eq!(json["ready"], true);
        assert_eq!(json["active_version"], "2");
    }

    #[tokio::test]
    async fn bare_timeline_defaults_to_active_not_1() {
        // The old default was the literal string "1" regardless of what is
        // actually active (§4.0 bug list).
        let state = test_state();
        register_ready(&state, "m", &["2"]);
        state.registry.activate_version("m", "2").unwrap();

        let app = Router::new()
            .route("/metrics/timeline/:model_name", axum::routing::get(timeline_model_handler))
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/metrics/timeline/m").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["version"], "2", "bare timeline must default to the active version");
    }

    // ===== M3: step downsampling + coverage headers =====

    async fn get(app: Router, uri: &str) -> axum::response::Response {
        app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn timeline_step_zero_is_rejected() {
        let app = Router::new().route("/metrics/timeline", axum::routing::get(timeline_handler));
        let resp = get(app, "/metrics/timeline?step=0").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "step=0 must be rejected");
    }

    #[tokio::test]
    async fn timeline_versioned_step_zero_is_rejected() {
        let app = Router::new().route(
            "/metrics/timeline/:model_name/versions/:version",
            axum::routing::get(timeline_model_version_handler),
        );
        let resp = get(app, "/metrics/timeline/m/versions/1?step=0").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn timeline_reports_coverage_and_downsamples() {
        use crate::metrics::aggregator::TIMELINE;
        crate::metrics::prometheus::register_metrics().ok();
        // One sample is enough: the assertions are count-relative.
        TIMELINE.sample("m3_step", "1").await;
        let raw_count = TIMELINE.get_timeline("m3_step", "1").await.len();
        assert!(raw_count >= 1);

        let app = Router::new().route("/metrics/timeline", axum::routing::get(timeline_handler));

        // Coverage headers describe the retention window.
        let resp = get(app.clone(), "/metrics/timeline").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("x-timeline-coverage").unwrap(),
            &TIMELINE.coverage_secs().to_string(),
        );
        assert_eq!(
            resp.headers().get("x-timeline-interval").unwrap(),
            &TIMELINE.sample_interval_secs().to_string(),
        );
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let full = json["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["model"] == "m3_step")
            .unwrap();
        assert_eq!(full["entries"].as_array().unwrap().len(), raw_count);

        // A step beyond the point count keeps exactly the latest point.
        let resp = get(app, "/metrics/timeline?step=100000").await;
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let sampled = json["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["model"] == "m3_step")
            .unwrap();
        let entries = sampled["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "oversized step must keep the latest point");
        assert_eq!(entries[0]["timestamp"], full["entries"].as_array().unwrap().last().unwrap()["timestamp"]);

        TIMELINE.remove("m3_step", "1").await;
    }

    // ===== §4.5: multi-version health =====

    #[tokio::test]
    async fn versions_endpoint_returns_multi_version_overview() {
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();
        state
            .registry
            .set_weights("m", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions",
                axum::routing::get(list_versions_handler),
            )
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/v2/models/m/versions").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["name"], "m");
        assert_eq!(json["active_version"], "1");
        let versions = json["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);

        let v1 = versions.iter().find(|v| v["version"] == "1").unwrap();
        assert_eq!(v1["active"], true);
        assert_eq!(v1["status"], "ready");
        assert_eq!(v1["weight"], 90);
        assert_eq!(v1["workers"]["total"], 0);
        assert_eq!(v1["workers"]["ready"], 0);
        assert!(v1["loaded_at"].as_u64().is_some(), "loaded_at must be epoch secs");

        let v2 = versions.iter().find(|v| v["version"] == "2").unwrap();
        assert_eq!(v2["active"], false);
        assert_eq!(v2["weight"], 10);
    }

    #[tokio::test]
    async fn server_health_groups_versions_by_model() {
        // §4.5: /health nests per-version entries under their model with the
        // active_version pointer.
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "2").unwrap();

        let app = Router::new()
            .route("/health", axum::routing::get(health_handler))
            .with_state(state.clone());
        let resp = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "ready");
        let models = json["models"].as_array().unwrap();
        assert_eq!(models.len(), 1, "one model groups both versions");
        let m = &models[0];
        assert_eq!(m["name"], "m");
        assert_eq!(m["active_version"], "2");
        let versions = m["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions.iter().all(|v| v["status"] == "ready"));
        assert!(versions.iter().all(|v| v["loaded_at"].as_u64().is_some()));
        assert!(m.get("version").is_none(), "flat per-version fields must be gone");
    }

    #[tokio::test]
    async fn versioned_preflight_uses_hit_version_cors_policy() {
        // P-CORS: a versioned route's preflight must answer with that version's
        // CORS policy, not the active version's. CORS is now a middleware
        // (preflight short-circuits before routing), not a per-route .options().
        use crate::config::{CorsPolicy, ModelPolicies};
        let state = test_state();
        register_ready(&state, "m", &["1", "2"]);
        state.registry.activate_version("m", "1").unwrap();
        let policies = |origin: &str| ModelPolicies {
            cors: Some(CorsPolicy {
                allow_origins: vec![origin.to_string()],
                allow_methods: vec!["POST".to_string()],
                allow_headers: vec!["content-type".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        state.registry.set_policies("m", "1", Some(policies("https://v1.example")));
        state.registry.set_policies("m", "2", Some(policies("https://v2.example")));

        let app = Router::new()
            .route(
                "/v2/models/:model_name/versions/:version/infer",
                axum::routing::post(infer_version_handler),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::http::cors::cors_middleware,
            ))
            .with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/v2/models/m/versions/2/infer")
                    .header("origin", "https://v2.example")
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "https://v2.example",
            "versioned preflight must use the hit version's policy"
        );
    }

    #[tokio::test]
    async fn preflight_with_disallowed_method_gets_no_cors_headers() {
        // 蓝图 P-CORS ⑥：预检仅当 Origin 命中 **且 method/headers 全在清单内**
        // 才附 CORS 头。本测试断言清单外 method（DELETE ∉ allow_methods）的预检
        // 不得附 ACAO。
        use crate::config::{CorsPolicy, ModelPolicies};
        let state = test_state();
        register_ready(&state, "m", &["1"]);
        state.registry.activate_version("m", "1").unwrap();
        state.registry.set_policies(
            "m",
            "1",
            Some(ModelPolicies {
                cors: Some(CorsPolicy {
                    allow_origins: vec!["https://app.example.com".into()],
                    allow_methods: vec!["POST".into()],
                    allow_headers: vec!["content-type".into()],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        let app = Router::new()
            .route(
                "/v2/models/:model_name/infer",
                axum::routing::post(infer_handler),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::http::cors::cors_middleware,
            ))
            .with_state(state.clone());
        // DELETE 不在 allow_methods 清单内 → 蓝图 ⑥ 预检不得附 CORS 头。
        let resp = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/v2/models/m/infer")
                    .header("origin", "https://app.example.com")
                    .header("access-control-request-method", "DELETE")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "蓝图 P-CORS ⑥: allow_methods 清单外的 method 预检不得附 ACAO"
        );
    }

    #[tokio::test]
    async fn no_origin_request_still_carries_vary_origin() {
        // 蓝图 P-CORS ④：`Vary: Origin` 必须始终附加——无 Origin 的响应也可能
        // 被缓存再服务给带 Origin 的请求（缓存正确性）。本测试断言无 Origin
        // 响应仍带 Vary: origin。
        use crate::config::{CorsPolicy, ModelPolicies};
        let state = test_state();
        register_ready(&state, "m", &["1"]);
        state.registry.activate_version("m", "1").unwrap();
        state.registry.set_policies(
            "m",
            "1",
            Some(ModelPolicies {
                cors: Some(CorsPolicy {
                    allow_origins: vec!["https://app.example.com".into()],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        let app = Router::new()
            .route(
                "/v2/models/:model_name/infer",
                axum::routing::post(infer_handler),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::http::cors::cors_middleware,
            ))
            .with_state(state.clone());
        // 同源/非浏览器请求不带 Origin → 蓝图 ④ Vary: Origin 仍须附加。
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/models/m/infer")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let vary = resp
            .headers()
            .get_all("vary")
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(
            vary.iter().any(|v| v == "origin"),
            "蓝图 P-CORS ④: Vary: Origin 必须始终附加（含无 Origin 请求），实际 vary={vary:?}"
        );
    }

    #[tokio::test]
    async fn cors_middleware_no_acao_for_disallowed_origin_on_actual_request() {
        // P-CORS security: an Origin not in the allowlist gets NO ACAO on the
        // actual response (browser blocks); Vary: Origin is still set.
        use crate::config::{CorsPolicy, ModelPolicies};
        let state = test_state();
        register_ready(&state, "m", &["1"]);
        state.registry.activate_version("m", "1").unwrap();
        state.registry.set_policies(
            "m",
            "1",
            Some(ModelPolicies {
                cors: Some(CorsPolicy {
                    allow_origins: vec!["https://app.example.com".into()],
                    allow_methods: vec!["POST".into()],
                    allow_headers: vec!["content-type".into()],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        let app = Router::new()
            .route(
                "/v2/models/:model_name/infer",
                axum::routing::post(infer_handler),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::http::cors::cors_middleware,
            ))
            .with_state(state.clone());
        // Attacker origin — must NOT receive ACAO.
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/models/m/infer")
                    .header("origin", "https://evil.example.com")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "disallowed Origin must not receive ACAO"
        );
        let vary = resp
            .headers()
            .get_all("vary")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect::<Vec<_>>();
        assert!(vary.contains(&"origin"), "Vary: Origin must still be set");
    }

    // ===== B1: WS readiness must check the resolved version =====

    /// Regression for B1: `handle_ws_stream` used to gate on the ACTIVE
    /// version's readiness (`is_ready(model, None)`), so a WS pinned via
    /// `x-lite-version` to a Ready non-active version was closed whenever
    /// the active version was not Ready.
    /// P5-2: pin 生效需 features.canary_override=true。
    #[tokio::test]
    async fn ws_stream_readiness_uses_resolved_version_not_active() {
        use futures::StreamExt;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let state = test_state_canary();
        // v1 = active but Failed; v2 = Ready (the pin target).
        state
            .registry
            .register("m", "1", Default::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        state
            .registry
            .set_status("m", "1", crate::registry::types::VersionStatus::Failed)
            .unwrap();
        state.registry.activate_version("m", "1").unwrap();
        register_ready(&state, "m", &["2"]);

        let app = Router::new()
            .route(
                "/v2/models/:model_name/stream",
                axum::routing::get(ws_stream_handler),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("ws://{addr}/v2/models/m/stream");
        let mut req = url.into_client_request().unwrap();
        req.headers_mut().insert("x-lite-version", "2".parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req)
            .await
            .expect("WS connect failed");

        // The pinned v2 is Ready, so the handler must be waiting for the
        // first client message — NOT closing. Receiving a close frame here
        // means the readiness gate still checks the active (Failed) version.
        let first = tokio::time::timeout(std::time::Duration::from_millis(500), ws.next()).await;
        assert!(
            first.is_err(),
            "server closed a WS pinned to a Ready version — readiness gate used the active version, got {first:?}"
        );
    }
    /// Round2 B5: the AppState alert engine must take its thresholds from
    /// `config.alerts`, not the hardcoded default (legacy default 100/500 —
    /// a queue depth of 7 only fires when the configured warning is lower).
    #[tokio::test]
    async fn alerts_thresholds_come_from_config() {
        let _ = crate::metrics::prometheus::register_metrics();
        let mut config = Config::default();
        config.alerts.queue_depth_warning = 5;
        let state = test_state_with(config);

        let agg = crate::metrics::aggregator::TimelineAggregator::new();
        crate::metrics::prometheus::QUEUE_DEPTH
            .with_label_values(&["b5_model", "1"])
            .set(7.0);
        agg.sample("b5_model", "1").await;

        let alerts = state.alert_engine.evaluate(&agg).await;
        assert!(
            alerts.iter().any(|a| a.rule == "queue_depth" && a.severity == "warning" && a.model == "b5_model"),
            "configured threshold (5) must fire at depth 7, got {alerts:?}"
        );
        crate::metrics::prometheus::QUEUE_DEPTH
            .with_label_values(&["b5_model", "1"])
            .set(0.0);
    }
}

// ===== B3: custom-route callback gap =====

#[cfg(test)]
mod custom_route_callback_tests {
    /// B3 (P1): `dispatch_custom_route` does not fire `on_inference_request`
    /// or `on_inference_response` callbacks, unlike the inference paths
    /// (`do_infer`, `sse_infer_impl`, `handle_ws_stream`).
    ///
    /// The spec states model-level callbacks should cover both inference and
    /// custom routes. Inference paths fire `on_inference_request` before
    /// queueing and `on_inference_response` after; `dispatch_custom_route`
    /// does neither — the callback runner is entirely silent for routes.
    ///
    /// This structural test verifies the source-level gap: the callback
    /// invocations exist in `do_infer` but not in `dispatch_custom_route`.
    #[test]
    fn test_dispatch_custom_route_does_not_fire_inference_callbacks() {
        let source = include_str!("custom_routes.rs");

        // Find the dispatch_custom_route function boundaries.
        let lines: Vec<&str> = source.lines().collect();
        let fn_start = lines
            .iter()
            .position(|l| l.contains("pub async fn dispatch_custom_route("))
            .expect("dispatch_custom_route must exist");

        // Find the matching closing brace (heuristic: next line starting
        // with `^}` at the same indent as `pub async fn`).
        let mut fn_end = fn_start;
        let mut depth = 0i32;
        let mut started = false;
        for (i, line) in lines.iter().enumerate().skip(fn_start) {
            if line.contains('{') {
                depth += line.matches('{').count() as i32;
                started = true;
            }
            if line.contains('}') {
                depth -= line.matches('}').count() as i32;
            }
            if started && depth == 0 {
                fn_end = i;
                break;
            }
        }

        let fn_body: Vec<&&str> = lines[fn_start..=fn_end].iter().collect();

        // The inference path calls fire_inference_request (in do_infer).
        let has_inference_request = source.contains("fire_inference_request");
        assert!(
            has_inference_request,
            "sanity: handlers.rs must reference fire_inference_request somewhere"
        );

        let fn_has_req_cb = fn_body
            .iter()
            .any(|l| l.contains("fire_inference_request"));
        let fn_has_resp_cb = fn_body
            .iter()
            .any(|l| l.contains("fire_inference_response"));

        // B3: The defect — dispatch_custom_route does NOT fire inference
        // callbacks (the spec says model-level callbacks cover both inference
        // and custom routes). These assertions FAIL against current code.
        // When fixed, they will pass.
        assert!(
            fn_has_req_cb,
            "B3: dispatch_custom_route must fire on_inference_request \
             callback. Currently it does not — only do_infer fires it."
        );

        assert!(
            fn_has_resp_cb,
            "B3: dispatch_custom_route must fire on_inference_response \
             callback. Currently it does not — only do_infer fires it."
        );

        // Counter-check: do_infer DOES call both (verify test methodology).
        let infer_source = include_str!("inference.rs");
        let infer_lines: Vec<&str> = infer_source.lines().collect();
        let do_infer_start = infer_lines
            .iter()
            .position(|l| l.contains("async fn do_infer("))
            .expect("do_infer must exist");
        let mut do_infer_end = do_infer_start;
        let mut depth = 0i32;
        let mut started = false;
        for (i, line) in infer_lines.iter().enumerate().skip(do_infer_start) {
            if line.contains('{') {
                depth += line.matches('{').count() as i32;
                started = true;
            }
            if line.contains('}') {
                depth -= line.matches('}').count() as i32;
            }
            if started && depth == 0 {
                do_infer_end = i;
                break;
            }
        }
        let do_infer_body: Vec<&&str> =
            infer_lines[do_infer_start..=do_infer_end].iter().collect();
        assert!(
            do_infer_body.iter().any(|l| l.contains("fire_inference_request")),
            "do_infer must fire fire_inference_request (sanity check)"
        );
        assert!(
            do_infer_body.iter().any(|l| l.contains("fire_inference_response")),
            "do_infer must fire fire_inference_response (sanity check)"
        );
    }



}
