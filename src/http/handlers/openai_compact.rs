//! openai-compact(阶段 6,批次 5,J5/J6/C19):OpenAI 兼容紧凑子集。
//!
//! 5 端点(根 `/v1`):`/v1/chat/completions` + `/v1/completions` +
//! `/v1/embeddings` + `/v1/models` + `/v1/models/{model}`。SSE 流式
//! (`data: {json}` + `data: [DONE]`),v2 infer 为内部底座。
//!
//! **翻译层在 worker 侧**(J6):server 薄透传——body 最小解析仅
//! `model`/`stream` 两字段用于路由与分流;chat 请求解析 /
//! completion·chunk·embeddings 构造全部进 Python helper
//! `lite_server/helpers/openai.py`(worker 作者集成)。流式臂复用 SSE 管线
//! (`stream::openai_stream_entry`,帧封装 = `SseFrameStyle::Openai`:鉴权/
//! 限流/cancel/指标/回调随管线继承);unary 臂复用 `run_infer`。经协议层
//! seam 接入:1 handler 模块 + routes.rs 一行挂载 + `ApiProtocol::OpenaiCompact`
//! 一个 arm(复用 openai.rs renderer),核心逻辑 / error.rs / inference.rs /
//! stream.rs / worker 零改动。
//!
//! **专属鉴权门**(2026-08-09 方案):`openai_compact.auth` 配置的
//! `Authorization: Bearer <key>` 校验在 3 个 handler 入口各调一次
//! (`check_openai_gate`)——/v1 5 端点全数覆盖,v2/gRPC/自定义路由/admin
//! 零影响,无 loopback 豁免。与 per-model `policies.auth` 独立,同时配置
//! 时 AND 叠加。选用 handler 内检查而非 route_layer:axum 0.7.9 的
//! route_layer 只遍历 **调用时 path_router 已注册的显式路由**逐个套 layer
//! (源码 routing/mod.rs:307 + path_router.rs:277;fallback_router 不受
//! 影响),而 mount 在 create_routes 链式组装中途被调用——/health、
//! /livez、/v2 全套系统路由均已注册,门被套上全部路径,泄漏到 /v2 与
//! /health(实测 401)。handler 检查作用域天然精确(与 per-model
//! enforce_auth 同风格)。

use super::inference::run_infer;
use super::{ApiBody, RequestBody};
use crate::error::{AppError, ProtocolError};
use crate::http::state::AppState;
use crate::request_context::RequestContext;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{Json, Response},
};
use serde_json::{json, Value};
use std::sync::Arc;

/// 挂载 5 条 /v1 路由。handler 经 `State<Arc<AppState>>` extractor 取状态,
/// route 注册时 S 由 handler 推断为 `Arc<AppState>`(与既有 infer/admin
/// handler 同一模式);外层 `.with_state(shared)` 的 S2 由 create_routes
/// 返回签名约束为 `()`,运行时 state 已擦除进 router 内部。
pub fn mount(router: axum::Router<Arc<AppState>>) -> axum::Router<Arc<AppState>> {
    router
        .route("/v1/chat/completions", axum::routing::post(openai_infer_handler))
        .route("/v1/completions", axum::routing::post(openai_infer_handler))
        .route("/v1/embeddings", axum::routing::post(openai_infer_handler))
        .route("/v1/models", axum::routing::get(v1_models_handler))
        .route("/v1/models/:model", axum::routing::get(v1_model_retrieve_handler))
}

/// /v1 专属鉴权门(openai_compact.auth):handler 入口调用。未配置 → 直通
/// (现状公开,零行为变化);未过 → 401 经协议层渲染(OpenAI 形状,与
/// per-model enforce_auth 的 401 同形状同风格)。缺 header 与错 key 区分
/// 文案,不落密钥本体。
fn check_openai_gate(state: &AppState, headers: &HeaderMap) -> Result<(), AppError> {
    let Some(gate) = state.openai_auth.as_ref() else {
        return Ok(());
    };
    if gate.check(headers) {
        return Ok(());
    }
    let missing = headers
        .get(gate.header_name())
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().is_empty())
        .unwrap_or(true);
    Err(AppError::Unauthorized(if missing {
        format!("missing API key (header: {})", gate.header_name())
    } else {
        format!("invalid API key (header: {})", gate.header_name())
    }))
}

/// body 最小解析(C19):仅 `model`/`stream` 两字段用于路由与分流。
/// 缺失/非法 model → 400(OpenAI 形状,经协议层分派)。
#[derive(Debug)]
struct OpenAiRoute {
    model: String,
    stream: bool,
}

fn parse_route(body: &RequestBody) -> Result<OpenAiRoute, AppError> {
    let bytes = match body {
        RequestBody::Json(b) => b.as_ref(),
        RequestBody::TritonBinary { body, json_head_len } => &body[..*json_head_len],
        RequestBody::Raw(..) => {
            return Err(AppError::InvalidRequestBody(
                "OpenAI endpoints require a JSON body".to_string(),
            ));
        }
    };
    let Ok(v) = serde_json::from_slice::<Value>(bytes) else {
        return Err(AppError::InvalidRequestBody(
            "request body is not valid JSON".to_string(),
        ));
    };
    let model = v.get("model").and_then(|m| m.as_str()).map(String::from).ok_or_else(|| {
        AppError::InvalidRequestBody("missing required field: model".to_string())
    })?;
    let stream = v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
    Ok(OpenAiRoute { model, stream })
}

/// /v1/chat/completions + /v1/completions + /v1/embeddings 共用透传 handler
/// (翻译层在 worker 侧)。`stream: true` → SSE(逐 chunk `data: {json}` +
/// `data: [DONE]`);`stream: false` → unary(复用 run_infer)。
pub async fn openai_infer_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    let protocol = crate::protocol::ApiProtocol::OpenaiCompact;
    check_openai_gate(&state, &headers).map_err(|error| ProtocolError { error, protocol })?;
    let route = parse_route(&body).map_err(|error| ProtocolError { error, protocol })?;
    if route.stream {
        // 批次 5 审计修复(B1/B2/B7/B8/B10):流式并入 SSE 管线——鉴权/限流/
        // validate/binary-flag 400/worker 流 cancel/指标/回调全随
        // sse_infer_entry_impl 继承,帧封装 = SseFrameStyle::Openai。
        crate::http::handlers::stream::openai_stream_entry(&state, &route.model, headers, body, cx)
            .await
            .map_err(|error| ProtocolError { error, protocol })
    } else {
        run_infer(
            state, route.model, None,
            "/predict".to_string(), headers, body, cx,
        )
        .await
    }
}

/// /v1/models 列表 = 注册模型名列表(OpenAI `{"object":"list","data":[...]}`)。
pub async fn v1_models_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ProtocolError> {
    let protocol = crate::protocol::ApiProtocol::OpenaiCompact;
    check_openai_gate(&state, &headers).map_err(|error| ProtocolError { error, protocol })?;
    let models: Vec<Value> = state
        .registry
        .list_loaded()
        .into_iter()
        .map(|(name, version, _)| {
            json!({
                "id": name,
                "object": "model",
                "created": 0,
                "owned_by": "lite-server",
                "version": version,
            })
        })
        .collect();
    Ok(Json(json!({"object": "list", "data": models})))
}

/// /v1/models/{model} 单模型对象;不存在 → 404(OpenAI 形状,经协议层分派)。
pub async fn v1_model_retrieve_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(model): Path<String>,
) -> Result<Json<Value>, ProtocolError> {
    let protocol = crate::protocol::ApiProtocol::OpenaiCompact;
    check_openai_gate(&state, &headers).map_err(|error| ProtocolError { error, protocol })?;
    crate::validation::validate_identifier(&model)
        .map_err(|error| ProtocolError { error, protocol })?;
    let versions = state.registry.list_versions(&model);
    if versions.is_empty() {
        return Err(ProtocolError {
            error: AppError::ModelNotFound(model),
            protocol,
        });
    }
    Ok(Json(json!({
        "id": model,
        "object": "model",
        "created": 0,
        "owned_by": "lite-server",
        "versions": versions.iter().map(|mv| mv.version.clone()).collect::<Vec<_>>(),
    })))
}

#[cfg(test)]
mod audit_tests {
    //! /audit protocol-compat 举证测试(2026-08-08):每个测试在当前代码上
    //! FAIL,证明对应缺陷存在;修复后转绿即回归锁。只含测试,不改实现。
    use super::*;
    use crate::access_control::OpenaiAuthGate;
    use crate::callback::CallbackRunner;
    use crate::config::{AuthPolicy, Config, ModelConfig};
    use crate::inference_queue::InferenceQueue;
    use crate::proto::liteserver as pb;
    use crate::registry::types::{ModelType, WorkerInfo, WorkerStatus};
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use axum::http::Request;
    use prost::Message;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn make_state() -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let cb = Arc::new(CallbackRunner::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            queue.clone(),
            "warn".to_string(),
            cb.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            wm,
            queue,
            Config::default(),
            std::path::PathBuf::new(),
            cb,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    /// 注册 ready + active + 1 worker 的模型(不含 ZMQ client——鉴权/校验
    /// 类测试应在触及 worker 层之前被拒绝)。
    fn register_ready(state: &AppState, model: &str, cfg: ModelConfig) {
        state
            .registry
            .register(model, "1", cfg, ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        state.registry.mark_ready(model, "1").unwrap();
        // open_worker_stream 检查 mv.workers.len() > 0。
        state
            .registry
            .set_workers(
                model,
                "1",
                vec![WorkerInfo {
                    worker_id: 0,
                    device: "cpu:0".to_string(),
                    endpoint: String::new(),
                    pid: None,
                    status: WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();
        assert!(state.registry.activate_version(model, "1").unwrap());
    }

    fn v1_router(state: Arc<AppState>) -> axum::Router {
        mount(axum::Router::new()).with_state(state)
    }

    /// oneshot 按值消耗 router(内含唯一 state 引用)——流式测试必须保留
    /// 一个 AppState 引用到测试结束,否则 ZMQ client 随 state 提前 drop、
    /// actor 关停、帧永远到不了(调试实录)。
    fn v1_router_keep(state: &Arc<AppState>) -> axum::Router {
        v1_router(state.clone())
    }

    fn chat_req(model: &str, stream: bool) -> Request<Body> {
        Request::builder()
            .uri("/v1/chat/completions")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(format!(
                r#"{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}],"stream":{stream}}}"#
            )))
            .unwrap()
    }

    fn ipc_endpoint(tag: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("audit-oai-{}-{}.sock", tag, std::process::id()))
                    .display()
            )
        }
        #[cfg(not(unix))]
        {
            format!("tcp://127.0.0.1:{}", 38000 + std::process::id() % 1000)
        }
    }

    /// B1(范围/安全):/v1 流式臂跳过 per-model enforce_auth。unary 同端点
    /// 经 run_infer → do_infer → enforce_auth(inference.rs:172)401;
    /// stream:true 直落 open_worker_stream(无 ZMQ client → 500)。
    #[tokio::test]
    async fn test_audit_v1_stream_bypasses_per_model_auth() {
        let state = make_state();
        register_ready(&state, "m", ModelConfig::default());
        // register 不携带 policies(独立字段)——用 set_policies 配置 auth
        // (stream.rs 既有测试先例)。
        state.registry.set_policies(
            "m",
            "1",
            Some(crate::config::ModelPolicies {
                auth: Some(AuthPolicy {
                    header: "x-api-key".to_string(),
                    keys: vec!["secret".to_string()],
                }),
                ..Default::default()
            }),
        );
        let app = v1_router(state);
        let resp = app.oneshot(chat_req("m", true)).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::UNAUTHORIZED,
            "stream:true must enforce policies.auth like unary (missing key → 401)"
        );
    }

    /// B6(控制流):流式臂缺 validate_identifier(unary 在 run_infer 有)。
    /// 非法模型名 unary → 400,stream:true → 404(校验缺失)。
    #[tokio::test]
    async fn test_audit_v1_stream_validates_model_identifier() {
        let state = make_state();
        register_ready(&state, "m", ModelConfig::default());
        let app = v1_router(state);
        let resp = app.oneshot(chat_req("bad name!", true)).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::BAD_REQUEST,
            "invalid model identifier must 400 (unary parity), not fall through to 404"
        );
    }

    /// B8(观测缺失):/events 早期拒绝经 record_stream_rejected →
    /// record_request_end 计数(stream.rs:242-249);/v1 流式臂零计数,
    /// 拒绝在 liteserver_requests_total 中完全隐形。
    #[tokio::test]
    async fn test_audit_v1_stream_rejection_recorded() {
        let state = make_state();
        let app = v1_router(state);
        let counter = crate::metrics::prometheus::REQUESTS_TOTAL
            .with_label_values(&["audit-v1-rej", "", "4xx"]);
        let before = counter.get();
        let resp = app.oneshot(chat_req("audit-v1-rej", true)).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
        assert!(
            counter.get() > before,
            "SSE parity: early rejection must be counted via record_stream_rejected"
        );
    }

    /// PAIR worker:Open → Error("boom") + Done。
    fn spawn_error_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(4000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let Ok(req) = pb::Request::decode(bytes.as_slice()) else { continue };
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                if !matches!(st.action, Some(pb::stream_request::Action::Open(_))) {
                    continue;
                }
                let mk = |payload| pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(payload),
                    })),
                    ..Default::default()
                };
                let _ = s.send(
                    mk(pb::stream_response::Payload::Error(pb::StreamError {
                        message: "boom".to_string(),
                    }))
                    .encode_to_vec(),
                    0,
                );
                let _ = s.send(
                    mk(pb::stream_response::Payload::Done(pb::StreamDone::default()))
                        .encode_to_vec(),
                    0,
                );
            }
        })
    }

    /// B13(协议一致性):模块文档(openai_compact.rs:147-149)与方案阶段 6
    /// 均声明流中途错误帧 = `data: {"error": {...}}`(OpenAI SSE 惯例,对象);
    /// 实现发扁平字符串 `{"error": "<msg>"}`,且不像 /events 管线
    /// (stream.rs:444-448)那样先尝试解析 worker 的结构化错误。
    #[tokio::test]
    async fn test_audit_v1_stream_error_frame_is_object() {
        let endpoint = ipc_endpoint("err-shape");
        let _w = spawn_error_worker(endpoint.clone());
        let state = make_state();
        register_ready(&state, "m", ModelConfig::default());
        let client = Arc::new(crate::transport::zmq::WorkerZmqClient::new(endpoint));
        state
            .worker_manager
            .insert_zmq_clients_for_test("m", "1", vec![client])
            .await;
        // 等 PAIR 握手(既有 stream.rs 测试的 200ms 先例)。
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let app = v1_router_keep(&state);
        let resp = app.oneshot(chat_req("m", true)).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            axum::body::to_bytes(resp.into_body(), 1 << 20),
        )
        .await
        .expect("SSE body must close within timeout")
        .unwrap();
        let text = String::from_utf8_lossy(&body);
        let err_json = text
            .lines()
            .find(|l| l.starts_with("data: {"))
            .map(|l| &l["data: ".len()..])
            .unwrap_or_else(|| panic!("must carry an error data frame, body = {text:?}"));
        let v: serde_json::Value = serde_json::from_str(err_json).unwrap();
        assert!(
            v.get("error").is_some_and(|e| e.is_object()),
            "mid-stream error frame must be an object per module doc/plan, got: {text}"
        );
    }

    /// PAIR worker:Open → Chunk("c1"),150ms 后 Chunk("c2"),随后等
    /// StreamCancel(见到即置 flag)。
    fn spawn_two_chunk_worker(
        endpoint: String,
        cancel_seen: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(4000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let Ok(req) = pb::Request::decode(bytes.as_slice()) else { continue };
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                match st.action {
                    Some(pb::stream_request::Action::Open(_)) => {
                        let mk = |data: &str| pb::Response {
                            payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                                stream_id: st.stream_id.clone(),
                                payload: Some(pb::stream_response::Payload::Chunk(
                                    pb::StreamChunkResponse {
                                        data: bytes::Bytes::copy_from_slice(data.as_bytes()),
                                        is_final: false,
                                    },
                                )),
                            })),
                            ..Default::default()
                        };
                        let _ = s.send(mk("c1").encode_to_vec(), 0);
                        std::thread::sleep(std::time::Duration::from_millis(150));
                        let _ = s.send(mk("c2").encode_to_vec(), 0);
                    }
                    Some(pb::stream_request::Action::Cancel(_)) => {
                        cancel_seen.store(true, Ordering::Relaxed);
                        return;
                    }
                    _ => {}
                }
            }
        })
    }

    /// B2(资源泄露):客户端断开(event_tx.send 失败 → break)后从不向
    /// worker 发 StreamCancel——worker 继续生成直到自然完成。SSE 管线恒
    /// cancel(stream.rs:506-511),方案阶段 6 复用清单也列了
    /// build_stream_cancel(未落地)。
    #[tokio::test]
    async fn test_audit_v1_stream_cancels_worker_on_client_disconnect() {
        use tokio_stream::StreamExt;
        let cancel_seen = Arc::new(AtomicBool::new(false));
        let endpoint = ipc_endpoint("cancel");
        let _w = spawn_two_chunk_worker(endpoint.clone(), cancel_seen.clone());
        let state = make_state();
        register_ready(&state, "m", ModelConfig::default());
        let client = Arc::new(crate::transport::zmq::WorkerZmqClient::new(endpoint));
        state
            .worker_manager
            .insert_zmq_clients_for_test("m", "1", vec![client])
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let app = v1_router_keep(&state);
        let resp = app.oneshot(chat_req("m", true)).await.unwrap();
        assert_eq!(resp.status(), 200);
        // 读首帧后断开(客户端取消)。
        let mut frames = resp.into_body().into_data_stream();
        let first = tokio::time::timeout(std::time::Duration::from_secs(3), frames.next()).await;
        assert!(matches!(first, Ok(Some(_))), "first chunk must arrive");
        drop(frames);
        // 服务器转发第二帧失败 → 应发现客户端断开并 cancel worker。
        for _ in 0..90 {
            if cancel_seen.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("worker never received StreamCancel after client disconnect (SSE parity)");
    }

    // ===== openai_compact.auth 专属鉴权门(2026-08-09 方案)=====

    fn with_gate(state: &mut Arc<AppState>, secret: &str) {
        // make_state 刚返回的 Arc 唯一 → get_mut 成功(注入门后由
        // v1_router 捕获)。
        let state = Arc::get_mut(state).expect("state Arc must be unique");
        state.openai_auth = OpenaiAuthGate::build(Some(&crate::config::EndpointControl::Key {
            key: "authorization".to_string(),
            value: Some(secret.to_string()),
            value_env: None,
            value_file: None,
        }))
        .unwrap()
        .map(std::sync::Arc::new);
    }

    fn models_req() -> Request<Body> {
        Request::builder().uri("/v1/models").method("GET").body(Body::empty()).unwrap()
    }

    fn models_req_with(key: &str) -> Request<Body> {
        Request::builder()
            .uri("/v1/models")
            .method("GET")
            .header("authorization", format!("Bearer {key}"))
            .body(Body::empty())
            .unwrap()
    }

    /// 门配置后:/v1 缺 key → 401;带 Bearer → 过门落到 handler(未注册
    /// 模型 → 404 model_not_found,而非门的 401)。
    #[tokio::test]
    async fn test_v1_gate_rejects_without_key_accepts_bearer() {
        let mut state = make_state();
        with_gate(&mut state, "sk-secret");
        let app = v1_router(state);

        let resp = app.clone().oneshot(chat_req("m", false)).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);

        let req = Request::builder()
            .uri("/v1/chat/completions")
            .method("POST")
            .header("content-type", "application/json")
            .header("authorization", "Bearer sk-secret")
            .body(Body::from(
                r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "过门后应落到 handler(404 model_not_found),不是门的 401"
        );

        let req = Request::builder()
            .uri("/v1/chat/completions")
            .method("POST")
            .header("content-type", "application/json")
            .header("authorization", "Bearer wrong")
            .body(Body::from(
                r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED, "错 key 必须拒绝");
    }

    /// 401 响应体为 OpenAI 形状(官方 SDK 解析 message/type 出可读错误)。
    #[tokio::test]
    async fn test_v1_gate_401_body_is_openai_shape() {
        let mut state = make_state();
        with_gate(&mut state, "sk-secret");
        let app = v1_router(state);
        let resp = app.oneshot(chat_req("m", false)).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["error"]["type"], "authentication_error");
        assert!(
            v["error"]["message"].as_str().unwrap().contains("missing API key"),
            "缺 header 文案:{}",
            v["error"]["message"]
        );
        assert!(v["error"]["param"].is_null());
    }

    /// /v1/models 列表同样要 key(OpenAI SDK 会先打它;列表端点无 model
    /// 上下文,per-model policies.auth 覆盖不到——正是专属门的职责)。
    #[tokio::test]
    async fn test_v1_gate_models_endpoints_require_key() {
        let mut state = make_state();
        with_gate(&mut state, "sk-secret");
        let app = v1_router(state);
        let resp = app.clone().oneshot(models_req()).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
        let resp = app.oneshot(models_req_with("sk-secret")).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// 未配置 auth → 中间件直通,/v1 维持现状公开(零行为变化)。
    #[tokio::test]
    async fn test_v1_gate_unconfigured_is_passthrough() {
        let state = make_state();
        let app = v1_router(state);
        let resp = app.oneshot(models_req()).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }
}
