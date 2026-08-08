//! openai-compact(阶段 6,批次 5,J5/J6/C19):OpenAI 兼容紧凑子集。
//!
//! 5 端点(根 `/v1`):`/v1/chat/completions` + `/v1/completions` +
//! `/v1/embeddings` + `/v1/models` + `/v1/models/{model}`。SSE 流式
//! (`data: {json}` + `data: [DONE]`),v2 infer 为内部底座。
//!
//! **翻译层在 worker 侧**(J6):server 薄透传——body 最小解析仅
//! `model`/`stream` 两字段用于路由与分流 + SSE 帧编码;chat 请求解析 /
//! completion·chunk·embeddings 构造全部进 Python helper
//! `lite_server/helpers/openai.py`(worker 作者集成)。经协议层 seam 接入:
//! 1 handler 模块 + routes.rs 一行挂载 + `ApiProtocol::OpenaiCompact`
//! 一个 arm(复用 openai.rs renderer),核心逻辑 / error.rs / inference.rs /
//! stream.rs / worker 零改动。

use super::inference::{build_request_meta, resolve_version, run_infer};
use super::{ApiBody, RequestBody};
use crate::error::{AppError, ProtocolError};
use crate::http::state::AppState;
use crate::request_context::RequestContext;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Json, Response},
};
use axum::response::sse::{Event, Sse};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;

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
    let route = parse_route(&body).map_err(|error| ProtocolError { error, protocol })?;
    if route.stream {
        openai_stream(&state, &route.model, headers, body, cx)
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
) -> Result<Json<Value>, ProtocolError> {
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
    Path(model): Path<String>,
) -> Result<Json<Value>, ProtocolError> {
    let protocol = crate::protocol::ApiProtocol::OpenaiCompact;
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

/// SSE 流式(stream: true):复用 `open_worker_stream` + `streaming::recv_chunk`
/// 流消费(核心零改动),帧编码 = OpenAI SSE 风格:
/// - Chunk → `data: <worker chunk JSON>`(worker 经 openai.py 构造 chunk);
/// - Error → `data: {"error": {...}}`(OpenAI SSE 惯例,HTTP 状态码由首个
///   响应固定);
/// - Done → `data: [DONE]`。
async fn openai_stream(
    state: &Arc<AppState>,
    model: &str,
    headers: HeaderMap,
    body: RequestBody,
    cx: RequestContext,
) -> Result<Response, AppError> {
    let span = tracing::info_span!(
        "inference",
        model = %model,
        version = "auto",
        request_id = %cx.request_id,
        body_kind = "openai_sse",
    );
        let resolved_version =
            resolve_version(state, model, None, &headers).await?;
        if !state.registry.is_ready(model, Some(&resolved_version)) {
            return Err(AppError::ModelNotReady(format!(
                "{} version {} is not ready",
                model, resolved_version
            )));
        }
        let deadline =
            crate::deadline::resolve_from_http(&headers, state.config.server.timeout);
        let payload_bytes = body.bytes();
        let meta = build_request_meta(
            &headers,
            payload_bytes.clone(),
            "/predict",
            &cx,
            deadline.unix_ns,
        );
        let (_stream_id, _worker_client, mut chunk_rx) = crate::http::handlers::stream::open_worker_stream(
            state, model, &resolved_version, meta, payload_bytes, false,
        )
        .await?;

        let stream_deadline = if deadline.client_specified {
            crate::deadline::to_instant(deadline.unix_ns)
        } else {
            None
        };
        let stream_idle =
            crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs);
        let (event_tx, event_rx) =
            mpsc::channel::<Result<Event, std::convert::Infallible>>(64);

        tokio::spawn(async move {
            loop {
                let chunk = match crate::streaming::recv_chunk(
                    &mut chunk_rx,
                    stream_deadline,
                    stream_idle,
                )
                .await
                {
                    Ok(Some(c)) => c,
                    Ok(None) => break,
                    Err(_) => break,
                };
                let event = match &chunk.payload {
                    Some(crate::proto::liteserver::stream_response::Payload::Chunk(c)) => {
                        // worker chunk = openai.py 构造的 chunk JSON(透传)
                        Some(Event::default().data(String::from_utf8_lossy(&c.data)))
                    }
                    Some(crate::proto::liteserver::stream_response::Payload::Error(e)) => {
                        // 流中途错误:OpenAI SSE 惯例,错误在后续事件内
                        Some(Event::default().data(json!({"error": e.message}).to_string()))
                    }
                    Some(crate::proto::liteserver::stream_response::Payload::Done(_)) => {
                        // OpenAI SSE 以 [DONE] 终止
                        Some(Event::default().data("[DONE]"))
                    }
                    _ => None,
                };
                if let Some(event) = event {
                    if event_tx.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            }
        }
        .instrument(span.clone()));

    Ok(Sse::new(ReceiverStream::new(event_rx)).into_response())
}
