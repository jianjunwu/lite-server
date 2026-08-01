use super::*;
use super::inference::{build_request_meta, resolve_version};
use crate::error::AppError;
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::request_context::RequestContext;
use crate::streaming;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Json, Response},
};
use axum::extract::ws::{Message, WebSocket};
use axum::response::sse::{Event, Sse};
use serde_json::{json, Value};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

async fn open_worker_stream(
    state: &Arc<AppState>,
    model_name: &str,
    resolved_version: &str,
    meta: pb::RequestMeta,
    payload_bytes: bytes::Bytes,
) -> Result<(String, mpsc::Receiver<pb::StreamResponse>), AppError> {
    let mv = state
        .registry
        .get(model_name, Some(resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", model_name)));
    }

    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, resolved_version)
        .await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", model_name, resolved_version)))?;

    // Skip ejected workers for streaming requests. P8-1: when the request
    // carries a sequence_id already mapped to a registered, non-ejected worker,
    // connect directly to it (sticky); otherwise the normal pick, then record.
    let outlier = state.worker_manager.get_outlier_state(model_name, resolved_version).await;
    let seq_registry = state.inference_queue.sequence_registry();
    let preferred = meta.sequence_id.as_deref().and_then(|seq| {
        let w = seq_registry.lookup(seq, model_name, resolved_version)?;
        let ejected = outlier.as_ref().map(|o| o.is_ejected(w)).unwrap_or(false);
        (w < num_workers && !ejected).then_some(w)
    });
    let worker_id = preferred.unwrap_or_else(|| match &outlier {
        Some(o) => crate::worker::pick_worker_skip_ejected(num_workers, o),
        None => crate::worker::pick_worker_random(num_workers),
    });
    if let Some(seq) = meta.sequence_id.as_deref() {
        seq_registry.record(seq, model_name, resolved_version, worker_id);
    }

    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }

    let client = &clients[worker_id];
    let stream_id = format!("stream-{}", Uuid::new_v4());
    let open_req = streaming::build_stream_open(stream_id.clone(), payload_bytes, Some(meta), false);

    let chunk_rx = client.send_stream(open_req, stream_id.clone()).await?;
    Ok((stream_id, chunk_rx))
}

// ===== SSE Streaming =====

pub async fn sse_infer_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiJson(payload): ApiJson<Value>,
) -> Response {
    let result =
        sse_infer_entry(&state, &model_name, None, headers, payload, cx).await;
    attach_cors_headers(&state, &model_name, result)
}

pub async fn sse_infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiJson(payload): ApiJson<Value>,
) -> Response {
    let result = sse_infer_entry(
        &state, &model_name, Some(version), headers, payload, cx,
    )
    .await;
    attach_cors_headers(&state, &model_name, result)
}

/// Shared entry for SSE inference: validation, ready check, rate limiting,
/// and stream setup. Returns a `Response` so the caller can uniformly wrap
/// CORS around both the success stream-start and any early error.
async fn sse_infer_entry(
    state: &Arc<AppState>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    payload: Value,
    cx: RequestContext,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(model_name)?;
    if let Some(ref v) = version {
        crate::validation::validate_version(v)?;
    }
    let resolved_version = resolve_version(state, model_name, version, &headers).await?;
    if !state.registry.is_ready(model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }
    // Auth + rate limit (rate limit shares the /predict bucket with unary infer).
    if let Some(mv) = state.registry.get(model_name, Some(&resolved_version)) {
        enforce_auth(mv.policies.auth.as_ref(), &headers)?;
        enforce_rate_limit(state, mv.policies.rate_limit.as_ref(), model_name, &cx.client_ip).await?;
    }
    let sse = sse_infer_impl(
        state.clone(),
        model_name.to_string(),
        resolved_version,
        headers,
        payload,
        cx,
    )
    .await?;
    Ok(sse.into_response())
}

async fn sse_infer_impl(
    state: Arc<AppState>,
    model_name: String,
    resolved_version: String,
    headers: HeaderMap,
    payload: Value,
    cx: RequestContext,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {

    let meta = build_request_meta(&headers, &payload, "/predict", &cx);
    let payload_bytes = meta.payload.clone();
    let (stream_id, mut chunk_rx) = open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await?;

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "sse");
    }

    let (event_tx, event_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        while let Some(chunk) = chunk_rx.recv().await {
            let event = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
                    if stream_metrics {
                        if first_chunk {
                            prometheus::record_stream_ttft(&model_name, &resolved_version, "sse", open_time.elapsed().as_secs_f64());
                            first_chunk = false;
                        } else {
                            prometheus::record_stream_tbt(&model_name, &resolved_version, "sse", last_chunk_time.elapsed().as_secs_f64());
                        }
                        last_chunk_time = std::time::Instant::now();
                        prometheus::record_stream_chunk(&model_name, &resolved_version, "sse");
                    }
                    let data = String::from_utf8_lossy(&c.data);
                    Event::default().data(&data)
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    // Try to parse as structured error from HTTPException
                    let event_data = match serde_json::from_str::<serde_json::Value>(&e.message) {
                        Ok(val) if val.get("error").and_then(|err| err.get("type")).is_some() => {
                            json!({"error": val["error"]}).to_string()
                        }
                        _ => json!({"error": e.message}).to_string(),
                    };
                    Event::default().data(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    Event::default().data("[DONE]")
                }
                _ => continue,
            };
            if event_tx.send(Ok(event)).await.is_err() {
                break;
            }
            if matches!(chunk.payload, Some(pb::stream_response::Payload::Done(_))) {
                break;
            }
        }
        if stream_metrics {
            prometheus::record_stream_close(&model_name, &resolved_version, "sse");
        }
        // Ensure stream is cleaned up on worker side
        let cancel_req = streaming::build_stream_cancel(stream_id);
        open_worker_stream_cancel(&state, &model_name, &resolved_version, cancel_req).await;
    });

    Ok(Sse::new(ReceiverStream::new(event_rx)))
}

/// Send a stream cancel request to all workers for a model version.
/// Best-effort: workers that don't own the stream will ignore it.
async fn open_worker_stream_cancel(
    state: &Arc<AppState>,
    model_name: &str,
    version: &str,
    cancel_req: pb::Request,
) {
    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, version)
        .await;
    match clients {
        Some(list) => {
            for client in &list {
                // Fire-and-forget: a stream Cancel carries no unary reply — the
                // worker just signals the generator to stop — so send_raw must
                // be used. client.send would register a pending reply slot and
                // block up to ZMQ_RESPONSE_TIMEOUT (300s) for a reply that
                // never comes. Because this runs inline after [DONE] while the
                // SSE/WS `event_tx`/socket is still held, that 300s wait would
                // keep the response stream open and hang any client draining it.
                let _ = client.send_raw(cancel_req.clone()).await;
            }
        }
        None => {
            // Worker may already be unloaded; nothing to cancel
        }
    }
}

// ===== WebSocket Streaming =====

pub async fn ws_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    cx: RequestContext,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, None, headers, socket, cx))
}

pub async fn ws_stream_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    cx: RequestContext,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if let Err(e) = crate::validation::validate_version(&version) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, Some(version), headers, socket, cx))
}

async fn handle_ws_stream(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    headers: HeaderMap,
    mut socket: WebSocket,
    cx: RequestContext,
) {
    let resolved_version = match resolve_version(&state, &model_name, version, &headers).await {
        Ok(v) => v,
        Err(_) => {
            let _ = socket.close().await;
            return;
        }
    };

    if !state.registry.is_ready(&model_name, Some(&resolved_version)) {
        let _ = socket.close().await;
        return;
    }

    // Rate limit (same logic as HTTP infer). client_ip comes from the
    // RequestContext filled once by context_middleware (including the
    // direct-connection peer fallback), so key="ip" limits per real client.
    if let Some(mv) = state.registry.get(&model_name, Some(&resolved_version)) {
        if enforce_auth(mv.policies.auth.as_ref(), &headers).is_err()
            || enforce_rate_limit(
                &state,
                mv.policies.rate_limit.as_ref(),
                &model_name,
                &cx.client_ip,
            )
            .await
            .is_err()
        {
            let _ = socket
                .send(Message::Text(
                    json!({"error": "rate limit exceeded"}).to_string(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    }

    // Wait for first message from client (the request payload)
    let first_msg = match socket.recv().await {
        Some(Ok(Message::Text(text))) => text,
        Some(Ok(Message::Binary(bin))) => String::from_utf8_lossy(&bin).to_string(),
        _ => {
            let _ = socket.close().await;
            return;
        }
    };

    let payload: Value = match serde_json::from_str(&first_msg) {
        Ok(v) => v,
        Err(_) => {
            let _ = socket.send(Message::Text(json!({"error": "invalid JSON"}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    // The original upgrade-request headers flow into meta; client_ip /
    // request_id come from the RequestContext (P-MW single fill).
    let meta = build_request_meta(&headers, &payload, "/predict", &cx);
    let payload_bytes = meta.payload.clone();

    let (stream_id, mut chunk_rx) = match open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await {
        Ok(r) => r,
        Err(e) => {
            let _ = socket.send(Message::Text(json!({"error": e.to_string()}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "websocket");
    }

    // Clone before move so cancel can reference them after the task completes
    let model_name_owned = model_name.clone();
    let version_owned = resolved_version.clone();

    // Spawn task to forward worker chunks -> WebSocket
    let send_task = tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        while let Some(chunk) = chunk_rx.recv().await {
            let msg = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
                    if stream_metrics {
                        if first_chunk {
                            prometheus::record_stream_ttft(&model_name, &resolved_version, "websocket", open_time.elapsed().as_secs_f64());
                            first_chunk = false;
                        } else {
                            prometheus::record_stream_tbt(&model_name, &resolved_version, "websocket", last_chunk_time.elapsed().as_secs_f64());
                        }
                        last_chunk_time = std::time::Instant::now();
                        prometheus::record_stream_chunk(&model_name, &resolved_version, "websocket");
                    }
                    Message::Binary(c.data.to_vec())
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    // Try to parse as structured error from HTTPException
                    let event_data = match serde_json::from_str::<serde_json::Value>(&e.message) {
                        Ok(val) if val.get("error").and_then(|err| err.get("type")).is_some() => {
                            json!({"error": val["error"]}).to_string()
                        }
                        _ => json!({"error": e.message}).to_string(),
                    };
                    Message::Text(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    Message::Text(json!({"done": true}).to_string())
                }
                _ => continue,
            };
            if socket.send(msg).await.is_err() {
                break;
            }
            if matches!(chunk.payload, Some(pb::stream_response::Payload::Done(_))) {
                let _ = socket.close().await;
                break;
            }
        }
        if stream_metrics {
            prometheus::record_stream_close(&model_name, &resolved_version, "websocket");
        }
        stream_id
    });

    // For now, we don't handle bidirectional streaming over WebSocket
    // Just wait for the send task to complete
    let completed_stream_id = match send_task.await {
        Ok(sid) => sid,
        Err(_) => return,
    };

    // Send cancel to clean up
    let cancel_req = streaming::build_stream_cancel(completed_stream_id);
    open_worker_stream_cancel(&state, &model_name_owned, &version_owned, cancel_req).await;
}

