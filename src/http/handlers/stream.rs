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
    // P-CORS: CORS headers are attached by `cors_middleware` (no longer per-handler).
    sse_infer_entry(&state, &model_name, None, headers, payload, cx)
        .await
        .into_response()
}

pub async fn sse_infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiJson(payload): ApiJson<Value>,
) -> Response {
    sse_infer_entry(
        &state, &model_name, Some(version), headers, payload, cx,
    )
    .await
    .into_response()
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

    let deadline = crate::deadline::resolve_from_http(&headers, state.config.server.timeout);
    let meta = build_request_meta(&headers, &payload, "/predict", &cx, deadline.unix_ns);
    let payload_bytes = meta.payload.clone();
    // P-DEADLINE streaming bound (client-specified only).
    let (stream_deadline, stream_idle) = if deadline.client_specified {
        (
            crate::deadline::to_instant(deadline.unix_ns),
            crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs),
        )
    } else {
        (None, None)
    };
    let (stream_id, mut chunk_rx) = open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await?;

    // Task D: fire InferenceRequest once the worker stream opened and arm the
    // response callback. cx is not captured by the spawn, so request_id /
    // client_ip are cloned here and moved in. open_time (inside the spawn) is
    // the elapsed reference for the response.
    let cb_runner = state.callback_runner.clone();
    let req_ctx = crate::callback::InferenceContext {
        model_name: model_name.clone(),
        version: resolved_version.clone(),
        route: "/predict".to_string(),
        protocol: crate::callback::Protocol::Sse,
        request_id: cx.request_id.clone(),
        client_ip: cx.client_ip.clone(),
        elapsed_us: None,
    };
    crate::callback::fire_inference_request(&cb_runner, &req_ctx);

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "sse");
    }

    let (event_tx, event_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        loop {
            let chunk = match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                .await
            {
                Ok(Some(c)) => c,
                Ok(None) => break, // worker closed the stream
                Err(elapsed) => {
                    // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                    tracing::warn!(
                        ?elapsed, stream_id = %stream_id,
                        "sse stream closed: deadline/idle elapsed"
                    );
                    break;
                }
            };
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
                    // Task D: terminal Error frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
                    Event::default().data(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    // Task D: terminal Done frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
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
    // P-CORS (评审 1.3): browsers send no preflight for WS, so the CORS
    // middleware can't stop cross-site WS hijacking — check Origin at upgrade.
    if !crate::http::cors::ws_origin_allowed(&state, &model_name, None, &headers) {
        return (axum::http::StatusCode::FORBIDDEN, "WebSocket Origin not allowed").into_response();
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
    if !crate::http::cors::ws_origin_allowed(&state, &model_name, Some(&version), &headers) {
        return (axum::http::StatusCode::FORBIDDEN, "WebSocket Origin not allowed").into_response();
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
    let deadline = crate::deadline::resolve_from_http(&headers, state.config.server.timeout);
    let meta = build_request_meta(&headers, &payload, "/predict", &cx, deadline.unix_ns);
    let payload_bytes = meta.payload.clone();
    // P-DEADLINE streaming bound (client-specified only).
    let (stream_deadline, stream_idle) = if deadline.client_specified {
        (
            crate::deadline::to_instant(deadline.unix_ns),
            crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs),
        )
    } else {
        (None, None)
    };

    let (stream_id, mut chunk_rx) = match open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await {
        Ok(r) => r,
        Err(e) => {
            let _ = socket.send(Message::Text(json!({"error": e.to_string()}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    // Task D: fire InferenceRequest once the worker stream opened and arm the
    // response callback. cx is not captured by the spawn, so request_id /
    // client_ip are cloned here and moved in. open_time (inside the spawn) is
    // the elapsed reference for the response.
    let cb_runner = state.callback_runner.clone();
    let req_ctx = crate::callback::InferenceContext {
        model_name: model_name.clone(),
        version: resolved_version.clone(),
        route: "/predict".to_string(),
        protocol: crate::callback::Protocol::WebSocket,
        request_id: cx.request_id.clone(),
        client_ip: cx.client_ip.clone(),
        elapsed_us: None,
    };
    crate::callback::fire_inference_request(&cb_runner, &req_ctx);

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

        loop {
            let chunk = match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                .await
            {
                Ok(Some(c)) => c,
                Ok(None) => break, // worker closed the stream
                Err(elapsed) => {
                    // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                    tracing::warn!(
                        ?elapsed, stream_id = %stream_id,
                        "websocket stream closed: deadline/idle elapsed"
                    );
                    break;
                }
            };
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
                    // Task D: terminal Error frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
                    Message::Text(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    // Task D: terminal Done frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
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

#[cfg(test)]
mod tests {
    //! Task D (HTTP): SSE inference callbacks. The forwarder spawns with a
    //! 64-event buffer, so it processes the worker's Chunk + Done (and fires the
    //! callback) independently of client draining — these tests therefore hold the
    //! Sse and poll for the callback count rather than draining the body. WS uses
    //! the identical fire_inference_request/response helpers (structural parity;
    //! integration-covered).

    use super::*;
    use crate::callback::{Callback, CallbackRunner, InferenceContext, Protocol};
    use crate::config::ModelConfig;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::types::{ModelType, WorkerInfo, WorkerStatus};
    use crate::registry::ModelRegistry;
    use crate::request_context::RequestContext;
    use crate::transport::zmq::WorkerZmqClient;
    use crate::worker::WorkerManager;
    use bytes::Bytes;
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn ipc_endpoint(tag: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("sse-{}-{}.sock", tag, std::process::id()))
                    .display()
            )
        }
        #[cfg(not(unix))]
        {
            format!("tcp://127.0.0.1:{}", 37000 + std::process::id() % 1000)
        }
    }

    fn make_state(cb: Arc<CallbackRunner>) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
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
            crate::config::Config::default(),
            std::path::PathBuf::new(),
            cb,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    /// Register a ready model backed by one ZMQ stream client (test hook).
    async fn ready_state(model: &str, endpoint: String, cb: Arc<CallbackRunner>) -> Arc<AppState> {
        let state = make_state(cb);
        state
            .registry
            .register(model, "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        state.registry.mark_ready(model, "1").unwrap();
        // open_worker_stream checks mv.workers.len() > 0.
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
        let client = Arc::new(WorkerZmqClient::new(endpoint));
        state
            .worker_manager
            .insert_zmq_clients_for_test(model, "1", vec![client])
            .await;
        state
    }

    /// PAIR worker: Open → one Chunk + Done.
    fn spawn_done_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let is_open = matches!(
                    req.payload,
                    Some(pb::request::Payload::Stream(pb::StreamRequest {
                        action: Some(pb::stream_request::Action::Open(_)),
                        ..
                    }))
                );
                if !is_open {
                    let _ = s.send(pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(), 0);
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let mk = |payload| pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(payload),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let _ = s.send(
                    mk(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                        data: Bytes::from_static(b"{}"),
                        is_final: false,
                    }))
                    .encode_to_vec(),
                    0,
                );
                let _ = s.send(mk(pb::stream_response::Payload::Done(pb::StreamDone::default())).encode_to_vec(), 0);
            }
        })
    }

    /// PAIR worker: Open → one Error frame, then close (forwarder breaks on the
    /// peer disconnect after the Error callback fires).
    fn spawn_error_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let is_open = matches!(
                    req.payload,
                    Some(pb::request::Payload::Stream(pb::StreamRequest {
                        action: Some(pb::stream_request::Action::Open(_)),
                        ..
                    }))
                );
                if !is_open {
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let resp = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                            message: "boom".to_string(),
                        })),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let _ = s.send(resp.encode_to_vec(), 0);
                return; // close → forwarder observes disconnect and breaks
            }
        })
    }

    struct CountingCallback {
        req: AtomicUsize,
        resp: AtomicUsize,
        last: Mutex<Option<InferenceContext>>,
    }

    #[async_trait::async_trait]
    impl Callback for CountingCallback {
        async fn on_inference_request(&self, _ctx: &InferenceContext) {
            self.req.fetch_add(1, Ordering::Relaxed);
        }
        async fn on_inference_response(&self, ctx: &InferenceContext) {
            self.resp.fetch_add(1, Ordering::Relaxed);
            *self.last.lock().unwrap() = Some(ctx.clone());
        }
    }

    async fn wait_for<F: Fn() -> bool>(cond: F, label: &str) {
        for _ in 0..60 {
            if cond() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("condition never met within ~1.5s: {}", label);
    }

    fn test_cx() -> RequestContext {
        RequestContext {
            request_id: "sse-rid".to_string(),
            client_ip: "127.0.0.1".to_string(),
            trace_cx: opentelemetry::Context::new(),
            protocol: Protocol::Http,
            principal: None,
        }
    }

    #[tokio::test]
    async fn sse_callbacks_fire_on_done() {
        let model = "sse_done";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_done_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json!({}),
            test_cx(),
        )
        .await
        .expect("sse must open");
        // Hold the Sse so event_rx stays alive while the forwarder processes Done.
        wait_for(|| cb.resp.load(Ordering::Relaxed) >= 1, "resp>=1").await;
        drop(sse);

        assert_eq!(cb.req.load(Ordering::Relaxed), 1, "request fires once");
        assert_eq!(cb.resp.load(Ordering::Relaxed), 1, "Done fires response");
        let protocol = cb
            .last
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.protocol)
            .unwrap_or(Protocol::Http);
        assert_eq!(protocol, Protocol::Sse, "response ctx must carry the Sse protocol");
        assert!(
            cb.last.lock().unwrap().as_ref().unwrap().elapsed_us.is_some(),
            "response elapsed_us must be set"
        );
    }

    #[tokio::test]
    async fn sse_callbacks_fire_on_error() {
        let model = "sse_err";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json!({}),
            test_cx(),
        )
        .await
        .expect("sse must open");
        wait_for(|| cb.resp.load(Ordering::Relaxed) >= 1, "resp>=1").await;
        drop(sse);

        assert_eq!(cb.req.load(Ordering::Relaxed), 1);
        assert_eq!(cb.resp.load(Ordering::Relaxed), 1, "Error frame fires response");
    }

    #[tokio::test]
    async fn sse_callback_not_fired_when_stream_dropped() {
        let model = "sse_drop";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_done_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json!({}),
            test_cx(),
        )
        .await
        .expect("sse must open");
        // Drop immediately → event_rx goes away → the forwarder's first
        // event_tx.send errors, so it breaks before the Done frame: no response.
        drop(sse);

        wait_for(|| cb.req.load(Ordering::Relaxed) >= 1, "req>=1").await;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(cb.req.load(Ordering::Relaxed), 1, "request still fires on cancel");
        assert_eq!(cb.resp.load(Ordering::Relaxed), 0, "cancel must NOT fire response");
    }
}

