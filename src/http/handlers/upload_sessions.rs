//! Resumable chunked upload sessions (>1GiB model files).
//!
//! A session stages chunks under `{repo}/.tmp-upload-{sid}/` — the same
//! staging prefix as the multipart upload, so startup tmp cleanup (H7)
//! covers crash residue. Chunks land atomically (`{ci}.part` + rename), the
//! received-set is rebuilt by scanning the chunk dir (never in-memory
//! state), so a session survives client refreshes AND server restarts.
//! `complete` concatenates chunks in order, verifies size/sha256, then
//! hands the standard staged layout to `finalize_upload` — validation,
//! zip-bomb caps, swap commit, artifact retention and auto-load are all
//! inherited from the multipart path.

use super::files::{finalize_upload, StagedUploadFile, UploadQuery};
use super::{ApiJson, ApiQuery, AppError, AppState};
use crate::request_context::RequestContext;
use axum::{
    body::Body,
    extract::{Path, State},
    response::Json,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest;
use std::sync::Arc;
use tracing::info;

/// Fixed chunk size — server-side constant so the received-bitmap semantics
/// stay trivial (clients learn it from the init response).
pub const CHUNK_SIZE: u64 = 16 * 1024 * 1024;

/// Chunk size recorded into new sessions. Tests shrink it so multi-chunk
/// flows exercise on kilobytes, not megs — handlers always honor the value
/// in session.json, never this constant directly.
fn default_chunk_size() -> u64 {
    #[cfg(test)]
    {
        64
    }
    #[cfg(not(test))]
    {
        CHUNK_SIZE
    }
}

/// Bound on files per session and on concurrent sessions per model/version
/// (abuse guards; the plan's v1 rule is "reject, clean up first").
const MAX_SESSION_FILES: usize = 1024;
const MAX_ACTIVE_SESSIONS: usize = 8;

// ===== Session meta (persisted as session.json in the staging dir) =====

#[derive(Serialize, Deserialize, Clone, PartialEq)]
struct SessionFile {
    name: String,
    size: u64,
    sha256: Option<String>,
    is_lma: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum SessionState {
    Uploading,
    Completing,
}

#[derive(Serialize, Deserialize, Clone)]
struct SessionMeta {
    model: String,
    version: String,
    chunk_size: u64,
    files: Vec<SessionFile>,
    state: SessionState,
}

#[derive(Deserialize)]
pub struct InitFile {
    name: String,
    size: u64,
    sha256: Option<String>,
}

#[derive(Deserialize)]
pub struct InitRequest {
    files: Vec<InitFile>,
}

// ===== Path / meta helpers =====

fn staging_dir(repo: &std::path::Path, sid: &str) -> Result<std::path::PathBuf, AppError> {
    // The sid becomes a directory name — only accept real uuids.
    let _ = uuid::Uuid::parse_str(sid)
        .map_err(|_| AppError::Validation("invalid session id".to_string()))?;
    Ok(repo.join(format!(".tmp-upload-{}", sid)))
}

fn chunks_dir(staging: &std::path::Path, file_index: usize) -> std::path::PathBuf {
    staging.join(".chunks").join(file_index.to_string())
}

fn meta_path(staging: &std::path::Path) -> std::path::PathBuf {
    staging.join("session.json")
}

async fn read_meta(staging: &std::path::Path) -> Result<SessionMeta, AppError> {
    let raw = tokio::fs::read_to_string(meta_path(staging))
        .await
        .map_err(|_| AppError::ModelNotFound("upload session not found".to_string()))?;
    serde_json::from_str(&raw)
        .map_err(|_| AppError::Internal("corrupt upload session meta".to_string()))
}

async fn write_meta(staging: &std::path::Path, meta: &SessionMeta) -> Result<(), AppError> {
    let raw = serde_json::to_string(meta).map_err(AppError::Serialization)?;
    let tmp = staging.join("session.json.tmp");
    tokio::fs::write(&tmp, raw).await.map_err(AppError::Io)?;
    tokio::fs::rename(&tmp, meta_path(staging))
        .await
        .map_err(AppError::Io)
}

/// Keep the staging dir "alive" for startup tmp cleanup: the sweep judges
/// staleness by mtime, and writing into `.chunks/` subdirs does not refresh
/// the top-level dir, so every chunk PUT touches session.json instead.
async fn touch_meta(staging: &std::path::Path) {
    // std File::set_modified (tokio's File lacks it on this version); the
    // syscall is trivially fast, no spawn_blocking needed.
    if let Ok(file) = std::fs::File::open(meta_path(staging)) {
        let _ = file.set_modified(std::time::SystemTime::now());
    }
}

/// Expected chunk count for a declared file size.
fn expected_chunks(size: u64, chunk_size: u64) -> u64 {
    size.div_ceil(chunk_size)
}

/// The received chunk set of one file, rebuilt from disk (restart-safe).
async fn scan_received(staging: &std::path::Path, file_index: usize) -> Vec<u64> {
    let mut received = Vec::new();
    let dir = chunks_dir(staging, file_index);
    if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            // Only final names count — a leftover `.part` is a crashed write.
            if let Ok(index) = name.parse::<u64>() {
                received.push(index);
            }
        }
    }
    received.sort_unstable();
    received
}

/// Multipart-parity filename rule (B1): strip any path components, reject
/// empty and dot-prefixed names.
fn sanitize_upload_name(name: &str) -> Result<String, AppError> {
    let safe = std::path::Path::new(name)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    if safe.is_empty() || safe.starts_with('.') {
        return Err(AppError::Validation(format!(
            "invalid upload file name: {}",
            name
        )));
    }
    Ok(safe)
}

/// Load + cross-check a session against the URL's model/version.
async fn load_session(
    state: &AppState,
    model: &str,
    version: &str,
    sid: &str,
) -> Result<(std::path::PathBuf, SessionMeta), AppError> {
    let staging = staging_dir(&state.repo_path, sid)?;
    let meta = read_meta(&staging).await?;
    if meta.model != model || meta.version != version {
        return Err(AppError::ModelNotFound(
            "upload session not found".to_string(),
        ));
    }
    Ok((staging, meta))
}

/// Per-staging-dir mutex so concurrent `complete` calls for one session
/// serialize (and the loser sees state != uploading → 409).
fn complete_locks()
-> &'static dashmap::DashMap<std::path::PathBuf, Arc<tokio::sync::Mutex<()>>> {
    static LOCKS: std::sync::OnceLock<
        dashmap::DashMap<std::path::PathBuf, Arc<tokio::sync::Mutex<()>>>,
    > = std::sync::OnceLock::new();
    LOCKS.get_or_init(dashmap::DashMap::new)
}

// ===== Handlers =====

/// POST .../upload-sessions — declare the file set, get a session id.
pub async fn create_upload_session(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiJson(body): ApiJson<InitRequest>,
) -> Result<(axum::http::StatusCode, Json<Value>), AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    if body.files.is_empty() || body.files.len() > MAX_SESSION_FILES {
        return Err(AppError::Validation(format!(
            "a session declares 1..={} files",
            MAX_SESSION_FILES
        )));
    }

    let mut files = Vec::with_capacity(body.files.len());
    let mut total: u64 = 0;
    for f in &body.files {
        let safe = sanitize_upload_name(&f.name)?;
        let is_lma = safe.ends_with(".lma");
        // Multipart parity: a version upload accepts a single .lma artifact.
        if is_lma && files.iter().any(|e: &SessionFile| e.is_lma) {
            return Err(AppError::InvalidRequestBody(
                "a version upload accepts a single .lma artifact".to_string(),
            ));
        }
        total = total.saturating_add(f.size);
        files.push(SessionFile {
            name: safe,
            size: f.size,
            sha256: f.sha256.clone(),
            is_lma,
        });
    }
    // Reject at declaration time (F11b parity: the cap gates compressed
    // upload bytes), not after the bytes already landed.
    if let Some(max) = state.config.server.max_upload_bytes {
        if total > max {
            return Err(AppError::PayloadTooLarge {
                max_size: max as usize,
                actual_size: Some(total),
            });
        }
    }

    // Bound concurrent sessions per model/version: leftovers are resumable
    // state, not garbage, so a client starting fresh must DELETE first.
    let mut active = 0usize;
    if let Ok(mut entries) = tokio::fs::read_dir(&state.repo_path).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with(".tmp-upload-") {
                continue;
            }
            if let Ok(meta) = read_meta(&entry.path()).await {
                if meta.model == model_name && meta.version == version {
                    active += 1;
                }
            }
        }
    }
    if active >= MAX_ACTIVE_SESSIONS {
        return Err(AppError::Conflict(format!(
            "too many active upload sessions for {}/{} (max {}); resume or delete one first",
            model_name, version, MAX_ACTIVE_SESSIONS
        )));
    }

    let sid = uuid::Uuid::new_v4().to_string();
    let staging = staging_dir(&state.repo_path, &sid)?;
    tokio::fs::create_dir_all(&staging)
        .await
        .map_err(AppError::Io)?;
    let meta = SessionMeta {
        model: model_name.clone(),
        version: version.clone(),
        chunk_size: default_chunk_size(),
        files,
        state: SessionState::Uploading,
    };
    write_meta(&staging, &meta).await?;

    Ok((
        axum::http::StatusCode::CREATED,
        Json(json!({
            "session_id": sid,
            "chunk_size": default_chunk_size(),
        })),
    ))
}

/// GET .../upload-sessions/{sid} — received bitmap for client resume.
pub async fn get_upload_session(
    State(state): State<Arc<AppState>>,
    Path((model_name, version, sid)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    let (staging, meta) = load_session(&state, &model_name, &version, &sid).await?;

    let mut files = Vec::new();
    for (i, f) in meta.files.iter().enumerate() {
        let received = scan_received(&staging, i).await;
        let expected = expected_chunks(f.size, meta.chunk_size);
        files.push(json!({
            "name": f.name,
            "size": f.size,
            "received_chunks": received,
            "complete": received.len() as u64 == expected,
        }));
    }
    Ok(Json(json!({
        "session_id": sid,
        "chunk_size": meta.chunk_size,
        "state": match meta.state {
            SessionState::Uploading => "uploading",
            SessionState::Completing => "completing",
        },
        "files": files,
    })))
}

/// PUT .../upload-sessions/{sid}/files/{fi}/chunks/{ci} — one chunk, raw
/// body. Idempotent: re-PUTting a chunk index overwrites it (client retry
/// after an ambiguous failure is always safe).
pub async fn put_session_chunk(
    State(state): State<Arc<AppState>>,
    Path((model_name, version, sid, file_index, chunk_index)): Path<(String, String, String, usize, u64)>,
    body: Body,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    let (staging, meta) = load_session(&state, &model_name, &version, &sid).await?;
    if meta.state != SessionState::Uploading {
        return Err(AppError::Conflict(
            "session is completing; no further chunks accepted".to_string(),
        ));
    }
    let file = meta.files.get(file_index).ok_or_else(|| {
        AppError::Validation(format!("file index {} out of range", file_index))
    })?;
    let expected = expected_chunks(file.size, meta.chunk_size);
    if chunk_index >= expected {
        return Err(AppError::Validation(format!(
            "chunk index {} out of range (file {} expects {} chunks)",
            chunk_index, file_index, expected
        )));
    }
    // Strict per-chunk length: non-final chunks are exactly chunk_size, the
    // final one the remainder — then the received bitmap alone proves
    // completeness and `complete` only re-verifies the declared hash.
    let want = if chunk_index + 1 == expected {
        file.size - chunk_index * meta.chunk_size
    } else {
        meta.chunk_size
    };

    let dir = chunks_dir(&staging, file_index);
    tokio::fs::create_dir_all(&dir).await.map_err(AppError::Io)?;
    let part = dir.join(format!("{}.part", chunk_index));
    let final_path = dir.join(chunk_index.to_string());

    let mut file_out = tokio::fs::File::create(&part).await.map_err(AppError::Io)?;
    let mut written: u64 = 0;
    let mut stream = body.into_data_stream();
    let write_result: Result<(), AppError> = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| AppError::Transport(format!("read chunk body: {}", e)))?;
            written += chunk.len() as u64;
            if written > want {
                return Err(if written > meta.chunk_size {
                    AppError::PayloadTooLarge {
                        max_size: meta.chunk_size as usize,
                        actual_size: Some(written),
                    }
                } else {
                    AppError::InvalidRequestBody(format!(
                        "chunk {} expects exactly {} bytes",
                        chunk_index, want
                    ))
                });
            }
            tokio::io::AsyncWriteExt::write_all(&mut file_out, &chunk)
                .await
                .map_err(AppError::Io)?;
        }
        tokio::io::AsyncWriteExt::flush(&mut file_out)
            .await
            .map_err(AppError::Io)?;
        file_out.sync_all().await.map_err(AppError::Io)?;
        if written != want {
            return Err(AppError::InvalidRequestBody(format!(
                "chunk {} expects exactly {} bytes (got {})",
                chunk_index, want, written
            )));
        }
        Ok(())
    }
    .await;
    drop(file_out);
    if let Err(e) = write_result {
        let _ = tokio::fs::remove_file(&part).await;
        return Err(e);
    }
    tokio::fs::rename(&part, &final_path)
        .await
        .map_err(AppError::Io)?;
    touch_meta(&staging).await;

    Ok(Json(json!({ "received": chunk_index })))
}

/// POST .../upload-sessions/{sid}/complete — concatenate, verify, finalize.
/// Idempotent outcome: a retried complete re-runs the whole tail; chunk
/// files are only removed after a successful commit.
pub async fn complete_upload_session(
    State(state): State<Arc<AppState>>,
    Path((model_name, version, sid)): Path<(String, String, String)>,
    ApiQuery(query): ApiQuery<UploadQuery>,
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    // Existence + model/version binding check (meta itself is re-read under
    // the lock below — a concurrent complete may have advanced it).
    let (staging, _meta) = load_session(&state, &model_name, &version, &sid).await?;

    let lock = complete_locks()
        .entry(staging.clone())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _guard = lock.lock().await;

    // Re-read under the lock: a concurrent complete may have advanced state.
    let meta = read_meta(&staging).await?;
    if meta.state != SessionState::Uploading {
        return Err(AppError::Conflict(
            "session is already completing".to_string(),
        ));
    }

    // Every declared chunk must be on disk (strict lengths were enforced at
    // PUT time, so presence implies well-formedness).
    for (i, f) in meta.files.iter().enumerate() {
        let expected = expected_chunks(f.size, meta.chunk_size);
        let received = scan_received(&staging, i).await;
        if received.len() as u64 != expected {
            return Err(AppError::Conflict(format!(
                "file '{}' is missing {} chunks",
                f.name,
                expected - received.len() as u64
            )));
        }
    }

    let mut completing = meta.clone();
    completing.state = SessionState::Completing;
    write_meta(&staging, &completing).await?;

    let result = assemble_and_finalize(&state, &model_name, &staging, &meta, &query).await;
    match result {
        Ok(response) => {
            complete_locks().remove(&staging);
            // The commit already moved the version dir out of staging;
            // chunks and the assembled artifacts are session residue.
            let _ = tokio::fs::remove_dir_all(&staging).await;
            info!(model = %model_name, version = %response.1, "Chunked upload completed");
            crate::audit::control_plane(
                Some(&cx),
                &state.access_control,
                crate::callback::Protocol::Http,
                "upload",
                &model_name,
                Some(&response.1),
                "chunked",
            );
            Ok(response.0)
        }
        Err(e) => {
            // Roll back to uploading so the client can fix (re-PUT the bad
            // chunks after a hash mismatch) and retry complete.
            let mut revert = completing;
            revert.state = SessionState::Uploading;
            let _ = write_meta(&staging, &revert).await;
            Err(e)
        }
    }
}

struct FinalizedResponse(Json<Value>, String);

/// Concatenate chunks into the standard staged layout and run the shared
/// upload tail. Returns the success JSON and the effective version.
async fn assemble_and_finalize(
    state: &AppState,
    model_name: &str,
    staging: &std::path::Path,
    meta: &SessionMeta,
    query: &UploadQuery,
) -> Result<FinalizedResponse, AppError> {
    let mut staged: Vec<StagedUploadFile> = Vec::new();
    for (i, f) in meta.files.iter().enumerate() {
        let dest = if f.is_lma {
            staging.join(&f.name)
        } else {
            let version_dir = staging.join(&meta.version);
            tokio::fs::create_dir_all(&version_dir)
                .await
                .map_err(AppError::Io)?;
            version_dir.join(&f.name)
        };
        let assembled = assemble_file(staging, i, f, meta.chunk_size, &dest).await;
        if let Err(e) = assembled {
            let _ = tokio::fs::remove_file(&dest).await;
            return Err(e);
        }
        staged.push(StagedUploadFile {
            name: f.name.clone(),
            path: dest,
            is_lma: f.is_lma,
        });
    }

    let auto_load = query.load.unwrap_or(true);
    let outcome = finalize_upload(
        state,
        model_name,
        staging,
        &staged,
        Some(&meta.version),
        auto_load,
    )
    .await?;

    let mut response = json!({
        "success": true,
        "model": model_name,
        "version": outcome.version,
        "files": outcome.files,
        "loaded": auto_load && outcome.load_error.is_none(),
    });
    if let Some(error) = outcome.load_error {
        response["load_error"] = json!(error);
    }
    Ok(FinalizedResponse(Json(response), outcome.version))
}

/// Stream-concatenate one file's chunks in order into `dest`, verifying the
/// declared size and (when given) sha256. RAM stays O(chunk).
async fn assemble_file(
    staging: &std::path::Path,
    file_index: usize,
    file: &SessionFile,
    chunk_size: u64,
    dest: &std::path::Path,
) -> Result<(), AppError> {
    let expected = expected_chunks(file.size, chunk_size);
    let mut out = tokio::fs::File::create(dest).await.map_err(AppError::Io)?;
    let mut hasher = file.sha256.as_ref().map(|_| sha2::Sha256::new());
    let mut total: u64 = 0;
    for ci in 0..expected {
        let chunk_path = chunks_dir(staging, file_index).join(ci.to_string());
        let data = tokio::fs::read(&chunk_path).await.map_err(AppError::Io)?;
        total += data.len() as u64;
        if let Some(h) = hasher.as_mut() {
            h.update(&data);
        }
        tokio::io::AsyncWriteExt::write_all(&mut out, &data)
            .await
            .map_err(AppError::Io)?;
    }
    tokio::io::AsyncWriteExt::flush(&mut out)
        .await
        .map_err(AppError::Io)?;
    out.sync_all().await.map_err(AppError::Io)?;

    if total != file.size {
        return Err(AppError::InvalidRequestBody(format!(
            "assembled size mismatch for '{}': declared {} bytes, got {}",
            file.name, file.size, total
        )));
    }
    if let (Some(h), Some(declared)) = (hasher, file.sha256.as_ref()) {
        let actual = format!("{:x}", h.finalize());
        if &actual != declared {
            return Err(AppError::InvalidRequestBody(format!(
                "sha256 mismatch for '{}': declared {}, assembled {}",
                file.name, declared, actual
            )));
        }
    }
    Ok(())
}

/// DELETE .../upload-sessions/{sid} — abort and clean the staging dir.
pub async fn delete_upload_session(
    State(state): State<Arc<AppState>>,
    Path((model_name, version, sid)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    let (staging, _meta) = load_session(&state, &model_name, &version, &sid).await?;
    complete_locks().remove(&staging);
    tokio::fs::remove_dir_all(&staging)
        .await
        .map_err(AppError::Io)?;
    Ok(Json(json!({ "success": true })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use std::sync::atomic::AtomicBool;
    use tower::ServiceExt;

    fn test_app_state_with_config(repo_path: std::path::PathBuf, config: Config) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path.clone(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            config,
            repo_path,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    fn test_app_state(repo_path: std::path::PathBuf) -> Arc<AppState> {
        test_app_state_with_config(repo_path, Config::default())
    }

    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/v2/repository/models/:model_name/versions/:version/upload-sessions",
                axum::routing::post(create_upload_session),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/upload-sessions/:session_id",
                axum::routing::get(get_upload_session).delete(delete_upload_session),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/upload-sessions/:session_id/complete",
                axum::routing::post(complete_upload_session),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/upload-sessions/:session_id/files/:file_index/chunks/:chunk_index",
                axum::routing::put(put_session_chunk),
            )
            .with_state(state)
    }

    fn unique_tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lite-server-ups-{}-{}-{}",
            tag,
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    fn json_req(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// init a session, returning its id.
    async fn init_session(app: &Router, model: &str, version: &str, files: Value) -> String {
        let response = app
            .clone()
            .oneshot(json_req(
                "POST",
                &format!("/v2/repository/models/{}/versions/{}/upload-sessions", model, version),
                json!({ "files": files }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        body_json(response).await["session_id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn put_chunk(
        app: &Router,
        model: &str,
        version: &str,
        sid: &str,
        fi: usize,
        ci: u64,
        data: Vec<u8>,
    ) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/v2/repository/models/{}/versions/{}/upload-sessions/{}/files/{}/chunks/{}",
                        model, version, sid, fi, ci
                    ))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(data))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn get_session(app: &Router, model: &str, version: &str, sid: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v2/repository/models/{}/versions/{}/upload-sessions/{}",
                        model, version, sid
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        body_json(response).await
    }

    async fn complete(app: &Router, model: &str, version: &str, sid: &str) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/v2/repository/models/{}/versions/{}/upload-sessions/{}/complete?load=false",
                        model, version, sid
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    // ===== init =====

    #[tokio::test]
    async fn test_init_returns_session_id_and_chunk_size() {
        let tmp = unique_tmp("init");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));

        let response = app
            .oneshot(json_req(
                "POST",
                "/v2/repository/models/mymodel/versions/1/upload-sessions",
                json!({ "files": [{ "name": "model.py", "size": 100 }] }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = body_json(response).await;
        assert!(body["session_id"].as_str().unwrap().len() >= 32);
        assert_eq!(body["chunk_size"].as_u64().unwrap(), default_chunk_size());

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_init_rejects_bad_file_sets() {
        let tmp = unique_tmp("initbad");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let uri = "/v2/repository/models/mymodel/versions/1/upload-sessions";

        for files in [
            json!([]),
            json!([{ "name": ".hidden", "size": 1 }]),
            json!([{ "name": "a.lma", "size": 1 }, { "name": "b.lma", "size": 1 }]),
        ] {
            let response = app
                .clone()
                .oneshot(json_req("POST", uri, json!({ "files": files })))
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "file set must be rejected: {files}"
            );
        }

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_init_rejects_declared_size_over_cap() {
        let tmp = unique_tmp("initcap");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let mut config = Config::default();
        config.server.max_upload_bytes = Some(100);
        let app = test_router(test_app_state_with_config(tmp.clone(), config));

        let response = app
            .oneshot(json_req(
                "POST",
                "/v2/repository/models/mymodel/versions/1/upload-sessions",
                json!({ "files": [{ "name": "weights.bin", "size": 101 }] }),
            ))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "the cap must reject at declaration time, not after bytes landed"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== chunk PUT + GET bitmap =====

    #[tokio::test]
    async fn test_chunk_bitmap_tracks_out_of_order_puts() {
        let tmp = unique_tmp("bitmap");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        // 3 chunks of 64 (192 bytes).
        let sid = init_session(&app, "mymodel", "1", json!([{ "name": "w.bin", "size": 192 }])).await;

        put_chunk(&app, "mymodel", "1", &sid, 0, 2, vec![b'c'; 64]).await;
        put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'a'; 64]).await;

        let session = get_session(&app, "mymodel", "1", &sid).await;
        assert_eq!(
            session["files"][0]["received_chunks"],
            json!([0, 2]),
            "bitmap must reflect exactly the landed chunks"
        );
        assert_eq!(session["files"][0]["complete"], false);

        put_chunk(&app, "mymodel", "1", &sid, 0, 1, vec![b'b'; 64]).await;
        let session = get_session(&app, "mymodel", "1", &sid).await;
        assert_eq!(session["files"][0]["received_chunks"], json!([0, 1, 2]));
        assert_eq!(session["files"][0]["complete"], true);

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_chunk_put_enforces_exact_length_and_bounds() {
        let tmp = unique_tmp("strict");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let sid = init_session(&app, "mymodel", "1", json!([{ "name": "w.bin", "size": 192 }])).await;

        // Non-final chunk with a short body → 400.
        let response = put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'a'; 10]).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        // Out-of-range chunk index → 400.
        let response = put_chunk(&app, "mymodel", "1", &sid, 0, 3, vec![b'a'; 64]).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        // Out-of-range file index → 400.
        let response = put_chunk(&app, "mymodel", "1", &sid, 9, 0, vec![b'a'; 64]).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        // Nothing partial may be counted after the failures.
        let session = get_session(&app, "mymodel", "1", &sid).await;
        assert_eq!(session["files"][0]["received_chunks"], json!([]));

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_chunk_put_is_idempotent() {
        let tmp = unique_tmp("idem");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let sid = init_session(&app, "mymodel", "1", json!([{ "name": "w.bin", "size": 64 }])).await;

        put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'x'; 64]).await;
        put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'y'; 64]).await;
        let session = get_session(&app, "mymodel", "1", &sid).await;
        assert_eq!(session["files"][0]["received_chunks"], json!([0]));

        // The last write wins.
        let response = complete(&app, "mymodel", "1", &sid).await;
        assert_eq!(response.status(), StatusCode::OK);
        let content = tokio::fs::read(tmp.join("mymodel").join("1").join("w.bin"))
            .await
            .unwrap();
        assert_eq!(content, vec![b'y'; 64]);

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== complete =====

    #[tokio::test]
    async fn test_complete_assembles_chunks_and_commits() {
        let tmp = unique_tmp("complete");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));

        let payload: Vec<u8> = (0..200u32).map(|i| (i % 251) as u8).collect();
        let sha = format!("{:x}", sha2::Sha256::digest(&payload));
        let sid = init_session(
            &app,
            "mymodel",
            "1",
            json!([{ "name": "weights.bin", "size": payload.len(), "sha256": sha }]),
        )
        .await;

        // 200 bytes = 3 full chunks of 64 + a 8-byte tail; upload out of order.
        let chunks: Vec<&[u8]> = payload.chunks(64).collect();
        assert_eq!(chunks.len(), 4);
        for (ci, data) in chunks.iter().enumerate().rev() {
            let response = put_chunk(&app, "mymodel", "1", &sid, 0, ci as u64, data.to_vec()).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        let response = complete(&app, "mymodel", "1", &sid).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["version"], "1");

        let committed = tokio::fs::read(tmp.join("mymodel").join("1").join("weights.bin"))
            .await
            .unwrap();
        assert_eq!(committed, payload, "assembled bytes must equal the source file");

        // Staging is gone after a successful complete.
        let mut found_staging = false;
        let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_name().to_string_lossy().starts_with(".tmp-upload-") {
                found_staging = true;
            }
        }
        assert!(!found_staging, "staging must be removed after complete");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_complete_with_missing_chunks_is_409() {
        let tmp = unique_tmp("missing");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let sid = init_session(&app, "mymodel", "1", json!([{ "name": "w.bin", "size": 128 }])).await;

        put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'a'; 64]).await;
        let response = complete(&app, "mymodel", "1", &sid).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        // The session stays resumable.
        let session = get_session(&app, "mymodel", "1", &sid).await;
        assert_eq!(session["state"], "uploading");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_complete_hash_mismatch_keeps_session_retryable() {
        let tmp = unique_tmp("hash");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let bogus = "0".repeat(64);
        let sid = init_session(
            &app,
            "mymodel",
            "1",
            json!([{ "name": "w.bin", "size": 64, "sha256": bogus }]),
        )
        .await;

        put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'a'; 64]).await;
        let response = complete(&app, "mymodel", "1", &sid).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "a sha256 mismatch must reject the complete"
        );

        // The session rolls back to uploading — chunks can be fixed and the
        // complete retried (staging preserved).
        let session = get_session(&app, "mymodel", "1", &sid).await;
        assert_eq!(session["state"], "uploading");
        assert_eq!(session["files"][0]["received_chunks"], json!([0]));

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_complete_multi_file_raw_and_swap_replace() {
        let tmp = unique_tmp("multi");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));

        let sid = init_session(
            &app,
            "mymodel",
            "1",
            json!([
                { "name": "model.py", "size": 64 },
                { "name": "config.yaml", "size": 64 },
            ]),
        )
        .await;
        put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'p'; 64]).await;
        put_chunk(&app, "mymodel", "1", &sid, 1, 0, vec![b'c'; 64]).await;
        let response = complete(&app, "mymodel", "1", &sid).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(tmp.join("mymodel").join("1").join("model.py").exists());
        assert!(tmp.join("mymodel").join("1").join("config.yaml").exists());

        // A second session for the same version replaces the dir wholesale
        // (swap semantics inherited from finalize_upload).
        let sid2 = init_session(&app, "mymodel", "1", json!([{ "name": "model.py", "size": 64 }])).await;
        put_chunk(&app, "mymodel", "1", &sid2, 0, 0, vec![b'n'; 64]).await;
        let response = complete(&app, "mymodel", "1", &sid2).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !tmp.join("mymodel").join("1").join("config.yaml").exists(),
            "re-upload must replace the version directory wholesale"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== delete =====

    #[tokio::test]
    async fn test_delete_session_removes_staging() {
        let tmp = unique_tmp("delete");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let sid = init_session(&app, "mymodel", "1", json!([{ "name": "w.bin", "size": 64 }])).await;
        put_chunk(&app, "mymodel", "1", &sid, 0, 0, vec![b'a'; 64]).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!(
                        "/v2/repository/models/mymodel/versions/1/upload-sessions/{}",
                        sid
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let mut found_staging = false;
        let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_name().to_string_lossy().starts_with(".tmp-upload-") {
                found_staging = true;
            }
        }
        assert!(!found_staging);

        // The session is gone for good.
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v2/repository/models/mymodel/versions/1/upload-sessions/{}",
                        sid
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== cross-checks =====

    #[tokio::test]
    async fn test_session_bound_to_model_and_version() {
        let tmp = unique_tmp("bound");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let sid = init_session(&app, "mymodel", "1", json!([{ "name": "w.bin", "size": 64 }])).await;

        // Same sid under a different version must not resolve.
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v2/repository/models/mymodel/versions/2/upload-sessions/{}",
                        sid
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_active_session_cap() {
        let tmp = unique_tmp("cap");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let app = test_router(test_app_state(tmp.clone()));
        let uri = "/v2/repository/models/mymodel/versions/1/upload-sessions";

        for _ in 0..MAX_ACTIVE_SESSIONS {
            let response = app
                .clone()
                .oneshot(json_req("POST", uri, json!({ "files": [{ "name": "w.bin", "size": 1 }] })))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }
        let response = app
            .oneshot(json_req("POST", uri, json!({ "files": [{ "name": "w.bin", "size": 1 }] })))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "the 9th concurrent session must be refused"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}
