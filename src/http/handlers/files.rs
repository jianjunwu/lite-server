use super::{ApiQuery, AppError, AppState};
use axum::{
    extract::{Multipart, Path, State},
    http::header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    response::{Json, Response},
};
use serde::Deserialize;
use serde_json::{json, Value};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tracing::{info, warn};

// ===== Upload Model =====

#[derive(Deserialize)]
pub struct UploadQuery {
    pub load: Option<bool>,
}

pub async fn upload_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiQuery(query): ApiQuery<UploadQuery>,
    mut multipart: Multipart,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    // For .lma uploads the manifest version (always v-stripped by the
    // packer) names the on-disk version directory; normalize the URL
    // version the same way so unpack, load, and the response all agree.
    let mut effective_version = version.clone();

    // H3: everything lands in a staging dir first and is moved into place
    // atomically after the whole request succeeds. Failures and timeouts
    // leave no partial files and no empty version dirs: the guard removes
    // staging, and the target is only created by the final rename.
    let staging = state
        .repo_path
        .join(format!(".tmp-upload-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&staging)
        .await
        .map_err(AppError::Io)?;
    let _staging_guard = StagingGuard(staging.clone());

    let mut uploaded_files: Vec<String> = Vec::new();
    let mut total_bytes: u64 = 0;

    while let Some(mut field) = multipart.next_field().await.map_err(|e| {
        AppError::Validation(format!("multipart error: {}", e))
    })? {
        let filename = field
            .file_name()
            .unwrap_or("unnamed")
            .to_string();

        if filename.ends_with(".lma") {
            // The artifact's internal layout carries the version directory
            // prefix ({version}/...), so unpack --flat into the staging
            // root — mirroring the scanner's repository-root auto-unpack —
            // instead of the version directory (which would nest
            // {name}/{v}/{v}/). --expect-version fails before extraction
            // if the manifest version does not match the upload URL.
            effective_version = version.strip_prefix('v').unwrap_or(&version).to_string();
            let tmp_file = staging.join(&filename);
            total_bytes += stream_field_to_file(&mut field, &tmp_file).await?;

            // H1: bound the unpack subprocess with the scanner's tunable.
            let unpack_timeout = std::time::Duration::from_secs_f32(
                state.config.tunables.unpack_timeout_secs,
            );
            let output = run_unpack(
                &crate::python::resolve_python_interpreter(),
                &tmp_file,
                &staging,
                &effective_version,
                unpack_timeout,
            )
            .await?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(AppError::Validation(format!(
                    "artifact unpack failed: {}",
                    stderr.trim()
                )));
            }

            // F10a: retain the original artifact so downloads can serve it
            // back without repacking (preserving the author signature).
            let artifacts_dir = state.repo_path.join(".artifacts");
            tokio::fs::create_dir_all(&artifacts_dir)
                .await
                .map_err(AppError::Io)?;
            let artifact_name = format!("{}_v{}.lma", model_name, effective_version);
            tokio::fs::copy(&tmp_file, artifacts_dir.join(&artifact_name))
                .await
                .map_err(AppError::Io)?;

            uploaded_files.push(filename);
        } else {
            // Raw file: stage into the version directory (F11a streams the
            // field to disk instead of buffering it in memory). Sanitize
            // filename: strip any path components.
            let safe_name = std::path::Path::new(&filename)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if safe_name.is_empty() || safe_name.starts_with('.') {
                continue;
            }
            let version_dir = staging.join(&version);
            tokio::fs::create_dir_all(&version_dir)
                .await
                .map_err(AppError::Io)?;
            let file_path = version_dir.join(&safe_name);
            total_bytes += stream_field_to_file(&mut field, &file_path).await?;
            uploaded_files.push(safe_name);
        }
    }

    if uploaded_files.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    // H3: move staged content into place — version dirs via swap semantics
    // (replaced wholesale, never partial), model-root files by overwrite.
    commit_staging(&state.repo_path, &model_name, &staging).await?;

    // Optionally auto-load after upload; `loaded` reports the real outcome
    // instead of echoing the ?load= query param.
    let auto_load = query.load.unwrap_or(true);
    let mut load_error: Option<String> = None;
    if auto_load {
        let load_dir = state.repo_path.join(&model_name).join(&effective_version);
        let config_path = load_dir.join("config.yaml");
        let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
        state.config.apply_model_defaults(&mut config);
        if let Err(e) = state
            .worker_manager
            .load_model(&model_name, &effective_version, &config)
            .await
        {
            warn!("Auto-load after upload failed: {}", e);
            load_error = Some(e.to_string());
        }
        let active = state.registry.get_active_version(&model_name);
        if active.is_none() {
            let _ = state.registry.activate_version(&model_name, &effective_version);
        }
    }

    info!(
        model = %model_name,
        version = %effective_version,
        files = ?uploaded_files,
        bytes = total_bytes,
        "Model uploaded"
    );

    let mut response = json!({
        "success": true,
        "model": model_name,
        "version": effective_version,
        "files": uploaded_files,
        "loaded": auto_load && load_error.is_none(),
    });
    if let Some(error) = load_error {
        response["load_error"] = json!(error);
    }

    Ok(Json(response))
}

/// Stream a multipart field to `dest`, returning the bytes written
/// (F11a: RAM bounded by chunk size instead of buffering the whole field).
async fn stream_field_to_file(
    field: &mut axum::extract::multipart::Field<'_>,
    dest: &std::path::Path,
) -> Result<u64, AppError> {
    let mut file = tokio::fs::File::create(dest).await.map_err(AppError::Io)?;
    let mut total: u64 = 0;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| AppError::Transport(format!("read upload field: {}", e)))?
    {
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
            .await
            .map_err(AppError::Io)?;
        total += chunk.len() as u64;
    }
    tokio::io::AsyncWriteExt::flush(&mut file)
        .await
        .map_err(AppError::Io)?;
    Ok(total)
}

/// Run the Python unpack CLI against `tmp_file`, extracting flat into
/// `dest_root` and enforcing the manifest version. Bounded by `timeout`
/// (H1) — a hung unpack is killed instead of holding the upload forever.
async fn run_unpack(
    interpreter: &str,
    tmp_file: &std::path::Path,
    dest_root: &std::path::Path,
    expect_version: &str,
    timeout: std::time::Duration,
) -> Result<std::process::Output, AppError> {
    let mut child = tokio::process::Command::new(interpreter)
        .args([
            "-m",
            "lite_server",
            "unpack",
            tmp_file.to_str().unwrap_or(""),
            "--to",
            dest_root.to_str().unwrap_or(""),
            "--flat",
            "--expect-version",
            expect_version,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| AppError::Internal(format!("failed to run python unpack: {}", e)))?;

    // wait() (not wait_with_output) keeps the child available for kill on
    // timeout; stdout/stderr are drained manually afterwards.
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => {
            let status = result
                .map_err(|e| AppError::Internal(format!("python unpack failed: {}", e)))?;
            let mut stdout = Vec::new();
            if let Some(mut out) = child.stdout.take() {
                let _ = tokio::io::AsyncReadExt::read_to_end(&mut out, &mut stdout).await;
            }
            let mut stderr = Vec::new();
            if let Some(mut err) = child.stderr.take() {
                let _ = tokio::io::AsyncReadExt::read_to_end(&mut err, &mut stderr).await;
            }
            Ok(std::process::Output {
                status,
                stdout,
                stderr,
            })
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(AppError::Internal(format!(
                "artifact unpack timed out after {:.0}s",
                timeout.as_secs_f32()
            )))
        }
    }
}

/// Move a staged version directory into place with swap semantics:
/// the existing target is renamed aside (dot-prefixed, invisible to the
/// scanner) and removed after the new directory lands; a failed rename
/// rolls the old directory back.
async fn swap_dir_into(src: &std::path::Path, dst: &std::path::Path) -> Result<(), AppError> {
    if dst.exists() {
        let backup = dst.with_file_name(format!(
            ".{}.old-{}",
            dst.file_name().unwrap_or_default().to_string_lossy(),
            uuid::Uuid::new_v4()
        ));
        tokio::fs::rename(dst, &backup).await.map_err(AppError::Io)?;
        match tokio::fs::rename(src, dst).await {
            Ok(()) => {
                let _ = tokio::fs::remove_dir_all(&backup).await;
                Ok(())
            }
            Err(e) => {
                // Rollback: restore the previous version directory.
                let _ = tokio::fs::rename(&backup, dst).await;
                Err(AppError::Io(e))
            }
        }
    } else {
        tokio::fs::rename(src, dst).await.map_err(AppError::Io)
    }
}

/// Move all staged content into the model directory: directories become
/// version dirs (swap semantics), files (manifest.json, requirements.txt)
/// overwrite their model-root counterparts.
async fn commit_staging(
    repo_path: &std::path::Path,
    model_name: &str,
    staging: &std::path::Path,
) -> Result<(), AppError> {
    let model_root = repo_path.join(model_name);
    tokio::fs::create_dir_all(&model_root)
        .await
        .map_err(AppError::Io)?;

    let mut entries = tokio::fs::read_dir(staging)
        .await
        .map_err(AppError::Io)?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let src = entry.path();
        if src.is_dir() {
            let dst = model_root.join(entry.file_name());
            swap_dir_into(&src, &dst).await?;
        } else {
            let dst = model_root.join(entry.file_name());
            // Overwrite semantics: rename replaces atomically on Unix but
            // fails if the destination exists on Windows.
            if dst.exists() {
                tokio::fs::remove_file(&dst).await.map_err(AppError::Io)?;
            }
            tokio::fs::rename(&src, &dst).await.map_err(AppError::Io)?;
        }
    }
    Ok(())
}

/// Removes the staging directory on drop. Drop cannot await, so the
/// removal is spawned; a crashed process leaves the residue for startup
/// cleanup (H7) — invisible to scanner/index via the dot-directory skip.
struct StagingGuard(std::path::PathBuf);

impl Drop for StagingGuard {
    fn drop(&mut self) {
        let path = self.0.clone();
        tokio::spawn(async move {
            let _ = tokio::fs::remove_dir_all(&path).await;
        });
    }
}

// ===== Download Model =====

#[derive(Deserialize)]
pub struct DownloadQuery {
    pub file: Option<String>,
}

pub async fn download_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiQuery(query): ApiQuery<DownloadQuery>,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;

    if !model_dir.exists() {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    // Single file download
    if let Some(ref file_name) = query.file {
        // Validate file name doesn't contain path separators
        if file_name.contains("..") || file_name.contains('/') || file_name.contains('\\') {
            return Err(AppError::Validation("invalid file name".to_string()));
        }
        let file_path = model_dir.join(file_name);
        // Ensure resolved path is inside model_dir
        let canonical_file = file_path.canonicalize().map_err(AppError::Io)?;
        let canonical_dir = model_dir.canonicalize().map_err(AppError::Io)?;
        if !canonical_file.starts_with(&canonical_dir) {
            return Err(AppError::Validation("path traversal rejected".to_string()));
        }

        let data = tokio::fs::read(&canonical_file)
            .await
            .map_err(AppError::Io)?;
        let content_type = if file_name.ends_with(".py") || file_name.ends_with(".yaml") || file_name.ends_with(".yml") || file_name.ends_with(".json") || file_name.ends_with(".txt") || file_name.ends_with(".md") {
            "text/plain; charset=utf-8"
        } else {
            "application/octet-stream"
        };

        let response = Response::builder()
            .header(CONTENT_TYPE, content_type)
            .header(
                CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", file_name),
            )
            .body(axum::body::Body::from(data))
            .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
        return Ok(response);
    }

    // Full directory download as .lma
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-download-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&tmp_dir)
        .await
        .map_err(AppError::Io)?;

    let output = tokio::process::Command::new(crate::python::resolve_python_interpreter())
        .args([
            "-m",
            "lite_server",
            "pack",
            model_dir.to_str().unwrap_or(""),
            "--version",
            &version,
            "--output",
            tmp_dir.to_str().unwrap_or(""),
        ])
        .output()
        .await
        .map_err(|e| AppError::Internal(format!("failed to run python pack: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        return Err(AppError::Internal(format!("pack failed: {}", stderr.trim())));
    }

    // Find the generated .lma file
    let mut lma_file = None;
    let mut entries = tokio::fs::read_dir(&tmp_dir)
        .await
        .map_err(AppError::Io)?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.path().extension().map(|e| e == "lma").unwrap_or(false) {
            lma_file = Some(entry.path());
            break;
        }
    }

    let lma_path = lma_file.ok_or_else(|| {
        AppError::Internal("pack produced no .lma file".to_string())
    })?;

    let data = tokio::fs::read(&lma_path)
        .await
        .map_err(AppError::Io)?;
    let artifact_name = lma_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Clean up temp dir
    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;

    let response = Response::builder()
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", artifact_name),
        )
        .body(axum::body::Body::from(data))
        .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
    Ok(response)
}

// ===== List Files =====

pub async fn list_files_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;

    if !model_dir.exists() {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&model_dir)
        .await
        .map_err(AppError::Io)?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let metadata = entry.metadata().await.ok();
        let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let modified = metadata
            .as_ref()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());

        files.push(json!({
            "name": name,
            "size": size,
            "modified": modified,
            "is_dir": path.is_dir(),
        }));
    }

    Ok(Json(json!({
        "model": model_name,
        "version": version,
        "files": files,
    })))
}

#[cfg(test)]
mod upload_download_tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    fn test_app_state(repo_path: std::path::PathBuf) -> Arc<AppState> {
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
            Config::default(),
            repo_path,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/v2/repository/models/:model_name/versions/:version/upload",
                axum::routing::post(upload_model_handler),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/download",
                axum::routing::get(download_model_handler),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/files",
                axum::routing::get(list_files_handler),
            )
            .with_state(state)
    }

    // ===== List Files Tests =====

    #[tokio::test]
    async fn test_list_files_returns_directory_contents() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-list-test-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "print('hello')")
            .await
            .unwrap();
        tokio::fs::write(model_dir.join("config.yaml"), "max_batch_size: 1")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["model"], "mymodel");
        assert_eq!(json["version"], "1");
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);

        let names: Vec<&str> = files.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"model.py"));
        assert!(names.contains(&"config.yaml"));

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_list_files_returns_404_for_missing_model() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-list-404-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/nonexistent/versions/1/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== Download Single File Tests =====

    #[tokio::test]
    async fn test_download_single_file() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-test-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=model.py")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let disposition = response
            .headers()
            .get(CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(disposition.contains("model.py"));

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"def predict(x): return x");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_download_rejects_path_traversal() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-traversal-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "test")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=../../../etc/passwd")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== Upload Tests =====

    #[tokio::test]
    async fn test_upload_raw_file() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-test-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        // Build multipart body manually
        let boundary = "----testboundary123";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"model.py\"\r\n\
             Content-Type: text/x-python\r\n\r\n\
             def predict(x): return x\r\n\
             --{boundary}--\r\n"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["model"], "mymodel");
        assert_eq!(json["version"], "1");
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "model.py");

        // Verify file was written
        let content = tokio::fs::read_to_string(tmp.join("mymodel").join("1").join("model.py"))
            .await
            .unwrap();
        assert_eq!(content, "def predict(x): return x");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_upload_rejects_invalid_model_name() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-invalid-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----testboundary123";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"model.py\"\r\n\r\n\
             test\r\n\
             --{boundary}--\r\n"
        );

        // Use a model name with invalid characters (space) that still matches the route
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/bad%20name/versions/1/upload")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== .lma Upload Tests =====

    /// Pack a minimal model fixture into a `.lma` artifact via the Python CLI.
    /// Model files live under `<tmp>/src/{name}/{version}/`; the artifact is
    /// written to `<tmp>/pkgs/{name}_v{version}.lma`.
    async fn pack_fixture_lma(
        tmp: &std::path::Path,
        name: &str,
        version: &str,
    ) -> std::path::PathBuf {
        let version_dir = tmp.join("src").join(name).join(version);
        tokio::fs::create_dir_all(&version_dir).await.unwrap();
        tokio::fs::write(version_dir.join("model.py"), "def predict(x): return x\n")
            .await
            .unwrap();
        tokio::fs::write(version_dir.join("config.yaml"), "max_batch_size: 1\n")
            .await
            .unwrap();

        let pkgs_dir = tmp.join("pkgs");
        tokio::fs::create_dir_all(&pkgs_dir).await.unwrap();

        let output = tokio::process::Command::new(crate::python::resolve_python_interpreter())
            .args([
                "-m",
                "lite_server.cli",
                "pack",
                tmp.join("src").join(name).to_str().unwrap(),
                "--version",
                version,
                "--output",
                pkgs_dir.to_str().unwrap(),
            ])
            .output()
            .await
            .expect("failed to run lite-server pack");
        assert!(
            output.status.success(),
            "pack failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        pkgs_dir.join(format!("{}_v{}.lma", name, version))
    }

    /// Build a minimal multipart body carrying one binary file field.
    fn multipart_body(boundary: &str, filename: &str, data: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(
            format!(
                "--{boundary}\r\n\
                 Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
                 Content-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        body
    }

    #[tokio::test]
    async fn test_upload_lma_places_files_without_nesting() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-lma-upload-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let lma = pack_fixture_lma(&tmp, "mymodel", "1").await;
        let data = tokio::fs::read(&lma).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----lmatestboundary";
        let body = multipart_body(boundary, "mymodel_v1.lma", &data);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["loaded"], false, "load=false must report loaded=false");

        // Version files must land directly under {name}/{v} — the artifact's
        // internal version prefix must not nest a duplicate directory.
        let model_py = tmp.join("mymodel").join("1").join("model.py");
        assert!(
            model_py.exists(),
            "model.py must land at {{name}}/{{v}}/model.py"
        );
        assert!(
            !tmp.join("mymodel").join("1").join("1").exists(),
            "unpack must not nest a duplicate version directory"
        );
        // Canonical layout: manifest lands at the model root, matching the
        // repository-root auto-unpack path in the scanner.
        assert!(tmp.join("mymodel").join("manifest.json").exists());

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_upload_lma_rejects_manifest_version_mismatch() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-lma-mismatch-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let lma = pack_fixture_lma(&tmp, "mymodel", "1").await;
        let data = tokio::fs::read(&lma).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----lmatestboundary";
        let body = multipart_body(boundary, "mymodel_v1.lma", &data);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/2/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // The check must fail before extraction: neither the packaged
        // version nor the requested one may appear on disk.
        assert!(
            !tmp.join("mymodel").join("1").exists(),
            "packaged version must not be extracted on mismatch"
        );
        assert!(
            !tmp.join("mymodel").join("2").join("model.py").exists(),
            "requested version must not contain files on mismatch"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== Staging / Atomic Placement Tests (plan H3/H4, F10a) =====

    #[tokio::test]
    async fn test_upload_corrupted_lma_leaves_no_residue() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-lma-corrupt-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----lmatestboundary";
        let body = multipart_body(boundary, "mymodel_v1.lma", b"this is not a zip");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // H3: a failed upload must leave nothing behind — no empty version
        // directory, no staging residue, no retained artifact. The staging
        // guard's removal is spawned, so wait briefly for it.
        assert!(
            !tmp.join("mymodel").exists(),
            "failed upload must not leave a model directory"
        );
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        loop {
            let mut residue = false;
            let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(".tmp-upload") {
                    residue = true;
                }
            }
            if !residue {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "staging residue after failed upload"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        assert!(
            !tmp.join(".artifacts").exists(),
            "failed upload must not retain an artifact"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_upload_replaces_version_dir_swap_semantics() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-swap-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);
        let boundary = "----swaptestboundary";

        // First upload: two files.
        let body = multipart_body(
            boundary,
            "model.py",
            b"def predict(x): return 'A'\n",
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Add a second file directly to the version dir (simulating an
        // earlier upload of a larger file set).
        tokio::fs::write(tmp.join("mymodel").join("1").join("old.txt"), "stale")
            .await
            .unwrap();

        // Second upload: only model.py. Swap semantics must replace the
        // whole version directory, so old.txt must disappear.
        let body = multipart_body(
            boundary,
            "model.py",
            b"def predict(x): return 'B'\n",
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content = tokio::fs::read_to_string(tmp.join("mymodel").join("1").join("model.py"))
            .await
            .unwrap();
        assert_eq!(content, "def predict(x): return 'B'\n");
        assert!(
            !tmp.join("mymodel").join("1").join("old.txt").exists(),
            "re-upload must replace the version directory wholesale"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_upload_lma_retains_artifact_in_artifacts_dir() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-lma-retain-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let lma = pack_fixture_lma(&tmp, "mymodel", "1").await;
        let data = tokio::fs::read(&lma).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----lmatestboundary";
        let body = multipart_body(boundary, "mymodel_v1.lma", &data);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // F10a: the original .lma must be retained byte-identically in
        // <repo>/.artifacts/{name}_v{version}.lma (downloads can then
        // serve it back without repacking and keep the author signature).
        let retained = tmp.join(".artifacts").join("mymodel_v1.lma");
        assert!(retained.exists(), "original artifact must be retained");
        let retained_bytes = tokio::fs::read(&retained).await.unwrap();
        assert_eq!(retained_bytes, data, "retained artifact must be byte-identical");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== H1: unpack subprocess timeout =====

    /// A hung unpack must be killed after the timeout (H1) instead of
    /// holding the upload forever. Uses a fake interpreter so the test
    /// does not touch the process-global python resolution env vars.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_run_unpack_times_out_hung_process() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = std::env::temp_dir().join(format!(
            "lite-server-unpack-timeout-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        // A "python" that never returns.
        let fake_py = tmp.join("fake-python");
        tokio::fs::write(&fake_py, "#!/bin/sh\nsleep 60\n")
            .await
            .unwrap();
        tokio::fs::set_permissions(&fake_py, std::fs::Permissions::from_mode(0o755))
            .await
            .unwrap();

        let lma = tmp.join("fake.lma");
        tokio::fs::write(&lma, "not a zip").await.unwrap();
        let dest = tmp.join("out");

        let result = run_unpack(
            fake_py.to_str().unwrap(),
            &lma,
            &dest,
            "1",
            std::time::Duration::from_millis(500),
        )
        .await;

        let err = result.expect_err("unpack must fail on timeout");
        match err {
            AppError::Internal(msg) => {
                assert!(msg.contains("timed out"), "unexpected message: {}", msg);
            }
            _ => panic!("expected Internal timeout error"),
        }

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}

