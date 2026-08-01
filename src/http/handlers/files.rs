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

    let target_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;
    tokio::fs::create_dir_all(&target_dir)
        .await
        .map_err(AppError::Io)?;

    let mut uploaded_files: Vec<String> = Vec::new();

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        AppError::Validation(format!("multipart error: {}", e))
    })? {
        let filename = field
            .file_name()
            .unwrap_or("unnamed")
            .to_string();

        let data = field.bytes().await.map_err(|e| {
            AppError::Transport(format!("read upload field: {}", e))
        })?;

        if filename.ends_with(".lma") {
            // Save .lma to temp file, then unpack via Python CLI
            let tmp_dir = std::env::temp_dir().join(format!(
                "lite-server-upload-{}",
                uuid::Uuid::new_v4()
            ));
            tokio::fs::create_dir_all(&tmp_dir)
                .await
                .map_err(AppError::Io)?;
            let tmp_file = tmp_dir.join(&filename);
            tokio::fs::write(&tmp_file, &data)
                .await
                .map_err(AppError::Io)?;

            let output = tokio::process::Command::new(crate::python::resolve_python_interpreter())
                .args([
                    "-m",
                    "lite_server",
                    "unpack",
                    tmp_file.to_str().unwrap_or(""),
                    "--to",
                    target_dir.to_str().unwrap_or(""),
                    "--flat",
                ])
                .output()
                .await
                .map_err(|e| AppError::Internal(format!("failed to run python unpack: {}", e)))?;

            // Clean up temp dir
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(AppError::Validation(format!(
                    "artifact unpack failed: {}",
                    stderr.trim()
                )));
            }
            uploaded_files.push(filename);
        } else {
            // Raw file: write directly to target dir
            // Sanitize filename: strip any path components
            let safe_name = std::path::Path::new(&filename)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if safe_name.is_empty() || safe_name.starts_with('.') {
                continue;
            }
            let file_path = target_dir.join(&safe_name);
            tokio::fs::write(&file_path, &data)
                .await
                .map_err(AppError::Io)?;
            uploaded_files.push(safe_name);
        }
    }

    if uploaded_files.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    // Optionally auto-load after upload
    let auto_load = query.load.unwrap_or(true);
    if auto_load {
        let config_path = target_dir.join("config.yaml");
        let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
        state.config.apply_model_defaults(&mut config);
        if let Err(e) = state.worker_manager.load_model(&model_name, &version, &config).await {
            warn!("Auto-load after upload failed: {}", e);
        }
        let active = state.registry.get_active_version(&model_name);
        if active.is_none() {
            let _ = state.registry.activate_version(&model_name, &version);
        }
    }

    info!(
        model = %model_name,
        version = %version,
        files = ?uploaded_files,
        "Model uploaded"
    );

    Ok(Json(json!({
        "success": true,
        "model": model_name,
        "version": version,
        "files": uploaded_files,
        "loaded": auto_load,
    })))
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
}

