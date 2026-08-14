use super::{ApiQuery, AppError, AppState};
use crate::request_context::RequestContext;
use axum::{
    extract::{Multipart, Path, State},
    http::header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE},
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
    let mut has_raw = false;

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
            // H2: bound concurrent pack/unpack subprocesses.
            let _permit = acquire_file_op_permit(unpack_timeout).await?;
            let output = run_unpack(
                &crate::python::resolve_python_interpreter(),
                &tmp_file,
                &staging,
                Some(&effective_version),
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
            has_raw = true;
        }

        // F11b: per-field cumulative enforcement of the upload size cap.
        if let Some(max) = state.config.server.max_upload_bytes {
            if total_bytes > max {
                return Err(AppError::PayloadTooLarge {
                    max_size: max as usize,
                    actual_size: Some(total_bytes),
                });
            }
        }
    }

    if uploaded_files.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    // H3: move staged content into place — version dirs via swap semantics
    // (replaced wholesale, never partial), model-root files by overwrite.
    commit_staging(&state.repo_path, &model_name, &staging).await?;

    // C2 (drift patch): a raw-file upload replaced the version content —
    // drop the stale original artifact (if any), or F10b would keep
    // serving the old package as the truth. Downloads then fall back to
    // repacking the new disk tree. Only after a successful commit — a
    // failed upload must not destroy the previous artifact.
    if has_raw {
        let _ = crate::http::handlers::admin::remove_linked_artifacts(
            &state.repo_path,
            &model_name,
            &version,
        )
        .await;
    }

    // Optionally auto-load after upload; `loaded` reports the real outcome
    // instead of echoing the ?load= query param.
    let auto_load = query.load.unwrap_or(true);
    let load_error = auto_load_uploaded(&state, &model_name, &effective_version, auto_load).await;

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

/// Auto-load an uploaded version (shared by both upload endpoints).
/// Returns the load error detail (None when not requested or on success).
async fn auto_load_uploaded(
    state: &AppState,
    model_name: &str,
    version: &str,
    auto_load: bool,
) -> Option<String> {
    if !auto_load {
        return None;
    }
    let load_dir = state.repo_path.join(model_name).join(version);
    let config_path = load_dir.join("config.yaml");
    let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
    state.config.apply_model_defaults(&mut config);
    if let Err(e) = state
        .worker_manager
        .load_model(model_name, version, &config)
        .await
    {
        warn!("Auto-load after upload failed: {}", e);
        return Some(e.to_string());
    }
    let active = state.registry.get_active_version(model_name);
    if active.is_none() {
        let _ = state.registry.activate_version(model_name, version);
    }
    None
}

/// F8: model-level upload — accepts exactly one `.lma` artifact; the
/// version comes from the package manifest (the URL carries no version).
/// Reuses the staging/swap/auto-load pipeline of the versioned endpoint.
pub async fn upload_model_package_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    ApiQuery(query): ApiQuery<UploadQuery>,
    cx: RequestContext,
    mut multipart: Multipart,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;

    let staging = state
        .repo_path
        .join(format!(".tmp-upload-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&staging)
        .await
        .map_err(AppError::Io)?;
    let _staging_guard = StagingGuard(staging.clone());

    let mut uploaded_files: Vec<String> = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut effective_version: Option<String> = None;

    while let Some(mut field) = multipart.next_field().await.map_err(|e| {
        AppError::Validation(format!("multipart error: {}", e))
    })? {
        let filename = field.file_name().unwrap_or("unnamed").to_string();

        if !filename.ends_with(".lma") {
            return Err(AppError::InvalidRequestBody(format!(
                "model-level upload accepts a single .lma artifact (got '{}'); \
                 raw files need the versioned endpoint \
                 /v2/repository/models/{}/versions/{{v}}/upload",
                filename, model_name
            )));
        }
        if effective_version.is_some() {
            return Err(AppError::InvalidRequestBody(
                "model-level upload accepts a single .lma artifact".to_string(),
            ));
        }

        let tmp_file = staging.join(&filename);
        total_bytes += stream_field_to_file(&mut field, &tmp_file).await?;
        if let Some(max) = state.config.server.max_upload_bytes {
            if total_bytes > max {
                return Err(AppError::PayloadTooLarge {
                    max_size: max as usize,
                    actual_size: Some(total_bytes),
                });
            }
        }

        // Unpack without a version expectation — the version is read from
        // the manifest below and validated against the URL model name.
        let unpack_timeout = std::time::Duration::from_secs_f32(
            state.config.tunables.unpack_timeout_secs,
        );
        // H2: bound concurrent pack/unpack subprocesses.
        let _permit = acquire_file_op_permit(unpack_timeout).await?;
        let output = run_unpack(
            &crate::python::resolve_python_interpreter(),
            &tmp_file,
            &staging,
            None,
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

        // The unpacked manifest.json sits at the staging root.
        let manifest_raw = tokio::fs::read_to_string(staging.join("manifest.json"))
            .await
            .map_err(|_| AppError::Validation("artifact manifest.json missing".to_string()))?;
        let manifest: Value = serde_json::from_str(&manifest_raw)
            .map_err(|_| AppError::Validation("artifact manifest.json invalid".to_string()))?;
        let manifest_name = manifest["name"]
            .as_str()
            .ok_or_else(|| AppError::Validation("artifact manifest lacks a name".to_string()))?;
        if manifest_name != model_name {
            return Err(AppError::InvalidRequestBody(format!(
                "artifact is for model '{}', not '{}'",
                manifest_name, model_name
            )));
        }
        let mversion = manifest["version"]
            .as_str()
            .ok_or_else(|| AppError::Validation("artifact manifest lacks a version".to_string()))?;
        let v = mversion.strip_prefix('v').unwrap_or(mversion).to_string();
        crate::validation::validate_version(&v)?;
        effective_version = Some(v);
        uploaded_files.push(filename);

        // F10a: retain the original artifact (same as the versioned path).
        let artifacts_dir = state.repo_path.join(".artifacts");
        tokio::fs::create_dir_all(&artifacts_dir)
            .await
            .map_err(AppError::Io)?;
        let artifact_name = format!("{}_v{}.lma", model_name, effective_version.as_deref().unwrap_or(""));
        tokio::fs::copy(&tmp_file, artifacts_dir.join(&artifact_name))
            .await
            .map_err(AppError::Io)?;
    }

    let Some(effective_version) = effective_version else {
        return Err(AppError::Validation(
            "no .lma artifact in upload".to_string(),
        ));
    };
    if uploaded_files.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    commit_staging(&state.repo_path, &model_name, &staging).await?;

    let auto_load = query.load.unwrap_or(true);
    let load_error = auto_load_uploaded(&state, &model_name, &effective_version, auto_load).await;

    info!(
        model = %model_name,
        version = %effective_version,
        files = ?uploaded_files,
        bytes = total_bytes,
        "Model package uploaded (model-level)"
    );

    crate::audit::control_plane(
        Some(&cx),
        &state.access_control,
        crate::callback::Protocol::Http,
        "upload",
        &model_name,
        Some(&effective_version),
        "model-level",
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

/// H2/C7: bounds concurrent file-operation subprocesses (pack/unpack) —
/// permit=4, acquire with a timeout so a busy server fails fast instead of
/// queueing forever (starvation protection per the plan).
static FILE_OP_SEM: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();

fn file_op_sem() -> &'static tokio::sync::Semaphore {
    FILE_OP_SEM.get_or_init(|| tokio::sync::Semaphore::new(4))
}

async fn acquire_file_op_permit(
    timeout: std::time::Duration,
) -> Result<tokio::sync::SemaphorePermit<'static>, AppError> {
    match tokio::time::timeout(timeout, file_op_sem().acquire()).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => Err(AppError::Internal("file-op semaphore closed".to_string())),
        Err(_) => Err(AppError::Internal(format!(
            "file operation capacity exhausted after {:.0}s",
            timeout.as_secs_f32()
        ))),
    }
}

/// H1: wait for a child process bounded by `timeout` — a hung subprocess
/// is killed instead of holding the request open forever. wait() (not
/// wait_with_output) keeps the child available for kill on timeout;
/// stdout/stderr are drained manually afterwards.
async fn wait_child_bounded(
    child: &mut tokio::process::Child,
    timeout: std::time::Duration,
    what: &str,
) -> Result<std::process::Output, AppError> {
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => {
            let status = result
                .map_err(|e| AppError::Internal(format!("python {what} failed: {}", e)))?;
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
                "{what} timed out after {:.0}s",
                timeout.as_secs_f32()
            )))
        }
    }
}

/// Run the Python unpack CLI against `tmp_file`, extracting flat into
/// `dest_root` and enforcing the manifest version. Bounded by `timeout`
/// (H1) — a hung unpack is killed instead of holding the upload forever.
/// `expect_version: None` skips the version check (F8 model-level upload
/// reads the version from the manifest afterwards instead).
async fn run_unpack(
    interpreter: &str,
    tmp_file: &std::path::Path,
    dest_root: &std::path::Path,
    expect_version: Option<&str>,
    timeout: std::time::Duration,
) -> Result<std::process::Output, AppError> {
    let mut cmd = tokio::process::Command::new(interpreter);
    cmd.args([
        "-m",
        "lite_server",
        "unpack",
        tmp_file.to_str().unwrap_or(""),
        "--to",
        dest_root.to_str().unwrap_or(""),
        "--flat",
    ]);
    if let Some(v) = expect_version {
        cmd.args(["--expect-version", v]);
    }
    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| AppError::Internal(format!("failed to run python unpack: {}", e)))?;
    wait_child_bounded(&mut child, timeout, "artifact unpack").await
}

/// H1: run the Python pack CLI bounded by `timeout` (the download
/// counterpart of run_unpack).
async fn run_pack(
    interpreter: &str,
    model_dir: &std::path::Path,
    version: &str,
    output_dir: &std::path::Path,
    timeout: std::time::Duration,
) -> Result<std::process::Output, AppError> {
    let mut child = tokio::process::Command::new(interpreter)
        .args([
            "-m",
            "lite_server",
            "pack",
            model_dir.to_str().unwrap_or(""),
            "--version",
            version,
            "--output",
            output_dir.to_str().unwrap_or(""),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| AppError::Internal(format!("failed to run python pack: {}", e)))?;
    wait_child_bounded(&mut child, timeout, "artifact pack").await
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

/// F2: removes the pack temp dir once the response body stream is dropped
/// (completion or client abort) — the .lma file lives inside it, so
/// removal must wait for the stream's end. Pre-response failure paths
/// clean the dir directly instead.
struct DownloadCleanup(std::path::PathBuf);

impl Drop for DownloadCleanup {
    fn drop(&mut self) {
        let path = self.0.clone();
        tokio::spawn(async move {
            let _ = tokio::fs::remove_dir_all(&path).await;
        });
    }
}

/// F1: streams the packed .lma file as the response body (memory O(chunk)
/// instead of O(file)) and removes its temp dir when the stream is done.
#[pin_project::pin_project]
struct LmaDownloadBody {
    #[pin]
    inner: tokio_util::io::ReaderStream<tokio::fs::File>,
    _cleanup: DownloadCleanup,
}

impl futures::Stream for LmaDownloadBody {
    type Item = std::io::Result<bytes::Bytes>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pub async fn download_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiQuery(query): ApiQuery<DownloadQuery>,
    cx: RequestContext,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    download_version_impl(&state, &cx, &model_name, &version, query.file.as_deref()).await
}

/// Shared download core (versioned endpoint + F9's model-level endpoint).
async fn download_version_impl(
    state: &AppState,
    cx: &RequestContext,
    model_name: &str,
    version: &str,
    file: Option<&str>,
) -> Result<Response, AppError> {
    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, model_name, version)?;

    if !model_dir.exists() {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    // Single file download
    if let Some(file_name) = file {
        // Validate file name doesn't contain path separators
        if file_name.contains("..") || file_name.contains('/') || file_name.contains('\\') {
            return Err(AppError::Validation("invalid file name".to_string()));
        }
        let file_path = model_dir.join(file_name);
        // F6: a directory is not a downloadable file — 400, not the raw
        // Io 500 the read path would produce. A missing file is a 404.
        if file_path.is_dir() {
            return Err(AppError::Validation(format!(
                "{} is a directory; ?file= must name a single file",
                file_name
            )));
        }
        if !file_path.exists() {
            return Err(AppError::ModelNotFound(format!(
                "file {} not found in {} version {}",
                file_name, model_name, version
            )));
        }
        // Ensure resolved path is inside model_dir
        let canonical_file = file_path.canonicalize().map_err(AppError::Io)?;
        let canonical_dir = model_dir.canonicalize().map_err(AppError::Io)?;
        if !canonical_file.starts_with(&canonical_dir) {
            return Err(AppError::Validation("path traversal rejected".to_string()));
        }

        // F1: stream the file instead of buffering it whole; Content-Length
        // lets the client see progress/size up front.
        let size = tokio::fs::metadata(&canonical_file)
            .await
            .map_err(AppError::Io)?
            .len();
        let file = tokio::fs::File::open(&canonical_file)
            .await
            .map_err(AppError::Io)?;
        let content_type = if file_name.ends_with(".py") || file_name.ends_with(".yaml") || file_name.ends_with(".yml") || file_name.ends_with(".json") || file_name.ends_with(".txt") || file_name.ends_with(".md") {
            "text/plain; charset=utf-8"
        } else {
            "application/octet-stream"
        };

        crate::audit::control_plane(
            Some(cx),
            &state.access_control,
            crate::callback::Protocol::Http,
            "download",
            model_name,
            Some(version),
            &format!("file={}", file_name),
        );

        let response = Response::builder()
            .header(CONTENT_TYPE, content_type)
            .header(CONTENT_LENGTH, size.to_string())
            .header(
                CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", file_name),
            )
            .body(axum::body::Body::from_stream(
                tokio_util::io::ReaderStream::new(file),
            ))
            .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
        return Ok(response);
    }

    // Full directory download as .lma.

    // F10b: serve the original artifact when one exists — byte-identical
    // to what was uploaded/placed (author signature preserved) and free
    // (no repack). The artifact is the record of truth; drift between it
    // and the disk tree is surfaced by the E4 drift report.
    let artifact_name = format!("{}_v{}.lma", model_name, version);
    for dir in [state.repo_path.to_path_buf(), state.repo_path.join(".artifacts")] {
        let candidate = dir.join(&artifact_name);
        if candidate.is_file() {
            return serve_artifact_file(state, cx, model_name, version, &candidate, &artifact_name)
                .await;
        }
    }

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-download-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&tmp_dir)
        .await
        .map_err(AppError::Io)?;

    // H2: bound concurrent pack/unpack subprocesses; H1: bound this one
    // in time.
    let pack_timeout = std::time::Duration::from_secs_f32(
        state.config.tunables.unpack_timeout_secs,
    );
    let _permit = acquire_file_op_permit(pack_timeout).await?;
    let output = run_pack(
        &crate::python::resolve_python_interpreter(),
        &model_dir,
        version,
        &tmp_dir,
        pack_timeout,
    )
    .await
    .inspect_err(|_| {
        let dir = tmp_dir.clone();
        tokio::spawn(async move {
            let _ = tokio::fs::remove_dir_all(&dir).await;
        });
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        return Err(AppError::Internal(format!("pack failed: {}", stderr.trim())));
    }

    // Find the generated .lma file
    let mut lma_file = None;
    let entries = tokio::fs::read_dir(&tmp_dir).await.map_err(|e| {
        let dir = tmp_dir.clone();
        tokio::spawn(async move {
            let _ = tokio::fs::remove_dir_all(&dir).await;
        });
        AppError::Io(e)
    })?;
    let mut entries = entries;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.path().extension().map(|e| e == "lma").unwrap_or(false) {
            lma_file = Some(entry.path());
            break;
        }
    }

    let Some(lma_path) = lma_file else {
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        return Err(AppError::Internal("pack produced no .lma file".to_string()));
    };

    let size = tokio::fs::metadata(&lma_path)
        .await
        .map_err(AppError::Io)?
        .len();
    let file = tokio::fs::File::open(&lma_path).await.map_err(AppError::Io)?;
    let artifact_name = lma_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    crate::audit::control_plane(
        Some(cx),
        &state.access_control,
        crate::callback::Protocol::Http,
        "download",
        model_name,
        Some(version),
        "full",
    );

    let response = Response::builder()
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(CONTENT_LENGTH, size.to_string())
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", artifact_name),
        )
        .body(axum::body::Body::from_stream(LmaDownloadBody {
            inner: tokio_util::io::ReaderStream::new(file),
            _cleanup: DownloadCleanup(tmp_dir),
        }))
        .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
    Ok(response)
}

/// F10b: stream an existing .lma artifact file as the download response
/// (Content-Length + audit). The file is NOT removed afterwards — it is
/// the retained artifact, not a temp pack.
async fn serve_artifact_file(
    state: &AppState,
    cx: &RequestContext,
    model_name: &str,
    version: &str,
    path: &std::path::Path,
    artifact_name: &str,
) -> Result<Response, AppError> {
    let size = tokio::fs::metadata(path).await.map_err(AppError::Io)?.len();
    let file = tokio::fs::File::open(path).await.map_err(AppError::Io)?;

    crate::audit::control_plane(
        Some(cx),
        &state.access_control,
        crate::callback::Protocol::Http,
        "download",
        model_name,
        Some(version),
        "full (original artifact)",
    );

    Response::builder()
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(CONTENT_LENGTH, size.to_string())
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", artifact_name),
        )
        .body(axum::body::Body::from_stream(
            tokio_util::io::ReaderStream::new(file),
        ))
        .map_err(|e| AppError::Internal(format!("build response: {}", e)))
}

/// F9: model-level download — bare targets the active version (§4.4),
/// `?version=` overrides; no active version and no explicit version is a
/// 404 (same semantics as bare ready).
#[derive(Deserialize)]
pub struct ModelDownloadQuery {
    pub file: Option<String>,
    pub version: Option<String>,
}

pub async fn download_model_package_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    ApiQuery(query): ApiQuery<ModelDownloadQuery>,
    cx: RequestContext,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let version = match query.version {
        Some(v) => {
            crate::validation::validate_version(&v)?;
            v
        }
        None => state.registry.get_active_version(&model_name).ok_or_else(|| {
            AppError::ModelNotFound(format!(
                "{} has no active version; pass ?version= explicitly",
                model_name
            ))
        })?,
    };
    download_version_impl(&state, &cx, &model_name, &version, query.file.as_deref()).await
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
            Some("1"),
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

    // ===== Batch 2: F1/F2/F4/F6/F11b =====

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

    /// F1: single-file downloads must carry a Content-Length header equal
    /// to the file size (streaming enables it; whole-buffer responses
    /// don't need it and don't set it).
    #[tokio::test]
    async fn test_download_single_file_has_content_length() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-cl-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        let content = b"def predict(x): return x\n".to_vec();
        tokio::fs::write(model_dir.join("model.py"), &content)
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
        let content_length = response
            .headers()
            .get("content-length")
            .expect("F1: Content-Length header required")
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(content_length, content.len() as u64);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), content.as_slice(), "body bytes must match");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// F1: whole-directory .lma downloads must also carry Content-Length.
    #[tokio::test]
    async fn test_download_lma_has_content_length() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-lma-cl-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "def predict(x): return x\n")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let content_length = response
            .headers()
            .get("content-length")
            .expect("F1: Content-Length header required for .lma downloads")
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            content_length,
            body.len() as u64,
            "Content-Length must match the .lma byte size"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// F6: `?file=` pointing at a directory must be a 400, not an Io 500.
    #[tokio::test]
    async fn test_download_file_targeting_directory_is_400() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-dir-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(model_dir.join("subdir")).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "x = 1").await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=subdir")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "directory download must be 400, got {:?}",
            response.status()
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// F6: `?file=` that does not exist must be a 404, not an Io 500.
    #[tokio::test]
    async fn test_download_nonexistent_file_is_404() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-missing-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "x = 1").await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=missing.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "missing file download must be 404, got {:?}",
            response.status()
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// F2: the pack temp dir must be removed once the response body stream
    /// is consumed (the .lma file lives inside it, so cleanup is bound to
    /// the stream's end via the guard). Unit-level so the assertion polls
    /// only this test's own dir — an HTTP-level test would race the temp
    /// dirs of parallel download tests.
    #[tokio::test]
    async fn test_lma_download_body_cleans_tmp_on_completion() {
        use futures::StreamExt;

        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-body-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let file_path = tmp.join("mymodel_v1.lma");
        tokio::fs::write(&file_path, b"artifact-bytes").await.unwrap();
        let file = tokio::fs::File::open(&file_path).await.unwrap();

        {
            let mut body = LmaDownloadBody {
                inner: tokio_util::io::ReaderStream::new(file),
                _cleanup: DownloadCleanup(tmp.clone()),
            };
            while body.next().await.is_some() {}
            // Dropped here — the guard schedules the dir removal.
        }

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while tmp.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "F2: download temp dir must be cleaned after the stream ends"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }

    /// F11b: uploads exceeding `server.max_upload_bytes` must be 413.
    #[tokio::test]
    async fn test_upload_exceeding_max_upload_bytes_is_413() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-max-upload-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let mut config = Config::default();
        config.server.max_upload_bytes = Some(64);
        let state = test_app_state_with_config(tmp.clone(), config);
        let app = test_router(state);

        let boundary = "----maxuploadboundary";
        let body = multipart_body(boundary, "model.py", &[b'x'; 1024]);

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
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "oversized upload must be 413, got {:?}",
            response.status()
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== F8: model-level upload =====

    #[tokio::test]
    async fn test_model_level_upload_lma_places_files_and_reports_real_load() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-upload-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let lma = pack_fixture_lma(&tmp, "mymodel", "2").await;
        let data = tokio::fs::read(&lma).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/upload",
                axum::routing::post(upload_model_package_handler),
            )
            .with_state(state);

        let boundary = "----mluploadboundary";
        let body = multipart_body(boundary, "mymodel_v2.lma", &data);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/upload?load=false")
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
        let resp_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&resp_body).unwrap();
        // Version comes from the manifest, not the URL.
        assert_eq!(json["version"], "2", "{json}");

        // Files land at {name}/{v}/model.py — no nesting.
        assert!(tmp.join("mymodel").join("2").join("model.py").exists());
        assert!(!tmp.join("mymodel").join("2").join("2").exists());
        // F10a applies to the model-level path too.
        assert!(tmp.join(".artifacts").join("mymodel_v2.lma").exists());

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_model_level_upload_rejects_name_mismatch() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-upload-nm-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let lma = pack_fixture_lma(&tmp, "othermodel", "1").await;
        let data = tokio::fs::read(&lma).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/upload",
                axum::routing::post(upload_model_package_handler),
            )
            .with_state(state);

        let boundary = "----mluploadnmboundary";
        let body = multipart_body(boundary, "othermodel_v1.lma", &data);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/upload")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "manifest.name mismatch must be 400"
        );
        // Nothing landed.
        assert!(!tmp.join("mymodel").exists(), "no model dir on name mismatch");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_model_level_upload_rejects_raw_files() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-upload-raw-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/upload",
                axum::routing::post(upload_model_package_handler),
            )
            .with_state(state);

        let boundary = "----mluploadrawboundary";
        let body = multipart_body(boundary, "model.py", b"def predict(x): return x");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/upload")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "raw files must be rejected");
        let resp_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&resp_body).to_string();
        assert!(
            text.contains("versions/"),
            "error must point at the versioned endpoint: {text}"
        );
        assert!(!tmp.join("mymodel").exists());

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== F9: model-level download =====

    async fn model_dl_state(tmp: &std::path::Path) -> Arc<AppState> {
        let state = test_app_state(tmp.to_path_buf());
        for v in ["1", "2"] {
            let dir = tmp.join("mymodel").join(v);
            tokio::fs::create_dir_all(&dir).await.unwrap();
            tokio::fs::write(dir.join("model.py"), format!("VERSION {v} CONTENT\n"))
                .await
                .unwrap();
            state
                .registry
                .register(
                    "mymodel",
                    v,
                    crate::config::ModelConfig::default(),
                    crate::registry::types::ModelType::LitAPI,
                    dir,
                )
                .unwrap();
            state.registry.mark_ready("mymodel", v).unwrap();
        }
        state.registry.activate_version("mymodel", "2").unwrap();
        state
    }

    async fn model_dl_body(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }

    #[tokio::test]
    async fn test_model_level_download_bare_targets_active_version() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-dl-bare-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let state = model_dl_state(&tmp).await;

        let response = download_model_package_handler(
            State(state),
            Path("mymodel".to_string()),
            ApiQuery(ModelDownloadQuery { file: Some("model.py".to_string()), version: None }),
            crate::http::handlers::admin::audit_tests::test_cx(),
        )
        .await
        .expect("model-level download must succeed");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = model_dl_body(response).await;
        assert!(
            body.contains("VERSION 2"),
            "bare download must target the active version, got {body}"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_model_level_download_explicit_version_overrides_active() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-dl-ver-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let state = model_dl_state(&tmp).await;

        let response = download_model_package_handler(
            State(state),
            Path("mymodel".to_string()),
            ApiQuery(ModelDownloadQuery { file: Some("model.py".to_string()), version: Some("1".to_string()) }),
            crate::http::handlers::admin::audit_tests::test_cx(),
        )
        .await
        .expect("model-level download must succeed");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = model_dl_body(response).await;
        assert!(
            body.contains("VERSION 1"),
            "explicit ?version= must override the active version, got {body}"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_model_level_download_without_active_or_version_is_404() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-dl-404-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let state = test_app_state(tmp.clone());

        let err = download_model_package_handler(
            State(state),
            Path("mymodel".to_string()),
            ApiQuery(ModelDownloadQuery { file: None, version: None }),
            crate::http::handlers::admin::audit_tests::test_cx(),
        )
        .await
        .expect_err("no active version and no ?version= must 404");
        assert_eq!(err.http_status(), axum::http::StatusCode::NOT_FOUND, "{err:?}");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== F10b: original-artifact passthrough + drift patch =====

    #[tokio::test]
    async fn test_download_serves_retained_artifact_byte_identical() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-f10b-artifacts-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        // Upload an .lma (F10a retains it in .artifacts/).
        let lma = pack_fixture_lma(&tmp, "mymodel", "1").await;
        let original = tokio::fs::read(&lma).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);
        let boundary = "----f10buploadboundary";
        let body = multipart_body(boundary, "mymodel_v1.lma", &original);
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

        // Download must serve the ORIGINAL bytes (no repack — signature
        // and zip structure preserved).
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let downloaded = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            downloaded.as_ref(),
            original.as_slice(),
            "download must serve the retained artifact byte-identical"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_download_serves_root_placed_artifact() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-f10b-root-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "x = 1").await.unwrap();

        // An ops-placed artifact at the repo root (scanner auto-unpack
        // layout) — download must serve it as-is.
        let placed = b"PLACED-ORIGINAL-BYTES".to_vec();
        tokio::fs::write(tmp.join("mymodel_v1.lma"), &placed).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let downloaded = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(downloaded.as_ref(), placed.as_slice());

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_raw_upload_drops_stale_artifact() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-f10b-drift-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        // Upload an .lma first (creates .artifacts/ + root-copy scenario).
        let lma = pack_fixture_lma(&tmp, "mymodel", "1").await;
        let original = tokio::fs::read(&lma).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state.clone());
        let boundary = "----f10bdriftboundary";
        let body = multipart_body(boundary, "mymodel_v1.lma", &original);
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
        assert!(tmp.join(".artifacts").join("mymodel_v1.lma").exists());

        // A raw upload replaces the version content — the stale original
        // artifact must go (drift patch), so downloads repack from disk.
        let boundary2 = "----f10bdriftrawboundary";
        let body2 = multipart_body(boundary2, "model.py", b"def predict(x): return 'NEW'\n");
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary2),
                    )
                    .body(Body::from(body2))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !tmp.join(".artifacts").join("mymodel_v1.lma").exists(),
            "raw upload must drop the stale retained artifact"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== H1/H2: download subprocess timeout + shared semaphore =====

    /// H1: a hung pack subprocess must be killed (bounded wait), not held
    /// open forever.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_run_pack_times_out_hung_process() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = std::env::temp_dir().join(format!(
            "lite-server-pack-timeout-{}",
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

        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "x = 1").await.unwrap();
        let out = tmp.join("out");

        let result = run_pack(
            fake_py.to_str().unwrap(),
            &model_dir,
            "1",
            &out,
            std::time::Duration::from_millis(500),
        )
        .await;

        let err = result.expect_err("pack must fail on timeout");
        match err {
            AppError::Internal(msg) => {
                assert!(msg.contains("timed out"), "unexpected message: {}", msg);
            }
            _ => panic!("expected Internal timeout error"),
        }

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// H2/C7: the shared file-op semaphore permits exactly 4 concurrent
    /// subprocesses — the 5th acquire with a short timeout must fail
    /// (starvation protection). The hold window is tiny so parallel tests
    /// queue briefly rather than failing.
    #[tokio::test]
    async fn test_file_op_semaphore_bounds_concurrency() {
        let timeout = std::time::Duration::from_secs(120);
        let permits = [
            acquire_file_op_permit(timeout).await.expect("permit 1"),
            acquire_file_op_permit(timeout).await.expect("permit 2"),
            acquire_file_op_permit(timeout).await.expect("permit 3"),
            acquire_file_op_permit(timeout).await.expect("permit 4"),
        ];
        let fifth = acquire_file_op_permit(std::time::Duration::from_millis(50)).await;
        assert!(
            fifth.is_err(),
            "the 5th concurrent file-op must be refused after the acquire timeout"
        );
        drop(permits);
        // Capacity is restored after release.
        let _ = acquire_file_op_permit(timeout)
            .await
            .expect("permit must be available again after release");
    }

    /// F4: downloads must emit a `control_plane` audit record with
    /// action=download and the model/version fields.
    #[test]
    fn download_emits_structured_audit() {
        use crate::http::handlers::admin::audit_tests;
        use crate::request_context::RequestContext;

        let fields = audit_tests::run_captured(|| async {
            let tmp = std::env::temp_dir().join(format!(
                "lite-server-dl-audit-{}",
                std::process::id()
            ));
            let model_dir = tmp.join("mymodel").join("1");
            tokio::fs::create_dir_all(&model_dir).await.unwrap();
            tokio::fs::write(model_dir.join("model.py"), "x = 1").await.unwrap();

            let state = test_app_state(tmp.clone());
            let _resp = download_model_handler(
                State(state),
                Path(("mymodel".to_string(), "1".to_string())),
                ApiQuery(DownloadQuery { file: Some("model.py".to_string()) }),
                RequestContext {
                    request_id: "audit-rid".to_string(),
                    client_ip: "127.0.0.1".to_string(),
                    trace_cx: opentelemetry::Context::new(),
                    protocol: crate::callback::Protocol::Http,
                    principal: None,
                    api_protocol: None,
                },
            )
            .await
            .expect("download must succeed");

            let _ = tokio::fs::remove_dir_all(&tmp).await;
        });
        assert_eq!(
            fields.iter().find(|(n, _)| n == "action").map(|(_, v)| v.as_str()),
            Some("download"),
            "{fields:?}"
        );
        assert_eq!(
            fields.iter().find(|(n, _)| n == "model").map(|(_, v)| v.as_str()),
            Some("mymodel"),
            "{fields:?}"
        );
    }
}

