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

    // Ingest-only loop: stream every field into the staging dir
    // (F11a, RAM bounded by chunk size). Unpacking/validation/commit run
    // in the shared tail (finalize_upload) — same for the gRPC
    // UploadModel RPC.
    let mut staged: Vec<StagedUploadFile> = Vec::new();
    let mut total_bytes: u64 = 0;
    while let Some(mut field) = multipart.next_field().await.map_err(|e| {
        AppError::Validation(format!("multipart error: {}", e))
    })? {
        // A form field without a filename is not a file — reject it
        // instead of writing a literal "unnamed" file into the version dir.
        let filename = field
            .file_name()
            .ok_or_else(|| {
                AppError::Validation(
                    "multipart field without a filename — uploads carry files only".to_string(),
                )
            })?
            .to_string();

        if filename.ends_with(".lma") {
            // One .lma per version upload (gRPC UploadModel parity) — two
            // packages merged into one staging dir have no defined
            // placement/retention semantics.
            if staged.iter().any(|f| f.is_lma) {
                return Err(AppError::InvalidRequestBody(
                    "a version upload accepts a single .lma artifact".to_string(),
                ));
            }
            // B1: strip any path components (same rule as the raw branch)
            // — multer returns the filename unsanitized, so a `../` or
            // absolute filename would otherwise escape the staging dir.
            let safe_name = std::path::Path::new(&filename)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if safe_name.is_empty() || safe_name.starts_with('.') || !safe_name.ends_with(".lma") {
                return Err(AppError::Validation(format!(
                    "invalid .lma file name: {}",
                    filename
                )));
            }
            let tmp_file = staging.join(&safe_name);
            total_bytes = stream_field_to_file(
                &mut field,
                &tmp_file,
                total_bytes,
                state.config.server.max_upload_bytes,
            )
            .await?;
            staged.push(StagedUploadFile {
                name: safe_name,
                path: tmp_file,
                is_lma: true,
            });
        } else {
            // Raw file: stage into the version directory. Sanitize
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
            total_bytes = stream_field_to_file(
                &mut field,
                &file_path,
                total_bytes,
                state.config.server.max_upload_bytes,
            )
            .await?;
            staged.push(StagedUploadFile {
                name: safe_name,
                path: file_path,
                is_lma: false,
            });
        }
    }

    if staged.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    // Optionally auto-load after upload; `loaded` reports the real outcome
    // instead of echoing the ?load= query param.
    let auto_load = query.load.unwrap_or(true);
    let outcome = finalize_upload(
        &state,
        &model_name,
        &staging,
        &staged,
        Some(&version),
        auto_load,
    )
    .await?;

    info!(
        model = %model_name,
        version = %outcome.version,
        files = ?outcome.files,
        bytes = total_bytes,
        "Model uploaded"
    );

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

    Ok(Json(response))
}

/// Auto-load an uploaded version (shared by both upload endpoints and the
/// gRPC UploadModel RPC). Returns the load error detail (None when not
/// requested or on success).
pub(crate) async fn auto_load_uploaded(
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

// ===== Shared upload tail (HTTP handlers + gRPC UploadModel RPC) =====

/// A file staged during upload ingestion (multipart field or gRPC chunk
/// group) — sanitized name plus its location inside the staging dir.
pub(crate) struct StagedUploadFile {
    pub(crate) name: String,
    pub(crate) path: std::path::PathBuf,
    pub(crate) is_lma: bool,
}

/// Result of a finalized upload (both HTTP endpoints + gRPC UploadModel).
pub(crate) struct UploadOutcome {
    pub(crate) version: String,
    pub(crate) files: Vec<String>,
    pub(crate) load_error: Option<String>,
}

/// Unpack one staged .lma artifact into the staging root (H1 timeout via
/// the shared tunable + H2 subprocess semaphore), failing on nonzero exit.
async fn unpack_artifact(
    tmp_file: &std::path::Path,
    staging: &std::path::Path,
    expect_version: Option<&str>,
    unpack_timeout: std::time::Duration,
) -> Result<(), AppError> {
    let _permit = acquire_file_op_permit(unpack_timeout).await?;
    let output = run_unpack(
        &crate::python::resolve_python_interpreter(),
        tmp_file,
        staging,
        expect_version,
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
    Ok(())
}

/// Shared upload tail — unpack staged .lma artifacts (with manifest
/// verification), commit into the repo (swap semantics, H3), retain the
/// original artifact (F10a), patch the drift the raw files introduce (C2)
/// and auto-load. `url_version: None` = model-level (F8: exactly one
/// .lma, version read from the manifest); `Some(v)` = versioned endpoint.
pub(crate) async fn finalize_upload(
    state: &AppState,
    model_name: &str,
    staging: &std::path::Path,
    staged: &[StagedUploadFile],
    url_version: Option<&str>,
    load: bool,
) -> Result<UploadOutcome, AppError> {
    let unpack_timeout =
        std::time::Duration::from_secs_f32(state.config.tunables.unpack_timeout_secs);
    let has_raw = staged.iter().any(|f| !f.is_lma);
    let has_lma = staged.iter().any(|f| f.is_lma);

    let effective_version = match url_version {
        None => {
            // Model-level (F8): exactly one .lma and no raw files — there
            // is no URL version to place raw content under.
            if has_raw || staged.len() != 1 || !staged[0].is_lma {
                return Err(AppError::InvalidRequestBody(format!(
                    "model-level upload accepts a single .lma artifact; \
                     raw files need the versioned endpoint \
                     /v2/repository/models/{}/versions/{{v}}/upload",
                    model_name
                )));
            }
            // Unpack without a version expectation — the version is read
            // from the manifest below and validated against the URL model
            // name.
            unpack_artifact(&staged[0].path, staging, None, unpack_timeout).await?;

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
            v
        }
        Some(url_v) => {
            // For .lma uploads the manifest version (always v-stripped by
            // the packer) names the on-disk version directory; normalize
            // the URL version the same way so unpack, load, and the
            // response all agree. The artifact's internal layout carries
            // the version directory prefix ({version}/...), so unpack
            // --flat into the staging root — mirroring the scanner's
            // repository-root auto-unpack — instead of the version
            // directory (which would nest {name}/{v}/{v}/).
            // --expect-version fails before extraction if the manifest
            // version does not match the upload URL.
            let effective = if staged.iter().any(|f| f.is_lma) {
                url_v.strip_prefix('v').unwrap_or(url_v).to_string()
            } else {
                url_v.to_string()
            };
            for f in staged.iter().filter(|f| f.is_lma) {
                unpack_artifact(&f.path, staging, Some(&effective), unpack_timeout).await?;
            }
            effective
        }
    };

    // F11b (zip-bomb guard): max_upload_bytes counts COMPRESSED upload
    // bytes — a small archive can unpack to an unbounded tree. The
    // manifest's checksum-verified file sizes declare the unpacked total;
    // reject when it exceeds the cap too. None = no check (default).
    if has_lma {
        if let Some(max) = state.config.server.max_upload_bytes {
            if let Ok(raw) = tokio::fs::read_to_string(staging.join("manifest.json")).await {
                if let Ok(manifest) = serde_json::from_str::<Value>(&raw) {
                    let declared: u64 = manifest["files"]
                        .as_object()
                        .map(|files| {
                            files
                                .values()
                                .filter_map(|e| e["size"].as_u64())
                                .sum()
                        })
                        .unwrap_or(0);
                    if declared > max {
                        return Err(AppError::PayloadTooLarge {
                            max_size: max as usize,
                            actual_size: Some(declared),
                        });
                    }
                }
            }
        }
    }

    // R4: a MIXED raw + .lma upload with a v-prefixed URL version staged
    // the raw files under the raw URL string while the artifact unpacked
    // under the normalized version — merge the raw files into the
    // effective version dir before commit so no phantom version dir (the
    // scanner would collect it as a separate version) lands on disk.
    if has_raw && has_lma {
        if let Some(url_v) = url_version {
            if url_v != effective_version {
                let raw_dir = staging.join(url_v);
                if raw_dir.is_dir() {
                    let dst = staging.join(&effective_version);
                    tokio::fs::create_dir_all(&dst).await.map_err(AppError::Io)?;
                    for f in staged.iter().filter(|f| !f.is_lma) {
                        tokio::fs::rename(&f.path, dst.join(&f.name))
                            .await
                            .map_err(AppError::Io)?;
                    }
                    let _ = tokio::fs::remove_dir_all(&raw_dir).await;
                }
            }
        }
    }

    // H3: move staged content into place — version dirs via swap semantics
    // (replaced wholesale, never partial), model-root files by overwrite.
    commit_staging(&state.repo_path, model_name, staging).await?;

    // F10a: retain the original artifact(s) so downloads can serve them
    // back without repacking (preserving the author signature). Only after
    // a successful commit (B2): a failed upload must leave no orphan
    // artifact for F10b to serve as the version's truth. A retention
    // failure (e.g. ENOSPC) is NON-FATAL: the version content is already
    // the truth on disk and F10b falls back to repacking when the original
    // is absent — warn loudly instead of lying with a 500 (R-F10a).
    if has_lma {
        let artifacts_dir = state.repo_path.join(".artifacts");
        let artifact_name = format!("{}_v{}.lma", model_name, effective_version);
        let retain = async {
            tokio::fs::create_dir_all(&artifacts_dir).await?;
            for f in staged.iter().filter(|f| f.is_lma) {
                tokio::fs::copy(&f.path, artifacts_dir.join(&artifact_name)).await?;
            }
            Ok::<(), std::io::Error>(())
        };
        if let Err(e) = retain.await {
            warn!(
                model = %model_name,
                version = %effective_version,
                error = %e,
                "original artifact retention failed — downloads fall back to repacking (F10b)"
            );
        }
    }

    // C2 (drift patch): a raw-ONLY upload replaced the version content —
    // drop the stale original artifact (if any), or F10b would keep
    // serving the old package as the truth. Downloads then fall back to
    // repacking the new disk tree. A mixed upload carries a valid .lma
    // whose artifact was just retained above — the patch must not delete
    // it (R3). Only after a successful commit — a failed upload must not
    // destroy the previous artifact.
    if has_raw && !has_lma {
        let _ = crate::http::handlers::admin::remove_linked_artifacts(
            &state.repo_path,
            model_name,
            &effective_version,
        )
        .await;
    }

    let load_error = auto_load_uploaded(state, model_name, &effective_version, load).await;

    Ok(UploadOutcome {
        version: effective_version,
        files: staged.iter().map(|f| f.name.clone()).collect(),
        load_error,
    })
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

    // Ingest-only loop (shared tail = finalize_upload). Model-level shape
    // is enforced here for the HTTP surface; finalize_upload enforces it
    // again for the gRPC UploadModel RPC.
    let mut staged: Vec<StagedUploadFile> = Vec::new();
    let mut total_bytes: u64 = 0;
    while let Some(mut field) = multipart.next_field().await.map_err(|e| {
        AppError::Validation(format!("multipart error: {}", e))
    })? {
        let filename = field
            .file_name()
            .ok_or_else(|| {
                AppError::Validation(
                    "multipart field without a filename — uploads carry files only".to_string(),
                )
            })?
            .to_string();

        if !filename.ends_with(".lma") {
            return Err(AppError::InvalidRequestBody(format!(
                "model-level upload accepts a single .lma artifact (got '{}'); \
                 raw files need the versioned endpoint \
                 /v2/repository/models/{}/versions/{{v}}/upload",
                filename, model_name
            )));
        }
        if !staged.is_empty() {
            return Err(AppError::InvalidRequestBody(
                "model-level upload accepts a single .lma artifact".to_string(),
            ));
        }

        // B1: strip any path components (same rule as the raw branch of
        // the versioned endpoint) — multer returns the filename
        // unsanitized, so a `../` or absolute filename would otherwise
        // escape the staging dir.
        let safe_name = std::path::Path::new(&filename)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if safe_name.is_empty() || safe_name.starts_with('.') || !safe_name.ends_with(".lma") {
            return Err(AppError::Validation(format!(
                "invalid .lma file name: {}",
                filename
            )));
        }
        let tmp_file = staging.join(&safe_name);
        total_bytes = stream_field_to_file(
            &mut field,
            &tmp_file,
            total_bytes,
            state.config.server.max_upload_bytes,
        )
        .await?;
        staged.push(StagedUploadFile {
            name: safe_name,
            path: tmp_file,
            is_lma: true,
        });
    }

    if staged.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    let auto_load = query.load.unwrap_or(true);
    let outcome = finalize_upload(&state, &model_name, &staging, &staged, None, auto_load).await?;

    info!(
        model = %model_name,
        version = %outcome.version,
        files = ?outcome.files,
        bytes = total_bytes,
        "Model package uploaded (model-level)"
    );

    crate::audit::control_plane(
        Some(&cx),
        &state.access_control,
        crate::callback::Protocol::Http,
        "upload",
        &model_name,
        Some(&outcome.version),
        "model-level",
    );

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

    Ok(Json(response))
}

/// Stream a multipart field to `dest`, returning the bytes written
/// (F11a: RAM bounded by chunk size instead of buffering the whole field).
/// F11a: stream a multipart field to disk (RAM bounded by chunk size).
/// F11b/R6: the cumulative upload cap is enforced BEFORE writing each
/// chunk — the cap must bound disk usage during ingestion, not merely
/// reject after an oversize field already landed (parity with the gRPC
/// UploadModel per-message check). `prior_total` is the running total
/// across earlier fields; returns the new cumulative total.
async fn stream_field_to_file(
    field: &mut axum::extract::multipart::Field<'_>,
    dest: &std::path::Path,
    prior_total: u64,
    max_total: Option<u64>,
) -> Result<u64, AppError> {
    let mut file = tokio::fs::File::create(dest).await.map_err(AppError::Io)?;
    let mut total = prior_total;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| AppError::Transport(format!("read upload field: {}", e)))?
    {
        let new_total = total + chunk.len() as u64;
        if let Some(max) = max_total {
            if new_total > max {
                return Err(AppError::PayloadTooLarge {
                    max_size: max as usize,
                    actual_size: Some(new_total),
                });
            }
        }
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
            .await
            .map_err(AppError::Io)?;
        total = new_total;
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

pub(crate) async fn acquire_file_op_permit(
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
/// is killed instead of holding the request open forever. R2: stdout and
/// stderr are drained CONCURRENTLY with the wait — a child that writes more
/// than the pipe buffer blocks until someone reads, so a wait-then-drain
/// order turns every chatty-but-healthy child into a spurious timeout kill.
async fn wait_child_bounded(
    child: &mut tokio::process::Child,
    timeout: std::time::Duration,
    what: &str,
) -> Result<std::process::Output, AppError> {
    let mut stdout_handle = child.stdout.take();
    let mut stderr_handle = child.stderr.take();
    let drain_out = async move {
        let mut buf = Vec::new();
        if let Some(mut h) = stdout_handle.take() {
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut h, &mut buf).await;
        }
        buf
    };
    let drain_err = async move {
        let mut buf = Vec::new();
        if let Some(mut h) = stderr_handle.take() {
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut h, &mut buf).await;
        }
        buf
    };
    let wait_and_drain = async { tokio::join!(child.wait(), drain_out, drain_err) };
    match tokio::time::timeout(timeout, wait_and_drain).await {
        Ok((result, stdout, stderr)) => {
            let status = result
                .map_err(|e| AppError::Internal(format!("python {what} failed: {}", e)))?;
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
pub(crate) async fn run_unpack(
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
pub(crate) async fn commit_staging(
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
            // The staged upload artifact itself is not model content — it
            // is retained into .artifacts/ separately (F10a) after the
            // commit, and the staging guard removes what remains.
            if entry.file_name().to_string_lossy().ends_with(".lma") {
                continue;
            }
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
pub(crate) struct StagingGuard(pub(crate) std::path::PathBuf);

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

/// Shared download resolution (HTTP versioned/model-level endpoints + the
/// gRPC DownloadModel RPC). Resolves the file to stream: a single named
/// file, or the whole version as one .lma — original artifact passthrough
/// first (F10b), else a fresh pack into a temp dir whose cleanup guard
/// rides along (dropped when the consumer finishes streaming).
pub(crate) struct DownloadSource {
    pub(crate) path: std::path::PathBuf,
    pub(crate) size: u64,
    /// Content-disposition file name.
    pub(crate) file_name: String,
    pub(crate) content_type: &'static str,
    /// Audit detail suffix ("file=...", "full", "full (original artifact)").
    pub(crate) audit_detail: String,
    /// Pack temp-dir cleanup — None for retained files (single file and
    /// artifact passthrough are not temp).
    cleanup: Option<DownloadCleanup>,
}

pub(crate) async fn resolve_download_source(
    state: &AppState,
    model_name: &str,
    version: &str,
    file: Option<&str>,
) -> Result<DownloadSource, AppError> {
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

        let size = tokio::fs::metadata(&canonical_file)
            .await
            .map_err(AppError::Io)?
            .len();
        let content_type = if file_name.ends_with(".py") || file_name.ends_with(".yaml") || file_name.ends_with(".yml") || file_name.ends_with(".json") || file_name.ends_with(".txt") || file_name.ends_with(".md") {
            "text/plain; charset=utf-8"
        } else {
            "application/octet-stream"
        };
        return Ok(DownloadSource {
            path: canonical_file,
            size,
            file_name: file_name.to_string(),
            content_type,
            audit_detail: format!("file={}", file_name),
            cleanup: None,
        });
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
            let size = tokio::fs::metadata(&candidate)
                .await
                .map_err(AppError::Io)?
                .len();
            return Ok(DownloadSource {
                path: candidate,
                size,
                file_name: artifact_name,
                content_type: "application/octet-stream",
                audit_detail: "full (original artifact)".to_string(),
                cleanup: None,
            });
        }
    }

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-download-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&tmp_dir)
        .await
        .map_err(AppError::Io)?;
    // F2/B4: the cleanup guard is armed at creation — every early return
    // below (semaphore timeout, pack failure, missing artifact, I/O error)
    // removes the dir; the success path hands it to the stream consumer.
    let cleanup = DownloadCleanup(tmp_dir.clone());

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
    .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::Internal(format!("pack failed: {}", stderr.trim())));
    }

    // Find the generated .lma file
    let mut lma_file = None;
    let mut entries = tokio::fs::read_dir(&tmp_dir).await.map_err(AppError::Io)?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.path().extension().map(|e| e == "lma").unwrap_or(false) {
            lma_file = Some(entry.path());
            break;
        }
    }

    let Some(lma_path) = lma_file else {
        return Err(AppError::Internal("pack produced no .lma file".to_string()));
    };

    let size = tokio::fs::metadata(&lma_path)
        .await
        .map_err(AppError::Io)?
        .len();
    let artifact_name = lma_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    Ok(DownloadSource {
        path: lma_path,
        size,
        file_name: artifact_name,
        content_type: "application/octet-stream",
        audit_detail: "full".to_string(),
        cleanup: Some(cleanup),
    })
}

/// Shared download core (versioned endpoint + F9's model-level endpoint).
async fn download_version_impl(
    state: &AppState,
    cx: &RequestContext,
    model_name: &str,
    version: &str,
    file: Option<&str>,
) -> Result<Response, AppError> {
    let src = resolve_download_source(state, model_name, version, file).await?;

    crate::audit::control_plane(
        Some(cx),
        &state.access_control,
        crate::callback::Protocol::Http,
        "download",
        model_name,
        Some(version),
        &src.audit_detail,
    );

    // F1: stream the file instead of buffering it whole; Content-Length
    // lets the client see progress/size up front. The pack temp dir (when
    // any) is cleaned when the body stream is dropped.
    let file = tokio::fs::File::open(&src.path).await.map_err(AppError::Io)?;
    let body = match src.cleanup {
        Some(cleanup) => axum::body::Body::from_stream(LmaDownloadBody {
            inner: tokio_util::io::ReaderStream::new(file),
            _cleanup: cleanup,
        }),
        None => axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file)),
    };

    Response::builder()
        .header(CONTENT_TYPE, src.content_type)
        .header(CONTENT_LENGTH, src.size.to_string())
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", src.file_name),
        )
        .body(body)
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

/// A model-directory entry (HTTP handler + gRPC ListFiles RPC).
pub(crate) struct FileEntryInfo {
    pub(crate) name: String,
    pub(crate) size: u64,
    pub(crate) modified: Option<u64>,
    pub(crate) is_dir: bool,
}

/// Shared list-files core — validated directory listing of one version.
pub(crate) async fn list_files_impl(
    state: &AppState,
    model_name: &str,
    version: &str,
) -> Result<Vec<FileEntryInfo>, AppError> {
    crate::validation::validate_identifier(model_name)?;
    crate::validation::validate_version(version)?;

    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, model_name, version)?;

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

        files.push(FileEntryInfo {
            name,
            size,
            modified,
            is_dir: path.is_dir(),
        });
    }

    Ok(files)
}

pub async fn list_files_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let files = list_files_impl(&state, &model_name, &version).await?;
    Ok(Json(json!({
        "model": model_name,
        "version": version,
        "files": files
            .iter()
            .map(|f| json!({
                "name": f.name,
                "size": f.size,
                "modified": f.modified,
                "is_dir": f.is_dir,
            }))
            .collect::<Vec<Value>>(),
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

    /// D1 (audit): the inference request-body cap (DefaultBodyLimit on the
    /// whole router, http/mod.rs) must NOT reject artifact uploads — F11b's
    /// ruling is `max_upload_bytes` (default None = unlimited) as the ONLY
    /// gate. axum 0.7.9's Multipart DOES consume DefaultBodyLimit, so the
    /// upload routes carry their own `DefaultBodyLimit::disable()`. Mirror
    /// the production layering: global 1 KiB cap, upload a larger file.
    #[tokio::test]
    async fn test_upload_route_not_bound_by_request_body_limit() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-bodylimit-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        // Production layering (http/mod.rs): the whole router gets the
        // inference body cap; upload routes opt out.
        let app = crate::http::routes::create_routes(state)
            .layer(axum::extract::DefaultBodyLimit::max(1024));

        let boundary = "----testboundaryd1";
        let big = "x".repeat(4096);
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"weights.bin\"\r\n\
             Content-Type: application/octet-stream\r\n\r\n\
             {big}\r\n\
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

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "uploads are gated by max_upload_bytes (default None), not the \
             inference body cap — the upload routes disable DefaultBodyLimit"
        );

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
        // The staged upload artifact itself is not model content — it is
        // retained in .artifacts/, never copied into the model root.
        assert!(
            !tmp.join("mymodel").join("mymodel_v1.lma").exists(),
            "the staged .lma must not be committed into the model directory"
        );

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
    /// serial(download_tmp): this test creates a `lite-server-download-*`
    /// dir in the SHARED system temp; the semaphore-leak test scans that
    /// prefix — they must not interleave.
    #[tokio::test]
    #[serial_test::serial(download_tmp)]
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
    // serial(download_tmp): exhausts the GLOBAL file-op semaphore — must
    // not interleave with the other permit-holding / tmp-scanning tests.
    #[serial_test::serial(download_tmp)]
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

    // ===== Audit 2026-08-14: defect reproductions (batches 0-2) =====

    /// Build a multipart body carrying multiple file fields.
    fn multipart_body_multi(boundary: &str, parts: &[(&str, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        for (filename, data) in parts {
            body.extend_from_slice(
                format!(
                    "--{boundary}\r\n\
                     Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
                     Content-Type: application/octet-stream\r\n\r\n"
                )
                .as_bytes(),
            );
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    /// Audit B1 (scope assumption): the .lma branch builds the staging path
    /// from the RAW multipart filename (`staging.join(&filename)`) — multer
    /// does not sanitize it, so a `../` filename escapes the staging dir and
    /// writes attacker bytes outside it (the raw-file branch sanitizes via
    /// `Path::file_name`; the .lma branch must apply the same rule). Here the
    /// escape lands in the repo root, where the scanner would later treat a
    /// valid .lma as an auto-unpack candidate.
    #[tokio::test]
    async fn test_upload_lma_filename_traversal_cannot_escape_staging() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-lma-traversal-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----traversalboundary";
        let body = multipart_body(boundary, "../escaped_upload.lma", b"not a zip");

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

        // The garbage payload fails unpack either way; the security
        // invariant is that nothing was written outside the staging dir.
        assert_ne!(response.status(), StatusCode::OK);
        assert!(
            !tmp.join("escaped_upload.lma").exists(),
            "filename traversal wrote a file outside the staging area"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// Audit B1 (model-level variant): upload_model_package_handler has its
    /// own `staging.join(&filename)` site with the same missing sanitize.
    #[tokio::test]
    async fn test_model_level_upload_lma_filename_traversal_cannot_escape_staging() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-traversal-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = Router::new()
            .route(
                "/v2/repository/models/:model_name/upload",
                axum::routing::post(upload_model_package_handler),
            )
            .with_state(state);

        let boundary = "----mltraversalboundary";
        let body = multipart_body(boundary, "../escaped_ml_upload.lma", b"not a zip");

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

        assert_ne!(response.status(), StatusCode::OK);
        assert!(
            !tmp.join("escaped_ml_upload.lma").exists(),
            "filename traversal wrote a file outside the staging area"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// Audit B2 (ordering assumption): the F10a artifact retention runs
    /// BEFORE the size cap is enforced — a .lma that exceeds
    /// max_upload_bytes is rejected with 413, yet its artifact has already
    /// been copied into .artifacts/. A rejected upload must retain nothing:
    /// the orphan would be served back by F10b as the version's truth even
    /// though the upload never committed.
    #[tokio::test]
    async fn test_upload_oversize_lma_does_not_retain_artifact() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-lma-oversize-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let lma = pack_fixture_lma(&tmp, "mymodel", "1").await;
        let data = tokio::fs::read(&lma).await.unwrap();
        assert!(data.len() as u64 > 64, "fixture must exceed the test cap");

        let mut config = Config::default();
        config.server.max_upload_bytes = Some(64);
        let state = test_app_state_with_config(tmp.clone(), config);
        let app = test_router(state);

        let boundary = "----oversizeboundary";
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

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            !tmp.join(".artifacts").join("mymodel_v1.lma").exists(),
            "a rejected (413) upload must not retain an artifact"
        );
        assert!(
            !tmp.join("mymodel").exists(),
            "a rejected (413) upload must not place model files"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// Audit B2 (model-level variant): a valid .lma first field is unpacked
    /// and retained in .artifacts/; when a LATER field is rejected (raw
    /// files are not accepted model-level), the request fails but the
    /// artifact copy survives — an orphan the commit never produced.
    #[tokio::test]
    async fn test_model_level_upload_failure_after_unpack_leaves_no_artifact() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ml-orphan-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
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

        let boundary = "----mlorphanboundary";
        let body = multipart_body_multi(
            boundary,
            &[
                ("mymodel_v2.lma", data.as_slice()),
                ("model.py", b"def predict(x): return x\n"),
            ],
        );

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
            "raw files are rejected model-level"
        );
        assert!(
            !tmp.join(".artifacts").join("mymodel_v2.lma").exists(),
            "a failed upload must not retain an artifact"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// Audit B4 (resource assumption): the download handler creates its
    /// pack temp dir BEFORE acquiring the file-op semaphore, and the
    /// acquire's `?` early-return has no cleanup — a semaphore-timeout
    /// rejection leaks a `lite-server-download-*` dir (F2 mandated cleanup
    /// on every early-return). Exhaust the permits, then a download with a
    /// tiny acquire timeout is refused; no new temp dir may remain.
    #[tokio::test]
    #[serial_test::serial(download_tmp)]
    async fn test_download_semaphore_timeout_leaves_no_tmp_residue() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-semleak-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "def predict(x): return x\n")
            .await
            .unwrap();

        let mut config = Config::default();
        config.tunables.unpack_timeout_secs = 0.05;
        let state = test_app_state_with_config(tmp.clone(), config);
        let app = test_router(state);

        let temp = std::env::temp_dir();
        let before: std::collections::BTreeSet<String> = std::fs::read_dir(&temp)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with("lite-server-download-"))
            .collect();

        // Hold all four permits so the handler's acquire times out.
        let permits = [
            acquire_file_op_permit(std::time::Duration::from_secs(120)).await.expect("p1"),
            acquire_file_op_permit(std::time::Duration::from_secs(120)).await.expect("p2"),
            acquire_file_op_permit(std::time::Duration::from_secs(120)).await.expect("p3"),
            acquire_file_op_permit(std::time::Duration::from_secs(120)).await.expect("p4"),
        ];

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        drop(permits);

        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "acquire timeout must reject the download"
        );

        // The cleanup guard's removal is spawned from Drop, so poll briefly.
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        loop {
            let after: std::collections::BTreeSet<String> = std::fs::read_dir(&temp)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| n.starts_with("lite-server-download-"))
                .collect();
            let leaked: Vec<String> = after.difference(&before).cloned().collect();
            if leaked.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "semaphore-timeout rejection leaked temp dirs: {leaked:?}"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        };

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// Audit R2 (pipe-drain ordering, H1 helper): `wait_child_bounded`
    /// waits for child exit BEFORE draining the piped stdout/stderr. A
    /// child that writes more than the OS pipe buffer (~64KB) blocks on
    /// write and never exits, so the bounded wait always escalates to the
    /// timeout kill — a healthy but chatty pack/unpack subprocess is
    /// reported as "timed out" and killed. wait_with_output-style draining
    /// (concurrent reads) is the correct shape; the timeout must bound the
    /// whole wait+drain, not just wait().
    #[cfg(unix)]
    #[tokio::test]
    async fn test_run_unpack_completes_for_chatty_child() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = std::env::temp_dir().join(format!(
            "lite-server-unpack-chatty-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        // A "python" that writes 1 MiB to stderr (far beyond the pipe
        // buffer) and then exits 0 — a healthy but verbose child.
        let fake_py = tmp.join("fake-python");
        tokio::fs::write(&fake_py, "#!/bin/sh\nhead -c 1048576 /dev/zero >&2\nexit 0\n")
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
            std::time::Duration::from_secs(5),
        )
        .await;

        assert!(
            result.is_ok(),
            "a child that exits 0 after writing >pipe-buffer output must \
             complete, not be killed as 'timed out': {result:?}"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}

