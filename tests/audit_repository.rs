//! Audit tests for the model repository upload/download/delete surface
//! (.claude/model-upload-and-retire-plan.md, batches 0-4 — commits 39fffba,
//! f5c5afa, ebbe881, 33291a9, bc9b236, ef5993f). Each test reproduces one
//! defect found in the audit; they FAIL against the current implementation.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use lite_server::callback::CallbackRunner;
use lite_server::config::Config;
use lite_server::http::handlers::files::upload_model_handler;
use lite_server::http::state::AppState;
use lite_server::inference_queue::InferenceQueue;
use lite_server::registry::ModelRegistry;
use lite_server::worker::WorkerManager;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tower::ServiceExt;

fn test_app_state(repo_path: std::path::PathBuf, config: Config) -> Arc<AppState> {
    let registry = Arc::new(ModelRegistry::new());
    let inference_queue = Arc::new(InferenceQueue::new());
    let callback_runner = Arc::new(CallbackRunner::new());
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
        Arc::new(lite_server::rate_limit::RateLimiter::default()),
    ))
}

fn upload_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/v2/repository/models/:model_name/versions/:version/upload",
            axum::routing::post(upload_model_handler),
        )
        .with_state(state)
}

fn unique_tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "lite-server-audit-repo-{}-{}-{}",
        tag,
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
}

/// Pack a minimal model fixture into a `.lma` artifact via the Python CLI
/// (same fixture style as src/http/handlers/files.rs mod tests).
async fn pack_fixture_lma(
    tmp: &std::path::Path,
    name: &str,
    version: &str,
) -> Vec<u8> {
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

    let output = tokio::process::Command::new("python")
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

    tokio::fs::read(pkgs_dir.join(format!("{}_v{}.lma", name, version)))
        .await
        .unwrap()
}

fn multipart_header(boundary: &str, filename: &str) -> Vec<u8> {
    format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
         Content-Type: application/octet-stream\r\n\r\n"
    )
    .into_bytes()
}

/// Build a multipart body carrying multiple file fields.
fn multipart_body_multi(boundary: &str, parts: &[(&str, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    for (filename, data) in parts {
        body.extend_from_slice(&multipart_header(boundary, filename));
        body.extend_from_slice(data);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

/// R3 (plan C2 drift patch overreach): `finalize_upload` runs the F10a
/// artifact retention for any staged `.lma`, then unconditionally deletes
/// the same artifact again whenever the upload ALSO contains raw files
/// (`if has_raw` — the plan scopes the drift patch to raw-file uploads,
/// "raw 文件上传(非 .lma)…删除该版本旧原包;.lma 上传覆盖原包"). A mixed
/// raw + .lma upload is accepted (200) but the just-retained original
/// artifact is silently destroyed, so F10b downloads fall back to
/// repacking and the author signature is lost — the exact opposite of the
/// ".lma upload overwrites/keeps the artifact" ruling.
#[tokio::test]
async fn test_mixed_raw_and_lma_upload_keeps_retained_artifact() {
    let tmp = unique_tmp("mixed-artifact");
    tokio::fs::create_dir_all(&tmp).await.unwrap();

    let lma = pack_fixture_lma(&tmp, "mymodel", "1").await;

    let state = test_app_state(tmp.clone(), Config::default());
    let app = upload_router(state);

    let boundary = "----auditmixedboundary";
    let body = multipart_body_multi(
        boundary,
        &[
            ("model.py", b"def predict(x): return x\n"),
            ("mymodel_v1.lma", lma.as_slice()),
        ],
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

    assert!(
        tmp.join(".artifacts").join("mymodel_v1.lma").exists(),
        "a mixed upload carrying a valid .lma must keep the retained \
         artifact (F10a); the C2 drift patch is scoped to raw-only uploads"
    );

    let _ = tokio::fs::remove_dir_all(&tmp).await;
}

/// R4 (version normalization skew): raw multipart fields are staged under
/// the RAW url version (`staging.join(&version)`), while a `.lma` in the
/// same request is unpacked under the v-STRIPPED version and the response
/// reports the stripped one. With a `v`-prefixed URL version a mixed
/// upload lands the raw files in a phantom version dir (`mymodel/v2`)
/// that the response (`version: "2"`) and auto-load never mention — and
/// the scanner then discovers `v2` as a separate ghost version.
#[tokio::test]
async fn test_mixed_upload_vprefixed_version_places_raw_in_reported_version() {
    let tmp = unique_tmp("mixed-vprefix");
    tokio::fs::create_dir_all(&tmp).await.unwrap();

    // Packer normalizes the manifest version to "2" (v-stripped).
    let lma = pack_fixture_lma(&tmp, "mymodel", "2").await;

    let state = test_app_state(tmp.clone(), Config::default());
    let app = upload_router(state);

    let boundary = "----auditvprefixboundary";
    let body = multipart_body_multi(
        boundary,
        &[
            ("extra.txt", b"raw-bytes\n"),
            ("mymodel_v2.lma", lma.as_slice()),
        ],
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/repository/models/mymodel/versions/v2/upload?load=false")
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
    let json: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
    let reported = json["version"].as_str().unwrap().to_string();

    // Every staged file must land under the version the response reports.
    assert!(
        tmp.join("mymodel").join(&reported).join("extra.txt").exists(),
        "raw file must land under the reported version '{}', got disk layout: {:?}",
        reported,
        std::fs::read_dir(tmp.join("mymodel"))
            .map(|rd| rd
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect::<Vec<_>>())
            .unwrap_or_default()
    );
    assert!(
        !tmp.join("mymodel").join("v2").exists(),
        "no phantom 'v2' version dir may remain — the scanner would \
         discover it as a separate version"
    );

    let _ = tokio::fs::remove_dir_all(&tmp).await;
}

/// R6 (F11b enforcement granularity): the HTTP upload cap is checked only
/// AFTER each multipart field has been fully streamed to disk
/// (`total_bytes > max` at the field boundary). A single field larger
/// than `max_upload_bytes` is written to the staging dir in full before
/// the 413 is produced — the cap does not bound disk usage during
/// ingestion (the gRPC UploadModel RPC enforces the same cap per message
/// BEFORE writing). Proven by counting how many body bytes the handler
/// consumes before responding: it must stop shortly after the cap, not
/// drain the whole body.
#[tokio::test]
async fn test_upload_cap_stops_streaming_at_limit() {
    let tmp = unique_tmp("cap-midstream");
    tokio::fs::create_dir_all(&tmp).await.unwrap();

    let mut config = Config::default();
    config.server.max_upload_bytes = Some(1024);
    let state = test_app_state(tmp.clone(), config);
    let app = upload_router(state);

    // One field with 256 KiB of data — 256x the cap.
    let boundary = "----auditcapboundary";
    let mut body_bytes = multipart_header(boundary, "model.py");
    body_bytes.extend_from_slice(&vec![b'x'; 256 * 1024]);
    body_bytes.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let total = body_bytes.len();

    let pulled = Arc::new(AtomicUsize::new(0));
    let pulled_in_stream = pulled.clone();
    let chunks: Vec<Bytes> = body_bytes
        .chunks(8 * 1024)
        .map(Bytes::copy_from_slice)
        .collect();
    // yield_now between items: an always-Ready in-memory stream is drained
    // in full by multer's poll loop regardless of when the handler aborts
    // — a Pending between items models network backpressure so consumption
    // actually reflects how far the handler read.
    let stream = futures::stream::iter(chunks).then(move |c| {
        let counter = pulled_in_stream.clone();
        async move {
            tokio::task::yield_now().await;
            counter.fetch_add(c.len(), Ordering::SeqCst);
            Ok::<Bytes, std::io::Error>(c)
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={}", boundary),
                )
                .body(Body::from_stream(stream))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let consumed = pulled.load(Ordering::SeqCst);
    assert!(
        consumed < total / 2,
        "the cap must abort the upload shortly after {} bytes; the handler \
         consumed {consumed} of {total} body bytes (whole oversize field \
         streamed to disk before the 413)",
        1024
    );

    let _ = tokio::fs::remove_dir_all(&tmp).await;
}
