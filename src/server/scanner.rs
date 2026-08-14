use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::time::Duration;
use tracing::{info, warn};

#[derive(Clone)]
pub(super) struct RepoModel {
    pub(super) name: String,
    pub(super) version: String,
}
/// Auto-unpack .lma artifact files found in the model repository root.
/// Shells out to `python -m lite_server unpack` to extract each .lma into
/// the standard repo/model_name/version/ directory layout.
///
/// `seen` tracks (path, mtime) of artifacts already unpacked: the unpacker
/// does not delete the .lma after extraction, so without this the model
/// poller would re-unpack — and overwrite the extracted directory — every
/// tick. Each artifact is attempted once per mtime; a failed unpack is not
/// retried unless the file is replaced (new mtime).
pub(super) async fn auto_unpack_lma_files(
    repo_path: &Path,
    seen: &mut HashSet<(PathBuf, std::time::SystemTime)>,
    unpack_timeout: Duration,
) {
    let mut entries = match tokio::fs::read_dir(repo_path).await {
        Ok(e) => e,
        Err(_) => return,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if !path.extension().map(|e| e == "lma").unwrap_or(false) {
            continue;
        }
        let mtime = match tokio::fs::metadata(&path).await.and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !seen.insert((path.clone(), mtime)) {
            continue;
        }

        // H6: skip re-unpacking when the extracted model dir exists and is
        // at least as new as the artifact — a restart must not clobber an
        // up-to-date tree (extractall overwrites in place), and it skips
        // pointless unpack work on every boot. A replaced artifact has a
        // newer mtime and is unpacked normally.
        if let Some(name) = model_name_from_artifact(path.file_name()) {
            let model_dir = repo_path.join(&name);
            if model_dir.is_dir() {
                let fresh = tokio::fs::metadata(&model_dir)
                    .await
                    .and_then(|m| m.modified())
                    .is_ok_and(|dir_mtime| dir_mtime >= mtime);
                if fresh {
                    info!(
                        "Skipping .lma artifact {} — extracted directory is up to date",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    );
                    continue;
                }
            }
        }

        // H4: unpack into a staging dir, then move each extracted model
        // directory into the repo with swap semantics — a failed or hung
        // unpack can never leave a half-written model dir that
        // scan_repo_models would collect.
        let staging = repo_path.join(format!(".tmp-unpack-{}", uuid::Uuid::new_v4()));
        let output = tokio::time::timeout(
            unpack_timeout,
            tokio::process::Command::new(crate::python::resolve_python_interpreter())
                .args([
                    "-m",
                    "lite_server",
                    "unpack",
                    path.to_str().unwrap_or(""),
                    "--to",
                    staging.to_str().unwrap_or(""),
                ])
                .output(),
        )
        .await;

        let output = match output {
            Ok(o) => o,
            Err(_) => {
                warn!(
                    "Unpack of .lma artifact {} timed out after {:?}; skipping",
                    path.display(),
                    unpack_timeout
                );
                let _ = tokio::fs::remove_dir_all(&staging).await;
                continue;
            }
        };

        match output {
            Ok(out) if out.status.success() => {
                move_staging_into_repo(&staging, repo_path).await;
                info!(
                    "Auto-unpacked .lma artifact: {}",
                    path.file_name().unwrap_or_default().to_string_lossy()
                );
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                warn!(
                    "Failed to unpack .lma artifact {}: {}",
                    path.display(),
                    stderr.trim()
                );
            }
            Err(e) => {
                warn!(
                    "Failed to run unpack for .lma artifact {}: {}",
                    path.display(),
                    e
                );
            }
        }
        // Staging cleanup on every path (success move leaves it empty).
        let _ = tokio::fs::remove_dir_all(&staging).await;
    }
}

/// H6: model name from the packer naming convention `{name}_v{version}.lma`.
/// Non-conforming names return None (C3: convention holes accepted — the
/// check is skipped, the artifact is still unpacked).
fn model_name_from_artifact(file_name: Option<&std::ffi::OsStr>) -> Option<String> {
    let stem = file_name?.to_str()?.strip_suffix(".lma")?;
    let idx = stem.rfind("_v")?;
    if idx == 0 {
        return None;
    }
    Some(stem[..idx].to_string())
}

/// H4: move each extracted directory from the staging dir into the repo
/// root with swap semantics — an existing dir is renamed aside
/// (dot-prefixed, invisible to scan_repo_models), the new one is renamed
/// in, and the aside copy is removed. A failed move rolls the old tree
/// back.
async fn move_staging_into_repo(staging: &Path, repo_path: &Path) {
    let Ok(mut entries) = tokio::fs::read_dir(staging).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let src = entry.path();
        if !src.is_dir() {
            continue;
        }
        let Some(name) = src.file_name().map(|n| n.to_string_lossy().to_string()) else {
            continue;
        };
        let dst = repo_path.join(&name);
        let backup = repo_path.join(format!(".{}.old-{}", name, uuid::Uuid::new_v4()));
        if dst.exists() && tokio::fs::rename(&dst, &backup).await.is_err() {
            continue;
        }
        if tokio::fs::rename(&src, &dst).await.is_ok() {
            if backup.exists() {
                let _ = tokio::fs::remove_dir_all(&backup).await;
            }
        } else if backup.exists() {
            let _ = tokio::fs::rename(&backup, &dst).await;
        }
    }
}

pub(super) async fn scan_repo_models(repo_path: &Path) -> Vec<RepoModel> {
    let mut models = Vec::new();
    let mut entries = match tokio::fs::read_dir(repo_path).await {
        Ok(e) => e,
        Err(_) => return models,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let model_dir = entry.path();
        if !model_dir.is_dir() {
            continue;
        }
        // 无文件名的目录条目不可作为模型目录,跳过(read_dir 返回的路径实际必有文件名)
        let Some(model_name) = model_dir.file_name().map(|n| n.to_string_lossy().to_string())
        else {
            continue;
        };
        // Legal model names cannot start with a dot (IDENTIFIER_RE in
        // validation.rs), so dot directories (.artifacts, .git, staging
        // dirs) can never be models — skip them.
        if model_name.starts_with('.') {
            continue;
        }

        let mut versions = Vec::new();
        if let Ok(mut version_entries) = tokio::fs::read_dir(&model_dir).await {
            while let Ok(Some(ventry)) = version_entries.next_entry().await {
                let version_dir = ventry.path();
                if !version_dir.is_dir() {
                    continue;
                }
                let Some(version) = version_dir.file_name().map(|n| n.to_string_lossy().to_string())
                else {
                    continue;
                };
                // Same rule as model dirs: dot-prefixed names are staging or
                // backup leftovers, never model versions.
                if version.starts_with('.') {
                    continue;
                }
                let model_py = version_dir.join("model.py");
                let config_yaml = version_dir.join("config.yaml");

                let mut is_ensemble = false;
                if config_yaml.exists() {
                    if let Ok(content) = tokio::fs::read_to_string(&config_yaml).await {
                        is_ensemble = crate::config::config_content_is_ensemble(&content);
                    }
                }

                if model_py.exists() || is_ensemble {
                    versions.push(RepoModel {
                        name: model_name.clone(),
                        version,
                    });
                }
            }
        }

        models.extend(versions);
    }

    models
}

pub(super) fn group_by_model(models: Vec<RepoModel>) -> HashMap<String, Vec<RepoModel>> {
    let mut map: HashMap<String, Vec<RepoModel>> = HashMap::new();
    for m in models {
        map.entry(m.name.clone()).or_default().push(m);
    }
    map
}

/// Check if a filename matches any of the glob-like patterns.
/// Supports simple `*` wildcard (e.g., `*.py`, `model_*.yaml`).
pub(super) fn matches_patterns(filename: &str, patterns: &[String]) -> bool {
    if patterns.is_empty() {
        return true; // no patterns = match all
    }
    patterns.iter().any(|p| {
        if let Some(suffix) = p.strip_prefix("*.") {
            // Simple suffix match: "*.py" matches "foo.py"
            filename.ends_with(&format!(".{}", suffix))
        } else if let Some(prefix) = p.strip_suffix(".*") {
            // Prefix match: "model_.*" matches "model_abc"
            filename.starts_with(prefix)
        } else {
            // Exact match
            filename == p
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_patterns() {
        // Wildcard suffix
        assert!(matches_patterns("model.py", &["*.py".to_string()]));
        assert!(!matches_patterns("model.yaml", &["*.py".to_string()]));

        // Exact match
        assert!(matches_patterns("config.yaml", &["config.yaml".to_string()]));
        assert!(!matches_patterns("other.yaml", &["config.yaml".to_string()]));

        // Empty patterns = match all
        assert!(matches_patterns("anything.txt", &[]));

        // Multiple patterns
        let patterns = vec!["*.py".to_string(), "*.yaml".to_string()];
        assert!(matches_patterns("model.py", &patterns));
        assert!(matches_patterns("config.yaml", &patterns));
        assert!(!matches_patterns("data.json", &patterns));
    }

    // ===== B1 regression guard: ensemble detection in scan_repo_models =====

    /// Regression guard (fixed in e5e45f4): `scan_repo_models` used to detect
    /// ensemble models via `content.contains("ensemble:")`, which matched
    /// comments, documentation strings, and other non-structural
    /// occurrences, causing false positives. Detection is now structural
    /// (`config_content_is_ensemble`).
    #[tokio::test]
    async fn test_scan_repo_models_ensemble_detection_false_positive_on_comment() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ensemble-fp-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("test_model").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();

        // A config.yaml where "ensemble:" only appears in a comment.
        // model.py also exists, so the directory must be discovered as a
        // standard model; the pre-e5e45f4 string-contains check would
        // additionally have misclassified it as ensemble.
        let config = r#"# This model is NOT an ensemble; the line below is a comment.
# ensemble: would be here if it were one
max_batch_size: 4
batch_timeout: 0.05
"#;
        tokio::fs::write(model_dir.join("config.yaml"), config)
            .await
            .unwrap();
        tokio::fs::write(model_dir.join("model.py"), "class MyAPI(LitAPI): pass")
            .await
            .unwrap();

        let result = scan_repo_models(&tmp).await;

        // The ensemble flag is not observable on RepoModel; what we can
        // assert here is that a model with model.py is discovered. The pure
        // false-positive case (comment-only "ensemble:", no model.py) is
        // covered by the next test.
        assert_eq!(result.len(), 1, "model with model.py must be discovered");
        assert_eq!(result[0].name, "test_model");
        assert_eq!(result[0].version, "1");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    /// B1b regression guard: pure false positive — no model.py, only
    /// config.yaml with "ensemble:" in a comment. The pre-e5e45f4
    /// string-contains check incorrectly discovered this as an ensemble
    /// model; structural detection must skip it.
    #[tokio::test]
    async fn test_scan_repo_models_ensemble_detection_pure_false_positive() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-ensemble-fp2-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("no_py_model").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();

        // config.yaml where "ensemble:" only appears in a YAML comment.
        // No model.py — a genuine ensemble would have `ensemble:` as a YAML
        // key, but this has it as a comment/description string.
        let config = r#"description: "This is not an ensemble model"
# ensemble:
#   steps: ...
max_batch_size: 4
"#;
        tokio::fs::write(model_dir.join("config.yaml"), config)
            .await
            .unwrap();

        let result = scan_repo_models(&tmp).await;

        assert!(
            result.is_empty(),
            "directory with 'ensemble:' only in a comment must NOT be \
             discovered as a model. Got {} model(s).",
            result.len()
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== dot-directory skip (staging / VCS / artifact dirs) =====

    /// Dot-prefixed directories can never be models (legal model names
    /// cannot start with a dot — IDENTIFIER_RE in validation.rs). Staging
    /// dirs like `.tmp-upload-*` must not produce ghost model entries.
    #[tokio::test]
    async fn test_scan_repo_models_skips_dot_directories() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-scan-dotskip-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;

        // A staging dir shaped like a model: version dir with model.py.
        let staging = tmp.join(".tmp-upload-abc").join("1");
        tokio::fs::create_dir_all(&staging).await.unwrap();
        tokio::fs::write(staging.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        // A real model must still be discovered.
        let real = tmp.join("real_model").join("1");
        tokio::fs::create_dir_all(&real).await.unwrap();
        tokio::fs::write(real.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        let result = scan_repo_models(&tmp).await;

        assert_eq!(
            result.len(),
            1,
            "dot directories must be skipped, got {:?}",
            result.iter().map(|m| format!("{}:{}", m.name, m.version)).collect::<Vec<_>>()
        );
        assert_eq!(result[0].name, "real_model");
        assert_eq!(result[0].version, "1");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== H6/H4: artifact naming + staging cleanup =====

    #[test]
    fn test_model_name_from_artifact_packer_convention() {
        assert_eq!(
            model_name_from_artifact(Some(std::ffi::OsStr::new("mymodel_v1.lma"))),
            Some("mymodel".to_string())
        );
        // "_v" inside the model name must not confuse the split (rfind).
        assert_eq!(
            model_name_from_artifact(Some(std::ffi::OsStr::new("my_vmodel_v2.lma"))),
            Some("my_vmodel".to_string())
        );
        // Non-conforming names → None (check skipped, still unpacked).
        assert_eq!(
            model_name_from_artifact(Some(std::ffi::OsStr::new("artifact.lma"))),
            None
        );
        assert_eq!(model_name_from_artifact(None), None);
    }

    /// H4: a corrupt artifact must leave no staging residue and no model
    /// directories behind (the unpack fails inside the staging dir).
    #[tokio::test]
    async fn test_auto_unpack_corrupt_artifact_leaves_no_staging() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-unpack-corrupt-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        tokio::fs::write(tmp.join("mymodel_v1.lma"), b"not a zip at all")
            .await
            .unwrap();

        let mut seen = HashSet::new();
        auto_unpack_lma_files(&tmp, &mut seen, Duration::from_secs(30)).await;

        let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(
                !name.starts_with(".tmp-unpack"),
                "staging dir must be cleaned up, found {name}"
            );
            assert_eq!(name, "mymodel_v1.lma", "no other residue allowed");
        }

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}
