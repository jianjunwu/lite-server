//! `cache_registry` (config.rs `server.cache_registry`): snapshot the model
//! registry to disk on graceful shutdown and restore it on startup, so an
//! admin-activated version survives a restart without the operator re-issuing
//! the call. Caveat for admin-ADDED models (no config entry): the restart
//! load path only walks configured `load_models` outside `control_mode: "all"`
//! (see `reconcile.rs`), so under `explicit`/`auto` a restored strategy+pin
//! survives but the model stays inert (no workers) until it is added to config
//! or `control_mode` becomes `"all"`. Admin-activated versions of config
//! models work in every mode (the model loads from config; the pin re-picks
//! the admin's version).
//!
//! What is persisted: per-model strategy (`load_policy`, `max_loaded_versions`,
//! `weights`) and the active-version pins. The version table itself is NOT
//! persisted — seeding it would create zombie entries (reconcile skips
//! already-registered versions at `reconcile.rs`, and lifecycle rejects
//! duplicate loads), so workers would never spawn. Instead the normal load
//! path re-spawns workers; reconcile converges; its auto-activate fallback is
//! suppressed because a seeded pin already exists.
//!
//! Precedence: restore runs BEFORE the config-strategy loop, so a config's
//! explicit strategy wins; reconcile's `default_version` branch (also config)
//! overrides a seeded pin when set. The pin therefore survives only for models
//! without a config `default_version` — exactly the admin-managed case.
//!
//! Reset: delete `<repo>/.lite-server-registry.json`.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::registry::types::LoadPolicy;
use crate::registry::ModelRegistry;

/// On-disk snapshot file, written at the model-repository root.
const SNAPSHOT_FILENAME: &str = ".lite-server-registry.json";
/// Tmp suffix for the atomic write (same dir → same filesystem → atomic rename).
/// NOT `.lma` — the scanner's `auto_unpack_lma_files` would otherwise try to
/// unpack it.
const SNAPSHOT_TMP_SUFFIX: &str = ".tmp";

/// On-disk snapshot. `BTreeMap` for stable, diff-friendly JSON key ordering
/// (the live registry uses `DashMap`/`HashMap`, whose iteration order is random).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RegistrySnapshot {
    models: BTreeMap<String, PersistedModel>,
    active_versions: BTreeMap<String, String>,
}

/// The per-model strategy fields that are not derivable from disk. Runtime
/// fields (workers, status, timestamps) are deliberately absent — they are
/// re-established by the load path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedModel {
    /// `"all"` | `"latest"` | `"explicit"` — the lowercase form `set_strategy`
    /// parses (the `LoadPolicy` enum serializes to capitalized variant names,
    /// which would not round-trip through `set_strategy`).
    load_policy: String,
    max_loaded_versions: Option<usize>,
    weights: BTreeMap<String, u32>,
}

fn load_policy_to_string(p: LoadPolicy) -> &'static str {
    match p {
        LoadPolicy::All => "all",
        LoadPolicy::Latest => "latest",
        LoadPolicy::Explicit => "explicit",
    }
}

fn snapshot_path(repo_path: &Path) -> std::path::PathBuf {
    repo_path.join(SNAPSHOT_FILENAME)
}

fn build_snapshot(registry: &ModelRegistry) -> RegistrySnapshot {
    let models = registry
        .snapshot_strategies()
        .into_iter()
        .map(|(name, load_policy, max_loaded_versions, weights)| {
            (
                name,
                PersistedModel {
                    load_policy: load_policy_to_string(load_policy).to_string(),
                    max_loaded_versions,
                    weights,
                },
            )
        })
        .collect();
    RegistrySnapshot {
        models,
        active_versions: registry.snapshot_active_versions(),
    }
}

/// Snapshot the registry to `<repo>/.lite-server-registry.json` atomically
/// (write `.tmp` then rename, both in the repo dir). Errors are returned so the
/// caller can warn and continue — a snapshot failure must never block shutdown.
pub async fn save(registry: &ModelRegistry, repo_path: &Path) -> Result<(), AppError> {
    let snapshot = build_snapshot(registry);
    let json = serde_json::to_string_pretty(&snapshot)
        .map_err(|e| AppError::Internal(format!("registry snapshot serialize: {e}")))?;
    let dest = snapshot_path(repo_path);
    let tmp = repo_path.join(format!("{SNAPSHOT_FILENAME}{SNAPSHOT_TMP_SUFFIX}"));
    tokio::fs::write(&tmp, json)
        .await
        .map_err(|e| AppError::Internal(format!("registry snapshot write: {e}")))?;
    tokio::fs::rename(&tmp, &dest)
        .await
        .map_err(|e| AppError::Internal(format!("registry snapshot rename: {e}")))?;
    Ok(())
}

/// Restore strategy + active-version pins from the snapshot. Missing file is a
/// no-op (returns 0); a corrupt file is warned about and skipped. Only models
/// and versions whose directories still exist on disk are restored (avoids
/// zombie entries / stale pins for since-deleted models). Returns the number of
/// models whose strategy was restored.
pub async fn restore(registry: &ModelRegistry, repo_path: &Path) -> usize {
    let path = snapshot_path(repo_path);
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "registry cache: failed to read snapshot, skipping restore"
            );
            return 0;
        }
    };
    let snapshot: RegistrySnapshot = match serde_json::from_slice(&bytes) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "registry cache: corrupt snapshot, skipping restore (delete the file to reset)"
            );
            return 0;
        }
    };
    let mut restored = 0usize;
    for (name, pm) in &snapshot.models {
        // Skip models whose directory no longer exists (deleted since snapshot).
        if !repo_path.join(name).is_dir() {
            continue;
        }
        let strategy = crate::config::ModelStrategyConfig {
            name: name.clone(),
            load_policy: pm.load_policy.clone(),
            max_loaded_versions: pm.max_loaded_versions,
            weights: Some(pm.weights.iter().map(|(k, v)| (k.clone(), *v)).collect()),
            ..Default::default()
        };
        if let Err(e) = registry.set_strategy(name, &strategy) {
            tracing::warn!(
                model = %name,
                error = %e,
                "registry cache: set_strategy failed, skipping model"
            );
            continue;
        }
        restored += 1;
    }
    for (name, version) in &snapshot.active_versions {
        // Only seed the pin if the version directory still exists on disk.
        if repo_path.join(name).join(version).is_dir() {
            registry.force_pin_active_version(name, version);
        }
    }
    restored
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ModelConfig, ModelStrategyConfig};
    use crate::registry::types::ModelType;
    use crate::registry::ModelRegistry;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_config() -> ModelConfig {
        ModelConfig {
            max_batch_size: 1,
            ..Default::default()
        }
    }

    /// RAII temp dir (tempfile isn't a dependency). Unique per call; removed on drop.
    struct TmpRepo(PathBuf);
    impl TmpRepo {
        fn new(label: &str) -> Self {
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "lite-server-cache-test-{}-{n}-{label}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            TmpRepo(path)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TmpRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn snapshot_roundtrips_through_json() {
        let snap = RegistrySnapshot {
            models: {
                let mut m = BTreeMap::new();
                m.insert(
                    "m1".to_string(),
                    PersistedModel {
                        load_policy: "all".to_string(),
                        max_loaded_versions: Some(2),
                        weights: {
                            let mut w = BTreeMap::new();
                            w.insert("1".to_string(), 3);
                            w.insert("2".to_string(), 7);
                            w
                        },
                    },
                );
                m
            },
            active_versions: {
                let mut a = BTreeMap::new();
                a.insert("m1".to_string(), "2".to_string());
                a
            },
        };
        let json = serde_json::to_string_pretty(&snap).unwrap();
        let back: RegistrySnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snap);
        // BTreeMap → deterministic, sorted key ordering in the serialized form.
        assert!(json.contains("\"m1\""));
    }

    #[tokio::test]
    async fn save_then_restore_roundtrips_strategy_and_pin() {
        let repo = TmpRepo::new("roundtrip");
        let repo_path = repo.path().to_path_buf();
        // Lay down m1/1 on disk so restore's dir-existence check passes.
        let model_dir = repo_path.join("m1").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.py"), "# model").unwrap();

        // Build a registry with one registered model, a strategy, and a pin.
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, model_dir.clone())
            .unwrap();
        let strategy = ModelStrategyConfig {
            name: "m1".to_string(),
            load_policy: "all".to_string(),
            max_loaded_versions: Some(2),
            weights: Some({
                let mut w = std::collections::HashMap::new();
                w.insert("1".to_string(), 5u32);
                w
            }),
            ..Default::default()
        };
        reg.set_strategy("m1", &strategy).unwrap();
        reg.force_pin_active_version("m1", "1");

        // Snapshot to disk.
        save(&reg, &repo_path).await.expect("save");
        assert!(
            repo_path.join(SNAPSHOT_FILENAME).is_file(),
            "snapshot file must exist after save"
        );
        assert!(
            !repo_path
                .join(format!("{SNAPSHOT_FILENAME}{SNAPSHOT_TMP_SUFFIX}"))
                .exists(),
            "tmp file must be renamed away"
        );

        // Restore into a fresh registry (simulating restart).
        let reg2 = ModelRegistry::new();
        let restored = restore(&reg2, &repo_path).await;
        assert_eq!(restored, 1, "one model strategy restored");
        let s = reg2.get_strategy("m1").expect("strategy restored");
        assert_eq!(s.load_policy, "all");
        assert_eq!(s.max_loaded_versions, Some(2));
        assert_eq!(s.weights.as_ref().unwrap().get("1"), Some(&5));
        assert_eq!(reg2.get_active_version("m1").as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn restore_missing_file_is_noop() {
        let repo = TmpRepo::new("missing");
        let reg = ModelRegistry::new();
        let restored = restore(&reg, repo.path()).await;
        assert_eq!(restored, 0, "missing snapshot → no-op");
    }

    #[tokio::test]
    async fn restore_corrupt_file_is_noop() {
        let repo = TmpRepo::new("corrupt");
        std::fs::write(repo.path().join(SNAPSHOT_FILENAME), "{ not valid json").unwrap();
        let reg = ModelRegistry::new();
        let restored = restore(&reg, repo.path()).await;
        assert_eq!(restored, 0, "corrupt snapshot → skipped, no panic");
    }

    #[tokio::test]
    async fn restore_skips_deleted_model_and_stale_pin() {
        // Snapshot had m1/1 and m2/2, but m2's dir is gone on restart; the stale
        // m2 pin must NOT be seeded.
        let repo = TmpRepo::new("deleted");
        let live = repo.path().join("m1").join("1");
        std::fs::create_dir_all(&live).unwrap();
        std::fs::write(live.join("model.py"), "# model").unwrap();
        // m2 dir intentionally absent.
        let snap = RegistrySnapshot {
            models: {
                let mut m = BTreeMap::new();
                m.insert(
                    "m1".to_string(),
                    PersistedModel {
                        load_policy: "explicit".to_string(),
                        max_loaded_versions: None,
                        weights: BTreeMap::new(),
                    },
                );
                m.insert(
                    "m2".to_string(),
                    PersistedModel {
                        load_policy: "explicit".to_string(),
                        max_loaded_versions: None,
                        weights: BTreeMap::new(),
                    },
                );
                m
            },
            active_versions: {
                let mut a = BTreeMap::new();
                a.insert("m1".to_string(), "1".to_string());
                a.insert("m2".to_string(), "2".to_string());
                a
            },
        };
        std::fs::write(
            repo.path().join(SNAPSHOT_FILENAME),
            serde_json::to_string(&snap).unwrap(),
        )
        .unwrap();

        let reg = ModelRegistry::new();
        let restored = restore(&reg, repo.path()).await;
        assert_eq!(restored, 1, "only m1 (dir present) restored");
        assert_eq!(reg.get_active_version("m1").as_deref(), Some("1"));
        assert!(
            reg.get_active_version("m2").is_none(),
            "stale pin for deleted m2 must not be seeded"
        );
    }
}

