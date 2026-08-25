//! H7 startup tmp cleanup (model-upload-and-retire plan §六.2): sweep crash
//! leftovers before the initial model load — upload/pack staging dirs (temp
//! + repo root), swap backups and dead-pid worker sockets.

use std::path::Path;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

/// Entry age gate (plan: "> 24h"). Callers pass their own `now`/`max_age`
/// so tests can exercise the boundary without touching file mtimes.
pub(super) const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 3600);

/// Swap-backup restore window: a crash mid-swap is recovered within an
/// hour; older backups with a missing target are manual-delete residue and
/// are NOT restored (kept on disk, warned about once per boot).
const RESTORE_WINDOW: Duration = Duration::from_secs(3600);

/// Sweep crash residue at startup:
/// - `{temp}/lite-server-upload-*` / `{temp}/lite-server-download-*` dirs
///   older than `max_age`;
/// - repo-root staging dirs `.tmp-upload-*` / `.tmp-unpack-*` older than
///   `max_age`;
/// - swap backups `.{name}.old-{uuid}` (H3/H4 swap semantics), both at the
///   repo root (scanner model-dir swaps) and one level down (HTTP upload
///   version-dir swaps): removed when the target dir exists (the swap
///   completed — the backup is stale), RESTORED when it does not (the
///   crash hit the window between rename-aside and rename-in; restoring
///   is the only non-lossy recovery);
/// - `{temp}/lite-server/*.sock` whose embedded server pid is dead (the
///   pid component is the last underscore field — unparseable names are
///   kept, per "只清陈旧文件").
///
/// Returns `(removed, restored)` counts for the startup log.
pub(super) async fn startup_tmp_cleanup(
    repo_path: &Path,
    temp_root: &Path,
    now: SystemTime,
    max_age: Duration,
) -> (u64, u64) {
    let mut removed = 0u64;
    let mut restored = 0u64;

    // 1. Temp pack/unpack dirs (the download packer's output; the
    // lite-server-upload- prefix is legacy pre-0.9 residue).
    if let Ok(mut entries) = tokio::fs::read_dir(temp_root).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !(name.starts_with("lite-server-upload-")
                || name.starts_with("lite-server-download-"))
            {
                continue;
            }
            if !is_stale(&entry.path(), now, max_age).await {
                continue;
            }
            if tokio::fs::remove_dir_all(entry.path()).await.is_ok() {
                removed += 1;
                info!(path = %entry.path().display(), "startup cleanup: removed stale temp dir");
            }
        }
    }

    // 2. Repo-root staging dirs (shared with the hourly reaper, F5).
    removed += sweep_staging_dirs(repo_path, now, max_age).await;

    // 3. Repo-root swap backups, plus version-level swap backups one level
    // down: the scanner's root unpack swap parks `.{model}.old-*` at the
    // repo root, while the HTTP upload swap (files.rs swap_dir_into) parks
    // `.{version}.old-*` next to the version dir — both crash windows need
    // the same recovery. Also swept here: the registry snapshot's
    // write-transient tmp file (crash residue, removed regardless of age).
    if let Ok(mut entries) = tokio::fs::read_dir(repo_path).await {
        let mut model_dirs: Vec<std::path::PathBuf> = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            let path = entry.path();
            if !name.starts_with('.') {
                if path.is_dir() {
                    model_dirs.push(path);
                }
                continue;
            }
            if name.starts_with(".tmp-upload-") || name.starts_with(".tmp-unpack-") {
                continue; // handled by sweep_staging_dirs above
            }
            // The registry snapshot is written tmp-then-rename (cache.rs);
            // a crash between the two leaves the tmp file forever. It is
            // write-transient — any age is residue.
            if name == crate::registry::cache::SNAPSHOT_TMP_FILENAME {
                if path.is_file() && tokio::fs::remove_file(&path).await.is_ok() {
                    removed += 1;
                    info!(path = %path.display(), "startup cleanup: removed registry snapshot tmp residue");
                }
                continue;
            }
            sweep_swap_backup(repo_path, &name, &path, now, max_age, &mut removed, &mut restored)
                .await;
        }
        for dir in model_dirs {
            if let Ok(mut ventries) = tokio::fs::read_dir(&dir).await {
                while let Ok(Some(ventry)) = ventries.next_entry().await {
                    let vname = ventry.file_name().to_string_lossy().to_string();
                    if !vname.starts_with('.') {
                        continue;
                    }
                    sweep_swap_backup(
                        &dir,
                        &vname,
                        &ventry.path(),
                        now,
                        max_age,
                        &mut removed,
                        &mut restored,
                    )
                    .await;
                }
            }
        }
    }

    // 4. Dead-pid worker sockets (unix only — pid-scoped paths).
    #[cfg(unix)]
    {
        let sock_dir = temp_root.join("lite-server");
        if let Ok(mut entries) = tokio::fs::read_dir(&sock_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                let Some(stem) = name.strip_suffix(".sock") else {
                    continue;
                };
                let Some(pid) = stem.rsplit('_').next().and_then(|p| p.parse::<i32>().ok()) else {
                    continue;
                };
                if !entry.path().is_file() || pid_alive(pid) {
                    continue;
                }
                if tokio::fs::remove_file(entry.path()).await.is_ok() {
                    removed += 1;
                    info!(path = %entry.path().display(), "startup cleanup: removed dead-pid socket");
                }
            }
        }
    }

    (removed, restored)
}

/// Sweep stale repo-root staging dirs `.tmp-upload-*` / `.tmp-unpack-*`,
/// honouring the same age gate + session.json liveness double-gate
/// (staging_dir_is_stale) as the startup sweep. Shared by
/// startup_tmp_cleanup and the hourly reaper (server/mod.rs) — upload
/// sessions abandoned while the server runs must not wait for a restart.
/// Returns the number of dirs removed.
pub(super) async fn sweep_staging_dirs(
    repo_path: &Path,
    now: SystemTime,
    max_age: Duration,
) -> u64 {
    let mut removed = 0u64;
    if let Ok(mut entries) = tokio::fs::read_dir(repo_path).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !(name.starts_with(".tmp-upload-") || name.starts_with(".tmp-unpack-")) {
                continue;
            }
            let path = entry.path();
            if path.is_dir()
                && staging_dir_is_stale(&path, &name, now, max_age).await
                && tokio::fs::remove_dir_all(&path).await.is_ok()
            {
                removed += 1;
                info!(path = %path.display(), "cleanup: removed stale staging dir");
            }
        }
    }
    removed
}

/// Handle one `.{name}.old-{uuid}` swap backup parked inside `parent`:
/// removed when the target exists (the swap completed — the backup is
/// stale), RESTORED when it does not (the crash hit the window between
/// rename-aside and rename-in; restoring is the only non-lossy recovery).
/// Model and version names cannot contain dots (IDENTIFIER_RE /
/// VERSION_RE), so the first ".old-" delimits the original name
/// unambiguously.
async fn sweep_swap_backup(
    parent: &Path,
    name: &str,
    path: &Path,
    now: SystemTime,
    max_age: Duration,
    removed: &mut u64,
    restored: &mut u64,
) {
    let Some(idx) = name.find(".old-") else {
        return;
    };
    if idx == 0 || !path.is_dir() {
        return;
    }
    let target = parent.join(&name[1..idx]);
    if target.exists() {
        // The swap completed before the crash — the backup is stale.
        if is_stale(path, now, max_age).await && tokio::fs::remove_dir_all(path).await.is_ok() {
            *removed += 1;
            info!(path = %path.display(), "startup cleanup: removed stale swap backup");
        }
    } else {
        // Crash between rename-aside and rename-in — restore the previous
        // tree (deleting it would lose the model/version). Gated to the
        // crash-recovery window: a backup older than the window with a
        // missing target is more likely the residue of a MANUAL delete
        // (the delete paths remove backups themselves), and resurrecting
        // it would surprise. It is kept on disk and warned about, not
        // deleted (no data loss).
        if is_stale(path, now, RESTORE_WINDOW).await {
            warn!(
                path = %path.display(),
                "startup cleanup: swap backup past the recovery window with missing target — \
                 NOT restored (manual-delete residue?); remove it manually to silence"
            );
            return;
        }
        match tokio::fs::rename(path, &target).await {
            Ok(()) => {
                *restored += 1;
                info!(path = %path.display(), target = %target.display(), "startup cleanup: restored swap backup (target was missing)");
            }
            Err(e) => warn!(path = %path.display(), error = %e, "startup cleanup: failed to restore swap backup"),
        }
    }
}

/// Staleness for staging dirs. Chunked upload sessions keep their liveness
/// in `session.json`'s mtime (every chunk PUT touches it; writes into the
/// `.chunks/` subdirs do NOT refresh the top-level dir's mtime), so a
/// `.tmp-upload-*` dir with a session.json is stale only when BOTH mtimes
/// are past the gate — an active long-running resumable upload must never
/// be swept mid-flight.
async fn staging_dir_is_stale(path: &Path, name: &str, now: SystemTime, max_age: Duration) -> bool {
    if !is_stale(path, now, max_age).await {
        return false;
    }
    if name.starts_with(".tmp-upload-") {
        let meta = path.join("session.json");
        if meta.is_file() && !is_stale(&meta, now, max_age).await {
            return false;
        }
    }
    true
}

/// Whether `path` is older than `max_age` relative to `now`. Missing
/// metadata counts as stale (removal is safe — the entry is already broken).
async fn is_stale(path: &Path, now: SystemTime, max_age: Duration) -> bool {
    let Ok(md) = tokio::fs::metadata(path).await else {
        return true;
    };
    let Ok(mtime) = md.modified() else {
        return true;
    };
    match now.duration_since(mtime) {
        Ok(age) => age > max_age,
        Err(_) => true, // clock skew / future mtime — treat as stale
    }
}

/// Whether a pid refers to a live process (ESRCH = dead; EPERM = alive but
/// owned elsewhere — keep).
#[cfg(unix)]
fn pid_alive(pid: i32) -> bool {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lite-server-cleanup-test-{}-{}-{}",
            tag,
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    fn a_dead_pid() -> i32 {
        // Spawn and reap a child: its pid is guaranteed dead afterwards.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id() as i32;
        let _ = child.wait();
        pid
    }

    #[tokio::test]
    async fn removes_stale_temp_pack_dirs_and_keeps_fresh_ones() {
        // A private temp root — the real temp dir is shared with parallel
        // tests, so fixtures (and count assertions) must not touch it.
        let temp_root = unique_tmp("temp");
        tokio::fs::create_dir_all(&temp_root).await.unwrap();
        let stale = temp_root.join(format!("lite-server-download-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&stale).await.unwrap();
        let now = SystemTime::now();
        let (removed, restored) =
            startup_tmp_cleanup(&unique_tmp("repo"), &temp_root, now, Duration::ZERO).await;
        assert_eq!((removed, restored), (1, 0), "stale dir removed, nothing restored");
        assert!(!stale.exists());

        // A fresh dir survives an ordinary (24h) sweep.
        let fresh = temp_root.join(format!("lite-server-download-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&fresh).await.unwrap();
        let (removed, _) = startup_tmp_cleanup(
            &unique_tmp("repo"),
            &temp_root,
            SystemTime::now(),
            DEFAULT_MAX_AGE,
        )
        .await;
        assert_eq!(removed, 0, "fresh temp dir must be kept");
        let _ = tokio::fs::remove_dir_all(&temp_root).await;
    }

    #[tokio::test]
    async fn keeps_active_chunked_upload_sessions() {
        // A chunked-upload staging dir whose session.json was touched
        // recently is ACTIVE — it must survive the sweep even when the
        // top-level dir mtime is old (chunk writes don't refresh it).
        let repo_active = unique_tmp("staging-active");
        let active = repo_active.join(format!(".tmp-upload-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&active).await.unwrap();
        tokio::fs::write(active.join("session.json"), "{}")
            .await
            .unwrap();
        let old = SystemTime::now() - Duration::from_secs(48 * 3600);
        std::fs::File::open(&active)
            .unwrap()
            .set_modified(old)
            .unwrap();

        let (removed_active, _) = startup_tmp_cleanup(
            &repo_active,
            &unique_tmp("temp"),
            SystemTime::now(),
            DEFAULT_MAX_AGE,
        )
        .await;
        assert_eq!(removed_active, 0, "an active upload session must not be swept");
        assert!(active.exists());

        // Once session.json ALSO ages out, the dir is swept.
        std::fs::File::open(active.join("session.json"))
            .unwrap()
            .set_modified(old)
            .unwrap();
        let (removed_stale, _) = startup_tmp_cleanup(
            &repo_active,
            &unique_tmp("temp"),
            SystemTime::now(),
            DEFAULT_MAX_AGE,
        )
        .await;
        assert_eq!(removed_stale, 1, "a fully-stale session dir must be swept");
        assert!(!active.exists());

        let _ = tokio::fs::remove_dir_all(&repo_active).await;
    }

    #[tokio::test]
    async fn removes_stale_repo_staging_dirs_only() {
        let repo = unique_tmp("staging");
        let stale_upload = repo.join(format!(".tmp-upload-{}", uuid::Uuid::new_v4()));
        let stale_unpack = repo.join(format!(".tmp-unpack-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&stale_upload).await.unwrap();
        tokio::fs::create_dir_all(&stale_unpack).await.unwrap();
        // A real model dir (dot-free) must never be touched.
        tokio::fs::create_dir_all(repo.join("mymodel").join("1"))
            .await
            .unwrap();

        let (removed, _) = startup_tmp_cleanup(
            &repo,
            &unique_tmp("temp"),
            SystemTime::now(),
            Duration::ZERO,
        )
        .await;
        assert_eq!(removed, 2, "both stale staging dirs removed");
        assert!(repo.join("mymodel").join("1").exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// F5: the extracted staging-dir sweep (shared by startup and the
    /// hourly reaper) removes fully-stale staging dirs, keeps active
    /// upload sessions (session.json liveness double-gate) and never
    /// touches real model dirs.
    #[tokio::test]
    async fn sweep_staging_dirs_removes_stale_and_keeps_active_sessions() {
        let repo = unique_tmp("sweep");
        let stale = repo.join(format!(".tmp-upload-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&stale).await.unwrap();
        let old = SystemTime::now() - Duration::from_secs(48 * 3600);
        std::fs::File::open(&stale)
            .unwrap()
            .set_modified(old)
            .unwrap();
        let active = repo.join(format!(".tmp-upload-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&active).await.unwrap();
        tokio::fs::write(active.join("session.json"), "{}")
            .await
            .unwrap();
        // The active dir's top-level mtime is old; session.json is fresh.
        std::fs::File::open(&active)
            .unwrap()
            .set_modified(old)
            .unwrap();
        // A real model dir (dot-free) must never be touched.
        tokio::fs::create_dir_all(repo.join("mymodel").join("1"))
            .await
            .unwrap();

        let removed = sweep_staging_dirs(&repo, SystemTime::now(), DEFAULT_MAX_AGE).await;
        assert_eq!(removed, 1, "only the fully-stale staging dir is swept");
        assert!(!stale.exists());
        assert!(active.exists(), "an active upload session must survive");
        assert!(repo.join("mymodel").join("1").exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// The registry snapshot is written tmp-then-rename (cache.rs); a
    /// crash between the two leaves the tmp file forever. The name is
    /// write-transient — the sweep removes it regardless of age.
    #[tokio::test]
    async fn removes_registry_snapshot_tmp_residue_regardless_of_age() {
        let repo = unique_tmp("regtmp");
        tokio::fs::create_dir_all(&repo).await.unwrap();
        let tmp_file = repo.join(crate::registry::cache::SNAPSHOT_TMP_FILENAME);
        // A FRESH file: age must not matter for the write-transient name.
        tokio::fs::write(&tmp_file, "{}").await.unwrap();

        let (removed, _) = startup_tmp_cleanup(
            &repo,
            &unique_tmp("temp"),
            SystemTime::now(),
            DEFAULT_MAX_AGE,
        )
        .await;
        assert_eq!(removed, 1, "fresh registry tmp residue must be removed");
        assert!(!tmp_file.exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// A swap backup past the crash-recovery window (RESTORE_WINDOW) whose
    /// target is missing is manual-delete residue: NOT restored (a deleted
    /// model must not resurrect), but kept on disk (no data loss).
    #[tokio::test]
    async fn stale_swap_backup_with_missing_target_is_not_restored() {
        let repo = unique_tmp("stale-restore");
        let backup = repo.join(format!(".mymodel.old-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(backup.join("1")).await.unwrap();
        tokio::fs::write(backup.join("1").join("model.py"), "old")
            .await
            .unwrap();
        // Backdate beyond the restore window.
        let old = SystemTime::now() - Duration::from_secs(2 * 3600);
        std::fs::File::open(&backup)
            .unwrap()
            .set_modified(old)
            .unwrap();

        let (_removed, restored) = startup_tmp_cleanup(
            &repo,
            &unique_tmp("temp"),
            SystemTime::now(),
            Duration::ZERO,
        )
        .await;
        assert_eq!(restored, 0, "past the recovery window → NOT restored");
        assert!(backup.exists(), "kept on disk (no silent data loss)");
        assert!(!repo.join("mymodel").exists(), "no resurrection");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn restores_swap_backup_when_target_is_missing() {
        let repo = unique_tmp("restore");
        let backup = repo.join(format!(".mymodel.old-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(backup.join("1")).await.unwrap();
        tokio::fs::write(backup.join("1").join("model.py"), "old")
            .await
            .unwrap();

        let (removed, restored) = startup_tmp_cleanup(
            &repo,
            &unique_tmp("temp"),
            SystemTime::now(),
            Duration::ZERO,
        )
        .await;
        assert_eq!(removed, 0);
        assert_eq!(restored, 1, "the swap backup must be restored, not deleted");
        assert!(repo.join("mymodel").join("1").join("model.py").exists());
        assert!(!backup.exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn removes_stale_swap_backup_when_target_exists() {
        let repo = unique_tmp("stale-backup");
        tokio::fs::create_dir_all(repo.join("mymodel").join("1"))
            .await
            .unwrap();
        let backup = repo.join(format!(".mymodel.old-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&backup).await.unwrap();

        let (removed, restored) = startup_tmp_cleanup(
            &repo,
            &unique_tmp("temp"),
            SystemTime::now(),
            Duration::ZERO,
        )
        .await;
        assert_eq!((removed, restored), (1, 0), "completed swap → backup is stale");
        assert!(repo.join("mymodel").exists());
        assert!(!backup.exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn restores_version_level_swap_backup_when_target_is_missing() {
        // The HTTP upload swap (files.rs swap_dir_into) parks the backup
        // NEXT TO the version dir — repo/{model}/.{version}.old-{uuid} —
        // not at the repo root. A crash between rename-aside and rename-in
        // must recover that window too, else the old version tree is
        // stranded invisible (dot-prefixed) and the version is lost.
        let repo = unique_tmp("vrestore");
        let backup = repo
            .join("mymodel")
            .join(format!(".1.old-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&backup).await.unwrap();
        tokio::fs::write(backup.join("model.py"), "old")
            .await
            .unwrap();

        let (_removed, restored) = startup_tmp_cleanup(
            &repo,
            &unique_tmp("temp"),
            SystemTime::now(),
            Duration::ZERO,
        )
        .await;
        assert_eq!(
            restored, 1,
            "version-level swap backup must be restored, not stranded"
        );
        assert!(repo.join("mymodel").join("1").join("model.py").exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn deleted_model_swap_backup_is_not_restored_at_startup() {
        // O1 resurrection chain: a fresh swap backup survives its swap,
        // the model is then deleted via the API, and the server restarts
        // within the staleness window — the sweep must find nothing to
        // restore, or the deleted model comes back (G5's exact threat).
        use crate::callback::{CallbackRunner, Protocol};
        use crate::config::Config;
        use crate::http::state::AppState;
        use crate::inference_queue::InferenceQueue;
        use crate::registry::ModelRegistry;
        use crate::worker::WorkerManager;
        use std::sync::Arc;

        let repo = unique_tmp("resurrect");
        tokio::fs::create_dir_all(repo.join("mymodel").join("1"))
            .await
            .unwrap();
        tokio::fs::write(repo.join("mymodel").join("1").join("model.py"), "x = 1")
            .await
            .unwrap();
        let backup = repo.join(format!(".mymodel.old-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(backup.join("1")).await.unwrap();
        tokio::fs::write(backup.join("1").join("model.py"), "old")
            .await
            .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            queue.clone(),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let state = Arc::new(AppState::new(
            registry,
            wm,
            queue,
            Config::default(),
            repo.clone(),
            Arc::new(CallbackRunner::new()),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ));
        crate::http::handlers::admin::delete_model_core(
            &state,
            &state.access_control,
            None,
            Protocol::Http,
            "mymodel",
            false,
        )
        .await
        .expect("delete must succeed");

        let (_removed, restored) = startup_tmp_cleanup(
            &repo,
            &unique_tmp("temp"),
            SystemTime::now(),
            DEFAULT_MAX_AGE,
        )
        .await;
        assert_eq!(restored, 0, "a deleted model must not be restored");
        assert!(
            !repo.join("mymodel").exists(),
            "the deleted model must stay deleted"
        );

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn removes_dead_pid_sockets_and_keeps_live_ones() {
        let temp_root = unique_tmp("temp");
        let sock_dir = temp_root.join("lite-server");
        tokio::fs::create_dir_all(&sock_dir).await.unwrap();

        let dead = sock_dir.join(format!("m_1_0_{}.sock", a_dead_pid()));
        let live = sock_dir.join(format!("m_1_0_{}.sock", std::process::id()));
        let unparseable = sock_dir.join("not-a-sock-file.txt");
        tokio::fs::write(&dead, b"").await.unwrap();
        tokio::fs::write(&live, b"").await.unwrap();
        tokio::fs::write(&unparseable, b"").await.unwrap();

        let (removed, _) = startup_tmp_cleanup(
            &unique_tmp("repo"),
            &temp_root,
            SystemTime::now(),
            DEFAULT_MAX_AGE,
        )
        .await;
        assert_eq!(removed, 1, "only the dead-pid socket is removed");
        assert!(!dead.exists());
        assert!(live.exists(), "live server socket must be kept");
        assert!(unparseable.exists(), "unparseable names must be kept");

        let _ = tokio::fs::remove_dir_all(&temp_root).await;
    }
}
