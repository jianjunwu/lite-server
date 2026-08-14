//! H7 startup tmp cleanup (model-upload-and-retire plan §六.2): sweep crash
//! leftovers before the initial model load — upload/pack staging dirs (temp
//! + repo root), swap backups and dead-pid worker sockets.

use std::path::Path;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

/// Entry age gate (plan: "> 24h"). Callers pass their own `now`/`max_age`
/// so tests can exercise the boundary without touching file mtimes.
pub(super) const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 3600);

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

    // 2. Repo-root staging dirs and swap backups, plus version-level swap
    // backups one level down: the scanner's root unpack swap parks
    // `.{model}.old-*` at the repo root, while the HTTP upload swap
    // (files.rs swap_dir_into) parks `.{version}.old-*` next to the
    // version dir — both crash windows need the same recovery.
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
                if path.is_dir()
                    && is_stale(&path, now, max_age).await
                    && tokio::fs::remove_dir_all(&path).await.is_ok()
                {
                    removed += 1;
                    info!(path = %path.display(), "startup cleanup: removed stale staging dir");
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

    // 3. Dead-pid worker sockets (unix only — pid-scoped paths).
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
        // tree (deleting it would lose the model/version).
        match tokio::fs::rename(path, &target).await {
            Ok(()) => {
                *restored += 1;
                info!(path = %path.display(), target = %target.display(), "startup cleanup: restored swap backup (target was missing)");
            }
            Err(e) => warn!(path = %path.display(), error = %e, "startup cleanup: failed to restore swap backup"),
        }
    }
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
