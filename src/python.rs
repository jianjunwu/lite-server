//! Resolve the Python interpreter for shelling out to the `lite_server`
//! Python CLI/module — worker spawn, `.lma` unpack (repository scanner + file
//! upload), and `.lma` pack (directory download). All of these previously
//! hardcoded `python`, which is absent on Debian/Ubuntu and most container base
//! images that ship only `python3` (no `python-is-python3`).

/// Resolve the Python interpreter used to spawn Python subprocesses.
///
/// `LITESERVER_PYTHON` overrides everything (operator escape hatch for a pinned
/// interpreter). Otherwise probe PATH for `python3` first and fall back to
/// `python`; if neither is on PATH, return `python` so `Command` surfaces the
/// original spawn-not-found diagnostic unchanged.
pub(crate) fn resolve_python_interpreter() -> String {
    resolve_with(
        std::env::var_os("LITESERVER_PYTHON").as_deref(),
        std::env::var_os("PATH").as_deref(),
    )
}

/// Pure resolver — separated from env reads so it is unit-testable without
/// mutating process-global environment (cargo runs lib tests in parallel).
fn resolve_with(
    override_interp: Option<&std::ffi::OsStr>,
    path: Option<&std::ffi::OsStr>,
) -> String {
    if let Some(p) = override_interp {
        if !p.is_empty() {
            return p.to_string_lossy().into_owned();
        }
    }
    if let Some(path) = path {
        for candidate in ["python3", "python"] {
            if find_in_path(candidate, path).is_some() {
                return candidate.to_string();
            }
        }
    }
    "python".to_string()
}

/// First directory on PATH holding an executable `name` (mirrors `shutil.which`;
/// honours PATHEXT on Windows). `None` if not found in any PATH entry.
fn find_in_path(name: &str, path: &std::ffi::OsStr) -> Option<std::path::PathBuf> {
    for dir in std::env::split_paths(path) {
        for cand in exe_candidates(name) {
            let full = dir.join(&cand);
            if full.is_file() {
                return Some(full);
            }
        }
    }
    None
}

#[cfg(windows)]
fn exe_candidates(name: &str) -> Vec<String> {
    if name.to_ascii_lowercase().ends_with(".exe") {
        return vec![name.to_string()];
    }
    std::env::var("PATHEXT")
        .unwrap_or_else(|_| ".EXE;.CMD;.BAT".into())
        .split(';')
        .map(|ext| format!("{name}{ext}"))
        .collect()
}

#[cfg(not(windows))]
fn exe_candidates(name: &str) -> Vec<String> {
    vec![name.to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Create a temp PATH entry containing only the named files (empty regular
    /// files — `is_file()` is all the resolver checks). Unique per call so the
    /// parallel lib tests never share a directory.
    fn path_entry_with(files: &[&str]) -> OsString {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir: PathBuf = std::env::temp_dir()
            .join(format!("lite-server-pyresolve-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for f in files {
            std::fs::write(dir.join(f), b"").unwrap();
        }
        dir.into_os_string()
    }

    #[test]
    fn prefers_python3_when_only_python3_present() {
        let path = path_entry_with(&["python3"]);
        assert_eq!(resolve_with(None, Some(&path)), "python3");
    }

    #[test]
    fn falls_back_to_python_when_only_python_present() {
        let path = path_entry_with(&["python"]);
        assert_eq!(resolve_with(None, Some(&path)), "python");
    }

    #[test]
    fn prefers_python3_when_both_present() {
        let path = path_entry_with(&["python", "python3"]);
        assert_eq!(resolve_with(None, Some(&path)), "python3");
    }

    #[test]
    fn override_takes_precedence_over_path() {
        let path = path_entry_with(&["python3"]);
        assert_eq!(
            resolve_with(Some(std::ffi::OsStr::new("/opt/pinned/python")), Some(&path)),
            "/opt/pinned/python"
        );
    }

    #[test]
    fn defaults_to_python_when_neither_present() {
        let path = path_entry_with(&["ls", "cat"]);
        assert_eq!(resolve_with(None, Some(&path)), "python");
    }

    #[test]
    fn defaults_to_python_when_path_unset() {
        assert_eq!(resolve_with(None, None), "python");
    }
}
