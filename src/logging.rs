use std::fs::{File, OpenOptions, rename};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Guard that must be held to keep background log flush threads alive.
#[derive(Debug)]
pub struct LogGuard {
    _info_guard: Option<WorkerGuard>,
    _error_guard: Option<WorkerGuard>,
}

impl LogGuard {
    pub fn noop() -> Self {
        Self {
            _info_guard: None,
            _error_guard: None,
        }
    }
}

/// Normalize a CLI/config log level to the vocabulary tracing's EnvFilter accepts.
///
/// B3: the benchmark harness and Python users spell the level `warning` (the
/// Python `logging` name); tracing only accepts `warn`, so an un-normalized
/// `lite_server=warning` directive is dropped with an "error parsing level
/// filter" line and the server's own WARN logs go silent. Map the alias; leave
/// every other token untouched so we never rewrite a value we don't own.
fn normalize_log_level(level: &str) -> String {
    if level.eq_ignore_ascii_case("warning") {
        "warn".to_string()
    } else {
        level.to_string()
    }
}

// allow: 启动一次性初始化,参数即日志配置各字段(level/输出/轮转/OTel
// layer),两调用点(lib/main)直读配置装配,无收编语义。
#[allow(clippy::too_many_arguments)]
pub fn init(
    level: &str,
    info_output: Option<&str>,
    error_output: Option<&str>,
    rotation: &str,
    max_size: usize,
    backup_count: usize,
    include_hostname: bool,
    // P-TRACE: optional OTel layer (`telemetry.enabled` + feature). Attached in the
    // same `.init()` so tracing spans bridge to OTel spans when telemetry is on.
    otel_layer: Option<crate::telemetry::BoxedLayer>,
) -> LogGuard {
    // B3: normalize the level alias before building the filter (see helper).
    let level = normalize_log_level(level);
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("lite_server={},tokio=warn,hyper=warn", level)));

    // stdout layer (always)
    let stdout_layer = fmt::layer().with_target(true).with_thread_ids(true);

    let mut info_guard = None;
    let info_layer = if let Some(path) = info_output {
        match create_writer(path, rotation, max_size, backup_count, include_hostname) {
            Ok((writer, guard)) => {
                info_guard = Some(guard);
                Some(
                    fmt::layer()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(filter.clone()),
                )
            }
            Err(e) => {
                eprintln!("Failed to create info log file '{}': {}", path, e);
                None
            }
        }
    } else {
        None
    };

    let mut error_guard = None;
    let error_filter = EnvFilter::new("error");
    let error_layer = if let Some(path) = error_output {
        match create_writer(path, rotation, max_size, backup_count, include_hostname) {
            Ok((writer, guard)) => {
                error_guard = Some(guard);
                Some(
                    fmt::layer()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(error_filter),
                )
            }
            Err(e) => {
                eprintln!("Failed to create error log file '{}': {}", path, e);
                None
            }
        }
    } else {
        None
    };

    // P-TRACE: the OTel layer is `Layer<Registry>` (a boxed trait object), so it
    // must ride INNERMOST — applied to the bare registry before the fmt layers
    // (which are `Layer<S>` for any subscriber and adapt on top). When telemetry
    // is off, a no-op `Identity` layer keeps the subscriber type uniform.
    use tracing_subscriber::layer::Identity;
    let otel: crate::telemetry::BoxedLayer =
        otel_layer.unwrap_or_else(|| Box::new(Identity::new()));
    // Idempotent: the tracing global subscriber is a process singleton.
    // `.init()` panics on a second call, which would crash a process that calls
    // serve() again (e.g. stop_server() then restart, or an embedder re-entering
    // serve after a graceful stop). Ignore SetGlobalDefaultError so the first
    // subscriber stays installed; a later call's log level is not reapplied
    // (acceptable for re-serve).
    let _ = tracing_subscriber::registry()
        .with(otel)
        .with(filter)
        .with(stdout_layer)
        .with(info_layer)
        .with(error_layer)
        .try_init();

    LogGuard {
        _info_guard: info_guard,
        _error_guard: error_guard,
    }
}

/// Sanitize a hostname into filename-safe characters: keep `[A-Za-z0-9._-]`,
/// replace anything else (spaces, slashes, colons, ...) with `-`.
/// Dots are preserved so a FQDN like `node01.example.com` stays intact.
fn sanitize_hostname(host: &str) -> String {
    host.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Insert `hostname` into a filename as `{stem}-{hostname}.{ext}`, keeping the
/// extension. Returns the original name unchanged when `hostname` is empty.
fn with_hostname(file_name: &str, hostname: &str) -> String {
    if hostname.is_empty() {
        return file_name.to_string();
    }
    let p = std::path::Path::new(file_name);
    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or(file_name);
    match p.extension().and_then(|e| e.to_str()) {
        Some(ext) if !ext.is_empty() => format!("{}-{}.{}", stem, hostname, ext),
        _ => format!("{}-{}", stem, hostname),
    }
}

/// Best-effort system hostname, sanitized for use in a filename.
/// Returns `None` if the hostname cannot be read (never panics).
fn current_hostname() -> Option<String> {
    gethostname::gethostname()
        .into_string()
        .ok()
        .map(|h| sanitize_hostname(&h))
        .filter(|h| !h.is_empty())
}

fn create_writer(
    path: &str,
    rotation: &str,
    max_size: usize,
    backup_count: usize,
    include_hostname: bool,
) -> Result<(NonBlocking, WorkerGuard), std::io::Error> {
    let path = std::path::Path::new(path);
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;

    let original = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("lite-server.log");

    // Optionally inject the system hostname into the basename, e.g. server.log -> server-<host>.log.
    // `file_name` is the (possibly rewritten) basename used by daily/hourly appenders; `final_path`
    // is the full path carrying that basename, used by size/none appenders.
    let (file_name, final_path) = match (include_hostname, current_hostname()) {
        (true, Some(host)) => {
            let new_name = with_hostname(original, &host);
            let new_path = parent.join(&new_name);
            (new_name, new_path)
        }
        (true, None) => {
            eprintln!("hostname unavailable, log filename left unchanged");
            (original.to_string(), path.to_path_buf())
        }
        (false, _) => (original.to_string(), path.to_path_buf()),
    };

    match rotation {
        "daily" => {
            cleanup_old_logs(parent, &file_name, backup_count);
            spawn_log_cleanup(
                parent.to_path_buf(),
                file_name.clone(),
                backup_count,
                std::time::Duration::from_secs(24 * 3600),
            );
            let appender = tracing_appender::rolling::daily(parent, &file_name);
            let (writer, guard) = tracing_appender::non_blocking(appender);
            Ok((writer, guard))
        }
        "hourly" => {
            cleanup_old_logs(parent, &file_name, backup_count);
            spawn_log_cleanup(
                parent.to_path_buf(),
                file_name.clone(),
                backup_count,
                std::time::Duration::from_secs(3600),
            );
            let appender = tracing_appender::rolling::hourly(parent, &file_name);
            let (writer, guard) = tracing_appender::non_blocking(appender);
            Ok((writer, guard))
        }
        "size" => {
            let max_bytes = max_size.checked_mul(1024 * 1024).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("max_size {} MB overflows byte conversion", max_size),
                )
            })?;
            let appender = SizeRotatingAppender::new(final_path, max_bytes, backup_count)?;
            let (writer, guard) = tracing_appender::non_blocking(appender);
            Ok((writer, guard))
        }
        _ => {
            if rotation != "none" {
                eprintln!(
                    "unknown log rotation '{}', falling back to 'none'",
                    rotation
                );
            }
            let file = OpenOptions::new().append(true).create(true).open(&final_path)?;
            let (writer, guard) = tracing_appender::non_blocking(file);
            Ok((writer, guard))
        }
    }
}

/// Remove old rotated log files exceeding backup_count.
/// Matches files like `<name>.2024-01-01` or `<name>.2024-01-01-12`.
fn cleanup_old_logs(parent: &std::path::Path, file_name: &str, backup_count: usize) {
    let Ok(entries) = std::fs::read_dir(parent) else { return };
    let prefix = format!("{}.", file_name);
    let mut matches: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with(&prefix))
                .unwrap_or(false)
        })
        .collect();

    if matches.len() <= backup_count {
        return;
    }

    matches.sort();
    let to_remove = matches.len() - backup_count;
    for path in &matches[..to_remove] {
        let _ = std::fs::remove_file(path);
    }
}

/// Spawn a detached thread that periodically removes rotated log files
/// exceeding backup_count. A plain std thread is used because logging is
/// initialized before the tokio runtime exists; the thread is detached and
/// exits with the process.
///
/// B12 (leak-gap-audit-0821): idempotent per (dir, file_name, interval) —
/// logging::init is idempotent via try_init, but this side effect ran
/// unconditionally, so every serve()/stop/serve() cycle leaked one immortal
/// thread per file output. Returns true when THIS call spawned the thread.
fn spawn_log_cleanup(
    dir: PathBuf,
    file_name: String,
    backup_count: usize,
    interval: std::time::Duration,
) -> bool {
    static SPAWNED: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashSet<(PathBuf, String, std::time::Duration)>>,
    > = std::sync::OnceLock::new();
    let spawned = SPAWNED.get_or_init(Default::default);
    if !spawned
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert((dir.clone(), file_name.clone(), interval))
    {
        return false;
    }
    let result = std::thread::Builder::new()
        .name("log-cleanup".to_string())
        .spawn(move || loop {
            std::thread::sleep(interval);
            cleanup_old_logs(&dir, &file_name, backup_count);
        });
    if let Err(e) = result {
        eprintln!("failed to spawn log cleanup thread: {}", e);
        return false;
    }
    true
}

// ===== SizeRotatingAppender =====

#[derive(Clone)]
struct SizeRotatingAppender {
    inner: Arc<Mutex<SizeRotatingAppenderInner>>,
}

struct SizeRotatingAppenderInner {
    path: PathBuf,
    max_size: usize,
    backup_count: usize,
    file: File,
}

impl SizeRotatingAppender {
    fn new(path: PathBuf, max_size: usize, backup_count: usize) -> std::io::Result<Self> {
        if max_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_size must be > 0 (0 would rotate on every write)",
            ));
        }
        if backup_count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "backup_count must be > 0 (0 would delete the log on every rotation)",
            ));
        }
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(SizeRotatingAppenderInner {
                path,
                max_size,
                backup_count,
                file,
            })),
        })
    }

    fn rotate(&self) -> std::io::Result<()> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let ext = inner.path.extension().unwrap_or_default().to_string_lossy().to_string();
        let stem = inner.path.file_stem().unwrap_or_default().to_string_lossy().to_string();
        let parent = inner.path.parent().unwrap_or(std::path::Path::new("."));

        // Shift existing backups: .N -> .N+1, delete those beyond backup_count
        for i in (1..inner.backup_count).rev() {
            let from = parent.join(format!("{}.{}.{}", stem, i, ext));
            let to = parent.join(format!("{}.{}.{}", stem, i + 1, ext));
            if from.exists() {
                let _ = rename(&from, &to);
            }
        }
        // Current -> .1
        let backup = parent.join(format!("{}.1.{}", stem, ext));
        let _ = rename(&inner.path, &backup);

        // Remove files beyond backup_count
        for i in (inner.backup_count + 1).. {
            let old = parent.join(format!("{}.{}.{}", stem, i, ext));
            if old.exists() {
                let _ = std::fs::remove_file(&old);
            } else {
                break;
            }
        }

        inner.file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&inner.path)?;
        Ok(())
    }
}

impl Write for SizeRotatingAppender {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let current_size = inner.file.metadata()?.len() as usize;
        if current_size + buf.len() > inner.max_size {
            drop(inner);
            self.rotate()?;
            inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        }
        inner.file.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// After a panic inside the mutex, SizeRotatingAppender must still work.
    /// Without poisoning recovery, this test panics on the second .lock().unwrap().
    #[test]
    fn size_rotating_appender_survives_mutex_poisoning() {
        let dir = std::env::temp_dir().join(format!("lite-server-log-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.log");

        let appender = SizeRotatingAppender::new(path.clone(), 1024 * 1024, 7).unwrap();

        // Poison the mutex: panic while holding the lock
        let inner_clone = appender.inner.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = inner_clone.lock().unwrap();
            panic!("intentional poison");
        }));
        // Mutex is now poisoned

        // Write must still succeed after poisoning
        let mut a = appender.clone();
        let result = a.write(b"after poison\n");
        assert!(result.is_ok(), "write should succeed after mutex poisoning");

        // Flush must still succeed after poisoning
        let result = a.flush();
        assert!(result.is_ok(), "flush should succeed after mutex poisoning");

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn with_hostname_inserts_host_before_extension() {
        assert_eq!(with_hostname("server.log", "node01"), "server-node01.log");
    }

    #[test]
    fn with_hostname_appends_when_no_extension() {
        assert_eq!(with_hostname("server", "node01"), "server-node01");
    }

    #[test]
    fn with_hostname_preserves_dots_in_fqdn_host() {
        assert_eq!(
            with_hostname("server.log", "node01.example.com"),
            "server-node01.example.com.log"
        );
    }

    #[test]
    fn with_hostname_returns_original_when_host_empty() {
        assert_eq!(with_hostname("server.log", ""), "server.log");
    }

    #[test]
    fn with_hostname_preserves_dashes_in_stem() {
        assert_eq!(
            with_hostname("lite-server.log", "node01"),
            "lite-server-node01.log"
        );
    }

    #[test]
    fn sanitize_hostname_replaces_unsafe_chars_with_dash() {
        assert_eq!(sanitize_hostname("node 01/ex"), "node-01-ex");
    }

    #[test]
    fn sanitize_hostname_keeps_alnum_dot_dash_underscore() {
        assert_eq!(
            sanitize_hostname("node_01.example-2"),
            "node_01.example-2"
        );
    }

    // ===== defect reproduction tests =====
    //
    // Each test asserts CORRECT behaviour that the current code violates.
    // When the underlying defect is fixed, the test turns green.

    /// B1: `max_size=0` causes every `write()` to trigger a rotation (because
    /// `current_size + buf.len() > 0` is always true).  A zero-sized limit is
    /// a misconfiguration — it should be rejected at construction time.
    #[test]
    fn test_data_max_size_zero_should_be_rejected() {
        let dir = std::env::temp_dir()
            .join(format!("lite-server-maxsize-zero-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.log");

        let result = SizeRotatingAppender::new(path, 0, 3);
        assert!(
            result.is_err(),
            "max_size=0 is a misconfiguration and must be rejected at construction"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// B2: `backup_count=0` combined with rotation renames the current log to
    /// `<stem>.1.<ext>`, then the cleanup loop (starting at `backup_count+1`)
    /// immediately deletes it — all previous log content is lost.  Zero backups
    /// is a misconfiguration and should be rejected at construction time.
    #[test]
    fn test_data_backup_count_zero_should_be_rejected() {
        let dir = std::env::temp_dir()
            .join(format!("lite-server-backup-zero-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.log");

        let result = SizeRotatingAppender::new(path, 10, 0);
        assert!(
            result.is_err(),
            "backup_count=0 is a misconfiguration and must be rejected at construction"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Overflow: `max_size` is multiplied by 1024*1024 in create_writer — a
    /// huge value must be rejected with Err, not panic (debug) or wrap
    /// (release).
    #[test]
    fn test_data_max_size_overflow_should_be_rejected() {
        let dir = std::env::temp_dir()
            .join(format!("lite-server-maxsize-overflow-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.log");

        let result = create_writer(path.to_str().unwrap(), "size", usize::MAX, 3, false);
        assert!(
            result.is_err(),
            "max_size that overflows MB->bytes conversion must be rejected"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cleanup_old_logs_removes_oldest_beyond_backup_count() {
        let dir = std::env::temp_dir()
            .join(format!("lite-server-cleanup-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for day in 1..=5 {
            std::fs::write(dir.join(format!("app.log.2024-01-0{}", day)), b"x").unwrap();
        }

        cleanup_old_logs(&dir, "app.log", 2);

        assert!(!dir.join("app.log.2024-01-01").exists());
        assert!(!dir.join("app.log.2024-01-02").exists());
        assert!(!dir.join("app.log.2024-01-03").exists());
        assert!(dir.join("app.log.2024-01-04").exists());
        assert!(dir.join("app.log.2024-01-05").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cleanup_old_logs_keeps_all_when_within_backup_count() {
        let dir = std::env::temp_dir()
            .join(format!("lite-server-cleanup-keep-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for day in 1..=3 {
            std::fs::write(dir.join(format!("app.log.2024-01-0{}", day)), b"x").unwrap();
        }

        cleanup_old_logs(&dir, "app.log", 7);

        assert!(dir.join("app.log.2024-01-01").exists());
        assert!(dir.join("app.log.2024-01-02").exists());
        assert!(dir.join("app.log.2024-01-03").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_writer_includes_hostname_when_enabled() {
        let dir = std::env::temp_dir()
            .join(format!("lite-server-hostname-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("server.log");

        let (_writer, _guard) =
            create_writer(path.to_str().unwrap(), "none", 1, 1, true).unwrap();

        match current_hostname() {
            Some(host) => {
                let expected = dir.join(format!("server-{}.log", host));
                assert!(
                    expected.exists(),
                    "expected hostname-injected log file at {}",
                    expected.display()
                );
                assert!(
                    !path.exists(),
                    "original filename must not be used when hostname is injected"
                );
            }
            None => {
                assert!(
                    path.exists(),
                    "should fall back to original filename when hostname is unavailable"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// B12 (leak-gap-audit-0821): the cleanup thread spawns at most once per
    /// (dir, file_name, interval) — repeated logging::init calls across
    /// serve()/stop/serve() cycles must not accumulate immortal threads.
    #[test]
    fn spawn_log_cleanup_is_idempotent_per_output() {
        let dir = std::env::temp_dir().join(format!(
            "lite-server-logcleanup-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let interval = std::time::Duration::from_secs(24 * 3600);
        assert!(
            spawn_log_cleanup(dir.clone(), "info.log".to_string(), 3, interval),
            "the first call spawns the thread"
        );
        assert!(
            !spawn_log_cleanup(dir.clone(), "info.log".to_string(), 3, interval),
            "a serve()-restart repeat must NOT spawn a second thread"
        );
        assert!(
            spawn_log_cleanup(dir.clone(), "error.log".to_string(), 3, interval),
            "a different file output is a different registration"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
