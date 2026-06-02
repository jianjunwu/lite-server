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

pub fn init(
    level: &str,
    info_output: Option<&str>,
    error_output: Option<&str>,
    rotation: &str,
    max_size: usize,
    backup_count: usize,
    verbose: bool,
) -> LogGuard {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("lite_server={},tokio=warn,hyper=warn", level)));

    // stdout layer (always)
    let stdout_layer = fmt::layer().with_target(true).with_thread_ids(true);

    // stderr layer (when --log-verbose)
    let stderr_layer = if verbose {
        Some(fmt::layer().with_writer(std::io::stderr).with_ansi(false).with_target(true))
    } else {
        None
    };

    let mut info_guard = None;
    let info_layer = if let Some(path) = info_output {
        match create_writer(path, rotation, max_size, backup_count) {
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
        match create_writer(path, rotation, max_size, backup_count) {
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

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(stderr_layer)
        .with(info_layer)
        .with(error_layer)
        .init();

    LogGuard {
        _info_guard: info_guard,
        _error_guard: error_guard,
    }
}

fn create_writer(
    path: &str,
    rotation: &str,
    max_size: usize,
    backup_count: usize,
) -> Result<(NonBlocking, WorkerGuard), std::io::Error> {
    let path = std::path::Path::new(path);
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;

    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("lite-server.log");

    match rotation {
        "daily" => {
            cleanup_old_logs(parent, file_name, backup_count);
            let appender = tracing_appender::rolling::daily(parent, file_name);
            let (writer, guard) = tracing_appender::non_blocking(appender);
            Ok((writer, guard))
        }
        "hourly" => {
            cleanup_old_logs(parent, file_name, backup_count);
            let appender = tracing_appender::rolling::hourly(parent, file_name);
            let (writer, guard) = tracing_appender::non_blocking(appender);
            Ok((writer, guard))
        }
        "size" => {
            let appender = SizeRotatingAppender::new(path.to_path_buf(), max_size * 1024 * 1024, backup_count)?;
            let (writer, guard) = tracing_appender::non_blocking(appender);
            Ok((writer, guard))
        }
        _ => {
            let file = OpenOptions::new().append(true).create(true).open(path)?;
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
}
