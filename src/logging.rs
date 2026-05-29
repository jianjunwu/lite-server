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
    verbose: bool,
) -> LogGuard {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("lite_server={},worker=info,tokio=warn,hyper=warn", level)));

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
        match create_writer(path, rotation, max_size) {
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
        match create_writer(path, rotation, max_size) {
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
            let appender = tracing_appender::rolling::daily(parent, file_name);
            let (writer, guard) = tracing_appender::non_blocking(appender);
            Ok((writer, guard))
        }
        "size" => {
            let appender = SizeRotatingAppender::new(path.to_path_buf(), max_size * 1024 * 1024)?;
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

// ===== SizeRotatingAppender =====

#[derive(Clone)]
struct SizeRotatingAppender {
    inner: Arc<Mutex<SizeRotatingAppenderInner>>,
}

struct SizeRotatingAppenderInner {
    path: PathBuf,
    max_size: usize,
    file: File,
}

impl SizeRotatingAppender {
    fn new(path: PathBuf, max_size: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(SizeRotatingAppenderInner {
                path,
                max_size,
                file,
            })),
        })
    }

    fn rotate(&self) -> std::io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let backup = inner.path.with_extension("log.1");
        let _ = rename(&inner.path, &backup);
        inner.file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&inner.path)?;
        Ok(())
    }
}

impl Write for SizeRotatingAppender {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        let current_size = inner.file.metadata()?.len() as usize;
        if current_size + buf.len() > inner.max_size {
            drop(inner);
            self.rotate()?;
            inner = self.inner.lock().unwrap();
        }
        inner.file.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.lock().unwrap().file.flush()
    }
}
