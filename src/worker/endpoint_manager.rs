use crate::error::AppError;
use crate::registry::ModelRegistry;
use crate::worker::protocol::{EndpointRequest, EndpointResponse, EndpointRoute, EndpointStartup};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::timeout;
use tracing::{info, warn};

const ENDPOINT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Per-route policy table: axum-form route → (rate limit, pre-built CORS headers).
type RoutePolicyMap = HashMap<
    String,
    (Option<crate::worker::protocol::RateLimitPolicy>, Option<Arc<axum::http::HeaderMap>>),
>;

/// Manages the custom endpoint Python subprocess.
pub struct EndpointManager {
    repo_path: PathBuf,
    registry: Arc<ModelRegistry>,
    uds_path: PathBuf,
    process: RwLock<Option<EndpointProcess>>,
    routes: RwLock<Vec<EndpointRoute>>,
    /// Per-route policies keyed by axum-form route (e.g. "/pets/:id").
    /// The CORS value is a pre-built `Arc<HeaderMap>` (B9) — built once at
    /// ingest, Arc-shared per request.
    route_policies: RwLock<RoutePolicyMap>,
    /// Prevents concurrent restarts from multiple requests.
    /// Prevents concurrent restarts from multiple requests.
    restart_lock: Mutex<()>,
}

struct EndpointProcess {
    child: Child,
}

impl EndpointManager {
    pub fn new(repo_path: PathBuf, registry: Arc<ModelRegistry>) -> Self {
        let uds_path = std::env::temp_dir()
            .join(format!("lite-server-{}-endpoints.sock", std::process::id()));
        Self {
            repo_path,
            registry,
            uds_path,
            process: RwLock::new(None),
            routes: RwLock::new(Vec::new()),
            route_policies: RwLock::new(HashMap::new()),
            restart_lock: Mutex::new(()),
        }
    }

    /// Look up the RateLimit policy for a route (axum-form key).
    pub async fn rate_limit_policy(
        &self,
        route: &str,
    ) -> Option<crate::worker::protocol::RateLimitPolicy> {
        let policies = self.route_policies.read().await;
        policies.get(route).and_then(|(rl, _)| rl.clone())
    }

    /// Look up the pre-built CORS headers for a route (axum-form key).
    pub async fn cors_headers(
        &self,
        route: &str,
    ) -> Option<Arc<axum::http::HeaderMap>> {
        let policies = self.route_policies.read().await;
        policies.get(route).and_then(|(_, c)| c.clone())
    }

    pub async fn start(&self) -> Result<(), AppError> {
        self.spawn_endpoint_process().await
    }

    pub async fn routes(&self) -> Vec<EndpointRoute> {
        self.routes.read().await.clone()
    }

    pub async fn send_request(&self, request: EndpointRequest) -> Result<EndpointResponse, AppError> {
        let fut = async {
            // Retry once if connection fails (process might be restarting)
            match self.do_send_request(&request).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    warn!("Endpoint request failed, attempting restart: {}", e);
                    self.restart().await?;
                    self.do_send_request(&request).await
                }
            }
        };
        match timeout(ENDPOINT_REQUEST_TIMEOUT, fut).await {
            Ok(result) => result,
            Err(_) => Err(AppError::InferenceTimeout("endpoint request timeout".to_string())),
        }
    }

    async fn do_send_request(&self, request: &EndpointRequest) -> Result<EndpointResponse, AppError> {
        #[cfg(unix)]
        let mut stream = tokio::net::UnixStream::connect(&self.uds_path)
            .await
            .map_err(|e| AppError::Transport(format!("failed to connect to endpoint UDS: {}", e)))?;

        #[cfg(windows)]
        let mut stream = {
            let path_str = self.uds_path.to_string_lossy();
            let port = crate::transport::derive_port_from_path(&path_str);
            tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .map_err(|e| AppError::Transport(format!("failed to connect to endpoint: {}", e)))?
        };

        let encoded = serde_json::to_vec(request).map_err(|e| {
            AppError::Transport(format!("json serialize endpoint request: {}", e))
        })?;

        let len = encoded.len() as u32;
        let mut buf = Vec::with_capacity(4 + encoded.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&encoded);

        tokio::io::AsyncWriteExt::write_all(&mut stream, &buf)
            .await
            .map_err(|e| AppError::Transport(format!("write to endpoint UDS: {}", e)))?;

        // Read response
        let mut len_buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut len_buf)
            .await
            .map_err(|e| AppError::Transport(format!("read len from endpoint UDS: {}", e)))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
        if resp_len > MAX_FRAME_SIZE {
            return Err(AppError::FrameTooLarge);
        }

        let mut resp_buf = vec![0u8; resp_len];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut resp_buf)
            .await
            .map_err(|e| AppError::Transport(format!("read body from endpoint UDS: {}", e)))?;

        let response: EndpointResponse = serde_json::from_slice(&resp_buf).map_err(|e| {
            AppError::Transport(format!("json deserialize endpoint response: {}", e))
        })?;

        Ok(response)
    }

    /// Send a request and return a channel that yields streaming chunks.
    ///
    /// The Python side sends: header frame (stream:true) -> chunk frames -> done frame.
    /// This method reads the header, then spawns a task to forward chunks via channel.
    pub async fn send_stream_request(
        &self,
        request: EndpointRequest,
    ) -> Result<mpsc::Receiver<serde_json::Value>, AppError> {
        #[cfg(unix)]
        let mut stream = tokio::net::UnixStream::connect(&self.uds_path)
            .await
            .map_err(|e| AppError::Transport(format!("failed to connect to endpoint UDS: {}", e)))?;

        #[cfg(windows)]
        let mut stream = {
            let path_str = self.uds_path.to_string_lossy();
            let port = crate::transport::derive_port_from_path(&path_str);
            tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .map_err(|e| AppError::Transport(format!("failed to connect to endpoint: {}", e)))?
        };

        // Send request
        let encoded = serde_json::to_vec(&request).map_err(|e| {
            AppError::Transport(format!("json serialize endpoint request: {}", e))
        })?;
        let len = encoded.len() as u32;
        let mut buf = Vec::with_capacity(4 + encoded.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&encoded);
        tokio::io::AsyncWriteExt::write_all(&mut stream, &buf)
            .await
            .map_err(|e| AppError::Transport(format!("write to endpoint UDS: {}", e)))?;

        // Read stream header (first frame)
        let header = Self::read_frame(&mut stream).await?;
        let header_val: serde_json::Value = serde_json::from_slice(&header)
            .map_err(|e| AppError::Transport(format!("parse stream header: {}", e)))?;

        if !header_val.get("stream").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(AppError::Transport("expected stream header".to_string()));
        }

        let (tx, rx) = mpsc::channel(64);

        // Spawn task to read chunks and forward via channel
        tokio::spawn(async move {
            const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
            loop {
                match Self::read_frame(&mut stream).await {
                    Ok(frame_data) => {
                        if frame_data.len() > MAX_FRAME_SIZE {
                            let _ = tx.send(json!({"error": "chunk too large"})).await;
                            break;
                        }
                        let chunk: serde_json::Value = match serde_json::from_slice(&frame_data) {
                            Ok(v) => v,
                            Err(e) => {
                                let _ = tx.send(json!({"error": format!("parse chunk: {}", e)})).await;
                                break;
                            }
                        };
                        // Check for done marker
                        if chunk.get("type").and_then(|v| v.as_str()) == Some("done") {
                            break;
                        }
                        if tx.send(chunk).await.is_err() {
                            break; // receiver dropped
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(json!({"error": e.to_string()})).await;
                        break;
                    }
                }
            }
        });

        Ok(rx)
    }

    /// Read a single length-prefixed frame from the stream.
    async fn read_frame<S>(stream: &mut S) -> Result<Vec<u8>, AppError>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

        let mut len_buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(stream, &mut len_buf)
            .await
            .map_err(|e| AppError::Transport(format!("read frame len: {}", e)))?;
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len > MAX_FRAME_SIZE {
            return Err(AppError::FrameTooLarge);
        }
        let mut buf = vec![0u8; frame_len];
        tokio::io::AsyncReadExt::read_exact(stream, &mut buf)
            .await
            .map_err(|e| AppError::Transport(format!("read frame body: {}", e)))?;
        Ok(buf)
    }

    pub async fn restart(&self) -> Result<(), AppError> {
        let _guard = self.restart_lock.lock().await;
        info!("Restarting endpoint process");
        self.kill().await;
        self.spawn_endpoint_process().await
    }

    pub async fn shutdown(&self) {
        self.kill().await;
    }

    async fn kill(&self) {
        let mut proc = self.process.write().await;
        if let Some(mut p) = proc.take() {
            let _ = p.child.kill().await;
            let _ = p.child.wait().await;
        }
        #[cfg(unix)]
        let _ = tokio::fs::remove_file(&self.uds_path).await;
    }

    #[cfg(test)]
    async fn child_pid(&self) -> Option<u32> {
        self.process.read().await.as_ref().and_then(|p| p.child.id())
    }

    #[cfg(test)]
    fn set_uds_path(&mut self, path: PathBuf) {
        self.uds_path = path;
    }

    async fn spawn_endpoint_process(&self) -> Result<(), AppError> {
        // Ensure parent dir exists
        if let Some(parent) = self.uds_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        // Remove stale socket
        let _ = tokio::fs::remove_file(&self.uds_path).await;

        let python_path = WorkerManager::find_python_module_path().unwrap_or_default();
        // kill_on_drop(true) via new_worker_command: orphan safety net for
        // startup-failure early returns and panic/runtime-drop paths that
        // never reach kill().
        let mut cmd = super::new_worker_command(&python_path);

        let mut child = cmd
            .arg("-m")
            .arg("lite_server.worker.endpoints")
            .arg("--repo-path")
            .arg(&self.repo_path)
            .arg("--uds-path")
            .arg(&self.uds_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| AppError::Python(format!("failed to spawn endpoint worker: {}", e)))?;

        let stdout = child.stdout.take()
            .ok_or_else(|| AppError::Internal("endpoint stdout not piped".to_string()))?;
        let stderr = child.stderr.take()
            .ok_or_else(|| AppError::Internal("endpoint stderr not piped".to_string()))?;

        // Wait for startup signal
        let mut reader = BufReader::new(stdout);
        let mut startup_line = String::new();
        let n = timeout(Duration::from_secs(30), reader.read_line(&mut startup_line))
            .await
            .map_err(|_| AppError::InferenceTimeout("endpoint startup timeout".to_string()))?
            .map_err(AppError::Io)?;
        if n == 0 {
            return Err(AppError::WorkerCrashed("endpoint worker exited before ready".to_string()));
        }
        let stdout = reader.into_inner();

        let startup: EndpointStartup = serde_json::from_str(startup_line.trim())
            .map_err(|e| AppError::Internal(format!("endpoint startup JSON parse error: {}", e)))?;

        if startup.status != "ready" {
            return Err(AppError::WorkerCrashed(format!(
                "endpoint startup failed: {:?}",
                startup.message
            )));
        }

        // Protocol version negotiation
        let proto_ver = if startup.protocol_version.is_empty() {
            crate::worker::protocol::PROTOCOL_VERSION_V0.to_string()
        } else {
            startup.protocol_version.clone()
        };
        if !crate::worker::protocol::SUPPORTED_PROTOCOL_VERSIONS.contains(&proto_ver.as_str()) {
            return Err(AppError::Config(format!(
                "endpoint protocol version '{}' not supported",
                proto_ver
            )));
        }
        info!(
            "Endpoint process ready with {} routes (protocol: {})",
            startup.routes.len(),
            proto_ver
        );

        // Store routes + per-route policies (axum-form keys)
        {
            let mut routes = self.routes.write().await;
            let mut policies = self.route_policies.write().await;
            policies.clear();
            for ep in &startup.routes {
                let axum_route = crate::worker::protocol::convert_path_params(&ep.route);
                let cors_headers = ep.cors.as_ref().map(|c| Arc::new(c.header_map()));
                policies.insert(
                    axum_route,
                    (ep.rate_limit.clone(), cors_headers),
                );
            }
            *routes = startup.routes;
        }

        // Drain stdout so the endpoint worker does not get SIGPIPE/BrokenPipeError
        tokio::spawn(async move {
            let mut discard = [0u8; 1024];
            let mut stdout = stdout;
            loop {
                match stdout.read(&mut discard).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        // Start stderr logger
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut buf = Vec::with_capacity(1024);
            loop {
                buf.clear();
                match reader.read_until(b'\n', &mut buf).await {
                    Ok(0) => {
                        tracing::debug!("Endpoint stderr EOF");
                        break;
                    }
                    Ok(_) => {
                        while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                            buf.pop();
                        }
                        let line = String::from_utf8_lossy(&buf);
                        eprintln!("[endpoints] {}", line);
                    }
                    Err(e) => {
                        tracing::error!("Endpoint stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        {
            let mut proc = self.process.write().await;
            *proc = Some(EndpointProcess { child });
        }

        Ok(())
    }

    /// Build server snapshot from current registry state.
    pub async fn build_snapshot(&self) -> crate::worker::protocol::ServerSnapshot {
        let loaded = self.registry.list_loaded();
        let models: Vec<serde_json::Value> = loaded
            .into_iter()
            .map(|(name, version, _mv)| {
                json!({"name": name, "version": version})
            })
            .collect();

        crate::worker::protocol::ServerSnapshot {
            loaded_models: models,
            config: json!({}),
        }
    }
}

// Reuse the module path finder from WorkerManager
use super::WorkerManager;

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: dropping the manager without shutdown() must not orphan the
    /// Python endpoint process. The kill_on_drop safety net on the spawn
    /// command is the last resort for panic / runtime-drop / test-exit paths
    /// that never run graceful shutdown (observed as PPID=1
    /// `python -m lite_server.worker.endpoints` orphans).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_endpoint_manager_drop_leaves_no_orphan() {
        let repo = std::env::temp_dir()
            .join(format!("lite-server-ep-drop-test-{}", std::process::id()));
        std::fs::create_dir_all(&repo).unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let mut manager = EndpointManager::new(repo.clone(), registry);
        manager.set_uds_path(std::env::temp_dir().join(format!(
            "lite-server-{}-ep-drop-test.sock",
            std::process::id()
        )));
        manager.start().await.unwrap();
        let pid = manager
            .child_pid()
            .await
            .expect("endpoint child should be running") as i32;

        drop(manager);

        // kill_on_drop sends SIGKILL on drop and tokio reaps in the
        // background; poll briefly for the pid to disappear.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut alive = true;
        while std::time::Instant::now() < deadline {
            if unsafe { libc::kill(pid, 0) } != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !alive,
            "dropped endpoint process {} should be killed via kill_on_drop",
            pid
        );

        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Graceful shutdown must reap the endpoint process before returning —
    /// kill() is a synchronous kill+wait, so no process may outlive it.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_shutdown_leaves_no_orphan_endpoint_process() {
        let repo = std::env::temp_dir()
            .join(format!("lite-server-ep-shutdown-test-{}", std::process::id()));
        std::fs::create_dir_all(&repo).unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let mut manager = EndpointManager::new(repo.clone(), registry);
        manager.set_uds_path(std::env::temp_dir().join(format!(
            "lite-server-{}-ep-shutdown-test.sock",
            std::process::id()
        )));
        manager.start().await.unwrap();
        let pid = manager
            .child_pid()
            .await
            .expect("endpoint child should be running") as i32;

        manager.shutdown().await;

        let alive = unsafe { libc::kill(pid, 0) } == 0;
        assert!(
            !alive,
            "endpoint pid {} orphaned: still alive after shutdown returned",
            pid
        );

        let _ = std::fs::remove_dir_all(&repo);
    }
}
