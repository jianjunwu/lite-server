use crate::error::AppError;
use crate::registry::ModelRegistry;
use crate::worker::protocol::{EndpointRequest, EndpointResponse, EndpointRoute, EndpointStartup};
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{error, info, warn};

/// Manages the custom endpoint Python subprocess.
pub struct EndpointManager {
    repo_path: PathBuf,
    registry: Arc<ModelRegistry>,
    uds_path: PathBuf,
    process: RwLock<Option<EndpointProcess>>,
    routes: RwLock<Vec<EndpointRoute>>,
}

struct EndpointProcess {
    child: Child,
}

impl EndpointManager {
    pub fn new(repo_path: PathBuf, registry: Arc<ModelRegistry>) -> Self {
        let uds_path = std::env::temp_dir()
            .join("lite-server")
            .join("endpoints.sock");
        Self {
            repo_path,
            registry,
            uds_path,
            process: RwLock::new(None),
            routes: RwLock::new(Vec::new()),
        }
    }

    pub async fn start(&self) -> Result<(), AppError> {
        self.spawn_endpoint_process().await
    }

    pub async fn routes(&self) -> Vec<EndpointRoute> {
        self.routes.read().await.clone()
    }

    pub async fn send_request(&self, request: EndpointRequest) -> Result<EndpointResponse, AppError> {
        // Retry once if connection fails (process might be restarting)
        match self.do_send_request(&request).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                warn!("Endpoint request failed, attempting restart: {}", e);
                self.restart().await?;
                self.do_send_request(&request).await
            }
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
            let port = derive_port_from_path(&path_str);
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

        let mut resp_buf = vec![0u8; resp_len];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut resp_buf)
            .await
            .map_err(|e| AppError::Transport(format!("read body from endpoint UDS: {}", e)))?;

        let response: EndpointResponse = serde_json::from_slice(&resp_buf).map_err(|e| {
            AppError::Transport(format!("json deserialize endpoint response: {}", e))
        })?;

        Ok(response)
    }

    pub async fn restart(&self) -> Result<(), AppError> {
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

    async fn spawn_endpoint_process(&self) -> Result<(), AppError> {
        // Ensure parent dir exists
        if let Some(parent) = self.uds_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        // Remove stale socket
        let _ = tokio::fs::remove_file(&self.uds_path).await;

        let python_path = WorkerManager::find_python_module_path().unwrap_or_default();
        let mut cmd = Command::new("python");
        if !python_path.is_empty() {
            let current_pythonpath = std::env::var("PYTHONPATH").unwrap_or_default();
            let new_pythonpath = if current_pythonpath.is_empty() {
                python_path
            } else {
                format!("{}:{}", current_pythonpath, python_path)
            };
            cmd.env("PYTHONPATH", new_pythonpath);
        }

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

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        // Wait for startup signal
        let mut reader = BufReader::new(stdout).lines();
        let startup_line = timeout(Duration::from_secs(30), reader.next_line())
            .await
            .map_err(|_| AppError::InferenceTimeout("endpoint startup timeout".to_string()))?
            .map_err(|e| AppError::Io(e))?
            .ok_or_else(|| AppError::WorkerCrashed("endpoint worker exited before ready".to_string()))?;

        let startup: EndpointStartup = serde_json::from_str(&startup_line)
            .map_err(|e| AppError::Internal(format!("endpoint startup JSON parse error: {}", e)))?;

        if startup.status != "ready" {
            return Err(AppError::WorkerCrashed(format!(
                "endpoint startup failed: {:?}",
                startup.message
            )));
        }

        info!("Endpoint process ready with {} routes", startup.routes.len());

        // Store routes
        {
            let mut routes = self.routes.write().await;
            *routes = startup.routes;
        }

        // Start stderr logger
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                eprintln!("[endpoints] {}", line);
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
        let loaded = self.registry.list_loaded().await;
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

#[cfg(windows)]
fn derive_port_from_path(path: &str) -> u16 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    30000 + (hasher.finish() % 35535) as u16
}
