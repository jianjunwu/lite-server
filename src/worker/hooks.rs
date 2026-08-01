//! Worker lifecycle hooks: fire-and-forget shell/HTTP callbacks fired on
//! worker ready/exit/error, plus registry-policy extraction from a model
//! config.

use super::HookTasks;
use crate::config::ModelConfig;
use std::process::Stdio;
use std::time::Duration;
use tracing::warn;

/// Replace $MODEL, $VERSION, $WORKER_ID, $EXIT_CODE, $REASON placeholders in a string.
fn replace_hook_vars(template: &str, vars: &[(String, String)]) -> String {
    let mut result = template.to_string();
    for (key, value) in vars {
        result = result.replace(key.as_str(), value.as_str());
    }
    result
}

/// Execute a worker lifecycle hook (shell command + optional HTTP callback).
/// Both are fire-and-forget: spawned as background tasks, never block the caller.
/// Tasks are tracked in `hook_tasks` so WorkerManager::shutdown can abort them
/// instead of leaving them dangling (L2).
/// The HTTP client is built per firing so its timeout can come from
/// `WorkerHooksConfig::hook_http_timeout` (§3) instead of a process-wide constant.
pub fn execute_hook(
    hook_type: &str,
    hooks: &crate::config::WorkerHooksConfig,
    vars: Vec<(String, String)>,
    hook_tasks: &HookTasks,
) {
    // Determine which shell command and HTTP hook to use based on hook_type
    let shell_cmd = match hook_type {
        "ready" => hooks.on_ready.as_deref(),
        "exit" => hooks.on_exit.as_deref(),
        "error" => hooks.on_error.as_deref(),
        _ => None,
    };
    let http_hook = match hook_type {
        "ready" => hooks.on_ready_http.as_ref(),
        "exit" => hooks.on_exit_http.as_ref(),
        "error" => hooks.on_error_http.as_ref(),
        _ => None,
    };

    // Skip if no hooks configured
    if shell_cmd.is_none() && http_hook.is_none() {
        return;
    }

    // Shell hook: fire-and-forget
    if let Some(cmd) = shell_cmd {
        let resolved = replace_hook_vars(cmd, &vars);
        let hook_name = hook_type.to_string();
        hook_tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .spawn(async move {
                match tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(&resolved)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await
                {
                    Ok(status) => {
                        if !status.success() {
                            warn!("Hook '{}' command exited with {}", hook_name, status);
                        }
                    }
                    Err(e) => {
                        warn!("Hook '{}' command failed to execute: {}", hook_name, e);
                    }
                }
            });
    }

    // HTTP hook: fire-and-forget
    if let Some(http) = http_hook {
        let url = replace_hook_vars(&http.url, &vars);
        let method = http.method.clone();
        let body = http.body_template.as_deref().map(|t| replace_hook_vars(t, &vars));
        let hook_name = hook_type.to_string();
        let hook_timeout = Duration::from_secs_f32(hooks.hook_http_timeout);
        hook_tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .spawn(async move {
            // Per-call client so the timeout is configurable (§3). reqwest::Client
            // is Arc internally, so building it per firing is cheap.
            let client = reqwest::Client::builder()
                .timeout(hook_timeout)
                .build()
                .unwrap_or_default();
            let result = match method.to_uppercase().as_str() {
                "GET" => client.get(&url).send().await,
                _ => {
                    let b = body.unwrap_or_default();
                    client.post(&url).body(b).send().await
                }
            };
            match result {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        warn!("Hook '{}' HTTP {} returned {}", hook_name, url, resp.status());
                    }
                }
                Err(e) => {
                    warn!("Hook '{}' HTTP {} failed: {}", hook_name, url, e);
                }
            }
        });
    }
}

/// Registry-ready policies from a model config: None when nothing is
/// configured, so the registry keeps no policy state for that version.
pub(super) fn policies_from_config(config: &ModelConfig) -> Option<crate::config::ModelPolicies> {
    if config.policies.is_empty() {
        None
    } else {
        Some(config.policies.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_replace_hook_vars() {
        let vars = vec![
            ("$MODEL".to_string(), "bert".to_string()),
            ("$VERSION".to_string(), "v1".to_string()),
            ("$WORKER_ID".to_string(), "2".to_string()),
            ("$EXIT_CODE".to_string(), "137".to_string()),
            ("$REASON".to_string(), "crash".to_string()),
        ];
        let template = "model=$MODEL version=$VERSION worker=$WORKER_ID exit=$EXIT_CODE reason=$REASON";
        let result = replace_hook_vars(template, &vars);
        assert_eq!(result, "model=bert version=v1 worker=2 exit=137 reason=crash");
    }

    #[test]
    fn test_replace_hook_vars_no_match() {
        let vars = vec![("$MODEL".to_string(), "x".to_string())];
        let result = replace_hook_vars("no placeholders here", &vars);
        assert_eq!(result, "no placeholders here");
    }

    #[test]
    fn test_replace_hook_vars_empty_template() {
        let vars = vec![("$MODEL".to_string(), "x".to_string())];
        let result = replace_hook_vars("", &vars);
        assert_eq!(result, "");
    }

    #[test]
    fn test_execute_hook_no_hooks_configured() {
        // Should not panic when no hooks are configured
        let hooks = crate::config::WorkerHooksConfig::default();
        // Bound to a variable: dropping the last Arc aborts the JoinSet's
        // tasks (unlike tokio::spawn, which detaches).
        let hook_tasks: HookTasks = Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new()));
        execute_hook(
            "ready",
            &hooks,
            vec![("$MODEL".to_string(), "test".to_string())],
            &hook_tasks,
        );
    }

    #[test]
    fn test_worker_hooks_config_default_is_empty() {
        let hooks = crate::config::WorkerHooksConfig::default();
        assert!(hooks.on_ready.is_none());
        assert!(hooks.on_exit.is_none());
        assert!(hooks.on_error.is_none());
        assert!(hooks.on_ready_http.is_none());
        assert!(hooks.on_exit_http.is_none());
        assert!(hooks.on_error_http.is_none());
    }

    #[test]
    fn test_worker_hooks_config_yaml_roundtrip() {
        let hooks = crate::config::WorkerHooksConfig {
            on_ready: Some("echo ready".to_string()),
            on_exit: Some("echo exit".to_string()),
            on_error: None,
            on_ready_http: Some(crate::config::HttpHookConfig {
                url: "http://localhost/hook".to_string(),
                method: "POST".to_string(),
                body_template: Some(r#"{"model":"$MODEL"}"#.to_string()),
            }),
            on_exit_http: None,
            on_error_http: None,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&hooks).unwrap();
        let parsed: crate::config::WorkerHooksConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.on_ready, Some("echo ready".to_string()));
        assert!(parsed.on_ready_http.is_some());
        assert_eq!(parsed.on_ready_http.as_ref().unwrap().url, "http://localhost/hook");
    }

    #[tokio::test]
    async fn test_execute_shell_hook_runs_command() {
        // Use a hook that creates a temp file to verify execution
        let tmp = std::env::temp_dir().join(format!("lite-server-hook-test-{}", std::process::id()));
        let tmp_str = tmp.to_string_lossy().to_string();
        let hooks = crate::config::WorkerHooksConfig {
            on_ready: Some(format!("touch {}", tmp_str)),
            ..Default::default()
        };
        // Bound to a variable: dropping the last Arc aborts the JoinSet's
        // tasks (unlike tokio::spawn, which detaches).
        let hook_tasks: HookTasks = Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new()));
        execute_hook(
            "ready",
            &hooks,
            vec![("$MODEL".to_string(), "test".to_string())],
            &hook_tasks,
        );

        // Wait for the background task to complete
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(tmp.exists(), "shell hook should have created the file");
        let _ = tokio::fs::remove_file(&tmp).await;
    }
}
