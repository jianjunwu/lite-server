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

    // L2: reap completed hook tasks before spawning new ones — a JoinSet
    // retains every completed task's result until reaped, so without this
    // drain each firing would accumulate a JoinHandle until shutdown.
    {
        let mut set = hook_tasks.lock().unwrap_or_else(|e| e.into_inner());
        while set.try_join_next().is_some() {}
    }

    // Shell hook: fire-and-forget, but bounded (B2, leak-gap-audit-0821):
    // the same hook_http_timeout knob bounds shell hooks — a hung hook must
    // not park the JoinSet slot (and its child) forever. stdin is null so a
    // hook never blocks on the server's terminal; kill_on_drop reaps the
    // child if the task is aborted (WorkerManager::shutdown's abort_all);
    // on timeout the child is killed AND reaped. On unix the child runs in
    // its own process group so a compound command's grandchildren die with
    // it (kill_on_drop only covers the direct child — shutdown-abort of a
    // compound hook may still leave grandchildren, documented limit).
    if let Some(cmd) = shell_cmd {
        let resolved = replace_hook_vars(cmd, &vars);
        let hook_name = hook_type.to_string();
        let hook_timeout = Duration::from_secs_f32(hooks.hook_http_timeout);
        hook_tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .spawn(async move {
                #[allow(unused_mut)]
                let mut command = tokio::process::Command::new("sh");
                command
                    .arg("-c")
                    .arg(&resolved)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true);
                #[cfg(unix)]
                {
                    // tokio's Command has a native unix process_group.
                    command.process_group(0);
                }
                let mut child = match command.spawn() {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Hook '{}' command failed to execute: {}", hook_name, e);
                        return;
                    }
                };
                match tokio::time::timeout(hook_timeout, child.wait()).await {
                    Ok(Ok(status)) => {
                        if !status.success() {
                            warn!("Hook '{}' command exited with {}", hook_name, status);
                        }
                    }
                    Ok(Err(e)) => {
                        warn!("Hook '{}' command wait failed: {}", hook_name, e);
                    }
                    Err(_) => {
                        warn!(
                            "Hook '{}' command timed out after {:?}; killing",
                            hook_name, hook_timeout
                        );
                        #[cfg(unix)]
                        {
                            if let Some(pid) = child.id() {
                                // Kill the whole group: sh -c "a; b" keeps b
                                // as a grandchild that child.kill() misses.
                                unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
                            }
                        }
                        let _ = child.kill().await;
                        let _ = child.wait().await;
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
            // B15: on a client build failure (TLS backend init), fall back
            // LOUDLY — and apply the timeout per request too, so even the
            // fallback client (which carries no default timeout) stays
            // bounded instead of silently losing the one safety knob.
            let client = match reqwest::Client::builder()
                .timeout(hook_timeout)
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Hook '{}' HTTP client build failed ({}); using the \
                         fallback client with a per-request timeout",
                        hook_name, e
                    );
                    reqwest::Client::default()
                }
            };
            let result = match method.to_uppercase().as_str() {
                "GET" => client.get(&url).timeout(hook_timeout).send().await,
                _ => {
                    let b = body.unwrap_or_default();
                    client.post(&url).timeout(hook_timeout).body(b).send().await
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

    /// B2 (leak-gap-audit-0821): a hung shell hook must not park forever.
    /// The hook timeout (same knob as HTTP hooks) bounds the wait; on
    /// expiry the child is killed and reaped.
    #[cfg(unix)]
    #[tokio::test]
    async fn shell_hook_is_bounded_by_timeout() {
        let hooks = crate::config::WorkerHooksConfig {
            on_ready: Some("sleep 300".to_string()),
            hook_http_timeout: 0.2,
            ..Default::default()
        };
        let hook_tasks: HookTasks = Default::default();
        execute_hook("ready", &hooks, vec![], &hook_tasks);

        // The task must COMPLETE within a small multiple of the timeout —
        // today it parks on `sleep 300` and never finishes.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let completed = loop {
            {
                let mut set = hook_tasks.lock().unwrap_or_else(|e| e.into_inner());
                if set.try_join_next().is_some() {
                    break true;
                }
                if set.is_empty() {
                    break true; // task completed AND already reaped elsewhere
                }
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        };
        assert!(
            completed,
            "the hook task must conclude near the timeout, not park on the hung command"
        );
    }

    /// B2: aborting the hook task set (WorkerManager::shutdown's abort_all)
    /// must not orphan the hook's child process — kill_on_drop reaps it
    /// with the dropped future.
    #[cfg(unix)]
    #[tokio::test]
    async fn aborted_shell_hook_does_not_orphan_child() {
        let tag = format!("lite-server-hook-orphan-{}", std::process::id());
        let pidfile = std::env::temp_dir().join(&tag);
        let _ = std::fs::remove_file(&pidfile);
        let hooks = crate::config::WorkerHooksConfig {
            // exec keeps the recorded pid for the sleep itself.
            on_ready: Some(format!(
                "echo $$ > {}; exec sleep 300",
                pidfile.display()
            )),
            hook_http_timeout: 60.0,
            ..Default::default()
        };
        let hook_tasks: HookTasks = Default::default();
        execute_hook("ready", &hooks, vec![], &hook_tasks);

        let mut pid_contents = None;
        for _ in 0..50 {
            if let Ok(s) = std::fs::read_to_string(&pidfile) {
                pid_contents = Some(s);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let pid: i32 = pid_contents
            .expect("hook child must have recorded its pid")
            .trim()
            .parse()
            .unwrap();

        // Mirror WorkerManager::shutdown: abort every hook task.
        {
            let mut set = hook_tasks.lock().unwrap_or_else(|e| e.into_inner());
            set.abort_all();
        }
        // Give the kill a moment to land.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        if alive {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = std::fs::remove_file(&pidfile);
        assert!(
            !alive,
            "hook child pid {pid} survived abort_all: the dropped Child is never killed"
        );
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

    /// L2 regression lock: lifecycle-hook tasks are spawned into a shared
    /// `tokio::task::JoinSet` (`hook_tasks`); `execute_hook` reaps completed
    /// entries (try_join_next drain) before each spawn. This test fires a
    /// batch, waits for actual completion, then fires more (reaping) and
    /// asserts the retained count stays small.
    ///
    /// Completion-signaled, NOT wall-clock: each hook firing appends one line
    /// to a marker file and the test polls the line count (10s budget) — a
    /// fixed `sleep(200ms)` for 50 subprocess spawns flakes on a loaded
    /// machine (observed: retained 51 with zero reaped).
    #[tokio::test]
    async fn l2_completed_hook_tasks_are_reaped() {
        let marker = std::env::temp_dir().join(format!(
            "liteserver-l2-hook-{}.log",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&marker);
        let hooks = crate::config::WorkerHooksConfig {
            // Instant shell command that leaves a completion marker line.
            on_ready: Some(format!("echo . >> {}", marker.display())),
            ..Default::default()
        };
        let hook_tasks: HookTasks = Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new()));
        for _ in 0..50 {
            execute_hook("ready", &hooks, vec![], &hook_tasks);
        }
        // Wait until every spawned hook command actually RAN (marker lines) —
        // process exit observed, task completion trails by a scheduler tick.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let lines = std::fs::read_to_string(&marker)
                .map(|s| s.lines().count())
                .unwrap_or(0);
            if lines >= 50 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "50 hook commands did not complete within 10s ({lines} done)"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Fire more hooks: each spawn reaps every task the runtime has marked
        // complete. Loop until the JoinSet drains (converges once the exited
        // processes' tasks are observed complete) or the deadline expires.
        let retained = loop {
            execute_hook("ready", &hooks, vec![], &hook_tasks);
            let retained = hook_tasks.lock().unwrap_or_else(|e| e.into_inner()).len();
            if retained <= 4 || std::time::Instant::now() >= deadline {
                break retained;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let _ = std::fs::remove_file(&marker);
        assert!(
            retained <= 4,
            "L2: completed hook tasks not reaped from the JoinSet: retained \
             {retained} (expected bounded) — each hook firing accumulates a \
             JoinHandle until shutdown"
        );
    }
}
