//! Callback trait and runner for lite-server lifecycle hooks.
//!
//! Model operators can implement the ``Callback`` trait and register
//! instances declaratively (in ``config.yaml``) or programmatically
//! (via the Rust API).  The ``CallbackRunner`` dispatches events to
//! all registered callbacks with exception isolation.
//!
//! ## Event types
//!
//! | Event               | When                                          |
//! |---------------------|-----------------------------------------------|
//! | ``ServerStart``     | Server begins listening                       |
//! | ``ServerEnd``       | Server initiates graceful shutdown            |
//! | ``ModelLoad``       | A model version finishes loading              |
//! | ``ModelUnload``     | A model version is about to be unloaded       |
//! | ``ModelReload``     | A model version is hot-reloaded               |
//! | ``InferenceRequest``| An inference request arrives (before queue).   |
//! |                     | Streaming fires once the worker stream opens   |
//! |                     | successfully (streaming bypasses the queue).   |
//! | ``InferenceResponse``| An inference response is sent to the client.  |
//! |                     | Streaming fires on the terminal Done/Error     |
//! |                     | frame; cancel / disconnect / idle-truncate do  |
//! |                     | not fire (no terminal frame reached).          |

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

// ---------------------------------------------------------------------------
// Context types
// ---------------------------------------------------------------------------

/// Protocol of the incoming request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Http,
    Grpc,
    Sse,
    WebSocket,
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Http => write!(f, "http"),
            Protocol::Grpc => write!(f, "grpc"),
            Protocol::Sse => write!(f, "sse"),
            Protocol::WebSocket => write!(f, "ws"),
        }
    }
}

/// Context passed to ``ServerStart`` / ``ServerEnd`` callbacks.
#[derive(Debug, Clone)]
pub struct ServerContext {
    pub http_port: u16,
    pub grpc_port: u16,
    pub metrics_port: u16,
}

/// Context passed to ``ModelLoad`` / ``ModelUnload`` / ``ModelReload`` callbacks.
#[derive(Debug, Clone)]
pub struct ModelLifecycleContext {
    pub model_name: String,
    pub version: String,
    pub device: Option<String>,
}

/// Context passed to ``InferenceRequest`` / ``InferenceResponse`` callbacks.
#[derive(Debug, Clone)]
pub struct InferenceContext {
    pub model_name: String,
    pub version: String,
    pub route: String,
    pub protocol: Protocol,
    pub request_id: String,
    pub client_ip: String,
    /// Elapsed wall-clock time in microseconds (only set for response).
    pub elapsed_us: Option<u64>,
}

// ---------------------------------------------------------------------------
// Callback trait
// ---------------------------------------------------------------------------

/// Trait for server lifecycle callbacks.
///
/// Every method has a default no-op implementation — override only
/// the events you care about.  All hooks are async and receive a
/// shared reference to the runner (allowing callbacks to inspect or
/// trigger other callbacks if needed).
#[async_trait::async_trait]
pub trait Callback: Send + Sync + 'static {
    /// Called when the server starts listening.
    async fn on_server_start(&self, _ctx: &ServerContext) {}

    /// Called when the server begins graceful shutdown.
    async fn on_server_end(&self, _ctx: &ServerContext) {}

    /// Called after a model version finishes loading.
    async fn on_model_load(&self, _ctx: &ModelLifecycleContext) {}

    /// Called before a model version is unloaded.
    async fn on_model_unload(&self, _ctx: &ModelLifecycleContext) {}

    /// Called when a model version is hot-reloaded.
    async fn on_model_reload(&self, _ctx: &ModelLifecycleContext) {}

    /// Called when a model version becomes the active version (§4.2).
    async fn on_model_activate(&self, _ctx: &ModelLifecycleContext) {}

    /// Called when an inference request arrives (before queueing).
    async fn on_inference_request(&self, _ctx: &InferenceContext) {}

    /// Called when an inference response is sent to the client.
    async fn on_inference_response(&self, _ctx: &InferenceContext) {}
}

// ---------------------------------------------------------------------------
// CallbackRunner
// ---------------------------------------------------------------------------

/// Manages a collection of ``Callback`` implementations and dispatches
/// events to all of them with exception isolation.
pub struct CallbackRunner {
    callbacks: RwLock<Vec<Arc<dyn Callback>>>,
}

impl CallbackRunner {
    pub fn new() -> Self {
        Self {
            callbacks: RwLock::new(Vec::new()),
        }
    }

    /// Register a callback.
    pub async fn register(&self, cb: Arc<dyn Callback>) {
        self.callbacks.write().await.push(cb);
    }

    /// Return the number of registered callbacks.
    pub async fn len(&self) -> usize {
        self.callbacks.read().await.len()
    }

    /// Return true if no callbacks are registered.
    pub async fn is_empty(&self) -> bool {
        self.callbacks.read().await.is_empty()
    }

    /// Non-async fast path for the common (empty-runner) case: returns true when
    /// no callbacks are registered, using a non-blocking lock attempt. Under lock
    /// contention it conservatively returns false (falling back to a spawn),
    /// so it is safe to call on every inference request/response without an await.
    pub fn try_is_empty(&self) -> bool {
        self.callbacks
            .try_read()
            .map(|g| g.is_empty())
            .unwrap_or(false)
    }

    // ---- Trigger helpers ----

    /// Fire an event on all registered callbacks sequentially in the current task.
    /// Uses ``catch_unwind`` for panic isolation — a failing callback does not
    /// prevent other callbacks from executing.
    async fn trigger_sequential(&self, event: &str, f: impl Fn(Arc<dyn Callback>) -> futures::future::BoxFuture<'static, ()>) {
        use futures::FutureExt;
        let cbs = self.callbacks.read().await;
        for (i, cb) in cbs.iter().enumerate() {
            let fut = f(Arc::clone(cb));
            let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;
            if let Err(e) = result {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                warn!("Callback[{}] panicked during {}: {}", i, event, msg);
            }
        }
    }

    // ---- Public trigger API ----

    pub async fn on_server_start(&self, ctx: &ServerContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_server_start", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_server_start(&ctx).await })
        }).await;
    }

    pub async fn on_server_end(&self, ctx: &ServerContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_server_end", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_server_end(&ctx).await })
        }).await;
    }

    pub async fn on_model_load(&self, ctx: &ModelLifecycleContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_model_load", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_model_load(&ctx).await })
        }).await;
    }

    pub async fn on_model_unload(&self, ctx: &ModelLifecycleContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_model_unload", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_model_unload(&ctx).await })
        }).await;
    }

    pub async fn on_model_reload(&self, ctx: &ModelLifecycleContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_model_reload", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_model_reload(&ctx).await })
        }).await;
    }

    pub async fn on_model_activate(&self, ctx: &ModelLifecycleContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_model_activate", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_model_activate(&ctx).await })
        }).await;
    }

    pub async fn on_inference_request(&self, ctx: &InferenceContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_inference_request", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_inference_request(&ctx).await })
        }).await;
    }

    pub async fn on_inference_response(&self, ctx: &InferenceContext) {
        let ctx = ctx.clone();
        self.trigger_sequential("on_inference_response", move |cb| {
            let ctx = ctx.clone();
            Box::pin(async move { cb.on_inference_response(&ctx).await })
        }).await;
    }
}

impl Default for CallbackRunner {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Streaming fire helpers (task A+D parity: gRPC stream/decoupled/bidi + HTTP
// SSE/WS now trigger the inference callbacks like unary does).
// ---------------------------------------------------------------------------

/// Fire `InferenceRequest` off-thread. Used by streaming paths where the
/// request callback fires once the worker stream opens successfully (streaming
/// bypasses the queue, so open-success is the trigger). Spawns a task so the
/// caller (handler / forwarder) is never blocked by callback dispatch.
pub fn fire_inference_request(runner: &Arc<CallbackRunner>, ctx: &InferenceContext) {
    if runner.try_is_empty() {
        return;
    }
    let runner = Arc::clone(runner);
    let ctx = ctx.clone();
    tokio::spawn(async move {
        runner.on_inference_request(&ctx).await;
    });
}

/// Fire `InferenceResponse` off-thread on a stream's terminal frame (Done or
/// Error). `start` is the stream's reference Instant; `elapsed_us` is the wall
/// clock up to that moment. Cancel / disconnect / idle-truncate do NOT fire
/// (no terminal frame reached).
pub fn fire_inference_response(
    runner: &Arc<CallbackRunner>,
    ctx: &InferenceContext,
    start: std::time::Instant,
) {
    if runner.try_is_empty() {
        return;
    }
    let elapsed_us = Some((start.elapsed().as_secs_f64() * 1_000_000.0) as u64);
    let resp_ctx = InferenceContext {
        elapsed_us,
        ..ctx.clone()
    };
    let runner = Arc::clone(runner);
    tokio::spawn(async move {
        runner.on_inference_response(&resp_ctx).await;
    });
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingCallback {
        start_count: AtomicUsize,
        end_count: AtomicUsize,
        load_count: AtomicUsize,
    }

    impl CountingCallback {
        fn new() -> Self {
            Self {
                start_count: AtomicUsize::new(0),
                end_count: AtomicUsize::new(0),
                load_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl Callback for CountingCallback {
        async fn on_server_start(&self, _ctx: &ServerContext) {
            self.start_count.fetch_add(1, Ordering::Relaxed);
        }

        async fn on_server_end(&self, _ctx: &ServerContext) {
            self.end_count.fetch_add(1, Ordering::Relaxed);
        }

        async fn on_model_load(&self, _ctx: &ModelLifecycleContext) {
            self.load_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn test_empty_runner_does_nothing() {
        let runner = CallbackRunner::new();
        assert!(runner.is_empty().await);
        // These should not panic
        runner.on_server_start(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
        runner.on_server_end(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
    }

    #[tokio::test]
    async fn test_single_callback_receives_events() {
        let cb = Arc::new(CountingCallback::new());
        let runner = CallbackRunner::new();
        runner.register(cb.clone()).await;
        assert_eq!(runner.len().await, 1);

        runner.on_server_start(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
        assert_eq!(cb.start_count.load(Ordering::Relaxed), 1);

        runner.on_server_end(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
        assert_eq!(cb.end_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_model_lifecycle_callbacks() {
        let cb = Arc::new(CountingCallback::new());
        let runner = CallbackRunner::new();
        runner.register(cb.clone()).await;

        let ctx = ModelLifecycleContext {
            model_name: "test-model".into(),
            version: "1".into(),
            device: Some("cuda:0".into()),
        };
        runner.on_model_load(&ctx).await;
        assert_eq!(cb.load_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_model_activate_callback() {
        #[derive(Default)]
        struct ActivateCallback {
            activations: AtomicUsize,
            last: std::sync::Mutex<Option<(String, String)>>,
        }
        #[async_trait::async_trait]
        impl Callback for ActivateCallback {
            async fn on_model_activate(&self, ctx: &ModelLifecycleContext) {
                self.activations.fetch_add(1, Ordering::Relaxed);
                *self.last.lock().unwrap() =
                    Some((ctx.model_name.clone(), ctx.version.clone()));
            }
        }

        let cb = Arc::new(ActivateCallback::default());
        let runner = CallbackRunner::new();
        runner.register(cb.clone()).await;

        runner.on_model_activate(&ModelLifecycleContext {
            model_name: "m".into(),
            version: "2".into(),
            device: None,
        }).await;
        assert_eq!(cb.activations.load(Ordering::Relaxed), 1);
        assert_eq!(
            cb.last.lock().unwrap().clone(),
            Some(("m".to_string(), "2".to_string()))
        );
    }

    #[tokio::test]
    async fn test_exception_isolation() {
        struct PanicCallback;
        #[async_trait::async_trait]
        impl Callback for PanicCallback {
            async fn on_server_start(&self, _ctx: &ServerContext) {
                panic!("intentional panic in callback");
            }
        }

        let good = Arc::new(CountingCallback::new());
        let bad = Arc::new(PanicCallback);
        let runner = CallbackRunner::new();
        runner.register(bad).await;
        runner.register(good.clone()).await;

        // This should not panic; the good callback still runs
        runner.on_server_start(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
        assert_eq!(good.start_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_multiple_callbacks_all_receive_event() {
        let cb1 = Arc::new(CountingCallback::new());
        let cb2 = Arc::new(CountingCallback::new());
        let runner = CallbackRunner::new();
        runner.register(cb1.clone()).await;
        runner.register(cb2.clone()).await;

        runner.on_model_load(&ModelLifecycleContext {
            model_name: "m".into(),
            version: "v1".into(),
            device: None,
        }).await;

        assert_eq!(cb1.load_count.load(Ordering::Relaxed), 1);
        assert_eq!(cb2.load_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_inference_callbacks() {
        struct TrackCb {
            req_count: AtomicUsize,
            resp_count: AtomicUsize,
        }
        #[async_trait::async_trait]
        impl Callback for TrackCb {
            async fn on_inference_request(&self, _ctx: &InferenceContext) {
                self.req_count.fetch_add(1, Ordering::Relaxed);
            }
            async fn on_inference_response(&self, _ctx: &InferenceContext) {
                self.resp_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        let cb = Arc::new(TrackCb { req_count: AtomicUsize::new(0), resp_count: AtomicUsize::new(0) });
        let runner = CallbackRunner::new();
        runner.register(cb.clone()).await;

        let ctx = InferenceContext {
            model_name: "m".into(),
            version: "v1".into(),
            route: "/predict".into(),
            protocol: Protocol::Http,
            request_id: "req-1".into(),
            client_ip: "127.0.0.1".into(),
            elapsed_us: None,
        };
        runner.on_inference_request(&ctx).await;
        assert_eq!(cb.req_count.load(Ordering::Relaxed), 1);

        let resp_ctx = InferenceContext { elapsed_us: Some(1500), ..ctx };
        runner.on_inference_response(&resp_ctx).await;
        assert_eq!(cb.resp_count.load(Ordering::Relaxed), 1);
    }

    // ---- trigger_sequential tests ----

    #[tokio::test]
    async fn test_sequential_runs_in_current_task() {
        use std::sync::Mutex;

        struct TaskIdCallback {
            task_id: Mutex<Option<tokio::task::Id>>,
        }
        #[async_trait::async_trait]
        impl Callback for TaskIdCallback {
            async fn on_server_start(&self, _ctx: &ServerContext) {
                *self.task_id.lock().unwrap() = Some(tokio::task::id());
            }
        }

        let cb = Arc::new(TaskIdCallback { task_id: Mutex::new(None) });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;

        // Spawn a task so tokio::task::id() is available, then verify
        // the callback runs in the same task (not a sub-spawn).
        let handle = tokio::spawn({
            let runner = Arc::clone(&runner);
            async move {
                let caller_id = tokio::task::id();
                runner.on_server_start(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
                (caller_id, *cb.task_id.lock().unwrap())
            }
        });
        let (caller_id, cb_id) = handle.await.unwrap();
        assert_eq!(cb_id, Some(caller_id));
    }

    #[tokio::test]
    async fn test_sequential_panic_isolation() {
        struct PanicCallback;
        #[async_trait::async_trait]
        impl Callback for PanicCallback {
            async fn on_server_start(&self, _ctx: &ServerContext) {
                panic!("intentional panic");
            }
        }

        let good = Arc::new(CountingCallback::new());
        let bad = Arc::new(PanicCallback);
        let runner = CallbackRunner::new();
        runner.register(bad).await;
        runner.register(good.clone()).await;

        // trigger_sequential should catch the panic; good still runs
        runner.on_server_start(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
        assert_eq!(good.start_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_sequential_multiple_callbacks() {
        let cb1 = Arc::new(CountingCallback::new());
        let cb2 = Arc::new(CountingCallback::new());
        let runner = CallbackRunner::new();
        runner.register(cb1.clone()).await;
        runner.register(cb2.clone()).await;

        runner.on_server_start(&ServerContext { http_port: 8080, grpc_port: 9090, metrics_port: 9091 }).await;
        assert_eq!(cb1.start_count.load(Ordering::Relaxed), 1);
        assert_eq!(cb2.start_count.load(Ordering::Relaxed), 1);
    }
}
