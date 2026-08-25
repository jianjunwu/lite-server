//! Test-only tracing capture: a process-global, always-on subscriber whose
//! per-(thread, level) counters make log-level assertions deterministic
//! under cargo test's parallel scheduling.
//!
//! Why not `with_default` scoped subscribers: the callsite interest cache
//! is process-global, one value per callsite. Between a test's
//! `rebuild_interest_cache()` and its event, a parallel thread logging the
//! same callsite with the (subscriber-less) global dispatcher caches
//! `Interest::never` — the scoped thread's events then short-circuit at the
//! macro and its layer counts nothing (the
//! `test_client_errors_log_at_info_not_error` flake). An always-on GLOBAL
//! subscriber makes every interest rebuild resolve to `always` on every
//! thread, so poisoning is impossible process-wide; per-thread buckets keep
//! parallel tests' counts isolated.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::ThreadId;
use tracing::subscriber::Interest;
use tracing::Metadata;
use tracing_subscriber::prelude::*;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

/// Events emitted on one thread, tallied by level (debug/trace are not
/// distinguished — no assertion needs them so far).
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counters {
    pub error: u64,
    pub warn: u64,
    pub info: u64,
}

#[derive(Clone, Default)]
struct PerThreadLevelCounter {
    counts: Arc<Mutex<HashMap<ThreadId, Counters>>>,
}

impl<S: tracing::Subscriber> Layer<S> for PerThreadLevelCounter {
    fn register_callsite(&self, _meta: &'static Metadata<'static>) -> Interest {
        // The whole point: the process-global interest cache can never
        // resolve to NEVER, no matter which dispatcher rebuilds it.
        Interest::always()
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut map = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        let c = map.entry(std::thread::current().id()).or_default();
        match *event.metadata().level() {
            tracing::Level::ERROR => c.error += 1,
            tracing::Level::WARN => c.warn += 1,
            tracing::Level::INFO => c.info += 1,
            _ => {}
        }
    }
}

fn handle() -> Arc<Mutex<HashMap<ThreadId, Counters>>> {
    static HANDLE: OnceLock<Arc<Mutex<HashMap<ThreadId, Counters>>>> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let layer = PerThreadLevelCounter::default();
            let counts = layer.counts.clone();
            // Verified repo-wide: no other test installs a global default.
            // If that ever changes this install silently fails and every
            // capture assertion fails LOUDLY with zero counts — a
            // deterministic failure, never a flake.
            let _ = tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(layer),
            );
            counts
        })
        .clone()
}

/// Install the process-global always-on subscriber WITHOUT counting
/// anything. Scoped-dispatch tests (thread-local `set_default`, usually
/// paired with `rebuild_interest_cache`) must call this first: once the
/// global subscriber exists, every interest computation on every thread
/// resolves to `always`, so a parallel thread running on the no-op
/// default dispatcher can never poison a callsite to `never` and
/// short-circuit the scoped layer's events (the
/// `delete_version_audit_carries_request_context` flake — the module-only
/// runs lost simply because no `event_counts` test had installed the
/// global subscriber yet).
pub fn ensure_always_on_subscriber() {
    let _ = handle();
}

/// Run `f` and return the tracing events it emitted ON THIS THREAD, tallied
/// by level. Work `f` drives onto other threads (a multi-thread runtime,
/// std::thread) is attributed to those threads and invisible here — keep
/// `f` single-threaded (current-thread runtimes included).
pub fn event_counts<F: FnOnce()>(f: F) -> Counters {
    let counts = handle();
    let tid = std::thread::current().id();
    counts
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&tid);
    f();
    let result = counts
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&tid)
        .copied()
        .unwrap_or_default();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_count_events_by_level_on_the_calling_thread() {
        let c = event_counts(|| {
            tracing::info!("hello");
            tracing::error!("boom");
            tracing::debug!("not counted");
        });
        assert_eq!(c.info, 1);
        assert_eq!(c.error, 1);
        assert_eq!(c.warn, 0);
    }

    #[test]
    fn should_reset_the_bucket_between_runs() {
        let _ = event_counts(|| tracing::error!("first"));
        let c = event_counts(|| tracing::info!("second"));
        assert_eq!(c.error, 0, "the previous run's events must not leak in");
        assert_eq!(c.info, 1);
    }

    #[test]
    fn should_not_attribute_other_threads_events() {
        let c = event_counts(|| {
            std::thread::spawn(|| tracing::error!("off-thread")).join().unwrap();
            tracing::info!("on-thread");
        });
        assert_eq!(c.error, 0, "another thread's event must not be attributed");
        assert_eq!(c.info, 1);
    }
}
