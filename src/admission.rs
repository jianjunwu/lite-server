//! P-FLOW (§4.0.9): global in-flight admission control.
//!
//! A process-wide cap on concurrently-admitted *inference* requests, used to
//! shed load before memory/CPU saturate. Health and admin traffic is exempt
//! (probes must stay reachable under load) — the exemption is enforced by the
//! callers (HTTP middleware / gRPC handler), which classify the endpoint and
//! only acquire for inference.
//!
//! `cap == 0` means unlimited (the default): `try_acquire` always succeeds and
//! the returned guard is a no-op, so behaviour is unchanged when the operator
//! has not configured a cap.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct AdmissionCounter {
    inner: Arc<Inner>,
}

struct Inner {
    current: AtomicUsize,
    cap: usize,
}

impl AdmissionCounter {
    /// `cap == 0` → unlimited (always admits).
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                current: AtomicUsize::new(0),
                cap,
            }),
        }
    }

    /// Configured cap; 0 = unlimited.
    pub fn cap(&self) -> usize {
        self.inner.cap
    }

    /// Currently-admitted count.
    pub fn current(&self) -> usize {
        self.inner.current.load(Ordering::Relaxed)
    }

    /// Try to admit one request. Returns a guard on success (drop it to
    /// release the slot) or `None` if the cap is saturated.
    pub fn try_acquire(&self) -> Option<AdmissionGuard> {
        let cap = self.inner.cap;
        if cap == 0 {
            return Some(AdmissionGuard {
                inner: None,
            });
        }
        loop {
            let c = self.inner.current.load(Ordering::Acquire);
            if c >= cap {
                return None;
            }
            if self
                .inner
                .current
                .compare_exchange_weak(c, c + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(AdmissionGuard {
                    inner: Some(self.inner.clone()),
                });
            }
        }
    }
}

/// RAII slot: dropping releases the admission slot (no-op when cap is 0).
pub struct AdmissionGuard {
    inner: Option<Arc<Inner>>,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.current.fetch_sub(1, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_zero_is_unlimited_and_does_not_count() {
        let ac = AdmissionCounter::new(0);
        let g1 = ac.try_acquire().expect("cap 0 always admits");
        let g2 = ac.try_acquire().expect("cap 0 always admits");
        // unlimited → no counting observed
        assert_eq!(ac.current(), 0);
        drop(g1);
        drop(g2);
        assert_eq!(ac.current(), 0);
    }

    #[test]
    fn admits_up_to_cap_then_rejects() {
        let ac = AdmissionCounter::new(2);
        let g1 = ac.try_acquire().expect("first admit");
        assert_eq!(ac.current(), 1);
        let g2 = ac.try_acquire().expect("second admit");
        assert_eq!(ac.current(), 2);
        assert!(ac.try_acquire().is_none(), "third admit rejected at cap");
        assert_eq!(ac.current(), 2);
        drop(g1);
        assert_eq!(ac.current(), 1);
        // slot freed → admit succeeds again
        let g3 = ac.try_acquire().expect("re-admit after release");
        assert_eq!(ac.current(), 2);
        drop(g2);
        drop(g3);
        assert_eq!(ac.current(), 0);
    }

    #[test]
    fn guard_release_is_idempotent_via_drop_only() {
        // Dropping both guards returns the counter exactly to 0 (no over-decrement).
        let ac = AdmissionCounter::new(1);
        let g = ac.try_acquire().expect("admit");
        drop(g);
        assert_eq!(ac.current(), 0);
        assert!(ac.try_acquire().is_some(), "slot reusable after clean release");
    }

    #[test]
    fn concurrent_admits_never_exceed_cap() {
        let ac = AdmissionCounter::new(8);
        let ac = std::sync::Arc::new(ac);
        let guards = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        std::thread::scope(|s| {
            for _ in 0..64 {
                let ac = ac.clone();
                let guards = guards.clone();
                s.spawn(move || {
                    while let Some(g) = ac.try_acquire() {
                        guards.lock().unwrap().push(g);
                    }
                });
            }
        });
        // Exactly `cap` slots were acquired (others rejected); the counter never
        // exceeded the cap.
        assert_eq!(ac.current(), 8);
        assert_eq!(guards.lock().unwrap().len(), 8);
    }
}
