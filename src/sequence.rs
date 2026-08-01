//! P8-1 `sequence_id` cross-request worker affinity.
//!
//! [`SequenceRegistry`] remembers which worker last served a `sequence_id` for a
//! given `(model, version)` so subsequent same-`sequence_id` requests can be
//! biased onto it (sticky routing for KV-cache / decoder-state reuse). It is a
//! best-effort, **per-process** scheduling hint — never an isolation boundary
//! (isolation stays `access_control` + worker model boundaries) — and the
//! caller always falls back to normal selection when the mapped worker is gone
//! or ejected, so availability never depends on it.
//!
//! Entries are bounded by a TTL (`server.sequence_ttl_secs`) and an approximate
//! capacity cap (`server.max_sequences`); a 60s background sweep reaps expired
//! entries (see `LiteServer::run`).

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct SequenceRegistry {
    map: DashMap<Arc<str>, SequenceEntry>,
    ttl: Duration,
    max_entries: usize,
}

#[derive(Clone)]
struct SequenceEntry {
    model: Arc<str>,
    version: Arc<str>,
    worker_idx: usize,
    last_used: Instant,
}

impl SequenceRegistry {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            map: DashMap::new(),
            ttl,
            max_entries: max_entries.max(1),
        }
    }

    /// Record (or refresh) the worker that served `sequence_id` for
    /// `(model, version)`. Enforces the capacity cap on insert by evicting the
    /// least-recently-used entry.
    pub fn record(&self, sequence_id: &str, model: &str, version: &str, worker_idx: usize) {
        let now = Instant::now();
        if let Some(mut e) = self.map.get_mut(sequence_id) {
            e.model = model.into();
            e.version = version.into();
            e.worker_idx = worker_idx;
            e.last_used = now;
            return;
        }
        if self.map.len() >= self.max_entries {
            self.evict_oldest();
        }
        self.map.insert(
            sequence_id.into(),
            SequenceEntry {
                model: model.into(),
                version: version.into(),
                worker_idx,
                last_used: now,
            },
        );
    }

    /// Look up the mapped worker for `sequence_id` scoped to `(model, version)`.
    /// Returns `None` when absent, TTL-expired, or when the stored
    /// `(model, version)` does not match (a reused `sequence_id` for a different
    /// model is *not* given the other model's affinity). A hit refreshes
    /// `last_used`.
    pub fn lookup(&self, sequence_id: &str, model: &str, version: &str) -> Option<usize> {
        let expired = {
            let e = self.map.get(sequence_id)?;
            if e.last_used.elapsed() > self.ttl {
                true
            } else if e.model.as_ref() != model || e.version.as_ref() != version {
                return None;
            } else {
                // in-bounds hit; refresh via a write guard below
                false
            }
        };
        if expired {
            self.map.remove(sequence_id);
            return None;
        }
        let mut e = self.map.get_mut(sequence_id)?;
        e.last_used = Instant::now();
        Some(e.worker_idx)
    }

    /// Background sweep: drop TTL-expired entries, then trim to `max_entries`
    /// by oldest `last_used`. Returns the number removed.
    pub fn cleanup(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        self.map.retain(|_, e| {
            let keep = now.duration_since(e.last_used) < self.ttl;
            if !keep {
                removed += 1;
            }
            keep
        });
        // Capacity trim: collect and remove the oldest surplus entries in one
        // pass (O(n log n)) rather than repeated linear scans.
        let surplus = self.map.len().saturating_sub(self.max_entries);
        if surplus > 0 {
            let mut entries: Vec<(Arc<str>, Instant)> = self
                .map
                .iter()
                .map(|e| (e.key().clone(), e.last_used))
                .collect();
            entries.sort_unstable_by_key(|(_, t)| *t);
            for (k, _) in entries.into_iter().take(surplus) {
                self.map.remove(&k);
                removed += 1;
            }
        }
        removed
    }

    fn evict_oldest(&self) {
        let mut oldest_key: Option<Arc<str>> = None;
        let mut oldest_time = Instant::now();
        for e in self.map.iter() {
            if e.last_used <= oldest_time {
                oldest_time = e.last_used;
                oldest_key = Some(e.key().clone());
            }
        }
        if let Some(k) = oldest_key {
            self.map.remove(&k);
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn miss_then_hit_then_refresh() {
        let reg = SequenceRegistry::new(Duration::from_secs(3600), 1024);
        assert_eq!(reg.lookup("s1", "m", "v1"), None);
        reg.record("s1", "m", "v1", 2);
        assert_eq!(reg.lookup("s1", "m", "v1"), Some(2));
        // update to a different worker on re-record
        reg.record("s1", "m", "v1", 0);
        assert_eq!(reg.lookup("s1", "m", "v1"), Some(0));
    }

    #[test]
    fn model_version_mismatch_is_a_miss() {
        let reg = SequenceRegistry::new(Duration::from_secs(3600), 1024);
        reg.record("s1", "modelA", "v1", 1);
        // same seq, different model -> no affinity leak across models
        assert_eq!(reg.lookup("s1", "modelB", "v1"), None);
        assert_eq!(reg.lookup("s1", "modelA", "v2"), None);
        // original mapping still intact for its own (model, version)
        assert_eq!(reg.lookup("s1", "modelA", "v1"), Some(1));
    }

    #[test]
    fn ttl_expiry_drops_entry() {
        let reg = SequenceRegistry::new(Duration::from_millis(1), 1024);
        reg.record("s1", "m", "v1", 0);
        assert_eq!(reg.lookup("s1", "m", "v1"), Some(0));
        std::thread::sleep(Duration::from_millis(8));
        assert_eq!(reg.lookup("s1", "m", "v1"), None);
        assert!(reg.is_empty());
    }

    #[test]
    fn cleanup_reaps_expired_entries() {
        let reg = SequenceRegistry::new(Duration::from_millis(1), 1024);
        reg.record("s1", "m", "v1", 0);
        reg.record("s2", "m", "v1", 1);
        std::thread::sleep(Duration::from_millis(8));
        assert_eq!(reg.cleanup(), 2);
        assert!(reg.is_empty());
    }

    #[test]
    fn cleanup_keeps_valid_entries_under_cap() {
        // record() already enforces the cap at insert time; cleanup is a
        // safety net for concurrent-insert transient overflow. Steady state:
        // at-or-under cap with fresh entries => cleanup is a no-op.
        let reg = SequenceRegistry::new(Duration::from_secs(3600), 2);
        reg.record("a", "m", "v1", 0);
        reg.record("b", "m", "v1", 1);
        assert_eq!(reg.cleanup(), 0);
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.lookup("a", "m", "v1"), Some(0));
        assert_eq!(reg.lookup("b", "m", "v1"), Some(1));
    }

    #[test]
    fn record_enforces_cap_on_insert() {
        let reg = SequenceRegistry::new(Duration::from_secs(3600), 2);
        reg.record("a", "m", "v1", 0);
        std::thread::sleep(Duration::from_millis(2));
        reg.record("b", "m", "v1", 1);
        std::thread::sleep(Duration::from_millis(2));
        reg.record("c", "m", "v1", 2); // over cap -> evicts oldest ("a")
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.lookup("a", "m", "v1"), None);
        assert_eq!(reg.lookup("b", "m", "v1"), Some(1));
        assert_eq!(reg.lookup("c", "m", "v1"), Some(2));
    }
}
