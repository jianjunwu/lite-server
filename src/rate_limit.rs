//! Thread-safe rate limiter for lite-server.
//!
//! Uses a DashMap of std::sync::Mutex-wrapped TokenBuckets so concurrent
//! requests can acquire tokens without contention on separate keys.
//!
//! The critical section is await-free, so a blocking std::sync::Mutex is
//! correct and cheaper than a tokio::sync::Mutex. `acquire` clones the
//! bucket Arc out from under the DashMap shard lock before locking it.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub enum AcquireResult {
    Allowed,
    Rejected { retry_after_secs: u64 },
}

/// Ceiling for the Retry-After computed from a token-bucket wait: a
/// misconfigured (subnormal) rate makes the raw wait saturate u64.
const MAX_RETRY_AFTER_SECS: u64 = 86_400;

pub struct TokenBucket {
    rate: f64,        // tokens per second
    capacity: f64,
    tokens: f64,
    last_update: Instant,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        Self {
            rate,
            capacity,
            tokens: capacity,
            last_update: Instant::now(),
        }
    }
}

pub struct RateLimiter {
    buckets: DashMap<String, Arc<std::sync::Mutex<TokenBucket>>>,
    /// Max distinct buckets before new keys are rejected. 0 = unbounded.
    /// Bounds memory under spoofed-source floods where every request is a new
    /// source IP. Default mirrors `config::RateLimitConfig::max_buckets`.
    max_buckets: usize,
}

impl Default for RateLimiter {
    fn default() -> Self {
        // Keep in sync with config::RateLimitConfig::max_buckets default.
        Self::new(65_536)
    }
}

impl RateLimiter {
    pub fn new(max_buckets: usize) -> Self {
        Self {
            buckets: DashMap::new(),
            max_buckets,
        }
    }

    /// Try to acquire one token for *key*.
    ///
    /// The bucket `Arc` is cloned out from under the DashMap shard lock before
    /// the (blocking) bucket lock is taken, so no DashMap guard is held during
    /// contention on a single bucket. The whole critical section is synchronous
    /// math, hence std::sync::Mutex rather than tokio::sync::Mutex.
    pub fn acquire(&self, key: &str, rpm: f64, burst: f64) -> AcquireResult {
        // C1: a non-positive rpm is a misconfigured policy. Fail closed with a
        // sane Retry-After — otherwise the bucket drains its burst tokens and
        // later requests compute retry_after = (1-0)/0, saturating to u64::MAX.
        if rpm <= 0.0 {
            return AcquireResult::Rejected { retry_after_secs: 1 };
        }

        // Fast path: an existing bucket is read under DashMap's shard lock and
        // Arc-cloned with zero allocation. Only a brand-new key takes the write
        // path (entry + or_insert_with) to create the bucket.
        let bucket = match self.buckets.get(key) {
            Some(b) => b.clone(),
            None => {
                // #7: bound total buckets to protect memory under spoofed-source
                // floods. The len() check races with concurrent inserts across
                // DashMap shards, so the cap is approximate — a handful of
                // buckets over is harmless; enforcing it exactly would need a
                // global lock that defeats the DashMap's whole point.
                if self.max_buckets != 0 && self.buckets.len() >= self.max_buckets {
                    return AcquireResult::Rejected { retry_after_secs: 1 };
                }
                self.buckets
                    .entry(key.to_string())
                    .or_insert_with(|| {
                        Arc::new(std::sync::Mutex::new(TokenBucket::new(
                            rpm / 60.0,
                            burst,
                        )))
                    })
                    .clone()
            }
        };

        // std::sync::Mutex: poison only occurs after a panic while holding the
        // lock (none here — pure math), so recover the inner guard rather than
        // unwinding the request path.
        let mut b = bucket
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        // Policy hot-reload: refresh rate/capacity when they change.
        let rate = rpm / 60.0;
        if (b.rate - rate).abs() > f64::EPSILON || (b.capacity - burst).abs() > f64::EPSILON {
            b.rate = rate;
            b.capacity = burst;
            b.tokens = b.tokens.min(burst);
        }

        let now = Instant::now();
        let elapsed = now.duration_since(b.last_update).as_secs_f64();
        b.tokens = (b.tokens + elapsed * b.rate).min(b.capacity);
        b.last_update = now;

        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            AcquireResult::Allowed
        } else {
            // A positive-but-subnormal rpm makes rate effectively zero, so
            // the raw wait is astronomically large (or infinite on a full
            // underflow) and `as u64` would saturate to u64::MAX. Clamp to a
            // bounded Retry-After: fail closed, but sanely.
            let wait = (1.0 - b.tokens) / b.rate;
            AcquireResult::Rejected {
                retry_after_secs: wait.ceil().max(1.0).min(MAX_RETRY_AFTER_SECS as f64) as u64,
            }
        }
    }

    /// Evict buckets idle longer than *max_age*.  Returns the number removed.
    /// Called periodically from a background task to prevent unbounded growth
    /// when `key="ip"`.
    pub fn cleanup_stale(&self, max_age: Duration) -> usize {
        let now = Instant::now();
        let mut removed = 0usize;
        self.buckets.retain(|_, bucket| match bucket.try_lock() {
            Ok(b) => {
                let keep = now.duration_since(b.last_update) < max_age;
                if !keep {
                    removed += 1;
                }
                keep
            }
            Err(_) => true, // in use — keep
        });
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_and_refill() {
        let limiter = RateLimiter::default();
        // 60 RPM = 1 token/sec, burst = 1
        let result = limiter.acquire("test", 60.0, 1.0);
        assert!(matches!(result, AcquireResult::Allowed));
        // Bucket exhausted
        let result = limiter.acquire("test", 60.0, 1.0);
        assert!(matches!(result, AcquireResult::Rejected { .. }));
    }

    #[test]
    fn test_burst_exceeds_rate() {
        let limiter = RateLimiter::default();
        // burst = 5
        for _ in 0..5 {
            let result = limiter.acquire("burst", 60.0, 5.0);
            assert!(matches!(result, AcquireResult::Allowed));
        }
        let result = limiter.acquire("burst", 60.0, 5.0);
        assert!(matches!(result, AcquireResult::Rejected { .. }));
    }

    #[tokio::test]
    async fn test_policy_hot_reload() {
        let limiter = RateLimiter::default();
        // High rate (6000/min = 100/sec), burst = 1
        limiter.acquire("reload", 6000.0, 1.0);
        let result = limiter.acquire("reload", 6000.0, 1.0);
        assert!(matches!(result, AcquireResult::Rejected { .. }));

        // Policy hot-reload: burst changes but tokens don't reset.
        // With 100 tokens/sec, after 20ms we get ~2 tokens.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let result = limiter.acquire("reload", 6000.0, 10.0);
        assert!(matches!(result, AcquireResult::Allowed));
    }

    #[tokio::test]
    async fn test_concurrent_no_overshoot() {
        use std::sync::Arc;
        let limiter = Arc::new(RateLimiter::default());
        let mut handles = vec![];
        // 60 RPM, burst=10 — all 10 should succeed, 11th fails
        let key = Arc::new("concurrent".to_string());
        for _ in 0..12 {
            let limiter = limiter.clone();
            let key = key.clone();
            handles.push(tokio::task::spawn_blocking(move || {
                limiter.acquire(&key, 60.0, 10.0)
            }));
        }
        let mut allowed = 0;
        let mut rejected = 0;
        for h in handles {
            match h.await.unwrap() {
                AcquireResult::Allowed => allowed += 1,
                AcquireResult::Rejected { .. } => rejected += 1,
            }
        }
        assert_eq!(allowed, 10, "only burst tokens should be allowed");
        assert_eq!(rejected, 2);
    }

    #[test]
    fn test_cleanup_stale_removes_idle_buckets() {
        let limiter = RateLimiter::default();
        limiter.acquire("keep", 60.0, 1.0);
        limiter.acquire("stale", 60.0, 1.0);

        // Manually expire the "stale" bucket
        {
            let bucket = limiter.buckets.get("stale").unwrap().clone();
            let mut b = bucket.lock().unwrap_or_else(|p| p.into_inner());
            b.last_update = Instant::now() - Duration::from_secs(1200);
        }

        let removed = limiter.cleanup_stale(Duration::from_secs(600));
        assert_eq!(removed, 1);
        assert!(limiter.buckets.contains_key("keep"));
        assert!(!limiter.buckets.contains_key("stale"));
    }

    #[test]
    fn test_cleanup_preserves_in_use_buckets() {
        let limiter = Arc::new(RateLimiter::default());
        limiter.acquire("inuse", 60.0, 1.0);

        // Hold the lock while cleaning up
        let bucket = limiter.buckets.get("inuse").unwrap().clone();
        let _guard = bucket.lock().unwrap_or_else(|p| p.into_inner());

        let removed = limiter.cleanup_stale(Duration::from_secs(0)); // everything stale
        assert_eq!(removed, 0); // in-use bucket preserved
        assert!(limiter.buckets.contains_key("inuse"));
    }

    #[test]
    fn test_acquire_rejects_misconfigured_nonpositive_rpm() {
        // C1: rpm <= 0 is a misconfigured policy. Previously the first request
        // drained the burst bucket and later requests computed retry_after =
        // (1-0)/0 → saturated to u64::MAX. Fail closed with a sane Retry-After.
        let limiter = RateLimiter::default();
        let result = limiter.acquire("zero-rpm", 0.0, 1.0);
        assert!(
            matches!(result, AcquireResult::Rejected { retry_after_secs: 1 }),
            "zero rpm must be rejected immediately"
        );
        let result = limiter.acquire("neg-rpm", -5.0, 1.0);
        assert!(
            matches!(result, AcquireResult::Rejected { retry_after_secs: 1 }),
            "negative rpm must be rejected immediately"
        );
    }

    #[test]
    fn test_max_buckets_rejects_new_keys_beyond_cap() {
        // #7: under a spoofed-source flood every request is a new key and thus
        // a new bucket. A hard cap must fail closed (reject) once reached so
        // memory cannot grow unbounded between cleanup sweeps.
        //
        // burst is large so token exhaustion never interferes with the
        // bucket-count assertions below.
        let limiter = RateLimiter::new(2);
        assert!(matches!(limiter.acquire("a", 60.0, 100.0), AcquireResult::Allowed));
        assert!(matches!(limiter.acquire("b", 60.0, 100.0), AcquireResult::Allowed));
        // Cap reached — a brand-new key is rejected, not admitted as a 3rd bucket.
        assert!(matches!(
            limiter.acquire("c", 60.0, 100.0),
            AcquireResult::Rejected { .. }
        ));
        // Existing keys take the fast path and bypass the cap, so they still
        // acquire even though the bucket count is at the limit.
        assert!(matches!(limiter.acquire("a", 60.0, 100.0), AcquireResult::Allowed));
    }

    #[test]
    fn test_max_buckets_zero_means_unbounded() {
        // max_buckets = 0 disables the cap entirely (opt-out).
        let limiter = RateLimiter::new(0);
        for i in 0..1000 {
            let key = format!("k{}", i);
            assert!(matches!(limiter.acquire(&key, 60.0, 1.0), AcquireResult::Allowed));
        }
    }
}

    #[test]
    fn test_subnormal_positive_rpm_rejects_with_sane_retry_after() {
        // A positive-but-subnormal rpm (e.g. "1e-320" parsed from config)
        // passes the C1 guard (rpm <= 0.0) but makes the refill rate
        // effectively zero — the raw wait then saturates `as u64`, so every
        // client gets Retry-After: u64::MAX (~584 billion years) and the
        // bucket rejects forever. (1e-320, not 1e-330: the latter underflows
        // to 0.0 already at literal-parse time and never reaches the bucket.)
        let limiter = RateLimiter::default();
        let first = limiter.acquire("subnormal", 1e-320, 1.0);
        assert!(matches!(first, AcquireResult::Allowed), "burst token must pass");

        match limiter.acquire("subnormal", 1e-320, 1.0) {
            AcquireResult::Rejected { retry_after_secs } => {
                assert!(
                    retry_after_secs < 1_000_000,
                    "subnormal rpm must fail closed with a sane Retry-After, got {retry_after_secs}"
                );
            }
            AcquireResult::Allowed => panic!("a 0.0-refill bucket must reject after the burst"),
        }
    }
