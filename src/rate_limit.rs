//! Thread-safe rate limiter for lite-server.
//!
//! Uses a DashMap of tokio::Mutex-wrapped TokenBuckets so concurrent
//! requests can acquire tokens without contention on separate keys.
//!
//! The `acquire` method avoids holding the DashMap guard across `.await`
//! (which would make the future `!Send`), and supports in-place policy
//! hot-reload when rpm or burst parameters change (worker respawn).

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub enum AcquireResult {
    Allowed,
    Rejected { retry_after_secs: u64 },
}

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
    buckets: DashMap<String, Arc<tokio::sync::Mutex<TokenBucket>>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    /// Try to acquire one token for *key*.
    ///
    /// The short synchronous scope that clones the `Arc` avoids holding the
    /// DashMap guard across `.await`, which would make the future `!Send`.
    pub async fn acquire(&self, key: &str, rpm: f64, burst: f64) -> AcquireResult {
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
            None => self
                .buckets
                .entry(key.to_string())
                .or_insert_with(|| {
                    Arc::new(tokio::sync::Mutex::new(TokenBucket::new(
                        rpm / 60.0,
                        burst,
                    )))
                })
                .clone(),
        };

        let mut b = bucket.lock().await;

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
            let wait = (1.0 - b.tokens) / b.rate;
            AcquireResult::Rejected {
                retry_after_secs: wait.ceil().max(1.0) as u64,
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

    #[tokio::test]
    async fn test_acquire_and_refill() {
        let limiter = RateLimiter::new();
        // 60 RPM = 1 token/sec, burst = 1
        let result = limiter.acquire("test", 60.0, 1.0).await;
        assert!(matches!(result, AcquireResult::Allowed));
        // Bucket exhausted
        let result = limiter.acquire("test", 60.0, 1.0).await;
        assert!(matches!(result, AcquireResult::Rejected { .. }));
    }

    #[tokio::test]
    async fn test_burst_exceeds_rate() {
        let limiter = RateLimiter::new();
        // burst = 5
        for _ in 0..5 {
            let result = limiter.acquire("burst", 60.0, 5.0).await;
            assert!(matches!(result, AcquireResult::Allowed));
        }
        let result = limiter.acquire("burst", 60.0, 5.0).await;
        assert!(matches!(result, AcquireResult::Rejected { .. }));
    }

    #[tokio::test]
    async fn test_policy_hot_reload() {
        let limiter = RateLimiter::new();
        // High rate (6000/min = 100/sec), burst = 1
        limiter.acquire("reload", 6000.0, 1.0).await;
        let result = limiter.acquire("reload", 6000.0, 1.0).await;
        assert!(matches!(result, AcquireResult::Rejected { .. }));

        // Policy hot-reload: burst changes but tokens don't reset.
        // With 100 tokens/sec, after 20ms we get ~2 tokens.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let result = limiter.acquire("reload", 6000.0, 10.0).await;
        assert!(matches!(result, AcquireResult::Allowed));
    }

    #[tokio::test]
    async fn test_concurrent_no_overshoot() {
        use std::sync::Arc;
        let limiter = Arc::new(RateLimiter::new());
        let mut handles = vec![];
        // 60 RPM, burst=10 — all 10 should succeed, 11th fails
        let key = Arc::new("concurrent".to_string());
        for _ in 0..12 {
            let limiter = limiter.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                limiter.acquire(&key, 60.0, 10.0).await
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

    #[tokio::test]
    async fn test_cleanup_stale_removes_idle_buckets() {
        let limiter = RateLimiter::new();
        limiter.acquire("keep", 60.0, 1.0).await;
        limiter.acquire("stale", 60.0, 1.0).await;

        // Manually expire the "stale" bucket
        {
            let bucket = limiter.buckets.get("stale").unwrap().clone();
            let mut b = bucket.lock().await;
            b.last_update = Instant::now() - Duration::from_secs(1200);
        }

        let removed = limiter.cleanup_stale(Duration::from_secs(600));
        assert_eq!(removed, 1);
        assert!(limiter.buckets.contains_key("keep"));
        assert!(!limiter.buckets.contains_key("stale"));
    }

    #[tokio::test]
    async fn test_cleanup_preserves_in_use_buckets() {
        let limiter = Arc::new(RateLimiter::new());
        limiter.acquire("inuse", 60.0, 1.0).await;

        // Hold the lock while cleaning up
        let bucket = limiter.buckets.get("inuse").unwrap().clone();
        let _guard = bucket.lock().await;

        let removed = limiter.cleanup_stale(Duration::from_secs(0)); // everything stale
        assert_eq!(removed, 0); // in-use bucket preserved
        assert!(limiter.buckets.contains_key("inuse"));
    }

    #[tokio::test]
    async fn test_acquire_rejects_misconfigured_nonpositive_rpm() {
        // C1: rpm <= 0 is a misconfigured policy. Previously the first request
        // drained the burst bucket and later requests computed retry_after =
        // (1-0)/0 → saturated to u64::MAX. Fail closed with a sane Retry-After.
        let limiter = RateLimiter::new();
        let result = limiter.acquire("zero-rpm", 0.0, 1.0).await;
        assert!(
            matches!(result, AcquireResult::Rejected { retry_after_secs: 1 }),
            "zero rpm must be rejected immediately"
        );
        let result = limiter.acquire("neg-rpm", -5.0, 1.0).await;
        assert!(
            matches!(result, AcquireResult::Rejected { retry_after_secs: 1 }),
            "negative rpm must be rejected immediately"
        );
    }
}
