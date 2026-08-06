//! §6.4 / §7.5 acceptance bench for the tensor/bytes v2 change:
//! the "extractor + payload preparation" segment (body already aggregated →
//! `meta.payload` ready), v1 (`from_slice::<Value>` + `to_vec`) vs v2
//! (`&RawValue` zero-copy validation + `Bytes` refcount clone).
//!
//! Acceptance: 8 MiB JSON — v2 wall-time must drop ≥40% vs v1; 1 KiB / 1 MiB
//! must not regress. Allocation counts are reported alongside (v2 must
//! eliminate the Value DOM and the re-serialization buffer).

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::Value;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

// ----- counting global allocator (alloc-count reporting, not timing) -----
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
}
#[global_allocator]
static A: Counting = Counting;

fn allocs_per_call<F: FnMut() -> Bytes>(mut f: F, iters: usize) -> usize {
    // Warm up (lazy statics, thread-locals), then measure.
    for _ in 0..3 {
        black_box(f());
    }
    let start = ALLOCS.load(Ordering::Relaxed);
    for _ in 0..iters {
        black_box(f());
    }
    (ALLOCS.load(Ordering::Relaxed) - start) / iters
}

/// Realistic batch numeric payload: `{"data":[0,1,2,...]}` — the worst case
/// for a Value DOM (every element is an allocated AST node).
fn json_payload(target: usize) -> Vec<u8> {
    let mut s = String::from(r#"{"data":["#);
    let mut i = 0usize;
    while s.len() < target.saturating_sub(2) {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&i.to_string());
        i += 1;
    }
    s.push_str("]}");
    s.into_bytes()
}

/// v1: full DOM materialization + re-serialization into a fresh buffer.
fn v1_round_trip(body: &Bytes) -> Bytes {
    let v: Value = serde_json::from_slice(black_box(body)).unwrap();
    Bytes::from(serde_json::to_vec(black_box(&v)).unwrap())
}

/// v2: zero-allocation syntax validation, original bytes forwarded (O(1)
/// refcount clone into meta.payload).
fn v2_validate_forward(body: &Bytes) -> Bytes {
    serde_json::from_slice::<&serde_json::value::RawValue>(black_box(body)).unwrap();
    black_box(body.clone())
}

fn bench_extractor(c: &mut Criterion) {
    let sizes = [
        ("1KiB", 1usize << 10),
        ("1MiB", 1usize << 20),
        ("8MiB", 8usize << 20),
    ];
    let mut group = c.benchmark_group("extractor");
    // Keep the 8 MiB DOM case tractable: fewer samples, short warm-up.
    group
        .sample_size(20)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(4));
    for (label, size) in sizes {
        let body = Bytes::from(json_payload(size));
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_with_input(BenchmarkId::new("v1_value_round_trip", label), &body, |b, body| {
            b.iter(|| v1_round_trip(body));
        });
        group.bench_with_input(BenchmarkId::new("v2_rawvalue_forward", label), &body, |b, body| {
            b.iter(|| v2_validate_forward(body));
        });
    }
    group.finish();

    // Allocation-count report (separate from criterion timing).
    println!("\n=== allocations per call (warm) ===");
    for (label, size) in [("1KiB", 1usize << 10), ("1MiB", 1usize << 20), ("8MiB", 8usize << 20)] {
        let body = Bytes::from(json_payload(size));
        let v1 = allocs_per_call(|| v1_round_trip(&body), 10);
        let v2 = allocs_per_call(|| v2_validate_forward(&body), 10);
        println!("{label:>6}: v1={v1:>8} allocs/call   v2={v2:>3} allocs/call");
    }
}

criterion_group!(benches, bench_extractor);
criterion_main!(benches);
