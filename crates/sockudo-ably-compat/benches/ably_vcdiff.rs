use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const MAX_BASE_BYTES: usize = 64 * 1024;

struct CountingAllocator;

static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

// The benchmark process is single-purpose. Counting the system allocator here
// gives a stable allocation profile for each deterministic workload preflight.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegates the unchanged layout to the system allocator.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from the matching system allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Delegates the unchanged allocation metadata to the system allocator.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
enum Similarity {
    Similar,
    Dissimilar,
}

impl Similarity {
    const fn name(self) -> &'static str {
        match self {
            Self::Similar => "similar",
            Self::Dissimilar => "dissimilar",
        }
    }
}

#[derive(Default)]
struct ProjectionTotals {
    delta_messages: usize,
    output_bytes: usize,
}

fn deterministic_payload(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xff) as u8
        })
        .collect()
}

fn payload_pair(size: usize, similarity: Similarity) -> (Vec<u8>, Vec<u8>) {
    let base = deterministic_payload(size, 0x5eed_1234_9876_abcd);
    let mut target = match similarity {
        Similarity::Similar => base.clone(),
        Similarity::Dissimilar => deterministic_payload(size, 0xcafe_f00d_dead_beef),
    };
    if matches!(similarity, Similarity::Similar) {
        for index in (31..target.len()).step_by(256) {
            target[index] = target[index].wrapping_add(1);
        }
    }
    (base, target)
}

fn project_per_subscriber(base: &[u8], target: &[u8], subscribers: usize) -> ProjectionTotals {
    if target.len() > MAX_BASE_BYTES {
        return ProjectionTotals {
            output_bytes: target.len().saturating_mul(subscribers),
            ..ProjectionTotals::default()
        };
    }

    let mut totals = ProjectionTotals::default();
    for _ in 0..subscribers {
        match sockudo_delta::compute_vcdiff(base, target) {
            Ok(delta) if delta.len() < target.len() => {
                totals.delta_messages += 1;
                totals.output_bytes += STANDARD.encode(delta).len();
            }
            _ => totals.output_bytes += target.len(),
        }
    }
    totals
}

fn project_grouped(base: &[u8], target: &[u8], subscribers: usize) -> ProjectionTotals {
    if target.len() > MAX_BASE_BYTES {
        return ProjectionTotals {
            output_bytes: target.len().saturating_mul(subscribers),
            ..ProjectionTotals::default()
        };
    }

    match sockudo_delta::compute_vcdiff(base, target) {
        Ok(delta) if delta.len() < target.len() => ProjectionTotals {
            delta_messages: subscribers,
            output_bytes: STANDARD.encode(delta).len().saturating_mul(subscribers),
        },
        _ => ProjectionTotals {
            output_bytes: target.len().saturating_mul(subscribers),
            ..ProjectionTotals::default()
        },
    }
}

fn allocation_profile(
    workload: fn(&[u8], &[u8], usize) -> ProjectionTotals,
    base: &[u8],
    target: &[u8],
    subscribers: usize,
) -> (ProjectionTotals, u64, u64) {
    ALLOCATION_COUNT.store(0, Ordering::SeqCst);
    ALLOCATED_BYTES.store(0, Ordering::SeqCst);
    let totals = workload(base, target, subscribers);
    let count = ALLOCATION_COUNT.load(Ordering::SeqCst);
    let bytes = ALLOCATED_BYTES.load(Ordering::SeqCst);
    (totals, count, bytes)
}

fn benchmark_ably_vcdiff(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("ably_vcdiff_subscriber_projection");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    for size in [1024, 64 * 1024] {
        for similarity in [Similarity::Similar, Similarity::Dissimilar] {
            let (base, target) = payload_pair(size, similarity);
            for subscribers in [1, 100, 1_000] {
                let full_bytes = size.saturating_mul(subscribers);
                let (legacy, legacy_allocations, legacy_allocated_bytes) =
                    allocation_profile(project_per_subscriber, &base, &target, subscribers);
                let (grouped, grouped_allocations, grouped_allocated_bytes) =
                    allocation_profile(project_grouped, &base, &target, subscribers);
                let ratio = grouped.output_bytes as f64 / full_bytes as f64;
                eprintln!(
                    "ably_vcdiff_profile kind={} payload_bytes={} subscribers={} deltas={} ratio={:.4} legacy_allocations={} legacy_allocated_bytes={} grouped_allocations={} grouped_allocated_bytes={}",
                    similarity.name(),
                    size,
                    subscribers,
                    grouped.delta_messages,
                    ratio,
                    legacy_allocations,
                    legacy_allocated_bytes,
                    grouped_allocations,
                    grouped_allocated_bytes,
                );
                assert_eq!(legacy.delta_messages, grouped.delta_messages);
                assert_eq!(legacy.output_bytes, grouped.output_bytes);

                group.throughput(Throughput::Bytes(full_bytes as u64));
                group.bench_with_input(
                    BenchmarkId::new(
                        format!("legacy-{}-{}b", similarity.name(), size),
                        subscribers,
                    ),
                    &subscribers,
                    |bencher, subscribers| {
                        bencher.iter(|| {
                            black_box(project_per_subscriber(
                                black_box(&base),
                                black_box(&target),
                                black_box(*subscribers),
                            ))
                        });
                    },
                );
                group.bench_with_input(
                    BenchmarkId::new(
                        format!("grouped-{}-{}b", similarity.name(), size),
                        subscribers,
                    ),
                    &subscribers,
                    |bencher, subscribers| {
                        bencher.iter(|| {
                            black_box(project_grouped(
                                black_box(&base),
                                black_box(&target),
                                black_box(*subscribers),
                            ))
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark_ably_vcdiff);
criterion_main!(benches);
