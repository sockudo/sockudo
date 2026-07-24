//! Measures the bounded stats-recording cost against the pre-stats atomic path.
#![allow(dead_code, unused_imports)]

#[path = "../src/stats.rs"]
mod stats;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use stats::{StatsAggregator, StatsObservation, StatsRuntimeConfig};
use std::{
    hint::black_box,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const BURST: u64 = 1_000;

fn benchmark_stats_recording(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _runtime_guard = runtime.enter();
    let aggregator = StatsAggregator::new(
        None,
        StatsRuntimeConfig {
            queue_capacity: 4_096,
            flush_interval: Duration::from_millis(10),
            retention_seconds: 60,
            max_scan_entries: 10_000,
            cas_retries: 8,
        },
    );
    let baseline = AtomicU64::new(0);
    let mut group = criterion.benchmark_group("ably_stats_recording");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(30);
    group.throughput(Throughput::Elements(BURST));

    group.bench_function("atomic_counter_baseline", |bencher| {
        bencher.iter(|| {
            for _ in 0..BURST {
                black_box(baseline.fetch_add(1, Ordering::Relaxed));
            }
        });
    });
    group.bench_function("bounded_batch_and_flush", |bencher| {
        bencher.iter(|| {
            for _ in 0..BURST {
                let observation = StatsObservation::messages(
                    "app",
                    1_770_134_580_000,
                    "outbound",
                    "realtime",
                    1,
                    64,
                )
                .unwrap();
                aggregator.try_record(observation).unwrap();
            }
            runtime.block_on(aggregator.flush()).unwrap();
            black_box(aggregator.snapshot());
        });
    });
    group.finish();

    let snapshot = aggregator.snapshot();
    assert_eq!(snapshot.backlog, 0);
    assert_eq!(snapshot.dropped, 0);
}

criterion_group!(benches, benchmark_stats_recording);
criterion_main!(benches);
