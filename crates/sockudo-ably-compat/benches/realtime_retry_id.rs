//! Isolates the bounded CPU/allocation cost of deriving a stable server message ID
//! for Ably realtime publishes that do not already carry one.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::{hint::black_box, time::Duration};

const BATCH: u64 = 1_000;

fn benchmark_realtime_retry_id(criterion: &mut Criterion) {
    let connection_id = "connection-with-a-representative-identifier";
    let existing_id = "client-supplied-message-id";
    let mut group = criterion.benchmark_group("ably_realtime_retry_id");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function("before_no_derived_id", |bencher| {
        bencher.iter(|| {
            for _ in 0..BATCH {
                black_box(existing_id);
            }
        });
    });
    group.bench_function("after_derive_missing_id", |bencher| {
        bencher.iter(|| {
            for index in 0..BATCH {
                black_box(format!("{connection_id}:{}:{index}", 42_u64));
            }
        });
    });
    group.finish();
}

criterion_group!(benches, benchmark_realtime_retry_id);
criterion_main!(benches);
