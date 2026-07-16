//! Isolates the subscriber-grouping cost added by qualified channel projections.
#![allow(dead_code, unused_imports)]

#[path = "../src/codec.rs"]
mod codec;
#[path = "../src/protocol.rs"]
mod protocol;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use protocol::AblyFormat;
use std::{
    collections::{HashMap, HashSet},
    hint::black_box,
    sync::{Arc, Mutex},
    time::Duration,
};

const DELIVERY_DEDUPE_BOUND: usize = 4_096;

fn benchmark_fanout_grouping(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("ably_fanout_grouping");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);

    for subscribers in [0_usize, 1, 100, 1_000, 10_000] {
        let legacy = (0..subscribers)
            .map(|index| (format!("session-{index}"), AblyFormat::Json))
            .collect::<Vec<_>>();
        let parsed = (0..subscribers)
            .map(|index| {
                (
                    Arc::<str>::from(format!("session-{index}")),
                    Arc::<str>::from("room"),
                    AblyFormat::Json,
                )
            })
            .collect::<Vec<_>>();

        group.bench_with_input(
            BenchmarkId::new("pre_qualified_names", subscribers),
            &legacy,
            |bencher, subscribers| {
                bencher.iter(|| {
                    let mut grouped = HashMap::<AblyFormat, Vec<String>>::new();
                    for (session, format) in black_box(subscribers) {
                        grouped.entry(*format).or_default().push(session.clone());
                    }
                    black_box(grouped)
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("parsed_unqualified", subscribers),
            &parsed,
            |bencher, subscribers| {
                bencher.iter(|| {
                    let mut grouped = HashMap::<(AblyFormat, Arc<str>), Vec<Arc<str>>>::new();
                    for (session, requested_channel, format) in black_box(subscribers) {
                        grouped
                            .entry((*format, Arc::clone(requested_channel)))
                            .or_default()
                            .push(Arc::clone(session));
                    }
                    black_box(grouped)
                });
            },
        );
    }
    group.finish();
}

fn benchmark_remote_delivery_dedupe(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("ably_remote_delivery_dedupe");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);

    for deliveries in [1_usize, 100, 1_000, 10_000] {
        let positions = (0..deliveries)
            .map(|serial| (Arc::<str>::from("stream-1"), serial as u64 + 1))
            .collect::<Vec<_>>();
        group.bench_with_input(
            BenchmarkId::new("pre_dedupe_identity_read", deliveries),
            &positions,
            |bencher, positions| {
                bencher.iter(|| {
                    for position in black_box(positions) {
                        black_box(position);
                    }
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bounded_continuity_tracker", deliveries),
            &positions,
            |bencher, positions| {
                bencher.iter_batched(
                    || Mutex::new((0_u64, HashSet::new())),
                    |state| {
                        for (_, serial) in black_box(positions) {
                            let mut state = state.lock().unwrap();
                            if *serial <= state.0 || state.1.contains(serial) {
                                continue;
                            }
                            if *serial == state.0.saturating_add(1) {
                                state.0 = *serial;
                                while let Some(next) = state.0.checked_add(1) {
                                    if !state.1.remove(&next) {
                                        break;
                                    }
                                    state.0 = next;
                                }
                            } else if state.1.len() < DELIVERY_DEDUPE_BOUND {
                                state.1.insert(*serial);
                            }
                        }
                        black_box(state)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_fanout_grouping,
    benchmark_remote_delivery_dedupe
);
criterion_main!(benches);
