//! Isolates the subscriber-grouping cost added by qualified channel projections.
#![allow(dead_code, unused_imports)]

#[path = "../src/codec.rs"]
mod codec;
#[path = "../src/protocol.rs"]
mod protocol;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use protocol::AblyFormat;
use std::{collections::HashMap, hint::black_box, sync::Arc, time::Duration};

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

criterion_group!(benches, benchmark_fanout_grouping);
criterion_main!(benches);
