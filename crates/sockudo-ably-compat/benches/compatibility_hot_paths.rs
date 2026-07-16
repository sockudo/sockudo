//! Release-evidence benchmarks for the complete Ably compatibility data path.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sockudo_ably_compat::benchmark::{
    BenchFormat, CodecFixture, CompatibilityCase, FanoutFixture, HistoryFixture, ProjectionFixture,
    error_frame,
};
use std::{hint::black_box, time::Duration};

const PAYLOAD_SIZES: [usize; 3] = [64, 1_024, 64 * 1_024];
const SUBSCRIBER_COUNTS: [usize; 4] = [1, 100, 1_000, 10_000];

fn configured_group<'a>(
    criterion: &'a mut Criterion,
    name: &str,
) -> criterion::BenchmarkGroup<'a, criterion::measurement::WallTime> {
    let mut group = criterion.benchmark_group(name);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(20);
    group
}

fn benchmark_decode(criterion: &mut Criterion) {
    let mut group = configured_group(criterion, "ably_compat_decode");
    for payload_bytes in PAYLOAD_SIZES {
        let fixture = CodecFixture::encrypted(payload_bytes).expect("codec fixture");
        for format in [BenchFormat::Json, BenchFormat::MsgPack] {
            group.throughput(Throughput::Bytes(
                fixture.encoded(format).len().try_into().unwrap_or(u64::MAX),
            ));
            group.bench_with_input(
                BenchmarkId::new(format.as_str(), payload_bytes),
                &fixture,
                |bencher, fixture| {
                    bencher.iter(|| {
                        black_box(
                            fixture
                                .decode(black_box(format))
                                .expect("decode benchmark input"),
                        )
                    });
                },
            );
        }
    }
    group.finish();
}

fn benchmark_projection_and_encode(criterion: &mut Criterion) {
    let mut projection = configured_group(criterion, "ably_compat_projection");
    for payload_bytes in PAYLOAD_SIZES {
        let fixture = ProjectionFixture::encrypted(vec![0xa5; payload_bytes]);
        projection.throughput(Throughput::Bytes(payload_bytes as u64));
        projection.bench_with_input(
            BenchmarkId::new("encrypted_binary", payload_bytes),
            &fixture,
            |bencher, fixture| {
                bencher.iter(|| {
                    let projected = fixture.project().expect("projection benchmark input");
                    black_box(projected.projected_payload_len())
                });
            },
        );
    }
    projection.finish();

    let mut encode = configured_group(criterion, "ably_compat_encode");
    for payload_bytes in PAYLOAD_SIZES {
        let projected = ProjectionFixture::encrypted(vec![0xa5; payload_bytes])
            .project()
            .expect("projection fixture");
        for format in [BenchFormat::Json, BenchFormat::MsgPack] {
            encode.throughput(Throughput::Bytes(payload_bytes as u64));
            encode.bench_with_input(
                BenchmarkId::new(format.as_str(), payload_bytes),
                &projected,
                |bencher, projected| {
                    bencher.iter(|| {
                        black_box(
                            projected
                                .encode(black_box(format))
                                .expect("encode benchmark input"),
                        )
                    });
                },
            );
        }
    }
    encode.finish();

    let mut error = configured_group(criterion, "ably_compat_error_encode");
    let frame = error_frame();
    for format in [BenchFormat::Json, BenchFormat::MsgPack] {
        error.bench_with_input(
            BenchmarkId::from_parameter(format.as_str()),
            &frame,
            |bencher, frame| {
                bencher.iter(|| {
                    black_box(
                        frame
                            .encode(black_box(format))
                            .expect("error encode benchmark input"),
                    )
                });
            },
        );
    }
    error.finish();
}

fn benchmark_history_and_replay(criterion: &mut Criterion) {
    let history = HistoryFixture::new(1, 256);
    let mut projection = configured_group(criterion, "ably_compat_history_projection");
    projection.throughput(Throughput::Elements(1));
    projection.bench_function("one_message", |bencher| {
        bencher.iter(|| black_box(history.project_history().expect("history fixture")));
    });
    projection.finish();

    let replay = HistoryFixture::new(4_096, 64);
    let mut replay_group = configured_group(criterion, "ably_compat_replay");
    replay_group.sample_size(10);
    replay_group.measurement_time(Duration::from_secs(3));
    replay_group.throughput(Throughput::Elements(4_096));
    replay_group.bench_function("4096_messages", |bencher| {
        bencher.iter(|| black_box(replay.project_replay().expect("replay fixture")));
    });
    replay_group.finish();
}

fn benchmark_adapter_fanout(criterion: &mut Criterion) {
    let mut boundary = configured_group(criterion, "ably_compat_adapter_boundary");
    for case in [
        CompatibilityCase::Disabled,
        CompatibilityCase::EnabledNoSubscribers,
    ] {
        boundary.bench_function(case.as_str(), |bencher| {
            let mut fixture = FanoutFixture::new(case, 0).expect("boundary fixture");
            bencher.iter(|| {
                black_box(
                    fixture
                        .deliver_and_drain()
                        .expect("boundary benchmark delivery"),
                )
            });
        });
    }
    boundary.finish();

    for subscribers in SUBSCRIBER_COUNTS {
        let mut fanout = configured_group(criterion, "ably_compat_adapter_fanout");
        fanout.sample_size(10);
        fanout.measurement_time(Duration::from_secs(3));
        fanout.throughput(Throughput::Elements(subscribers as u64));
        for case in [
            CompatibilityCase::JsonOnly,
            CompatibilityCase::MsgPackOnly,
            CompatibilityCase::Mixed,
        ] {
            fanout.bench_with_input(
                BenchmarkId::new(case.as_str(), subscribers),
                &subscribers,
                |bencher, &subscriber_count| {
                    bencher.iter_batched(
                        || {
                            FanoutFixture::new(case, subscriber_count)
                                .expect("adapter fanout fixture")
                        },
                        |mut fixture| {
                            let observation = fixture
                                .deliver_and_drain()
                                .expect("adapter fanout delivery");
                            black_box(observation)
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
        fanout.finish();
    }
}

criterion_group!(
    benches,
    benchmark_decode,
    benchmark_projection_and_encode,
    benchmark_history_and_replay,
    benchmark_adapter_fanout,
);
criterion_main!(benches);
