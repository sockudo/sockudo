use ahash::AHasher;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parking_lot::Mutex as ParkingMutex;
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::sync::Mutex as StdMutex;

fn hash_with_default(payload: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    payload.hash(&mut hasher);
    hasher.finish()
}

fn hash_with_ahash(payload: &[u8]) -> u64 {
    let mut hasher = AHasher::default();
    payload.hash(&mut hasher);
    hasher.finish()
}

struct StdReplayState {
    messages: StdMutex<VecDeque<Vec<u8>>>,
}

struct ParkingReplayState {
    messages: ParkingMutex<VecDeque<Vec<u8>>>,
}

fn bench_base_message_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("adapter_base_message_hash");

    for size in [256_usize, 4096, 65_536] {
        let payload = (0..size)
            .map(|index| (index.wrapping_mul(31) & 0xff) as u8)
            .collect::<Vec<_>>();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::new("current_default_hasher", size),
            &payload,
            |b, bytes| {
                b.iter(|| black_box(hash_with_default(black_box(bytes))));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("replacement_ahash", size),
            &payload,
            |b, bytes| {
                b.iter(|| black_box(hash_with_ahash(black_box(bytes))));
            },
        );
    }

    group.finish();
}

fn bench_presence_stagger_hash(c: &mut Criterion) {
    let node_id = "9d7c5ff0-aeb2-4c48-91e6-bf6b5e588cb1".as_bytes();
    let mut group = c.benchmark_group("adapter_presence_stagger_hash");
    group.bench_function("current_default_hasher", |b| {
        b.iter(|| black_box(hash_with_default(black_box(node_id)) % 5_000));
    });
    group.bench_function("replacement_ahash", |b| {
        b.iter(|| black_box(hash_with_ahash(black_box(node_id)) % 5_000));
    });
    group.finish();
}

fn bench_replay_lock_store(c: &mut Criterion) {
    let payload = vec![42_u8; 256];
    let mut group = c.benchmark_group("adapter_replay_lock_store");
    group.throughput(Throughput::Elements(1024));

    group.bench_function("current_std_mutex", |b| {
        b.iter_batched(
            || StdReplayState {
                messages: StdMutex::new(VecDeque::with_capacity(1024)),
            },
            |state| {
                for serial in 0..1024 {
                    let mut messages = state.messages.lock().expect("benchmark mutex");
                    if messages.len() >= 1024 {
                        messages.pop_front();
                    }
                    let mut message = payload.clone();
                    message[0] = (serial & 0xff) as u8;
                    messages.push_back(message);
                }
                black_box(state.messages.lock().expect("benchmark mutex").len());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("replacement_parking_lot", |b| {
        b.iter_batched(
            || ParkingReplayState {
                messages: ParkingMutex::new(VecDeque::with_capacity(1024)),
            },
            |state| {
                for serial in 0..1024 {
                    let mut messages = state.messages.lock();
                    if messages.len() >= 1024 {
                        messages.pop_front();
                    }
                    let mut message = payload.clone();
                    message[0] = (serial & 0xff) as u8;
                    messages.push_back(message);
                }
                black_box(state.messages.lock().len());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_base_message_hash,
    bench_presence_stagger_hash,
    bench_replay_lock_store
);
criterion_main!(benches);
