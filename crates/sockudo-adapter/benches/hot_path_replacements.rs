use ahash::{AHashMap, AHasher, RandomState as AHashRandomState};
use compact_str::{CompactString, format_compact};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use dashmap::DashMap;
use papaya::HashMap as PapayaHashMap;
use parking_lot::Mutex as ParkingMutex;
use scc::{HashIndex as SccHashIndex, HashMap as SccHashMap};
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::sync::Mutex as StdMutex;
use std::thread;

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

fn short_channel_names(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("private-{index:04}"))
        .collect()
}

fn long_channel_names(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("presence-enterprise-region-eu-customer-{index:04}-updates"))
        .collect()
}

fn bench_compact_string_candidates(c: &mut Criterion) {
    const ITEMS: usize = 4096;

    let short_names = short_channel_names(ITEMS);
    let long_names = long_channel_names(ITEMS);
    let compact_short_names = short_names
        .iter()
        .map(CompactString::new)
        .collect::<Vec<_>>();

    let mut clone_group = c.benchmark_group("adapter_compact_string_clone");
    clone_group.throughput(Throughput::Elements(ITEMS as u64));
    clone_group.bench_function("current_string_short", |b| {
        b.iter(|| {
            let mut total_len = 0;
            for name in &short_names {
                let cloned = black_box(name.clone());
                total_len += cloned.len();
                black_box(cloned);
            }
            black_box(total_len)
        });
    });
    clone_group.bench_function("candidate_compact_str_short", |b| {
        b.iter(|| {
            let mut total_len = 0;
            for name in &compact_short_names {
                let cloned = black_box(name.clone());
                total_len += cloned.len();
                black_box(cloned);
            }
            black_box(total_len)
        });
    });
    clone_group.finish();

    let mut build_group = c.benchmark_group("adapter_compact_string_build_key");
    build_group.throughput(Throughput::Elements(ITEMS as u64));
    build_group.bench_function("current_string_short_composite", |b| {
        b.iter(|| {
            let mut total_len = 0;
            for channel in &short_names {
                let key = black_box(format!("app:{channel}"));
                total_len += key.len();
                black_box(key);
            }
            black_box(total_len)
        });
    });
    build_group.bench_function("candidate_compact_str_short_composite", |b| {
        b.iter(|| {
            let mut total_len = 0;
            for channel in &short_names {
                let key = black_box(format_compact!("app:{channel}"));
                total_len += key.len();
                black_box(key);
            }
            black_box(total_len)
        });
    });
    build_group.bench_function("current_string_long_composite", |b| {
        b.iter(|| {
            let mut total_len = 0;
            for channel in &long_names {
                let key = black_box(format!("production-app:{channel}"));
                total_len += key.len();
                black_box(key);
            }
            black_box(total_len)
        });
    });
    build_group.bench_function("candidate_compact_str_long_composite", |b| {
        b.iter(|| {
            let mut total_len = 0;
            for channel in &long_names {
                let key = black_box(format_compact!("production-app:{channel}"));
                total_len += key.len();
                black_box(key);
            }
            black_box(total_len)
        });
    });
    build_group.finish();

    let string_map = short_names
        .iter()
        .enumerate()
        .map(|(index, name)| (name.clone(), index))
        .collect::<AHashMap<_, _>>();
    let compact_map = short_names
        .iter()
        .enumerate()
        .map(|(index, name)| (CompactString::new(name), index))
        .collect::<AHashMap<_, _>>();

    let mut lookup_group = c.benchmark_group("adapter_compact_string_lookup");
    lookup_group.throughput(Throughput::Elements(ITEMS as u64));
    lookup_group.bench_function("current_string_map_by_str", |b| {
        b.iter(|| {
            let mut sum = 0;
            for key in &short_names {
                if let Some(value) = string_map.get(key.as_str()) {
                    sum += *value;
                }
            }
            black_box(sum)
        });
    });
    lookup_group.bench_function("candidate_compact_str_map_by_str", |b| {
        b.iter(|| {
            let mut sum = 0;
            for key in &short_names {
                if let Some(value) = compact_map.get(key.as_str()) {
                    sum += *value;
                }
            }
            black_box(sum)
        });
    });
    lookup_group.finish();
}

type DashAHashMap = DashMap<u64, u64, AHashRandomState>;
type PapayaAHashMap = PapayaHashMap<u64, u64, AHashRandomState>;
type SccAHashIndex = SccHashIndex<u64, u64, AHashRandomState>;
type SccAHashMap = SccHashMap<u64, u64, AHashRandomState>;

fn map_lookup_keys(entries: usize, operations: usize) -> Vec<u64> {
    debug_assert!(entries.is_power_of_two());
    (0..operations)
        .map(|index| ((index.wrapping_mul(1_103_515_245) + 12_345) & (entries - 1)) as u64)
        .collect()
}

fn populate_dashmap_current(entries: usize) -> DashMap<u64, u64> {
    let map = DashMap::with_capacity(entries);
    for key in 0..entries as u64 {
        map.insert(key, key);
    }
    map
}

fn populate_dashmap_ahash(entries: usize) -> DashAHashMap {
    let map = DashMap::with_capacity_and_hasher(entries, AHashRandomState::new());
    for key in 0..entries as u64 {
        map.insert(key, key);
    }
    map
}

fn populate_papaya_ahash(entries: usize) -> PapayaAHashMap {
    let map = PapayaHashMap::with_capacity_and_hasher(entries, AHashRandomState::new());
    {
        let pinned = map.pin();
        for key in 0..entries as u64 {
            pinned.insert(key, key);
        }
    }
    map
}

fn populate_scc_ahash(entries: usize) -> SccAHashMap {
    let map = SccHashMap::with_capacity_and_hasher(entries, AHashRandomState::new());
    for key in 0..entries as u64 {
        assert!(map.insert_sync(key, key).is_ok());
    }
    map
}

fn populate_scc_index_ahash(entries: usize) -> SccAHashIndex {
    let map = SccHashIndex::with_capacity_and_hasher(entries, AHashRandomState::new());
    for key in 0..entries as u64 {
        assert!(map.insert_sync(key, key).is_ok());
    }
    map
}

fn sum_dashmap_reads<S>(map: &DashMap<u64, u64, S>, keys: &[u64]) -> u64
where
    S: std::hash::BuildHasher + Clone,
{
    let mut sum = 0_u64;
    for key in keys {
        if let Some(value) = map.get(key) {
            sum = sum.wrapping_add(*value);
        }
    }
    sum
}

fn sum_papaya_reads(map: &PapayaAHashMap, keys: &[u64]) -> u64 {
    let pinned = map.pin();
    let mut sum = 0_u64;
    for key in keys {
        if let Some(value) = pinned.get(key) {
            sum = sum.wrapping_add(*value);
        }
    }
    sum
}

fn sum_scc_reads(map: &SccAHashMap, keys: &[u64]) -> u64 {
    let mut sum = 0_u64;
    for key in keys {
        if let Some(value) = map.read_sync(key, |_, value| *value) {
            sum = sum.wrapping_add(value);
        }
    }
    sum
}

fn sum_scc_index_reads(map: &SccAHashIndex, keys: &[u64]) -> u64 {
    let mut sum = 0_u64;
    for key in keys {
        if let Some(value) = map.read_sync(key, |_, value| *value) {
            sum = sum.wrapping_add(value);
        }
    }
    sum
}

fn sum_dashmap_reads_parallel<S>(map: &DashMap<u64, u64, S>, key_chunks: &[Vec<u64>]) -> u64
where
    S: std::hash::BuildHasher + Clone + Sync,
{
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(key_chunks.len());
        for keys in key_chunks {
            handles.push(scope.spawn(move || sum_dashmap_reads(map, keys)));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("benchmark reader thread"))
            .fold(0_u64, u64::wrapping_add)
    })
}

fn sum_papaya_reads_parallel(map: &PapayaAHashMap, key_chunks: &[Vec<u64>]) -> u64 {
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(key_chunks.len());
        for keys in key_chunks {
            handles.push(scope.spawn(move || sum_papaya_reads(map, keys)));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("benchmark reader thread"))
            .fold(0_u64, u64::wrapping_add)
    })
}

fn sum_scc_reads_parallel(map: &SccAHashMap, key_chunks: &[Vec<u64>]) -> u64 {
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(key_chunks.len());
        for keys in key_chunks {
            handles.push(scope.spawn(move || sum_scc_reads(map, keys)));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("benchmark reader thread"))
            .fold(0_u64, u64::wrapping_add)
    })
}

fn sum_scc_index_reads_parallel(map: &SccAHashIndex, key_chunks: &[Vec<u64>]) -> u64 {
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(key_chunks.len());
        for keys in key_chunks {
            handles.push(scope.spawn(move || sum_scc_index_reads(map, keys)));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("benchmark reader thread"))
            .fold(0_u64, u64::wrapping_add)
    })
}

fn bench_concurrent_map_candidates(c: &mut Criterion) {
    const ENTRIES: usize = 16_384;
    const OPERATIONS: usize = 65_536;
    const THREADS: usize = 4;

    let keys = map_lookup_keys(ENTRIES, OPERATIONS);
    let key_chunks = (0..THREADS)
        .map(|thread_index| {
            keys.iter()
                .skip(thread_index)
                .step_by(THREADS)
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let dashmap_current = populate_dashmap_current(ENTRIES);
    let dashmap_ahash = populate_dashmap_ahash(ENTRIES);
    let papaya_ahash = populate_papaya_ahash(ENTRIES);
    let scc_index_ahash = populate_scc_index_ahash(ENTRIES);
    let scc_ahash = populate_scc_ahash(ENTRIES);

    let mut read_group = c.benchmark_group("adapter_concurrent_map_read_hits");
    read_group.throughput(Throughput::Elements(OPERATIONS as u64));
    read_group.bench_function("current_dashmap_default", |b| {
        b.iter(|| black_box(sum_dashmap_reads(&dashmap_current, black_box(&keys))));
    });
    read_group.bench_function("dashmap_ahash", |b| {
        b.iter(|| black_box(sum_dashmap_reads(&dashmap_ahash, black_box(&keys))));
    });
    read_group.bench_function("candidate_papaya_ahash", |b| {
        b.iter(|| black_box(sum_papaya_reads(&papaya_ahash, black_box(&keys))));
    });
    read_group.bench_function("candidate_scc_ahash", |b| {
        b.iter(|| black_box(sum_scc_reads(&scc_ahash, black_box(&keys))));
    });
    read_group.bench_function("candidate_scc_hash_index_ahash", |b| {
        b.iter(|| black_box(sum_scc_index_reads(&scc_index_ahash, black_box(&keys))));
    });
    read_group.finish();

    let mut parallel_group = c.benchmark_group("adapter_concurrent_map_parallel_read_hits");
    parallel_group.throughput(Throughput::Elements(OPERATIONS as u64));
    parallel_group.bench_function("current_dashmap_default", |b| {
        b.iter(|| {
            black_box(sum_dashmap_reads_parallel(
                &dashmap_current,
                black_box(&key_chunks),
            ))
        });
    });
    parallel_group.bench_function("dashmap_ahash", |b| {
        b.iter(|| {
            black_box(sum_dashmap_reads_parallel(
                &dashmap_ahash,
                black_box(&key_chunks),
            ))
        });
    });
    parallel_group.bench_function("candidate_papaya_ahash", |b| {
        b.iter(|| {
            black_box(sum_papaya_reads_parallel(
                &papaya_ahash,
                black_box(&key_chunks),
            ))
        });
    });
    parallel_group.bench_function("candidate_scc_ahash", |b| {
        b.iter(|| black_box(sum_scc_reads_parallel(&scc_ahash, black_box(&key_chunks))));
    });
    parallel_group.bench_function("candidate_scc_hash_index_ahash", |b| {
        b.iter(|| {
            black_box(sum_scc_index_reads_parallel(
                &scc_index_ahash,
                black_box(&key_chunks),
            ))
        });
    });
    parallel_group.finish();

    let mut mixed_group = c.benchmark_group("adapter_concurrent_map_mixed_read_write");
    mixed_group.throughput(Throughput::Elements(OPERATIONS as u64));
    mixed_group.bench_function("current_dashmap_default", |b| {
        b.iter(|| {
            let mut sum = 0_u64;
            for (index, key) in keys.iter().enumerate() {
                let key = *key;
                if index % 10 == 0 {
                    dashmap_current.insert(key, key.wrapping_add(index as u64));
                } else if let Some(value) = dashmap_current.get(&key) {
                    sum = sum.wrapping_add(*value);
                }
            }
            black_box(sum)
        });
    });
    mixed_group.bench_function("dashmap_ahash", |b| {
        b.iter(|| {
            let mut sum = 0_u64;
            for (index, key) in keys.iter().enumerate() {
                let key = *key;
                if index % 10 == 0 {
                    dashmap_ahash.insert(key, key.wrapping_add(index as u64));
                } else if let Some(value) = dashmap_ahash.get(&key) {
                    sum = sum.wrapping_add(*value);
                }
            }
            black_box(sum)
        });
    });
    mixed_group.bench_function("candidate_papaya_ahash", |b| {
        b.iter(|| {
            let pinned = papaya_ahash.pin();
            let mut sum = 0_u64;
            for (index, key) in keys.iter().enumerate() {
                let key = *key;
                if index % 10 == 0 {
                    pinned.insert(key, key.wrapping_add(index as u64));
                } else if let Some(value) = pinned.get(&key) {
                    sum = sum.wrapping_add(*value);
                }
            }
            black_box(sum)
        });
    });
    mixed_group.bench_function("candidate_scc_ahash", |b| {
        b.iter(|| {
            let mut sum = 0_u64;
            for (index, key) in keys.iter().enumerate() {
                let key = *key;
                if index % 10 == 0 {
                    if let Some(value) = scc_ahash.update_sync(&key, |_, value| {
                        *value = key.wrapping_add(index as u64);
                        *value
                    }) {
                        sum = sum.wrapping_add(value);
                    }
                } else if let Some(value) = scc_ahash.read_sync(&key, |_, value| *value) {
                    sum = sum.wrapping_add(value);
                }
            }
            black_box(sum)
        });
    });
    mixed_group.bench_function("candidate_scc_hash_index_ahash", |b| {
        b.iter(|| {
            let mut sum = 0_u64;
            for (index, key) in keys.iter().enumerate() {
                let key = *key;
                if index % 10 == 0 {
                    let mut entry = scc_index_ahash.entry_sync(key).or_insert(0);
                    entry.update(key.wrapping_add(index as u64));
                    sum = sum.wrapping_add(*entry.get());
                } else if let Some(value) = scc_index_ahash.read_sync(&key, |_, value| *value) {
                    sum = sum.wrapping_add(value);
                }
            }
            black_box(sum)
        });
    });
    mixed_group.finish();

    let mut insert_group = c.benchmark_group("adapter_concurrent_map_insert_cold");
    insert_group.throughput(Throughput::Elements(ENTRIES as u64));
    insert_group.bench_function("current_dashmap_default", |b| {
        b.iter_batched(
            || map_lookup_keys(ENTRIES, ENTRIES),
            |insert_keys| {
                let map = DashMap::with_capacity(ENTRIES);
                for key in insert_keys {
                    map.insert(key, key);
                }
                black_box(map.len())
            },
            criterion::BatchSize::SmallInput,
        );
    });
    insert_group.bench_function("dashmap_ahash", |b| {
        b.iter_batched(
            || map_lookup_keys(ENTRIES, ENTRIES),
            |insert_keys| {
                let map = DashMap::with_capacity_and_hasher(ENTRIES, AHashRandomState::new());
                for key in insert_keys {
                    map.insert(key, key);
                }
                black_box(map.len())
            },
            criterion::BatchSize::SmallInput,
        );
    });
    insert_group.bench_function("candidate_papaya_ahash", |b| {
        b.iter_batched(
            || map_lookup_keys(ENTRIES, ENTRIES),
            |insert_keys| {
                let map = PapayaHashMap::with_capacity_and_hasher(ENTRIES, AHashRandomState::new());
                {
                    let pinned = map.pin();
                    for key in insert_keys {
                        pinned.insert(key, key);
                    }
                }
                black_box(map.len())
            },
            criterion::BatchSize::SmallInput,
        );
    });
    insert_group.bench_function("candidate_scc_ahash", |b| {
        b.iter_batched(
            || map_lookup_keys(ENTRIES, ENTRIES),
            |insert_keys| {
                let map = SccHashMap::with_capacity_and_hasher(ENTRIES, AHashRandomState::new());
                for key in insert_keys {
                    let _ = map.insert_sync(key, key);
                }
                black_box(map.len())
            },
            criterion::BatchSize::SmallInput,
        );
    });
    insert_group.bench_function("candidate_scc_hash_index_ahash", |b| {
        b.iter_batched(
            || map_lookup_keys(ENTRIES, ENTRIES),
            |insert_keys| {
                let map = SccHashIndex::with_capacity_and_hasher(ENTRIES, AHashRandomState::new());
                for key in insert_keys {
                    let _ = map.insert_sync(key, key);
                }
                black_box(map.len())
            },
            criterion::BatchSize::SmallInput,
        );
    });
    insert_group.finish();
}

criterion_group!(
    benches,
    bench_base_message_hash,
    bench_presence_stagger_hash,
    bench_replay_lock_store,
    bench_compact_string_candidates,
    bench_concurrent_map_candidates
);
criterion_main!(benches);
