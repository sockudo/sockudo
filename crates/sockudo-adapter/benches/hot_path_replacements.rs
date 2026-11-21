use ahash::{AHashMap, AHasher, RandomState as AHashRandomState};
use bytes::Bytes;
use compact_str::{CompactString, format_compact};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use dashmap::DashMap;
use papaya::HashMap as PapayaHashMap;
use parking_lot::Mutex as ParkingMutex;
use scc::{HashIndex as SccHashIndex, HashMap as SccHashMap};
use sockudo_protocol::WireFormat;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::wire::serialize_message;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex as StdMutex, Once};
use std::thread;

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static FANOUT_ALLOCATION_REPORT: Once = Once::new();
static WIRE_FORMAT_ALLOCATION_REPORT: Once = Once::new();

// SAFETY: every allocation operation delegates to the process system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller provided a valid allocation layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() && TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout came from the delegated system allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the pointer and layout came from the delegated system allocator.
        let pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !pointer.is_null() && TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        pointer
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn measure_allocations<T>(operation: impl FnOnce() -> T) -> (T, u64, u64) {
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    ALLOCATION_COUNT.store(0, Ordering::SeqCst);
    ALLOCATED_BYTES.store(0, Ordering::SeqCst);
    TRACK_ALLOCATIONS.store(true, Ordering::SeqCst);
    let result = operation();
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    (
        result,
        ALLOCATION_COUNT.load(Ordering::SeqCst),
        ALLOCATED_BYTES.load(Ordering::SeqCst),
    )
}

fn fanout_message(payload_bytes: usize) -> PusherMessage {
    PusherMessage::channel_event(
        "message",
        "bench-channel",
        sonic_rs::json!({"payload": "x".repeat(payload_bytes)}),
    )
}

fn legacy_protocol_fanout(message: &PusherMessage, sockets: usize) -> usize {
    (0..sockets)
        .map(|_| {
            let socket_message = message.clone();
            sonic_rs::to_vec(&socket_message).unwrap().len()
        })
        .sum()
}

fn serialized_bytes_fanout(message: &PusherMessage, sockets: usize) -> usize {
    let bytes = Bytes::from(sonic_rs::to_vec(message).unwrap());
    (0..sockets)
        .map(|_| {
            let socket_bytes = bytes.clone();
            socket_bytes.len()
        })
        .sum()
}

/// Per-socket encode with the production serializer (includes the MessagePack
/// `PusherMessage` clone + conversion).
fn legacy_wire_fanout(message: &PusherMessage, wire_format: WireFormat, sockets: usize) -> usize {
    (0..sockets)
        .map(|_| serialize_message(message, wire_format).unwrap().len())
        .sum()
}

/// Encode once per broadcast, then hand every socket a refcounted `Bytes`.
fn shared_wire_fanout(message: &PusherMessage, wire_format: WireFormat, sockets: usize) -> usize {
    let bytes = Bytes::from(serialize_message(message, wire_format).unwrap());
    (0..sockets).map(|_| bytes.clone().len()).sum()
}

fn bench_wire_format_fanout(c: &mut Criterion) {
    WIRE_FORMAT_ALLOCATION_REPORT.call_once(|| {
        let message = fanout_message(64 * 1024);
        for (label, wire_format) in [
            ("json", WireFormat::Json),
            ("msgpack", WireFormat::MessagePack),
        ] {
            let (_, legacy_allocations, legacy_bytes) =
                measure_allocations(|| legacy_wire_fanout(&message, wire_format, 1_000));
            let (_, shared_allocations, shared_bytes) =
                measure_allocations(|| shared_wire_fanout(&message, wire_format, 1_000));
            eprintln!(
                "allocation_profile name=adapter_wire_format_fanout_1000x64k variant={label}_legacy allocations={legacy_allocations} allocated_bytes={legacy_bytes}"
            );
            eprintln!(
                "allocation_profile name=adapter_wire_format_fanout_1000x64k variant={label}_shared allocations={shared_allocations} allocated_bytes={shared_bytes}"
            );
        }
    });

    let mut group = c.benchmark_group("adapter_wire_format_fanout");
    for (label, wire_format) in [
        ("json", WireFormat::Json),
        ("msgpack", WireFormat::MessagePack),
    ] {
        for payload_bytes in [1_024_usize, 64 * 1_024] {
            let message = fanout_message(payload_bytes);
            for sockets in [1_usize, 100, 1_000] {
                group.throughput(Throughput::Elements(sockets as u64));
                group.bench_with_input(
                    BenchmarkId::new(format!("{label}_legacy_{payload_bytes}b"), sockets),
                    &sockets,
                    |b, &sockets| {
                        b.iter(|| {
                            black_box(legacy_wire_fanout(
                                black_box(&message),
                                wire_format,
                                black_box(sockets),
                            ))
                        })
                    },
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("{label}_shared_{payload_bytes}b"), sockets),
                    &sockets,
                    |b, &sockets| {
                        b.iter(|| {
                            black_box(shared_wire_fanout(
                                black_box(&message),
                                wire_format,
                                black_box(sockets),
                            ))
                        })
                    },
                );
            }
        }
    }
    group.finish();
}

fn bench_serialized_fanout(c: &mut Criterion) {
    FANOUT_ALLOCATION_REPORT.call_once(|| {
        let message = fanout_message(64 * 1024);
        let (_, legacy_allocations, legacy_bytes) =
            measure_allocations(|| legacy_protocol_fanout(&message, 1_000));
        let (_, shared_allocations, shared_bytes) =
            measure_allocations(|| serialized_bytes_fanout(&message, 1_000));
        eprintln!(
            "allocation_profile name=adapter_v2_fanout_1000x64k variant=legacy allocations={legacy_allocations} allocated_bytes={legacy_bytes}"
        );
        eprintln!(
            "allocation_profile name=adapter_v2_fanout_1000x64k variant=shared_bytes allocations={shared_allocations} allocated_bytes={shared_bytes}"
        );
    });

    let mut group = c.benchmark_group("adapter_v2_serialized_fanout");
    for payload_bytes in [1_024_usize, 64 * 1_024] {
        let message = fanout_message(payload_bytes);
        for sockets in [1_usize, 100, 1_000] {
            group.throughput(Throughput::Elements(sockets as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("legacy_{payload_bytes}b"), sockets),
                &sockets,
                |b, &sockets| {
                    b.iter(|| {
                        black_box(legacy_protocol_fanout(
                            black_box(&message),
                            black_box(sockets),
                        ))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("shared_bytes_{payload_bytes}b"), sockets),
                &sockets,
                |b, &sockets| {
                    b.iter(|| {
                        black_box(serialized_bytes_fanout(
                            black_box(&message),
                            black_box(sockets),
                        ))
                    })
                },
            );
        }
    }
    group.finish();
}

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

#[derive(Clone, Copy)]
struct CountConsumers {
    channel_occupied_webhook: bool,
    channel_vacated_webhook: bool,
    subscription_count_webhook: bool,
    meta_channel: bool,
}

fn subscription_count_payload(channel: &str, count: usize) -> sonic_rs::Value {
    sonic_rs::json!({
        "channel": channel,
        "subscription_count": count,
    })
}

fn legacy_join_count_work(channels: &[String], consumers: CountConsumers) -> usize {
    let has_webhook = consumers.channel_occupied_webhook
        || consumers.channel_vacated_webhook
        || consumers.subscription_count_webhook;
    if !has_webhook && !consumers.meta_channel {
        return 0;
    }

    let mut work = 0;
    for (index, channel) in channels.iter().enumerate() {
        let current_count = if index % 4 == 0 { 1 } else { 2 };

        if current_count == 1 {
            if consumers.channel_occupied_webhook {
                work += 1;
            }
            black_box(subscription_count_payload(channel, current_count));
            work += 1;
        }

        black_box(subscription_count_payload(channel, current_count));
        work += 1;

        if consumers.subscription_count_webhook {
            work += 1;
        }
    }
    work
}

fn optimized_join_count_work(channels: &[String], consumers: CountConsumers) -> usize {
    if !consumers.channel_occupied_webhook
        && !consumers.subscription_count_webhook
        && !consumers.meta_channel
    {
        return 0;
    }

    let mut work = 0;
    for (index, channel) in channels.iter().enumerate() {
        let current_count = if index % 4 == 0 { 1 } else { 2 };

        if current_count == 1 && consumers.channel_occupied_webhook {
            work += 1;
        }

        if consumers.meta_channel {
            if current_count == 1 {
                black_box(subscription_count_payload(channel, current_count));
                work += 1;
            }
            black_box(subscription_count_payload(channel, current_count));
            work += 1;
        }

        if consumers.subscription_count_webhook {
            work += 1;
        }
    }
    work
}

fn legacy_leave_count_work(channels: &[String], consumers: CountConsumers) -> usize {
    let has_webhook = consumers.channel_occupied_webhook
        || consumers.channel_vacated_webhook
        || consumers.subscription_count_webhook;
    if !has_webhook && !consumers.meta_channel {
        return 0;
    }

    let mut work = 0;
    for (index, channel) in channels.iter().enumerate() {
        let current_count = if index % 4 == 0 { 0 } else { 1 };

        if consumers.subscription_count_webhook {
            work += 1;
        }

        black_box(subscription_count_payload(channel, current_count));
        work += 1;

        if current_count == 0 && consumers.channel_vacated_webhook {
            work += 1;
        }
    }
    work
}

fn optimized_leave_count_work(channels: &[String], consumers: CountConsumers) -> usize {
    if !consumers.channel_vacated_webhook
        && !consumers.subscription_count_webhook
        && !consumers.meta_channel
    {
        return 0;
    }

    let mut work = 0;
    for (index, channel) in channels.iter().enumerate() {
        let current_count = if index % 4 == 0 { 0 } else { 1 };

        if consumers.subscription_count_webhook {
            work += 1;
        }

        if consumers.meta_channel {
            black_box(subscription_count_payload(channel, current_count));
            work += 1;
        }

        if current_count == 0 && consumers.channel_vacated_webhook {
            work += 1;
        }
    }
    work
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

fn bench_subscription_count_notification_plans(c: &mut Criterion) {
    const ITEMS: usize = 4096;
    let channels = long_channel_names(ITEMS);
    let webhook_only_occupancy = CountConsumers {
        channel_occupied_webhook: true,
        channel_vacated_webhook: true,
        subscription_count_webhook: false,
        meta_channel: false,
    };
    let webhook_and_meta_occupancy = CountConsumers {
        meta_channel: true,
        ..webhook_only_occupancy
    };

    let mut group = c.benchmark_group("adapter_subscription_count_notification_plans");
    group.throughput(Throughput::Elements(ITEMS as u64));
    group.bench_function("legacy_webhook_only_join", |b| {
        b.iter(|| black_box(legacy_join_count_work(&channels, webhook_only_occupancy)));
    });
    group.bench_function("optimized_webhook_only_join", |b| {
        b.iter(|| black_box(optimized_join_count_work(&channels, webhook_only_occupancy)));
    });
    group.bench_function("legacy_webhook_only_leave", |b| {
        b.iter(|| black_box(legacy_leave_count_work(&channels, webhook_only_occupancy)));
    });
    group.bench_function("optimized_webhook_only_leave", |b| {
        b.iter(|| {
            black_box(optimized_leave_count_work(
                &channels,
                webhook_only_occupancy,
            ))
        });
    });
    group.bench_function("legacy_webhook_and_meta_join", |b| {
        b.iter(|| {
            black_box(legacy_join_count_work(
                &channels,
                webhook_and_meta_occupancy,
            ))
        });
    });
    group.bench_function("optimized_webhook_and_meta_join", |b| {
        b.iter(|| {
            black_box(optimized_join_count_work(
                &channels,
                webhook_and_meta_occupancy,
            ))
        });
    });
    group.bench_function("legacy_webhook_and_meta_leave", |b| {
        b.iter(|| {
            black_box(legacy_leave_count_work(
                &channels,
                webhook_and_meta_occupancy,
            ))
        });
    });
    group.bench_function("optimized_webhook_and_meta_leave", |b| {
        b.iter(|| {
            black_box(optimized_leave_count_work(
                &channels,
                webhook_and_meta_occupancy,
            ))
        });
    });
    group.finish();
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
    bench_concurrent_map_candidates,
    bench_subscription_count_notification_plans,
    bench_serialized_fanout,
    bench_wire_format_fanout
);
criterion_main!(benches);
