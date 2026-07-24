use criterion::{Criterion, criterion_group, criterion_main};
use dashmap::DashMap;
use sockudo_core::presence_registry::{PresenceRecord, PresenceRegistry};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

type LegacyKey = (String, String, String, String);

fn member(connection_id: &str, client_id: &str, serial: u64) -> PresenceRecord {
    PresenceRecord {
        connection_id: connection_id.to_string(),
        client_id: client_id.to_string(),
        id: format!("{connection_id}:{serial}:0"),
        data: Some(sonic_rs::json!({ "serial": serial })),
        encoding: Some("json".to_string()),
        extras: None,
        timestamp_ms: i64::try_from(serial).unwrap_or(i64::MAX),
    }
}

fn bench_presence_registry(c: &mut Criterion) {
    let legacy = DashMap::<LegacyKey, PresenceRecord>::new();
    let registry = PresenceRegistry::new(10_000);
    registry.register_connection("app", "connection");

    let mut group = c.benchmark_group("presence_member_cycle");
    group.bench_function("legacy_facade_dashmap", |b| {
        b.iter(|| {
            let key = (
                "app".to_string(),
                "room".to_string(),
                "connection".to_string(),
                "client".to_string(),
            );
            legacy.insert(key.clone(), member("connection", "client", 1));
            black_box(legacy.remove(&key));
        });
    });
    group.bench_function("native_authority", |b| {
        b.iter(|| {
            registry
                .enter("app", "room", member("connection", "client", 1))
                .expect("registered connection");
            black_box(
                registry
                    .leave("app", "room", "connection", "client")
                    .expect("leave"),
            );
        });
    });
    group.finish();

    let snapshot_registry = PresenceRegistry::new(1_000);
    for index in 0..110 {
        let connection_id = format!("connection-{index:03}");
        let client_id = format!("client-{index:03}");
        snapshot_registry.register_connection("app", &connection_id);
        snapshot_registry
            .enter("app", "room", member(&connection_id, &client_id, 1))
            .expect("seed member");
    }
    c.bench_function("presence_snapshot_110_sorted", |b| {
        b.iter(|| black_box(snapshot_registry.snapshot("app", "room")));
    });

    const PARALLEL_WORKERS: usize = 8;
    let parallel_legacy = Arc::new(DashMap::<LegacyKey, PresenceRecord>::new());
    let parallel_legacy_identities = (0..PARALLEL_WORKERS)
        .map(|index| {
            (
                format!("parallel-connection-{index}"),
                format!("parallel-client-{index}"),
            )
        })
        .collect::<Vec<_>>();
    c.bench_function("presence_parallel_cycle/legacy_disjoint_clients_8", |b| {
        b.iter_custom(|iterations| {
            let per_worker = iterations / PARALLEL_WORKERS as u64;
            let remainder = iterations % PARALLEL_WORKERS as u64;
            let started = Instant::now();
            std::thread::scope(|scope| {
                for (worker, (connection_id, client_id)) in
                    parallel_legacy_identities.iter().enumerate()
                {
                    let legacy = Arc::clone(&parallel_legacy);
                    let cycles = per_worker + u64::from((worker as u64) < remainder);
                    scope.spawn(move || {
                        for serial in 0..cycles {
                            let key = (
                                "app".to_string(),
                                "room".to_string(),
                                connection_id.clone(),
                                client_id.clone(),
                            );
                            legacy.insert(key.clone(), member(connection_id, client_id, serial));
                            black_box(legacy.remove(&key));
                        }
                    });
                }
            });
            let elapsed = started.elapsed();
            assert!(parallel_legacy.is_empty());
            elapsed
        });
    });

    for (name, shared_client) in [
        ("presence_parallel_cycle/disjoint_clients_8", false),
        ("presence_parallel_cycle/shared_client_8", true),
    ] {
        let registry = Arc::new(PresenceRegistry::new(10_000));
        let identities = (0..PARALLEL_WORKERS)
            .map(|index| {
                let connection_id = format!("parallel-connection-{index}");
                let client_id = format!("parallel-client-{index}");
                registry.register_connection("app", &connection_id);
                (connection_id, client_id)
            })
            .collect::<Vec<_>>();
        c.bench_function(name, |b| {
            b.iter_custom(|iterations| {
                let per_worker = iterations / PARALLEL_WORKERS as u64;
                let remainder = iterations % PARALLEL_WORKERS as u64;
                let started = Instant::now();
                std::thread::scope(|scope| {
                    for (worker, (connection_id, client_id)) in identities.iter().enumerate() {
                        let registry = Arc::clone(&registry);
                        let cycles = per_worker + u64::from((worker as u64) < remainder);
                        scope.spawn(move || {
                            let client_id = if shared_client {
                                "shared-client"
                            } else {
                                client_id
                            };
                            for serial in 0..cycles {
                                registry
                                    .enter("app", "room", member(connection_id, client_id, serial))
                                    .expect("registered connection");
                                black_box(
                                    registry
                                        .leave("app", "room", connection_id, client_id)
                                        .expect("leave"),
                                );
                            }
                        });
                    }
                });
                let elapsed = started.elapsed();
                assert_eq!(registry.member_count(), 0);
                elapsed
            });
        });
    }
}

criterion_group!(benches, bench_presence_registry);
criterion_main!(benches);
