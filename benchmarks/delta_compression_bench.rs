// Benchmarks for delta compression bandwidth and performance
//
// Compares Fossil Delta and Xdelta3 algorithms
// Run with: cargo bench --bench delta_compression_bench

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sockudo::delta_compression::{DeltaAlgorithm, DeltaCompressionConfig, DeltaCompressionManager};
use sockudo::websocket::SocketId;
use std::hint::black_box;

fn create_json_message(value: u32, size: usize) -> Vec<u8> {
    // Create a JSON-like payload where 10 numeric fields change between messages
    // to benchmark delta compression when multiple fields change.
    let base = format!(
        r#"{{"event":"test","data":{{"v1":{},"v2":{},"v3":{},"v4":{},"v5":{},"v6":{},"v7":{},"v8":{},"v9":{},"v10":{},"#,
        value,
        value.wrapping_add(1),
        value.wrapping_add(2),
        value.wrapping_add(3),
        value.wrapping_add(4),
        value.wrapping_add(5),
        value.wrapping_add(6),
        value.wrapping_add(7),
        value.wrapping_add(8),
        value.wrapping_add(9)
    );
    let padding = "x".repeat(size.saturating_sub(base.len() + 10));
    format!(r#"{}"padding":"{}"}}}}}}"#, base, padding).into_bytes()
}

fn benchmark_algorithm_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm_comparison");

    let algorithms = [
        ("fossil", DeltaAlgorithm::Fossil),
        ("xdelta3", DeltaAlgorithm::Xdelta3),
    ];

    // Test different message sizes
    for size in [100, 500, 1000, 5000].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        // Baseline: no compression
        group.bench_with_input(
            BenchmarkId::new("no_compression", size),
            size,
            |b, &size| {
                let message = create_json_message(123, size);
                b.iter(|| {
                    // Just access the message without any compression
                    black_box(&message);
                });
            },
        );

        for (algo_name, algorithm) in &algorithms {
            let config = DeltaCompressionConfig {
                algorithm: *algorithm,
                min_message_size: 50,
                ..Default::default()
            };
            let manager = DeltaCompressionManager::new(config);
            let socket_id = SocketId::new();
            manager.enable_for_socket(&socket_id);

            // Benchmark first message (full message)
            group.bench_with_input(
                BenchmarkId::new(format!("{}_full_message", algo_name), size),
                size,
                |b, &size| {
                    let message = create_json_message(123, size);
                    b.iter(|| {
                        let _ = manager.compress_message(
                            black_box(&socket_id),
                            black_box(&format!("test-channel-{}", algo_name)),
                            black_box("test-event"),
                            black_box(&message),
                            None,
                        );
                    });
                },
            );

            // Benchmark delta message
            group.bench_with_input(
                BenchmarkId::new(format!("{}_delta_message", algo_name), size),
                size,
                |b, &size| {
                    let message1 = create_json_message(123, size);
                    let message2 = create_json_message(456, size);

                    // Setup: send first message
                    let _ = manager.compress_message(
                        &socket_id,
                        &format!("bench-channel-{}", algo_name),
                        "test",
                        &message1,
                        None,
                    );

                    b.iter(|| {
                        let _ = manager.compress_message(
                            black_box(&socket_id),
                            black_box(&format!("bench-channel-{}", algo_name)),
                            black_box("test"),
                            black_box(&message2),
                            None,
                        );
                    });
                },
            );
        }
    }

    group.finish();
}

fn benchmark_compression_ratio_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_ratio_comparison");
    group.sample_size(50); // Reduce sample size for faster benchmarking

    let algorithms = [
        ("fossil", DeltaAlgorithm::Fossil),
        ("xdelta3", DeltaAlgorithm::Xdelta3),
    ];

    for size in [500, 1000, 2000].iter() {
        // Baseline: no compression
        group.bench_function(format!("no_compression_ratio_{}", size), |b| {
            let _message1 = create_json_message(123, *size);
            let message2 = create_json_message(456, *size);
            b.iter(|| {
                // Simulate sending full message every time (no delta)
                black_box(&message2);
            });
        });
        for (algo_name, algorithm) in &algorithms {
            let config = DeltaCompressionConfig {
                algorithm: *algorithm,
                min_message_size: 50,
                ..Default::default()
            };
            let manager = DeltaCompressionManager::new(config);
            let socket_id = SocketId::new();
            manager.enable_for_socket(&socket_id);

            group.bench_function(format!("{}_ratio_{}", algo_name, size), |b| {
                let message1 = create_json_message(123, *size);
                let message2 = create_json_message(456, *size);

                // Prime the compression
                let _ = manager.compress_message(
                    &socket_id,
                    &format!("ratio-test-{}", algo_name),
                    "test",
                    &message1,
                    None,
                );

                b.iter(|| {
                    // Note: compress_message is async, so we just measure the call overhead
                    // For actual compression ratio tests, use the integration tests
                    let _ = manager.compress_message(
                        black_box(&socket_id),
                        black_box(&format!("ratio-test-{}", algo_name)),
                        black_box("test"),
                        black_box(&message2),
                        None,
                    );
                });
            });
        }
    }

    group.finish();
}

fn benchmark_bandwidth_savings(c: &mut Criterion) {
    let mut group = c.benchmark_group("bandwidth_savings");

    let algorithms = [
        ("fossil", DeltaAlgorithm::Fossil),
        ("xdelta3", DeltaAlgorithm::Xdelta3),
    ];

    for (algo_name, algorithm) in &algorithms {
        let config = DeltaCompressionConfig {
            algorithm: *algorithm,
            min_message_size: 50,
            full_message_interval: 100, // Test long delta chains
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();
        manager.enable_for_socket(&socket_id);

        // Simulate realistic message stream
        let base_message = create_json_message(0, 1000);

        group.bench_function(format!("{}_message_stream", algo_name), |b| {
            let mut value = 0u32;
            // Send initial message
            let _ = manager.compress_message(
                &socket_id,
                &format!("stream-test-{}", algo_name),
                "test",
                &base_message,
                None,
            );

            b.iter(|| {
                value = value.wrapping_add(1);
                let msg = create_json_message(value, 1000);
                let _ = manager.compress_message(
                    black_box(&socket_id),
                    black_box(&format!("stream-test-{}", algo_name)),
                    black_box("test"),
                    black_box(&msg),
                    None,
                );
            });
        });
    }

    // Baseline: no compression
    group.bench_function("no_compression_baseline", |b| {
        let mut value = 0u32;
        b.iter(|| {
            value = value.wrapping_add(1);
            let msg = create_json_message(value, 1000);
            black_box(&msg);
        });
    });

    group.finish();
}

fn benchmark_small_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("small_changes");

    let algorithms = [
        ("fossil", DeltaAlgorithm::Fossil),
        ("xdelta3", DeltaAlgorithm::Xdelta3),
    ];

    // Baseline: no compression
    group.bench_function("no_compression_single_field_change", |b| {
        let _message1 = br#"{"event":"test","data":{"counter":0,"payload":"long_string_that_stays_the_same_across_messages_to_test_delta_efficiency"}}"#.to_vec();
        let message2 = br#"{"event":"test","data":{"counter":1,"payload":"long_string_that_stays_the_same_across_messages_to_test_delta_efficiency"}}"#.to_vec();
        b.iter(|| {
            black_box(&message2);
        });
    });

    // Test scenario: only one field changes (ideal for delta compression)
    for (algo_name, algorithm) in &algorithms {
        let config = DeltaCompressionConfig {
            algorithm: *algorithm,
            min_message_size: 50,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();
        manager.enable_for_socket(&socket_id);

        group.bench_function(format!("{}_single_field_change", algo_name), |b| {
            let message1 = br#"{"event":"test","data":{"counter":0,"payload":"long_string_that_stays_the_same_across_messages_to_test_delta_efficiency"}}"#.to_vec();
            let message2 = br#"{"event":"test","data":{"counter":1,"payload":"long_string_that_stays_the_same_across_messages_to_test_delta_efficiency"}}"#.to_vec();

            // Prime the compression
            let _ = manager.compress_message(
                &socket_id,
                &format!("small-change-{}", algo_name),
                "test",
                &message1,
                None,
            );

            b.iter(|| {
                let _ = manager.compress_message(
                    black_box(&socket_id),
                    black_box(&format!("small-change-{}", algo_name)),
                    black_box("test"),
                    black_box(&message2),
                    None,
                );
            });
        });
    }

    group.finish();
}

fn benchmark_large_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_changes");

    let algorithms = [
        ("fossil", DeltaAlgorithm::Fossil),
        ("xdelta3", DeltaAlgorithm::Xdelta3),
    ];

    // Baseline: no compression
    group.bench_function("no_compression_all_fields_change", |b| {
        let _message1 = create_json_message(0, 1000);
        let message2 = create_json_message(999999, 1000);
        b.iter(|| {
            black_box(&message2);
        });
    });

    // Test scenario: most fields change (worst case for delta compression)
    for (algo_name, algorithm) in &algorithms {
        let config = DeltaCompressionConfig {
            algorithm: *algorithm,
            min_message_size: 50,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();
        manager.enable_for_socket(&socket_id);

        group.bench_function(format!("{}_all_fields_change", algo_name), |b| {
            let message1 = create_json_message(0, 1000);
            let message2 = create_json_message(999999, 1000); // Completely different values

            // Prime the compression
            let _ = manager.compress_message(
                &socket_id,
                &format!("large-change-{}", algo_name),
                "test",
                &message1,
                None,
            );

            b.iter(|| {
                let _ = manager.compress_message(
                    black_box(&socket_id),
                    black_box(&format!("large-change-{}", algo_name)),
                    black_box("test"),
                    black_box(&message2),
                    None,
                );
            });
        });
    }

    group.finish();
}

fn benchmark_concurrent_sockets(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_sockets");

    let algorithms = [
        ("fossil", DeltaAlgorithm::Fossil),
        ("xdelta3", DeltaAlgorithm::Xdelta3),
    ];

    for num_sockets in [1, 10, 100].iter() {
        // Baseline: no compression
        group.bench_with_input(
            BenchmarkId::new("no_compression_parallel", num_sockets),
            num_sockets,
            |b, &num_sockets| {
                let message = create_json_message(123, 500);
                b.iter(|| {
                    for _ in 0..num_sockets {
                        black_box(&message);
                    }
                });
            },
        );
        for (algo_name, algorithm) in &algorithms {
            let config = DeltaCompressionConfig {
                algorithm: *algorithm,
                min_message_size: 50,
                ..Default::default()
            };
            let manager = DeltaCompressionManager::new(config);

            let sockets: Vec<SocketId> = (0..*num_sockets)
                .map(|_| {
                    let id = SocketId::new();
                    manager.enable_for_socket(&id);
                    id
                })
                .collect();

            group.bench_with_input(
                BenchmarkId::new(format!("{}_compress_parallel", algo_name), num_sockets),
                num_sockets,
                |b, _| {
                    let message = create_json_message(123, 500);
                    b.iter(|| {
                        for socket_id in &sockets {
                            let _ = manager.compress_message(
                                black_box(socket_id),
                                black_box(&format!("concurrent-test-{}", algo_name)),
                                black_box("test"),
                                black_box(&message),
                                None,
                            );
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_algorithm_comparison,
    benchmark_compression_ratio_comparison,
    benchmark_bandwidth_savings,
    benchmark_small_changes,
    benchmark_large_changes,
    benchmark_concurrent_sockets,
);
criterion_main!(benches);
