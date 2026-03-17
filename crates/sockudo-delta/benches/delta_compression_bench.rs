use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn make_payload(symbol: &str, seq: u32, price: f64, volume: f64) -> String {
    format!(
        "{{\"symbol\":\"{}\",\"seq\":{},\"price\":{:.6},\"volume\":{:.4},\"bid\":{:.6},\"ask\":{:.6}}}",
        symbol,
        seq,
        price,
        volume,
        price - 0.01,
        price + 0.01
    )
}

fn bench_fossil_delta(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta_compression");

    for size in ["small", "medium", "large"] {
        let (base, next) = match size {
            "small" => (
                make_payload("BTC", 1000, 50000.1234, 10.0),
                make_payload("BTC", 1001, 50000.2234, 10.2),
            ),
            "medium" => {
                let mut base = String::new();
                let mut next = String::new();
                for i in 0..40 {
                    base.push_str(&make_payload("ETH", 2000 + i, 3000.0 + i as f64, 8.0));
                    next.push_str(&make_payload("ETH", 2001 + i, 3000.1 + i as f64, 8.1));
                }
                (base, next)
            }
            _ => {
                let mut base = String::new();
                let mut next = String::new();
                for i in 0..200 {
                    base.push_str(&make_payload("SOL", 5000 + i, 120.0 + i as f64, 4.0));
                    next.push_str(&make_payload("SOL", 5001 + i, 120.05 + i as f64, 4.1));
                }
                (base, next)
            }
        };

        group.bench_with_input(
            BenchmarkId::new("fossil", size),
            &(&base, &next),
            |b, (old_msg, new_msg)| {
                b.iter(|| {
                    // Keep argument order aligned with server implementation: delta(new, old)
                    let d = fossil_delta::delta(black_box(new_msg), black_box(old_msg));
                    black_box(d.len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_fossil_delta);
criterion_main!(benches);
