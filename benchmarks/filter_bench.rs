use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use sockudo::filter::matches;
use sockudo::filter::node::FilterNodeBuilder;

use ahash::AHashMap;
use std::hint::black_box;

fn create_tags(pairs: &[(&str, &str)]) -> AHashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// Simple filter benchmarks
fn bench_simple_eq_filter(c: &mut Criterion) {
    let filter = FilterNodeBuilder::eq("event_type", "goal");
    let tags = create_tags(&[("event_type", "goal")]);

    c.bench_function("filter/simple_eq/single", |b| {
        b.iter(|| matches(black_box(&filter), black_box(&tags)))
    });
}

fn bench_simple_eq_filter_10k(c: &mut Criterion) {
    let filter = FilterNodeBuilder::eq("event_type", "goal");
    let tags = create_tags(&[("event_type", "goal")]);

    c.bench_function("filter/simple_eq/10k", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                let _ = matches(black_box(&filter), black_box(&tags));
            }
        })
    });
}

// Complex filter benchmarks
fn bench_complex_filter(c: &mut Criterion) {
    let filter = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("event_type", "shot"),
            FilterNodeBuilder::gte("xG", "0.8"),
            FilterNodeBuilder::neq("outcome", "saved"),
        ]),
    ]);

    let matching_tags = create_tags(&[("event_type", "shot"), ("xG", "0.85"), ("outcome", "goal")]);

    c.bench_function("filter/complex/single", |b| {
        b.iter(|| matches(black_box(&filter), black_box(&matching_tags)))
    });
}

fn bench_complex_filter_10k(c: &mut Criterion) {
    let filter = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("event_type", "shot"),
            FilterNodeBuilder::gte("xG", "0.8"),
            FilterNodeBuilder::neq("outcome", "saved"),
        ]),
    ]);

    let matching_tags = create_tags(&[("event_type", "shot"), ("xG", "0.85"), ("outcome", "goal")]);

    c.bench_function("filter/complex/10k", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                let _ = matches(black_box(&filter), black_box(&matching_tags));
            }
        })
    });
}

// Numeric comparison benchmarks
fn bench_numeric_comparisons(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/numeric");

    let tags_int = create_tags(&[("count", "100")]);
    let tags_decimal = create_tags(&[("price", "99.5")]);

    let filter_gt = FilterNodeBuilder::gt("count", "42");
    let filter_gte = FilterNodeBuilder::gte("price", "99.5");

    group.bench_function("gt_integer", |b| {
        b.iter(|| matches(black_box(&filter_gt), black_box(&tags_int)))
    });

    group.bench_function("gte_decimal", |b| {
        b.iter(|| matches(black_box(&filter_gte), black_box(&tags_decimal)))
    });

    group.finish();
}

// String operation benchmarks
fn bench_string_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/string");

    let tags = create_tags(&[("ticker", "GOOGLE")]);

    let filter_sw = FilterNodeBuilder::starts_with("ticker", "GOO");
    let filter_ew = FilterNodeBuilder::ends_with("ticker", "LE");
    let filter_ct = FilterNodeBuilder::contains("ticker", "OOG");

    group.bench_function("starts_with", |b| {
        b.iter(|| matches(black_box(&filter_sw), black_box(&tags)))
    });

    group.bench_function("ends_with", |b| {
        b.iter(|| matches(black_box(&filter_ew), black_box(&tags)))
    });

    group.bench_function("contains", |b| {
        b.iter(|| matches(black_box(&filter_ct), black_box(&tags)))
    });

    group.finish();
}

// Set operation benchmarks
fn bench_set_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/set");

    let tags = create_tags(&[("event_type", "goal")]);

    let filter_in_small = FilterNodeBuilder::in_set("event_type", &["goal", "shot"]);
    let filter_in_large = FilterNodeBuilder::in_set(
        "event_type",
        &[
            "pass", "tackle", "shot", "save", "goal", "penalty", "corner", "freekick",
        ],
    );

    group.bench_function("in_2_values", |b| {
        b.iter(|| matches(black_box(&filter_in_small), black_box(&tags)))
    });

    group.bench_function("in_8_values", |b| {
        b.iter(|| matches(black_box(&filter_in_large), black_box(&tags)))
    });

    group.finish();
}

// Logical operation benchmarks
fn bench_logical_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/logical");

    let tags = create_tags(&[("event_type", "shot"), ("xG", "0.85"), ("outcome", "goal")]);

    let filter_and = FilterNodeBuilder::and(vec![
        FilterNodeBuilder::eq("event_type", "shot"),
        FilterNodeBuilder::gte("xG", "0.8"),
    ]);

    let filter_or = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::eq("event_type", "shot"),
    ]);

    let filter_not = FilterNodeBuilder::not(FilterNodeBuilder::eq("event_type", "pass"));

    group.bench_function("and_2_nodes", |b| {
        b.iter(|| matches(black_box(&filter_and), black_box(&tags)))
    });

    group.bench_function("or_2_nodes", |b| {
        b.iter(|| matches(black_box(&filter_or), black_box(&tags)))
    });

    group.bench_function("not_1_node", |b| {
        b.iter(|| matches(black_box(&filter_not), black_box(&tags)))
    });

    group.finish();
}

// Filter construction benchmarks
fn bench_filter_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/construction");

    group.bench_function("simple_eq", |b| {
        b.iter(|| FilterNodeBuilder::eq("event_type", "goal"))
    });

    group.bench_function("complex_nested", |b| {
        b.iter(|| {
            FilterNodeBuilder::or(vec![
                FilterNodeBuilder::eq("event_type", "goal"),
                FilterNodeBuilder::and(vec![
                    FilterNodeBuilder::eq("event_type", "shot"),
                    FilterNodeBuilder::gte("xG", "0.8"),
                ]),
            ])
        })
    });

    group.finish();
}

// Validation benchmarks
fn bench_filter_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/validation");

    let simple_filter = FilterNodeBuilder::eq("event_type", "goal");
    let complex_filter = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("event_type", "shot"),
            FilterNodeBuilder::gte("xG", "0.8"),
        ]),
    ]);

    group.bench_function("simple", |b| {
        b.iter(|| black_box(&simple_filter).validate())
    });

    group.bench_function("complex", |b| {
        b.iter(|| black_box(&complex_filter).validate())
    });

    group.finish();
}

// Scaling benchmarks - different subscriber counts
fn bench_broadcast_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/broadcast_simulation");

    let filter = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("event_type", "shot"),
            FilterNodeBuilder::gte("xG", "0.8"),
        ]),
    ]);

    let matching_tags = create_tags(&[("event_type", "shot"), ("xG", "0.85")]);

    for count in [100, 1000, 5000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                for _ in 0..count {
                    let _ = matches(black_box(&filter), black_box(&matching_tags));
                }
            });
        });
    }

    group.finish();
}

// Early exit optimization benchmark
fn bench_early_exit(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/early_exit");

    let tags_match_first = create_tags(&[("event_type", "goal")]);
    let tags_match_last = create_tags(&[("event_type", "penalty")]);
    let tags_no_match = create_tags(&[("event_type", "pass")]);

    // OR operation - should exit early on first match
    let filter_or = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::eq("event_type", "shot"),
        FilterNodeBuilder::eq("event_type", "penalty"),
    ]);

    group.bench_function("or_match_first", |b| {
        b.iter(|| matches(black_box(&filter_or), black_box(&tags_match_first)))
    });

    group.bench_function("or_match_last", |b| {
        b.iter(|| matches(black_box(&filter_or), black_box(&tags_match_last)))
    });

    group.bench_function("or_no_match", |b| {
        b.iter(|| matches(black_box(&filter_or), black_box(&tags_no_match)))
    });

    // AND operation - should exit early on first non-match
    let filter_and = FilterNodeBuilder::and(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::eq("team", "Real Madrid"),
        FilterNodeBuilder::eq("player", "Mbappe"),
    ]);

    group.bench_function("and_fail_first", |b| {
        b.iter(|| matches(black_box(&filter_and), black_box(&tags_no_match)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_eq_filter,
    bench_simple_eq_filter_10k,
    bench_complex_filter,
    bench_complex_filter_10k,
    bench_numeric_comparisons,
    bench_string_operations,
    bench_set_operations,
    bench_logical_operations,
    bench_filter_construction,
    bench_filter_validation,
    bench_broadcast_simulation,
    bench_early_exit,
);

criterion_main!(benches);
