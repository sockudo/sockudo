use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use serde::Deserialize;
use sockudo_filter::{MessagePredicate, ProjectedDocument, SubscriptionView};
use std::hint::black_box;

#[derive(Debug, Deserialize)]
struct Event {
    event_type: String,
    xg: f64,
    minute: u16,
    team: String,
}

fn is_important(event: &Event) -> bool {
    event.event_type == "goal"
        || (event.event_type == "shot" && event.xg >= 0.8)
        || (event.team == "home" && event.minute >= 80)
}

fn bench_filter_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_eval");

    let goal = r#"{"event_type":"goal","xg":0.95,"minute":44,"team":"away"}"#;
    let shot = r#"{"event_type":"shot","xg":0.82,"minute":67,"team":"home"}"#;
    let pass = r#"{"event_type":"pass","xg":0.01,"minute":12,"team":"away"}"#;

    for (name, payload) in [("goal", goal), ("shot", shot), ("pass", pass)] {
        group.bench_with_input(
            BenchmarkId::new("sonic_parse_and_eval", name),
            &payload,
            |b, raw| {
                b.iter(|| {
                    let event: Event =
                        sonic_rs::from_str(black_box(raw)).expect("valid benchmark JSON");
                    black_box(is_important(&event));
                });
            },
        );
    }

    group.finish();
}

fn bench_compiled_predicate(c: &mut Criterion) {
    let cheap = MessagePredicate::compile(SubscriptionView {
        events: vec!["order.updated".into()],
        ..SubscriptionView::default()
    })
    .unwrap();
    let expression = MessagePredicate::compile(SubscriptionView {
        expression: Some("data.total >= `100` && headers.priority == `\"high\"`".into()),
        ..SubscriptionView::default()
    })
    .unwrap();
    let tags = std::collections::BTreeMap::<String, String>::new();
    let document = ProjectedDocument::new(&serde_json::json!({
        "data": {"total": 125},
        "headers": {"priority": "high"}
    }))
    .unwrap();

    let mut group = c.benchmark_group("compiled_predicate");
    group.bench_function("event_only", |b| {
        b.iter(|| {
            black_box(cheap.matches_projected(
                black_box(Some("order.updated")),
                black_box(&tags),
                None,
            ))
        });
    });
    group.bench_function("jmespath_projected_reused", |b| {
        b.iter(|| {
            black_box(expression.matches_projected(
                None,
                black_box(&tags),
                Some(black_box(&document)),
            ))
        });
    });
    group.finish();
}

criterion_group!(benches, bench_filter_eval, bench_compiled_predicate);
criterion_main!(benches);
