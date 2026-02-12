use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use serde::Deserialize;
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

criterion_group!(benches, bench_filter_eval);
criterion_main!(benches);
