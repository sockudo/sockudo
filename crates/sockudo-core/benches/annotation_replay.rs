use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationId, AnnotationSerial, AnnotationStore, AnnotationType,
    MemoryAnnotationStore, RawAnnotationReplayRequest, StoredAnnotationEvent,
};
use sockudo_core::versioned_messages::MessageSerial;
use std::hint::black_box;

const APP_ID: &str = "bench-app";
const CHANNEL: &str = "bench-channel";
const NOISY_EVENTS: usize = 2_000;
const TARGET_EVENTS: usize = 100;
const PAGE_SIZE: usize = 50;

fn stored_event(index: usize, message_serial: MessageSerial) -> StoredAnnotationEvent {
    StoredAnnotationEvent {
        app_id: APP_ID.to_string(),
        channel_id: CHANNEL.to_string(),
        stored_at_ms: index as i64,
        annotation: Annotation {
            id: AnnotationId::new(format!("id:{index:020}")).unwrap(),
            action: AnnotationAction::Create,
            serial: AnnotationSerial::new(format!("ann:{index:020}")).unwrap(),
            message_serial,
            annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
            name: None,
            client_id: None,
            count: None,
            data: None,
            encoding: None,
            timestamp: index as i64,
        },
    }
}

fn annotation_replay(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = MemoryAnnotationStore::new();
    let target = MessageSerial::new("msg:target").unwrap();
    runtime.block_on(async {
        for index in 0..NOISY_EVENTS {
            store
                .append_event(stored_event(
                    index,
                    MessageSerial::new(format!("msg:noise-{index:020}")).unwrap(),
                ))
                .await
                .unwrap();
        }
        for offset in 0..TARGET_EVENTS {
            store
                .append_event(stored_event(NOISY_EVENTS + offset, target.clone()))
                .await
                .unwrap();
        }
    });

    let mut group = c.benchmark_group("annotation_rest_page");
    group.bench_function("channel_replay_then_filter", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let page = store
                    .replay_raw(RawAnnotationReplayRequest {
                        app_id: APP_ID.to_string(),
                        channel_id: CHANNEL.to_string(),
                        message_serial: None,
                        after_annotation_serial: None,
                        limit: usize::MAX,
                    })
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|event| event.message_serial() == &target)
                    .take(PAGE_SIZE)
                    .collect::<Vec<_>>();
                black_box(page)
            })
        })
    });
    group.bench_function("message_scoped_bounded_replay", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let page = store
                    .replay_raw(RawAnnotationReplayRequest {
                        app_id: APP_ID.to_string(),
                        channel_id: CHANNEL.to_string(),
                        message_serial: Some(target.clone()),
                        after_annotation_serial: None,
                        limit: PAGE_SIZE,
                    })
                    .await
                    .unwrap();
                black_box(page)
            })
        })
    });
    group.finish();
}

criterion_group!(benches, annotation_replay);
criterion_main!(benches);
