use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use sockudo_push::{
    AcceptAllDispatcher, ChannelSubscription, DeliveryBatch, DeliveryJob, DeviceDetails,
    DevicePushDetails, DevicePushState, FanoutConfig, FormFactor, MemoryPushQueue, MemoryPushStore,
    Platform, ProviderDispatchWorker, PublishIntent, PublishTarget, PushAcceptRequest,
    PushDeviceStore, PushMetrics, PushPayload, PushPipeline, PushPlanner, PushProviderKind,
    PushQueue, PushQueuePayload, PushQueueStage, PushRecipient, PushShardWorker,
    PushSubscriptionStore, SecretString, render_provider_payload,
};
use tokio::runtime::Runtime;

const APP_ID: &str = "app-1";
const CHANNEL: &str = "bench";
const BENCH_NOW_MS: u64 = 1_778_070_000_000;

fn bench_admission(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_admission");
    group.throughput(Throughput::Elements(1));
    group.bench_function("memory_accept_publish_only", |b| {
        b.iter_batched(
            || setup_runtime_pipeline(0, FanoutConfig::default(), 0),
            |fixture| {
                fixture.runtime.block_on(async move {
                    let outcome = fixture
                        .pipeline
                        .accept_publish(
                            PushAcceptRequest {
                                intent: sample_intent(
                                    next_publish_id("admission"),
                                    vec![PublishTarget::Channel {
                                        channel: CHANNEL.to_owned(),
                                    }],
                                ),
                                expected_recipients: 0,
                            },
                            BENCH_NOW_MS,
                        )
                        .await
                        .expect("benchmark publish accepted");
                    black_box(outcome.publish_id);
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_fast_fanout_planning(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_fast_fanout_planning");
    for recipients in [1_u64, 1_000, 10_000] {
        group.throughput(Throughput::Elements(recipients));
        group.bench_function(
            format!("memory_plan_fast_path_{recipients}_recipients"),
            |b| {
                b.iter_batched(
                    || {
                        let fixture = setup_runtime_pipeline(
                            recipients as usize,
                            FanoutConfig::default(),
                            recipients,
                        );
                        accept_channel_publish(&fixture, "fast", recipients);
                        fixture
                    },
                    |fixture| {
                        fixture.runtime.block_on(async move {
                            let planner = PushPlanner::new(
                                fixture.store.clone(),
                                fixture.queue.clone(),
                                fixture.config,
                            );
                            black_box(planner.run_once("bench-planner").await.unwrap());
                            black_box(
                                fixture
                                    .queue
                                    .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                                    .await
                                    .unwrap()
                                    .ready_depth,
                            );
                        });
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_sharded_fanout(c: &mut Criterion) {
    let config = FanoutConfig {
        fast_threshold: 1_000,
        shard_size: 5_000,
        page_size: 1_000,
        provider_batch_size: 500,
        ..FanoutConfig::default()
    };

    let mut group = c.benchmark_group("push_sharded_fanout");
    group.throughput(Throughput::Elements(20_000));
    group.bench_function("memory_create_shard_jobs_20k_recipients", |b| {
        b.iter_batched(
            || {
                let fixture = setup_runtime_pipeline(20_000, config.clone(), 20_000);
                accept_channel_publish(&fixture, "shard-plan", 20_000);
                fixture
            },
            |fixture| {
                fixture.runtime.block_on(async move {
                    let planner = PushPlanner::new(
                        fixture.store.clone(),
                        fixture.queue.clone(),
                        fixture.config,
                    );
                    black_box(planner.run_once("bench-planner").await.unwrap());
                    black_box(
                        fixture
                            .queue
                            .lag(PushQueueStage::ShardJobs)
                            .await
                            .unwrap()
                            .ready_depth,
                    );
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.throughput(Throughput::Elements(5_000));
    group.bench_function("memory_expand_one_shard_5k_recipients", |b| {
        b.iter_batched(
            || {
                let fixture = setup_runtime_pipeline(20_000, config.clone(), 20_000);
                accept_channel_publish(&fixture, "shard-worker", 20_000);
                fixture.runtime.block_on(async {
                    PushPlanner::new(
                        fixture.store.clone(),
                        fixture.queue.clone(),
                        fixture.config.clone(),
                    )
                    .run_once("bench-planner")
                    .await
                    .unwrap();
                });
                fixture
            },
            |fixture| {
                fixture.runtime.block_on(async move {
                    let worker = PushShardWorker::new(
                        fixture.store.clone(),
                        fixture.queue.clone(),
                        fixture.config,
                    );
                    black_box(worker.run_once("bench-shard").await.unwrap());
                    black_box(
                        fixture
                            .queue
                            .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                            .await
                            .unwrap()
                            .ready_depth,
                    );
                });
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_queue");
    group.throughput(Throughput::Elements(500));
    group.bench_function("memory_produce_consume_ack_delivery_batch_500_jobs", |b| {
        b.iter_batched(
            || {
                (
                    Runtime::new().expect("tokio runtime"),
                    Arc::new(MemoryPushQueue::new()),
                )
            },
            |(runtime, queue)| {
                runtime.block_on(async move {
                    let batch = sample_delivery_batch(next_publish_id("queue"), 500);
                    queue
                        .produce(
                            PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                            batch.queue_key(),
                            PushQueuePayload::DeliveryBatch(Box::new(batch)),
                        )
                        .await
                        .unwrap();
                    let messages = queue
                        .consume(
                            PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                            "bench-provider",
                            1,
                            30_000,
                        )
                        .await
                        .unwrap();
                    let message = messages.into_iter().next().expect("queued batch");
                    queue.ack(message.ack).await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_provider_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_provider_dispatch");
    group.throughput(Throughput::Elements(500));
    group.bench_function("mock_accept_all_dispatch_500_jobs", |b| {
        b.iter_batched(
            || {
                let runtime = Runtime::new().expect("tokio runtime");
                let queue = Arc::new(MemoryPushQueue::new());
                runtime.block_on(async {
                    let batch = sample_delivery_batch(next_publish_id("dispatch"), 500);
                    queue
                        .produce(
                            PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                            batch.queue_key(),
                            PushQueuePayload::DeliveryBatch(Box::new(batch)),
                        )
                        .await
                        .unwrap();
                });
                (runtime, queue)
            },
            |(runtime, queue)| {
                runtime.block_on(async move {
                    let dispatcher = Arc::new(AcceptAllDispatcher::new(PushProviderKind::Fcm));
                    let mut worker = ProviderDispatchWorker::new(
                        PushProviderKind::Fcm,
                        queue.clone(),
                        dispatcher,
                    );
                    black_box(worker.run_once("bench-provider").await.unwrap());
                    black_box(
                        queue
                            .lag(PushQueueStage::DeliveryResults)
                            .await
                            .unwrap()
                            .ready_depth,
                    );
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_provider_rendering(c: &mut Criterion) {
    let payload = sample_payload();
    let mut group = c.benchmark_group("push_provider_rendering");
    for provider in [
        PushProviderKind::Fcm,
        PushProviderKind::Apns,
        PushProviderKind::WebPush,
        PushProviderKind::Hms,
        PushProviderKind::Wns,
    ] {
        group.bench_function(format!("render_{provider:?}"), |b| {
            b.iter(|| {
                black_box(render_provider_payload(provider, black_box(&payload), &[]).unwrap());
            });
        });
    }
    group.finish();
}

fn bench_metrics(c: &mut Criterion) {
    let metrics = PushMetrics::default();
    c.bench_function("push_metrics_counter_increment", |b| {
        b.iter(|| {
            metrics.delivery_jobs_emitted(PushProviderKind::Fcm, black_box(APP_ID), 1);
        });
    });
}

struct PipelineFixture {
    runtime: Runtime,
    store: Arc<MemoryPushStore>,
    queue: Arc<MemoryPushQueue>,
    config: FanoutConfig,
    pipeline: PushPipeline,
}

fn setup_runtime_pipeline(
    recipient_count: usize,
    config: FanoutConfig,
    expected_recipients: u64,
) -> PipelineFixture {
    let runtime = Runtime::new().expect("tokio runtime");
    let store = Arc::new(MemoryPushStore::new());
    let queue = Arc::new(MemoryPushQueue::new());
    runtime.block_on(seed_channel(&store, recipient_count));
    let pipeline = PushPipeline::new(store.clone(), queue.clone(), config.clone())
        .with_max_publish_log_lag(expected_recipients.saturating_add(100_000));
    PipelineFixture {
        runtime,
        store,
        queue,
        config,
        pipeline,
    }
}

fn accept_channel_publish(
    fixture: &PipelineFixture,
    label: &'static str,
    expected_recipients: u64,
) {
    fixture.runtime.block_on(async {
        fixture
            .pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(
                        next_publish_id(label),
                        vec![PublishTarget::Channel {
                            channel: CHANNEL.to_owned(),
                        }],
                    ),
                    expected_recipients,
                },
                BENCH_NOW_MS,
            )
            .await
            .expect("benchmark publish accepted");
    });
}

async fn seed_channel(store: &Arc<MemoryPushStore>, recipient_count: usize) {
    for index in 0..recipient_count {
        let device_id = format!("device-{index}");
        let device = sample_device(&device_id);
        store.upsert_device(device.clone()).await.unwrap();
        store
            .upsert_subscription(ChannelSubscription::from_device(CHANNEL, &device))
            .await
            .unwrap();
    }
}

fn sample_intent(publish_id: String, targets: Vec<PublishTarget>) -> PublishIntent {
    PublishIntent {
        app_id: APP_ID.to_owned(),
        publish_id,
        targets,
        payload: sample_payload(),
        provider_overrides: vec![],
        not_before_ms: None,
        expires_at_ms: None,
    }
}

fn sample_payload() -> PushPayload {
    PushPayload {
        template_id: None,
        template_data: json!({ "data": { "headline": "benchmark" } }),
        title: Some("Benchmark".to_owned()),
        body: Some("Body".to_owned()),
        icon: None,
        sound: None,
        collapse_key: None,
    }
}

fn sample_device(device_id: &str) -> DeviceDetails {
    DeviceDetails {
        app_id: APP_ID.to_owned(),
        id: device_id.to_owned(),
        client_id: Some("client-1".to_owned()),
        form_factor: FormFactor::Phone,
        platform: Platform::Android,
        metadata: json!({}),
        device_secret: SecretString::new(format!("pbkdf2-sha256$120000$bench${device_id}"))
            .unwrap(),
        timezone: "UTC".to_owned(),
        locale: "en".to_owned(),
        last_active_at_ms: 1,
        push: DevicePushDetails {
            recipient: PushRecipient::Fcm {
                registration_token: SecretString::new(format!("token-{device_id}")).unwrap(),
            },
            state: DevicePushState::Active,
            failure_count: 0,
            error_reason: None,
        },
        push_rate_policy: None,
    }
}

fn sample_delivery_batch(publish_id: String, jobs: usize) -> DeliveryBatch {
    let payload = Arc::new(sample_payload());
    DeliveryBatch {
        app_id: APP_ID.to_owned(),
        publish_id: publish_id.clone(),
        provider: PushProviderKind::Fcm,
        batch_id: "bench-batch-1".to_owned(),
        jobs: (0..jobs)
            .map(|index| DeliveryJob {
                app_id: APP_ID.to_owned(),
                publish_id: publish_id.clone(),
                provider: PushProviderKind::Fcm,
                batch_id: "bench-batch-1".to_owned(),
                device_id: Some(format!("device-{index}")),
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new(format!("token-{index}")).unwrap(),
                },
                payload: Arc::clone(&payload),
                attempt: 1,
                not_before_ms: None,
                expires_at_ms: None,
            })
            .collect(),
    }
}

fn next_publish_id(label: &'static str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!("{label}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

criterion_group!(
    benches,
    bench_admission,
    bench_fast_fanout_planning,
    bench_sharded_fanout,
    bench_queue,
    bench_provider_dispatch,
    bench_provider_rendering,
    bench_metrics
);
criterion_main!(benches);
