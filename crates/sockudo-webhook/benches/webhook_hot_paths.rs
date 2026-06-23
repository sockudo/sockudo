use ahash::AHashMap;
use async_trait::async_trait;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use parking_lot::Mutex as ParkingMutex;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobPayload, JobProcessorFnAsync, Webhook};
use sockudo_webhook::integration::{
    BatchingConfig, QueueManager, WebhookConfig, WebhookIntegration,
};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use tokio::sync::Mutex as TokioMutex;

#[derive(Default)]
struct NoopQueue;

#[async_trait]
impl QueueInterface for NoopQueue {
    async fn add_to_queue(
        &self,
        _queue_name: &str,
        _data: JobData,
    ) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn process_queue(
        &self,
        _queue_name: &str,
        _callback: JobProcessorFnAsync,
    ) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn disconnect(&self) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn check_health(&self) -> sockudo_core::error::Result<()> {
        Ok(())
    }
}

fn test_app() -> App {
    let policy = AppPolicy {
        webhooks: Some(vec![Webhook {
            event_types: vec!["member_added".to_string()],
            ..Webhook::default()
        }]),
        ..AppPolicy::default()
    };

    App::from_policy(
        "bench-app".to_string(),
        "bench-key".to_string(),
        "bench-secret".to_string(),
        true,
        policy,
    )
}

async fn direct_integration() -> (WebhookIntegration, App) {
    let app = test_app();
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app.clone()).await.unwrap();

    let queue_manager = Arc::new(QueueManager::new(Box::<NoopQueue>::default()));
    let integration = WebhookIntegration::new(
        WebhookConfig {
            batching: BatchingConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        },
        app_manager,
        Some(queue_manager),
    )
    .await
    .unwrap();

    (integration, app)
}

fn sample_job(index: usize) -> JobData {
    JobData {
        app_key: "bench-key".to_string(),
        app_id: "bench-app".to_string(),
        app_secret: "bench-secret".to_string(),
        payload: JobPayload {
            time_ms: index as i64,
            events: vec![sonic_rs::json!({
                "name": "member_added",
                "channel": "presence-status",
                "user_id": format!("user-{index}")
            })],
        },
        original_signature: format!("bench-sig-{index}"),
    }
}

fn sample_jobs(count: usize) -> Vec<JobData> {
    (0..count).map(sample_job).collect()
}

fn bench_send_member_added_direct(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let (integration, app) = runtime.block_on(direct_integration());

    let mut group = c.benchmark_group("webhook_presence_hot_path");
    group.throughput(Throughput::Elements(1));
    group.bench_function("send_member_added_direct_queue", |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let start = Instant::now();
                for _ in 0..iterations {
                    integration
                        .send_member_added(
                            black_box(&app),
                            black_box("presence-status"),
                            black_box("user-123"),
                        )
                        .await
                        .unwrap();
                    black_box(());
                }
                start.elapsed()
            })
        });
    });
    group.finish();
}

fn bench_batch_buffer_push(c: &mut Criterion) {
    const JOBS_PER_ITERATION: usize = 1024;

    let runtime = Runtime::new().expect("benchmark runtime");
    let mut group = c.benchmark_group("webhook_batch_buffer_push");
    group.throughput(Throughput::Elements(JOBS_PER_ITERATION as u64));

    group.bench_function("legacy_tokio_mutex_hashmap", |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let start = Instant::now();
                for _ in 0..iterations {
                    let jobs = sample_jobs(JOBS_PER_ITERATION);
                    let batched = TokioMutex::new(AHashMap::<String, Vec<JobData>>::new());

                    for job in jobs {
                        let mut guard = batched.lock().await;
                        guard.entry("webhooks".to_string()).or_default().push(job);
                    }

                    let guard = batched.lock().await;
                    black_box(guard.get("webhooks").map_or(0, Vec::len));
                }
                start.elapsed()
            })
        });
    });

    group.bench_function("parking_lot_vec", |b| {
        b.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                let jobs = sample_jobs(JOBS_PER_ITERATION);
                let batched = ParkingMutex::new(Vec::<JobData>::new());

                for job in jobs {
                    batched.lock().push(job);
                }

                black_box(batched.lock().len());
            }
            start.elapsed()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_send_member_added_direct,
    bench_batch_buffer_push
);
criterion_main!(benches);
