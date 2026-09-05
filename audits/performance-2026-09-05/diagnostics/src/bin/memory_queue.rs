use std::hint::black_box;
use std::time::Instant;
/// Additional dependency: sockudo-queue. No processor is registered: retain a
/// controlled backlog and compare equal-sized ready queues with/without dedup.
pub async fn run_memory_queue_diagnostics() {
    use sockudo_core::options::QueueReliabilityConfig;
    use sockudo_core::queue::{QueueInterface, QueueJobOptions};
    use sockudo_core::webhook_types::JobData;
    use sockudo_queue::MemoryQueueManager;
    for dedup in [false, true] {
        for count in [0usize, 1_000, 10_000, 30_000] {
            let queue = MemoryQueueManager::new_with_config(QueueReliabilityConfig {
                memory_capacity: 100_000,
                deduplication_ttl_ms: 3_600_000,
                ..Default::default()
            })
            .unwrap();
            for i in 0..count {
                queue
                    .enqueue(
                        "audit",
                        JobData::default(),
                        QueueJobOptions {
                            job_id: Some(format!("seed-{i}")),
                            deduplication_key: dedup.then(|| format!("seed-{i}")),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }
            for sample in 0..3 {
                let start = Instant::now();
                for i in 0..500 {
                    black_box(
                        queue
                            .enqueue(
                                "audit",
                                JobData::default(),
                                QueueJobOptions {
                                    job_id: Some(format!("probe-{sample}-{i}")),
                                    // Probe without dedup: even ordinary work scans old keys.
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap(),
                    );
                }
                println!(
                    "memory_queue_enqueue,dedup_seed={dedup},retained_dedup={count},sample={sample},probe_jobs=500,elapsed_us={}",
                    start.elapsed().as_micros()
                );
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    run_memory_queue_diagnostics().await;
}
