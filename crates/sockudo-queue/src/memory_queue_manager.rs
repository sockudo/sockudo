use crate::ArcJobProcessorFn;
use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use dashmap::DashMap;
use futures_util::stream::{self, StreamExt};
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Maximum number of jobs per queue to prevent unbounded memory growth
const MAX_QUEUE_SIZE: usize = 100_000;
const MAX_BATCH_SIZE: usize = 1_024;
const MAX_CONCURRENT_JOBS_PER_QUEUE: usize = 64;

/// Memory-based queue manager for simple deployments
pub struct MemoryQueueManager {
    queues: Arc<DashMap<String, VecDeque<JobData>, ahash::RandomState>>,
    processors: Arc<RwLock<HashMap<String, ArcJobProcessorFn>>>,
}

impl Default for MemoryQueueManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryQueueManager {
    pub fn new() -> Self {
        let queues = Arc::new(DashMap::with_hasher(ahash::RandomState::new()));
        let processors = Arc::new(RwLock::new(HashMap::new()));

        Self { queues, processors }
    }

    /// Starts the background processing loop. Should be called once after setup.
    pub fn start_processing(&self) {
        let queues = Arc::clone(&self.queues);
        let processors = Arc::clone(&self.processors);

        info!("memory queue processing loop started");

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            loop {
                interval.tick().await;

                let queue_names: Vec<String> =
                    queues.iter().map(|entry| entry.key().clone()).collect();

                for queue_name in queue_names {
                    let processor = {
                        let processors_guard = processors.read().unwrap();
                        processors_guard.get(&queue_name).cloned()
                    };

                    if let Some(processor) = processor
                        && let Some(mut jobs_queue) = queues.get_mut(&queue_name)
                    {
                        if jobs_queue.is_empty() {
                            continue;
                        }

                        let batch_len = MAX_BATCH_SIZE.min(jobs_queue.len());
                        let jobs: Vec<JobData> = jobs_queue.drain(..batch_len).collect();
                        debug!(queue = %queue_name, job_count = jobs.len(), "processing memory queue batch");
                        drop(jobs_queue);

                        stream::iter(jobs)
                            .for_each_concurrent(MAX_CONCURRENT_JOBS_PER_QUEUE, |job| {
                                let processor_clone = processor.clone();
                                async move {
                                    if let Err(e) = processor_clone(job).await {
                                        tracing::error!(error = %e, "failed to process webhook job");
                                    }
                                }
                            })
                            .await;
                    }
                }
            }
        });
    }
}

#[async_trait]
impl QueueInterface for MemoryQueueManager {
    async fn add_to_queue(
        &self,
        queue_name: &str,
        data: JobData,
    ) -> sockudo_core::error::Result<()> {
        let mut queue = self.queues.entry(queue_name.to_string()).or_default();

        if queue.len() >= MAX_QUEUE_SIZE {
            let to_remove = queue.len() - MAX_QUEUE_SIZE + 1;
            warn!(queue = %queue_name, capacity = MAX_QUEUE_SIZE, dropped = to_remove, "memory queue at capacity, dropping oldest jobs");
            for _ in 0..to_remove {
                queue.pop_front();
            }
        }

        queue.push_back(data);
        Ok(())
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> sockudo_core::error::Result<()> {
        self.processors
            .write()
            .unwrap()
            .insert(queue_name.to_string(), Arc::from(callback));
        debug!(queue = %queue_name, "registered processor for memory queue");

        Ok(())
    }

    async fn disconnect(&self) -> sockudo_core::error::Result<()> {
        self.queues.clear();
        self.processors.write().unwrap().clear();
        Ok(())
    }

    async fn check_health(&self) -> sockudo_core::error::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::webhook_types::{JobData, JobPayload};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_add_to_queue() {
        let manager = MemoryQueueManager::new();
        let data = JobData {
            job_id: None,
            app_key: "test_key".to_string(),
            app_id: "test_id".to_string(),
            app_secret: "test_secret".to_string(),
            payload: JobPayload {
                time_ms: chrono::Utc::now().timestamp_millis(),
                events: vec![],
            },
            original_signature: "test_signature".to_string(),
        };

        manager
            .add_to_queue("test_queue", data.clone())
            .await
            .unwrap();

        assert_eq!(manager.queues.get("test_queue").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_add_batch_to_queue() {
        let manager = MemoryQueueManager::new();
        let data = JobData {
            job_id: None,
            app_key: "test_key".to_string(),
            app_id: "test_id".to_string(),
            app_secret: "test_secret".to_string(),
            payload: JobPayload {
                time_ms: chrono::Utc::now().timestamp_millis(),
                events: vec![],
            },
            original_signature: "test_signature".to_string(),
        };

        manager
            .add_batch_to_queue("test_queue", vec![data.clone(), data])
            .await
            .unwrap();

        assert_eq!(manager.queues.get("test_queue").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_disconnect() {
        let manager = MemoryQueueManager::new();
        let data = JobData {
            job_id: None,
            app_key: "test_key".to_string(),
            app_id: "test_id".to_string(),
            app_secret: "test_secret".to_string(),
            payload: JobPayload {
                time_ms: chrono::Utc::now().timestamp_millis(),
                events: vec![],
            },
            original_signature: "test_signature".to_string(),
        };

        manager.add_to_queue("test_queue", data).await.unwrap();
        assert!(!manager.queues.is_empty());

        manager.disconnect().await.unwrap();
        assert!(manager.queues.is_empty());
    }

    #[tokio::test]
    async fn processing_limits_per_queue_concurrency() {
        let manager = MemoryQueueManager::new();
        manager.start_processing();

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(Notify::new());
        let total_jobs = MAX_CONCURRENT_JOBS_PER_QUEUE * 2;

        let active_clone = Arc::clone(&active);
        let max_active_clone = Arc::clone(&max_active);
        let completed_clone = Arc::clone(&completed);
        let done_clone = Arc::clone(&done);

        manager
            .process_queue(
                "bounded",
                Box::new(move |_job| {
                    let active = Arc::clone(&active_clone);
                    let max_active = Arc::clone(&max_active_clone);
                    let completed = Arc::clone(&completed_clone);
                    let done = Arc::clone(&done_clone);
                    Box::pin(async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(current, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        if completed.fetch_add(1, Ordering::SeqCst) + 1 == total_jobs {
                            done.notify_waiters();
                        }
                        Ok(())
                    })
                }),
            )
            .await
            .unwrap();

        for _ in 0..total_jobs {
            manager
                .add_to_queue(
                    "bounded",
                    JobData {
                        job_id: None,
                        app_key: "test_key".to_string(),
                        app_id: "test_id".to_string(),
                        app_secret: "test_secret".to_string(),
                        payload: JobPayload {
                            time_ms: chrono::Utc::now().timestamp_millis(),
                            events: vec![],
                        },
                        original_signature: "test_signature".to_string(),
                    },
                )
                .await
                .unwrap();
        }

        timeout(Duration::from_secs(3), done.notified())
            .await
            .expect("jobs should finish");

        assert!(max_active.load(Ordering::SeqCst) <= MAX_CONCURRENT_JOBS_PER_QUEUE);
    }
}
