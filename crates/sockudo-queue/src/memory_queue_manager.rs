use crate::ArcJobProcessorFn;
use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use dashmap::DashMap;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Maximum number of jobs per queue to prevent unbounded memory growth
const MAX_QUEUE_SIZE: usize = 100_000;

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

        info!("{}", "Starting memory queue processing loop...".to_string());

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

                        let jobs: Vec<JobData> = jobs_queue.drain(..).collect();
                        debug!(
                            "Processing {} jobs from memory queue {}",
                            jobs.len(),
                            queue_name
                        );
                        drop(jobs_queue);

                        for job in jobs {
                            let processor_clone = processor.clone();
                            tokio::spawn(async move {
                                if let Err(e) = processor_clone(job).await {
                                    tracing::error!("Failed to process webhook job: {}", e);
                                }
                            });
                        }
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
            warn!(
                "Memory queue '{}' at capacity ({}), dropping {} oldest job(s)",
                queue_name, MAX_QUEUE_SIZE, to_remove
            );
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
        debug!("Registered processor for memory queue: {}", queue_name);

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

    #[tokio::test]
    async fn test_add_to_queue() {
        let manager = MemoryQueueManager::new();
        let data = JobData {
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
    async fn test_disconnect() {
        let manager = MemoryQueueManager::new();
        let data = JobData {
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
}
