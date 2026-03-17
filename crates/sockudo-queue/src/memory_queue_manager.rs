use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use dashmap::DashMap;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Maximum number of jobs per queue to prevent unbounded memory growth
const MAX_QUEUE_SIZE: usize = 100_000;

/// Memory-based queue manager for simple deployments
pub struct MemoryQueueManager {
    queues: Arc<DashMap<String, Vec<JobData>, ahash::RandomState>>,
    processors: Arc<DashMap<String, ArcJobProcessorFn, ahash::RandomState>>,
}

impl Default for MemoryQueueManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryQueueManager {
    pub fn new() -> Self {
        let queues = Arc::new(DashMap::with_hasher(ahash::RandomState::new()));
        let processors = Arc::new(DashMap::with_hasher(ahash::RandomState::new()));

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

                let queue_names: Vec<String> = queues
                    .iter()
                    .map(|entry| entry.key().clone())
                    .filter(|name| processors.contains_key(name))
                    .collect();

                for queue_name in queue_names {
                    if let Some(processor) = processors.get(&queue_name)
                        && let Some((key, mut jobs_vec)) = queues.remove(&queue_name)
                    {
                        if !jobs_vec.is_empty() {
                            debug!(
                                "Processing {} jobs from memory queue {}",
                                jobs_vec.len(),
                                key
                            );

                            queues.insert(key, Vec::new());

                            for job in jobs_vec.drain(..) {
                                let processor_clone = Arc::clone(&processor);
                                tokio::spawn(async move {
                                    if let Err(e) = processor_clone(job).await {
                                        tracing::error!("Failed to process webhook job: {}", e);
                                    }
                                });
                            }
                        } else {
                            queues.insert(key, jobs_vec);
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
            queue.drain(0..to_remove);
        }

        queue.push(data);
        Ok(())
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> sockudo_core::error::Result<()> {
        self.processors
            .insert(queue_name.to_string(), Arc::from(callback));
        debug!("Registered processor for memory queue: {}", queue_name);

        Ok(())
    }

    async fn disconnect(&self) -> sockudo_core::error::Result<()> {
        self.queues.clear();
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
