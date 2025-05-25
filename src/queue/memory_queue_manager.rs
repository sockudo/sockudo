// --- MemoryQueueManager ---
// No major logical changes, but added comments and ensured consistency.

use crate::queue::{ArcJobProcessorFn, QueueInterface};
use crate::webhook::sender::JobProcessorFnAsync;
use crate::webhook::types::JobData;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Memory-based queue manager for simple deployments
pub struct MemoryQueueManager {
    // Use channels to simulate a queue in memory
    // DashMap<String, Vec<JobData>> is implicitly Send + Sync if JobData is Send
    queues: DashMap<String, Vec<JobData>, ahash::RandomState>,
    // Store Arc'd callbacks to be consistent with Redis manager and avoid potential issues if Box wasn't 'static
    processors: DashMap<String, ArcJobProcessorFn, ahash::RandomState>,
}

impl MemoryQueueManager {
    pub fn new() -> Self {
        let queues = DashMap::with_hasher(ahash::RandomState::new());
        let processors = DashMap::with_hasher(ahash::RandomState::new());

        Self { queues, processors }
    }

    /// Starts the background processing loop. Should be called once after setup.
    pub fn start_processing(&self) {
        // Clone Arcs for the background task
        let queues = self.queues.clone();
        let processors = self.processors.clone();

        info!("{}", "Starting memory queue processing loop...".to_string());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            loop {
                interval.tick().await;

                // Iterate through queues. DashMap allows concurrent access.
                for queue_entry in queues.iter() {
                    // Use iter() for read access
                    let queue_name = queue_entry.key();

                    // Get the processor for this queue
                    if let Some(processor) = processors.get(queue_name) {
                        // Get a mutable reference to the queue's Vec
                        if let Some(mut jobs_vec) = queues.get_mut(queue_name) {
                            // Take all jobs from the queue for this tick
                            // Note: If a job processor is slow, it blocks others in the same queue during this tick.
                            // Consider spawning tasks per job for better isolation if needed.

                            if !jobs_vec.is_empty() {
                                info!(
                                    "{}",
                                    format!(
                                        "Processing {} jobs from memory queue {}",
                                        jobs_vec.len(),
                                        queue_name
                                    )
                                );
                                // Process each job sequentially within this tick
                                for job in jobs_vec.drain(..) {
                                    // Clone the Arc'd processor for the call
                                    let processor_clone = Arc::clone(&processor);
                                    processor_clone(job).await.unwrap();
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}

#[async_trait]
impl QueueInterface for MemoryQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> crate::error::Result<()> {
        // Ensure queue Vec exists using entry API for atomicity
        self.queues
            .entry(queue_name.to_string())
            .or_default()
            .push(data);
        Ok(())
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> crate::error::Result<()> {
        // Ensure the queue Vec exists (might be redundant if add_to_queue is always called first, but safe)
        self.queues.entry(queue_name.to_string()).or_default();

        // Register processor, wrapping it in Arc
        self.processors
            .insert(queue_name.to_string(), Arc::from(callback)); // Store as Arc
        info!(
            "{}",
            format!("Registered processor for memory queue: {}", queue_name)
        );

        Ok(())
    }

    async fn disconnect(&self) -> crate::error::Result<()> {
        self.queues.clear();
        Ok(())
    }
}
