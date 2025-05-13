// --- MemoryQueueManager ---
// No major logical changes, but added comments and ensured consistency.

use crate::log::Log;
use crate::queue::{ArcJobProcessorFn, JobProcessorFn, QueueInterface};
use crate::webhook::sender::JobProcessorFnAsync;
use crate::webhook::types::JobData;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Memory-based queue manager for simple deployments
pub struct MemoryQueueManager {
    // Use channels to simulate a queue in memory
    // DashMap<String, Vec<JobData>> is implicitly Send + Sync if JobData is Send
    queues: dashmap::DashMap<String, Vec<JobData>>,
    // Store Arc'd callbacks to be consistent with Redis manager and avoid potential issues if Box wasn't 'static
    processors: dashmap::DashMap<String, ArcJobProcessorFn>,
}

impl MemoryQueueManager {
    pub fn new() -> Self {
        let queues = dashmap::DashMap::new();
        let processors = dashmap::DashMap::new();

        Self { queues, processors }
    }

    /// Starts the background processing loop. Should be called once after setup.
    pub fn start_processing(&self) {
        // Clone Arcs for the background task
        let queues = self.queues.clone();
        let processors = self.processors.clone();

        Log::info("Starting memory queue processing loop...".to_string());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            loop {
                interval.tick().await;

                // Iterate through queues. DashMap allows concurrent access.
                for queue_entry in queues.iter() {
                    // Use iter() for read access
                    let queue_name = queue_entry.key().clone();

                    // Get the processor for this queue
                    if let Some(processor) = processors.get(&queue_name) {
                        // Get a mutable reference to the queue's Vec
                        if let Some(mut jobs_vec) = queues.get_mut(&queue_name) {
                            // Take all jobs from the queue for this tick
                            // Note: If a job processor is slow, it blocks others in the same queue during this tick.
                            // Consider spawning tasks per job for better isolation if needed.
                            let jobs_to_process: Vec<JobData> = jobs_vec.drain(..).collect();

                            if !jobs_to_process.is_empty() {
                                Log::info(format!(
                                    "Processing {} jobs from memory queue {}",
                                    jobs_to_process.len(),
                                    queue_name
                                ));
                                // Process each job sequentially within this tick
                                for job in jobs_to_process {
                                    // Clone the Arc'd processor for the call
                                    let processor_clone = processor.clone();
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
        Log::info(format!(
            "Registered processor for memory queue: {}",
            queue_name
        ));

        Ok(())
    }

    async fn disconnect(&self) -> crate::error::Result<()> {
        // Nothing needed for memory queue
        Ok(())
    }
}
