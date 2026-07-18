use super::{CleanupConfig, CleanupSenderHandle, WorkerThreadsResolve, worker::CleanupWorker};
use crossfire::mpsc;
use sockudo_adapter::connection_manager::ConnectionManager;
use sockudo_core::app::AppManager;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::PresenceHistoryConfig;
use sockudo_core::presence_history::PresenceHistoryStore;
use sockudo_webhook::WebhookIntegration;
use std::sync::Arc;
use tracing::{error, info};

/// Multi-worker cleanup system that distributes work across multiple worker threads
pub struct MultiWorkerCleanupSystem {
    senders: Vec<CleanupSenderHandle>,
    worker_handles: Vec<tokio::task::JoinHandle<()>>,
    config: CleanupConfig,
}

impl MultiWorkerCleanupSystem {
    pub fn new(
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        webhook_integration: Option<Arc<WebhookIntegration>>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_config: PresenceHistoryConfig,
        config: CleanupConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Self {
        let num_workers = config.worker_threads.resolve();

        info!(
            worker_count = num_workers,
            "initializing multi-worker cleanup system"
        );

        let mut senders = Vec::with_capacity(num_workers);
        let mut worker_handles = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let (sender, receiver) = mpsc::bounded_async(config.queue_buffer_size);

            let worker_config = config.clone();

            let worker = CleanupWorker::new(
                connection_manager.clone(),
                app_manager.clone(),
                webhook_integration.clone(),
                presence_history_store.clone(),
                presence_history_config.clone(),
                worker_config.clone(),
                metrics.clone(),
            );

            let handle = tokio::spawn(async move {
                info!(worker_id = worker_id, "cleanup worker starting");
                worker.run(receiver).await;
                info!(worker_id = worker_id, "cleanup worker stopped");
            });

            senders.push(sender);
            worker_handles.push(handle);
        }

        info!(
            worker_count = num_workers,
            batch_size = config.batch_size,
            "multi-worker cleanup system initialized"
        );

        Self {
            senders,
            worker_handles,
            config,
        }
    }

    /// Get the main sender for sending tasks - this will distribute work across workers
    pub fn get_sender(&self) -> super::MultiWorkerSender {
        super::MultiWorkerSender::new(self.senders.clone())
    }

    /// Get a direct sender for single worker optimization (avoids wrapper overhead)
    pub fn get_direct_sender(&self) -> Option<CleanupSenderHandle> {
        if self.senders.len() == 1 {
            self.senders.first().cloned()
        } else {
            None
        }
    }

    /// Get worker handles for shutdown
    pub fn get_worker_handles(self) -> Vec<tokio::task::JoinHandle<()>> {
        self.worker_handles
    }

    /// Get configuration
    pub fn get_config(&self) -> &CleanupConfig {
        &self.config
    }

    /// Shutdown all workers gracefully
    pub async fn shutdown(self) -> Result<(), String> {
        info!("Shutting down multi-worker cleanup system...");

        drop(self.senders);

        let mut shutdown_errors = Vec::new();
        for (i, handle) in self.worker_handles.into_iter().enumerate() {
            if let Err(e) = handle.await {
                error!(worker_id = i, error = %e, "worker shutdown error");
                shutdown_errors.push(format!("worker {i} shutdown error: {e}"));
            }
        }

        if shutdown_errors.is_empty() {
            info!("Multi-worker cleanup system shutdown complete");
            Ok(())
        } else {
            Err(format!(
                "Shutdown completed with {} errors: {:?}",
                shutdown_errors.len(),
                shutdown_errors
            ))
        }
    }
}
