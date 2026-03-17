use super::{CleanupConfig, CleanupSenderHandle, WorkerThreadsResolve, worker::CleanupWorker};
use crossfire::mpsc;
use sockudo_adapter::connection_manager::ConnectionManager;
use sockudo_core::app::AppManager;
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
        config: CleanupConfig,
    ) -> Self {
        let num_workers = config.worker_threads.resolve();

        info!(
            "Initializing multi-worker cleanup system with {} workers",
            num_workers
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
                worker_config.clone(),
            );

            let handle = tokio::spawn(async move {
                info!("Cleanup worker {} starting", worker_id);
                worker.run(receiver).await;
                info!("Cleanup worker {} stopped", worker_id);
            });

            senders.push(sender);
            worker_handles.push(handle);
        }

        info!(
            "Multi-worker cleanup system initialized with {} workers, batch_size={} per worker",
            num_workers, config.batch_size
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
                let error_msg = format!("Worker {} shutdown error: {}", i, e);
                error!("{}", error_msg);
                shutdown_errors.push(error_msg);
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
