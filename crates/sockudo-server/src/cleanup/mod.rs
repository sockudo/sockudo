pub mod multi_worker;
pub mod worker;

// Re-export core cleanup types from sockudo-adapter
pub use sockudo_adapter::cleanup::{
    AuthInfo, CleanupChannelFlavor, CleanupReceiverHandle, CleanupSender, CleanupSenderHandle,
    ConnectionCleanupInfo, DisconnectTask, MultiWorkerSender, WorkerStats,
};

// Re-export CleanupConfig and WorkerThreadsConfig from sockudo-core options
pub use sockudo_core::options::{CleanupConfig, WorkerThreadsConfig};

// Re-export CancellationToken for use by callers who want graceful shutdown
pub use tokio_util::sync::CancellationToken;

/// Extension trait for WorkerThreadsConfig to resolve the actual thread count
pub trait WorkerThreadsResolve {
    fn resolve(&self) -> usize;
}

impl WorkerThreadsResolve for WorkerThreadsConfig {
    fn resolve(&self) -> usize {
        match self {
            WorkerThreadsConfig::Auto => {
                let cpu_count = num_cpus::get();
                let auto_threads = (cpu_count / 4).clamp(1, 4);
                tracing::info!(
                    "Auto-detected {} CPUs, using {} cleanup worker threads",
                    cpu_count,
                    auto_threads
                );
                auto_threads
            }
            WorkerThreadsConfig::Fixed(n) => *n,
        }
    }
}

/// Manages the cleanup system including workers and graceful shutdown
pub struct CleanupSystem {
    /// Cancellation token for graceful shutdown
    cancel_token: CancellationToken,
    /// Worker task handles
    worker_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl CleanupSystem {
    /// Create a new cleanup system (call start_workers to begin processing)
    pub fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            worker_handles: Vec::new(),
        }
    }

    /// Get a clone of the cancellation token for use when starting workers
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Add a worker handle to track
    pub fn add_worker_handle(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.worker_handles.push(handle);
    }

    /// Initiate graceful shutdown of all workers
    pub async fn shutdown(self) {
        tracing::info!("Initiating cleanup system shutdown...");

        // Signal all workers to stop
        self.cancel_token.cancel();

        // Wait for all workers to finish
        for (i, handle) in self.worker_handles.into_iter().enumerate() {
            match handle.await {
                Ok(()) => {
                    tracing::debug!("Cleanup worker {} shut down successfully", i);
                }
                Err(e) => {
                    tracing::warn!("Cleanup worker {} task failed: {}", i, e);
                }
            }
        }

        tracing::info!("Cleanup system shutdown complete");
    }

    /// Check if shutdown has been requested
    pub fn is_shutting_down(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
}

impl Default for CleanupSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct WebhookEvent {
    pub event_type: String,
    pub app_id: String,
    pub channel: String,
    pub user_id: Option<String>,
    pub data: sonic_rs::Value,
}
