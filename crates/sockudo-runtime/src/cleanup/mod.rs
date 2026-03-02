use sockudo_types::socket::SocketId;
use std::time::Instant;
use tokio::sync::mpsc;

pub mod multi_worker;
pub mod worker;

// Re-export CancellationToken for use by callers who want graceful shutdown
pub use sockudo_core::cleanup::{CleanupConfig, WorkerThreadsConfig};
pub use tokio_util::sync::CancellationToken;

/// Unified cleanup sender that abstracts over single vs multi-worker implementations
#[derive(Clone)]
pub enum CleanupSender {
    /// Direct sender for single worker (optimized path)
    Direct(mpsc::Sender<DisconnectTask>),
    /// Multi-worker sender with round-robin distribution
    Multi(multi_worker::MultiWorkerSender),
}

impl CleanupSender {
    /// Send a disconnect task to the cleanup system
    pub fn try_send(
        &self,
        task: DisconnectTask,
    ) -> Result<(), Box<mpsc::error::TrySendError<DisconnectTask>>> {
        match self {
            CleanupSender::Direct(sender) => sender.try_send(task).map_err(Box::new),
            CleanupSender::Multi(sender) => {
                // Convert MultiWorkerSender's SendError to TrySendError
                sender.send(task).map_err(|e| {
                    // MultiWorkerSender returns SendError when all queues are full or closed
                    // We treat this as "Full" for backpressure handling
                    Box::new(mpsc::error::TrySendError::Full(e.0))
                })
            }
        }
    }

    /// Check if the sender is still operational
    pub fn is_closed(&self) -> bool {
        match self {
            CleanupSender::Direct(sender) => sender.is_closed(),
            CleanupSender::Multi(sender) => !sender.is_available(),
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
    /// This will:
    /// 1. Signal all workers to stop via the cancellation token
    /// 2. Wait for all workers to finish processing their final batches
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
pub struct DisconnectTask {
    pub socket_id: SocketId,
    pub app_id: String,
    pub subscribed_channels: Vec<String>,
    pub user_id: Option<String>,
    pub timestamp: Instant,
    pub connection_info: Option<ConnectionCleanupInfo>,
}

#[derive(Debug, Clone)]
pub struct ConnectionCleanupInfo {
    pub presence_channels: Vec<String>,
    pub auth_info: Option<AuthInfo>,
}

#[derive(Debug, Clone)]
pub struct AuthInfo {
    pub user_id: String,
    pub user_info: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WebhookEvent {
    pub event_type: String,
    pub app_id: String,
    pub channel: String,
    pub user_id: Option<String>,
    pub data: sonic_rs::Value,
}
