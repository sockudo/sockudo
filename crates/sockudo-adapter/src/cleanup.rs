use crossfire::mpsc;
use sockudo_core::websocket::SocketId;
use std::time::Instant;

pub type CleanupChannelFlavor = mpsc::Array<DisconnectTask>;
pub type CleanupSenderHandle = crossfire::MAsyncTx<CleanupChannelFlavor>;
pub type CleanupReceiverHandle = crossfire::AsyncRx<CleanupChannelFlavor>;

/// Unified cleanup sender that abstracts over single vs multi-worker implementations
#[derive(Clone)]
pub enum CleanupSender {
    /// Direct sender for single worker (optimized path)
    Direct(CleanupSenderHandle),
    /// Multi-worker sender with round-robin distribution
    Multi(MultiWorkerSender),
}

impl CleanupSender {
    /// Send a disconnect task to the cleanup system
    pub fn try_send(
        &self,
        task: DisconnectTask,
    ) -> Result<(), Box<crossfire::TrySendError<DisconnectTask>>> {
        match self {
            CleanupSender::Direct(sender) => sender.try_send(task).map_err(Box::new),
            CleanupSender::Multi(sender) => sender
                .send(task)
                .map_err(|e| Box::new(crossfire::TrySendError::Full(*e.0))),
        }
    }

    /// Check if the sender is still operational
    pub fn is_closed(&self) -> bool {
        match self {
            CleanupSender::Direct(sender) => sender.is_disconnected(),
            CleanupSender::Multi(sender) => !sender.is_available(),
        }
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

/// Multi-worker sender for distributing disconnect tasks across multiple workers
#[derive(Clone)]
pub struct MultiWorkerSender {
    senders: Vec<CleanupSenderHandle>,
    next_worker: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Error returned when all worker queues are full or closed
pub struct SendError(pub Box<DisconnectTask>);

impl MultiWorkerSender {
    pub fn new(senders: Vec<CleanupSenderHandle>) -> Self {
        Self {
            senders,
            next_worker: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Send a task using round-robin distribution
    pub fn send(&self, task: DisconnectTask) -> Result<(), SendError> {
        if self.senders.is_empty() {
            return Err(SendError(Box::new(task)));
        }

        let start = self
            .next_worker
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let len = self.senders.len();

        // Try round-robin, then try all others
        for i in 0..len {
            let idx = (start + i) % len;
            match self.senders[idx].try_send(task.clone()) {
                Ok(()) => return Ok(()),
                Err(_) => continue,
            }
        }

        Err(SendError(Box::new(task)))
    }

    pub fn is_available(&self) -> bool {
        self.senders.iter().any(|s| !s.is_disconnected())
    }

    /// Send a disconnect task with fallback to other workers if one fails
    pub fn send_with_fallback(
        &self,
        task: DisconnectTask,
    ) -> Result<(), SendError> {
        self.send(task)
    }

    /// Get the number of workers
    pub fn worker_count(&self) -> usize {
        self.senders.len()
    }

    /// Get statistics about worker availability
    pub fn get_worker_stats(&self) -> WorkerStats {
        let total = self.senders.len();
        let available = self
            .senders
            .iter()
            .filter(|sender| !sender.is_disconnected())
            .count();
        let closed = total - available;

        WorkerStats {
            total_workers: total,
            available_workers: available,
            closed_workers: closed,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(senders: Vec<CleanupSenderHandle>) -> Self {
        Self {
            senders,
            next_worker: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

/// Statistics about worker availability
#[derive(Debug, Clone)]
pub struct WorkerStats {
    pub total_workers: usize,
    pub available_workers: usize,
    pub closed_workers: usize,
}
