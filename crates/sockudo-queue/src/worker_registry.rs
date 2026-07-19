use parking_lot::Mutex;
use std::future::Future;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Owns backend worker tasks so queue shutdown never leaves detached consumers.
#[derive(Default)]
pub(crate) struct WorkerRegistry {
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl WorkerRegistry {
    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.handles.lock().push(tokio::spawn(future));
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) {
        let handles = std::mem::take(&mut *self.handles.lock());
        let deadline = tokio::time::Instant::now() + timeout;
        for mut handle in handles {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut handle).await.is_err() {
                handle.abort();
                let _ = handle.await;
            }
        }
    }
}
