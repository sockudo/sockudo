//! Bounded device identity cryptography for asynchronous request handlers.
//!
//! The synchronous domain helpers retain their public behavior. Network handlers
//! use this shared budget so KDF work cannot occupy Tokio's I/O workers or create
//! an unbounded blocking pool backlog.
use crate::domain::{SecretString, hash_device_identity_token, verify_device_identity_token};
use std::sync::{Arc, LazyLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum DeviceIdentityCryptoError {
    #[error("device identity cryptography admission is full")]
    Overloaded,
    #[error("device identity cryptography is unavailable")]
    Unavailable,
    #[error("device identity cryptography worker failed")]
    WorkerFailed,
}

struct CryptoBudget {
    admitted: Arc<Semaphore>,
    running: Arc<Semaphore>,
}

impl CryptoBudget {
    fn new(admitted: usize, running: usize) -> Self {
        Self {
            admitted: Arc::new(Semaphore::new(admitted)),
            running: Arc::new(Semaphore::new(running)),
        }
    }

    fn admit(&self) -> Result<OwnedSemaphorePermit, DeviceIdentityCryptoError> {
        self.admitted
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => DeviceIdentityCryptoError::Overloaded,
                tokio::sync::TryAcquireError::Closed => DeviceIdentityCryptoError::Unavailable,
            })
    }

    async fn run<T: Send + 'static>(
        &self,
        admission: OwnedSemaphorePermit,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, DeviceIdentityCryptoError> {
        let running = self
            .running
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DeviceIdentityCryptoError::Unavailable)?;
        tokio::task::spawn_blocking(move || {
            // Cancellation of the caller cannot release either permit while
            // the non-cancellable cryptographic operation is still running.
            let _admission = admission;
            let _running = running;
            work()
        })
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "device identity cryptography worker failed");
            DeviceIdentityCryptoError::WorkerFailed
        })
    }
}

static DEVICE_CRYPTO: LazyLock<CryptoBudget> = LazyLock::new(|| {
    let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
    CryptoBudget::new(64, (parallelism / 4).clamp(1, 8))
});

/// Hash without blocking async workers; reject admission overload before copying secrets.
pub async fn hash_device_identity_token_async(
    token: &SecretString,
) -> Result<SecretString, DeviceIdentityCryptoError> {
    let admission = DEVICE_CRYPTO.admit()?;
    let token = token.clone();
    DEVICE_CRYPTO
        .run(admission, move || hash_device_identity_token(&token))
        .await
}

/// Verify with the same KDF, iteration floor and comparison as the synchronous API.
pub async fn verify_device_identity_token_async(
    token: &str,
    stored_hash: &SecretString,
) -> Result<bool, DeviceIdentityCryptoError> {
    let admission = DEVICE_CRYPTO.admit()?;
    let token = Zeroizing::new(token.to_owned());
    let stored_hash = stored_hash.clone();
    DEVICE_CRYPTO
        .run(admission, move || {
            verify_device_identity_token(&token, &stored_hash)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn async_identity_preserves_valid_invalid_and_malformed_results() {
        let token = SecretString::new("device identity test token").unwrap();
        let hash = hash_device_identity_token_async(&token).await.unwrap();
        assert!(verify_device_identity_token(token.expose_secret(), &hash));
        assert!(
            verify_device_identity_token_async(token.expose_secret(), &hash)
                .await
                .unwrap()
        );
        assert!(
            !verify_device_identity_token_async("wrong", &hash)
                .await
                .unwrap()
        );
        assert!(!verify_device_identity_token_async("", &hash).await.unwrap());
        assert!(
            !verify_device_identity_token_async("token", &SecretString::new("malformed").unwrap())
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_worker_keeps_capacity_until_crypto_finishes() {
        let budget = Arc::new(CryptoBudget::new(1, 1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let task_budget = budget.clone();
        let task = tokio::spawn(async move {
            let permit = task_budget.admit().unwrap();
            task_budget
                .run(permit, move || {
                    started_tx.send(()).unwrap();
                    finish_rx.recv().unwrap();
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(matches!(
            budget.admit(),
            Err(DeviceIdentityCryptoError::Overloaded)
        ));
        finish_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(permit) = budget.admit() {
                    drop(permit);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn queued_work_is_bounded_and_cancellation_reclaims_admission() {
        let budget = Arc::new(CryptoBudget::new(2, 1));
        let running = budget.running.clone().acquire_owned().await.unwrap();
        let admission = budget.admit().unwrap();
        let other = budget.admit().unwrap();
        assert!(matches!(
            budget.admit(),
            Err(DeviceIdentityCryptoError::Overloaded)
        ));
        let queued_budget = budget.clone();
        let queued = tokio::spawn(async move { queued_budget.run(admission, || ()).await });
        tokio::task::yield_now().await;
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        assert!(budget.admit().is_ok());
        drop(other);
        drop(running);
    }
}
