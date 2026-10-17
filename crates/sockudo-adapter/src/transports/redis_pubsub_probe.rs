//! Liveness probe for long-lived Redis subscriber connections.
//!
//! A subscriber connection only reads. When Redis goes away without a FIN or RST reaching the
//! client (a network partition, or a managed instance replaced behind the same address during a
//! resize, maintenance, or failover) nothing ever fails on that socket: it stays open and
//! delivers nothing, forever. Publishing keeps working because publish connections write, get an
//! error, and reconnect. A periodic PING on the subscriber connection exposes the loss: a
//! replaced host answers it with an RST, a vanished one never answers.

use std::future::Future;
use std::time::Duration;

/// Why a subscriber connection was declared dead.
#[derive(Debug)]
pub(crate) enum PubSubProbeFailure {
    /// The PING failed, for example because the connection was reset.
    Failed(redis::RedisError),
    /// The PING got no answer within one probe interval.
    TimedOut,
}

/// PINGs every `interval` and resolves once a PING fails or is not answered within `interval`.
/// A zero `interval` disables probing: the future then never resolves.
pub(crate) async fn pubsub_connection_lost<F, Fut>(
    interval: Duration,
    mut ping: F,
) -> PubSubProbeFailure
where
    F: FnMut() -> Fut,
    Fut: Future<Output = redis::RedisResult<()>>,
{
    if interval.is_zero() {
        return std::future::pending().await;
    }
    loop {
        tokio::time::sleep(interval).await;
        match tokio::time::timeout(interval, ping()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return PubSubProbeFailure::Failed(error),
            Err(_) => return PubSubProbeFailure::TimedOut,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Instant;

    const INTERVAL: Duration = Duration::from_millis(20);

    #[tokio::test]
    async fn keeps_probing_while_pings_succeed() {
        let pings = Arc::new(AtomicUsize::new(0));
        let counter = pings.clone();
        let probe = pubsub_connection_lost(INTERVAL, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        assert!(tokio::time::timeout(INTERVAL * 6, probe).await.is_err());
        assert!(pings.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn reports_a_failed_ping() {
        let started = Instant::now();
        let failure = pubsub_connection_lost(INTERVAL, || async {
            Err(redis::RedisError::from(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset,
            )))
        })
        .await;
        assert!(matches!(failure, PubSubProbeFailure::Failed(_)));
        assert!(started.elapsed() >= INTERVAL);
    }

    #[tokio::test]
    async fn reports_an_unanswered_ping_after_one_interval() {
        let started = Instant::now();
        let failure = pubsub_connection_lost(INTERVAL, || {
            std::future::pending::<redis::RedisResult<()>>()
        })
        .await;
        assert!(matches!(failure, PubSubProbeFailure::TimedOut));
        assert!(started.elapsed() >= INTERVAL * 2);
    }

    #[tokio::test]
    async fn zero_interval_disables_probing() {
        let pings = Arc::new(AtomicUsize::new(0));
        let counter = pings.clone();
        let probe = pubsub_connection_lost(Duration::ZERO, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        assert!(tokio::time::timeout(INTERVAL * 5, probe).await.is_err());
        assert_eq!(pings.load(Ordering::SeqCst), 0);
    }
}
