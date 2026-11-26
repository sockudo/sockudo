use sockudo_core::error::Result;
use sockudo_core::options::REDIS_HEALTH_CHECK_RETRY_DELAY_MS;
use std::{future::Future, time::Duration};
use tokio::time::{Instant, sleep, timeout};
use tracing::warn;

pub(super) async fn check_health<C, Connect, Ping>(
    adapter: &'static str,
    budget: Duration,
    attempts: u32,
    connect: impl Fn() -> Connect,
    ping: impl Fn(C) -> Ping,
) -> Result<()>
where
    Connect: Future<Output = Result<C>>,
    Ping: Future<Output = Result<()>>,
{
    let started_at = Instant::now();
    let retry_delay = Duration::from_millis(REDIS_HEALTH_CHECK_RETRY_DELAY_MS);
    for attempt in 1..attempts {
        // Sentinel reconnection can take longer than a single PING attempt.
        let result = match connect().await {
            Ok(conn) => {
                let remaining = budget.saturating_sub(started_at.elapsed());
                let retries_left = attempts - attempt;
                let delay_budget = retry_delay * retries_left;
                if remaining <= delay_budget {
                    return ping(conn).await;
                }
                let attempt_timeout = (remaining - delay_budget) / (retries_left + 1);
                timeout(attempt_timeout, ping(conn)).await
            }
            Err(error) => Ok(Err(error)),
        };

        match result {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(_)) => {
                // Redis errors can contain response data.
                warn!(
                    adapter,
                    attempt,
                    retry_delay_ms = REDIS_HEALTH_CHECK_RETRY_DELAY_MS,
                    "redis adapter health check failed, retrying"
                );
            }
            Err(error) => {
                warn!(adapter, attempt, retry_delay_ms = REDIS_HEALTH_CHECK_RETRY_DELAY_MS, error = %error, "redis adapter health check timed out, retrying");
            }
        }
        sleep(retry_delay).await;
    }

    // The outer timeout bounds the final attempt.
    ping(connect().await?).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::error::Error;
    use std::{
        cell::Cell,
        future::{pending, ready},
    };

    #[tokio::test]
    async fn slow_connection_setup_can_use_the_overall_deadline() {
        // A 1.2 s rebuild exceeds the first PING's 950 ms budget.
        for slow_attempt in [1, 2] {
            let connections = Cell::new(0);
            let budget = Duration::from_secs(2);
            let check = check_health(
                "redis",
                budget,
                2,
                || async {
                    let count = connections.get() + 1;
                    connections.set(count);
                    if count == slow_attempt {
                        sleep(Duration::from_millis(1200)).await;
                    }
                    Ok(count)
                },
                |connection| {
                    ready(if connection == slow_attempt {
                        Ok(())
                    } else {
                        Err(Error::Redis("connection invalidated".to_string()))
                    })
                },
            );

            timeout(budget, check).await.unwrap().unwrap();
            assert_eq!(connections.get(), slow_attempt);
        }
    }

    #[tokio::test]
    async fn retries_a_stalled_ping_then_recovers() {
        let pings = Cell::new(0);
        let budget = Duration::from_secs(2);
        let check = check_health(
            "redis",
            budget,
            2,
            || ready(Ok(())),
            |()| async {
                let attempt = pings.get() + 1;
                pings.set(attempt);
                if attempt == 1 {
                    pending().await
                } else {
                    Ok(())
                }
            },
        );

        timeout(budget, check).await.unwrap().unwrap();
        assert_eq!(pings.get(), 2);
    }

    #[tokio::test]
    async fn returns_the_last_error_after_the_configured_attempts() {
        for attempts in [1, 2, 6] {
            let pings = Cell::new(0);
            let budget = Duration::from_secs(2);
            let check = check_health(
                "redis",
                budget,
                attempts,
                || ready(Ok(())),
                |()| {
                    let attempt = pings.get() + 1;
                    pings.set(attempt);
                    ready(Err(Error::Redis(format!("attempt {attempt}"))))
                },
            );

            let result = timeout(budget, check).await.unwrap();
            assert!(
                matches!(result, Err(Error::Redis(message)) if message == format!("attempt {attempts}"))
            );
            assert_eq!(pings.get(), attempts);
        }
    }

    #[tokio::test]
    async fn overall_deadline_bounds_both_connection_setup_and_ping() {
        let budget = Duration::from_millis(600);
        let check = check_health("redis", budget, 2, pending::<Result<()>>, |()| {
            ready(Ok(()))
        });
        assert!(timeout(budget, check).await.is_err());

        let check = check_health(
            "redis",
            budget,
            2,
            || ready(Ok(())),
            |()| pending::<Result<()>>(),
        );
        assert!(timeout(budget, check).await.is_err());
    }
}
