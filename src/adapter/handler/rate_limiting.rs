// src/adapter/handler/rate_limiting.rs
use super::ConnectionHandler;
use crate::app::config::App;
use crate::error::{Error, Result};
use crate::rate_limiter::memory_limiter::MemoryRateLimiter;
use crate::websocket::SocketId;
use std::sync::Arc;
use tracing::{info, warn};

impl ConnectionHandler {
    pub async fn setup_rate_limiting(&self, socket_id: &SocketId, app_config: &App) -> Result<()> {
        if app_config.max_client_events_per_second > 0 {
            let limiter = Arc::new(MemoryRateLimiter::new(
                app_config.max_client_events_per_second,
                1, // Per second
            ));
            self.client_event_limiters
                .insert(socket_id.clone(), limiter);
            info!(
                "Initialized client event rate limiter for socket {}: {} events/sec",
                socket_id, app_config.max_client_events_per_second
            );
        }
        Ok(())
    }

    pub async fn check_client_event_rate_limit(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        event_name: &str,
    ) -> Result<()> {
        if let Some(limiter_arc) = self.client_event_limiters.get(socket_id) {
            // Track rate limit check
            if let Some(ref metrics) = self.metrics {
                let metrics_locked = metrics.lock().await;
                metrics_locked.mark_rate_limit_check(&app_config.id, "client_events");
            }

            let limiter = limiter_arc.value();
            let limit_result = limiter.increment(socket_id.as_ref()).await?;

            if !limit_result.allowed {
                // Track rate limit trigger
                if let Some(ref metrics) = self.metrics {
                    let metrics_locked = metrics.lock().await;
                    metrics_locked.mark_rate_limit_triggered(&app_config.id, "client_events");
                }

                warn!(
                    "Client event rate limit exceeded for socket {}: event '{}'",
                    socket_id, event_name
                );
                return Err(Error::ClientEventRateLimit);
            }
        } else if app_config.max_client_events_per_second > 0 {
            warn!(
                "Client event rate limiter not found for socket {} though app config expects one",
                socket_id
            );
            return Err(Error::Internal("Rate limiter misconfiguration".to_string()));
        }

        Ok(())
    }
}
