// src/adapter/handler/rate_limiting.rs
use super::ConnectionHandler;
use crate::memory_rate_limiter::MemoryRateLimiter;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::rate_limiter::RateLimiter;
use sockudo_core::websocket::SocketId;
use std::sync::Arc;
use tracing::{debug, warn};

impl ConnectionHandler {
    pub async fn setup_message_rate_limiting(
        &self,
        socket_id: &SocketId,
        app_config: &App,
    ) -> Result<()> {
        if let Some(config) = app_config.message_rate_limit()
            && config.enabled
            && config.max_attempts > 0
        {
            let limiter: Arc<dyn RateLimiter + Send + Sync> = Arc::new(MemoryRateLimiter::new(
                config.max_attempts,
                config.decay_seconds,
            ));
            self.message_limiters.insert(*socket_id, limiter);
            debug!(
                socket_id = %socket_id,
                max_attempts = config.max_attempts,
                decay_seconds = config.decay_seconds,
                "message rate limiter initialized"
            );
        }
        Ok(())
    }

    pub async fn check_message_rate_limit(
        &self,
        socket_id: &SocketId,
        app_config: &App,
    ) -> Result<()> {
        if let Some(limiter_arc) = self.message_limiters.get(socket_id) {
            if let Some(ref metrics) = self.metrics {
                metrics.mark_rate_limit_check(&app_config.id, "messages");
            }

            let limit_result = limiter_arc
                .value()
                .increment(&socket_id.to_string())
                .await?;

            if !limit_result.allowed {
                if let Some(ref metrics) = self.metrics {
                    metrics.mark_rate_limit_triggered(&app_config.id, "messages");
                }

                warn!(socket_id = %socket_id, "message rate limit exceeded");

                let terminate = app_config
                    .message_rate_limit()
                    .map(|c| c.terminate_on_limit)
                    .unwrap_or(false);

                if terminate {
                    return Err(Error::ClientEventRateLimitTerminate);
                }
                return Err(Error::ClientEventRateLimit);
            }
        }
        Ok(())
    }

    pub async fn setup_rate_limiting(&self, socket_id: &SocketId, app_config: &App) -> Result<()> {
        if app_config.client_events_per_second_limit() > 0 {
            let limiter = Arc::new(MemoryRateLimiter::new(
                app_config.client_events_per_second_limit(),
                app_config.client_event_decay_seconds(),
            ));
            self.client_event_limiters.insert(*socket_id, limiter);
            debug!(
                socket_id = %socket_id,
                max_attempts = app_config.client_events_per_second_limit(),
                decay_seconds = app_config.client_event_decay_seconds(),
                "client event rate limiter initialized"
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
            if let Some(ref metrics) = self.metrics {
                metrics.mark_rate_limit_check(&app_config.id, "client_events");
            }

            let limiter = limiter_arc.value();
            let limit_result = limiter.increment(&socket_id.to_string()).await?;

            if !limit_result.allowed {
                if let Some(ref metrics) = self.metrics {
                    metrics.mark_rate_limit_triggered(&app_config.id, "client_events");
                }

                warn!(
                    socket_id = %socket_id,
                    event = %event_name,
                    "client event rate limit exceeded"
                );

                if app_config.terminate_on_limit() {
                    return Err(Error::ClientEventRateLimitTerminate);
                }
                return Err(Error::ClientEventRateLimit);
            }
        } else if app_config.client_events_per_second_limit() > 0 {
            warn!(
                socket_id = %socket_id,
                "client event rate limiter not found though app config expects one"
            );
            return Err(Error::Internal("Rate limiter misconfiguration".to_string()));
        }

        Ok(())
    }
}
