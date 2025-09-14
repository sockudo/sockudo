// src/adapter/handler/webhook_management.rs
use super::ConnectionHandler;
use super::types::*;
use crate::app::config::App;
use crate::error::Result;
use crate::websocket::SocketId;
use tracing::warn;

impl ConnectionHandler {
    pub async fn send_client_event_webhook(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: &ClientEventRequest,
    ) -> Result<()> {
        if let Some(webhook_integration) = &self.webhook_integration {
            // Get user_id for presence channels - clone the string to avoid lifetime issues
            let user_id = if request.channel.starts_with("presence-") {
                let mut connection_manager = self.connection_manager.lock().await;
                if let Some(conn_arc) = connection_manager
                    .get_connection(socket_id, &app_config.id)
                    .await
                {
                    let conn_locked = conn_arc.inner.lock().await;
                    conn_locked
                        .state
                        .presence
                        .as_ref()
                        .and_then(|p_map| p_map.get(&request.channel))
                        .map(|pi| pi.user_id.clone()) // Clone the String instead of borrowing
                } else {
                    None
                }
            } else {
                None
            };

            webhook_integration
                .send_client_event(
                    app_config,
                    &request.channel,
                    &request.event,
                    request.data.clone(),
                    Some(socket_id.as_ref()),
                    user_id.as_deref(), // Convert Option<String> to Option<&str>
                )
                .await
                .unwrap_or_else(|e| {
                    warn!(
                        "Failed to send client_event webhook for {}: {}",
                        request.channel, e
                    );
                });
        }

        Ok(())
    }

    pub async fn send_client_event_webhook_static(
        webhook_integration: &std::sync::Arc<crate::webhook::integration::WebhookIntegration>,
        socket_id: &SocketId,
        app_config: &App,
        request: &ClientEventRequest,
    ) -> Result<()> {
        // For async spawned webhooks, we skip presence user_id lookup to avoid
        // holding connection manager locks across spawn boundaries
        webhook_integration
            .send_client_event(
                app_config,
                &request.channel,
                &request.event,
                request.data.clone(),
                Some(socket_id.as_ref()),
                None, // Skip user_id for async webhooks to avoid lock dependencies
            )
            .await
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to send client_event webhook for {}: {}",
                    request.channel, e
                );
            });

        Ok(())
    }
}
