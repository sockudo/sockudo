// src/adapter/handler/timeout_management.rs
use super::ConnectionHandler;
use crate::error::Result;
use crate::protocol::messages::PusherMessage;
use crate::websocket::SocketId;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

impl ConnectionHandler {
    pub async fn setup_initial_timeouts(
        &self,
        socket_id: &SocketId,
        app_config: &crate::app::config::App,
    ) -> Result<()> {
        // Set activity timeout
        self.set_activity_timeout(&app_config.id, socket_id).await?;

        // Set user authentication timeout if required
        if app_config.enable_user_authentication.unwrap_or(false) {
            let auth_timeout = self.server_options.user_authentication_timeout;
            self.set_user_authentication_timeout(&app_config.id, socket_id, auth_timeout)
                .await?;
        }

        Ok(())
    }

    pub async fn set_activity_timeout(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        let socket_id_clone = socket_id.clone();
        let app_id_clone = app_id.to_string();
        let connection_manager = self.connection_manager.clone();

        // Clear any existing timeout
        self.clear_activity_timeout(app_id, socket_id).await?;

        let timeout_handle = tokio::spawn(async move {
            sleep(Duration::from_secs(120)).await;

            let mut conn_manager = connection_manager.lock().await;
            if let Some(conn) = conn_manager
                .get_connection(&socket_id_clone, &app_id_clone)
                .await
            {
                let mut ws = conn.0.lock().await;

                // Send ping first, then set a shorter timeout for pong
                let ping_message = PusherMessage::ping();
                if ws.send_message(&ping_message).is_ok() {
                    info!(
                        "Sent ping to socket {} due to activity timeout",
                        socket_id_clone
                    );

                    // Wait for pong response (shorter timeout)
                    drop(ws); // Release the lock before sleeping
                    sleep(Duration::from_secs(30)).await;

                    // Check if we received a pong (activity was updated)
                    if let Some(conn) = conn_manager
                        .get_connection(&socket_id_clone, &app_id_clone)
                        .await
                    {
                        let mut ws = conn.0.lock().await;
                        if ws.state.time_since_last_ping() > Duration::from_secs(140) {
                            // No pong received, close connection
                            let _ = ws
                                .close(4201, "Pong reply not received in time".to_string())
                                .await;
                        }
                    }
                } else {
                    warn!("Failed to send ping to socket {}", socket_id_clone);
                }
            }
        });

        // Store the timeout handle
        let mut conn_manager = self.connection_manager.lock().await;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.0.lock().await;
            ws.state.timeouts.activity_timeout_handle = Some(timeout_handle);
        }

        Ok(())
    }

    pub async fn clear_activity_timeout(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        let mut conn_manager = self.connection_manager.lock().await;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.0.lock().await;
            ws.state.timeouts.clear_activity_timeout();
        }
        Ok(())
    }

    pub async fn update_activity_timeout(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        // Update last activity time
        let mut conn_manager = self.connection_manager.lock().await;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.0.lock().await;
            ws.update_activity();
        }
        Ok(())
    }

    pub async fn set_user_authentication_timeout(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        timeout_seconds: u64,
    ) -> Result<()> {
        let socket_id_clone = socket_id.clone();
        let app_id_clone = app_id.to_string();
        let connection_manager = self.connection_manager.clone();

        // Clear any existing auth timeout
        self.clear_user_authentication_timeout(app_id, socket_id)
            .await?;

        let timeout_handle = tokio::spawn(async move {
            sleep(Duration::from_secs(timeout_seconds)).await;

            let mut conn_manager = connection_manager.lock().await;
            if let Some(conn) = conn_manager
                .get_connection(&socket_id_clone, &app_id_clone)
                .await
            {
                let mut ws = conn.0.lock().await;

                // Check if user is still not authenticated
                if !ws.state.is_authenticated() {
                    let _ = ws
                        .close(
                            4009,
                            "Connection not authorized within timeout.".to_string(),
                        )
                        .await;
                }
            }
        });

        // Store the timeout handle
        let mut conn_manager = self.connection_manager.lock().await;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.0.lock().await;
            ws.state.timeouts.auth_timeout_handle = Some(timeout_handle);
        }

        Ok(())
    }

    pub async fn clear_user_authentication_timeout(
        &self,
        app_id: &str,
        socket_id: &SocketId,
    ) -> Result<()> {
        let mut conn_manager = self.connection_manager.lock().await;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.0.lock().await;
            ws.state.timeouts.clear_auth_timeout();
        }
        Ok(())
    }

    pub async fn handle_ping_frame(
        &self,
        socket_id: &SocketId,
        app_config: &crate::app::config::App,
    ) -> Result<()> {
        // Update activity and send pong
        self.update_activity_timeout(&app_config.id, socket_id)
            .await?;

        let mut conn_manager = self.connection_manager.lock().await;
        if let Some(conn) = conn_manager.get_connection(socket_id, &app_config.id).await {
            let ws = conn.0.lock().await;
            let pong_message = PusherMessage::pong();
            ws.send_message(&pong_message)?;
        }

        Ok(())
    }
}
