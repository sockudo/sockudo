use super::ConnectionHandler;
use sockudo_core::error::Result;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::ProtocolVersion;
use sockudo_protocol::constants::PONG_TIMEOUT;
use sockudo_protocol::messages::PusherMessage;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

impl ConnectionHandler {
    pub async fn setup_initial_timeouts(
        &self,
        socket_id: &SocketId,
        app_config: &sockudo_core::app::App,
    ) -> Result<()> {
        // Set activity timeout
        self.set_activity_timeout(&app_config.id, socket_id).await?;

        // Set user authentication timeout if required
        if app_config.user_authentication_enabled() {
            let auth_timeout = self.server_options.user_authentication_timeout;
            self.set_user_authentication_timeout(&app_config.id, socket_id, auth_timeout)
                .await?;
        }

        Ok(())
    }

    pub async fn set_activity_timeout(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        // Clear any existing timeout
        self.clear_activity_timeout(app_id, socket_id).await?;

        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, app_id)
            .await
        else {
            return Ok(());
        };

        if connection.protocol_version == ProtocolVersion::V2 {
            debug!(
                socket_id = %socket_id,
                "native websocket heartbeat handles v2 activity timeout"
            );
            return Ok(());
        }

        let socket_id_clone = *socket_id;
        let app_id_clone = app_id.to_string();
        let connection_manager = self.connection_manager.clone();
        let handler = self.clone();
        let activity_timeout = self.server_options.activity_timeout;

        let timeout_handle = tokio::spawn(async move {
            // Initial sleep before first check
            sleep(Duration::from_secs(activity_timeout)).await;

            loop {
                // Check if connection still exists and get actual inactivity time
                let conn_manager = Arc::clone(&connection_manager);
                let conn = match conn_manager
                    .get_connection(&socket_id_clone, &app_id_clone)
                    .await
                {
                    Some(c) => c,
                    None => {
                        // Connection already cleaned up, nothing to do
                        return;
                    }
                };

                // Check actual time since last activity
                let time_since_activity = {
                    let ws = conn.inner.lock().await;
                    ws.state.time_since_last_ping()
                };

                // If less than activity timeout seconds have passed since last activity, wait more
                if time_since_activity < Duration::from_secs(activity_timeout) {
                    let remaining = Duration::from_secs(activity_timeout) - time_since_activity;
                    debug!(
                        socket_id = %socket_id_clone,
                        inactive_seconds = time_since_activity.as_secs(),
                        remaining_seconds = remaining.as_secs(),
                        "socket remains active"
                    );
                    drop(conn_manager);
                    sleep(remaining).await;
                    // Continue to check again without additional delay
                    continue;
                }

                // Truly inactive for activity timeout duration, send ping
                let ping_result = {
                    let mut ws = conn.inner.lock().await;
                    if !ws.is_connected() {
                        debug!(socket_id = %socket_id_clone, "closed connection cleanup started");
                        drop(ws);
                        handler.spawn_disconnect(
                            &app_id_clone,
                            socket_id_clone,
                            "closed connection",
                        );
                        break;
                    }
                    ws.state.status = sockudo_core::websocket::ConnectionStatus::PingSent(
                        std::time::Instant::now(),
                    );
                    let ping_message = PusherMessage::ping();
                    ws.send_message(&ping_message)
                };

                match ping_result {
                    Ok(_) => {
                        debug!(
                            socket_id = %socket_id_clone,
                            "activity timeout ping sent"
                        );

                        // Release locks before waiting for pong
                        drop(conn_manager);

                        // Wait for pong response
                        sleep(Duration::from_secs(PONG_TIMEOUT)).await;

                        // Re-acquire lock to check pong status
                        let conn_manager = Arc::clone(&connection_manager);
                        if let Some(conn) = conn_manager
                            .get_connection(&socket_id_clone, &app_id_clone)
                            .await
                        {
                            let mut ws = conn.inner.lock().await;
                            // Check if we're still in PingSent state (no pong received)
                            if let sockudo_core::websocket::ConnectionStatus::PingSent(ping_time) =
                                ws.state.status
                                && ping_time.elapsed() > Duration::from_secs(PONG_TIMEOUT)
                            {
                                warn!(
                                    socket_id = %socket_id_clone,
                                    timeout_seconds = PONG_TIMEOUT,
                                    "pong timeout reached, forcing cleanup"
                                );
                                let _ = Box::pin(
                                    ws.close(4201, "Pong reply not received in time".to_string()),
                                )
                                .await;
                                drop(ws);
                                handler.spawn_disconnect(
                                    &app_id_clone,
                                    socket_id_clone,
                                    "pong timeout",
                                );
                                break;
                            }
                        }
                        // After handling ping/pong, wait full activity timeout before next check
                        drop(conn_manager);
                        sleep(Duration::from_secs(activity_timeout)).await;
                    }
                    Err(e) => {
                        // Connection is broken (e.g., broken pipe)
                        // This is expected when client disconnects abruptly
                        debug!(
                            socket_id = %socket_id_clone,
                            error = %e,
                            "activity timeout ping send failed"
                        );

                        handler.spawn_disconnect(
                            &app_id_clone,
                            socket_id_clone,
                            "ping send failed",
                        );
                        break; // Cleanup runs in its own task
                    }
                }
            }
        });

        // Store the timeout handle
        let conn_manager = &self.connection_manager;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.inner.lock().await;
            ws.state.timeouts.activity_timeout_handle = Some(timeout_handle);
        }

        Ok(())
    }

    /// Runs `handle_disconnect` in its own task. The activity-timeout task must not run it
    /// inline: `handle_disconnect` marks the connection `disconnecting` and then aborts the
    /// activity task (`clear_activity_timeout`), so any later await that is pending (a contended
    /// connection lock) dropped the cleanup half-done. The connection then stayed in the adapter
    /// and its presence channels forever, because every later disconnect saw `disconnecting`.
    fn spawn_disconnect(&self, app_id: &str, socket_id: SocketId, reason: &'static str) {
        let handler = self.clone();
        let app_id = app_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = handler.handle_disconnect(&app_id, &socket_id).await {
                warn!(socket_id = %socket_id, error = %e, reason, "activity timeout disconnect failed");
            }
        });
    }

    pub async fn clear_activity_timeout(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        let conn_manager = &self.connection_manager;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.inner.lock().await;
            ws.state.timeouts.clear_activity_timeout();
        }
        Ok(())
    }

    pub async fn update_activity_timeout(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        // Update last activity time
        let conn_manager = &self.connection_manager;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.inner.lock().await;
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
        let socket_id_clone = *socket_id;
        let app_id_clone = app_id.to_string();
        let connection_manager = self.connection_manager.clone();

        // Clear any existing auth timeout
        self.clear_user_authentication_timeout(app_id, socket_id)
            .await?;

        let timeout_handle = tokio::spawn(async move {
            sleep(Duration::from_secs(timeout_seconds)).await;

            let conn_manager = connection_manager;
            if let Some(conn) = conn_manager
                .get_connection(&socket_id_clone, &app_id_clone)
                .await
            {
                let mut ws = conn.inner.lock().await;

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
        let conn_manager = &self.connection_manager;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.inner.lock().await;
            ws.state.timeouts.auth_timeout_handle = Some(timeout_handle);
        }

        Ok(())
    }

    pub async fn clear_user_authentication_timeout(
        &self,
        app_id: &str,
        socket_id: &SocketId,
    ) -> Result<()> {
        let conn_manager = &self.connection_manager;
        if let Some(conn) = conn_manager.get_connection(socket_id, app_id).await {
            let mut ws = conn.inner.lock().await;
            ws.state.timeouts.clear_auth_timeout();
        }
        Ok(())
    }

    pub async fn handle_ping_frame(
        &self,
        socket_id: &SocketId,
        app_config: &sockudo_core::app::App,
    ) -> Result<()> {
        // sockudo-ws automatically sends the native Pong; only update activity here.
        self.update_activity_timeout(&app_config.id, socket_id)
            .await?;

        let conn_manager = &self.connection_manager;
        if let Some(conn) = conn_manager.get_connection(socket_id, &app_config.id).await {
            let mut ws = conn.inner.lock().await;
            ws.state.mark_alive();
        }

        Ok(())
    }
}
