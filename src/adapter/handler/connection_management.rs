// src/adapter/handler/connection_management.rs
use super::ConnectionHandler;
use crate::app::config::App;
use crate::error::{Error, Result};
use crate::protocol::messages::PusherMessage;
use crate::websocket::SocketId;
use fastwebsockets::{Frame, Payload, WebSocketWrite};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::io::WriteHalf;
use tracing::warn;

impl ConnectionHandler {
    pub async fn send_message_to_socket(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        self.connection_manager
            .lock()
            .await
            .send_message(app_id, socket_id, message)
            .await
    }

    pub async fn broadcast_to_channel(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
    ) -> Result<()> {
        self.connection_manager
            .lock()
            .await
            .send(channel, message, exclude_socket, &app_config.id)
            .await
    }

    pub async fn close_connection(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        code: u16,
        reason: &str,
    ) -> Result<()> {
        let mut conn_manager = self.connection_manager.lock().await;
        if let Some(conn) = conn_manager.get_connection(socket_id, &app_config.id).await {
            let mut conn_locked = conn.0.lock().await;
            conn_locked
                .close(code, reason.to_string())
                .await
                .map_err(|e| Error::InternalError(format!("Failed to close connection: {}", e)))
        } else {
            warn!("Connection not found for close: {}", socket_id);
            Ok(())
        }
    }

    pub async fn get_channel_member_count(&self, app_config: &App, channel: &str) -> Result<usize> {
        self.connection_manager
            .lock()
            .await
            .get_channel_members(&app_config.id, channel)
            .await
            .map(|members| members.len())
    }

    pub async fn verify_channel_subscription(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
    ) -> Result<()> {
        let is_subscribed = self
            .connection_manager
            .lock()
            .await
            .is_in_channel(&app_config.id, channel, socket_id)
            .await?;

        if !is_subscribed {
            return Err(Error::ClientEventError(format!(
                "Socket {} is not subscribed to channel {}",
                socket_id, channel
            )));
        }

        Ok(())
    }

    async fn send_error_frame(
        ws_tx: &mut WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        error: &Error,
    ) {
        let error_message = PusherMessage::error(error.close_code(), error.to_string(), None);

        if let Ok(payload) = serde_json::to_string(&error_message) {
            let payload = Payload::from(payload.as_bytes());
            if let Err(e) = ws_tx.write_frame(Frame::text(payload.into())).await {
                warn!("Failed to send error frame: {}", e);
            }
        }

        if let Err(e) = ws_tx
            .write_frame(Frame::close(
                error.close_code(),
                error.to_string().as_bytes(),
            ))
            .await
        {
            warn!("Failed to send close frame: {}", e);
        }
    }
}
