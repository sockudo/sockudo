// src/adapter/handler/authentication.rs
use super::ConnectionHandler;
use super::types::*;
use crate::channel_manager::ChannelManager;
use sockudo_core::app::App;
use sockudo_core::auth::AuthValidator;
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::SocketId;

impl ConnectionHandler {
    pub async fn verify_channel_authentication(
        &self,
        app_config: &App,
        socket_id: &SocketId,
        request: &SubscriptionRequest,
    ) -> Result<bool> {
        // Public channels don't need authentication
        if !request.channel.starts_with("presence-") && !request.channel.starts_with("private-") {
            return Ok(true);
        }

        // Private/presence channels require authentication
        let signature = request.auth.as_ref().ok_or_else(|| {
            Error::Auth("Authentication signature required for this channel".into())
        })?;

        // Create a temporary PusherMessage for signature validation
        let temp_message = sockudo_protocol::messages::PusherMessage {
            channel: Some(request.channel.clone()),
            event: Some("pusher:subscribe".to_string()),
            data: Some(sockudo_protocol::messages::MessageData::Json(
                sonic_rs::json!({
                    "channel": request.channel,
                    "auth": signature,
                    "channel_data": request.channel_data
                }),
            )),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        let is_valid = ChannelManager::signature_is_valid(
            app_config.clone(),
            socket_id,
            signature,
            temp_message,
        );

        Ok(is_valid)
    }

    pub async fn verify_signin_authentication(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: &SignInRequest,
    ) -> Result<()> {
        let auth_validator = AuthValidator::new(self.app_manager.clone());

        // Extract the signature from the auth string (format: "app-key:signature")
        let signature = if let Some(colon_pos) = request.auth.find(':') {
            &request.auth[colon_pos + 1..]
        } else {
            &request.auth
        };

        let is_valid = auth_validator
            .validate_channel_auth(*socket_id, &app_config.key, &request.user_data, signature)
            .await?;

        if !is_valid {
            return Err(Error::Auth("Connection not authorized for signin.".into()));
        }

        Ok(())
    }
}
