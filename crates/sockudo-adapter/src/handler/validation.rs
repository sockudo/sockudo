// src/adapter/handler/validation.rs
use super::ConnectionHandler;
use super::types::*;
use sockudo_core::app::{App, ChannelNamespace};
use sockudo_core::channel::ChannelType;
use sockudo_core::error::{Error, Result};
use sockudo_core::utils;
use sockudo_core::websocket::ConnectionCapabilities;
use sockudo_protocol::ProtocolVersion;
use sockudo_protocol::constants::*;
use sonic_rs::Value;
use sonic_rs::prelude::*;

fn validate_namespace_permission(
    namespace: &ChannelNamespace,
    channel: &str,
    action: &str,
) -> Result<()> {
    if action == "subscribe" && namespace.allow_subscribe_for_client == Some(false) {
        return Err(Error::Auth(format!(
            "Namespace '{}' does not allow client subscriptions",
            namespace.name
        )));
    }

    if action == "publish" && namespace.allow_publish_for_client == Some(false) {
        return Err(Error::Auth(format!(
            "Namespace '{}' does not allow client publishes",
            namespace.name
        )));
    }

    if action == "subscribe"
        && channel.starts_with("presence-")
        && namespace.allow_presence_for_client == Some(false)
    {
        return Err(Error::Auth(format!(
            "Namespace '{}' does not allow client presence subscriptions",
            namespace.name
        )));
    }

    Ok(())
}

fn validate_capability_permission(
    capabilities: &ConnectionCapabilities,
    channel: &str,
    action: &str,
) -> Result<()> {
    let allowed = match action {
        "publish" => capabilities.allows_publish(channel),
        _ => capabilities.allows_subscribe(channel),
    };

    if allowed {
        Ok(())
    } else {
        Err(Error::Auth(format!(
            "Connection is not allowed to {} channel '{}'",
            action, channel
        )))
    }
}

impl ConnectionHandler {
    pub async fn validate_subscription_request(
        &self,
        socket_id: &sockudo_core::websocket::SocketId,
        app_config: &App,
        request: &SubscriptionRequest,
    ) -> Result<()> {
        if !app_config.enabled {
            return Err(Error::ApplicationDisabled);
        }

        if utils::is_meta_channel(&request.channel) {
            let Some(connection) = self
                .connection_manager
                .get_connection(socket_id, &app_config.id)
                .await
            else {
                return Err(Error::ConnectionNotFound);
            };

            if connection.protocol_version != ProtocolVersion::V2 {
                return Err(Error::Channel(
                    "Metachannel subscriptions are only supported on protocol V2".into(),
                ));
            }

            return Ok(());
        }

        let connection = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await;

        let is_v2 = connection
            .as_ref()
            .is_some_and(|connection| connection.protocol_version == ProtocolVersion::V2);

        let is_wildcard = utils::is_wildcard_subscription_pattern(&request.channel);
        if is_wildcard {
            let Some(connection) = connection.as_ref() else {
                return Err(Error::ConnectionNotFound);
            };

            if connection.protocol_version != ProtocolVersion::V2 {
                return Err(Error::Channel(
                    "Wildcard subscriptions are only supported on protocol V2".into(),
                ));
            }

            utils::validate_wildcard_subscription_pattern(&request.channel)?;
        } else {
            utils::validate_channel_name(app_config, &request.channel).await?;
        }

        // Check if authentication is required and provided
        let requires_auth =
            request.channel.starts_with("presence-") || request.channel.starts_with("private-");

        if requires_auth && request.auth.is_none() {
            return Err(Error::Auth(
                "Authentication signature required for this channel".into(),
            ));
        }

        if is_v2 {
            self.validate_v2_namespace_permissions(app_config, &request.channel)
                .await?;
            self.validate_v2_capability(socket_id, app_config, &request.channel, "subscribe")
                .await?;
        }

        self.validate_v2_channel_access(socket_id, app_config, &request.channel)
            .await?;

        Ok(())
    }

    pub async fn validate_presence_subscription(
        &self,
        app_config: &App,
        request: &SubscriptionRequest,
    ) -> Result<()> {
        let channel_data = request.channel_data.as_ref().ok_or_else(|| {
            Error::InvalidMessageFormat("Missing channel_data for presence channel".into())
        })?;

        // Parse and validate user info size
        let user_info_payload: Value = sonic_rs::from_str(channel_data).map_err(|_| {
            Error::InvalidMessageFormat("Invalid channel_data JSON for presence".into())
        })?;

        let user_info = user_info_payload
            .get("user_info")
            .cloned()
            .unwrap_or_default();
        let user_info_size_kb = utils::data_to_bytes_flexible(vec![user_info]) / 1024;

        if let Some(max_size) = app_config.max_presence_member_size_limit_kb()
            && user_info_size_kb > max_size as usize
        {
            return Err(Error::Channel(format!(
                "Presence member data size ({user_info_size_kb}KB) exceeds limit ({max_size}KB)"
            )));
        }

        // Check member count limit
        if let Some(max_members) = app_config.max_presence_members_limit() {
            let current_count = self
                .get_channel_member_count(app_config, &request.channel)
                .await?;
            if current_count >= max_members as usize {
                return Err(Error::OverCapacity);
            }
        }

        Ok(())
    }

    pub async fn validate_client_event(
        &self,
        socket_id: &sockudo_core::websocket::SocketId,
        app_config: &App,
        request: &ClientEventRequest,
    ) -> Result<()> {
        // Check if client events are enabled
        if !app_config.client_messages_enabled() {
            return Err(Error::ClientEvent(
                "Client events are not enabled for this app".into(),
            ));
        }

        // Check for reserved prefixes that clients cannot use
        if request.event.starts_with("pusher:")
            || request.event.starts_with("pusher_internal:")
            || request.event.starts_with("sockudo:")
            || request.event.starts_with("sockudo_internal:")
        {
            return Err(Error::InvalidEventName(
                "Client events cannot use reserved prefixes".into(),
            ));
        }

        // Validate event name
        if !request.event.starts_with(CLIENT_EVENT_PREFIX) {
            return Err(Error::InvalidEventName(
                "Client events must start with 'client-'".into(),
            ));
        }

        // Validate event name length
        let max_event_len = app_config
            .event_name_limit()
            .unwrap_or(DEFAULT_EVENT_NAME_MAX_LENGTH as u32);
        if request.event.len() > max_event_len as usize {
            return Err(Error::InvalidEventName(format!(
                "Event name exceeds maximum length of {max_event_len}"
            )));
        }

        // Validate channel name length
        let max_channel_len = app_config
            .max_channel_name_limit()
            .unwrap_or(DEFAULT_CHANNEL_NAME_MAX_LENGTH as u32);
        if request.channel.len() > max_channel_len as usize {
            return Err(Error::InvalidChannelName(format!(
                "Channel name exceeds maximum length of {max_channel_len}"
            )));
        }

        // Validate channel type
        let channel_type = ChannelType::from_name(&request.channel);
        if !matches!(channel_type, ChannelType::Private | ChannelType::Presence) {
            return Err(Error::ClientEvent(
                "Client events can only be sent to private or presence channels".into(),
            ));
        }

        if utils::is_wildcard_subscription_pattern(&request.channel) {
            return Err(Error::ClientEvent(
                "Client events cannot target wildcard subscription channels".into(),
            ));
        }

        if utils::is_meta_channel(&request.channel) {
            return Err(Error::ClientEvent(
                "Client events cannot target metachannels".into(),
            ));
        }

        // Validate payload size
        if let Some(max_payload_kb) = app_config.event_payload_limit_kb() {
            let payload_size = utils::data_to_bytes_flexible(vec![request.data.clone()]);
            if payload_size > (max_payload_kb as usize * 1024) {
                return Err(Error::ClientEvent(format!(
                    "Event payload size ({payload_size} bytes) exceeds limit ({max_payload_kb}KB)"
                )));
            }
        }

        let connection = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await;
        let is_v2 = connection
            .as_ref()
            .is_some_and(|connection| connection.protocol_version == ProtocolVersion::V2);

        if is_v2 {
            self.validate_v2_namespace_publish_permissions(app_config, &request.channel)
                .await?;
            self.validate_v2_capability(socket_id, app_config, &request.channel, "publish")
                .await?;
        }

        self.validate_v2_channel_access(socket_id, app_config, &request.channel)
            .await?;

        Ok(())
    }

    async fn validate_v2_channel_access(
        &self,
        socket_id: &sockudo_core::websocket::SocketId,
        app_config: &App,
        channel: &str,
    ) -> Result<()> {
        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        else {
            // Client event validation is a preflight check. If the socket is already
            // gone by the time we validate, connection lifecycle handling will reject
            // the message on the real send path.
            return Ok(());
        };

        if connection.protocol_version != ProtocolVersion::V2 {
            return Ok(());
        }

        if utils::is_wildcard_subscription_pattern(channel) {
            return Ok(());
        }

        let Some(allowed_users) = utils::channel_user_limit_ids(channel)? else {
            return Ok(());
        };

        let namespace =
            utils::resolve_channel_namespace(app_config, channel)?.ok_or_else(|| {
                Error::Channel("User-limited channels require a defined namespace".into())
            })?;

        if !namespace.allow_user_limited_channels.unwrap_or(false) {
            return Err(Error::Channel(format!(
                "Namespace '{}' does not allow user-limited channels",
                namespace.name
            )));
        }

        let Some(user_id) = connection.get_user_id().await else {
            return Err(Error::Auth(
                "User-limited channels require an authenticated V2 user".into(),
            ));
        };

        if allowed_users.iter().any(|allowed| *allowed == user_id) {
            return Ok(());
        }

        Err(Error::Auth(format!(
            "User '{}' is not allowed to access channel '{}'",
            user_id, channel
        )))
    }

    async fn validate_v2_namespace_permissions(
        &self,
        app_config: &App,
        channel: &str,
    ) -> Result<()> {
        let Some(namespace) = utils::resolve_channel_namespace(app_config, channel)? else {
            return Ok(());
        };

        validate_namespace_permission(namespace, channel, "subscribe")
    }

    async fn validate_v2_namespace_publish_permissions(
        &self,
        app_config: &App,
        channel: &str,
    ) -> Result<()> {
        let Some(namespace) = utils::resolve_channel_namespace(app_config, channel)? else {
            return Ok(());
        };

        validate_namespace_permission(namespace, channel, "publish")
    }

    async fn validate_v2_capability(
        &self,
        socket_id: &sockudo_core::websocket::SocketId,
        app_config: &App,
        channel: &str,
        action: &str,
    ) -> Result<()> {
        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        else {
            return Ok(());
        };

        if connection.protocol_version != ProtocolVersion::V2 {
            return Ok(());
        }

        let Some(capabilities) = connection.get_connection_capabilities().await else {
            return Ok(());
        };

        validate_capability_permission(&capabilities, channel, action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_permission_denies_subscribe_when_flag_disabled() {
        let namespace = ChannelNamespace {
            name: "chat".to_string(),
            channel_name_pattern: None,
            max_channel_name_length: None,
            allow_user_limited_channels: None,
            allow_subscribe_for_client: Some(false),
            allow_publish_for_client: None,
            allow_presence_for_client: None,
        };

        let err =
            validate_namespace_permission(&namespace, "chat:room-1", "subscribe").unwrap_err();
        assert!(
            err.to_string()
                .contains("Namespace 'chat' does not allow client subscriptions")
        );
    }

    #[test]
    fn namespace_permission_denies_publish_when_flag_disabled() {
        let namespace = ChannelNamespace {
            name: "chat".to_string(),
            channel_name_pattern: None,
            max_channel_name_length: None,
            allow_user_limited_channels: None,
            allow_subscribe_for_client: None,
            allow_publish_for_client: Some(false),
            allow_presence_for_client: None,
        };

        let err = validate_namespace_permission(&namespace, "private-chat:room-1", "publish")
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Namespace 'chat' does not allow client publishes")
        );
    }

    #[test]
    fn capability_permission_denies_subscribe_when_pattern_missing() {
        let capabilities = ConnectionCapabilities {
            subscribe: Some(vec!["news:*".to_string()]),
            publish: None,
            presence: None,
        };

        let err =
            validate_capability_permission(&capabilities, "chat:room-1", "subscribe").unwrap_err();
        assert!(
            err.to_string()
                .contains("Connection is not allowed to subscribe channel 'chat:room-1'")
        );
    }

    #[test]
    fn capability_permission_denies_publish_when_pattern_missing() {
        let capabilities = ConnectionCapabilities {
            subscribe: None,
            publish: Some(vec!["private-news:*".to_string()]),
            presence: None,
        };

        let err = validate_capability_permission(&capabilities, "private-chat:room-1", "publish")
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Connection is not allowed to publish channel 'private-chat:room-1'")
        );
    }

    #[test]
    fn capability_permission_allows_matching_presence_pattern() {
        let capabilities = ConnectionCapabilities {
            subscribe: None,
            publish: None,
            presence: Some(vec!["presence-chat:*".to_string()]),
        };

        assert!(
            validate_capability_permission(&capabilities, "presence-chat:room-1", "subscribe")
                .is_ok()
        );
    }
}
