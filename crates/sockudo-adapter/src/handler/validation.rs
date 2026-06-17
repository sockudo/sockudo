// src/adapter/handler/validation.rs
use super::ConnectionHandler;
use super::types::*;
use sockudo_core::app::{App, ChannelNamespace};
use sockudo_core::channel::ChannelType;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::HistoryConfig;
use sockudo_core::utils;
use sockudo_core::websocket::ConnectionCapabilities;
use sockudo_protocol::ProtocolVersion;
use sockudo_protocol::constants::*;
use sockudo_protocol::messages::{
    AI_ERROR_EVENT_NOT_PERMITTED, AI_ERROR_INVALID_TRANSPORT_HEADER, AiHeaderValidationError,
    AiPublishTrust, PusherMessage, is_ai_client_publish_event,
};
use sonic_rs::Value;
use sonic_rs::prelude::*;

fn record_ai_rejection(handler: &ConnectionHandler, app_id: &str, code: u32) {
    if let Some(metrics) = handler.metrics() {
        metrics.mark_ai_transport_rejected(app_id, code);
    }
}

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
        "annotation_subscribe" => {
            capabilities.allows_subscribe(channel)
                && capabilities.allows_annotation_subscribe(channel)
        }
        "history" => capabilities.allows_history(channel),
        "presence" => capabilities
            .presence
            .as_deref()
            .is_none_or(|patterns| ConnectionCapabilities::matches_any(patterns, channel)),
        "publish" => capabilities.allows_publish(channel),
        _ => capabilities
            .subscribe
            .as_deref()
            .is_none_or(|patterns| ConnectionCapabilities::matches_any(patterns, channel)),
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

fn validate_rewind_request(
    app_config: &App,
    channel: &str,
    is_v2: bool,
    global_history: &HistoryConfig,
) -> Result<()> {
    if !is_v2 {
        return Err(Error::Channel(
            "Channel rewind is only supported on protocol V2".into(),
        ));
    }

    if !app_config
        .resolved_history(channel, global_history)
        .rewind_allowed()
    {
        return Err(Error::Channel(format!(
            "Channel rewind is disabled by policy for channel '{}'",
            channel
        )));
    }

    Ok(())
}

impl ConnectionHandler {
    pub async fn validate_ai_event_publish(
        &self,
        socket_id: &sockudo_core::websocket::SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let channel = message.channel.as_deref().ok_or_else(|| {
            record_ai_rejection(self, &app_config.id, AI_ERROR_INVALID_TRANSPORT_HEADER);
            Error::AiTransport {
                code: AI_ERROR_INVALID_TRANSPORT_HEADER,
                name: "ai_invalid_transport_header",
                message: "channel required for AI event".to_string(),
            }
        })?;
        let event = message.event.as_deref().ok_or_else(|| {
            record_ai_rejection(self, &app_config.id, AI_ERROR_INVALID_TRANSPORT_HEADER);
            Error::AiTransport {
                code: AI_ERROR_INVALID_TRANSPORT_HEADER,
                name: "ai_invalid_transport_header",
                message: "event required for AI event".to_string(),
            }
        })?;

        if !self.server_options.ai_transport.matches_channel(channel) {
            record_ai_rejection(self, &app_config.id, AI_ERROR_EVENT_NOT_PERMITTED);
            return Err(Error::AiTransport {
                code: AI_ERROR_EVENT_NOT_PERMITTED,
                name: "ai_event_not_permitted",
                message: "AI Transport is not enabled for this channel".to_string(),
            });
        }

        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        else {
            return Err(Error::ConnectionNotFound);
        };

        if connection.protocol_version != ProtocolVersion::V2 {
            record_ai_rejection(self, &app_config.id, AI_ERROR_EVENT_NOT_PERMITTED);
            return Err(Error::AiTransport {
                code: AI_ERROR_EVENT_NOT_PERMITTED,
                name: "ai_event_not_permitted",
                message: "AI events require protocol v2".to_string(),
            });
        }

        if !is_ai_client_publish_event(event) {
            record_ai_rejection(self, &app_config.id, AI_ERROR_EVENT_NOT_PERMITTED);
            return Err(Error::AiTransport {
                code: AI_ERROR_EVENT_NOT_PERMITTED,
                name: "ai_event_not_permitted",
                message: "agent AI events require trusted app-key publish".to_string(),
            });
        }

        let verified_client_id = connection.get_user_id().await.ok_or_else(|| {
            record_ai_rejection(self, &app_config.id, AI_ERROR_INVALID_TRANSPORT_HEADER);
            Error::Auth("AI client events require an authenticated client_id".to_string())
        })?;

        message.validate_ai_headers().map_err(|error| {
            record_ai_rejection(self, &app_config.id, error.code);
            ai_header_error(error)
        })?;
        validate_ai_client_id_headers(
            message,
            Some(verified_client_id.as_str()),
            AiPublishTrust::Client,
        )
        .map_err(|error| {
            record_ai_rejection(self, &app_config.id, error.code);
            ai_header_error(error)
        })?;

        self.validate_v2_namespace_publish_permissions(app_config, channel)
            .await?;
        self.validate_v2_capability(socket_id, app_config, channel, "publish")
            .await?;
        self.validate_v2_channel_access(socket_id, app_config, channel)
            .await?;

        if let Some(metrics) = self.metrics() {
            metrics.mark_ai_transport_validated(&app_config.id, event);
        }

        Ok(())
    }

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

        if request.rewind.is_some() {
            validate_rewind_request(
                app_config,
                &request.channel,
                is_v2,
                &self.server_options().history,
            )?;
        }
        if request.annotation_subscribe {
            if !self.server_options().annotations.enabled {
                return Err(Error::Channel(format!(
                    "Annotations are disabled globally for channel '{}'",
                    request.channel
                )));
            }
            if !app_config.annotations_enabled_for_channel(&request.channel) {
                return Err(Error::Channel(format!(
                    "Annotations are disabled by channel policy for channel '{}'",
                    request.channel
                )));
            }
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
            if request.channel.starts_with("presence-") {
                self.validate_v2_capability(socket_id, app_config, &request.channel, "presence")
                    .await?;
            }
            if request.rewind.is_some() {
                self.validate_v2_capability(socket_id, app_config, &request.channel, "history")
                    .await?;
            }
            if request.annotation_subscribe {
                self.validate_v2_capability(
                    socket_id,
                    app_config,
                    &request.channel,
                    "annotation_subscribe",
                )
                .await?;
            }
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

    pub(crate) async fn validate_v2_channel_access(
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

    pub(crate) async fn validate_v2_capability(
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
            if action == "annotation_subscribe" {
                return Err(Error::Auth(format!(
                    "Connection is not allowed to receive raw annotations for channel '{channel}'"
                )));
            }
            return Ok(());
        };

        validate_capability_permission(&capabilities, channel, action)
    }
}

pub fn ai_header_error(error: AiHeaderValidationError) -> Error {
    Error::AiTransport {
        code: error.code,
        name: error.name,
        message: error.message,
    }
}

pub fn validate_ai_client_id_headers(
    message: &PusherMessage,
    verified_client_id: Option<&str>,
    trust: AiPublishTrust,
) -> std::result::Result<(), AiHeaderValidationError> {
    if matches!(trust, AiPublishTrust::TrustedApp) {
        return Ok(());
    }

    let Some(headers) = message.ai_transport_headers() else {
        return Ok(());
    };

    for (key, value) in headers.iter() {
        if key.ends_with("-client-id") && Some(value) != verified_client_id {
            return Err(AiHeaderValidationError::client_id_spoof(format!(
                "extras.ai.transport.{key} does not match verified identity"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::app::{
        AppChannelsPolicy, AppHistoryConfig, AppPolicy, NamespaceHistoryConfig,
    };
    use sockudo_protocol::messages::{AiExtras, MessageExtras};
    use std::collections::HashMap;

    #[test]
    fn namespace_permission_denies_subscribe_when_flag_disabled() {
        let namespace = ChannelNamespace {
            name: "chat".to_string(),
            channel_name_pattern: None,
            max_channel_name_length: None,
            annotations_enabled: None,
            allow_user_limited_channels: None,
            allow_subscribe_for_client: Some(false),
            allow_publish_for_client: None,
            allow_presence_for_client: None,
            history: None,
            presence_history: None,
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
            annotations_enabled: None,
            allow_user_limited_channels: None,
            allow_subscribe_for_client: None,
            allow_publish_for_client: Some(false),
            allow_presence_for_client: None,
            history: None,
            presence_history: None,
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        };

        assert!(
            validate_capability_permission(&capabilities, "presence-chat:room-1", "subscribe")
                .is_ok()
        );
    }

    #[test]
    fn rewind_validation_rejects_disabled_policy() {
        let app = App::from_policy(
            "app".to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            AppPolicy {
                history: Some(AppHistoryConfig {
                    enabled: Some(true),
                    rewind_enabled: Some(false),
                    retention_window_seconds: None,
                    max_messages_per_channel: None,
                    max_bytes_per_channel: None,
                }),
                ..Default::default()
            },
        );
        let global = HistoryConfig {
            enabled: true,
            ..HistoryConfig::default()
        };

        let err = validate_rewind_request(&app, "chat:room-1", true, &global).unwrap_err();
        assert!(
            err.to_string()
                .contains("Channel rewind is disabled by policy for channel 'chat:room-1'")
        );
    }

    #[test]
    fn rewind_validation_namespace_override_beats_app_default() {
        let app = App::from_policy(
            "app".to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            AppPolicy {
                history: Some(AppHistoryConfig {
                    enabled: Some(true),
                    rewind_enabled: Some(false),
                    retention_window_seconds: None,
                    max_messages_per_channel: None,
                    max_bytes_per_channel: None,
                }),
                channels: AppChannelsPolicy {
                    channel_namespaces: Some(vec![ChannelNamespace {
                        name: "chat".to_string(),
                        channel_name_pattern: None,
                        max_channel_name_length: None,
                        annotations_enabled: None,
                        allow_user_limited_channels: None,
                        allow_subscribe_for_client: None,
                        allow_publish_for_client: None,
                        allow_presence_for_client: None,
                        history: Some(NamespaceHistoryConfig {
                            rewind_enabled: Some(true),
                            retention_window_seconds: None,
                            max_messages_per_channel: None,
                            max_bytes_per_channel: None,
                        }),
                        presence_history: None,
                    }]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let global = HistoryConfig {
            enabled: true,
            ..HistoryConfig::default()
        };

        assert!(validate_rewind_request(&app, "chat:room-1", true, &global).is_ok());
    }

    #[test]
    fn presence_capability_does_not_satisfy_subscribe_gate() {
        let capabilities = ConnectionCapabilities {
            presence: Some(vec!["presence-room".to_string()]),
            ..Default::default()
        };

        assert!(
            validate_capability_permission(&capabilities, "presence-room", "subscribe").is_ok(),
            "legacy signin capabilities without an explicit subscribe list remain implicit"
        );

        let token_capabilities = ConnectionCapabilities {
            subscribe: Some(Vec::new()),
            presence: Some(vec!["presence-room".to_string()]),
            ..Default::default()
        };

        assert!(
            validate_capability_permission(&token_capabilities, "presence-room", "subscribe")
                .is_err()
        );
        assert!(
            validate_capability_permission(&token_capabilities, "presence-room", "presence")
                .is_ok()
        );
    }

    #[test]
    fn subscribe_capability_does_not_satisfy_presence_gate() {
        let capabilities = ConnectionCapabilities {
            subscribe: Some(vec!["presence-room".to_string()]),
            presence: Some(Vec::new()),
            ..Default::default()
        };

        assert!(
            validate_capability_permission(&capabilities, "presence-room", "subscribe").is_ok()
        );
        assert!(
            validate_capability_permission(&capabilities, "presence-room", "presence").is_err()
        );
    }

    #[test]
    fn ai_client_id_headers_reject_spoofed_client_identity() {
        let message = PusherMessage {
            event: Some("ai-input".to_string()),
            channel: Some("private-ai".to_string()),
            data: None,
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: Some(MessageExtras {
                ai: Some(AiExtras {
                    transport: Some(HashMap::from([(
                        "input-client-id".to_string(),
                        "attacker".to_string(),
                    )])),
                    codec: None,
                }),
                ..Default::default()
            }),
            delta_sequence: None,
            delta_conflation_key: None,
        };

        let err = validate_ai_client_id_headers(&message, Some("user-1"), AiPublishTrust::Client)
            .unwrap_err();
        assert_eq!(
            err.code,
            sockudo_protocol::messages::AI_ERROR_CLIENT_ID_SPOOF
        );
    }

    #[test]
    fn ai_client_id_headers_allow_trusted_app_publish() {
        let message = PusherMessage {
            event: Some("ai-output".to_string()),
            channel: Some("private-ai".to_string()),
            data: None,
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: Some(MessageExtras {
                ai: Some(AiExtras {
                    transport: Some(HashMap::from([(
                        "turn-client-id".to_string(),
                        "server-agent".to_string(),
                    )])),
                    codec: None,
                }),
                ..Default::default()
            }),
            delta_sequence: None,
            delta_conflation_key: None,
        };

        validate_ai_client_id_headers(&message, None, AiPublishTrust::TrustedApp).unwrap();
    }
}
