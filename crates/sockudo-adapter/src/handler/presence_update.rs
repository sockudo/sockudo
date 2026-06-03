use super::ConnectionHandler;
use crate::memory_rate_limiter::MemoryRateLimiter;
use sockudo_core::app::App;
use sockudo_core::channel::ChannelType;
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::ProtocolVersion;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sonic_rs::{JsonValueTrait, Value};
use std::sync::Arc;

const MAX_PRESENCE_UPDATE_BYTES: usize = 1024;

impl ConnectionHandler {
    pub(crate) async fn handle_presence_update(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let connection = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
            .ok_or_else(|| Error::InvalidMessageFormat("unknown connection".to_string()))?;

        if connection.protocol_version != ProtocolVersion::V2 {
            return Err(ai_presence_error("presence_update requires protocol V2"));
        }

        let (channel, data) = parse_presence_update(message)?;
        if ChannelType::from_name(&channel) != ChannelType::Presence {
            return Err(ai_presence_error(
                "presence_update requires a presence channel",
            ));
        }

        let payload_bytes = sonic_rs::to_vec(&data)
            .map_err(|e| Error::Serialization(format!("failed to encode presence data: {e}")))?;
        if payload_bytes.len() > MAX_PRESENCE_UPDATE_BYTES {
            return Err(Error::AiTransport {
                code: 40009,
                name: "payload_too_large",
                message: "presence_update data exceeds 1024 bytes".to_string(),
            });
        }

        self.validate_v2_capability(socket_id, app_config, &channel, "presence")
            .await?;

        let member = self
            .connection_manager
            .get_presence_member(&app_config.id, &channel, socket_id)
            .await
            .ok_or_else(|| ai_presence_error("presence_update requires active membership"))?;

        self.check_presence_update_rate_limit(socket_id, app_config, &channel, &member.user_id)
            .await?;

        let presence_history_policy =
            app_config.resolved_presence_history(&channel, &self.server_options().presence_history);

        self.presence_manager
            .handle_member_updated(
                &self.connection_manager,
                Arc::clone(self.presence_history_store()),
                presence_history_policy.enabled,
                self.webhook_integration.as_ref(),
                self.metrics.as_ref(),
                app_config,
                &channel,
                socket_id,
                &member.user_id,
                data,
                Some(presence_history_policy.retention()),
            )
            .await
    }

    async fn check_presence_update_rate_limit(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
        user_id: &str,
    ) -> Result<()> {
        let limit = self
            .server_options()
            .presence
            .update_rate_limit_per_member_per_second;
        let key = presence_update_rate_key(&app_config.id, channel, user_id);
        let limiter = self
            .presence_update_limiters
            .entry(*socket_id)
            .or_insert_with(|| Arc::new(MemoryRateLimiter::new(limit, 1)));

        let result = limiter.value().increment(&key).await?;
        if result.allowed {
            return Ok(());
        }

        if let Some(ref metrics) = self.metrics {
            metrics.mark_rate_limit_triggered(&app_config.id, "presence_update");
        }

        Err(Error::ClientEventRateLimit)
    }
}

fn parse_presence_update(message: &PusherMessage) -> Result<(String, Value)> {
    let data = match &message.data {
        Some(MessageData::Json(value)) => value.clone(),
        Some(MessageData::String(raw)) => sonic_rs::from_str(raw)
            .map_err(|_| ai_presence_error("presence_update data must be a JSON object"))?,
        Some(MessageData::Structured { extra, .. }) => sonic_rs::to_value(extra)
            .map_err(|e| Error::Serialization(format!("failed to decode structured data: {e}")))?,
        None => return Err(ai_presence_error("presence_update data is required")),
    };

    let channel = data
        .get("channel")
        .and_then(Value::as_str)
        .filter(|channel| !channel.is_empty())
        .ok_or_else(|| ai_presence_error("presence_update channel is required"))?
        .to_string();

    let member_data = data
        .get("data")
        .cloned()
        .ok_or_else(|| ai_presence_error("presence_update data field is required"))?;

    Ok((channel, member_data))
}

fn ai_presence_error(message: &str) -> Error {
    Error::AiTransport {
        code: 104009,
        name: "ai_presence_update_invalid",
        message: message.to_string(),
    }
}

fn presence_update_rate_key(app_id: &str, channel: &str, user_id: &str) -> String {
    format!("{app_id}:{channel}:{user_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_presence_update_accepts_json_data() {
        let message = PusherMessage {
            event: Some("sockudo:presence_update".to_string()),
            channel: None,
            data: Some(MessageData::Json(sonic_rs::json!({
                "channel": "presence-agent",
                "data": {"status": "thinking"}
            }))),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        let (channel, data) = parse_presence_update(&message).unwrap();
        assert_eq!(channel, "presence-agent");
        assert_eq!(data.get("status").and_then(Value::as_str), Some("thinking"));
    }

    #[test]
    fn parse_presence_update_rejects_missing_data_field() {
        let message = PusherMessage {
            event: Some("sockudo:presence_update".to_string()),
            channel: None,
            data: Some(MessageData::Json(sonic_rs::json!({
                "channel": "presence-agent"
            }))),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        assert!(parse_presence_update(&message).is_err());
    }

    #[test]
    fn presence_update_rate_key_preserves_member_scope() {
        assert_eq!(
            presence_update_rate_key("app-1", "presence-room", "user-1"),
            "app-1:presence-room:user-1"
        );
    }
}
