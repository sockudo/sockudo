use super::ConnectionHandler;
use bytes::Bytes;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::SocketId;
use serde::Deserialize;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sonic_rs::json;
use std::collections::HashMap;
use tracing::{debug, warn};

impl ConnectionHandler {
    /// Handle a `pusher:resume` event from a reconnecting client.
    ///
    /// The client sends `{ "event": "pusher:resume", "data": "{\"channel_serials\":{\"ch\":42}}" }`.
    /// For each channel, the server replays all messages with serial > the client's last serial.
    /// If the buffer no longer has messages old enough, a per-channel `pusher:resume_failed` is sent.
    pub async fn handle_resume(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let replay_buffer = match &self.replay_buffer {
            Some(rb) => rb,
            None => {
                self.send_message_to_socket(
                    &app_config.id,
                    socket_id,
                    PusherMessage::error(
                        4301,
                        "Connection recovery is not enabled".to_string(),
                        None,
                    ),
                )
                .await?;
                return Ok(());
            }
        };

        let channel_serials = parse_channel_serials(message)?;

        if channel_serials.is_empty() {
            self.send_message_to_socket(
                &app_config.id,
                socket_id,
                PusherMessage::error(
                    4302,
                    "No channel_serials provided".to_string(),
                    None,
                ),
            )
            .await?;
            return Ok(());
        }

        let mut recovered_channels = Vec::new();
        let mut failed_channels = Vec::new();

        for (channel, last_serial) in &channel_serials {
            match replay_buffer.get_messages_after(&app_config.id, channel, *last_serial) {
                Some(messages) => {
                    debug!(
                        "Replaying {} messages for channel {} (after serial {}) to socket {}",
                        messages.len(),
                        channel,
                        last_serial,
                        socket_id
                    );
                    for msg_bytes in messages {
                        if let Err(e) = self
                            .send_raw_bytes_to_socket(socket_id, &app_config.id, msg_bytes)
                            .await
                        {
                            warn!(
                                "Failed to replay message to socket {} for channel {}: {}",
                                socket_id, channel, e
                            );
                            break;
                        }
                    }
                    recovered_channels.push(channel.clone());
                }
                None => {
                    debug!(
                        "Resume failed for channel {} (serial {}): buffer expired",
                        channel, last_serial
                    );
                    failed_channels.push(channel.clone());
                }
            }
        }

        // Send per-channel resume_failed events
        for channel in &failed_channels {
            let fail_msg = PusherMessage {
                event: Some(sockudo_protocol::constants::EVENT_RESUME_FAILED.to_string()),
                channel: Some(channel.clone()),
                data: Some(MessageData::String(
                    json!({ "reason": "Messages expired from replay buffer" }).to_string(),
                )),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                serial: None,
            };
            self.send_message_to_socket(&app_config.id, socket_id, fail_msg)
                .await
                .ok();
        }

        // Send resume_success summary
        let success_msg = PusherMessage {
            event: Some(sockudo_protocol::constants::EVENT_RESUME_SUCCESS.to_string()),
            channel: None,
            data: Some(MessageData::String(
                json!({
                    "recovered": recovered_channels,
                    "failed": failed_channels,
                })
                .to_string(),
            )),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            serial: None,
        };
        self.send_message_to_socket(&app_config.id, socket_id, success_msg)
            .await?;

        Ok(())
    }

    /// Send pre-serialized message bytes directly to a socket.
    async fn send_raw_bytes_to_socket(
        &self,
        socket_id: &SocketId,
        app_id: &str,
        bytes: Vec<u8>,
    ) -> Result<()> {
        if let Some(conn) = self
            .connection_manager
            .get_connection(socket_id, app_id)
            .await
        {
            conn.send_broadcast(Bytes::from(bytes))
        } else {
            Err(Error::ConnectionClosed(format!(
                "Socket {} not found during replay",
                socket_id
            )))
        }
    }
}

/// Parse `channel_serials` from a resume message's data field.
/// Expects: `{ "channel_serials": { "channel-name": 42, ... } }`
fn parse_channel_serials(message: &PusherMessage) -> Result<HashMap<String, u64>> {
    let data_str = match &message.data {
        Some(MessageData::String(s)) => s.clone(),
        Some(MessageData::Json(v)) => v.to_string(),
        _ => {
            return Err(Error::InvalidMessageFormat(
                "Missing data in resume message".to_string(),
            ));
        }
    };

    #[derive(Deserialize)]
    struct ResumeData {
        channel_serials: Option<HashMap<String, u64>>,
    }

    let resume_data: ResumeData = sonic_rs::from_str(&data_str)
        .map_err(|e| Error::InvalidMessageFormat(format!("Invalid resume data JSON: {e}")))?;

    resume_data.channel_serials.ok_or_else(|| {
        Error::InvalidMessageFormat(
            "Missing channel_serials in resume data".to_string(),
        )
    })
}
