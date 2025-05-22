use super::PresenceMemberInfo;
use super::types::ChannelType;
use crate::adapter::Adapter;
use crate::app::config::App;
use crate::error::Error;
use crate::protocol::messages::{MessageData, PusherMessage};
use crate::token::{Token, secure_compare};
use crate::websocket::SocketId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceMember {
    pub(crate) user_id: String,
    pub(crate) user_info: Value,
    pub(crate) socket_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub(crate) success: bool,
    pub channel_connections: Option<i32>,
    pub auth_error: Option<String>,
    pub member: Option<PresenceMember>,
    pub error_message: Option<String>,
    pub error_code: Option<i32>,
    pub _type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveResponse {
    pub(crate) left: bool,
    pub remaining_connections: Option<usize>,
    pub member: Option<PresenceMember>,
}

pub struct ChannelManager {
    connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
}

impl ChannelManager {
    pub fn new(connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>) -> Self {
        Self { connection_manager }
    }

    pub async fn subscribe(
        &self,
        socket_id: &str,
        data: &PusherMessage,
        channel_name: &str,
        is_authenticated: bool,
        app_id: &str,
    ) -> Result<JoinResponse, Error> {
        let channel_type = ChannelType::from_name(channel_name);

        if channel_type.requires_authentication() && !is_authenticated {
            return Err(Error::AuthError("Channel requires authentication".into()));
        }

        // Create SocketId without clone by using borrowed str
        let socket_id = SocketId(socket_id.to_string());

        let mut connection_manager = self.connection_manager.lock().await;

        if connection_manager
            .is_in_channel(app_id, channel_name, &socket_id)
            .await?
        {
            let channel = connection_manager.get_channel(app_id, channel_name).await?;

            return Ok(JoinResponse {
                success: true,
                channel_connections: Some(channel.len() as i32),
                member: None,
                auth_error: None,
                error_message: None,
                error_code: None,
                _type: None,
            });
        }

        // Handle presence channel subscription
        let member = if channel_type == ChannelType::Presence {
            Some(self.parse_presence_data(&data.data)?)
        } else {
            None
        };

        // Add socket to channel
        connection_manager
            .add_to_channel(app_id, channel_name, &socket_id)
            .await
            .expect("TODO: panic message");

        let total_connections = connection_manager
            .get_channel(app_id, channel_name)
            .await?
            .len();

        Ok(JoinResponse {
            success: true,
            channel_connections: Some(total_connections as i32),
            member,
            auth_error: None,
            error_message: None,
            error_code: None,
            _type: None,
        })
    }

    pub async fn unsubscribe(
        &self,
        socket_id: &str,
        channel_name: &str,
        app_id: &str,
        user_id: Option<&str>,
    ) -> Result<LeaveResponse, Error> {
        let socket_id = SocketId(socket_id.to_string());
        let mut connection_manager = self.connection_manager.lock().await;

        let member = if ChannelType::from_name(channel_name) == ChannelType::Presence {
            if let Some(user_id) = user_id {
                let members = connection_manager
                    .get_channel_members(app_id, channel_name)
                    .await?;

                members.get(user_id).map(|member| PresenceMember {
                    user_id: member.user_id.clone(),
                    user_info: member.user_info.clone().unwrap_or_default(),
                    socket_id: None,
                })
            } else {
                None
            }
        } else {
            None
        };

        let socket_removed = connection_manager
            .remove_from_channel(app_id, channel_name, &socket_id)
            .await;

        let remaining_connections = connection_manager
            .get_channel(app_id, channel_name)
            .await?
            .len();

        if remaining_connections == 0 {
            connection_manager
                .remove_channel(app_id, channel_name)
                .await;
        }

        Ok(LeaveResponse {
            left: socket_removed?,
            remaining_connections: Some(remaining_connections),
            member,
        })
    }

    fn parse_presence_data(&self, data: &Option<MessageData>) -> Result<PresenceMember, Error> {
        let channel_data = data
            .as_ref()
            .ok_or_else(|| Error::ChannelError("Missing presence data".into()))?;

        match channel_data {
            MessageData::Structured {
                channel_data,
                extra,
                ..
            } => {
                let data: Value = serde_json::from_str(
                    channel_data
                        .as_ref()
                        .ok_or_else(|| Error::ChannelError("Missing channel_data".into()))?,
                )?;

                self.extract_presence_member(&data, extra)
            }
            MessageData::Json(data) => self.extract_presence_member(data, &Default::default()),
            _ => Err(Error::ChannelError("Invalid presence data format".into())),
        }
    }

    fn extract_presence_member(
        &self,
        data: &Value,
        extra: &HashMap<String, Value>,
    ) -> Result<PresenceMember, Error> {
        let user_id = data
            .get("user_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ChannelError("Invalid user_id".into()))?
            .to_string();

        let user_info = data.get("user_info").cloned().unwrap_or_default();

        let socket_id = extra
            .get("socket_id")
            .and_then(|v| v.as_str())
            .map(ToString::to_string);

        Ok(PresenceMember {
            user_id,
            user_info,
            socket_id,
        })
    }

    pub fn signature_is_valid(
        &self,
        app_config: App,
        socket_id: &SocketId,
        signature: &str,
        message: PusherMessage,
    ) -> bool {
        let expected = Self::get_expected_signature(app_config, socket_id, message);
        secure_compare(signature, &expected)
    }

    pub fn get_expected_signature(
        app_config: App,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> String {
        let token = Token::new(app_config.key.clone(), app_config.secret);
        format!(
            "{}:{}",
            app_config.key,
            token.sign(&Self::get_data_to_sign_for_signature(socket_id, message))
        )
    }

    fn get_data_to_sign_for_signature(socket_id: &SocketId, message: PusherMessage) -> String {
        let message_data = message.data.unwrap();

        match message_data {
            MessageData::Structured {
                channel_data,
                channel,
                ..
            } => {
                let channel = channel.unwrap();
                if ChannelType::from_name(&channel) == ChannelType::Presence {
                    format!("{}:{}:{}", socket_id, channel, channel_data.unwrap())
                } else {
                    format!("{}:{}", socket_id, channel)
                }
            }
            _ => panic!("Invalid message data"),
        }
    }

    pub async fn get_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>, Error> {
        let mut connection_manager = self.connection_manager.lock().await;
        connection_manager
            .get_channel_members(app_id, channel)
            .await
    }
}
