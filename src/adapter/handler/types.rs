// src/adapter/handler/types.rs
use crate::websocket::SocketId;
use crate::protocol::messages::{PusherMessage, MessageData};
use serde_json::Value;


#[derive(Debug)]
pub struct SubscriptionRequest {
    pub channel: String,
    pub auth: Option<String>,
    pub channel_data: Option<String>,
}

#[derive(Debug)]
pub struct ClientEventRequest {
    pub event: String,
    pub channel: String,
    pub data: Value,
}

#[derive(Debug)]
pub struct SigninRequest {
    pub user_data: String,
    pub auth: String,
}

impl SubscriptionRequest {
    pub fn from_message(message: &PusherMessage) -> crate::error::Result<Self> {
        let (channel, auth, channel_data) = match &message.data {
            Some(MessageData::Structured { channel, extra, .. }) => {
                let ch = channel.as_ref()
                    .ok_or_else(|| crate::error::Error::InvalidMessageFormat(
                        "Missing channel field".into()
                    ))?;
                let auth = extra.get("auth").and_then(Value::as_str).map(String::from);
                let channel_data = extra.get("channel_data").and_then(Value::as_str).map(String::from);
                (ch.clone(), auth, channel_data)
            }
            Some(MessageData::Json(data)) => {
                let ch = data.get("channel")
                    .and_then(Value::as_str)
                    .ok_or_else(|| crate::error::Error::InvalidMessageFormat(
                        "Missing channel field".into()
                    ))?;
                let auth = data.get("auth").and_then(Value::as_str).map(String::from);
                let channel_data = data.get("channel_data").and_then(Value::as_str).map(String::from);
                (ch.to_string(), auth, channel_data)
            }
            _ => return Err(crate::error::Error::InvalidMessageFormat(
                "Invalid subscription data format".into()
            )),
        };

        Ok(Self { channel, auth, channel_data })
    }
}

impl SigninRequest {
    pub fn from_message(message: &PusherMessage) -> crate::error::Result<Self> {
        let extract_field = |data: &Value, field: &str| -> crate::error::Result<String> {
            data.get(field)
                .and_then(Value::as_str)
                .map(String::from)
                .ok_or_else(|| crate::error::Error::AuthError(
                    format!("Missing '{}' field in signin data", field)
                ))
        };

        match &message.data {
            Some(MessageData::Json(data)) => {
                Ok(Self {
                    user_data: extract_field(data, "user_data")?,
                    auth: extract_field(data, "auth")?,
                })
            }
            Some(MessageData::Structured { extra, .. }) => {
                Ok(Self {
                    user_data: extra.get("user_data")
                        .and_then(Value::as_str)
                        .map(String::from)
                        .ok_or_else(|| crate::error::Error::AuthError(
                            "Missing 'user_data' field in signin data".into()
                        ))?,
                    auth: extra.get("auth")
                        .and_then(Value::as_str)
                        .map(String::from)
                        .ok_or_else(|| crate::error::Error::AuthError(
                            "Missing 'auth' field in signin data".into()
                        ))?,
                })
            }
            _ => Err(crate::error::Error::InvalidMessageFormat(
                "Invalid signin data format".into()
            )),
        }
    }
}
