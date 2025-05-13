use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub enum ChannelType {
    Public,
    Private,
    Presence,
    PrivateEncrypted,
}

impl ChannelType {
    pub fn from_name(channel_name: &str) -> Self {
        match channel_name.split_once('-') {
            Some(("private", "encrypted")) => Self::PrivateEncrypted,
            Some(("private", _)) => Self::Private,
            Some(("presence", _)) => Self::Presence,
            _ => Self::Public,
        }
    }

    pub fn requires_authentication(&self) -> bool {
        matches!(
            self,
            ChannelType::Private | ChannelType::Presence | ChannelType::PrivateEncrypted
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceMemberInfo {
    pub user_id: String,
    pub user_info: Option<serde_json::Value>,
}
