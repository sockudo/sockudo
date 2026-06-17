use crate::{Result, SockudoError};
use std::fmt;
use std::str::FromStr;

/// Type-safe channel representation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Channel {
    Public(PublicChannel),
    Private(PrivateChannel),
    Presence(PresenceChannel),
    Encrypted(EncryptedChannel),
}

/// Public channel type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublicChannel(ChannelName);

/// Private channel type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrivateChannel(ChannelName);

/// Presence channel type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PresenceChannel(ChannelName);

/// Encrypted channel type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncryptedChannel(ChannelName);

/// Validated channel name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelName(String);

impl ChannelName {
    /// Creates a new channel name with validation
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        validate_channel_name(&name)?;
        Ok(Self(name.to_owned()))
    }

    /// Gets the channel name as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the inner String
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for ChannelName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChannelName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Channel type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelType {
    Public,
    Private,
    Presence,
    Encrypted,
}

impl Channel {
    /// Creates a channel from a string, automatically detecting the type
    pub fn from_string(s: impl Into<String>) -> Result<Self> {
        let s = s.into();

        if s.starts_with("private-encrypted-") {
            let name = s.strip_prefix("private-encrypted-").unwrap();
            Ok(Channel::Encrypted(EncryptedChannel(ChannelName::new(
                name,
            )?)))
        } else if s.starts_with("presence-") {
            let name = s.strip_prefix("presence-").unwrap();
            Ok(Channel::Presence(PresenceChannel(ChannelName::new(name)?)))
        } else if s.starts_with("private-") {
            let name = s.strip_prefix("private-").unwrap();
            Ok(Channel::Private(PrivateChannel(ChannelName::new(name)?)))
        } else {
            Ok(Channel::Public(PublicChannel(ChannelName::new(s)?)))
        }
    }

    /// Gets the full channel name including prefix
    pub fn full_name(&self) -> String {
        match self {
            Channel::Public(ch) => ch.0.to_string(),
            Channel::Private(ch) => format!("private-{}", ch.0),
            Channel::Presence(ch) => format!("presence-{}", ch.0),
            Channel::Encrypted(ch) => format!("private-encrypted-{}", ch.0),
        }
    }

    /// Gets the channel type
    pub fn channel_type(&self) -> ChannelType {
        match self {
            Channel::Public(_) => ChannelType::Public,
            Channel::Private(_) => ChannelType::Private,
            Channel::Presence(_) => ChannelType::Presence,
            Channel::Encrypted(_) => ChannelType::Encrypted,
        }
    }

    /// Checks if the channel requires authentication
    pub fn requires_auth(&self) -> bool {
        !matches!(self, Channel::Public(_))
    }

    /// Checks if the channel is encrypted
    pub fn is_encrypted(&self) -> bool {
        matches!(self, Channel::Encrypted(_))
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_name())
    }
}

impl FromStr for Channel {
    type Err = SockudoError;

    fn from_str(s: &str) -> Result<Self> {
        Channel::from_string(s)
    }
}

// Implement convenience constructors for specific channel types
impl PublicChannel {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self(ChannelName::new(name)?))
    }
}

impl PrivateChannel {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self(ChannelName::new(name)?))
    }
}

impl PresenceChannel {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self(ChannelName::new(name)?))
    }
}

impl EncryptedChannel {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self(ChannelName::new(name)?))
    }
}

// Validation moved here from util.rs
use regex::Regex;
use std::sync::LazyLock;

static CHANNEL_NAME_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_\-=@,.;]+$").unwrap());

fn validate_channel_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(SockudoError::Validation {
            message: "Channel name cannot be empty".to_string(),
        });
    }

    if name.len() > 200 {
        return Err(SockudoError::Validation {
            message: format!("Channel name too long: '{}' (max 200 characters)", name),
        });
    }

    if !CHANNEL_NAME_PATTERN.is_match(name) {
        return Err(SockudoError::Validation {
            message: format!(
                "Invalid channel name: '{}'. Must match pattern: [A-Za-z0-9_\\-=@,.;]+",
                name
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_creation() {
        assert!(Channel::from_string("test-channel").is_ok());
        assert!(Channel::from_string("private-test").is_ok());
        assert!(Channel::from_string("presence-test").is_ok());
        assert!(Channel::from_string("private-encrypted-test").is_ok());
    }

    #[test]
    fn test_channel_type_detection() {
        let public = Channel::from_string("test").unwrap();
        assert_eq!(public.channel_type(), ChannelType::Public);

        let private = Channel::from_string("private-test").unwrap();
        assert_eq!(private.channel_type(), ChannelType::Private);
    }

    #[test]
    fn test_channel_name_validation() {
        assert!(ChannelName::new("").is_err());
        assert!(ChannelName::new("a".repeat(201)).is_err());
        assert!(ChannelName::new("test channel").is_err()); // space not allowed
        assert!(ChannelName::new("test-channel_123").is_ok());
    }
}
