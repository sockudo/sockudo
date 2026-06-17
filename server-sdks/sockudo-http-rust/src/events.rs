use crate::{Channel, Result, Sockudo, SockudoError};
#[cfg(feature = "encryption")]
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use sonic_rs::{Value, json};
use std::collections::HashMap;
use std::fmt;

/// V2 extras for publish events.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageExtras {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub echo: Option<bool>,
}

#[cfg(all(feature = "encryption", feature = "sodiumoxide"))]
use std::sync::Once;

#[cfg(all(feature = "encryption", feature = "sodiumoxide"))]
static SODIUM_INIT: Once = Once::new();

/// Initialize sodiumoxide once
#[cfg(all(feature = "encryption", feature = "sodiumoxide"))]
fn init_sodium() -> Result<()> {
    SODIUM_INIT.call_once(|| {
        sodiumoxide::init().expect("Failed to initialize sodiumoxide");
    });
    Ok(())
}

/// Event data that can be either a string or JSON
#[derive(Debug, Clone, PartialEq)]
pub enum EventData {
    String(String),
    Json(Value),
}

impl EventData {
    /// Creates event data from a string
    pub fn from_string(s: impl Into<String>) -> Self {
        EventData::String(s.into())
    }

    /// Creates event data from a JSON value
    pub fn from_json(value: Value) -> Self {
        EventData::Json(value)
    }

    /// Gets the event data as a JSON value
    pub fn as_json(&self) -> Result<Value> {
        match self {
            EventData::String(s) => sonic_rs::from_str(s).map_err(SockudoError::Json),
            EventData::Json(v) => Ok(v.clone()),
        }
    }
}

impl fmt::Display for EventData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventData::String(s) => write!(f, "{}", s),
            EventData::Json(v) => write!(f, "{}", sonic_rs::to_string(v).unwrap_or_default()),
        }
    }
}

impl From<String> for EventData {
    fn from(s: String) -> Self {
        EventData::String(s)
    }
}

impl From<&str> for EventData {
    fn from(s: &str) -> Self {
        EventData::String(s.to_string())
    }
}

impl From<Value> for EventData {
    fn from(v: Value) -> Self {
        EventData::Json(v)
    }
}

/// Generates a random idempotency key as a UUID v4 string.
pub fn generate_idempotency_key() -> String {
    let mut bytes = [0u8; 16];
    rand::fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

/// Event data for triggering
#[derive(Debug, Serialize)]
pub struct Event {
    pub name: String,
    pub data: String,
    pub channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<MessageExtras>,
}

/// Batch event data
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchEvent {
    pub name: String,
    pub channel: String,
    pub data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<MessageExtras>,
}

impl BatchEvent {
    /// Creates a new batch event with EventData
    pub fn new(
        name: impl Into<String>,
        channel: impl Into<String>,
        data: impl Into<EventData>,
    ) -> Self {
        Self {
            name: name.into(),
            channel: channel.into(),
            data: data.into().to_string(),
            socket_id: None,
            info: None,
            tags: None,
            idempotency_key: None,
            extras: None,
        }
    }

    /// Sets the socket ID to exclude
    pub fn with_socket_id(mut self, socket_id: impl Into<String>) -> Self {
        self.socket_id = Some(socket_id.into());
        self
    }

    /// Sets the info parameter
    pub fn with_info(mut self, info: impl Into<String>) -> Self {
        self.info = Some(info.into());
        self
    }

    /// Sets the tags for tag filtering
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Sets the idempotency key for deduplication
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Sets a randomly generated idempotency key (UUID v4)
    pub fn with_auto_idempotency_key(mut self) -> Self {
        self.idempotency_key = Some(generate_idempotency_key());
        self
    }

    /// Sets the V2 extras
    pub fn with_extras(mut self, extras: MessageExtras) -> Self {
        self.extras = Some(extras);
        self
    }

    /// Sets the ephemeral flag in extras
    pub fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.extras
            .get_or_insert_with(MessageExtras::default)
            .ephemeral = Some(ephemeral);
        self
    }

    /// Sets the echo flag in extras
    pub fn with_echo(mut self, echo: bool) -> Self {
        self.extras.get_or_insert_with(MessageExtras::default).echo = Some(echo);
        self
    }
}

/// Parameters for triggering events
#[derive(Debug, Clone, Default)]
pub struct TriggerParams {
    pub socket_id: Option<String>,
    pub info: Option<String>,
    pub tags: Option<HashMap<String, String>>,
    pub idempotency_key: Option<String>,
    pub extras: Option<MessageExtras>,
}

impl TriggerParams {
    /// Creates a new TriggerParams builder
    pub fn builder() -> TriggerParamsBuilder {
        TriggerParamsBuilder::default()
    }
}

/// Builder for TriggerParams
#[derive(Debug, Default)]
pub struct TriggerParamsBuilder {
    socket_id: Option<String>,
    info: Option<String>,
    tags: Option<HashMap<String, String>>,
    idempotency_key: Option<String>,
    extras: Option<MessageExtras>,
}

impl TriggerParamsBuilder {
    /// Sets the socket ID to exclude
    pub fn socket_id(mut self, socket_id: impl Into<String>) -> Self {
        self.socket_id = Some(socket_id.into());
        self
    }

    /// Sets the info parameter
    pub fn info(mut self, info: impl Into<String>) -> Self {
        self.info = Some(info.into());
        self
    }

    /// Sets the tags for tag filtering
    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Sets the idempotency key for deduplication
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Sets a randomly generated idempotency key (UUID v4)
    pub fn auto_idempotency_key(mut self) -> Self {
        self.idempotency_key = Some(generate_idempotency_key());
        self
    }

    /// Sets the V2 extras
    pub fn extras(mut self, extras: MessageExtras) -> Self {
        self.extras = Some(extras);
        self
    }

    /// Sets the ephemeral flag in extras
    pub fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.extras
            .get_or_insert_with(MessageExtras::default)
            .ephemeral = Some(ephemeral);
        self
    }

    /// Sets the echo flag in extras
    pub fn with_echo(mut self, echo: bool) -> Self {
        self.extras.get_or_insert_with(MessageExtras::default).echo = Some(echo);
        self
    }

    /// Builds the TriggerParams
    pub fn build(self) -> TriggerParams {
        TriggerParams {
            socket_id: self.socket_id,
            info: self.info,
            tags: self.tags,
            idempotency_key: self.idempotency_key,
            extras: self.extras,
        }
    }
}

/// Encrypts data for encrypted channels
#[cfg(feature = "encryption")]
fn encrypt(sockudo: &Sockudo, channel: &str, data: &EventData) -> Result<String> {
    #[cfg(feature = "sodiumoxide")]
    {
        encrypt_sodiumoxide(sockudo, channel, data)
    }

    #[cfg(not(feature = "sodiumoxide"))]
    {
        encrypt_pure_rust(sockudo, channel, data)
    }
}

/// Encrypts data using sodiumoxide
#[cfg(all(feature = "encryption", feature = "sodiumoxide"))]
fn encrypt_sodiumoxide(sockudo: &Sockudo, channel: &str, data: &EventData) -> Result<String> {
    init_sodium()?;

    // Ensure master key is present
    let _master_key =
        sockudo
            .config()
            .encryption_master_key()
            .ok_or_else(|| SockudoError::Encryption {
                message: "Set encryptionMasterKey before triggering events on encrypted channels"
                    .to_string(),
            })?;

    // Generate a random nonce
    let nonce_bytes =
        sodiumoxide::randombytes::randombytes(sodiumoxide::crypto::secretbox::NONCEBYTES);
    let nonce =
        sodiumoxide::crypto::secretbox::Nonce::from_slice(&nonce_bytes).ok_or_else(|| {
            SockudoError::Encryption {
                message: "Failed to create nonce from random bytes".to_string(),
            }
        })?;

    // Get channel shared secret
    let shared_secret_bytes = sockudo.channel_shared_secret(channel)?;

    // Convert to cryptographic Key type
    let key =
        sodiumoxide::crypto::secretbox::Key::from_slice(&shared_secret_bytes).ok_or_else(|| {
            SockudoError::Encryption {
                message: format!(
                    "Channel shared secret must be {} bytes long, but was {} bytes.",
                    sodiumoxide::crypto::secretbox::KEYBYTES,
                    shared_secret_bytes.len()
                ),
            }
        })?;

    // Get data as bytes
    let data_string = data.to_string();
    let data_bytes = data_string.as_bytes();

    // Encrypt the data
    let ciphertext = sodiumoxide::crypto::secretbox::seal(data_bytes, &nonce, &key);

    // Return encrypted payload as JSON string
    let encrypted_payload = json!({
        "nonce": BASE64.encode(nonce.as_ref()),
        "ciphertext": BASE64.encode(&ciphertext),
    });

    Ok(sonic_rs::to_string(&encrypted_payload)?)
}

/// Encrypts data using pure Rust crypto libraries
#[cfg(all(feature = "encryption", not(feature = "sodiumoxide")))]
fn encrypt_pure_rust(sockudo: &Sockudo, channel: &str, data: &EventData) -> Result<String> {
    use chacha20poly1305::{
        ChaCha20Poly1305, Nonce,
        aead::{Aead, AeadCore, KeyInit, OsRng},
    };

    // Ensure master key is present
    let _master_key =
        sockudo
            .config()
            .encryption_master_key()
            .ok_or_else(|| SockudoError::Encryption {
                message: "Set encryptionMasterKey before triggering events on encrypted channels"
                    .to_string(),
            })?;

    // Get channel shared secret
    let shared_secret_bytes = sockudo.channel_shared_secret(channel)?;

    // Create cipher
    let cipher = ChaCha20Poly1305::new_from_slice(&shared_secret_bytes).map_err(|_| {
        SockudoError::Encryption {
            message: "Failed to create cipher from shared secret".to_string(),
        }
    })?;

    // Generate random nonce
    let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);

    // Encrypt the data
    let data_string = data.to_string();
    let ciphertext = cipher
        .encrypt(&nonce, data_string.as_bytes())
        .map_err(|_| SockudoError::Encryption {
            message: "Encryption failed".to_string(),
        })?;

    // Return encrypted payload as JSON string
    let encrypted_payload = json!({
        "nonce": BASE64.encode(&nonce),
        "ciphertext": BASE64.encode(&ciphertext),
    });

    Ok(sonic_rs::to_string(&encrypted_payload)?)
}

/// Stub function when encryption is disabled
#[cfg(not(feature = "encryption"))]
#[allow(dead_code)]
fn encrypt(_sockudo: &Sockudo, _channel: &str, _data: &EventData) -> Result<String> {
    Err(SockudoError::Encryption {
        message: "Encryption support is not enabled. Enable the 'encryption' feature to use encrypted channels.".to_string(),
    })
}

/// Triggers an event on channels
pub async fn trigger<D: Into<EventData>>(
    sockudo: &Sockudo,
    channels: &[Channel],
    event_name: impl AsRef<str>,
    data: D,
    params: Option<&TriggerParams>,
) -> Result<reqwest::Response> {
    let data = data.into();
    let event_name = event_name.as_ref();

    // Validate event name
    if event_name.len() > 200 {
        return Err(SockudoError::Validation {
            message: format!("Event name too long: '{}' (max 200 characters)", event_name),
        });
    }

    // Convert channels to strings
    let channel_strings: Vec<String> = channels.iter().map(|c| c.full_name()).collect();

    // Extract idempotency key for the header
    let idempotency_key = params.and_then(|p| p.idempotency_key.clone());

    let mut extra_headers = HashMap::new();
    if let Some(ref key) = idempotency_key {
        extra_headers.insert("X-Idempotency-Key".to_string(), key.clone());
    }

    if channels.len() == 1 && channels[0].is_encrypted() {
        #[cfg(feature = "encryption")]
        {
            let encrypted_data = encrypt(sockudo, &channel_strings[0], &data)?;

            let mut event = Event {
                name: event_name.to_string(),
                data: encrypted_data,
                channels: channel_strings,
                socket_id: None,
                info: None,
                tags: None,
                idempotency_key: idempotency_key.clone(),
                extras: None,
            };

            if let Some(params) = params {
                event.socket_id = params.socket_id.clone();
                event.info = params.info.clone();
                event.tags = params.tags.clone();
                event.extras = params.extras.clone();
            }

            let event_json = sonic_rs::to_value(&event)?;
            sockudo
                .post_with_headers("/events", &event_json, &extra_headers)
                .await
        }

        #[cfg(not(feature = "encryption"))]
        {
            Err(SockudoError::Encryption {
                message: "Encryption support is not enabled. Enable the 'encryption' feature to use encrypted channels.".to_string(),
            })
        }
    } else {
        // Check for encrypted channels in multi-channel trigger
        for channel in channels {
            if channel.is_encrypted() {
                return Err(SockudoError::Validation {
                    message:
                        "You cannot trigger to multiple channels when using encrypted channels"
                            .to_string(),
                });
            }
        }

        let mut event = Event {
            name: event_name.to_string(),
            data: data.to_string(),
            channels: channel_strings,
            socket_id: None,
            info: None,
            tags: None,
            idempotency_key: idempotency_key.clone(),
            extras: None,
        };

        if let Some(params) = params {
            event.socket_id = params.socket_id.clone();
            event.info = params.info.clone();
            event.tags = params.tags.clone();
            event.extras = params.extras.clone();
        }

        let event_json = sonic_rs::to_value(&event)?;
        sockudo
            .post_with_headers("/events", &event_json, &extra_headers)
            .await
    }
}

/// Triggers an event on channel names (backward compatibility)
pub async fn trigger_on_channels<D: Into<EventData>>(
    sockudo: &Sockudo,
    channels: &[String],
    event_name: impl AsRef<str>,
    data: D,
    params: Option<&TriggerParams>,
) -> Result<reqwest::Response> {
    let channels: Result<Vec<Channel>> = channels.iter().map(Channel::from_string).collect();
    let channels = channels?;
    trigger(sockudo, &channels, event_name, data, params).await
}

/// Triggers a batch of events
pub async fn trigger_batch(
    sockudo: &Sockudo,
    mut batch: Vec<BatchEvent>,
    idempotency_key: Option<&str>,
) -> Result<reqwest::Response> {
    // Validate batch size
    if batch.is_empty() {
        return Err(SockudoError::Validation {
            message: "Batch cannot be empty".to_string(),
        });
    }

    if batch.len() > 10 {
        return Err(SockudoError::Validation {
            message: format!("Batch too large: {} events (max 10)", batch.len()),
        });
    }

    // Encrypt data for encrypted channels
    for event in &mut batch {
        let channel = Channel::from_string(&event.channel)?;
        if channel.is_encrypted() {
            #[cfg(feature = "encryption")]
            {
                let data = EventData::String(event.data.clone());
                event.data = encrypt(sockudo, &event.channel, &data)?;
            }

            #[cfg(not(feature = "encryption"))]
            {
                return Err(SockudoError::Encryption {
                    message: "Encryption support is not enabled. Enable the 'encryption' feature to use encrypted channels.".to_string(),
                });
            }
        }
    }

    let batch_payload = json!({ "batch": batch });
    let mut extra_headers = HashMap::new();
    if let Some(key) = idempotency_key {
        extra_headers.insert("X-Idempotency-Key".to_string(), key.to_string());
    }
    sockudo
        .post_with_headers("/batch_events", &batch_payload, &extra_headers)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    #[test]
    fn test_event_data_conversions() {
        // Test string
        let data = EventData::from_string("hello");
        assert_eq!(data.to_string(), "hello");

        // Test JSON
        let json_data = json!({"key": "value"});
        let data = EventData::from_json(json_data.clone());
        assert_eq!(data.as_json().unwrap(), json_data);

        // Test From implementations
        let data: EventData = "test".into();
        assert!(matches!(data, EventData::String(_)));

        let data: EventData = json!({"test": 123}).into();
        assert!(matches!(data, EventData::Json(_)));
    }

    #[test]
    fn test_batch_event_builder() {
        let event = BatchEvent::new("test-event", "test-channel", "test-data")
            .with_socket_id("123.456")
            .with_info("test-info");

        assert_eq!(event.name, "test-event");
        assert_eq!(event.channel, "test-channel");
        assert_eq!(event.data, "test-data");
        assert_eq!(event.socket_id, Some("123.456".to_string()));
        assert_eq!(event.info, Some("test-info".to_string()));
    }

    #[test]
    fn test_batch_event_with_tags() {
        let mut tags = HashMap::new();
        tags.insert("symbol".to_string(), "BONK".to_string());
        tags.insert("price_usd".to_string(), "0.00001".to_string());

        let event =
            BatchEvent::new("test-event", "test-channel", "test-data").with_tags(tags.clone());

        assert_eq!(event.tags, Some(tags));
    }

    #[test]
    fn test_trigger_params_builder() {
        let params = TriggerParams::builder()
            .socket_id("123.456")
            .info("test-info")
            .build();

        assert_eq!(params.socket_id, Some("123.456".to_string()));
        assert_eq!(params.info, Some("test-info".to_string()));
        assert_eq!(params.idempotency_key, None);
    }

    #[test]
    fn test_trigger_params_builder_with_tags() {
        let mut tags = HashMap::new();
        tags.insert("event_type".to_string(), "goal".to_string());

        let params = TriggerParams::builder().tags(tags.clone()).build();

        assert_eq!(params.tags, Some(tags));
    }

    #[test]
    fn test_trigger_params_builder_with_idempotency_key() {
        let params = TriggerParams::builder()
            .socket_id("123.456")
            .idempotency_key("my-unique-key-123")
            .build();

        assert_eq!(params.socket_id, Some("123.456".to_string()));
        assert_eq!(
            params.idempotency_key,
            Some("my-unique-key-123".to_string())
        );
    }

    #[test]
    fn test_trigger_params_builder_with_auto_idempotency_key() {
        let params = TriggerParams::builder().auto_idempotency_key().build();

        assert!(params.idempotency_key.is_some());
        let key = params.idempotency_key.unwrap();
        // UUID v4 format: 8-4-4-4-12 hex chars
        assert_eq!(key.len(), 36);
        assert_eq!(key.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn test_generate_idempotency_key() {
        let key1 = generate_idempotency_key();
        let key2 = generate_idempotency_key();

        // Each key should be a valid UUID v4 (36 chars with dashes)
        assert_eq!(key1.len(), 36);
        assert_eq!(key2.len(), 36);
        // Keys should be unique
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_batch_event_with_idempotency_key() {
        let event = BatchEvent::new("test-event", "test-channel", "test-data")
            .with_idempotency_key("batch-key-123");

        assert_eq!(event.idempotency_key, Some("batch-key-123".to_string()));
    }

    #[test]
    fn test_batch_event_with_auto_idempotency_key() {
        let event =
            BatchEvent::new("test-event", "test-channel", "test-data").with_auto_idempotency_key();

        assert!(event.idempotency_key.is_some());
        assert_eq!(event.idempotency_key.unwrap().len(), 36);
    }

    #[test]
    fn test_event_serialization_with_idempotency_key() {
        let event = Event {
            name: "test".to_string(),
            data: "{}".to_string(),
            channels: vec!["test-channel".to_string()],
            socket_id: None,
            info: None,
            tags: None,
            idempotency_key: Some("key-123".to_string()),
            extras: None,
        };

        let json_str = sonic_rs::to_string(&event).unwrap();
        assert!(json_str.contains("\"idempotency_key\":\"key-123\""));
    }

    #[test]
    fn test_event_serialization_without_idempotency_key() {
        let event = Event {
            name: "test".to_string(),
            data: "{}".to_string(),
            channels: vec!["test-channel".to_string()],
            socket_id: None,
            info: None,
            tags: None,
            idempotency_key: None,
            extras: None,
        };

        let json_str = sonic_rs::to_string(&event).unwrap();
        assert!(!json_str.contains("idempotency_key"));
    }
}
