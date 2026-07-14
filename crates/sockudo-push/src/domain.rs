use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use aws_lc_rs::pbkdf2;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sonic_rs::Value;
use sonic_rs::prelude::*;
use thiserror::Error;
use url::{Host, Url};
use zeroize::Zeroize;

pub const MAX_METADATA_BYTES: usize = 4096;
pub const MAX_TEMPLATE_DATA_BYTES: usize = 8192;
pub const MAX_PROVIDER_OVERRIDE_BYTES: usize = 4096;
pub const MAX_RENDERED_TEMPLATE_BYTES: usize = 4096;
pub const MAX_CURSOR_BYTES: usize = 2048;
pub const MAX_APP_ID_BYTES: usize = 128;
pub const MAX_PUSH_TARGETS: usize = 1024;
pub const MAX_PUSH_TITLE_BYTES: usize = 256;
pub const MAX_PUSH_BODY_BYTES: usize = 4096;
pub const MAX_PUSH_ICON_BYTES: usize = 2048;
pub const MAX_APNS_PAYLOAD_BYTES: usize = 4096;
pub const MAX_FCM_PAYLOAD_BYTES: usize = 4096;
pub const MAX_WEB_PUSH_PAYLOAD_BYTES: usize = 4096;
pub const MAX_HMS_PAYLOAD_BYTES: usize = 4096;
pub const MAX_WNS_PAYLOAD_BYTES: usize = 5000;
pub const MAX_REALTIME_PAYLOAD_BYTES: usize = 64 * 1024;
pub const DEVICE_IDENTITY_TOKEN_BYTES: usize = 32;
pub const DEVICE_SECRET_PBKDF2_ITERATIONS: u32 = 120_000;
pub const DEFAULT_PUSH_FANOUT_FAST_THRESHOLD: u64 = 10_000;
pub const DEFAULT_PUSH_FANOUT_SHARD_SIZE: u64 = 100_000;
pub const DEFAULT_PUSH_FANOUT_PAGE_SIZE: usize = 1_000;
pub const DEFAULT_PUSH_PROVIDER_BATCH_SIZE: usize = 500;
pub const DEFAULT_PUSH_STATUS_RETENTION_DAYS: u64 = 30;
pub const DEFAULT_PUSH_RETRY_INITIAL_BACKOFF_MS: u64 = 1_000;
pub const DEFAULT_PUSH_RETRY_MAX_BACKOFF_MS: u64 = 60_000;
pub const DEFAULT_PUSH_RETRY_MAX_ATTEMPTS: u32 = 5;
pub const DEFAULT_PUSH_RETRY_MAX_AGE_MS: u64 = 86_400_000;
pub const DEFAULT_PUSH_RETRY_JITTER_RATIO_PERCENT: u8 = 20;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PushDomainError {
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("{field} exceeds {max_bytes} bytes")]
    TooLarge {
        field: &'static str,
        max_bytes: usize,
    },
    #[error("{field} is invalid: {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },
    #[error("{field} is not allowed: {reason}")]
    DisallowedField {
        field: &'static str,
        reason: &'static str,
    },
    #[error("cursor decode failed")]
    CursorDecode,
    #[error("cursor belongs to app {found}, expected {expected}")]
    CursorAppMismatch { expected: String, found: String },
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Result<Self, PushDomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(PushDomainError::EmptyField { field: "secret" });
        }
        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    pub fn stable_hash(&self) -> String {
        stable_hash(self.0.as_bytes())
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

impl Drop for SecretString {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub fn generate_device_identity_token() -> SecretString {
    let bytes: [u8; DEVICE_IDENTITY_TOKEN_BYTES] = rand::random();
    SecretString(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn hash_device_identity_token(token: &SecretString) -> SecretString {
    let salt: [u8; 16] = rand::random();
    hash_device_identity_token_with_salt(token.expose_secret(), &salt)
}

pub fn verify_device_identity_token(token: &str, stored_hash: &SecretString) -> bool {
    let parts = stored_hash.expose_secret().split('$').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "pbkdf2-sha256" {
        return false;
    }

    let Ok(iterations) = parts[1].parse::<u32>() else {
        return false;
    };
    if iterations < DEVICE_SECRET_PBKDF2_ITERATIONS {
        return false;
    }
    let Ok(salt) = URL_SAFE_NO_PAD.decode(parts[2]) else {
        return false;
    };

    let candidate = pbkdf2_sha256(token.as_bytes(), &salt, iterations);
    let candidate = URL_SAFE_NO_PAD.encode(candidate);
    fixed_length_secure_compare(&candidate, parts[3])
}

pub fn is_hashed_device_secret(secret: &SecretString) -> bool {
    secret.expose_secret().starts_with("pbkdf2-sha256$")
}

fn hash_device_identity_token_with_salt(token: &str, salt: &[u8]) -> SecretString {
    let digest = pbkdf2_sha256(token.as_bytes(), salt, DEVICE_SECRET_PBKDF2_ITERATIONS);
    SecretString(format!(
        "pbkdf2-sha256${}${}${}",
        DEVICE_SECRET_PBKDF2_ITERATIONS,
        URL_SAFE_NO_PAD.encode(salt),
        URL_SAFE_NO_PAD.encode(digest)
    ))
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let iterations = NonZeroU32::new(iterations.max(1)).expect("iterations are clamped to >= 1");
    let mut output = [0_u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        password,
        &mut output,
    );
    output
}

fn fixed_length_secure_compare(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let max_len = a_bytes.len().max(b_bytes.len());
    let mut diff = a_bytes.len() ^ b_bytes.len();
    for index in 0..max_len {
        let left = a_bytes.get(index).copied().unwrap_or(0);
        let right = b_bytes.get(index).copied().unwrap_or(0);
        diff |= usize::from(left ^ right);
    }
    diff == 0
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EncryptedSecret(String);

impl EncryptedSecret {
    pub fn new(value: impl Into<String>) -> Result<Self, PushDomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(PushDomainError::EmptyField {
                field: "encrypted_secret",
            });
        }
        Ok(Self(value))
    }

    pub fn ciphertext(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EncryptedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FormFactor {
    Phone,
    Tablet,
    Desktop,
    Tv,
    Car,
    Watch,
    Embedded,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Platform {
    Android,
    Ios,
    Browser,
    Windows,
    Macos,
    Watchos,
    Tvos,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PushProviderKind {
    Fcm,
    Apns,
    WebPush,
    Hms,
    Wns,
    Realtime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DevicePushState {
    Active,
    Failing,
    Failed,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "transportType", rename_all = "camelCase")]
pub enum PushRecipient {
    #[serde(rename = "gcm")]
    Fcm {
        #[serde(rename = "registrationToken")]
        registration_token: SecretString,
    },
    #[serde(rename = "apns")]
    Apns {
        #[serde(rename = "deviceToken")]
        device_token: SecretString,
    },
    #[serde(rename = "web")]
    Web {
        endpoint: SecretString,
        p256dh: SecretString,
        auth: SecretString,
    },
    #[serde(rename = "hms")]
    Hms {
        #[serde(rename = "registrationToken")]
        registration_token: SecretString,
    },
    #[serde(rename = "wns")]
    Wns {
        #[serde(rename = "channelUri")]
        channel_uri: SecretString,
    },
    #[serde(rename = "realtime")]
    Realtime { channel: String },
}

impl PushRecipient {
    pub fn provider(&self) -> PushProviderKind {
        match self {
            Self::Fcm { .. } => PushProviderKind::Fcm,
            Self::Apns { .. } => PushProviderKind::Apns,
            Self::Web { .. } => PushProviderKind::WebPush,
            Self::Hms { .. } => PushProviderKind::Hms,
            Self::Wns { .. } => PushProviderKind::Wns,
            Self::Realtime { .. } => PushProviderKind::Realtime,
        }
    }

    pub fn token_hash(&self) -> String {
        match self {
            Self::Fcm { registration_token } | Self::Hms { registration_token } => {
                registration_token.stable_hash()
            }
            Self::Apns { device_token } => device_token.stable_hash(),
            Self::Web { endpoint, .. } => endpoint.stable_hash(),
            Self::Wns { channel_uri } => channel_uri.stable_hash(),
            Self::Realtime { channel } => stable_hash(channel.as_bytes()),
        }
    }

    pub fn validate(&self) -> Result<(), PushDomainError> {
        match self {
            Self::Fcm { registration_token } | Self::Hms { registration_token } => {
                require_secret("registrationToken", registration_token)
            }
            Self::Apns { device_token } => require_secret("deviceToken", device_token),
            Self::Web {
                endpoint,
                p256dh,
                auth,
            } => {
                require_secret("endpoint", endpoint)?;
                validate_web_push_endpoint(endpoint.expose_secret())?;
                require_secret("p256dh", p256dh)?;
                require_secret("auth", auth)
            }
            Self::Wns { channel_uri } => {
                require_secret("channelUri", channel_uri)?;
                validate_web_push_endpoint(channel_uri.expose_secret())
            }
            Self::Realtime { channel } => require_non_empty("channel", channel),
        }
    }
}

impl fmt::Debug for PushRecipient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushRecipient")
            .field("provider", &self.provider())
            .field("token_hash", &self.token_hash())
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBucketPolicy {
    pub capacity: u32,
    pub refill_per_second: u32,
}

impl TokenBucketPolicy {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        if self.capacity == 0 {
            return Err(PushDomainError::InvalidField {
                field: "capacity",
                reason: "must be greater than zero",
            });
        }
        if self.refill_per_second == 0 {
            return Err(PushDomainError::InvalidField {
                field: "refillPerSecond",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DevicePushDetails {
    pub recipient: PushRecipient,
    pub state: DevicePushState,
    #[serde(default)]
    pub failure_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<String>,
}

impl fmt::Debug for DevicePushDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DevicePushDetails")
            .field("recipient", &self.recipient)
            .field("state", &self.state)
            .field("failure_count", &self.failure_count)
            .field(
                "error_reason",
                &self.error_reason.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceDetails {
    pub app_id: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub form_factor: FormFactor,
    pub platform: Platform,
    #[serde(default)]
    pub metadata: Value,
    pub device_secret: SecretString,
    pub timezone: String,
    pub locale: String,
    #[serde(default)]
    pub last_active_at_ms: u64,
    pub push: DevicePushDetails,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_rate_policy: Option<TokenBucketPolicy>,
}

impl DeviceDetails {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        validate_app_id(&self.app_id)?;
        require_non_empty("id", &self.id)?;
        if self
            .id
            .starts_with(ChannelSubscription::CLIENT_SCOPE_PREFIX)
        {
            return Err(PushDomainError::InvalidField {
                field: "id",
                reason: "uses a reserved client-subscription prefix",
            });
        }
        require_secret("deviceSecret", &self.device_secret)?;
        if !is_hashed_device_secret(&self.device_secret) {
            return Err(PushDomainError::InvalidField {
                field: "deviceSecret",
                reason: "must be stored as a pbkdf2-sha256 hash",
            });
        }
        require_non_empty("timezone", &self.timezone)?;
        require_non_empty("locale", &self.locale)?;
        require_json_bound("metadata", &self.metadata, MAX_METADATA_BYTES)?;
        self.push.recipient.validate()?;
        if let Some(policy) = &self.push_rate_policy {
            policy.validate()?;
        }
        Ok(())
    }

    pub fn device_key(&self) -> String {
        format!("device:{}:{}", self.app_id, self.id)
    }

    pub fn registration_fingerprint(&self) -> String {
        stable_hash(
            format!(
                "{}:{}:{:?}:{}",
                self.app_id,
                self.id,
                self.push.recipient.provider(),
                self.push.recipient.token_hash()
            )
            .as_bytes(),
        )
    }

    pub fn record_delivery_failure(&mut self, threshold: u32, reason: impl Into<String>) {
        let threshold = threshold.max(2);
        self.push.failure_count = self.push.failure_count.saturating_add(1);
        self.push.error_reason = Some(redact_error_reason(reason.into()));
        self.push.state = match self.push.state {
            DevicePushState::Active => DevicePushState::Failing,
            DevicePushState::Failing if self.push.failure_count >= threshold => {
                DevicePushState::Failed
            }
            DevicePushState::Failing => DevicePushState::Failing,
            DevicePushState::Failed => DevicePushState::Failed,
        };
    }

    pub fn record_delivery_success(&mut self) {
        if self.push.state == DevicePushState::Failing {
            self.push.state = DevicePushState::Active;
            self.push.failure_count = 0;
            self.push.error_reason = None;
        }
    }
}

impl fmt::Debug for DeviceDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceDetails")
            .field("app_id", &self.app_id)
            .field("id", &self.id)
            .field("client_id", &self.client_id)
            .field("form_factor", &self.form_factor)
            .field("platform", &self.platform)
            .field("metadata", &"[REDACTED]")
            .field("device_secret", &"[REDACTED]")
            .field("timezone", &self.timezone)
            .field("locale", &self.locale)
            .field("last_active_at_ms", &self.last_active_at_ms)
            .field("push", &self.push)
            .field("push_rate_policy", &self.push_rate_policy)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSubscription {
    pub app_id: String,
    pub channel: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub provider: PushProviderKind,
    pub token_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_version: Option<u64>,
}

impl ChannelSubscription {
    const CLIENT_SCOPE_PREFIX: &'static str = "__sockudo_client_scope__:";

    pub fn from_device(channel: impl Into<String>, device: &DeviceDetails) -> Self {
        Self {
            app_id: device.app_id.clone(),
            channel: channel.into(),
            device_id: device.id.clone(),
            client_id: device.client_id.clone(),
            provider: device.push.recipient.provider(),
            token_hash: device.push.recipient.token_hash(),
            credential_version: None,
        }
    }

    pub fn from_client(
        app_id: impl Into<String>,
        channel: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Self {
        let client_id = client_id.into();
        Self {
            app_id: app_id.into(),
            channel: channel.into(),
            device_id: format!(
                "{}{}",
                Self::CLIENT_SCOPE_PREFIX,
                stable_hash(client_id.as_bytes())
            ),
            client_id: Some(client_id.clone()),
            provider: PushProviderKind::Realtime,
            token_hash: stable_hash(client_id.as_bytes()),
            credential_version: None,
        }
    }

    pub fn scoped_client_id(&self) -> Option<&str> {
        let client_id = self.client_id.as_deref()?;
        let expected = format!(
            "{}{}",
            Self::CLIENT_SCOPE_PREFIX,
            stable_hash(client_id.as_bytes())
        );
        (self.device_id == expected).then_some(client_id)
    }

    pub fn validate(&self) -> Result<(), PushDomainError> {
        validate_app_id(&self.app_id)?;
        require_non_empty("channel", &self.channel)?;
        require_non_empty("deviceId", &self.device_id)?;
        require_non_empty("tokenHash", &self.token_hash)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCredential {
    pub app_id: String,
    pub credential_id: String,
    pub provider: PushProviderKind,
    pub version: u64,
    pub material: ProviderCredentialMaterial,
}

impl ProviderCredential {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        validate_app_id(&self.app_id)?;
        require_non_empty("credentialId", &self.credential_id)?;
        if self.version == 0 {
            return Err(PushDomainError::InvalidField {
                field: "version",
                reason: "must be greater than zero",
            });
        }
        self.material.validate()
    }
}

impl fmt::Debug for ProviderCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderCredential")
            .field("app_id", &self.app_id)
            .field("credential_id", &self.credential_id)
            .field("provider", &self.provider)
            .field("version", &self.version)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "camelCase")]
pub enum ProviderCredentialMaterial {
    #[serde(rename = "fcm")]
    Fcm {
        service_account_json: EncryptedSecret,
    },
    #[serde(rename = "apns")]
    Apns {
        #[serde(skip_serializing_if = "Option::is_none")]
        p12: Option<EncryptedSecret>,
        #[serde(skip_serializing_if = "Option::is_none")]
        p12_password: Option<EncryptedSecret>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pem: Option<EncryptedSecret>,
        #[serde(skip_serializing_if = "Option::is_none")]
        team_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        key_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        private_key: Option<EncryptedSecret>,
    },
    #[serde(rename = "webPush")]
    WebPush {
        public_key: String,
        private_key: EncryptedSecret,
    },
    #[serde(rename = "hms")]
    Hms {
        hms_app_id: String,
        client_secret: EncryptedSecret,
    },
    #[serde(rename = "wns")]
    Wns {
        package_sid: String,
        client_secret: EncryptedSecret,
    },
}

impl ProviderCredentialMaterial {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        match self {
            Self::Fcm {
                service_account_json,
            } => require_encrypted("serviceAccountJson", service_account_json),
            Self::Apns {
                p12,
                pem,
                team_id,
                key_id,
                private_key,
                ..
            } => {
                let token_auth_present =
                    team_id.is_some() && key_id.is_some() && private_key.is_some();
                if p12.is_none() && pem.is_none() && !token_auth_present {
                    return Err(PushDomainError::InvalidField {
                        field: "material",
                        reason: "apns requires p12, pem, or token auth material",
                    });
                }
                if token_auth_present {
                    require_non_empty("teamId", team_id.as_deref().unwrap_or_default())?;
                    require_non_empty("keyId", key_id.as_deref().unwrap_or_default())?;
                }
                Ok(())
            }
            Self::WebPush {
                public_key,
                private_key,
            } => {
                require_non_empty("publicKey", public_key)?;
                require_encrypted("privateKey", private_key)
            }
            Self::Hms {
                hms_app_id,
                client_secret,
            } => {
                require_non_empty("hmsAppId", hms_app_id)?;
                require_encrypted("clientSecret", client_secret)
            }
            Self::Wns {
                package_sid,
                client_secret,
            } => {
                require_non_empty("packageSid", package_sid)?;
                require_encrypted("clientSecret", client_secret)
            }
        }
    }
}

impl fmt::Debug for ProviderCredentialMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let provider = match self {
            Self::Fcm { .. } => "fcm",
            Self::Apns { .. } => "apns",
            Self::WebPush { .. } => "webPush",
            Self::Hms { .. } => "hms",
            Self::Wns { .. } => "wns",
        };
        f.debug_struct("ProviderCredentialMaterial")
            .field("provider", &provider)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateContent {
    pub title: String,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sound: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collapse_key: Option<String>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationTemplate {
    pub app_id: String,
    pub template_id: String,
    pub default_locale: String,
    pub locales: BTreeMap<String, TemplateContent>,
    #[serde(default)]
    pub provider_overrides: BTreeMap<PushProviderKind, ProviderOverridePayload>,
}

impl NotificationTemplate {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        validate_app_id(&self.app_id)?;
        require_non_empty("templateId", &self.template_id)?;
        require_non_empty("defaultLocale", &self.default_locale)?;
        if !self.locales.contains_key(&self.default_locale) {
            return Err(PushDomainError::InvalidField {
                field: "defaultLocale",
                reason: "must exist in locales",
            });
        }
        for (locale, content) in &self.locales {
            require_non_empty("locale", locale)?;
            require_non_empty("title", &content.title)?;
            require_non_empty("body", &content.body)?;
        }
        for override_payload in self.provider_overrides.values() {
            override_payload.validate()?;
        }
        Ok(())
    }

    pub fn resolve_locale(&self, requested_locale: Option<&str>) -> Option<&TemplateContent> {
        if let Some(locale) = requested_locale {
            if let Some(content) = self.locales.get(locale) {
                return Some(content);
            }
            let normalized = locale.replace('_', "-");
            let segments = normalized.split('-').collect::<Vec<_>>();
            for length in (1..segments.len()).rev() {
                let candidate = segments[..length].join("-");
                if let Some(content) = self.locales.get(&candidate) {
                    return Some(content);
                }
            }
        }
        self.locales.get(&self.default_locale)
    }
}

impl fmt::Debug for NotificationTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotificationTemplate")
            .field("app_id", &self.app_id)
            .field("template_id", &self.template_id)
            .field("default_locale", &self.default_locale)
            .field("locales", &self.locales.keys().collect::<Vec<_>>())
            .field("provider_overrides", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    #[serde(default)]
    pub template_data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sound: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collapse_key: Option<String>,
}

impl PushPayload {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        require_optional_bound("title", &self.title, MAX_PUSH_TITLE_BYTES)?;
        require_optional_bound("body", &self.body, MAX_PUSH_BODY_BYTES)?;
        require_optional_bound("icon", &self.icon, MAX_PUSH_ICON_BYTES)?;
        if let Some(icon) = &self.icon {
            validate_https_url("icon", icon)?;
        }
        require_json_bound("templateData", &self.template_data, MAX_TEMPLATE_DATA_BYTES)
    }
}

impl fmt::Debug for PushPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushPayload")
            .field("template_id", &self.template_id)
            .field("template_data", &"[REDACTED]")
            .field("title", &self.title)
            .field("body", &self.body.as_ref().map(|_| "[REDACTED]"))
            .field("icon", &self.icon)
            .field("sound", &self.sound)
            .field("collapse_key", &self.collapse_key)
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOverridePayload {
    pub provider: PushProviderKind,
    pub payload: Value,
}

impl ProviderOverridePayload {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        require_json_bound("payload", &self.payload, MAX_PROVIDER_OVERRIDE_BYTES)
    }
}

impl fmt::Debug for ProviderOverridePayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderOverridePayload")
            .field("provider", &self.provider)
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderedProviderPayload {
    pub provider: PushProviderKind,
    pub payload: Value,
    pub used_override: bool,
}

impl fmt::Debug for RenderedProviderPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RenderedProviderPayload")
            .field("provider", &self.provider)
            .field("payload", &"[REDACTED]")
            .field("used_override", &self.used_override)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PublishTarget {
    Device {
        device_id: String,
    },
    Client {
        client_id: String,
    },
    Channel {
        channel: String,
    },
    RegisteredTopic {
        topic: String,
    },
    UserTopic {
        topic: String,
    },
    Recipient {
        recipient: PushRecipient,
    },
    ProviderTopic {
        provider: PushProviderKind,
        topic: String,
    },
    ProviderCondition {
        provider: PushProviderKind,
        condition: String,
    },
    IndexedFilter {
        filter: Value,
    },
}

impl PublishTarget {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        match self {
            Self::Device { device_id } => require_non_empty("deviceId", device_id),
            Self::Client { client_id } => require_non_empty("clientId", client_id),
            Self::Channel { channel } => require_non_empty("channel", channel),
            Self::RegisteredTopic { topic }
            | Self::UserTopic { topic }
            | Self::ProviderTopic { topic, .. } => require_non_empty("topic", topic),
            Self::ProviderCondition { condition, .. } => require_non_empty("condition", condition),
            Self::Recipient { recipient } => recipient.validate(),
            Self::IndexedFilter { filter } => {
                require_json_bound("filter", filter, MAX_PROVIDER_OVERRIDE_BYTES)
            }
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishIntent {
    pub app_id: String,
    pub publish_id: String,
    pub targets: Vec<PublishTarget>,
    pub payload: PushPayload,
    #[serde(default)]
    pub provider_overrides: Vec<ProviderOverridePayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_before_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

impl PublishIntent {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        validate_app_id(&self.app_id)?;
        require_non_empty("publishId", &self.publish_id)?;
        if self.targets.is_empty() {
            return Err(PushDomainError::InvalidField {
                field: "targets",
                reason: "must contain at least one target",
            });
        }
        if self.targets.len() > MAX_PUSH_TARGETS {
            return Err(PushDomainError::InvalidField {
                field: "targets",
                reason: "too many targets",
            });
        }
        for target in &self.targets {
            target.validate()?;
        }
        self.payload.validate()?;
        for override_payload in &self.provider_overrides {
            override_payload.validate()?;
        }
        Ok(())
    }

    pub fn idempotency_key(&self) -> String {
        let mut canonical = to_json_value(self).expect("PublishIntent serialization is infallible");
        canonicalize_intent_json(&mut canonical);
        let mut bytes = Vec::new();
        write_canonical_json(&canonical, &mut bytes)
            .expect("canonical intent serialization is infallible");
        stable_hash(bytes.as_slice())
    }
}

impl fmt::Debug for PublishIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishIntent")
            .field("app_id", &self.app_id)
            .field("publish_id", &self.publish_id)
            .field("targets", &self.targets)
            .field("payload", &self.payload)
            .field("provider_overrides", &"[REDACTED]")
            .field("not_before_ms", &self.not_before_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PublishLifecycleState {
    Queued,
    Planning,
    Throttled,
    #[serde(rename = "quota_exceeded")]
    QuotaExceeded,
    Dispatching,
    Cancelled,
    Expired,
    Failed,
    #[serde(rename = "dead_lettered")]
    DeadLettered,
    Succeeded,
    PartiallySucceeded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FanoutRegime {
    FastPath,
    ShardPath,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FanoutConfig {
    pub fast_threshold: u64,
    pub shard_size: u64,
    pub page_size: usize,
    pub provider_batch_size: usize,
    pub status_retention_days: u64,
}

impl Default for FanoutConfig {
    fn default() -> Self {
        let config = Self {
            fast_threshold: DEFAULT_PUSH_FANOUT_FAST_THRESHOLD,
            shard_size: DEFAULT_PUSH_FANOUT_SHARD_SIZE,
            page_size: DEFAULT_PUSH_FANOUT_PAGE_SIZE,
            provider_batch_size: DEFAULT_PUSH_PROVIDER_BATCH_SIZE,
            status_retention_days: DEFAULT_PUSH_STATUS_RETENTION_DAYS,
        };
        debug_assert!(config.validate().is_ok());
        config
    }
}

impl FanoutConfig {
    pub fn validate(&self) -> Result<(), PushDomainError> {
        if self.fast_threshold == 0 {
            return Err(PushDomainError::InvalidField {
                field: "fastThreshold",
                reason: "must be greater than zero",
            });
        }
        if self.shard_size == 0 {
            return Err(PushDomainError::InvalidField {
                field: "shardSize",
                reason: "must be greater than zero",
            });
        }
        if self.page_size == 0 {
            return Err(PushDomainError::InvalidField {
                field: "pageSize",
                reason: "must be greater than zero",
            });
        }
        if self.provider_batch_size == 0 {
            return Err(PushDomainError::InvalidField {
                field: "providerBatchSize",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishCounters {
    pub planned: u64,
    pub dispatched: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub expired: u64,
    #[serde(default)]
    pub retry_scheduled: u64,
    #[serde(default)]
    pub retry_attempted: u64,
    #[serde(default)]
    pub dead_lettered: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishStatus {
    pub app_id: String,
    pub publish_id: String,
    pub state: PublishLifecycleState,
    pub counters: PublishCounters,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fanout_regime: Option<FanoutRegime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishLogEvent {
    pub app_id: String,
    pub publish_id: String,
    pub event_id: String,
    pub occurred_at_ms: u64,
    pub intent: PublishIntent,
    pub fanout_regime: FanoutRegime,
    pub expected_recipients: u64,
    pub fast_threshold: u64,
    pub shard_size: u64,
}

impl PublishLogEvent {
    pub fn queue_key(&self) -> String {
        format!("{}:{}", self.app_id, self.publish_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ShardJobStatus {
    Pending,
    Running,
    Deferred,
    Complete,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardJob {
    pub app_id: String,
    pub publish_id: String,
    pub shard_id: String,
    pub target: PublishTarget,
    pub payload: PushPayload,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_overrides: Vec<ProviderOverridePayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_before_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PushCursor>,
    pub page_size: usize,
    pub shard_size: u64,
    pub emitted_recipients: u64,
    pub emitted_batches: u64,
    pub status: ShardJobStatus,
}

impl ShardJob {
    pub fn queue_key(&self) -> String {
        format!("{}:{}:{}", self.app_id, self.publish_id, self.shard_id)
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryJob {
    pub app_id: String,
    pub publish_id: String,
    pub provider: PushProviderKind,
    pub batch_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    pub recipient: PushRecipient,
    #[serde(with = "arc_push_payload")]
    pub payload: Arc<PushPayload>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_arc_rendered_provider_payload"
    )]
    pub rendered_payload: Option<Arc<RenderedProviderPayload>>,
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_attempt_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_before_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

mod arc_push_payload {
    use super::PushPayload;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;

    pub fn serialize<S>(payload: &Arc<PushPayload>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        payload.as_ref().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<PushPayload>, D::Error>
    where
        D: Deserializer<'de>,
    {
        PushPayload::deserialize(deserializer).map(Arc::new)
    }
}

mod optional_arc_rendered_provider_payload {
    use super::RenderedProviderPayload;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;

    pub fn serialize<S>(
        payload: &Option<Arc<RenderedProviderPayload>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        payload.as_deref().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<Arc<RenderedProviderPayload>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<RenderedProviderPayload>::deserialize(deserializer)
            .map(|payload| payload.map(Arc::new))
    }
}

impl DeliveryJob {
    pub fn idempotency_key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.app_id,
            self.publish_id,
            provider_key(self.provider),
            self.batch_id,
            self.recipient.token_hash()
        )
    }
}

impl fmt::Debug for DeliveryJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeliveryJob")
            .field("app_id", &self.app_id)
            .field("publish_id", &self.publish_id)
            .field("provider", &self.provider)
            .field("batch_id", &self.batch_id)
            .field("device_id", &self.device_id)
            .field("recipient", &self.recipient)
            .field("payload", &self.payload)
            .field(
                "rendered_payload",
                &self.rendered_payload.as_ref().map(|_| "[REDACTED]"),
            )
            .field("attempt", &self.attempt)
            .field("first_attempt_at_ms", &self.first_attempt_at_ms)
            .field("not_before_ms", &self.not_before_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryBatch {
    pub app_id: String,
    pub publish_id: String,
    pub provider: PushProviderKind,
    pub batch_id: String,
    pub jobs: Vec<DeliveryJob>,
}

impl DeliveryBatch {
    pub fn idempotency_key(&self) -> String {
        stable_hash(
            format!(
                "{}:{}:{}:{}:{}",
                self.app_id,
                self.publish_id,
                provider_key(self.provider),
                self.batch_id,
                self.jobs.len()
            )
            .as_bytes(),
        )
    }

    pub fn queue_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.app_id,
            self.publish_id,
            provider_key(self.provider),
            self.batch_id
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeliveryOutcome {
    Accepted,
    Rejected,
    Retryable,
    Expired,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureClass {
    DeviceTerminal,
    DeviceTransient,
    ProviderTransient,
    ProviderQuota,
    CredentialAuth,
    CallerPayload,
    NetworkTransport,
    #[default]
    Unknown,
}

impl ProviderFailureClass {
    pub fn label(self) -> &'static str {
        match self {
            Self::DeviceTerminal => "device_terminal",
            Self::DeviceTransient => "device_transient",
            Self::ProviderTransient => "provider_transient",
            Self::ProviderQuota => "provider_quota",
            Self::CredentialAuth => "credential_auth",
            Self::CallerPayload => "caller_payload",
            Self::NetworkTransport => "network_transport",
            Self::Unknown => "unknown",
        }
    }

    pub fn is_device_terminal(self) -> bool {
        matches!(self, Self::DeviceTerminal)
    }

    pub fn is_device_transient(self) -> bool {
        matches!(self, Self::DeviceTransient)
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderError {
    pub class: String,
    #[serde(default, skip_serializing_if = "ProviderFailureClass::is_unknown")]
    pub failure_class: ProviderFailureClass,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ProviderError {
    pub fn resolved_failure_class(&self) -> ProviderFailureClass {
        if !self.failure_class.is_unknown() {
            return self.failure_class;
        }
        ProviderFailureClass::from_legacy_error(&self.class, self.reason.as_deref())
    }
}

impl ProviderFailureClass {
    fn from_legacy_error(class: &str, reason: Option<&str>) -> Self {
        match class {
            "invalid_token" | "unregistered" | "not_registered" | "expired_channel" => {
                if reason.is_some_and(|reason| reason.eq_ignore_ascii_case("sender_id_mismatch")) {
                    Self::CredentialAuth
                } else {
                    Self::DeviceTerminal
                }
            }
            "device_transient" => Self::DeviceTransient,
            "quota" => Self::ProviderQuota,
            "unavailable" => Self::ProviderTransient,
            "auth_failure" => Self::CredentialAuth,
            "invalid_payload" => Self::CallerPayload,
            "network_transport" => Self::NetworkTransport,
            _ => Self::Unknown,
        }
    }

    pub(crate) fn from_legacy_url_class(class: &str) -> Self {
        match class {
            "invalid_token" => Self::DeviceTerminal,
            "invalid_payload" => Self::CallerPayload,
            "auth_failure" => Self::CredentialAuth,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Debug for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderError")
            .field("class", &self.class)
            .field("failure_class", &self.resolved_failure_class())
            .field("reason", &self.reason.as_ref().map(|_| "[REDACTED]"))
            .field("retry_after_ms", &self.retry_after_ms)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryResult {
    pub app_id: String,
    pub publish_id: String,
    pub provider: PushProviderKind,
    pub batch_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    pub outcome: DeliveryOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProviderError>,
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryFeedback {
    pub result: DeliveryResult,
    pub delivery_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_job: Option<Box<DeliveryJob>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_attempt_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

impl DeliveryFeedback {
    pub fn from_result(result: DeliveryResult) -> Self {
        let delivery_key = format!(
            "{}:{}:{}:{}:{}",
            result.app_id,
            result.publish_id,
            provider_key(result.provider),
            result.batch_id,
            result.device_id.as_deref().unwrap_or("[provider-target]")
        );
        Self {
            result,
            delivery_key,
            retry_job: None,
            first_attempt_at_ms: None,
            expires_at_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryEvent {
    pub app_id: String,
    pub publish_id: String,
    pub event_id: String,
    pub occurred_at_ms: u64,
    pub result: DeliveryResult,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetter {
    pub app_id: String,
    pub publish_id: String,
    pub stage: String,
    pub key: String,
    pub reason: String,
    pub occurred_at_ms: u64,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryScheduleEntry {
    pub app_id: String,
    pub publish_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<PushProviderKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<Box<DeliveryJob>>,
    #[serde(default = "default_retry_attempt")]
    pub attempt: u32,
    #[serde(default)]
    pub first_attempt_at_ms: u64,
    #[serde(default)]
    pub next_attempt_at_ms: u64,
    #[serde(default = "default_retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ProviderError>,
    #[serde(default)]
    pub retry_idempotency_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stage: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<sonic_rs::Value>,
}

impl fmt::Debug for RetryScheduleEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryScheduleEntry")
            .field("app_id", &self.app_id)
            .field("publish_id", &self.publish_id)
            .field("provider", &self.provider)
            .field("job", &self.job)
            .field("attempt", &self.attempt)
            .field("first_attempt_at_ms", &self.first_attempt_at_ms)
            .field("next_attempt_at_ms", &self.next_attempt_at_ms)
            .field("max_attempts", &self.max_attempts)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("last_error", &self.last_error)
            .field("retry_idempotency_key", &self.retry_idempotency_key)
            .field("stage", &self.stage)
            .field("key", &self.key)
            .field("not_before_ms", &self.not_before_ms)
            .field("payload", &self.payload.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

fn default_retry_attempt() -> u32 {
    1
}

fn default_retry_max_attempts() -> u32 {
    DEFAULT_PUSH_RETRY_MAX_ATTEMPTS
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PushCursorKind {
    Device,
    ChannelSubscription,
    Credential,
    Template,
    ScheduledJob,
    PublishStatus,
    PublishLog,
    DeliveryEvent,
    DeadLetter,
    OperatorInvalidation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushCursor {
    pub app_id: String,
    pub kind: PushCursorKind,
    pub position: String,
    pub issued_at_ms: u64,
}

impl PushCursor {
    pub fn encode(&self) -> Result<String, PushDomainError> {
        require_non_empty("appId", &self.app_id)?;
        require_non_empty("position", &self.position)?;
        let bytes = sonic_rs::to_vec(self).map_err(|_| PushDomainError::CursorDecode)?;
        if bytes.len() > MAX_CURSOR_BYTES {
            return Err(PushDomainError::TooLarge {
                field: "cursor",
                max_bytes: MAX_CURSOR_BYTES,
            });
        }
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn decode(encoded: &str, expected_app_id: &str) -> Result<Self, PushDomainError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| PushDomainError::CursorDecode)?;
        if bytes.len() > MAX_CURSOR_BYTES {
            return Err(PushDomainError::TooLarge {
                field: "cursor",
                max_bytes: MAX_CURSOR_BYTES,
            });
        }
        let cursor: Self =
            sonic_rs::from_slice(&bytes).map_err(|_| PushDomainError::CursorDecode)?;
        if cursor.app_id != expected_app_id {
            return Err(PushDomainError::CursorAppMismatch {
                expected: expected_app_id.to_owned(),
                found: cursor.app_id,
            });
        }
        Ok(cursor)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteDeviceOutcome {
    Deleted,
    NotFound,
}

impl DeleteDeviceOutcome {
    pub fn is_success(self) -> bool {
        true
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), PushDomainError> {
    if value.trim().is_empty() {
        return Err(PushDomainError::EmptyField { field });
    }
    Ok(())
}

fn validate_app_id(value: &str) -> Result<(), PushDomainError> {
    require_non_empty("appId", value)?;
    if value.len() > MAX_APP_ID_BYTES {
        return Err(PushDomainError::TooLarge {
            field: "appId",
            max_bytes: MAX_APP_ID_BYTES,
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(PushDomainError::InvalidField {
            field: "appId",
            reason: "must contain only ASCII letters, digits, underscore, or hyphen",
        });
    }
    Ok(())
}

fn require_optional_bound(
    field: &'static str,
    value: &Option<String>,
    max_bytes: usize,
) -> Result<(), PushDomainError> {
    if value
        .as_deref()
        .is_some_and(|value| value.len() > max_bytes)
    {
        return Err(PushDomainError::TooLarge { field, max_bytes });
    }
    Ok(())
}

fn validate_https_url(field: &'static str, value: &str) -> Result<(), PushDomainError> {
    let parsed = Url::parse(value).map_err(|_| PushDomainError::InvalidField {
        field,
        reason: "must be a valid URL",
    })?;
    if parsed.scheme() != "https" {
        return Err(PushDomainError::InvalidField {
            field,
            reason: "must use https",
        });
    }
    Ok(())
}

fn require_secret(field: &'static str, value: &SecretString) -> Result<(), PushDomainError> {
    require_non_empty(field, value.expose_secret())
}

fn require_encrypted(field: &'static str, value: &EncryptedSecret) -> Result<(), PushDomainError> {
    require_non_empty(field, value.ciphertext())
}

fn require_json_bound(
    field: &'static str,
    value: &Value,
    max_bytes: usize,
) -> Result<(), PushDomainError> {
    let len = sonic_rs::to_vec(value)
        .map_err(|_| PushDomainError::InvalidField {
            field,
            reason: "must be serializable JSON",
        })?
        .len();
    if len > max_bytes {
        return Err(PushDomainError::TooLarge { field, max_bytes });
    }
    Ok(())
}

pub(crate) fn to_json_value<T: Serialize>(value: &T) -> Result<Value, sonic_rs::Error> {
    let bytes = sonic_rs::to_vec(value)?;
    sonic_rs::from_slice(&bytes)
}

pub(crate) fn from_json_value<T: DeserializeOwned>(value: &Value) -> Result<T, sonic_rs::Error> {
    let bytes = sonic_rs::to_vec(value)?;
    sonic_rs::from_slice(&bytes)
}

pub(crate) fn validate_web_push_endpoint(endpoint: &str) -> Result<(), PushDomainError> {
    let parsed = Url::parse(endpoint).map_err(|_| PushDomainError::InvalidField {
        field: "endpoint",
        reason: "must be a valid URL",
    })?;

    if parsed.scheme() != "https" {
        return Err(PushDomainError::InvalidField {
            field: "endpoint",
            reason: "must use https",
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(PushDomainError::DisallowedField {
            field: "endpoint",
            reason: "userinfo is not allowed",
        });
    }

    let Some(host) = parsed.host_str().map(|host| host.to_ascii_lowercase()) else {
        return Err(PushDomainError::InvalidField {
            field: "endpoint",
            reason: "must include a host",
        });
    };

    let local_tests_allowed = std::env::var("PUSH_ALLOW_LOCAL_WEB_PUSH_ENDPOINTS")
        .ok()
        .is_some_and(|raw| matches!(raw.as_str(), "1" | "true" | "TRUE" | "yes"));

    if (host == "localhost" || host.ends_with(".local")) && !local_tests_allowed {
        return Err(PushDomainError::DisallowedField {
            field: "endpoint",
            reason: "localhost endpoints are only allowed in local tests",
        });
    }

    if (parsed.host().is_some_and(host_variant_is_private_or_local)
        || host.parse::<IpAddr>().is_ok_and(is_private_or_local_ip))
        && !local_tests_allowed
    {
        return Err(PushDomainError::DisallowedField {
            field: "endpoint",
            reason: "private or local IP ranges are not allowed",
        });
    }

    if host_matches_list(&host, "PUSH_WEBPUSH_DENIED_HOSTS") {
        return Err(PushDomainError::DisallowedField {
            field: "endpoint",
            reason: "host is denied",
        });
    }

    let allow_list = std::env::var("PUSH_WEBPUSH_ALLOWED_HOSTS").ok();
    if allow_list
        .as_deref()
        .is_some_and(|raw| !raw.trim().is_empty())
        && !host_matches_list(&host, "PUSH_WEBPUSH_ALLOWED_HOSTS")
    {
        return Err(PushDomainError::DisallowedField {
            field: "endpoint",
            reason: "host is not allowed",
        });
    }

    Ok(())
}

fn host_variant_is_private_or_local(host: Host<&str>) -> bool {
    match host {
        Host::Domain(_) => false,
        Host::Ipv4(ip) => is_private_or_local_ip(IpAddr::V4(ip)),
        Host::Ipv6(ip) => is_private_or_local_ip(IpAddr::V6(ip)),
    }
}

fn host_matches_list(host: &str, env_name: &str) -> bool {
    std::env::var(env_name).ok().is_some_and(|raw| {
        raw.split(',').any(|entry| {
            let entry = entry.trim().to_ascii_lowercase();
            !entry.is_empty()
                && (host == entry
                    || entry
                        .strip_prefix('.')
                        .is_some_and(|suffix| host.ends_with(suffix)))
        })
    })
}

fn is_private_or_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.octets()[0] == 0
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_private_or_local_ip(IpAddr::V4(mapped));
            }
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast()
        }
    }
}

fn canonicalize_intent_json(value: &mut Value) {
    if let Some(map) = value.as_object_mut()
        && let Some(overrides) = map
            .get_mut(&"providerOverrides")
            .and_then(Value::as_array_mut)
    {
        overrides.sort_by(|left, right| {
            left.get("provider")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .cmp(
                    right
                        .get("provider")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                )
        });
    }
}

fn write_canonical_json(value: &Value, out: &mut Vec<u8>) -> Result<(), sonic_rs::Error> {
    if let Some(items) = value.as_array() {
        out.push(b'[');
        for (index, item) in items.iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            write_canonical_json(item, out)?;
        }
        out.push(b']');
        return Ok(());
    }

    if let Some(map) = value.as_object() {
        out.push(b'{');
        let mut entries = map.iter().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(right.0));
        for (index, (key, value)) in entries.into_iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            out.extend_from_slice(sonic_rs::to_string(key)?.as_bytes());
            out.push(b':');
            write_canonical_json(value, out)?;
        }
        out.push(b'}');
        return Ok(());
    }

    out.extend_from_slice(sonic_rs::to_string(value)?.as_bytes());
    Ok(())
}

fn redact_error_reason(value: String) -> String {
    let lower = value.to_ascii_lowercase();
    let looks_like_token = value.len() >= 24
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'='));
    if value.len() > 128
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("endpoint")
        || lower.contains("auth")
        || lower.contains("http://")
        || lower.contains("https://")
        || looks_like_token
    {
        format!("[REDACTED:{}]", &stable_hash(value.as_bytes())[..12])
    } else {
        value
    }
}

pub(crate) fn provider_key(provider: PushProviderKind) -> &'static str {
    match provider {
        PushProviderKind::Fcm => "fcm",
        PushProviderKind::Apns => "apns",
        PushProviderKind::WebPush => "webpush",
        PushProviderKind::Hms => "hms",
        PushProviderKind::Wns => "wns",
        PushProviderKind::Realtime => "realtime",
    }
}

pub fn stable_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    fn secret(value: &str) -> SecretString {
        SecretString::new(value).unwrap()
    }

    fn encrypted(value: &str) -> EncryptedSecret {
        EncryptedSecret::new(value).unwrap()
    }

    fn sample_device() -> DeviceDetails {
        let token = secret("device-identity-token");
        DeviceDetails {
            app_id: "app-1".to_owned(),
            id: "device-1".to_owned(),
            client_id: Some("client-1".to_owned()),
            form_factor: FormFactor::Phone,
            platform: Platform::Android,
            metadata: json!({"model": "Pixel"}),
            device_secret: hash_device_identity_token(&token),
            timezone: "Europe/Madrid".to_owned(),
            locale: "en-US".to_owned(),
            last_active_at_ms: 86_400_000,
            push: DevicePushDetails {
                recipient: PushRecipient::Fcm {
                    registration_token: secret("fcm-token-secret"),
                },
                state: DevicePushState::Active,
                failure_count: 0,
                error_reason: None,
            },
            push_rate_policy: Some(TokenBucketPolicy {
                capacity: 10,
                refill_per_second: 1,
            }),
        }
    }

    #[test]
    fn device_details_use_camel_case_serde_and_provider_recipient_shape() {
        let device = sample_device();
        let encoded = to_json_value(&device).unwrap();

        assert_eq!(encoded["appId"], "app-1");
        assert_eq!(encoded["clientId"], "client-1");
        assert_eq!(encoded["formFactor"], "phone");
        assert!(
            encoded["deviceSecret"]
                .as_str()
                .unwrap()
                .starts_with("pbkdf2-sha256$")
        );
        assert_eq!(encoded["push"]["state"], "ACTIVE");
        assert_eq!(encoded["push"]["recipient"]["transportType"], "gcm");
        assert_eq!(
            encoded["push"]["recipient"]["registrationToken"],
            "fcm-token-secret"
        );

        let decoded: DeviceDetails = from_json_value(&encoded).unwrap();
        assert_eq!(decoded, device);
    }

    #[test]
    fn debug_redacts_tokens_credentials_metadata_and_raw_payloads() {
        let device = sample_device();
        let debug = format!("{device:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("fcm-token-secret"));
        assert!(!debug.contains("device-identity-token"));
        assert!(!debug.contains("Pixel"));

        let push_debug = format!(
            "{:?}",
            DevicePushDetails {
                error_reason: Some("provider leaked token fcm-token-secret".to_owned()),
                ..device.push.clone()
            }
        );
        assert!(push_debug.contains("[REDACTED]"));
        assert!(!push_debug.contains("fcm-token-secret"));

        let credential = ProviderCredential {
            app_id: "app-1".to_owned(),
            credential_id: "cred-1".to_owned(),
            provider: PushProviderKind::Fcm,
            version: 1,
            material: ProviderCredentialMaterial::Fcm {
                service_account_json: encrypted("encrypted-service-account-json"),
            },
        };
        let debug = format!("{credential:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("encrypted-service-account-json"));

        let override_payload = ProviderOverridePayload {
            provider: PushProviderKind::Apns,
            payload: json!({"aps": {"alert": "raw-body-secret"}}),
        };
        let debug = format!("{override_payload:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("raw-body-secret"));

        let provider_error = ProviderError {
            class: "invalid-token".to_owned(),
            failure_class: ProviderFailureClass::DeviceTerminal,
            reason: Some("token fcm-token-secret rejected".to_owned()),
            retry_after_ms: None,
        };
        let debug = format!("{provider_error:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("fcm-token-secret"));
    }

    #[test]
    fn validation_rejects_unbounded_metadata_and_invalid_recipient_fields() {
        let mut device = sample_device();
        device.metadata = json!({"blob": "x".repeat(MAX_METADATA_BYTES + 1)});

        assert!(matches!(
            device.validate(),
            Err(PushDomainError::TooLarge {
                field: "metadata",
                ..
            })
        ));

        let web = PushRecipient::Web {
            endpoint: secret("http://example.invalid/push"),
            p256dh: secret("p256dh"),
            auth: secret("auth"),
        };
        assert_eq!(
            web.validate(),
            Err(PushDomainError::InvalidField {
                field: "endpoint",
                reason: "must use https",
            })
        );

        let policy = TokenBucketPolicy {
            capacity: 0,
            refill_per_second: 1,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn device_identity_tokens_are_hashed_and_verifiable() {
        let token = generate_device_identity_token();
        let hash = hash_device_identity_token(&token);

        assert!(is_hashed_device_secret(&hash));
        assert!(verify_device_identity_token(token.expose_secret(), &hash));
        assert!(!verify_device_identity_token("wrong-token", &hash));
        assert!(!hash.expose_secret().contains(token.expose_secret()));
    }

    #[test]
    fn pbkdf2_sha256_matches_known_test_vectors() {
        assert_eq!(
            hex::encode(pbkdf2_sha256(b"password", b"salt", 1)),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
        );
        assert_eq!(
            hex::encode(pbkdf2_sha256(b"password", b"salt", 2)),
            "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43"
        );
    }

    #[test]
    fn device_validation_rejects_raw_device_secrets() {
        let mut device = sample_device();
        device.device_secret = secret("raw-secret");

        assert_eq!(
            device.validate(),
            Err(PushDomainError::InvalidField {
                field: "deviceSecret",
                reason: "must be stored as a pbkdf2-sha256 hash"
            })
        );
    }

    #[test]
    fn web_push_endpoint_validation_blocks_ssrf_targets() {
        let recipient = PushRecipient::Web {
            endpoint: secret("https://127.0.0.1/push"),
            p256dh: secret("p256dh"),
            auth: secret("auth"),
        };
        assert!(matches!(
            recipient.validate(),
            Err(PushDomainError::DisallowedField {
                field: "endpoint",
                ..
            })
        ));

        let recipient = PushRecipient::Web {
            endpoint: secret("http://push.example.test/push"),
            p256dh: secret("p256dh"),
            auth: secret("auth"),
        };
        assert_eq!(
            recipient.validate(),
            Err(PushDomainError::InvalidField {
                field: "endpoint",
                reason: "must use https"
            })
        );

        for endpoint in [
            "https://169.254.169.254/latest/meta-data",
            "https://[::1]/push",
            "https://[::ffff:10.0.0.1]/push",
            "https://attacker.example@10.0.0.1/push",
        ] {
            let recipient = PushRecipient::Web {
                endpoint: secret(endpoint),
                p256dh: secret("p256dh"),
                auth: secret("auth"),
            };
            assert!(recipient.validate().is_err(), "{endpoint} must be rejected");
        }
    }

    #[test]
    fn cursor_encoding_is_opaque_app_scoped_and_round_trips() {
        let cursor = PushCursor {
            app_id: "app-1".to_owned(),
            kind: PushCursorKind::ChannelSubscription,
            position: "channel:room-1:device-9".to_owned(),
            issued_at_ms: 42,
        };

        let encoded = cursor.encode().unwrap();
        assert!(!encoded.contains("app-1"));
        assert_eq!(PushCursor::decode(&encoded, "app-1").unwrap(), cursor);
        assert!(matches!(
            PushCursor::decode(&encoded, "app-2"),
            Err(PushDomainError::CursorAppMismatch { .. })
        ));
        assert!(PushCursor::decode("not valid base64", "app-1").is_err());
    }

    #[test]
    fn idempotency_keys_are_stable_and_token_changes_update_registration_not_device_key() {
        let original = sample_device();
        let identical = sample_device();
        assert_eq!(original.device_key(), identical.device_key());
        assert_eq!(
            original.registration_fingerprint(),
            identical.registration_fingerprint()
        );

        let mut changed_token = sample_device();
        changed_token.push.recipient = PushRecipient::Fcm {
            registration_token: secret("new-fcm-token-secret"),
        };
        assert_eq!(original.device_key(), changed_token.device_key());
        assert_ne!(
            original.registration_fingerprint(),
            changed_token.registration_fingerprint()
        );

        let intent = PublishIntent {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            targets: vec![PublishTarget::Channel {
                channel: "private-room".to_owned(),
            }],
            payload: PushPayload {
                template_id: Some("welcome".to_owned()),
                template_data: json!({"name": "Ada"}),
                title: None,
                body: None,
                icon: None,
                sound: None,
                collapse_key: None,
            },
            provider_overrides: vec![],
            not_before_ms: None,
            expires_at_ms: Some(1000),
        };
        assert_eq!(intent.idempotency_key(), intent.clone().idempotency_key());
    }

    #[test]
    fn device_push_state_transitions_follow_failure_threshold_rules() {
        let mut device = sample_device();

        device.record_delivery_failure(3, "UNREGISTERED token fcm-token-secret");
        assert_eq!(device.push.state, DevicePushState::Failing);
        assert_eq!(device.push.failure_count, 1);
        assert!(
            !device
                .push
                .error_reason
                .as_ref()
                .unwrap()
                .contains("fcm-token-secret")
        );

        device.record_delivery_success();
        assert_eq!(device.push.state, DevicePushState::Active);
        assert_eq!(device.push.failure_count, 0);
        assert_eq!(device.push.error_reason, None);

        device.record_delivery_failure(2, "first failure");
        device.record_delivery_failure(2, "second failure");
        assert_eq!(device.push.state, DevicePushState::Failed);

        device.record_delivery_success();
        assert_eq!(device.push.state, DevicePushState::Failed);
    }

    #[test]
    fn template_fallback_uses_exact_locale_then_language_then_default() {
        let template = NotificationTemplate {
            app_id: "app-1".to_owned(),
            template_id: "welcome".to_owned(),
            default_locale: "en".to_owned(),
            locales: BTreeMap::from([
                (
                    "en".to_owned(),
                    TemplateContent {
                        title: "Hello".to_owned(),
                        body: "Default body".to_owned(),
                        icon: None,
                        sound: None,
                        collapse_key: None,
                    },
                ),
                (
                    "en-US".to_owned(),
                    TemplateContent {
                        title: "Howdy".to_owned(),
                        body: "US body".to_owned(),
                        icon: None,
                        sound: None,
                        collapse_key: Some("welcome".to_owned()),
                    },
                ),
                (
                    "fr".to_owned(),
                    TemplateContent {
                        title: "Bonjour".to_owned(),
                        body: "FR body".to_owned(),
                        icon: None,
                        sound: None,
                        collapse_key: None,
                    },
                ),
            ]),
            provider_overrides: BTreeMap::new(),
        };

        assert_eq!(
            template.resolve_locale(Some("en-US")).unwrap().title,
            "Howdy"
        );
        assert_eq!(
            template.resolve_locale(Some("fr-CA")).unwrap().title,
            "Bonjour"
        );
        assert_eq!(
            template.resolve_locale(Some("es-ES")).unwrap().title,
            "Hello"
        );
        assert!(template.validate().is_ok());
    }

    #[test]
    fn channel_subscriptions_are_separate_app_scoped_push_rows() {
        let device = sample_device();
        let subscription = ChannelSubscription::from_device("private-room", &device);

        assert_eq!(subscription.app_id, device.app_id);
        assert_eq!(subscription.channel, "private-room");
        assert_eq!(subscription.device_id, device.id);
        assert_eq!(subscription.provider, PushProviderKind::Fcm);
        assert_eq!(subscription.token_hash, device.push.recipient.token_hash());
        assert!(subscription.validate().is_ok());
    }

    #[test]
    fn realtime_recipient_is_a_native_validated_push_target() {
        let recipient = PushRecipient::Realtime {
            channel: "private:device-inbox".to_owned(),
        };

        assert_eq!(recipient.provider(), PushProviderKind::Realtime);
        assert_eq!(recipient.token_hash(), stable_hash(b"private:device-inbox"));
        assert!(recipient.validate().is_ok());
        assert!(
            PushRecipient::Realtime {
                channel: String::new(),
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn deleting_missing_device_is_successful() {
        assert!(DeleteDeviceOutcome::Deleted.is_success());
        assert!(DeleteDeviceOutcome::NotFound.is_success());
    }
}
