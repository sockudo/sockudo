use crate::options::{ConnectionRecoveryConfig, IdempotencyConfig};
use crate::webhook_types::Webhook;
use ahash::AHashMap;
use async_trait::async_trait;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_aux::field_attributes::{
    deserialize_number_from_string, deserialize_option_number_from_string,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppPolicy {
    pub limits: AppLimitsPolicy,
    pub features: AppFeaturesPolicy,
    pub channels: AppChannelsPolicy,
    pub webhooks: Option<Vec<Webhook>>,
    pub idempotency: Option<AppIdempotencyConfig>,
    pub connection_recovery: Option<AppConnectionRecoveryConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppLimitsPolicy {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub max_connections: u32,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_backend_events_per_second: Option<u32>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub max_client_events_per_second: u32,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_read_requests_per_second: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_presence_members_per_channel: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_presence_member_size_in_kb: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_channel_name_length: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_channels_at_once: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_name_length: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_payload_in_kb: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_batch_size: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppFeaturesPolicy {
    pub enable_client_messages: bool,
    pub enable_user_authentication: Option<bool>,
    pub enable_watchlist_events: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppChannelsPolicy {
    #[serde(default, deserialize_with = "deserialize_and_validate_origins")]
    pub allowed_origins: Option<Vec<String>>,
    pub channel_delta_compression: Option<AHashMap<String, crate::delta_types::ChannelDeltaConfig>>,
    pub channel_namespaces: Option<Vec<ChannelNamespace>>,
}

#[derive(Debug, Clone, Copy)]
pub struct AppPolicyRef<'a> {
    pub limits: AppLimitsPolicyRef,
    pub features: AppFeaturesPolicyRef<'a>,
    pub channels: AppChannelsPolicyRef<'a>,
    pub webhooks: Option<&'a [Webhook]>,
    pub idempotency: Option<&'a AppIdempotencyConfig>,
    pub connection_recovery: Option<&'a AppConnectionRecoveryConfig>,
}

#[derive(Debug, Clone, Copy)]
pub struct AppLimitsPolicyRef {
    pub max_connections: u32,
    pub max_backend_events_per_second: Option<u32>,
    pub max_client_events_per_second: u32,
    pub max_read_requests_per_second: Option<u32>,
    pub max_presence_members_per_channel: Option<u32>,
    pub max_presence_member_size_in_kb: Option<u32>,
    pub max_channel_name_length: Option<u32>,
    pub max_event_channels_at_once: Option<u32>,
    pub max_event_name_length: Option<u32>,
    pub max_event_payload_in_kb: Option<u32>,
    pub max_event_batch_size: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
pub struct AppFeaturesPolicyRef<'a> {
    pub enable_client_messages: bool,
    pub enable_user_authentication: Option<bool>,
    pub enable_watchlist_events: Option<bool>,
    _marker: std::marker::PhantomData<&'a ()>,
}

#[derive(Debug, Clone, Copy)]
pub struct AppChannelsPolicyRef<'a> {
    pub allowed_origins: Option<&'a [String]>,
    pub channel_delta_compression:
        Option<&'a AHashMap<String, crate::delta_types::ChannelDeltaConfig>>,
    pub channel_namespaces: Option<&'a [ChannelNamespace]>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct App {
    pub id: String,
    pub key: String,
    pub secret: String,
    pub enabled: bool,
    #[serde(default)]
    pub policy: AppPolicy,
}

impl App {
    pub fn from_policy(
        id: String,
        key: String,
        secret: String,
        enabled: bool,
        policy: AppPolicy,
    ) -> Self {
        Self {
            id,
            key,
            secret,
            enabled,
            policy,
        }
    }

    pub fn policy(&self) -> AppPolicy {
        self.policy.clone()
    }

    #[inline]
    pub fn policy_ref(&self) -> AppPolicyRef<'_> {
        AppPolicyRef {
            limits: AppLimitsPolicyRef {
                max_connections: self.policy.limits.max_connections,
                max_backend_events_per_second: self.policy.limits.max_backend_events_per_second,
                max_client_events_per_second: self.policy.limits.max_client_events_per_second,
                max_read_requests_per_second: self.policy.limits.max_read_requests_per_second,
                max_presence_members_per_channel: self
                    .policy
                    .limits
                    .max_presence_members_per_channel,
                max_presence_member_size_in_kb: self.policy.limits.max_presence_member_size_in_kb,
                max_channel_name_length: self.policy.limits.max_channel_name_length,
                max_event_channels_at_once: self.policy.limits.max_event_channels_at_once,
                max_event_name_length: self.policy.limits.max_event_name_length,
                max_event_payload_in_kb: self.policy.limits.max_event_payload_in_kb,
                max_event_batch_size: self.policy.limits.max_event_batch_size,
            },
            features: AppFeaturesPolicyRef {
                enable_client_messages: self.policy.features.enable_client_messages,
                enable_user_authentication: self.policy.features.enable_user_authentication,
                enable_watchlist_events: self.policy.features.enable_watchlist_events,
                _marker: std::marker::PhantomData,
            },
            channels: AppChannelsPolicyRef {
                allowed_origins: self.policy.channels.allowed_origins.as_deref(),
                channel_delta_compression: self.policy.channels.channel_delta_compression.as_ref(),
                channel_namespaces: self.policy.channels.channel_namespaces.as_deref(),
            },
            webhooks: self.policy.webhooks.as_deref(),
            idempotency: self.policy.idempotency.as_ref(),
            connection_recovery: self.policy.connection_recovery.as_ref(),
        }
    }

    #[inline]
    pub fn limits(&self) -> AppLimitsPolicy {
        self.policy.limits.clone()
    }

    #[inline]
    pub fn features(&self) -> AppFeaturesPolicy {
        self.policy.features.clone()
    }

    #[inline]
    pub fn channels(&self) -> AppChannelsPolicy {
        self.policy.channels.clone()
    }

    #[inline]
    pub fn policy_mut(&mut self) -> &mut AppPolicy {
        &mut self.policy
    }

    #[inline]
    pub fn watchlist_events_enabled(&self) -> bool {
        self.policy
            .features
            .enable_watchlist_events
            .unwrap_or(false)
    }

    #[inline]
    pub fn client_messages_enabled(&self) -> bool {
        self.policy.features.enable_client_messages
    }

    #[inline]
    pub fn max_connections_limit(&self) -> u32 {
        self.policy.limits.max_connections
    }

    #[inline]
    pub fn user_authentication_enabled(&self) -> bool {
        self.policy
            .features
            .enable_user_authentication
            .unwrap_or(false)
    }

    #[inline]
    pub fn max_channel_name_limit(&self) -> Option<u32> {
        self.policy.limits.max_channel_name_length
    }

    #[inline]
    pub fn max_presence_members_limit(&self) -> Option<u32> {
        self.policy.limits.max_presence_members_per_channel
    }

    #[inline]
    pub fn max_presence_member_size_limit_kb(&self) -> Option<u32> {
        self.policy.limits.max_presence_member_size_in_kb
    }

    #[inline]
    pub fn client_events_per_second_limit(&self) -> u32 {
        self.policy.limits.max_client_events_per_second
    }

    #[inline]
    pub fn namespaces(&self) -> Option<&[ChannelNamespace]> {
        self.policy.channels.channel_namespaces.as_deref()
    }

    #[inline]
    pub fn event_name_limit(&self) -> Option<u32> {
        self.policy.limits.max_event_name_length
    }

    #[inline]
    pub fn event_payload_limit_kb(&self) -> Option<u32> {
        self.policy.limits.max_event_payload_in_kb
    }

    #[inline]
    pub fn event_channels_at_once_limit(&self) -> Option<u32> {
        self.policy.limits.max_event_channels_at_once
    }

    #[inline]
    pub fn event_batch_size_limit(&self) -> Option<u32> {
        self.policy.limits.max_event_batch_size
    }

    #[inline]
    pub fn allowed_origins_ref(&self) -> Option<&[String]> {
        self.policy.channels.allowed_origins.as_deref()
    }

    #[inline]
    pub fn channel_delta_compression_ref(
        &self,
    ) -> Option<&AHashMap<String, crate::delta_types::ChannelDeltaConfig>> {
        self.policy.channels.channel_delta_compression.as_ref()
    }

    #[inline]
    pub fn webhooks_ref(&self) -> Option<&[Webhook]> {
        self.policy.webhooks.as_deref()
    }

    #[inline]
    pub fn idempotency_override(&self) -> Option<&AppIdempotencyConfig> {
        self.policy.idempotency.as_ref()
    }

    #[inline]
    pub fn connection_recovery_override(&self) -> Option<&AppConnectionRecoveryConfig> {
        self.policy.connection_recovery.as_ref()
    }

    #[inline]
    pub fn resolved_idempotency(&self, global: &IdempotencyConfig) -> IdempotencyConfig {
        match self.idempotency_override() {
            Some(app_config) => IdempotencyConfig {
                enabled: app_config.enabled.unwrap_or(global.enabled),
                ttl_seconds: app_config.ttl_seconds.unwrap_or(global.ttl_seconds),
                max_key_length: global.max_key_length,
            },
            None => global.clone(),
        }
    }

    #[inline]
    pub fn resolved_connection_recovery(
        &self,
        global: &ConnectionRecoveryConfig,
    ) -> ConnectionRecoveryConfig {
        match self.connection_recovery_override() {
            Some(app_config) => ConnectionRecoveryConfig {
                enabled: app_config.enabled.unwrap_or(global.enabled),
                buffer_ttl_seconds: app_config
                    .buffer_ttl_seconds
                    .unwrap_or(global.buffer_ttl_seconds),
                max_buffer_size: app_config.max_buffer_size.unwrap_or(global.max_buffer_size),
            },
            None => global.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct AppSerde {
    id: String,
    key: String,
    secret: String,
    enabled: bool,
    #[serde(default)]
    policy: Option<AppPolicy>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    max_connections: u32,
    enable_client_messages: bool,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_backend_events_per_second: Option<u32>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    max_client_events_per_second: u32,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_read_requests_per_second: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_presence_members_per_channel: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_presence_member_size_in_kb: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_channel_name_length: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_event_channels_at_once: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_event_name_length: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_event_payload_in_kb: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    max_event_batch_size: Option<u32>,
    #[serde(default)]
    enable_user_authentication: Option<bool>,
    #[serde(default)]
    webhooks: Option<Vec<Webhook>>,
    #[serde(default)]
    enable_watchlist_events: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_and_validate_origins")]
    allowed_origins: Option<Vec<String>>,
    #[serde(default)]
    channel_delta_compression: Option<AHashMap<String, crate::delta_types::ChannelDeltaConfig>>,
    #[serde(default)]
    idempotency: Option<AppIdempotencyConfig>,
    #[serde(default)]
    connection_recovery: Option<AppConnectionRecoveryConfig>,
    #[serde(default)]
    channel_namespaces: Option<Vec<ChannelNamespace>>,
}

impl<'de> Deserialize<'de> for App {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let app = AppSerde::deserialize(deserializer)?;

        if let Some(policy) = app.policy {
            return Ok(Self {
                id: app.id,
                key: app.key,
                secret: app.secret,
                enabled: app.enabled,
                policy,
            });
        }

        Ok(Self::from_policy(
            app.id,
            app.key,
            app.secret,
            app.enabled,
            AppPolicy {
                limits: AppLimitsPolicy {
                    max_connections: app.max_connections,
                    max_backend_events_per_second: app.max_backend_events_per_second,
                    max_client_events_per_second: app.max_client_events_per_second,
                    max_read_requests_per_second: app.max_read_requests_per_second,
                    max_presence_members_per_channel: app.max_presence_members_per_channel,
                    max_presence_member_size_in_kb: app.max_presence_member_size_in_kb,
                    max_channel_name_length: app.max_channel_name_length,
                    max_event_channels_at_once: app.max_event_channels_at_once,
                    max_event_name_length: app.max_event_name_length,
                    max_event_payload_in_kb: app.max_event_payload_in_kb,
                    max_event_batch_size: app.max_event_batch_size,
                },
                features: AppFeaturesPolicy {
                    enable_client_messages: app.enable_client_messages,
                    enable_user_authentication: app.enable_user_authentication,
                    enable_watchlist_events: app.enable_watchlist_events,
                },
                channels: AppChannelsPolicy {
                    allowed_origins: app.allowed_origins,
                    channel_delta_compression: app.channel_delta_compression,
                    channel_namespaces: app.channel_namespaces,
                },
                webhooks: app.webhooks,
                idempotency: app.idempotency,
                connection_recovery: app.connection_recovery,
            },
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ChannelNamespace {
    pub name: String,
    pub channel_name_pattern: Option<String>,
    pub max_channel_name_length: Option<u32>,
    #[serde(default)]
    pub allow_user_limited_channels: Option<bool>,
    #[serde(default)]
    pub allow_subscribe_for_client: Option<bool>,
    #[serde(default)]
    pub allow_publish_for_client: Option<bool>,
    #[serde(default)]
    pub allow_presence_for_client: Option<bool>,
}

impl ChannelNamespace {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("channel namespace name must not be empty".to_string());
        }

        if self.name.contains(':') {
            return Err(format!(
                "channel namespace '{}' must not contain ':'",
                self.name
            ));
        }

        if let Some(pattern) = &self.channel_name_pattern {
            Regex::new(pattern).map_err(|e| {
                format!(
                    "invalid channel_name_pattern for namespace '{}': {e}",
                    self.name
                )
            })?;
        }

        if let Some(max_len) = self.max_channel_name_length
            && max_len == 0
        {
            return Err(format!(
                "max_channel_name_length for namespace '{}' must be greater than 0",
                self.name
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppIdempotencyConfig {
    pub enabled: Option<bool>,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppConnectionRecoveryConfig {
    pub enabled: Option<bool>,
    pub buffer_ttl_seconds: Option<u64>,
    pub max_buffer_size: Option<usize>,
}

fn deserialize_and_validate_origins<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let value = Option::<Vec<String>>::deserialize(deserializer)?;

    if let Some(ref origins) = value
        && let Err(validation_error) =
            crate::origin_validation::OriginValidator::validate_patterns(origins)
    {
        return Err(D::Error::custom(format!(
            "Origin pattern validation failed: {}",
            validation_error
        )));
    }

    Ok(value)
}

#[async_trait]
pub trait AppManager: Send + Sync + 'static {
    async fn init(&self) -> crate::error::Result<()>;
    async fn create_app(&self, config: App) -> crate::error::Result<()>;
    async fn update_app(&self, config: App) -> crate::error::Result<()>;
    async fn delete_app(&self, app_id: &str) -> crate::error::Result<()>;
    async fn get_apps(&self) -> crate::error::Result<Vec<App>>;
    async fn find_by_key(&self, key: &str) -> crate::error::Result<Option<App>>;
    async fn find_by_id(&self, app_id: &str) -> crate::error::Result<Option<App>>;
    async fn check_health(&self) -> crate::error::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_app_json(limits_overrides: &str, policy_suffix: &str) -> String {
        format!(
            r#"{{"id":"test","key":"key","secret":"secret","enabled":true,"policy":{{"limits":{{"max_connections":100,"max_client_events_per_second":100{limits_overrides}}},"features":{{"enable_client_messages":false}}{policy_suffix}}}}}"#
        )
    }

    #[test]
    fn deserialize_optional_numbers_from_integers() {
        let json = test_app_json(
            r#","max_presence_members_per_channel":100,"max_event_payload_in_kb":64"#,
            "",
        );
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(
            app.policy.limits.max_presence_members_per_channel,
            Some(100)
        );
        assert_eq!(app.policy.limits.max_event_payload_in_kb, Some(64));
    }

    #[test]
    fn deserialize_optional_numbers_from_strings() {
        let json = test_app_json(
            r#","max_presence_members_per_channel":"100","max_event_payload_in_kb":"64""#,
            "",
        );
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(
            app.policy.limits.max_presence_members_per_channel,
            Some(100)
        );
        assert_eq!(app.policy.limits.max_event_payload_in_kb, Some(64));
    }

    #[test]
    fn deserialize_optional_numbers_from_null() {
        let json = test_app_json(r#","max_presence_members_per_channel":null"#, "");
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(app.policy.limits.max_presence_members_per_channel, None);
    }

    #[test]
    fn deserialize_optional_numbers_missing_fields() {
        let json = test_app_json("", "");
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(app.policy.limits.max_presence_members_per_channel, None);
        assert_eq!(app.policy.limits.max_event_payload_in_kb, None);
    }

    #[test]
    fn cache_round_trip_preserves_optional_numbers() {
        let json = test_app_json(
            r#","max_presence_members_per_channel":100,"max_backend_events_per_second":50,"max_channel_name_length":200"#,
            "",
        );
        let app: App = sonic_rs::from_str(&json).unwrap();

        let cached = sonic_rs::to_string(&app).unwrap();
        let restored: App = sonic_rs::from_str(&cached).unwrap();

        assert_eq!(
            restored.policy.limits.max_presence_members_per_channel,
            Some(100)
        );
        assert_eq!(
            restored.policy.limits.max_backend_events_per_second,
            Some(50)
        );
        assert_eq!(restored.policy.limits.max_channel_name_length, Some(200));
    }

    #[test]
    fn deserialize_channel_namespaces() {
        let json = test_app_json(
            "",
            r#","channels":{"channel_namespaces":[{"name":"chat","channel_name_pattern":"^chat:[a-z0-9-]+$","max_channel_name_length":64,"allow_user_limited_channels":true,"allow_subscribe_for_client":true,"allow_publish_for_client":false,"allow_presence_for_client":true}]}"#,
        );
        let app: App = sonic_rs::from_str(&json).unwrap();
        let namespaces = app.policy.channels.channel_namespaces.unwrap();
        assert_eq!(namespaces.len(), 1);
        assert_eq!(namespaces[0].name, "chat");
        assert_eq!(namespaces[0].max_channel_name_length, Some(64));
        assert_eq!(namespaces[0].allow_user_limited_channels, Some(true));
        assert_eq!(namespaces[0].allow_subscribe_for_client, Some(true));
        assert_eq!(namespaces[0].allow_publish_for_client, Some(false));
        assert_eq!(namespaces[0].allow_presence_for_client, Some(true));
    }

    #[test]
    fn deserialize_legacy_flat_shape_for_compatibility() {
        let json = r#"{"id":"test","key":"key","secret":"secret","enabled":true,"max_connections":"100","enable_client_messages":false,"max_client_events_per_second":"100","allowed_origins":["https://example.com"]}"#;
        let app: App = sonic_rs::from_str(json).unwrap();
        assert_eq!(app.policy.limits.max_connections, 100);
        assert!(!app.policy.features.enable_client_messages);
        assert_eq!(
            app.policy.channels.allowed_origins,
            Some(vec!["https://example.com".to_string()])
        );
    }

    #[test]
    fn validate_channel_namespace_rejects_invalid_regex() {
        let ns = ChannelNamespace {
            name: "chat".to_string(),
            channel_name_pattern: Some("[".to_string()),
            max_channel_name_length: None,
            allow_user_limited_channels: None,
            allow_subscribe_for_client: None,
            allow_publish_for_client: None,
            allow_presence_for_client: None,
        };
        assert!(ns.validate().is_err());
    }

    #[test]
    fn app_policy_round_trip_preserves_grouped_configuration() {
        let policy = AppPolicy {
            limits: AppLimitsPolicy {
                max_connections: 250,
                max_backend_events_per_second: Some(50),
                max_client_events_per_second: 75,
                max_read_requests_per_second: Some(100),
                max_presence_members_per_channel: Some(25),
                max_presence_member_size_in_kb: Some(4),
                max_channel_name_length: Some(120),
                max_event_channels_at_once: Some(20),
                max_event_name_length: Some(80),
                max_event_payload_in_kb: Some(64),
                max_event_batch_size: Some(10),
            },
            features: AppFeaturesPolicy {
                enable_client_messages: true,
                enable_user_authentication: Some(true),
                enable_watchlist_events: Some(true),
            },
            channels: AppChannelsPolicy {
                allowed_origins: Some(vec!["https://example.com".to_string()]),
                channel_delta_compression: None,
                channel_namespaces: Some(vec![ChannelNamespace {
                    name: "chat".to_string(),
                    channel_name_pattern: Some("^chat:[a-z0-9-]+$".to_string()),
                    max_channel_name_length: Some(64),
                    allow_user_limited_channels: Some(true),
                    allow_subscribe_for_client: Some(true),
                    allow_publish_for_client: Some(true),
                    allow_presence_for_client: Some(true),
                }]),
            },
            webhooks: None,
            idempotency: Some(AppIdempotencyConfig {
                enabled: Some(true),
                ttl_seconds: Some(300),
            }),
            connection_recovery: Some(AppConnectionRecoveryConfig {
                enabled: Some(true),
                buffer_ttl_seconds: Some(120),
                max_buffer_size: Some(50),
            }),
        };

        let app = App::from_policy(
            "test".to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            policy.clone(),
        );

        assert_eq!(app.policy().limits.max_connections, 250);
        assert!(app.policy().features.enable_client_messages);
        assert_eq!(
            app.policy().channels.channel_namespaces.unwrap()[0].name,
            "chat"
        );
        assert_eq!(app.policy().idempotency.unwrap().ttl_seconds, Some(300));
        assert_eq!(
            app.policy().connection_recovery.unwrap().max_buffer_size,
            Some(50)
        );
    }
}
