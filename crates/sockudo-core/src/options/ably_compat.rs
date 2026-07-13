use serde::{Deserialize, Serialize};
use std::fmt;

/// Optional Ably compatibility credential registry.
///
/// The primary `App.key`/`App.secret` credential remains available even when
/// this registry is disabled. Extra keys are considered only when `enabled`
/// is true.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AblyCompatConfig {
    pub enabled: bool,
    pub keys: Vec<AblyCompatKeyConfig>,
    pub max_token_ttl_ms: i64,
    pub token_request_timestamp_skew_ms: i64,
    pub nonce_ttl_seconds: u64,
}

impl Default for AblyCompatConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            keys: Vec::new(),
            max_token_ttl_ms: 24 * 60 * 60 * 1000,
            token_request_timestamp_skew_ms: 15 * 60 * 1000,
            nonce_ttl_seconds: 15 * 60,
        }
    }
}

impl AblyCompatConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_token_ttl_ms <= 0 {
            return Err("ably_compat.max_token_ttl_ms must be greater than 0".to_string());
        }
        if self.token_request_timestamp_skew_ms <= 0 {
            return Err(
                "ably_compat.token_request_timestamp_skew_ms must be greater than 0".to_string(),
            );
        }
        if self.nonce_ttl_seconds == 0 {
            return Err("ably_compat.nonce_ttl_seconds must be greater than 0".to_string());
        }

        let mut names = std::collections::HashSet::with_capacity(self.keys.len());
        for key in &self.keys {
            if key.app_id.is_empty() || key.key_name.is_empty() || key.secret.is_empty() {
                return Err(
                    "ably_compat keys require non-empty app_id, key_name, and secret".to_string(),
                );
            }
            if !names.insert(key.key_name.as_str()) {
                return Err(format!(
                    "ably_compat key_name '{}' is configured more than once",
                    key.key_name
                ));
            }
            if let Some(capability) = &key.capability {
                let parsed: serde_json::Value =
                    serde_json::from_str(capability).map_err(|error| {
                        format!(
                            "ably_compat key '{}' has invalid capability JSON: {error}",
                            key.key_name
                        )
                    })?;
                if !parsed.is_object() {
                    return Err(format!(
                        "ably_compat key '{}' capability must be a JSON object",
                        key.key_name
                    ));
                }
            }
        }
        Ok(())
    }
}

/// One additional key that resolves to an existing Sockudo application.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AblyCompatKeyConfig {
    pub app_id: String,
    pub key_name: String,
    pub secret: String,
    pub capability: Option<String>,
    pub revocable_tokens: bool,
    pub enabled: bool,
    pub rotation_id: Option<String>,
    pub created_at_ms: Option<i64>,
    pub expires_at_ms: Option<i64>,
    pub revoked_at_ms: Option<i64>,
}

impl Default for AblyCompatKeyConfig {
    fn default() -> Self {
        Self {
            app_id: String::new(),
            key_name: String::new(),
            secret: String::new(),
            capability: None,
            revocable_tokens: false,
            enabled: true,
            rotation_id: None,
            created_at_ms: None,
            expires_at_ms: None,
            revoked_at_ms: None,
        }
    }
}

impl fmt::Debug for AblyCompatKeyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AblyCompatKeyConfig")
            .field("app_id", &self.app_id)
            .field("key_name", &self.key_name)
            .field("secret", &"[REDACTED]")
            .field("capability", &self.capability)
            .field("revocable_tokens", &self.revocable_tokens)
            .field("enabled", &self.enabled)
            .field("rotation_id", &self.rotation_id)
            .field("created_at_ms", &self.created_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("revoked_at_ms", &self.revoked_at_ms)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_defaults_off_and_redacts_secrets() {
        let config = AblyCompatConfig::default();
        assert!(!config.enabled);
        let key = AblyCompatKeyConfig {
            app_id: "app".to_string(),
            key_name: "key".to_string(),
            secret: "never-print-this".to_string(),
            ..Default::default()
        };
        let debug = format!("{key:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("never-print-this"));
    }

    #[test]
    fn registry_rejects_duplicate_keys_and_invalid_capability() {
        let key = AblyCompatKeyConfig {
            app_id: "app".to_string(),
            key_name: "key".to_string(),
            secret: "secret".to_string(),
            ..Default::default()
        };
        let duplicate = AblyCompatConfig {
            enabled: true,
            keys: vec![key.clone(), key.clone()],
            ..Default::default()
        };
        assert!(duplicate.validate().unwrap_err().contains("more than once"));

        let invalid = AblyCompatConfig {
            enabled: true,
            keys: vec![AblyCompatKeyConfig {
                capability: Some("[]".to_string()),
                ..key
            }],
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().contains("JSON object"));
    }
}
