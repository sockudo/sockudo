//! App credential resolution.
//!
//! The in-process server resolves credentials from its `AppManager`; the
//! standalone binary uses a static table supplied on the command line.

use std::fmt;

use async_trait::async_trait;
use serde::Serialize;

use super::ApiError;

/// Signing credentials for one Sockudo app.
#[derive(Clone, PartialEq, Eq)]
pub struct AppCredentials {
    /// App id (path segment in `/apps/{app_id}/...`).
    pub app_id: String,
    /// App key (`auth_key`).
    pub key: String,
    /// App secret used for HMAC signing. Never logged.
    pub secret: String,
}

impl AppCredentials {
    /// Construct credentials.
    pub fn new(
        app_id: impl Into<String>,
        key: impl Into<String>,
        secret: impl Into<String>,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            key: key.into(),
            secret: secret.into(),
        }
    }

    /// Parse the `id:key:secret` spec accepted by the standalone binary.
    pub fn parse_spec(spec: &str) -> Result<Self, ApiError> {
        let mut parts = spec.splitn(3, ':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(id), Some(key), Some(secret))
                if !id.is_empty() && !key.is_empty() && !secret.is_empty() =>
            {
                Ok(Self::new(id.trim(), key.trim(), secret.trim()))
            }
            _ => Err(ApiError::InvalidRequest(
                "app credentials must use the form app_id:app_key:app_secret".to_string(),
            )),
        }
    }
}

impl fmt::Debug for AppCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppCredentials")
            .field("app_id", &self.app_id)
            .field("key", &self.key)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Non-secret description of an app exposed through MCP.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AppSummary {
    /// App id.
    pub id: String,
    /// Public app key.
    pub key: String,
    /// Whether the app accepts traffic.
    pub enabled: bool,
    /// Sanitized policy (limits, features, channel policy) when the source
    /// knows it. Secrets such as webhook headers are stripped by the source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<serde_json::Value>,
}

/// Source of app credentials and app metadata.
#[async_trait]
pub trait CredentialSource: Send + Sync + 'static {
    /// Resolve signing credentials for an app id.
    async fn resolve(&self, app_id: &str) -> Result<Option<AppCredentials>, ApiError>;

    /// Every app the source knows about, without secrets.
    async fn list_apps(&self) -> Result<Vec<AppSummary>, ApiError>;

    /// Detailed non-secret view of one app.
    async fn describe_app(&self, app_id: &str) -> Result<Option<AppSummary>, ApiError> {
        Ok(self
            .list_apps()
            .await?
            .into_iter()
            .find(|app| app.id == app_id))
    }
}

/// Fixed credential table.
#[derive(Debug, Clone, Default)]
pub struct StaticCredentials {
    apps: Vec<AppCredentials>,
}

impl StaticCredentials {
    /// Build from explicit credentials.
    pub fn new(apps: Vec<AppCredentials>) -> Self {
        Self { apps }
    }

    /// Number of configured apps.
    pub fn len(&self) -> usize {
        self.apps.len()
    }

    /// Whether no apps are configured.
    pub fn is_empty(&self) -> bool {
        self.apps.is_empty()
    }
}

#[async_trait]
impl CredentialSource for StaticCredentials {
    async fn resolve(&self, app_id: &str) -> Result<Option<AppCredentials>, ApiError> {
        Ok(self.apps.iter().find(|app| app.app_id == app_id).cloned())
    }

    async fn list_apps(&self) -> Result<Vec<AppSummary>, ApiError> {
        Ok(self
            .apps
            .iter()
            .map(|app| AppSummary {
                id: app.app_id.clone(),
                key: app.key.clone(),
                enabled: true,
                policy: None,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_parsing() {
        let creds = AppCredentials::parse_spec("app-1:key:se:cret").unwrap();
        assert_eq!(creds.app_id, "app-1");
        assert_eq!(creds.key, "key");
        assert_eq!(creds.secret, "se:cret");
        assert!(AppCredentials::parse_spec("app-1:key").is_err());
        assert!(AppCredentials::parse_spec("::").is_err());
    }

    #[test]
    fn debug_redacts_secret() {
        let text = format!("{:?}", AppCredentials::new("a", "k", "topsecret"));
        assert!(!text.contains("topsecret"));
    }
}
