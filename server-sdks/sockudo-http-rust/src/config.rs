use crate::{Result, SockudoError, Token};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use std::time::Duration;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Configuration for the Sockudo client
#[derive(Clone, Debug)]
pub struct Config {
    scheme: String,
    host: String,
    port: Option<u16>,
    app_id: String,
    token: Token,
    timeout: Duration,
    encryption_master_key: Option<EncryptionKey>,
    pool_max_idle_per_host: usize,
    enable_retry: bool,
    max_retries: u32,
    auto_idempotency_key: bool,
}

/// Wrapper for encryption key that ensures it's zeroed on drop
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
struct EncryptionKey(Vec<u8>);

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptionKey([REDACTED])")
    }
}

impl Config {
    /// Creates a new configuration builder
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Creates a new configuration (for backward compatibility)
    pub fn new(
        app_id: impl Into<String>,
        key: impl Into<String>,
        secret: impl Into<String>,
    ) -> Self {
        ConfigBuilder::default()
            .app_id(app_id)
            .key(key)
            .secret(secret)
            .build()
            .expect("Basic config should always be valid")
    }

    /// Validates the configuration
    pub fn validate(&self) -> Result<()> {
        if self.app_id.is_empty() {
            return Err(SockudoError::Config {
                message: "App ID cannot be empty".to_string(),
            });
        }

        if self.token.key.is_empty() {
            return Err(SockudoError::Config {
                message: "App key cannot be empty".to_string(),
            });
        }

        if let Some(ref key) = self.encryption_master_key
            && key.0.len() != 32
        {
            return Err(SockudoError::Config {
                message: format!("Encryption key must be 32 bytes, got {}", key.0.len()),
            });
        }

        Ok(())
    }

    // Getters
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }

    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn encryption_master_key(&self) -> Option<&[u8]> {
        self.encryption_master_key.as_ref().map(|k| k.0.as_slice())
    }

    pub fn pool_max_idle_per_host(&self) -> usize {
        self.pool_max_idle_per_host
    }

    pub fn enable_retry(&self) -> bool {
        self.enable_retry
    }

    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    pub fn auto_idempotency_key(&self) -> bool {
        self.auto_idempotency_key
    }

    /// Gets the base URL
    pub fn base_url(&self) -> String {
        let port = match self.port {
            Some(port) => format!(":{}", port),
            None => String::new(),
        };
        format!("{}://{}{}", self.scheme, self.host, port)
    }

    /// Gets the prefix path for API requests
    pub fn prefix_path(&self, sub_path: &str) -> String {
        format!("/apps/{}{}", self.app_id, sub_path)
    }
}

/// Builder for Sockudo configuration
#[derive(Default)]
pub struct ConfigBuilder {
    scheme: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    app_id: Option<String>,
    key: Option<String>,
    secret: Option<String>,
    timeout: Option<Duration>,
    encryption_master_key: Option<EncryptionKey>,
    pool_max_idle_per_host: Option<usize>,
    enable_retry: Option<bool>,
    max_retries: Option<u32>,
    auto_idempotency_key: Option<bool>,
}

impl ConfigBuilder {
    /// Sets the app ID
    pub fn app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Sets the app key
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sets the app secret
    pub fn secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    /// Sets the cluster
    pub fn cluster(mut self, cluster: impl AsRef<str>) -> Self {
        self.host = Some(format!("api-{}.sockudo.io", cluster.as_ref()));
        self
    }

    /// Sets a custom host
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Sets whether to use TLS
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.scheme = Some(if use_tls { "https" } else { "http" }.to_string());
        self
    }

    /// Sets the port
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Sets the timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the encryption master key from raw bytes
    pub fn encryption_master_key(mut self, key: Vec<u8>) -> Result<Self> {
        if key.len() != 32 {
            return Err(SockudoError::Config {
                message: format!("Encryption key must be 32 bytes, got {}", key.len()),
            });
        }
        self.encryption_master_key = Some(EncryptionKey(key));
        Ok(self)
    }

    /// Sets the encryption master key from base64
    pub fn encryption_master_key_base64(self, key_base64: impl AsRef<str>) -> Result<Self> {
        let decoded = BASE64
            .decode(key_base64.as_ref())
            .map_err(|e| SockudoError::Config {
                message: format!("Invalid base64 encryption key: {}", e),
            })?;

        self.encryption_master_key(decoded)
    }

    /// Sets the maximum idle connections per host
    pub fn pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.pool_max_idle_per_host = Some(max);
        self
    }

    /// Enables or disables retry logic
    pub fn enable_retry(mut self, enable: bool) -> Self {
        self.enable_retry = Some(enable);
        self
    }

    /// Sets the maximum number of retries
    pub fn max_retries(mut self, max: u32) -> Self {
        self.max_retries = Some(max);
        self
    }

    /// Enables or disables automatic deterministic idempotency key generation.
    /// When enabled (default), each `trigger()` / `trigger_batch()` call that lacks
    /// an explicit idempotency key will receive one derived from the client's session ID
    /// and a monotonically increasing publish serial.
    pub fn auto_idempotency_key(mut self, enable: bool) -> Self {
        self.auto_idempotency_key = Some(enable);
        self
    }

    /// Builds the configuration
    pub fn build(self) -> Result<Config> {
        let app_id = self.app_id.ok_or_else(|| SockudoError::Config {
            message: "App ID is required".to_string(),
        })?;

        let key = self.key.ok_or_else(|| SockudoError::Config {
            message: "App key is required".to_string(),
        })?;

        let secret = self.secret.ok_or_else(|| SockudoError::Config {
            message: "App secret is required".to_string(),
        })?;

        let config = Config {
            scheme: self.scheme.unwrap_or_else(|| "https".to_string()),
            host: self.host.unwrap_or_else(|| "api.sockudo.io".to_string()),
            port: self.port,
            app_id,
            token: Token::new(key, secret),
            timeout: self.timeout.unwrap_or(Duration::from_secs(30)),
            encryption_master_key: self.encryption_master_key,
            pool_max_idle_per_host: self.pool_max_idle_per_host.unwrap_or(10),
            enable_retry: self.enable_retry.unwrap_or(true),
            max_retries: self.max_retries.unwrap_or(3),
            auto_idempotency_key: self.auto_idempotency_key.unwrap_or(true),
        };

        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = Config::builder()
            .app_id("123")
            .key("key")
            .secret("secret")
            .cluster("eu")
            .timeout(Duration::from_secs(10))
            .enable_retry(false)
            .build()
            .unwrap();

        assert_eq!(config.app_id(), "123");
        assert_eq!(config.host(), "api-eu.sockudo.io");
        assert_eq!(config.timeout(), Duration::from_secs(10));
        assert!(!config.enable_retry());
    }

    #[test]
    fn test_config_validation() {
        assert!(Config::builder().build().is_err());
        assert!(Config::builder().app_id("123").build().is_err());
        assert!(
            Config::builder()
                .app_id("123")
                .key("key")
                .secret("secret")
                .build()
                .is_ok()
        );
    }

    #[test]
    fn test_encryption_key_validation() {
        let config = Config::builder()
            .app_id("123")
            .key("key")
            .secret("secret")
            .encryption_master_key(vec![0u8; 32])
            .unwrap()
            .build()
            .unwrap();

        assert!(config.encryption_master_key().is_some());

        // Wrong size should fail
        assert!(
            Config::builder()
                .app_id("123")
                .key("key")
                .secret("secret")
                .encryption_master_key(vec![0u8; 16])
                .is_err()
        );
    }
}
