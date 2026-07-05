use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::{ProviderError, ProviderFailureClass, SecretString};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAccessToken {
    pub token: SecretString,
    pub expires_at_ms: u64,
}

#[async_trait]
pub trait ProviderTokenSource: Send + Sync {
    async fn fetch_token(&self, now_ms: u64) -> Result<ProviderAccessToken, ProviderAuthError>;
}

#[derive(Clone)]
pub struct StaticTokenSource {
    token: SecretString,
    expires_at_ms: u64,
}

impl StaticTokenSource {
    pub fn new(token: SecretString, expires_at_ms: u64) -> Self {
        Self {
            token,
            expires_at_ms,
        }
    }
}

#[async_trait]
impl ProviderTokenSource for StaticTokenSource {
    async fn fetch_token(&self, _now_ms: u64) -> Result<ProviderAccessToken, ProviderAuthError> {
        Ok(ProviderAccessToken {
            token: self.token.clone(),
            expires_at_ms: self.expires_at_ms,
        })
    }
}

#[derive(Clone)]
pub struct CachedTokenProvider {
    source: Arc<dyn ProviderTokenSource + Send + Sync>,
    cache: Arc<tokio::sync::RwLock<Option<CachedProviderAccessToken>>>,
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
    refresh_skew_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CachedProviderAccessToken {
    token: SecretString,
    bearer_token: SecretString,
    expires_at_ms: u64,
}

impl CachedProviderAccessToken {
    fn new(token: ProviderAccessToken) -> Result<Self, ProviderAuthError> {
        let bearer_token = SecretString::new(format!("Bearer {}", token.token.expose_secret()))
            .map_err(|error| ProviderAuthError {
                class: "auth_failure",
                reason: error.to_string(),
            })?;
        Ok(Self {
            token: token.token,
            bearer_token,
            expires_at_ms: token.expires_at_ms,
        })
    }
}

impl CachedTokenProvider {
    pub fn new(source: Arc<dyn ProviderTokenSource + Send + Sync>) -> Self {
        Self {
            source,
            cache: Arc::new(tokio::sync::RwLock::new(None)),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            refresh_skew_ms: 5 * 60 * 1000,
        }
    }

    pub async fn access_token(&self, now_ms: u64) -> Result<SecretString, ProviderAuthError> {
        let cached = self.cache.read().await.clone();
        if let Some(cached) = cached
            && cached.expires_at_ms > now_ms.saturating_add(self.refresh_skew_ms)
        {
            return Ok(cached.token.clone());
        }

        let _refresh = self.refresh_lock.lock().await;
        let cached = self.cache.read().await.clone();
        if let Some(cached) = cached
            && cached.expires_at_ms > now_ms.saturating_add(self.refresh_skew_ms)
        {
            return Ok(cached.token.clone());
        }
        let refreshed = CachedProviderAccessToken::new(self.source.fetch_token(now_ms).await?)?;
        let token = refreshed.token.clone();
        *self.cache.write().await = Some(refreshed);
        Ok(token)
    }

    pub async fn bearer_token(&self, now_ms: u64) -> Result<SecretString, ProviderAuthError> {
        let cached = self.cache.read().await.clone();
        if let Some(cached) = cached
            && cached.expires_at_ms > now_ms.saturating_add(self.refresh_skew_ms)
        {
            return Ok(cached.bearer_token.clone());
        }

        let _refresh = self.refresh_lock.lock().await;
        let cached = self.cache.read().await.clone();
        if let Some(cached) = cached
            && cached.expires_at_ms > now_ms.saturating_add(self.refresh_skew_ms)
        {
            return Ok(cached.bearer_token.clone());
        }
        let refreshed = CachedProviderAccessToken::new(self.source.fetch_token(now_ms).await?)?;
        let bearer_token = refreshed.bearer_token.clone();
        *self.cache.write().await = Some(refreshed);
        Ok(bearer_token)
    }

    pub async fn invalidate(&self) {
        *self.cache.write().await = None;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAuthError {
    pub class: &'static str,
    pub reason: String,
}

pub(super) fn auth_error(error: ProviderAuthError) -> ProviderError {
    ProviderError {
        class: error.class.to_owned(),
        failure_class: ProviderFailureClass::CredentialAuth,
        reason: Some(error.reason),
        retry_after_ms: None,
    }
}
