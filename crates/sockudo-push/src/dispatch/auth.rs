use std::sync::Arc;
use std::time::{Duration, Instant};

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
    cache: Arc<tokio::sync::RwLock<TokenCache>>,
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
    refresh_skew_ms: u64,
}

#[derive(Default)]
struct TokenCache {
    token: Option<CachedProviderAccessToken>,
    failure: Option<(Instant, ProviderAuthError)>,
    generation: u64,
}

const REFRESH_FAILURE_COOLDOWN: Duration = Duration::from_millis(250);

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
            cache: Arc::new(tokio::sync::RwLock::new(TokenCache::default())),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            refresh_skew_ms: 5 * 60 * 1000,
        }
    }

    pub async fn access_token(&self, now_ms: u64) -> Result<SecretString, ProviderAuthError> {
        Ok(self.refresh(now_ms).await?.token)
    }

    pub async fn bearer_token(&self, now_ms: u64) -> Result<SecretString, ProviderAuthError> {
        Ok(self.refresh(now_ms).await?.bearer_token)
    }

    async fn refresh(&self, now_ms: u64) -> Result<CachedProviderAccessToken, ProviderAuthError> {
        if let Some(outcome) = self.cached_outcome(now_ms).await {
            return outcome;
        }
        let _refresh = self.refresh_lock.lock().await;
        loop {
            if let Some(outcome) = self.cached_outcome(now_ms).await {
                return outcome;
            }
            let generation = self.cache.read().await.generation;
            let outcome = self.source.fetch_token(now_ms).await.and_then(|token| {
                if token.expires_at_ms <= now_ms {
                    return Err(ProviderAuthError {
                        class: "auth_failure",
                        reason: "provider returned an expired access token".to_owned(),
                    });
                }
                CachedProviderAccessToken::new(token)
            });
            let mut cached = self.cache.write().await;
            // Invalidation fences a refresh already in flight. Never repopulate the
            // cache with credentials fetched before that invalidation.
            if cached.generation != generation {
                continue;
            }
            match &outcome {
                Ok(token) => {
                    cached.token = Some(token.clone());
                    cached.failure = None;
                }
                Err(error) => {
                    cached.token = None;
                    cached.failure =
                        Some((Instant::now() + REFRESH_FAILURE_COOLDOWN, error.clone()));
                }
            }
            return outcome;
        }
    }

    async fn cached_outcome(
        &self,
        now_ms: u64,
    ) -> Option<Result<CachedProviderAccessToken, ProviderAuthError>> {
        let cached = self.cache.read().await;
        if let Some(token) = cached
            .token
            .as_ref()
            .filter(|token| token.expires_at_ms > now_ms.saturating_add(self.refresh_skew_ms))
        {
            return Some(Ok(token.clone()));
        }
        cached
            .failure
            .as_ref()
            .and_then(|(until, error)| (Instant::now() < *until).then(|| Err(error.clone())))
    }

    pub async fn invalidate(&self) {
        let mut cached = self.cache.write().await;
        cached.generation = cached.generation.wrapping_add(1);
        cached.token = None;
        cached.failure = None;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Source {
        calls: AtomicUsize,
        fail: bool,
    }

    #[async_trait]
    impl ProviderTokenSource for Source {
        async fn fetch_token(&self, now_ms: u64) -> Result<ProviderAccessToken, ProviderAuthError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5)).await;
            if self.fail {
                Err(ProviderAuthError {
                    class: "auth_failure",
                    reason: "unavailable".to_owned(),
                })
            } else {
                Ok(ProviderAccessToken {
                    token: SecretString::new(format!("token-{call}")).unwrap(),
                    expires_at_ms: now_ms.saturating_add(3_600_000),
                })
            }
        }
    }

    #[tokio::test]
    async fn concurrent_successes_and_failures_share_one_refresh() {
        for fail in [false, true] {
            let source = Arc::new(Source {
                calls: AtomicUsize::new(0),
                fail,
            });
            let cache = CachedTokenProvider::new(source.clone());
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..64 {
                let cache = cache.clone();
                tasks.spawn(async move { cache.bearer_token(1_000).await });
            }
            while let Some(result) = tasks.join_next().await {
                assert_eq!(result.unwrap().is_err(), fail);
            }
            assert_eq!(source.calls.load(Ordering::SeqCst), 1);
            if fail {
                tokio::time::sleep(REFRESH_FAILURE_COOLDOWN).await;
                assert!(cache.access_token(1_000).await.is_err());
                assert_eq!(source.calls.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[tokio::test]
    async fn invalidation_fences_refresh_and_clears_failed_outcomes() {
        let source = Arc::new(Source {
            calls: AtomicUsize::new(0),
            fail: false,
        });
        let cache = CachedTokenProvider::new(source.clone());
        let task = tokio::spawn({
            let cache = cache.clone();
            async move { cache.access_token(1_000).await.unwrap() }
        });
        while source.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cache.invalidate().await;
        assert_eq!(task.await.unwrap().expose_secret(), "token-1");
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            cache.access_token(1_000).await.unwrap().expose_secret(),
            "token-1"
        );

        let source = Arc::new(Source {
            calls: AtomicUsize::new(0),
            fail: true,
        });
        let cache = CachedTokenProvider::new(source.clone());
        assert!(cache.access_token(0).await.is_err());
        cache.invalidate().await;
        assert!(cache.access_token(0).await.is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancelled_refresh_does_not_poison_admission() {
        let source = Arc::new(Source {
            calls: AtomicUsize::new(0),
            fail: false,
        });
        let cache = CachedTokenProvider::new(source.clone());
        let task = tokio::spawn({
            let cache = cache.clone();
            async move { cache.access_token(0).await }
        });
        while source.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        assert!(cache.access_token(0).await.is_ok());
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    }
}
