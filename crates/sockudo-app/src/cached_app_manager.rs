use async_trait::async_trait;
use sockudo_core::app::{App, AppManager};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::CacheSettings;
use std::sync::Arc;
use tracing::{debug, warn};

const CACHE_PREFIX_ID: &str = "app:id";
const CACHE_PREFIX_KEY: &str = "app:key";

/// Caching decorator for AppManager implementations
pub struct CachedAppManager {
    inner: Arc<dyn AppManager + Send + Sync>,
    cache: Arc<dyn CacheManager + Send + Sync>,
    settings: CacheSettings,
}

impl CachedAppManager {
    pub fn new(
        inner: Arc<dyn AppManager + Send + Sync>,
        cache: Arc<dyn CacheManager + Send + Sync>,
        settings: CacheSettings,
    ) -> Self {
        Self {
            inner,
            cache,
            settings,
        }
    }

    fn cache_key_for_id(app_id: &str) -> String {
        format!("{}:{}", CACHE_PREFIX_ID, app_id)
    }

    fn cache_key_for_key(app_key: &str) -> String {
        format!("{}:{}", CACHE_PREFIX_KEY, app_key)
    }

    fn serialize<T: serde::Serialize>(value: &T) -> Result<String> {
        sonic_rs::to_string(value)
            .map_err(|e| Error::Internal(format!("Serialization failed: {}", e)))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(json: &str) -> Result<T> {
        sonic_rs::from_str(json)
            .map_err(|e| Error::Internal(format!("Deserialization failed: {}", e)))
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        match self.cache.get(key).await {
            Ok(Some(json)) => match Self::deserialize(&json) {
                Ok(value) => {
                    debug!("Cache hit: {}", key);
                    Some(value)
                }
                Err(e) => {
                    warn!("Cache deserialize error for {}: {}", key, e);
                    None
                }
            },
            Ok(None) => {
                debug!("Cache miss: {}", key);
                None
            }
            Err(e) => {
                warn!("Cache get error for {}: {}", key, e);
                None
            }
        }
    }

    async fn set<T: serde::Serialize>(&self, key: &str, value: &T) {
        let json = match Self::serialize(value) {
            Ok(j) => j,
            Err(e) => {
                warn!("Cache serialize error for {}: {}", key, e);
                return;
            }
        };

        if let Err(e) = self.cache.set(key, &json, self.settings.ttl).await {
            warn!("Cache set error for {}: {}", key, e);
        }
    }

    async fn remove(&self, key: &str) {
        if let Err(e) = self.cache.remove(key).await {
            warn!("Cache remove error for {}: {}", key, e);
        }
    }

    async fn cache_app(&self, app: &App) {
        self.set(&Self::cache_key_for_id(&app.id), app).await;
        self.set(&Self::cache_key_for_key(&app.key), app).await;
    }

    async fn invalidate_app(&self, app_id: &str, app_key: &str) {
        self.remove(&Self::cache_key_for_id(app_id)).await;
        self.remove(&Self::cache_key_for_key(app_key)).await;
    }

    async fn invalidate_app_by_id(&self, app_id: &str) {
        let id_key = Self::cache_key_for_id(app_id);
        if let Some(app) = self.get::<App>(&id_key).await {
            self.invalidate_app(app_id, &app.key).await;
        } else {
            self.remove(&id_key).await;
        }
    }
}

#[async_trait]
impl AppManager for CachedAppManager {
    async fn init(&self) -> Result<()> {
        self.inner.init().await
    }

    /// Get an app by ID from cache or database
    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        if !self.settings.enabled {
            return self.inner.find_by_id(app_id).await;
        }

        let cache_key = Self::cache_key_for_id(app_id);
        if let Some(app) = self.get::<App>(&cache_key).await {
            return Ok(Some(app));
        }

        let app = self.inner.find_by_id(app_id).await?;
        if let Some(ref app) = app {
            self.cache_app(app).await;
        }

        Ok(app)
    }

    /// Get an app by key from cache or database
    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        if !self.settings.enabled {
            return self.inner.find_by_key(key).await;
        }

        let cache_key = Self::cache_key_for_key(key);
        if let Some(app) = self.get::<App>(&cache_key).await {
            return Ok(Some(app));
        }

        let app = self.inner.find_by_key(key).await?;
        if let Some(ref app) = app {
            self.cache_app(app).await;
        }

        Ok(app)
    }

    /// Register a new app in the database and cache
    async fn create_app(&self, config: App) -> Result<()> {
        self.inner.create_app(config.clone()).await?;

        if self.settings.enabled {
            self.cache_app(&config).await;
        }

        Ok(())
    }

    /// Update an existing app and refresh the cache
    async fn update_app(&self, config: App) -> Result<()> {
        self.inner.update_app(config.clone()).await?;

        if self.settings.enabled {
            self.invalidate_app(&config.id, &config.key).await;
            self.cache_app(&config).await;
        }

        Ok(())
    }

    /// Remove an app from the database and cache
    async fn delete_app(&self, app_id: &str) -> Result<()> {
        let app = if self.settings.enabled {
            self.inner.find_by_id(app_id).await?
        } else {
            None
        };

        self.inner.delete_app(app_id).await?;

        if self.settings.enabled {
            if let Some(app) = app {
                self.invalidate_app(app_id, &app.key).await;
            } else {
                self.invalidate_app_by_id(app_id).await;
            }
        }

        Ok(())
    }

    /// Get all apps from the database
    async fn get_apps(&self) -> Result<Vec<App>> {
        let apps = self.inner.get_apps().await?;

        if self.settings.enabled && !apps.is_empty() {
            for app in &apps {
                self.cache_app(app).await;
            }
            debug!("Cached {} apps", apps.len());
        }

        Ok(apps)
    }

    async fn check_health(&self) -> Result<()> {
        self.inner.check_health().await
    }
}
