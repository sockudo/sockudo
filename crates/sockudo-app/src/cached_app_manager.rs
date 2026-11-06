use crate::secret_cipher::AppSecretCipher;
use async_trait::async_trait;
use dashmap::DashMap;
use sockudo_core::app::{App, AppManager};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::CacheSettings;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{debug, warn};

const CACHE_PREFIX_ID: &str = "app:id";
const CACHE_PREFIX_KEY: &str = "app:key";
const SHARED_CACHE_ACCESS_TIMEOUT: Duration = Duration::from_millis(100);

/// Shared-cache form of an app when a secret encryption key is configured:
/// the app is stored with an empty `secret` and the sealed secret beside it,
/// so the plaintext secret never reaches the shared cache (e.g. Redis).
#[derive(serde::Serialize, serde::Deserialize)]
struct SealedCachedApp {
    app: App,
    secret_enc: String,
}

#[derive(Clone)]
struct LocalAppEntry {
    app: App,
    expires_at: Option<Instant>,
}

impl LocalAppEntry {
    fn is_fresh(&self) -> bool {
        self.expires_at
            .is_none_or(|expires_at| Instant::now() < expires_at)
    }
}

/// Caching decorator for AppManager implementations
pub struct CachedAppManager {
    inner: Arc<dyn AppManager + Send + Sync>,
    cache: Arc<dyn CacheManager + Send + Sync>,
    settings: CacheSettings,
    secret_cipher: Option<AppSecretCipher>,
    local_apps_by_id: DashMap<String, LocalAppEntry>,
    local_app_id_by_key: DashMap<String, String>,
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
            secret_cipher: None,
            local_apps_by_id: DashMap::new(),
            local_app_id_by_key: DashMap::new(),
        }
    }

    /// Seal app secrets with `cipher` before they are written to the shared
    /// cache. Entries are decrypted on read and only kept in memory in clear.
    pub fn with_secret_cipher(mut self, cipher: Option<AppSecretCipher>) -> Self {
        self.secret_cipher = cipher;
        self
    }

    fn cache_key_for_id(app_id: &str) -> String {
        format!("{}:{}", CACHE_PREFIX_ID, app_id)
    }

    fn cache_key_for_key(app_key: &str) -> String {
        format!("{}:{}", CACHE_PREFIX_KEY, app_key)
    }

    fn cache_index_label(key: &str) -> &'static str {
        if key.starts_with(CACHE_PREFIX_ID) {
            "app_id"
        } else if key.starts_with(CACHE_PREFIX_KEY) {
            "app_key"
        } else {
            "unknown"
        }
    }

    fn serialize<T: serde::Serialize>(value: &T) -> Result<String> {
        sonic_rs::to_string(value)
            .map_err(|e| Error::Internal(format!("Serialization failed: {}", e)))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(json: &str) -> Result<T> {
        sonic_rs::from_str(json)
            .map_err(|e| Error::Internal(format!("Deserialization failed: {}", e)))
    }

    fn local_expires_at(&self) -> Option<Instant> {
        if self.settings.ttl == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(self.settings.ttl))
        }
    }

    fn remember_app(&self, app: &App) {
        if let Some(previous) = self.local_apps_by_id.insert(
            app.id.clone(),
            LocalAppEntry {
                app: app.clone(),
                expires_at: self.local_expires_at(),
            },
        ) && previous.app.key != app.key
        {
            self.local_app_id_by_key.remove(&previous.app.key);
        }

        self.local_app_id_by_key
            .insert(app.key.clone(), app.id.clone());
    }

    fn remember_apps(&self, apps: &[App]) {
        self.local_apps_by_id.clear();
        self.local_app_id_by_key.clear();
        for app in apps {
            self.remember_app(app);
        }
    }

    fn forget_app(&self, app_id: &str, app_key: Option<&str>) {
        if let Some((_, previous)) = self.local_apps_by_id.remove(app_id) {
            self.local_app_id_by_key.remove(&previous.app.key);
        }
        if let Some(app_key) = app_key {
            self.local_app_id_by_key.remove(app_key);
        }
    }

    fn find_local_by_id(&self, app_id: &str) -> Option<App> {
        let entry = self
            .local_apps_by_id
            .get(app_id)
            .map(|entry| entry.value().clone())?;

        if entry.is_fresh() {
            Some(entry.app)
        } else {
            self.forget_app(app_id, Some(&entry.app.key));
            None
        }
    }

    fn find_local_by_key(&self, key: &str) -> Option<App> {
        let app_id = self
            .local_app_id_by_key
            .get(key)
            .map(|entry| entry.value().clone())?;
        self.find_local_by_id(&app_id)
    }

    async fn warm_local_index(&self) {
        match self.inner.get_apps().await {
            Ok(apps) => {
                self.remember_apps(&apps);
                debug!(apps_count = apps.len(), "local app index warmed");
            }
            Err(e) => {
                warn!(error = %e, "local app index warm failed");
            }
        }
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        match timeout(SHARED_CACHE_ACCESS_TIMEOUT, self.cache.get(key)).await {
            Err(_) => {
                warn!(
                    cache_index = Self::cache_index_label(key),
                    "Cache get timeout"
                );
                None
            }
            Ok(result) => match result {
                Ok(Some(json)) => match Self::deserialize(&json) {
                    Ok(value) => {
                        debug!(cache_index = Self::cache_index_label(key), "Cache hit");
                        Some(value)
                    }
                    Err(e) => {
                        warn!(
                            cache_index = Self::cache_index_label(key),
                            error = %e,
                            "Cache deserialize error"
                        );
                        None
                    }
                },
                Ok(None) => {
                    debug!(cache_index = Self::cache_index_label(key), "Cache miss");
                    None
                }
                Err(e) => {
                    warn!(
                        cache_index = Self::cache_index_label(key),
                        error = %e,
                        "Cache get error"
                    );
                    None
                }
            },
        }
    }

    async fn set<T: serde::Serialize>(&self, key: &str, value: &T) {
        let json = match Self::serialize(value) {
            Ok(j) => j,
            Err(e) => {
                warn!(
                    cache_index = Self::cache_index_label(key),
                    error = %e,
                    "app cache serialize error"
                );
                return;
            }
        };

        match timeout(
            SHARED_CACHE_ACCESS_TIMEOUT,
            self.cache.set(key, &json, self.settings.ttl),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(
                cache_index = Self::cache_index_label(key),
                error = %e,
                "Cache set error"
            ),
            Err(_) => warn!(
                cache_index = Self::cache_index_label(key),
                "Cache set timeout"
            ),
        }
    }

    async fn remove(&self, key: &str) {
        match timeout(SHARED_CACHE_ACCESS_TIMEOUT, self.cache.remove(key)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(
                cache_index = Self::cache_index_label(key),
                error = %e,
                "Cache remove error"
            ),
            Err(_) => warn!(
                cache_index = Self::cache_index_label(key),
                "Cache remove timeout"
            ),
        }
    }

    /// Reads an app from the shared cache. With a cipher configured only
    /// sealed entries are accepted; a plaintext or undecryptable entry is a
    /// miss, so it is reloaded from the app manager and overwritten sealed.
    async fn get_app(&self, key: &str) -> Option<App> {
        let Some(cipher) = self.secret_cipher.as_ref() else {
            return self.get::<App>(key).await;
        };
        let SealedCachedApp {
            mut app,
            secret_enc,
        } = self.get::<SealedCachedApp>(key).await?;
        match cipher.decrypt(&app.id, &secret_enc) {
            Ok(secret) => {
                app.secret = secret;
                Some(app)
            }
            Err(e) => {
                warn!(
                    cache_index = Self::cache_index_label(key),
                    error = %e,
                    "app cache entry secret decrypt failed"
                );
                None
            }
        }
    }

    async fn set_app(&self, key: &str, app: &App) {
        let Some(cipher) = self.secret_cipher.as_ref() else {
            self.set(key, app).await;
            return;
        };
        let secret_enc = match cipher.encrypt(&app.id, &app.secret) {
            Ok(secret_enc) => secret_enc,
            Err(e) => {
                warn!(
                    cache_index = Self::cache_index_label(key),
                    error = %e,
                    "app cache entry secret encrypt failed"
                );
                return;
            }
        };
        let mut app = app.clone();
        app.secret = String::new();
        self.set(key, &SealedCachedApp { app, secret_enc }).await;
    }

    async fn cache_app(&self, app: &App) {
        self.remember_app(app);
        self.set_app(&Self::cache_key_for_id(&app.id), app).await;
        self.set_app(&Self::cache_key_for_key(&app.key), app).await;
    }

    async fn invalidate_app(&self, app_id: &str, app_key: &str) {
        self.forget_app(app_id, Some(app_key));
        self.remove(&Self::cache_key_for_id(app_id)).await;
        self.remove(&Self::cache_key_for_key(app_key)).await;
    }

    async fn invalidate_app_by_id(&self, app_id: &str) {
        if let Some(app) = self.find_local_by_id(app_id) {
            self.invalidate_app(app_id, &app.key).await;
            return;
        }

        let id_key = Self::cache_key_for_id(app_id);
        if let Some(app) = self.get_app(&id_key).await {
            self.invalidate_app(app_id, &app.key).await;
        } else {
            self.forget_app(app_id, None);
            self.remove(&id_key).await;
        }
    }
}

#[async_trait]
impl AppManager for CachedAppManager {
    async fn init(&self) -> Result<()> {
        self.inner.init().await?;
        if self.settings.enabled {
            self.warm_local_index().await;
        }
        Ok(())
    }

    /// Get an app by ID from cache or database
    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        if !self.settings.enabled {
            return self.inner.find_by_id(app_id).await;
        }

        if let Some(app) = self.find_local_by_id(app_id) {
            return Ok(Some(app));
        }

        let cache_key = Self::cache_key_for_id(app_id);
        if let Some(app) = self.get_app(&cache_key).await {
            self.remember_app(&app);
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

        if let Some(app) = self.find_local_by_key(key) {
            return Ok(Some(app));
        }

        let cache_key = Self::cache_key_for_key(key);
        if let Some(app) = self.get_app(&cache_key).await {
            self.remember_app(&app);
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
        let previous_key = if self.settings.enabled {
            self.find_local_by_id(&config.id).map(|app| app.key)
        } else {
            None
        };

        self.inner.update_app(config.clone()).await?;

        if self.settings.enabled {
            if let Some(previous_key) = previous_key.as_deref()
                && previous_key != config.key
            {
                self.remove(&Self::cache_key_for_key(previous_key)).await;
            }
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

        if self.settings.enabled {
            self.remember_apps(&apps);
            for app in &apps {
                self.cache_app(app).await;
            }
            debug!(apps_count = apps.len(), "apps cached");
        }

        Ok(apps)
    }

    async fn check_health(&self) -> Result<()> {
        self.inner.check_health().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_app_manager::MemoryAppManager;
    use sockudo_cache::MemoryCacheManager;
    use sockudo_core::cache::CacheManager;
    use sockudo_core::cache::CacheScanPage;
    use sockudo_core::options::MemoryCacheOptions;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::sleep;

    #[test]
    fn cache_log_labels_do_not_contain_app_identifiers_or_keys() {
        let id_key = CachedAppManager::cache_key_for_id("sensitive-app-id");
        let app_key = CachedAppManager::cache_key_for_key("sensitive-app-key");

        assert_eq!(CachedAppManager::cache_index_label(&id_key), "app_id");
        assert_eq!(CachedAppManager::cache_index_label(&app_key), "app_key");
        assert!(!CachedAppManager::cache_index_label(&app_key).contains("sensitive"));
    }

    struct SlowCacheManager {
        get_delay: Duration,
        get_calls: AtomicUsize,
        set_calls: AtomicUsize,
        remove_calls: AtomicUsize,
    }

    impl SlowCacheManager {
        fn new(get_delay: Duration) -> Self {
            Self {
                get_delay,
                get_calls: AtomicUsize::new(0),
                set_calls: AtomicUsize::new(0),
                remove_calls: AtomicUsize::new(0),
            }
        }

        fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl CacheManager for SlowCacheManager {
        async fn has(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }

        async fn get(&self, _key: &str) -> Result<Option<String>> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            sleep(self.get_delay).await;
            Ok(None)
        }

        async fn set(&self, _key: &str, _value: &str, _ttl_seconds: u64) -> Result<()> {
            self.set_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn remove(&self, _key: &str) -> Result<()> {
            self.remove_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn disconnect(&self) -> Result<()> {
            Ok(())
        }

        async fn ttl(&self, _key: &str) -> Result<Option<Duration>> {
            Ok(None)
        }

        async fn scan_prefix(&self, _prefix: &str, _limit: usize) -> Result<Vec<(String, String)>> {
            Ok(Vec::new())
        }

        async fn scan_prefix_page(
            &self,
            _prefix: &str,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<CacheScanPage> {
            Ok(CacheScanPage::default())
        }
    }

    fn create_test_app(id: &str) -> App {
        App::from_policy(
            id.to_string(),
            format!("{}_key", id),
            format!("{}_secret", id),
            true,
            sockudo_core::app::AppPolicy {
                limits: sockudo_core::app::AppLimitsPolicy {
                    max_connections: 100,
                    max_client_events_per_second: 100,
                    ..Default::default()
                },
                features: sockudo_core::app::AppFeaturesPolicy {
                    enable_client_messages: false,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
    }

    async fn create_test_manager() -> CachedAppManager {
        let inner = Arc::new(MemoryAppManager::new());
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions {
                ttl: 300,
                cleanup_interval: 60,
                max_capacity: 1000,
            },
        ));
        let settings = CacheSettings {
            enabled: true,
            ttl: 300,
        };

        CachedAppManager::new(inner, cache, settings)
    }

    #[tokio::test]
    async fn test_find_by_id_caches_result() {
        let manager = create_test_manager().await;
        let app = create_test_app("test1");

        manager.create_app(app.clone()).await.unwrap();

        let found1 = manager.find_by_id("test1").await.unwrap();
        assert!(found1.is_some());

        let found2 = manager.find_by_id("test1").await.unwrap();
        assert!(found2.is_some());
        assert_eq!(found2.unwrap().id, "test1");
    }

    #[tokio::test]
    async fn test_find_by_key_caches_result() {
        let manager = create_test_manager().await;
        let app = create_test_app("test2");

        manager.create_app(app.clone()).await.unwrap();

        let found = manager.find_by_key("test2_key").await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().key, "test2_key");
    }

    #[tokio::test]
    async fn test_update_invalidates_cache() {
        let manager = create_test_manager().await;
        let mut app = create_test_app("test3");

        manager.create_app(app.clone()).await.unwrap();

        let found = manager.find_by_id("test3").await.unwrap().unwrap();
        assert_eq!(found.max_connections_limit(), 100);

        app.policy.limits.max_connections = 200;
        manager.update_app(app).await.unwrap();

        let found_updated = manager.find_by_id("test3").await.unwrap().unwrap();
        assert_eq!(found_updated.max_connections_limit(), 200);
    }

    #[tokio::test]
    async fn test_delete_invalidates_cache() {
        let manager = create_test_manager().await;
        let app = create_test_app("test4");

        manager.create_app(app).await.unwrap();
        assert!(manager.find_by_id("test4").await.unwrap().is_some());

        manager.delete_app("test4").await.unwrap();
        assert!(manager.find_by_id("test4").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_find_by_id_uses_local_index_before_shared_cache() {
        let inner = Arc::new(MemoryAppManager::new());
        let cache = Arc::new(SlowCacheManager::new(Duration::from_secs(1)));
        let manager = CachedAppManager::new(
            inner,
            cache.clone(),
            CacheSettings {
                enabled: true,
                ttl: 300,
            },
        );
        let app = create_test_app("local-fast");

        manager.create_app(app).await.unwrap();

        let found = timeout(Duration::from_millis(50), manager.find_by_id("local-fast"))
            .await
            .expect("local app lookup should not wait for shared cache")
            .unwrap()
            .unwrap();

        assert_eq!(found.id, "local-fast");
        assert_eq!(
            cache.get_calls(),
            0,
            "local app lookup should issue zero shared cache gets"
        );
    }

    #[tokio::test]
    async fn test_find_by_key_uses_local_index_before_shared_cache() {
        let inner = Arc::new(MemoryAppManager::new());
        let cache = Arc::new(SlowCacheManager::new(Duration::from_secs(1)));
        let manager = CachedAppManager::new(
            inner,
            cache.clone(),
            CacheSettings {
                enabled: true,
                ttl: 300,
            },
        );
        let app = create_test_app("local-key-fast");

        manager.create_app(app).await.unwrap();

        let found = timeout(
            Duration::from_millis(50),
            manager.find_by_key("local-key-fast_key"),
        )
        .await
        .expect("local key lookup should not wait for shared cache")
        .unwrap()
        .unwrap();

        assert_eq!(found.id, "local-key-fast");
        assert_eq!(
            cache.get_calls(),
            0,
            "local key lookup should issue zero shared cache gets"
        );
    }

    #[tokio::test]
    async fn test_slow_shared_cache_get_falls_back_to_inner_manager_quickly() {
        let inner = Arc::new(MemoryAppManager::new());
        inner
            .create_app(create_test_app("cold-fallback"))
            .await
            .unwrap();
        let cache = Arc::new(SlowCacheManager::new(Duration::from_secs(1)));
        let manager = CachedAppManager::new(
            inner,
            cache.clone(),
            CacheSettings {
                enabled: true,
                ttl: 300,
            },
        );

        let found = timeout(
            Duration::from_millis(250),
            manager.find_by_id("cold-fallback"),
        )
        .await
        .expect("shared cache timeout should leave room for inner fallback")
        .unwrap()
        .unwrap();

        assert_eq!(found.id, "cold-fallback");
        assert_eq!(cache.get_calls(), 1);
    }

    #[tokio::test]
    async fn test_init_warms_local_index_without_shared_cache_get() {
        let inner = Arc::new(MemoryAppManager::new());
        inner
            .create_app(create_test_app("warm-init"))
            .await
            .unwrap();
        let cache = Arc::new(SlowCacheManager::new(Duration::from_secs(1)));
        let manager = CachedAppManager::new(
            inner,
            cache.clone(),
            CacheSettings {
                enabled: true,
                ttl: 300,
            },
        );

        manager.init().await.unwrap();

        let found = timeout(Duration::from_millis(50), manager.find_by_id("warm-init"))
            .await
            .expect("warm local index lookup should not wait for shared cache")
            .unwrap()
            .unwrap();

        assert_eq!(found.id, "warm-init");
        assert_eq!(
            cache.get_calls(),
            0,
            "warm init should not require shared cache reads"
        );
    }

    fn test_cipher() -> AppSecretCipher {
        AppSecretCipher::from_encoded_key("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap()
    }

    #[tokio::test]
    async fn test_shared_cache_never_holds_plaintext_secret_with_cipher() {
        let inner = Arc::new(MemoryAppManager::new());
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let settings = CacheSettings {
            enabled: true,
            ttl: 300,
        };
        let manager = CachedAppManager::new(inner.clone(), cache.clone(), settings.clone())
            .with_secret_cipher(Some(test_cipher()));
        let mut app = create_test_app("sealed");
        app.secret = "very-secret-value".to_string();
        manager.create_app(app).await.unwrap();

        for key in [
            CachedAppManager::cache_key_for_id("sealed"),
            CachedAppManager::cache_key_for_key("sealed_key"),
        ] {
            let raw = cache.get(&key).await.unwrap().expect("entry cached");
            assert!(
                !raw.contains("very-secret-value"),
                "shared cache entry must not contain the plaintext secret"
            );
        }

        // Another node with the same key reads the sealed entry from the
        // shared cache (its local index is empty) and gets the clear secret.
        let other =
            CachedAppManager::new(inner, cache, settings).with_secret_cipher(Some(test_cipher()));
        let found = other
            .get_app(&CachedAppManager::cache_key_for_key("sealed_key"))
            .await
            .expect("sealed entry decrypts");
        assert_eq!(found.secret, "very-secret-value");
    }

    #[tokio::test]
    async fn test_plaintext_shared_cache_entry_is_a_miss_with_cipher() {
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let settings = CacheSettings {
            enabled: true,
            ttl: 300,
        };
        let key = CachedAppManager::cache_key_for_id("legacy");
        let plain = CachedAppManager::new(
            Arc::new(MemoryAppManager::new()),
            cache.clone(),
            settings.clone(),
        );
        plain.set_app(&key, &create_test_app("legacy")).await;

        let sealed = CachedAppManager::new(Arc::new(MemoryAppManager::new()), cache, settings)
            .with_secret_cipher(Some(test_cipher()));
        assert!(sealed.get_app(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let inner = Arc::new(MemoryAppManager::new());
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let settings = CacheSettings {
            enabled: false,
            ttl: 300,
        };

        let manager = CachedAppManager::new(inner, cache, settings);
        let app = create_test_app("test5");

        manager.create_app(app).await.unwrap();

        let found = manager.find_by_id("test5").await.unwrap();
        assert!(found.is_some());
    }
}
