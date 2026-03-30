use async_trait::async_trait;
use moka::future::Cache;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sockudo_core::app::App;
use sockudo_core::app::AppManager;
use sockudo_core::error::{Error, Result};
use std::time::Duration;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb_types::{RecordId, RecordIdKey, SurrealValue};
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct SurrealDbConfig {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub table_name: String,
    pub cache_ttl: u64,
    pub cache_max_capacity: u64,
}

impl Default for SurrealDbConfig {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:8000".to_string(),
            namespace: "sockudo".to_string(),
            database: "sockudo".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            table_name: "applications".to_string(),
            cache_ttl: 300,
            cache_max_capacity: 100,
        }
    }
}

pub struct SurrealDbAppManager {
    config: SurrealDbConfig,
    db: Surreal<Any>,
    app_cache: Cache<String, App>,
}

impl SurrealDbAppManager {
    pub async fn new(config: SurrealDbConfig) -> Result<Self> {
        info!("Initializing SurrealDB AppManager at {}", config.url);

        Self::validate_identifier(&config.table_name, "table_name")?;

        let db = connect(config.url.as_str())
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to SurrealDB: {e}")))?;

        db.signin(Root {
            username: config.username.clone(),
            password: config.password.clone(),
        })
        .await
        .map_err(|e| Error::Internal(format!("Failed to authenticate with SurrealDB: {e}")))?;

        db.use_ns(config.namespace.as_str())
            .use_db(config.database.as_str())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to select SurrealDB namespace/database: {e}"
                ))
            })?;

        let app_cache = Cache::builder()
            .time_to_live(Duration::from_secs(config.cache_ttl))
            .max_capacity(config.cache_max_capacity)
            .build();

        let manager = Self {
            config,
            db,
            app_cache,
        };

        manager.ensure_schema().await?;

        Ok(manager)
    }

    fn validate_identifier(identifier: &str, field_name: &str) -> Result<()> {
        let valid = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$")
            .expect("identifier regex must compile")
            .is_match(identifier);

        if valid {
            Ok(())
        } else {
            Err(Error::Internal(format!(
                "Invalid SurrealDB {field_name} '{identifier}'. Use only letters, numbers, and underscores."
            )))
        }
    }

    fn index_name(&self) -> String {
        format!("{}_key_idx", self.config.table_name)
    }

    fn record_resource(&self, app_id: &str) -> (String, String) {
        (self.config.table_name.clone(), app_id.to_string())
    }

    async fn cache_app(&self, app: &App) {
        self.app_cache.insert(app.id.clone(), app.clone()).await;
        self.app_cache.insert(app.key.clone(), app.clone()).await;
    }

    async fn ensure_schema(&self) -> Result<()> {
        let query = format!(
            "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS key UNIQUE;",
            self.config.table_name,
            self.index_name(),
            self.config.table_name
        );

        self.db
            .query(query)
            .await
            .map_err(|e| Error::Internal(format!("Failed to initialize SurrealDB schema: {e}")))?;

        Ok(())
    }

    async fn find_by_id_uncached(&self, app_id: &str) -> Result<Option<App>> {
        let record: Option<SurrealAppRecord> =
            self.db
                .select(self.record_resource(app_id))
                .await
                .map_err(|e| Error::Internal(format!("Failed to fetch app from SurrealDB: {e}")))?;

        record.map(TryInto::try_into).transpose()
    }

    pub async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        if let Some(app) = self.app_cache.get(app_id).await {
            return Ok(Some(app));
        }

        debug!("Cache miss for app {}, fetching from SurrealDB", app_id);

        let app = self.find_by_id_uncached(app_id).await?;
        if let Some(app) = &app {
            self.cache_app(app).await;
        }

        Ok(app)
    }

    pub async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        if let Some(app) = self.app_cache.get(key).await {
            return Ok(Some(app));
        }

        debug!("Cache miss for app key {}, fetching from SurrealDB", key);

        let mut response = self
            .db
            .query(format!(
                "SELECT * FROM {} WHERE key = $key LIMIT 1;",
                self.config.table_name
            ))
            .bind(("key", key.to_string()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to query app from SurrealDB: {e}")))?;

        let record: Option<SurrealAppRecord> = response
            .take(0usize)
            .map_err(|e| Error::Internal(format!("Failed to decode SurrealDB app record: {e}")))?;

        let app = record.map(TryInto::try_into).transpose()?;
        if let Some(app) = &app {
            self.cache_app(app).await;
        }

        Ok(app)
    }

    pub async fn create_app(&self, app: App) -> Result<()> {
        let stored = StoredApp::from(&app);

        let _: Option<StoredApp> = self
            .db
            .create(self.record_resource(&app.id))
            .content(stored)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create app in SurrealDB: {e}")))?;

        self.cache_app(&app).await;
        Ok(())
    }

    pub async fn update_app(&self, app: App) -> Result<()> {
        let existing = self.find_by_id_uncached(&app.id).await?;
        let stored = StoredApp::from(&app);

        let _: Option<StoredApp> = self
            .db
            .update(self.record_resource(&app.id))
            .content(stored)
            .await
            .map_err(|e| Error::Internal(format!("Failed to update app in SurrealDB: {e}")))?;

        if let Some(existing) = existing {
            self.app_cache.remove(&existing.id).await;
            self.app_cache.remove(&existing.key).await;
        }
        self.cache_app(&app).await;

        Ok(())
    }

    pub async fn delete_app(&self, app_id: &str) -> Result<()> {
        let existing = self.find_by_id_uncached(app_id).await?;

        let deleted: Option<StoredApp> = self
            .db
            .delete(self.record_resource(app_id))
            .await
            .map_err(|e| Error::Internal(format!("Failed to delete app from SurrealDB: {e}")))?;

        if deleted.is_none() && existing.is_none() {
            return Err(Error::InvalidAppKey);
        }

        if let Some(app) = existing {
            self.app_cache.remove(&app.id).await;
            self.app_cache.remove(&app.key).await;
        } else {
            self.app_cache.remove(app_id).await;
        }

        Ok(())
    }

    pub async fn get_apps(&self) -> Result<Vec<App>> {
        let records: Vec<SurrealAppRecord> = self
            .db
            .select(self.config.table_name.clone())
            .await
            .map_err(|e| {
            Error::Internal(format!("Failed to fetch apps from SurrealDB: {e}"))
        })?;

        let apps = records
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<App>>>()?;

        for app in &apps {
            self.cache_app(app).await;
        }

        Ok(apps)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredApp {
    key: String,
    payload: String,
}

impl From<&App> for StoredApp {
    fn from(app: &App) -> Self {
        Self {
            key: app.key.clone(),
            payload: sonic_rs::to_string(app).expect("App serialization must succeed"),
        }
    }
}

#[derive(Debug, Deserialize, SurrealValue)]
struct SurrealAppRecord {
    id: RecordId,
    #[serde(flatten)]
    app: StoredApp,
}

impl TryFrom<SurrealAppRecord> for App {
    type Error = Error;

    fn try_from(record: SurrealAppRecord) -> Result<Self> {
        let mut app: App = sonic_rs::from_str(&record.app.payload).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode Sockudo app payload from SurrealDB: {e}"
            ))
        })?;
        app.id = match record.id.key {
            RecordIdKey::String(id) => id,
            key => {
                return Err(Error::Internal(format!(
                    "Unsupported SurrealDB app record key type: {:?}",
                    key
                )));
            }
        };
        app.key = record.app.key;
        Ok(app)
    }
}

#[async_trait]
impl AppManager for SurrealDbAppManager {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn create_app(&self, config: App) -> Result<()> {
        self.create_app(config).await
    }

    async fn update_app(&self, config: App) -> Result<()> {
        self.update_app(config).await
    }

    async fn delete_app(&self, app_id: &str) -> Result<()> {
        self.delete_app(app_id).await
    }

    async fn get_apps(&self) -> Result<Vec<App>> {
        self.get_apps().await
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        self.find_by_key(key).await
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        self.find_by_id(app_id).await
    }

    async fn check_health(&self) -> Result<()> {
        self.db
            .health()
            .await
            .map_err(|e| Error::Internal(format!("App manager SurrealDB connection failed: {e}")))
    }
}

impl Clone for SurrealDbAppManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            db: self.db.clone(),
            app_cache: self.app_cache.clone(),
        }
    }
}
