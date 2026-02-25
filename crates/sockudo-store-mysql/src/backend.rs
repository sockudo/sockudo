use async_trait::async_trait;
use moka::future::Cache;
use sockudo_core::app_store::AppStore;
use sockudo_types::app::App;
use sockudo_types::delta::ChannelDeltaConfig;
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use std::time::Duration;
use thiserror::Error as ThisError;
use tracing::{debug, info};

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("{0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct DatabaseConnection {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub table_name: String,
    pub connection_pool_size: u32,
    pub pool_min: Option<u32>,
    pub pool_max: Option<u32>,
    pub cache_ttl: u64,
    pub cache_max_capacity: u64,
}

#[derive(Debug, Clone)]
pub struct DatabasePooling {
    pub enabled: bool,
    pub min: u32,
    pub max: u32,
}

impl Default for DatabasePooling {
    fn default() -> Self {
        Self {
            enabled: true,
            min: 5,
            max: 10,
        }
    }
}

pub struct MySqlAppStore {
    config: DatabaseConnection,
    pool: MySqlPool,
    app_cache: Cache<String, App>,
}

impl MySqlAppStore {
    pub async fn new(config: DatabaseConnection, pooling: DatabasePooling) -> Result<Self> {
        let password = urlencoding::encode(&config.password);
        let connection_string = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.username, password, config.host, config.port, config.database
        );

        let mut opts = MySqlPoolOptions::new();
        opts = if pooling.enabled {
            let min = config.pool_min.unwrap_or(pooling.min);
            let max = config.pool_max.unwrap_or(pooling.max);
            opts.min_connections(min).max_connections(max)
        } else {
            opts.max_connections(config.connection_pool_size)
        };

        let pool = opts
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(180))
            .connect(&connection_string)
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to MySQL: {e}")))?;

        let app_cache = Cache::builder()
            .time_to_live(Duration::from_secs(config.cache_ttl))
            .max_capacity(config.cache_max_capacity)
            .build();

        let store = Self {
            config,
            pool,
            app_cache,
        };
        store.ensure_table_exists().await?;
        Ok(store)
    }

    async fn ensure_table_exists(&self) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS `{}` (
                id VARCHAR(255) PRIMARY KEY,
                `key` VARCHAR(255) UNIQUE NOT NULL,
                secret VARCHAR(255) NOT NULL,
                max_connections INT UNSIGNED NOT NULL,
                enable_client_messages BOOLEAN NOT NULL DEFAULT FALSE,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                max_backend_events_per_second INT UNSIGNED NULL,
                max_client_events_per_second INT UNSIGNED NOT NULL,
                max_read_requests_per_second INT UNSIGNED NULL,
                max_presence_members_per_channel INT UNSIGNED NULL,
                max_presence_member_size_in_kb INT UNSIGNED NULL,
                max_channel_name_length INT UNSIGNED NULL,
                max_event_channels_at_once INT UNSIGNED NULL,
                max_event_name_length INT UNSIGNED NULL,
                max_event_payload_in_kb INT UNSIGNED NULL,
                max_event_batch_size INT UNSIGNED NULL,
                enable_user_authentication BOOLEAN NULL,
                enable_watchlist_events BOOLEAN NULL,
                webhooks JSON NULL,
                allowed_origins JSON NULL,
                channel_delta_compression JSON NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.config.table_name
        );

        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create MySQL table: {e}")))?;

        Ok(())
    }

    fn row_to_app(&self, row: &MySqlRow) -> Result<App> {
        let webhooks_json: Option<String> = row
            .try_get("webhooks")
            .map_err(|e| Error::Internal(format!("Failed to decode webhooks column: {e}")))?;
        let origins_json: Option<String> = row.try_get("allowed_origins").map_err(|e| {
            Error::Internal(format!("Failed to decode allowed_origins column: {e}"))
        })?;
        let channel_delta_json: Option<String> =
            row.try_get("channel_delta_compression").map_err(|e| {
                Error::Internal(format!(
                    "Failed to decode channel_delta_compression column: {e}"
                ))
            })?;

        let webhooks = webhooks_json
            .as_deref()
            .map(sonic_rs::from_str)
            .transpose()
            .map_err(|e| Error::Internal(format!("Invalid webhooks json: {e}")))?;

        let allowed_origins = origins_json
            .as_deref()
            .map(sonic_rs::from_str)
            .transpose()
            .map_err(|e| Error::Internal(format!("Invalid allowed_origins json: {e}")))?;

        let channel_delta_compression: Option<ahash::AHashMap<String, ChannelDeltaConfig>> =
            channel_delta_json
                .as_deref()
                .map(sonic_rs::from_str)
                .transpose()
                .map_err(|e| {
                    Error::Internal(format!("Invalid channel_delta_compression json: {e}"))
                })?;

        Ok(App {
            id: row
                .try_get("id")
                .map_err(|e| Error::Internal(format!("Missing id: {e}")))?,
            key: row
                .try_get("key")
                .map_err(|e| Error::Internal(format!("Missing key: {e}")))?,
            secret: row
                .try_get("secret")
                .map_err(|e| Error::Internal(format!("Missing secret: {e}")))?,
            max_connections: row
                .try_get("max_connections")
                .map_err(|e| Error::Internal(format!("Missing max_connections: {e}")))?,
            enable_client_messages: row
                .try_get("enable_client_messages")
                .map_err(|e| Error::Internal(format!("Missing enable_client_messages: {e}")))?,
            enabled: row
                .try_get("enabled")
                .map_err(|e| Error::Internal(format!("Missing enabled: {e}")))?,
            max_backend_events_per_second: row.try_get("max_backend_events_per_second").map_err(
                |e| Error::Internal(format!("Invalid max_backend_events_per_second: {e}")),
            )?,
            max_client_events_per_second: row.try_get("max_client_events_per_second").map_err(
                |e| Error::Internal(format!("Missing max_client_events_per_second: {e}")),
            )?,
            max_read_requests_per_second: row.try_get("max_read_requests_per_second").map_err(
                |e| Error::Internal(format!("Invalid max_read_requests_per_second: {e}")),
            )?,
            max_presence_members_per_channel: row
                .try_get("max_presence_members_per_channel")
                .map_err(|e| {
                    Error::Internal(format!("Invalid max_presence_members_per_channel: {e}"))
                })?,
            max_presence_member_size_in_kb: row.try_get("max_presence_member_size_in_kb").map_err(
                |e| Error::Internal(format!("Invalid max_presence_member_size_in_kb: {e}")),
            )?,
            max_channel_name_length: row
                .try_get("max_channel_name_length")
                .map_err(|e| Error::Internal(format!("Invalid max_channel_name_length: {e}")))?,
            max_event_channels_at_once: row
                .try_get("max_event_channels_at_once")
                .map_err(|e| Error::Internal(format!("Invalid max_event_channels_at_once: {e}")))?,
            max_event_name_length: row
                .try_get("max_event_name_length")
                .map_err(|e| Error::Internal(format!("Invalid max_event_name_length: {e}")))?,
            max_event_payload_in_kb: row
                .try_get("max_event_payload_in_kb")
                .map_err(|e| Error::Internal(format!("Invalid max_event_payload_in_kb: {e}")))?,
            max_event_batch_size: row
                .try_get("max_event_batch_size")
                .map_err(|e| Error::Internal(format!("Invalid max_event_batch_size: {e}")))?,
            enable_user_authentication: row
                .try_get("enable_user_authentication")
                .map_err(|e| Error::Internal(format!("Invalid enable_user_authentication: {e}")))?,
            webhooks,
            enable_watchlist_events: row
                .try_get("enable_watchlist_events")
                .map_err(|e| Error::Internal(format!("Invalid enable_watchlist_events: {e}")))?,
            allowed_origins,
            channel_delta_compression,
        })
    }

    async fn get_app_by_column(&self, column_name: &str, value: &str) -> Result<Option<App>> {
        let query = format!(
            r#"SELECT
                id, `key`, secret, max_connections,
                enable_client_messages, enabled,
                max_backend_events_per_second,
                max_client_events_per_second,
                max_read_requests_per_second,
                max_presence_members_per_channel,
                max_presence_member_size_in_kb,
                max_channel_name_length,
                max_event_channels_at_once,
                max_event_name_length,
                max_event_payload_in_kb,
                max_event_batch_size,
                enable_user_authentication,
                enable_watchlist_events,
                webhooks,
                allowed_origins,
                channel_delta_compression
               FROM `{}`
               WHERE {} = ?"#,
            self.config.table_name, column_name
        );

        let row = sqlx::query(&query)
            .bind(value)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch app by {column_name}: {e}")))?;

        row.as_ref().map(|r| self.row_to_app(r)).transpose()
    }

    async fn upsert_app(&self, app: App) -> Result<()> {
        let webhooks_json = app
            .webhooks
            .as_ref()
            .map(sonic_rs::to_string)
            .transpose()
            .map_err(|e| Error::Internal(format!("Failed to serialize webhooks: {e}")))?;
        let origins_json = app
            .allowed_origins
            .as_ref()
            .map(sonic_rs::to_string)
            .transpose()
            .map_err(|e| Error::Internal(format!("Failed to serialize allowed_origins: {e}")))?;
        let channel_delta_json = app
            .channel_delta_compression
            .as_ref()
            .map(sonic_rs::to_string)
            .transpose()
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to serialize channel_delta_compression: {e}"
                ))
            })?;

        let query = format!(
            r#"INSERT INTO `{}` (
                id, `key`, secret, max_connections,
                enable_client_messages, enabled,
                max_backend_events_per_second,
                max_client_events_per_second,
                max_read_requests_per_second,
                max_presence_members_per_channel,
                max_presence_member_size_in_kb,
                max_channel_name_length,
                max_event_channels_at_once,
                max_event_name_length,
                max_event_payload_in_kb,
                max_event_batch_size,
                enable_user_authentication,
                enable_watchlist_events,
                webhooks,
                allowed_origins,
                channel_delta_compression
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                `key` = VALUES(`key`),
                secret = VALUES(secret),
                max_connections = VALUES(max_connections),
                enable_client_messages = VALUES(enable_client_messages),
                enabled = VALUES(enabled),
                max_backend_events_per_second = VALUES(max_backend_events_per_second),
                max_client_events_per_second = VALUES(max_client_events_per_second),
                max_read_requests_per_second = VALUES(max_read_requests_per_second),
                max_presence_members_per_channel = VALUES(max_presence_members_per_channel),
                max_presence_member_size_in_kb = VALUES(max_presence_member_size_in_kb),
                max_channel_name_length = VALUES(max_channel_name_length),
                max_event_channels_at_once = VALUES(max_event_channels_at_once),
                max_event_name_length = VALUES(max_event_name_length),
                max_event_payload_in_kb = VALUES(max_event_payload_in_kb),
                max_event_batch_size = VALUES(max_event_batch_size),
                enable_user_authentication = VALUES(enable_user_authentication),
                enable_watchlist_events = VALUES(enable_watchlist_events),
                webhooks = VALUES(webhooks),
                allowed_origins = VALUES(allowed_origins),
                channel_delta_compression = VALUES(channel_delta_compression)"#,
            self.config.table_name
        );

        sqlx::query(&query)
            .bind(&app.id)
            .bind(&app.key)
            .bind(&app.secret)
            .bind(app.max_connections)
            .bind(app.enable_client_messages)
            .bind(app.enabled)
            .bind(app.max_backend_events_per_second)
            .bind(app.max_client_events_per_second)
            .bind(app.max_read_requests_per_second)
            .bind(app.max_presence_members_per_channel)
            .bind(app.max_presence_member_size_in_kb)
            .bind(app.max_channel_name_length)
            .bind(app.max_event_channels_at_once)
            .bind(app.max_event_name_length)
            .bind(app.max_event_payload_in_kb)
            .bind(app.max_event_batch_size)
            .bind(app.enable_user_authentication)
            .bind(app.enable_watchlist_events)
            .bind(webhooks_json)
            .bind(origins_json)
            .bind(channel_delta_json)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to upsert app '{}': {e}", app.id)))?;

        self.app_cache.insert(app.id.clone(), app).await;
        Ok(())
    }
}

#[async_trait]
impl AppStore for MySqlAppStore {
    type Error = Error;

    async fn init(&self) -> Result<()> {
        self.ensure_table_exists().await
    }

    async fn create_app(&self, config: App) -> Result<()> {
        self.upsert_app(config).await
    }

    async fn update_app(&self, config: App) -> Result<()> {
        self.upsert_app(config).await
    }

    async fn delete_app(&self, app_id: &str) -> Result<()> {
        let query = format!("DELETE FROM `{}` WHERE id = ?", self.config.table_name);
        sqlx::query(&query)
            .bind(app_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to delete app '{app_id}': {e}")))?;
        self.app_cache.remove(app_id).await;
        Ok(())
    }

    async fn get_apps(&self) -> Result<Vec<App>> {
        let query = format!(
            "SELECT * FROM `{}` ORDER BY updated_at DESC, created_at DESC",
            self.config.table_name
        );
        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to list apps: {e}")))?;
        rows.iter().map(|r| self.row_to_app(r)).collect()
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        if let Some(app) = self
            .app_cache
            .iter()
            .find(|(_, app)| app.key == key)
            .map(|(_, app)| app)
        {
            return Ok(Some(app));
        }
        let app = self.get_app_by_column("`key`", key).await?;
        if let Some(ref value) = app {
            self.app_cache.insert(value.id.clone(), value.clone()).await;
        }
        Ok(app)
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        if let Some(app) = self.app_cache.get(app_id).await {
            return Ok(Some(app));
        }
        let app = self.get_app_by_column("id", app_id).await?;
        if let Some(ref value) = app {
            self.app_cache.insert(value.id.clone(), value.clone()).await;
        }
        Ok(app)
    }

    async fn check_health(&self) -> Result<()> {
        let row = sqlx::query("SELECT 1 AS ok")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("MySQL health check failed: {e}")))?;
        let ok: i32 = row
            .try_get("ok")
            .map_err(|e| Error::Internal(format!("Invalid MySQL health response: {e}")))?;
        if ok == 1 {
            debug!("MySQL app store healthy");
            Ok(())
        } else {
            Err(Error::Internal(
                "MySQL health check returned non-1".to_string(),
            ))
        }
    }
}

pub async fn new_mysql_store(
    config: DatabaseConnection,
    pooling: DatabasePooling,
) -> Result<MySqlAppStore> {
    info!("Creating MySQL app store backend");
    MySqlAppStore::new(config, pooling).await
}

pub async fn new_mysql_store_with_default_pooling(
    config: DatabaseConnection,
) -> Result<MySqlAppStore> {
    MySqlAppStore::new(config, DatabasePooling::default()).await
}
