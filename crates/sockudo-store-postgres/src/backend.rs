use async_trait::async_trait;
use moka::future::Cache;
use sockudo_core::app_store::AppStore;
use sockudo_types::app::App;
use sockudo_types::delta::ChannelDeltaConfig;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
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

pub struct PgSqlAppStore {
    config: DatabaseConnection,
    pool: PgPool,
    app_cache: Cache<String, App>,
}

impl PgSqlAppStore {
    const SELECT_COLUMNS: &'static str = r#"id, key, secret, max_connections,
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
                webhooks::text AS webhooks,
                allowed_origins::text AS allowed_origins,
                channel_delta_compression::text AS channel_delta_compression"#;

    pub async fn new(config: DatabaseConnection, pooling: DatabasePooling) -> Result<Self> {
        let password = urlencoding::encode(&config.password);
        let connection_string = format!(
            "postgresql://{}:{}@{}:{}/{}",
            config.username, password, config.host, config.port, config.database
        );

        let mut opts = PgPoolOptions::new();
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
            .map_err(|e| Error::Internal(format!("Failed to connect to PostgreSQL: {e}")))?;

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
            CREATE TABLE IF NOT EXISTS {} (
                id VARCHAR(255) PRIMARY KEY,
                key VARCHAR(255) UNIQUE NOT NULL,
                secret VARCHAR(255) NOT NULL,
                max_connections INTEGER NOT NULL,
                enable_client_messages BOOLEAN NOT NULL DEFAULT FALSE,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                max_backend_events_per_second INTEGER,
                max_client_events_per_second INTEGER NOT NULL,
                max_read_requests_per_second INTEGER,
                max_presence_members_per_channel INTEGER,
                max_presence_member_size_in_kb INTEGER,
                max_channel_name_length INTEGER,
                max_event_channels_at_once INTEGER,
                max_event_name_length INTEGER,
                max_event_payload_in_kb INTEGER,
                max_event_batch_size INTEGER,
                enable_user_authentication BOOLEAN,
                enable_watchlist_events BOOLEAN,
                webhooks JSONB,
                allowed_origins JSONB,
                channel_delta_compression JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
            self.config.table_name
        );

        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create PostgreSQL table: {e}")))?;
        Ok(())
    }

    fn row_to_app(&self, row: &PgRow) -> Result<App> {
        let webhooks_json: Option<String> = row
            .try_get("webhooks")
            .map_err(|e| Error::Internal(format!("Failed to decode webhooks: {e}")))?;
        let origins_json: Option<String> = row
            .try_get("allowed_origins")
            .map_err(|e| Error::Internal(format!("Failed to decode allowed_origins: {e}")))?;
        let channel_delta_json: Option<String> =
            row.try_get("channel_delta_compression").map_err(|e| {
                Error::Internal(format!("Failed to decode channel_delta_compression: {e}"))
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

        let channel_delta_compression = channel_delta_json
            .as_deref()
            .map(sonic_rs::from_str::<ahash::AHashMap<String, ChannelDeltaConfig>>)
            .transpose()
            .map_err(|e| Error::Internal(format!("Invalid channel_delta_compression json: {e}")))?;

        let parse_u32 = |field: &str| -> Result<u32> {
            let value: i32 = row
                .try_get(field)
                .map_err(|e| Error::Internal(format!("Invalid {field}: {e}")))?;
            u32::try_from(value)
                .map_err(|_| Error::Internal(format!("Invalid negative {field}: {value}")))
        };

        let parse_opt_u32 = |field: &str| -> Result<Option<u32>> {
            let value: Option<i32> = row
                .try_get(field)
                .map_err(|e| Error::Internal(format!("Invalid {field}: {e}")))?;
            value
                .map(|v| {
                    u32::try_from(v)
                        .map_err(|_| Error::Internal(format!("Invalid negative {field}: {v}")))
                })
                .transpose()
        };

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
            max_connections: parse_u32("max_connections")?,
            enable_client_messages: row
                .try_get("enable_client_messages")
                .map_err(|e| Error::Internal(format!("Missing enable_client_messages: {e}")))?,
            enabled: row
                .try_get("enabled")
                .map_err(|e| Error::Internal(format!("Missing enabled: {e}")))?,
            max_backend_events_per_second: parse_opt_u32("max_backend_events_per_second")?,
            max_client_events_per_second: parse_u32("max_client_events_per_second")?,
            max_read_requests_per_second: parse_opt_u32("max_read_requests_per_second")?,
            max_presence_members_per_channel: parse_opt_u32("max_presence_members_per_channel")?,
            max_presence_member_size_in_kb: parse_opt_u32("max_presence_member_size_in_kb")?,
            max_channel_name_length: parse_opt_u32("max_channel_name_length")?,
            max_event_channels_at_once: parse_opt_u32("max_event_channels_at_once")?,
            max_event_name_length: parse_opt_u32("max_event_name_length")?,
            max_event_payload_in_kb: parse_opt_u32("max_event_payload_in_kb")?,
            max_event_batch_size: parse_opt_u32("max_event_batch_size")?,
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
            r#"INSERT INTO {} (
                id, key, secret, max_connections,
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
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,CAST($19 AS JSONB),CAST($20 AS JSONB),CAST($21 AS JSONB)
            )
            ON CONFLICT (id) DO UPDATE SET
                key = EXCLUDED.key,
                secret = EXCLUDED.secret,
                max_connections = EXCLUDED.max_connections,
                enable_client_messages = EXCLUDED.enable_client_messages,
                enabled = EXCLUDED.enabled,
                max_backend_events_per_second = EXCLUDED.max_backend_events_per_second,
                max_client_events_per_second = EXCLUDED.max_client_events_per_second,
                max_read_requests_per_second = EXCLUDED.max_read_requests_per_second,
                max_presence_members_per_channel = EXCLUDED.max_presence_members_per_channel,
                max_presence_member_size_in_kb = EXCLUDED.max_presence_member_size_in_kb,
                max_channel_name_length = EXCLUDED.max_channel_name_length,
                max_event_channels_at_once = EXCLUDED.max_event_channels_at_once,
                max_event_name_length = EXCLUDED.max_event_name_length,
                max_event_payload_in_kb = EXCLUDED.max_event_payload_in_kb,
                max_event_batch_size = EXCLUDED.max_event_batch_size,
                enable_user_authentication = EXCLUDED.enable_user_authentication,
                enable_watchlist_events = EXCLUDED.enable_watchlist_events,
                webhooks = EXCLUDED.webhooks,
                allowed_origins = EXCLUDED.allowed_origins,
                channel_delta_compression = EXCLUDED.channel_delta_compression,
                updated_at = CURRENT_TIMESTAMP"#,
            self.config.table_name
        );

        sqlx::query(&query)
            .bind(&app.id)
            .bind(&app.key)
            .bind(&app.secret)
            .bind(app.max_connections as i32)
            .bind(app.enable_client_messages)
            .bind(app.enabled)
            .bind(app.max_backend_events_per_second.map(|v| v as i32))
            .bind(app.max_client_events_per_second as i32)
            .bind(app.max_read_requests_per_second.map(|v| v as i32))
            .bind(app.max_presence_members_per_channel.map(|v| v as i32))
            .bind(app.max_presence_member_size_in_kb.map(|v| v as i32))
            .bind(app.max_channel_name_length.map(|v| v as i32))
            .bind(app.max_event_channels_at_once.map(|v| v as i32))
            .bind(app.max_event_name_length.map(|v| v as i32))
            .bind(app.max_event_payload_in_kb.map(|v| v as i32))
            .bind(app.max_event_batch_size.map(|v| v as i32))
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
impl AppStore for PgSqlAppStore {
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
        let query = format!("DELETE FROM {} WHERE id = $1", self.config.table_name);
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
            "SELECT {} FROM {} ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST",
            Self::SELECT_COLUMNS,
            self.config.table_name,
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

        let query = format!(
            "SELECT {} FROM {} WHERE key = $1",
            Self::SELECT_COLUMNS,
            self.config.table_name
        );
        let row = sqlx::query(&query)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch app by key: {e}")))?;
        let app = row.as_ref().map(|r| self.row_to_app(r)).transpose()?;
        if let Some(ref value) = app {
            self.app_cache.insert(value.id.clone(), value.clone()).await;
        }
        Ok(app)
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        if let Some(app) = self.app_cache.get(app_id).await {
            return Ok(Some(app));
        }

        let query = format!(
            "SELECT {} FROM {} WHERE id = $1",
            Self::SELECT_COLUMNS,
            self.config.table_name
        );
        let row = sqlx::query(&query)
            .bind(app_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch app by id: {e}")))?;
        let app = row.as_ref().map(|r| self.row_to_app(r)).transpose()?;
        if let Some(ref value) = app {
            self.app_cache.insert(value.id.clone(), value.clone()).await;
        }
        Ok(app)
    }

    async fn check_health(&self) -> Result<()> {
        let row = sqlx::query("SELECT 1 AS ok")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("PostgreSQL health check failed: {e}")))?;
        let ok: i32 = row
            .try_get("ok")
            .map_err(|e| Error::Internal(format!("Invalid PostgreSQL health response: {e}")))?;
        if ok == 1 {
            debug!("PostgreSQL app store healthy");
            Ok(())
        } else {
            Err(Error::Internal(
                "PostgreSQL health check returned non-1".to_string(),
            ))
        }
    }
}

pub async fn new_postgres_store(
    config: DatabaseConnection,
    pooling: DatabasePooling,
) -> Result<PgSqlAppStore> {
    info!("Creating PostgreSQL app store backend");
    PgSqlAppStore::new(config, pooling).await
}

pub async fn new_postgres_store_with_default_pooling(
    config: DatabaseConnection,
) -> Result<PgSqlAppStore> {
    PgSqlAppStore::new(config, DatabasePooling::default()).await
}
