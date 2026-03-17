use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use moka::future::Cache;
use sockudo_core::app::{App, AppManager};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{DatabaseConnection, DatabasePooling};
use sockudo_core::token::Token;
use sockudo_core::webhook_types::Webhook;
use sockudo_core::websocket::SocketId;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// PostgreSQL-based implementation of the AppManager
pub struct PgSQLAppManager {
    config: DatabaseConnection,
    pool: PgPool,
    app_cache: Cache<String, App>,
}

impl PgSQLAppManager {
    /// Create a new PostgreSQL-based AppManager with the provided configuration
    pub async fn new(config: DatabaseConnection, pooling: DatabasePooling) -> Result<Self> {
        info!(
            "Initializing PostgreSQL AppManager with database {}",
            config.database
        );

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

        let manager = Self {
            config,
            pool,
            app_cache,
        };

        manager.ensure_table_exists().await?;

        Ok(manager)
    }

    /// Create the applications table if it doesn't exist
    async fn ensure_table_exists(&self) -> Result<()> {
        let create_table_query = format!(
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        "#,
            self.config.table_name
        );

        sqlx::query(&create_table_query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create PostgreSQL table: {e}")))?;

        let columns_to_add = vec![
            ("allowed_origins", "JSONB"),
            ("webhooks", "JSONB"),
            ("channel_delta_compression", "JSONB"),
        ];

        for (column_name, column_type) in columns_to_add {
            let add_column_query = format!(
                r#"ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}"#,
                self.config.table_name, column_name, column_type
            );

            sqlx::query(&add_column_query)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to add column '{}' to table '{}': {}",
                        column_name, self.config.table_name, e
                    ))
                })?;
        }

        info!("Ensured table '{}' exists", self.config.table_name);
        Ok(())
    }

    /// Get an app by ID from cache or database
    pub async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        if let Some(app) = self.app_cache.get(app_id).await {
            return Ok(Some(app));
        }

        debug!("Cache miss for app {}, fetching from database", app_id);

        let query = format!(
            r#"SELECT
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
            FROM {} WHERE id = $1"#,
            self.config.table_name
        );

        let app_result = sqlx::query_as::<_, AppRow>(&query)
            .bind(app_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                error!("Database error fetching app {}: {}", app_id, e);
                Error::Internal(format!("Failed to fetch app from PostgreSQL: {e}"))
            })?;

        if let Some(app_row) = app_result {
            let app = app_row.into_app();
            self.app_cache.insert(app_id.to_string(), app.clone()).await;
            Ok(Some(app))
        } else {
            Ok(None)
        }
    }

    /// Get an app by key from cache or database
    pub async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        debug!("Fetching app by key {} from database", key);

        let query = format!(
            r#"SELECT
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
            FROM {} WHERE key = $1"#,
            self.config.table_name
        );

        let app_result = sqlx::query_as::<_, AppRow>(&query)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                error!("Database error fetching app by key {}: {}", key, e);
                Error::Internal(format!("Failed to fetch app from PostgreSQL: {e}"))
            })?;

        if let Some(app_row) = app_result {
            let app = app_row.into_app();
            self.app_cache.insert(app.id.clone(), app.clone()).await;
            Ok(Some(app))
        } else {
            Ok(None)
        }
    }

    /// Register a new app in the database
    pub async fn create_app(&self, app: App) -> Result<()> {
        info!("Registering new app: {}", app.id);

        let query = format!(
            r#"INSERT INTO {} (
                id, key, secret, max_connections, enable_client_messages, enabled,
                max_backend_events_per_second, max_client_events_per_second,
                max_read_requests_per_second, max_presence_members_per_channel,
                max_presence_member_size_in_kb, max_channel_name_length,
                max_event_channels_at_once, max_event_name_length,
                max_event_payload_in_kb, max_event_batch_size, enable_user_authentication,
                enable_watchlist_events, webhooks, allowed_origins,
                channel_delta_compression
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)"#,
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
            .bind(sqlx::types::Json(&app.webhooks))
            .bind(sqlx::types::Json(&app.allowed_origins))
            .bind(sqlx::types::Json(&app.channel_delta_compression))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Database error registering app {}: {}", app.id, e);
                Error::Internal(format!("Failed to insert app into PostgreSQL: {e}"))
            })?;

        self.app_cache.insert(app.id.clone(), app).await;

        Ok(())
    }

    /// Update an existing app in the database
    pub async fn update_app(&self, app: App) -> Result<()> {
        info!("Updating app: {}", app.id);

        let query = format!(
            r#"UPDATE {} SET
                key = $1, secret = $2, max_connections = $3, enable_client_messages = $4, enabled = $5,
                max_backend_events_per_second = $6, max_client_events_per_second = $7,
                max_read_requests_per_second = $8, max_presence_members_per_channel = $9,
                max_presence_member_size_in_kb = $10, max_channel_name_length = $11,
                max_event_channels_at_once = $12, max_event_name_length = $13,
                max_event_payload_in_kb = $14, max_event_batch_size = $15,
                enable_user_authentication = $16, enable_watchlist_events = $17,
                webhooks = $18, allowed_origins = $19,
                channel_delta_compression = $20,
                updated_at = CURRENT_TIMESTAMP
                WHERE id = $21"#,
            self.config.table_name
        );

        let result = sqlx::query(&query)
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
            .bind(sqlx::types::Json(&app.webhooks))
            .bind(sqlx::types::Json(&app.allowed_origins))
            .bind(sqlx::types::Json(&app.channel_delta_compression))
            .bind(&app.id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Database error updating app {}: {}", app.id, e);
                Error::Internal(format!("Failed to update app in PostgreSQL: {e}"))
            })?;

        if result.rows_affected() == 0 {
            return Err(Error::InvalidAppKey);
        }

        self.app_cache.insert(app.id.clone(), app).await;

        Ok(())
    }

    /// Remove an app from the database
    pub async fn delete_app(&self, app_id: &str) -> Result<()> {
        info!("Removing app: {}", app_id);

        let query = format!("DELETE FROM {} WHERE id = $1", self.config.table_name);

        let result = sqlx::query(&query)
            .bind(app_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Database error removing app {}: {}", app_id, e);
                Error::Internal(format!("Failed to delete app from PostgreSQL: {e}"))
            })?;

        if result.rows_affected() == 0 {
            return Err(Error::InvalidAppKey);
        }

        self.app_cache.remove(app_id).await;

        Ok(())
    }

    /// Get all apps from the database
    pub async fn get_apps(&self) -> Result<Vec<App>> {
        info!("Fetching all apps from database");

        let query = format!(
            r#"SELECT
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
        FROM {}"#,
            self.config.table_name
        );

        let app_rows = sqlx::query_as::<_, AppRow>(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                error!("Database error fetching all apps: {}", e);
                Error::Internal(format!("Failed to fetch apps from PostgreSQL: {e}"))
            })?;

        warn!("Fetched {} app rows from database.", app_rows.len());

        let apps = stream::iter(app_rows)
            .map(|row| async {
                let app = row.into_app();
                self.app_cache.insert(app.id.clone(), app.clone()).await;
                app
            })
            .buffer_unordered(self.config.connection_pool_size as usize)
            .collect::<Vec<App>>()
            .await;

        info!("Finished processing and caching {} apps.", apps.len());

        Ok(apps)
    }

    /// Validate if an app ID exists
    pub async fn validate_key(&self, app_id: &str) -> Result<bool> {
        Ok(self.find_by_id(app_id).await?.is_some())
    }

    /// Validate a signature against an app's secret
    pub async fn validate_signature(
        &self,
        app_id: &str,
        signature: &str,
        body: &str,
    ) -> Result<bool> {
        let app = self.find_by_id(app_id).await?.ok_or(Error::InvalidAppKey)?;

        let token = Token::new(app.key.clone(), app.secret.clone());
        let expected = token.sign(body);

        Ok(signature == expected)
    }

    /// Validate if a channel name is valid for an app
    pub async fn validate_channel_name(&self, app_id: &str, channel: &str) -> Result<()> {
        let app = self.find_by_id(app_id).await?.ok_or(Error::InvalidAppKey)?;

        let max_length = app.max_channel_name_length.unwrap_or(200);
        if channel.len() > max_length as usize {
            return Err(Error::InvalidChannelName(format!(
                "Channel name too long. Max length is {max_length}"
            )));
        }

        let valid_chars = regex::Regex::new(r"^[a-zA-Z0-9_\-=@,.;]+$").unwrap();
        if !valid_chars.is_match(channel) {
            return Err(Error::InvalidChannelName(
                "Channel name contains invalid characters".to_string(),
            ));
        }

        Ok(())
    }

    /// Check if client events are enabled for an app
    pub async fn can_handle_client_events(&self, app_key: &str) -> Result<bool> {
        Ok(self
            .find_by_key(app_key)
            .await?
            .map(|app| app.enable_client_messages)
            .unwrap_or(false))
    }

    /// Validate user authentication
    pub async fn validate_user_auth(&self, socket_id: &SocketId, auth: &str) -> Result<bool> {
        let parts: Vec<&str> = auth.split(':').collect();
        if parts.len() < 2 {
            return Err(Error::Auth("Invalid auth format".into()));
        }

        let app_key = parts[0];
        let signature = parts[1..].join(":");

        let app = self
            .find_by_key(app_key)
            .await?
            .ok_or(Error::InvalidAppKey)?;

        let string_to_sign = format!("{socket_id}::user::{signature}");

        let token = Token::new(app.key.clone(), app.secret.clone());

        Ok(token.verify(&string_to_sign, &signature))
    }
}

/// Row struct for SQLx query results
#[derive(sqlx::FromRow)]
struct AppRow {
    id: String,
    key: String,
    secret: String,
    max_connections: i32,
    enable_client_messages: bool,
    enabled: bool,
    max_backend_events_per_second: Option<i32>,
    max_client_events_per_second: i32,
    max_read_requests_per_second: Option<i32>,
    max_presence_members_per_channel: Option<i32>,
    max_presence_member_size_in_kb: Option<i32>,
    max_channel_name_length: Option<i32>,
    max_event_channels_at_once: Option<i32>,
    max_event_name_length: Option<i32>,
    max_event_payload_in_kb: Option<i32>,
    max_event_batch_size: Option<i32>,
    enable_user_authentication: Option<bool>,
    enable_watchlist_events: Option<bool>,
    #[sqlx(json(nullable))]
    webhooks: Option<Vec<Webhook>>,
    #[sqlx(json(nullable))]
    allowed_origins: Option<Vec<String>>,
    #[sqlx(json(nullable))]
    channel_delta_compression:
        Option<ahash::AHashMap<String, sockudo_core::delta_types::ChannelDeltaConfig>>,
}

impl AppRow {
    fn into_app(self) -> App {
        App {
            id: self.id,
            key: self.key,
            secret: self.secret,
            max_connections: self.max_connections as u32,
            enable_client_messages: self.enable_client_messages,
            enabled: self.enabled,
            max_backend_events_per_second: self.max_backend_events_per_second.map(|v| v as u32),
            max_client_events_per_second: self.max_client_events_per_second as u32,
            max_read_requests_per_second: self.max_read_requests_per_second.map(|v| v as u32),
            max_presence_members_per_channel: self
                .max_presence_members_per_channel
                .map(|v| v as u32),
            max_presence_member_size_in_kb: self.max_presence_member_size_in_kb.map(|v| v as u32),
            max_channel_name_length: self.max_channel_name_length.map(|v| v as u32),
            max_event_channels_at_once: self.max_event_channels_at_once.map(|v| v as u32),
            max_event_name_length: self.max_event_name_length.map(|v| v as u32),
            max_event_payload_in_kb: self.max_event_payload_in_kb.map(|v| v as u32),
            max_event_batch_size: self.max_event_batch_size.map(|v| v as u32),
            enable_user_authentication: self.enable_user_authentication,
            webhooks: self.webhooks,
            enable_watchlist_events: self.enable_watchlist_events,
            allowed_origins: self.allowed_origins,
            channel_delta_compression: self.channel_delta_compression,
        }
    }
}

#[async_trait]
impl AppManager for PgSQLAppManager {
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

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        self.find_by_id(app_id).await
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        self.find_by_key(key).await
    }

    async fn check_health(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!("App manager PostgreSQL connection failed: {e}"))
            })?;
        Ok(())
    }
}

impl Clone for PgSQLAppManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            pool: self.pool.clone(),
            app_cache: self.app_cache.clone(),
        }
    }
}
