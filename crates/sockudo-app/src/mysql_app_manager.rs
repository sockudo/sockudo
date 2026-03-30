use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use moka::future::Cache;
use sockudo_core::app::{
    App, AppConnectionRecoveryConfig, AppIdempotencyConfig, AppManager, AppPolicy,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{DatabaseConnection, DatabasePooling};
use sockudo_core::token::Token;
use sockudo_core::webhook_types::Webhook;
use sockudo_core::websocket::SocketId;
use sqlx::{MySqlPool, mysql::MySqlPoolOptions};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// MySQL-based implementation of the AppManager
pub struct MySQLAppManager {
    config: DatabaseConnection,
    pool: MySqlPool,
    app_cache: Cache<String, App>,
}

impl MySQLAppManager {
    /// Create a new MySQL-based AppManager with the provided configuration
    pub async fn new(config: DatabaseConnection, pooling: DatabasePooling) -> Result<Self> {
        info!(
            "{}",
            format!(
                "Initializing MySQL AppManager with database {}",
                config.database
            )
        );

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

        let manager = Self {
            config,
            pool,
            app_cache,
        };

        manager.ensure_table_exists().await?;

        Ok(manager)
    }

    /// Helper function to add a column if it doesn't exist
    async fn add_column_if_not_exists(&self, column_name: &str, column_type: &str) -> Result<()> {
        let check_query = format!(
            r#"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME = '{}'
               AND COLUMN_NAME = '{}'"#,
            self.config.table_name, column_name
        );

        let column_exists: Option<(String,)> = sqlx::query_as(&check_query)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to check if column '{}' exists: {}",
                    column_name, e
                ))
            })?;

        if column_exists.is_none() {
            let add_column_query = format!(
                r#"ALTER TABLE `{}` ADD COLUMN {} {}"#,
                self.config.table_name, column_name, column_type
            );

            if let Err(e) = sqlx::query(&add_column_query).execute(&self.pool).await {
                let mut is_duplicate_column_error = false;
                if let Some(db_err) = e.as_database_error()
                    && db_err.code() == Some(std::borrow::Cow::from("1060"))
                {
                    is_duplicate_column_error = true;
                }

                if is_duplicate_column_error {
                    warn!("Column '{}' already exists (race condition)", column_name);
                } else {
                    return Err(Error::Internal(format!(
                        "Failed to add column '{}': {}",
                        column_name, e
                    )));
                }
            } else {
                info!(
                    "Added column '{}' to table '{}'",
                    column_name, self.config.table_name
                );
            }
        }

        Ok(())
    }

    /// Create the applications table if it doesn't exist
    async fn ensure_table_exists(&self) -> Result<()> {
        const CREATE_TABLE_QUERY: &str = r#"
            CREATE TABLE IF NOT EXISTS `applications` (
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
                policy JSON NULL,
                webhooks JSON NULL,
                allowed_origins JSON NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        "#;

        let query = if self.config.table_name != "applications" {
            CREATE_TABLE_QUERY.replace("applications", &self.config.table_name)
        } else {
            CREATE_TABLE_QUERY.to_string()
        };

        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create MySQL table: {e}")))?;

        let columns_to_add = vec![
            ("allowed_origins", "JSON NULL"),
            ("enable_watchlist_events", "BOOLEAN NULL"),
            ("webhooks", "JSON NULL"),
            ("channel_delta_compression", "JSON NULL"),
            ("idempotency", "JSON NULL"),
            ("connection_recovery", "JSON NULL"),
            ("policy", "JSON NULL"),
        ];

        for (column_name, column_type) in columns_to_add {
            self.add_column_if_not_exists(column_name, column_type)
                .await?;
        }

        info!(
            "{}",
            format!("Ensured table '{}' exists", self.config.table_name)
        );
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
                policy,
                webhooks,
                allowed_origins,
                channel_delta_compression,
                idempotency,
                connection_recovery
            FROM `{}` WHERE id = ?"#,
            self.config.table_name
        );

        let app_result = sqlx::query_as::<_, AppRow>(&query)
            .bind(app_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                error!(
                    "{}",
                    format!("Database error fetching app {}: {}", app_id, e)
                );
                Error::Internal(format!("Failed to fetch app from MySQL: {e}"))
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
        if let Some(app) = self.app_cache.get(key).await {
            return Ok(Some(app));
        }

        debug!("Cache miss for app key {}, fetching from database", key);

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
                policy,
                webhooks,
                allowed_origins,
                channel_delta_compression,
                idempotency,
                connection_recovery
            FROM `{}` WHERE `key` = ?"#,
            self.config.table_name
        );

        let app_result = sqlx::query_as::<_, AppRow>(&query)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                error!(
                    "{}",
                    format!("Database error fetching app by key {}: {}", key, e)
                );
                Error::Internal(format!("Failed to fetch app from MySQL: {e}"))
            })?;

        if let Some(app_row) = app_result {
            let app = app_row.into_app();
            let app_id = app.id.clone();
            self.app_cache.insert(app_id, app.clone()).await;
            Ok(Some(app.clone()))
        } else {
            Ok(None)
        }
    }

    /// Register a new app in the database
    pub async fn create_app(&self, app: App) -> Result<()> {
        info!("{}", format!("Registering new app: {}", app.id));
        let policy = app.policy();

        let query = format!(
            r#"INSERT INTO `{}` (
                id, `key`, secret, max_connections, enable_client_messages, enabled,
                max_backend_events_per_second, max_client_events_per_second,
                max_read_requests_per_second, max_presence_members_per_channel,
                max_presence_member_size_in_kb, max_channel_name_length,
                max_event_channels_at_once, max_event_name_length,
                max_event_payload_in_kb, max_event_batch_size, enable_user_authentication,
                enable_watchlist_events, policy, webhooks, allowed_origins, channel_delta_compression,
                idempotency, connection_recovery
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            self.config.table_name
        );

        sqlx::query(&query)
            .bind(&app.id)
            .bind(&app.key)
            .bind(&app.secret)
            .bind(policy.limits.max_connections)
            .bind(policy.features.enable_client_messages)
            .bind(app.enabled)
            .bind(policy.limits.max_backend_events_per_second)
            .bind(policy.limits.max_client_events_per_second)
            .bind(policy.limits.max_read_requests_per_second)
            .bind(policy.limits.max_presence_members_per_channel)
            .bind(policy.limits.max_presence_member_size_in_kb)
            .bind(policy.limits.max_channel_name_length)
            .bind(policy.limits.max_event_channels_at_once)
            .bind(policy.limits.max_event_name_length)
            .bind(policy.limits.max_event_payload_in_kb)
            .bind(policy.limits.max_event_batch_size)
            .bind(policy.features.enable_user_authentication)
            .bind(policy.features.enable_watchlist_events)
            .bind(sqlx::types::Json(&policy))
            .bind(sqlx::types::Json(&policy.webhooks))
            .bind(sqlx::types::Json(&policy.channels.allowed_origins))
            .bind(sqlx::types::Json(
                &policy.channels.channel_delta_compression,
            ))
            .bind(sqlx::types::Json(&policy.idempotency))
            .bind(sqlx::types::Json(&policy.connection_recovery))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(
                    "{}",
                    format!("Database error registering app {}: {}", app.id, e)
                );
                Error::Internal(format!("Failed to insert app into MySQL: {e}"))
            })?;

        self.app_cache.insert(app.id.clone(), app).await;

        Ok(())
    }

    /// Update an existing app in the database
    pub async fn update_app(&self, app: App) -> Result<()> {
        info!("{}", format!("Updating app: {}", app.id));
        let policy = app.policy();

        let query = format!(
            r#"UPDATE `{}` SET
                `key` = ?, secret = ?, max_connections = ?, enable_client_messages = ?, enabled = ?,
                max_backend_events_per_second = ?, max_client_events_per_second = ?,
                max_read_requests_per_second = ?, max_presence_members_per_channel = ?,
                max_presence_member_size_in_kb = ?, max_channel_name_length = ?,
                max_event_channels_at_once = ?, max_event_name_length = ?,
                max_event_payload_in_kb = ?, max_event_batch_size = ?, enable_user_authentication = ?,
                enable_watchlist_events = ?, policy = ?, webhooks = ?, allowed_origins = ?,
                channel_delta_compression = ?,
                idempotency = ?,
                connection_recovery = ?
                WHERE id = ?"#,
            self.config.table_name
        );

        let result = sqlx::query(&query)
            .bind(&app.key)
            .bind(&app.secret)
            .bind(policy.limits.max_connections)
            .bind(policy.features.enable_client_messages)
            .bind(app.enabled)
            .bind(policy.limits.max_backend_events_per_second)
            .bind(policy.limits.max_client_events_per_second)
            .bind(policy.limits.max_read_requests_per_second)
            .bind(policy.limits.max_presence_members_per_channel)
            .bind(policy.limits.max_presence_member_size_in_kb)
            .bind(policy.limits.max_channel_name_length)
            .bind(policy.limits.max_event_channels_at_once)
            .bind(policy.limits.max_event_name_length)
            .bind(policy.limits.max_event_payload_in_kb)
            .bind(policy.limits.max_event_batch_size)
            .bind(policy.features.enable_user_authentication)
            .bind(policy.features.enable_watchlist_events)
            .bind(sqlx::types::Json(&policy))
            .bind(sqlx::types::Json(&policy.webhooks))
            .bind(sqlx::types::Json(&policy.channels.allowed_origins))
            .bind(sqlx::types::Json(
                &policy.channels.channel_delta_compression,
            ))
            .bind(sqlx::types::Json(&policy.idempotency))
            .bind(sqlx::types::Json(&policy.connection_recovery))
            .bind(&app.id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(
                    "{}",
                    format!("Database error updating app {}: {}", app.id, e)
                );
                Error::Internal(format!("Failed to update app in MySQL: {e}"))
            })?;

        if result.rows_affected() == 0 {
            return Err(Error::InvalidAppKey);
        }

        self.app_cache.insert(app.id.clone(), app).await;

        Ok(())
    }

    /// Remove an app from the database
    pub async fn delete_app(&self, app_id: &str) -> Result<()> {
        info!("{}", format!("Removing app: {}", app_id));

        let query = format!(r#"DELETE FROM `{}` WHERE id = ?"#, self.config.table_name);

        let result = sqlx::query(&query)
            .bind(app_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(
                    "{}",
                    format!("Database error removing app {}: {}", app_id, e)
                );
                Error::Internal(format!("Failed to delete app from MySQL: {e}"))
            })?;

        if result.rows_affected() == 0 {
            return Err(Error::InvalidAppKey);
        }

        self.app_cache.remove(app_id).await;

        Ok(())
    }

    /// Get all apps from the database
    pub async fn get_apps(&self) -> Result<Vec<App>> {
        info!("{}", "Fetching all apps from database");

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
            policy,
            webhooks,
            allowed_origins,
            channel_delta_compression,
            idempotency,
            connection_recovery
        FROM `{}`"#,
            self.config.table_name
        );

        let app_rows = sqlx::query_as::<_, AppRow>(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                error!("{}", format!("Database error fetching all apps: {}", e));
                Error::Internal(format!("Failed to fetch apps from MySQL: {e}"))
            })?;

        warn!(
            "{}",
            format!("Fetched {} app rows from database.", app_rows.len())
        );

        let apps = stream::iter(app_rows)
            .map(|row| async {
                let app = row.into_app();
                let app_arc = Arc::new(app.clone());
                self.app_cache.insert(app_arc.id.clone(), app.clone()).await;
                app
            })
            .buffer_unordered(self.config.connection_pool_size as usize)
            .collect::<Vec<App>>()
            .await;

        info!(
            "{}",
            format!("Finished processing and caching {} apps.", apps.len())
        );

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

        let max_length = app.max_channel_name_limit().unwrap_or(200);
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
            .map(|app| app.client_messages_enabled())
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
    max_connections: u32,
    enable_client_messages: bool,
    enabled: bool,
    max_backend_events_per_second: Option<u32>,
    max_client_events_per_second: u32,
    max_read_requests_per_second: Option<u32>,
    max_presence_members_per_channel: Option<u32>,
    max_presence_member_size_in_kb: Option<u32>,
    max_channel_name_length: Option<u32>,
    max_event_channels_at_once: Option<u32>,
    max_event_name_length: Option<u32>,
    max_event_payload_in_kb: Option<u32>,
    max_event_batch_size: Option<u32>,
    enable_user_authentication: Option<bool>,
    enable_watchlist_events: Option<bool>,
    #[sqlx(json(nullable))]
    policy: Option<AppPolicy>,
    #[sqlx(json(nullable))]
    webhooks: Option<Vec<Webhook>>,
    #[sqlx(json(nullable))]
    allowed_origins: Option<Vec<String>>,
    #[sqlx(json(nullable))]
    channel_delta_compression:
        Option<ahash::AHashMap<String, sockudo_core::delta_types::ChannelDeltaConfig>>,
    #[sqlx(json(nullable))]
    idempotency: Option<AppIdempotencyConfig>,
    #[sqlx(json(nullable))]
    connection_recovery: Option<AppConnectionRecoveryConfig>,
}

impl AppRow {
    fn into_app(self) -> App {
        if let Some(policy) = self.policy {
            return App::from_policy(self.id, self.key, self.secret, self.enabled, policy);
        }

        App::from_policy(
            self.id,
            self.key,
            self.secret,
            self.enabled,
            sockudo_core::app::AppPolicy {
                limits: sockudo_core::app::AppLimitsPolicy {
                    max_connections: self.max_connections,
                    max_backend_events_per_second: self.max_backend_events_per_second,
                    max_client_events_per_second: self.max_client_events_per_second,
                    max_read_requests_per_second: self.max_read_requests_per_second,
                    max_presence_members_per_channel: self.max_presence_members_per_channel,
                    max_presence_member_size_in_kb: self.max_presence_member_size_in_kb,
                    max_channel_name_length: self.max_channel_name_length,
                    max_event_channels_at_once: self.max_event_channels_at_once,
                    max_event_name_length: self.max_event_name_length,
                    max_event_payload_in_kb: self.max_event_payload_in_kb,
                    max_event_batch_size: self.max_event_batch_size,
                },
                features: sockudo_core::app::AppFeaturesPolicy {
                    enable_client_messages: self.enable_client_messages,
                    enable_user_authentication: self.enable_user_authentication,
                    enable_watchlist_events: self.enable_watchlist_events,
                },
                channels: sockudo_core::app::AppChannelsPolicy {
                    allowed_origins: self.allowed_origins,
                    channel_delta_compression: self.channel_delta_compression,
                    channel_namespaces: None,
                },
                webhooks: self.webhooks,
                idempotency: self.idempotency,
                connection_recovery: self.connection_recovery,
            },
        )
    }
}

#[async_trait]
impl AppManager for MySQLAppManager {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn create_app(&self, config: App) -> Result<()> {
        let config_clone = config.clone();
        self.create_app(config_clone).await
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
            .map_err(|e| Error::Internal(format!("App manager MySQL connection failed: {e}")))?;
        Ok(())
    }
}

impl Clone for MySQLAppManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            pool: self.pool.clone(),
            app_cache: self.app_cache.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_app(id: &str) -> App {
        App::from_policy(
            id.to_string(),
            format!("{id}_key"),
            format!("{id}_secret"),
            true,
            sockudo_core::app::AppPolicy {
                limits: sockudo_core::app::AppLimitsPolicy {
                    max_connections: 100,
                    max_backend_events_per_second: Some(1000),
                    max_client_events_per_second: 100,
                    max_read_requests_per_second: Some(1000),
                    max_presence_members_per_channel: Some(100),
                    max_presence_member_size_in_kb: Some(10),
                    max_channel_name_length: Some(200),
                    max_event_channels_at_once: Some(10),
                    max_event_name_length: Some(200),
                    max_event_payload_in_kb: Some(100),
                    max_event_batch_size: Some(10),
                },
                features: sockudo_core::app::AppFeaturesPolicy {
                    enable_client_messages: true,
                    enable_user_authentication: Some(true),
                    enable_watchlist_events: None,
                },
                channels: sockudo_core::app::AppChannelsPolicy::default(),
                webhooks: None,
                idempotency: None,
                connection_recovery: None,
            },
        )
    }

    async fn is_mysql_available() -> bool {
        let config = DatabaseConnection {
            username: "sockudo".to_string(),
            password: "sockudo123".to_string(),
            database: "sockudo".to_string(),
            table_name: "applications".to_string(),
            cache_ttl: 5,
            port: 13306,
            ..Default::default()
        };

        MySQLAppManager::new(config, DatabasePooling::default())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn test_mysql_app_manager() {
        // Skip test if MySQL is not available
        if !is_mysql_available().await {
            eprintln!("Skipping test: MySQL not available");
            return;
        }

        // Setup test database with proper credentials for Docker MySQL dev environment
        let config = DatabaseConnection {
            username: "sockudo".to_string(),
            password: "sockudo123".to_string(),
            database: "sockudo".to_string(),
            table_name: "applications".to_string(),
            cache_ttl: 5, // Short TTL for testing
            port: 13306,
            ..Default::default()
        };

        // Create manager
        let manager = MySQLAppManager::new(config, DatabasePooling::default())
            .await
            .unwrap();

        // Test registering an app
        let test_app = create_test_app("test1");
        manager.create_app(test_app.clone()).await.unwrap();

        // Test getting an app
        let app = manager.find_by_id("test1").await.unwrap().unwrap();
        assert_eq!(app.id, "test1");
        assert_eq!(app.key, "test1_key");

        // Test getting an app by key
        let app = manager.find_by_key("test1_key").await.unwrap().unwrap();
        assert_eq!(app.id, "test1");

        // Test updating an app
        let mut updated_app = test_app.clone();
        updated_app.policy.limits.max_connections = 200;
        manager.update_app(updated_app).await.unwrap();

        let app = manager.find_by_id("test1").await.unwrap().unwrap();
        assert_eq!(app.policy.limits.max_connections, 200);

        // Test cache expiration
        tokio::time::sleep(Duration::from_secs(6)).await;

        // Add another app
        let test_app2 = create_test_app("test2");
        manager.create_app(test_app2).await.unwrap();

        // Get all apps
        let apps = manager.get_apps().await.unwrap();
        assert_eq!(apps.len(), 2);

        // Test removing an app
        manager.delete_app("test1").await.unwrap();
        assert!(manager.find_by_id("test1").await.unwrap().is_none());

        // Cleanup
        manager.delete_app("test2").await.unwrap();
    }
}
