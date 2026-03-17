use async_trait::async_trait;
use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use scylla::{DeserializeRow, SerializeRow};
use sockudo_core::app::{App, AppManager};
use sockudo_core::error::{Error, Result};
use sockudo_core::webhook_types::Webhook;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Configuration for ScyllaDB App Manager
#[derive(Debug, Clone)]
pub struct ScyllaDbConfig {
    pub nodes: Vec<String>,
    pub keyspace: String,
    pub table_name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub replication_class: String,
    pub replication_factor: u32,
}

impl Default for ScyllaDbConfig {
    fn default() -> Self {
        Self {
            nodes: vec!["127.0.0.1:9042".to_string()],
            keyspace: "sockudo".to_string(),
            table_name: "applications".to_string(),
            username: None,
            password: None,
            replication_class: "SimpleStrategy".to_string(),
            replication_factor: 3,
        }
    }
}

/// ScyllaDB-based implementation of the AppManager
pub struct ScyllaDbAppManager {
    config: ScyllaDbConfig,
    session: Arc<Session>,
    insert_stmt: Arc<PreparedStatement>,
    update_stmt: Arc<PreparedStatement>,
    delete_stmt: Arc<PreparedStatement>,
}

impl ScyllaDbAppManager {
    /// Create a new ScyllaDB-based AppManager with the provided configuration
    pub async fn new(config: ScyllaDbConfig) -> Result<Self> {
        info!(
            "Initializing ScyllaDB AppManager with nodes: {:?}",
            config.nodes
        );

        let mut builder = SessionBuilder::new().known_nodes(&config.nodes);

        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            builder = builder.user(username, password);
        }

        let session = builder
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to ScyllaDB cluster: {e}")))?;

        let session = Arc::new(session);

        Self::ensure_keyspace_and_table(&session, &config).await?;

        let insert_query = format!(
            r#"INSERT INTO {}.{} (
                id, key, secret, max_connections, enable_client_messages, enabled,
                max_backend_events_per_second, max_client_events_per_second,
                max_read_requests_per_second, max_presence_members_per_channel,
                max_presence_member_size_in_kb, max_channel_name_length,
                max_event_channels_at_once, max_event_name_length,
                max_event_payload_in_kb, max_event_batch_size,
                enable_user_authentication, enable_watchlist_events,
                webhooks, allowed_origins, channel_delta_compression,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()), toTimestamp(now()))"#,
            config.keyspace, config.table_name
        );

        let update_query = format!(
            r#"UPDATE {}.{} SET
                key = ?, secret = ?, max_connections = ?, enable_client_messages = ?, enabled = ?,
                max_backend_events_per_second = ?, max_client_events_per_second = ?,
                max_read_requests_per_second = ?, max_presence_members_per_channel = ?,
                max_presence_member_size_in_kb = ?, max_channel_name_length = ?,
                max_event_channels_at_once = ?, max_event_name_length = ?,
                max_event_payload_in_kb = ?, max_event_batch_size = ?,
                enable_user_authentication = ?, enable_watchlist_events = ?,
                webhooks = ?, allowed_origins = ?,
                channel_delta_compression = ?,
                updated_at = toTimestamp(now())
            WHERE id = ?"#,
            config.keyspace, config.table_name
        );

        let delete_query = format!(
            r#"DELETE FROM {}.{} WHERE id = ?"#,
            config.keyspace, config.table_name
        );

        let insert_stmt = session
            .prepare(insert_query)
            .await
            .map_err(|e| Error::Internal(format!("Failed to prepare insert statement: {e}")))?;

        let update_stmt = session
            .prepare(update_query)
            .await
            .map_err(|e| Error::Internal(format!("Failed to prepare update statement: {e}")))?;

        let delete_stmt = session
            .prepare(delete_query)
            .await
            .map_err(|e| Error::Internal(format!("Failed to prepare delete statement: {e}")))?;

        Ok(Self {
            config,
            session,
            insert_stmt: Arc::new(insert_stmt),
            update_stmt: Arc::new(update_stmt),
            delete_stmt: Arc::new(delete_stmt),
        })
    }

    /// Create the keyspace and applications table if they don't exist
    async fn ensure_keyspace_and_table(
        session: &Arc<Session>,
        config: &ScyllaDbConfig,
    ) -> Result<()> {
        let create_keyspace_query = format!(
            r#"CREATE KEYSPACE IF NOT EXISTS {}
               WITH replication = {{'class': '{}', 'replication_factor': {}}}"#,
            config.keyspace, config.replication_class, config.replication_factor
        );

        session
            .query_unpaged(create_keyspace_query, &[])
            .await
            .map_err(|e| Error::Internal(format!("Failed to create keyspace: {e}")))?;

        info!("Ensured keyspace '{}' exists", config.keyspace);

        let create_table_query = format!(
            r#"CREATE TABLE IF NOT EXISTS {}.{} (
                id text PRIMARY KEY,
                key text,
                secret text,
                max_connections int,
                enable_client_messages boolean,
                enabled boolean,
                max_backend_events_per_second int,
                max_client_events_per_second int,
                max_read_requests_per_second int,
                max_presence_members_per_channel int,
                max_presence_member_size_in_kb int,
                max_channel_name_length int,
                max_event_channels_at_once int,
                max_event_name_length int,
                max_event_payload_in_kb int,
                max_event_batch_size int,
                enable_user_authentication boolean,
                enable_watchlist_events boolean,
                webhooks text,
                allowed_origins text,
                channel_delta_compression text,
                created_at timestamp,
                updated_at timestamp
            )"#,
            config.keyspace, config.table_name
        );

        session
            .query_unpaged(create_table_query, &[])
            .await
            .map_err(|e| Error::Internal(format!("Failed to create table: {e}")))?;

        info!(
            "Ensured table '{}.{}' exists",
            config.keyspace, config.table_name
        );

        let create_index_query = format!(
            r#"CREATE INDEX IF NOT EXISTS ON {}.{} (key)"#,
            config.keyspace, config.table_name
        );

        session
            .query_unpaged(create_index_query, &[])
            .await
            .map_err(|e| Error::Internal(format!("Failed to create index on key: {e}")))?;

        // Migration: add channel_delta_compression column for existing tables
        let alter_query = format!(
            "ALTER TABLE {}.{} ADD channel_delta_compression text",
            config.keyspace, config.table_name
        );
        if let Err(e) = session.query_unpaged(alter_query, &[]).await {
            let err_str = e.to_string();
            // Ignore "already exists" errors (column was already added or is in CREATE TABLE)
            if !err_str.contains("already exist") && !err_str.contains("conflicts") {
                return Err(Error::Internal(format!(
                    "Failed to add channel_delta_compression column: {e}"
                )));
            }
        }

        Ok(())
    }
}

/// Struct for ScyllaDB app row (both serialization and deserialization)
#[derive(SerializeRow, DeserializeRow)]
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
    webhooks: Option<String>,
    allowed_origins: Option<String>,
    channel_delta_compression: Option<String>,
}

/// Struct for UPDATE (SET fields first, then id for WHERE)
#[derive(SerializeRow)]
struct UpdateRow {
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
    webhooks: Option<String>,
    allowed_origins: Option<String>,
    channel_delta_compression: Option<String>,
    id: String,
}

impl UpdateRow {
    fn from_app(app: &App) -> Result<Self> {
        let webhooks = app
            .webhooks
            .as_ref()
            .map(|w| {
                sonic_rs::to_string(w)
                    .map_err(|e| Error::Internal(format!("Failed to serialize webhooks: {}", e)))
            })
            .transpose()?;

        let allowed_origins = app
            .allowed_origins
            .as_ref()
            .map(|o| {
                sonic_rs::to_string(o).map_err(|e| {
                    Error::Internal(format!("Failed to serialize allowed_origins: {}", e))
                })
            })
            .transpose()?;

        let channel_delta_compression = app
            .channel_delta_compression
            .as_ref()
            .map(|c| {
                sonic_rs::to_string(c).map_err(|e| {
                    Error::Internal(format!(
                        "Failed to serialize channel_delta_compression: {}",
                        e
                    ))
                })
            })
            .transpose()?;

        Ok(Self {
            key: app.key.clone(),
            secret: app.secret.clone(),
            max_connections: app.max_connections as i32,
            enable_client_messages: app.enable_client_messages,
            enabled: app.enabled,
            max_backend_events_per_second: app.max_backend_events_per_second.map(|v| v as i32),
            max_client_events_per_second: app.max_client_events_per_second as i32,
            max_read_requests_per_second: app.max_read_requests_per_second.map(|v| v as i32),
            max_presence_members_per_channel: app
                .max_presence_members_per_channel
                .map(|v| v as i32),
            max_presence_member_size_in_kb: app.max_presence_member_size_in_kb.map(|v| v as i32),
            max_channel_name_length: app.max_channel_name_length.map(|v| v as i32),
            max_event_channels_at_once: app.max_event_channels_at_once.map(|v| v as i32),
            max_event_name_length: app.max_event_name_length.map(|v| v as i32),
            max_event_payload_in_kb: app.max_event_payload_in_kb.map(|v| v as i32),
            max_event_batch_size: app.max_event_batch_size.map(|v| v as i32),
            enable_user_authentication: app.enable_user_authentication,
            enable_watchlist_events: app.enable_watchlist_events,
            webhooks,
            allowed_origins,
            channel_delta_compression,
            id: app.id.clone(),
        })
    }
}

impl AppRow {
    fn from_app(app: &App) -> Result<Self> {
        let webhooks = app
            .webhooks
            .as_ref()
            .map(|w| {
                sonic_rs::to_string(w)
                    .map_err(|e| Error::Internal(format!("Failed to serialize webhooks: {}", e)))
            })
            .transpose()?;

        let allowed_origins = app
            .allowed_origins
            .as_ref()
            .map(|o| {
                sonic_rs::to_string(o).map_err(|e| {
                    Error::Internal(format!("Failed to serialize allowed_origins: {}", e))
                })
            })
            .transpose()?;

        let channel_delta_compression = app
            .channel_delta_compression
            .as_ref()
            .map(|c| {
                sonic_rs::to_string(c).map_err(|e| {
                    Error::Internal(format!(
                        "Failed to serialize channel_delta_compression: {}",
                        e
                    ))
                })
            })
            .transpose()?;

        Ok(Self {
            id: app.id.clone(),
            key: app.key.clone(),
            secret: app.secret.clone(),
            max_connections: app.max_connections as i32,
            enable_client_messages: app.enable_client_messages,
            enabled: app.enabled,
            max_backend_events_per_second: app.max_backend_events_per_second.map(|v| v as i32),
            max_client_events_per_second: app.max_client_events_per_second as i32,
            max_read_requests_per_second: app.max_read_requests_per_second.map(|v| v as i32),
            max_presence_members_per_channel: app
                .max_presence_members_per_channel
                .map(|v| v as i32),
            max_presence_member_size_in_kb: app.max_presence_member_size_in_kb.map(|v| v as i32),
            max_channel_name_length: app.max_channel_name_length.map(|v| v as i32),
            max_event_channels_at_once: app.max_event_channels_at_once.map(|v| v as i32),
            max_event_name_length: app.max_event_name_length.map(|v| v as i32),
            max_event_payload_in_kb: app.max_event_payload_in_kb.map(|v| v as i32),
            max_event_batch_size: app.max_event_batch_size.map(|v| v as i32),
            enable_user_authentication: app.enable_user_authentication,
            enable_watchlist_events: app.enable_watchlist_events,
            webhooks,
            allowed_origins,
            channel_delta_compression,
        })
    }

    fn into_app(self) -> App {
        App {
            id: self.id.clone(),
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
            enable_watchlist_events: self.enable_watchlist_events,
            webhooks: self.webhooks.and_then(|json| {
                sonic_rs::from_str::<Vec<Webhook>>(&json)
                    .map_err(|e| {
                        error!("Failed to deserialize webhooks for app {}: {}", self.id, e)
                    })
                    .ok()
            }),
            allowed_origins: self.allowed_origins.and_then(|json| {
                sonic_rs::from_str::<Vec<String>>(&json)
                    .map_err(|e| {
                        error!(
                            "Failed to deserialize allowed_origins for app {}: {}",
                            self.id, e
                        )
                    })
                    .ok()
            }),
            channel_delta_compression: self.channel_delta_compression.and_then(|json| {
                sonic_rs::from_str::<ahash::AHashMap<String, sockudo_core::delta_types::ChannelDeltaConfig>>(&json)
                    .map_err(|e| {
                        error!(
                            "Failed to deserialize channel_delta_compression for app {}: {}",
                            self.id, e
                        )
                    })
                    .ok()
            }),
        }
    }
}

#[async_trait]
impl AppManager for ScyllaDbAppManager {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn create_app(&self, app: App) -> Result<()> {
        let values = AppRow::from_app(&app)?;

        self.session
            .execute_unpaged(&self.insert_stmt, values)
            .await
            .map_err(|e| {
                error!("Database error creating app {}: {}", app.id, e);
                Error::Internal(format!("Failed to insert app into ScyllaDB: {e}"))
            })?;

        Ok(())
    }

    async fn update_app(&self, app: App) -> Result<()> {
        let values = UpdateRow::from_app(&app)?;

        self.session
            .execute_unpaged(&self.update_stmt, values)
            .await
            .map_err(|e| {
                error!("Database error updating app {}: {}", app.id, e);
                Error::Internal(format!("Failed to update app in ScyllaDB: {e}"))
            })?;

        Ok(())
    }

    async fn delete_app(&self, app_id: &str) -> Result<()> {
        self.session
            .execute_unpaged(&self.delete_stmt, (app_id,))
            .await
            .map_err(|e| {
                error!("Database error deleting app {}: {}", app_id, e);
                Error::Internal(format!("Failed to delete app from ScyllaDB: {e}"))
            })?;

        Ok(())
    }

    async fn get_apps(&self) -> Result<Vec<App>> {
        let query = format!(
            r#"SELECT id, key, secret, max_connections, enable_client_messages, enabled,
                max_backend_events_per_second, max_client_events_per_second,
                max_read_requests_per_second, max_presence_members_per_channel,
                max_presence_member_size_in_kb, max_channel_name_length,
                max_event_channels_at_once, max_event_name_length,
                max_event_payload_in_kb, max_event_batch_size,
                enable_user_authentication, enable_watchlist_events,
                webhooks, allowed_origins, channel_delta_compression
            FROM {}.{}"#,
            self.config.keyspace, self.config.table_name
        );

        let mut apps = Vec::new();
        let mut rows_stream = self
            .session
            .query_iter(query, &[])
            .await
            .map_err(|e| Error::Internal(format!("Failed to query apps from ScyllaDB: {e}")))?
            .rows_stream::<AppRow>()
            .map_err(|e| Error::Internal(format!("Failed to create rows stream: {e}")))?;

        while let Some(app_row) = rows_stream
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch app row: {e}")))?
        {
            apps.push(app_row.into_app());
        }

        Ok(apps)
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        debug!("Fetching app by key {} from ScyllaDB", key);

        let query = format!(
            r#"SELECT id, key, secret, max_connections, enable_client_messages, enabled,
                max_backend_events_per_second, max_client_events_per_second,
                max_read_requests_per_second, max_presence_members_per_channel,
                max_presence_member_size_in_kb, max_channel_name_length,
                max_event_channels_at_once, max_event_name_length,
                max_event_payload_in_kb, max_event_batch_size,
                enable_user_authentication, enable_watchlist_events,
                webhooks, allowed_origins, channel_delta_compression
            FROM {}.{} WHERE key = ?"#,
            self.config.keyspace, self.config.table_name
        );

        let mut rows_stream = self
            .session
            .query_iter(query, (key,))
            .await
            .map_err(|e| {
                error!("Database error fetching app by key {}: {}", key, e);
                Error::Internal(format!("Failed to fetch app by key from ScyllaDB: {e}"))
            })?
            .rows_stream::<AppRow>()
            .map_err(|e| Error::Internal(format!("Failed to create rows stream: {e}")))?;

        if let Some(app_row) = rows_stream
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch app row: {e}")))?
        {
            let app = app_row.into_app();
            Ok(Some(app))
        } else {
            Ok(None)
        }
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        debug!("Fetching app {} from ScyllaDB", app_id);

        let query = format!(
            r#"SELECT id, key, secret, max_connections, enable_client_messages, enabled,
                max_backend_events_per_second, max_client_events_per_second,
                max_read_requests_per_second, max_presence_members_per_channel,
                max_presence_member_size_in_kb, max_channel_name_length,
                max_event_channels_at_once, max_event_name_length,
                max_event_payload_in_kb, max_event_batch_size,
                enable_user_authentication, enable_watchlist_events,
                webhooks, allowed_origins, channel_delta_compression
            FROM {}.{} WHERE id = ?"#,
            self.config.keyspace, self.config.table_name
        );

        let mut rows_stream = self
            .session
            .query_iter(query, (app_id,))
            .await
            .map_err(|e| {
                error!("Database error fetching app {}: {}", app_id, e);
                Error::Internal(format!("Failed to fetch app from ScyllaDB: {e}"))
            })?
            .rows_stream::<AppRow>()
            .map_err(|e| Error::Internal(format!("Failed to create rows stream: {e}")))?;

        if let Some(app_row) = rows_stream
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch app row: {e}")))?
        {
            let app = app_row.into_app();
            Ok(Some(app))
        } else {
            Ok(None)
        }
    }

    async fn check_health(&self) -> Result<()> {
        let query = "SELECT key FROM system.local WHERE key = 'local'";
        self.session
            .query_unpaged(query, &[])
            .await
            .map_err(|e| Error::Internal(format!("App manager ScyllaDB connection failed: {e}")))?;
        Ok(())
    }
}
