use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
#[cfg(all(feature = "push", feature = "mysql"))]
use sqlx::mysql::MySqlPoolOptions;
#[cfg(all(feature = "push", feature = "postgres"))]
use sqlx::postgres::PgPoolOptions;
#[cfg(any(
    feature = "dynamodb",
    feature = "mysql",
    feature = "postgres",
    feature = "scylladb",
    feature = "surrealdb"
))]
use std::sync::Arc;
#[cfg(any(feature = "mysql", feature = "postgres"))]
use std::time::Duration;

#[cfg(all(feature = "push", feature = "dynamodb"))]
pub(crate) async fn create_dynamodb_push_store(
    config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    let db = &config.database.dynamodb;
    let mut aws_config_builder =
        aws_config::from_env().region(aws_sdk_dynamodb::config::Region::new(db.region.clone()));

    if let Some(endpoint) = &db.endpoint_url {
        aws_config_builder = aws_config_builder.endpoint_url(endpoint);
    }
    if let (Some(access_key), Some(secret_key)) = (&db.aws_access_key_id, &db.aws_secret_access_key)
    {
        let credentials_provider = aws_sdk_dynamodb::config::Credentials::new(
            access_key, secret_key, None, None, "static",
        );
        aws_config_builder = aws_config_builder.credentials_provider(credentials_provider);
    }
    if let Some(profile) = &db.aws_profile_name {
        aws_config_builder = aws_config_builder.profile_name(profile);
    }

    let client = aws_sdk_dynamodb::Client::new(&aws_config_builder.load().await);
    let table = push_table_name(&db.table_name);
    let store = sockudo_push::DynamoDbPushStore::new(client, table)
        .await
        .map_err(|e| Error::Internal(format!("Failed to create DynamoDB push store: {e}")))?;
    Ok(Arc::new(store))
}

#[cfg(all(feature = "push", not(feature = "dynamodb")))]
pub(crate) async fn create_dynamodb_push_store(
    _config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    Err(Error::Internal(
        "push storage driver 'dynamodb' requires the 'dynamodb' feature".to_owned(),
    ))
}

#[cfg(all(feature = "push", feature = "surrealdb"))]
pub(crate) async fn create_surrealdb_push_store(
    config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    use surrealdb::engine::any::connect;
    use surrealdb::opt::auth::Root;

    let db_config = &config.database.surrealdb;
    let db = connect(db_config.url.as_str())
        .await
        .map_err(|e| Error::Internal(format!("Failed to connect push store to SurrealDB: {e}")))?;
    db.signin(Root {
        username: db_config.username.clone(),
        password: db_config.password.clone(),
    })
    .await
    .map_err(|e| {
        Error::Internal(format!(
            "Failed to authenticate push store to SurrealDB: {e}"
        ))
    })?;
    db.use_ns(db_config.namespace.as_str())
        .use_db(db_config.database.as_str())
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to select push store SurrealDB namespace/database: {e}"
            ))
        })?;

    let table = push_table_name(&db_config.table_name);
    let store = sockudo_push::SurrealDbPushStore::new(db, table)
        .await
        .map_err(|e| Error::Internal(format!("Failed to create SurrealDB push store: {e}")))?;
    Ok(Arc::new(store))
}

#[cfg(all(feature = "push", not(feature = "surrealdb")))]
pub(crate) async fn create_surrealdb_push_store(
    _config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    Err(Error::Internal(
        "push storage driver 'surrealdb' requires the 'surrealdb' feature".to_owned(),
    ))
}

#[cfg(all(feature = "push", feature = "scylladb"))]
pub(crate) async fn create_scylladb_push_store(
    config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    let db = &config.database.scylladb;
    let mut builder =
        scylla::client::session_builder::SessionBuilder::new().known_nodes(db.nodes.clone());
    if let (Some(username), Some(password)) = (&db.username, &db.password) {
        builder = builder.user(username, password);
    }
    let session =
        Arc::new(builder.build().await.map_err(|e| {
            Error::Internal(format!("Failed to connect push store to ScyllaDB: {e}"))
        })?);

    let table = push_table_name(&db.table_name);
    let store = sockudo_push::ScyllaDbPushStore::new(
        session,
        db.keyspace.clone(),
        table,
        db.replication_class.clone(),
        db.replication_factor,
    )
    .await
    .map_err(|e| Error::Internal(format!("Failed to create ScyllaDB push store: {e}")))?;
    Ok(Arc::new(store))
}

#[cfg(all(feature = "push", not(feature = "scylladb")))]
pub(crate) async fn create_scylladb_push_store(
    _config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    Err(Error::Internal(
        "push storage driver 'scylladb' requires the 'scylladb' feature".to_owned(),
    ))
}

#[cfg(feature = "push")]
fn push_table_name(base: &str) -> String {
    let normalized = base
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{normalized}_push_records")
}

#[cfg(all(feature = "push", feature = "postgres"))]
pub(crate) async fn create_postgres_push_store(
    config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    let db = &config.database.postgres;
    let password = urlencoding::encode(&db.password);
    let connection_string = format!(
        "postgresql://{}:{}@{}:{}/{}",
        db.username, password, db.host, db.port, db.database
    );
    let mut opts = PgPoolOptions::new();
    opts = if config.database_pooling.enabled {
        opts.min_connections(db.pool_min.unwrap_or(config.database_pooling.min))
            .max_connections(db.pool_max.unwrap_or(config.database_pooling.max))
    } else {
        opts.max_connections(db.connection_pool_size)
    };
    let pool = opts
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(180))
        .connect(&connection_string)
        .await
        .map_err(|e| Error::Internal(format!("Failed to connect push store to PostgreSQL: {e}")))?;
    let store = sockudo_push::PostgresPushStore::new(pool);
    store
        .assert_schema_version()
        .await
        .map_err(|e| Error::Internal(format!("PostgreSQL push schema check failed: {e}")))?;
    Ok(Arc::new(store))
}

#[cfg(all(feature = "push", not(feature = "postgres")))]
pub(crate) async fn create_postgres_push_store(
    _config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    Err(Error::Internal(
        "push storage driver 'postgres' requires the 'postgres' feature".to_owned(),
    ))
}

#[cfg(all(feature = "push", feature = "mysql"))]
pub(crate) async fn create_mysql_push_store(
    config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    let db = &config.database.mysql;
    let password = urlencoding::encode(&db.password);
    let connection_string = format!(
        "mysql://{}:{}@{}:{}/{}",
        db.username, password, db.host, db.port, db.database
    );
    let mut opts = MySqlPoolOptions::new();
    opts = if config.database_pooling.enabled {
        opts.min_connections(db.pool_min.unwrap_or(config.database_pooling.min))
            .max_connections(db.pool_max.unwrap_or(config.database_pooling.max))
    } else {
        opts.max_connections(db.connection_pool_size)
    };
    let pool = opts
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(180))
        .connect(&connection_string)
        .await
        .map_err(|e| Error::Internal(format!("Failed to connect push store to MySQL: {e}")))?;
    let store = sockudo_push::MySqlPushStore::new(pool);
    store
        .assert_schema_version()
        .await
        .map_err(|e| Error::Internal(format!("MySQL push schema check failed: {e}")))?;
    Ok(Arc::new(store))
}

#[cfg(all(feature = "push", not(feature = "mysql")))]
pub(crate) async fn create_mysql_push_store(
    _config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    Err(Error::Internal(
        "push storage driver 'mysql' requires the 'mysql' feature".to_owned(),
    ))
}
