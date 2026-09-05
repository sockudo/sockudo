#[cfg(feature = "versioned-messages")]
mod annotation_store;
mod history_record;
mod history_store;
mod history_store_impl;
mod schema;
mod stream_state;
#[cfg(feature = "versioned-messages")]
mod version_store;
mod writers;

use sockudo_core::error::{Error, Result};
use sqlx::PgConnection;

#[cfg(feature = "versioned-messages")]
pub(super) use annotation_store::PostgresAnnotationStore;
pub(super) use history_store::PostgresHistoryStore;
#[cfg(feature = "versioned-messages")]
pub(super) use version_store::PostgresVersionStore;

#[derive(Clone)]
struct HistoryTables {
    streams: String,
    entries: String,
    version_streams: String,
    version_messages: String,
    version_entries: String,
    annotation_streams: String,
    annotation_events: String,
    annotation_projections: String,
}

async fn lock_postgres_schema(conn: &mut PgConnection, lock_name: &str) -> Result<()> {
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
        .bind(lock_name)
        .execute(conn)
        .await
        .map_err(|e| Error::Internal(format!("Failed to lock PostgreSQL schema init: {e}")))?;
    Ok(())
}

async fn unlock_postgres_schema(conn: &mut PgConnection, lock_name: &str) -> Result<()> {
    sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
        .bind(lock_name)
        .execute(conn)
        .await
        .map_err(|e| Error::Internal(format!("Failed to unlock PostgreSQL schema init: {e}")))?;
    Ok(())
}

#[cfg(test)]
pub(super) async fn simulate_legacy_retention(
    db: &sockudo_core::options::DatabaseConnection,
    config: sockudo_core::options::HistoryConfig,
) {
    let store = PostgresHistoryStore::new(
        db,
        &sockudo_core::options::DatabasePooling::default(),
        config,
        None,
        None,
    )
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {} SET retention_initialized=FALSE,retained_messages=777,retained_bytes=999 WHERE app_id='c6-app' AND channel='legacy'",store.tables.streams).as_str())).execute(&store.pool).await.unwrap();
}
