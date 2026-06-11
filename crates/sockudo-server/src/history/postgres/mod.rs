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
