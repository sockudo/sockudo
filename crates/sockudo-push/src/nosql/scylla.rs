use super::ScyllaDbPushStore;
use super::document::{DocumentBackend, StoredDocument};
use super::helpers::*;
use crate::storage::PushStorageResult;
use async_trait::async_trait;

#[cfg(feature = "scylladb")]
#[derive(Clone)]
pub struct ScyllaDbDocumentBackend {
    session: std::sync::Arc<scylla::client::session::Session>,
    keyspace: String,
    table: String,
}

#[cfg(feature = "scylladb")]
impl ScyllaDbPushStore {
    pub async fn new(
        session: std::sync::Arc<scylla::client::session::Session>,
        keyspace: impl Into<String>,
        table: impl Into<String>,
        replication_class: impl AsRef<str>,
        replication_factor: u32,
    ) -> PushStorageResult<Self> {
        let backend = ScyllaDbDocumentBackend {
            session,
            keyspace: keyspace.into(),
            table: table.into(),
        };
        validate_identifier(&backend.keyspace, "ScyllaDB push keyspace")?;
        validate_identifier(&backend.table, "ScyllaDB push table")?;
        backend
            .ensure_schema(replication_class.as_ref(), replication_factor)
            .await?;
        Ok(Self::with_backend(backend))
    }
}

#[cfg(feature = "scylladb")]
impl ScyllaDbDocumentBackend {
    fn fq_table(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }

    async fn ensure_schema(
        &self,
        replication_class: &str,
        replication_factor: u32,
    ) -> PushStorageResult<()> {
        validate_identifier(replication_class, "ScyllaDB replication class")?;
        self.session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': '{}', 'replication_factor': {}}}",
                    self.keyspace, replication_class, replication_factor
                ),
                (),
            )
            .await
            .map_err(scylla_error)?;
        self.session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {} (family_app text, pk text, sk text, app_id text, data text, PRIMARY KEY ((family_app), pk, sk)) WITH CLUSTERING ORDER BY (pk ASC, sk ASC)",
                    self.fq_table()
                ),
                (),
            )
            .await
            .map_err(scylla_error)?;
        Ok(())
    }
}

#[cfg(feature = "scylladb")]
#[async_trait]
impl DocumentBackend for ScyllaDbDocumentBackend {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()> {
        self.session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (family_app, pk, sk, app_id, data) VALUES (?, ?, ?, ?, ?)",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk, sk, app_id, data),
            )
            .await
            .map_err(scylla_error)?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<bool> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (family_app, pk, sk, app_id, data) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk, sk, app_id, data),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        let applied = rows
            .maybe_first_row::<(bool,)>()
            .map_err(scylla_error)?
            .map(|row| row.0)
            .unwrap_or(false);
        Ok(applied)
    }

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "SELECT data FROM {} WHERE family_app = ? AND pk = ? AND sk = ?",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk, sk),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.maybe_first_row::<(String,)>()
            .map_err(scylla_error)
            .map(|row| row.map(|row| row.0))
    }

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "DELETE FROM {} WHERE family_app = ? AND pk = ? AND sk = ? IF EXISTS",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk, sk),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.maybe_first_row::<(bool,)>()
            .map_err(scylla_error)
            .map(|row| row.map(|row| row.0).unwrap_or(false))
    }

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "SELECT pk, sk, data FROM {} WHERE family_app = ?",
                    self.fq_table()
                ),
                (family_app(family, app_id),),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.rows::<(String, String, String)>()
            .map_err(scylla_error)?
            .map(|row| {
                row.map(|(pk, sk, data)| StoredDocument { pk, sk, data })
                    .map_err(scylla_error)
            })
            .collect()
    }

    async fn scan_pk(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "SELECT pk, sk, data FROM {} WHERE family_app = ? AND pk = ?",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.rows::<(String, String, String)>()
            .map_err(scylla_error)?
            .map(|row| {
                row.map(|(pk, sk, data)| StoredDocument { pk, sk, data })
                    .map_err(scylla_error)
            })
            .collect()
    }

    async fn scan_pk_page(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "SELECT pk, sk, data FROM {} WHERE family_app = ? AND pk = ? AND sk > ? LIMIT ?",
                    self.fq_table()
                ),
                (
                    family_app(family, app_id),
                    pk,
                    start_after.unwrap_or(""),
                    limit.max(1) as i32,
                ),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.rows::<(String, String, String)>()
            .map_err(scylla_error)?
            .map(|row| {
                row.map(|(pk, sk, data)| StoredDocument { pk, sk, data })
                    .map_err(scylla_error)
            })
            .collect()
    }

    async fn scan_app_page_by_pk(
        &self,
        family: &'static str,
        app_id: &str,
        start_after_pk: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "SELECT pk, sk, data FROM {} WHERE family_app = ? AND pk > ? LIMIT ?",
                    self.fq_table()
                ),
                (
                    family_app(family, app_id),
                    start_after_pk.unwrap_or(""),
                    limit.max(1) as i32,
                ),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.rows::<(String, String, String)>()
            .map_err(scylla_error)?
            .map(|row| {
                row.map(|(pk, sk, data)| StoredDocument { pk, sk, data })
                    .map_err(scylla_error)
            })
            .collect()
    }

    async fn delete_many(
        &self,
        family: &'static str,
        app_id: &str,
        keys: &[(String, String)],
    ) -> PushStorageResult<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        use scylla::statement::batch::{Batch, BatchType};

        let family_app = family_app(family, app_id);
        for chunk in keys.chunks(64) {
            let mut batch = Batch::new(BatchType::Unlogged);
            let statement = scylla::statement::Statement::new(format!(
                "DELETE FROM {} WHERE family_app = ? AND pk = ? AND sk = ?",
                self.fq_table()
            ));
            let mut values = Vec::with_capacity(chunk.len());
            for (pk, sk) in chunk {
                batch.append_statement(statement.clone());
                values.push((family_app.clone(), pk.clone(), sk.clone()));
            }
            self.session
                .batch(&batch, values)
                .await
                .map_err(scylla_error)?;
        }
        Ok(keys.len() as u64)
    }
}
