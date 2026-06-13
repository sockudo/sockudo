use super::SurrealDbPushStore;
use super::document::{DocumentBackend, StoredDocument};
use super::helpers::*;
use crate::storage::PushStorageResult;
use async_trait::async_trait;
use surrealdb_types::SurrealValue;

#[cfg(feature = "surrealdb")]
#[derive(Clone)]
pub struct SurrealDbDocumentBackend {
    db: surrealdb::Surreal<surrealdb::engine::any::Any>,
    table: String,
}

#[cfg(feature = "surrealdb")]
impl SurrealDbPushStore {
    pub async fn new(
        db: surrealdb::Surreal<surrealdb::engine::any::Any>,
        table: impl Into<String>,
    ) -> PushStorageResult<Self> {
        let backend = SurrealDbDocumentBackend {
            db,
            table: table.into(),
        };
        validate_identifier(&backend.table, "SurrealDB push table")?;
        backend.ensure_schema().await?;
        Ok(Self::with_backend(backend))
    }
}

#[cfg(feature = "surrealdb")]
#[derive(Debug, serde::Deserialize, surrealdb_types::SurrealValue)]
struct SurrealDocumentRow {
    pk: String,
    sk: String,
    data: String,
}

#[cfg(feature = "surrealdb")]
impl SurrealDbDocumentBackend {
    async fn ensure_schema(&self) -> PushStorageResult<()> {
        self.db
            .query(format!(
                "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
                 DEFINE INDEX IF NOT EXISTS {}_key ON TABLE {} FIELDS family_app, pk, sk UNIQUE;\
                 DEFINE INDEX IF NOT EXISTS {}_app ON TABLE {} FIELDS family_app;",
                self.table, self.table, self.table, self.table, self.table
            ))
            .await
            .map_err(surreal_error)?;
        Ok(())
    }

    fn record_id(family: &'static str, app_id: &str, pk: &str, sk: &str) -> String {
        let digest =
            crate::domain::stable_hash(format!("{family}\0{app_id}\0{pk}\0{sk}").as_bytes());
        digest.replace('-', "_")
    }
}

#[cfg(feature = "surrealdb")]
#[async_trait]
impl DocumentBackend for SurrealDbDocumentBackend {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()> {
        self.db
            .query("UPSERT type::record($table, $id) SET family_app = $family_app, app_id = $app_id, pk = $pk, sk = $sk, data = $data")
            .bind(("table", self.table.clone()))
            .bind(("id", Self::record_id(family, app_id, pk, sk)))
            .bind(("family_app", family_app(family, app_id)))
            .bind(("app_id", app_id.to_owned()))
            .bind(("pk", pk.to_owned()))
            .bind(("sk", sk.to_owned()))
            .bind(("data", data))
            .await
            .map_err(surreal_error)?;
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
            .db
            .query("CREATE type::record($table, $id) SET family_app = $family_app, app_id = $app_id, pk = $pk, sk = $sk, data = $data")
            .bind(("table", self.table.clone()))
            .bind(("id", Self::record_id(family, app_id, pk, sk)))
            .bind(("family_app", family_app(family, app_id)))
            .bind(("app_id", app_id.to_owned()))
            .bind(("pk", pk.to_owned()))
            .bind(("sk", sk.to_owned()))
            .bind(("data", data))
            .await;
        let mut response = match result {
            Ok(response) => response,
            Err(error)
                if error.to_string().to_ascii_lowercase().contains("already")
                    || error.to_string().to_ascii_lowercase().contains("duplicate")
                    || error.to_string().to_ascii_lowercase().contains("unique") =>
            {
                return Ok(false);
            }
            Err(error) => return Err(surreal_error(error)),
        };
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(!rows.is_empty())
    }

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT pk, sk, data FROM {} WHERE family_app = $family_app AND pk = $pk AND sk = $sk LIMIT 1",
                self.table
            ))
            .bind(("family_app", family_app(family, app_id)))
            .bind(("pk", pk.to_owned()))
            .bind(("sk", sk.to_owned()))
            .await
            .map_err(surreal_error)?;
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(rows.into_iter().next().map(|row| row.data))
    }

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool> {
        let mut response = self
            .db
            .query("DELETE type::record($table, $id) RETURN BEFORE")
            .bind(("table", self.table.clone()))
            .bind(("id", Self::record_id(family, app_id, pk, sk)))
            .await
            .map_err(surreal_error)?;
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(!rows.is_empty())
    }

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT pk, sk, data FROM {} WHERE family_app = $family_app ORDER BY pk ASC, sk ASC",
                self.table
            ))
            .bind(("family_app", family_app(family, app_id)))
            .await
            .map_err(surreal_error)?;
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(rows
            .into_iter()
            .map(|row| StoredDocument {
                pk: row.pk,
                sk: row.sk,
                data: row.data,
            })
            .collect())
    }

    async fn scan_pk(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT pk, sk, data FROM {} WHERE family_app = $family_app AND pk = $pk ORDER BY sk ASC",
                self.table
            ))
            .bind(("family_app", family_app(family, app_id)))
            .bind(("pk", pk.to_owned()))
            .await
            .map_err(surreal_error)?;
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(rows
            .into_iter()
            .map(|row| StoredDocument {
                pk: row.pk,
                sk: row.sk,
                data: row.data,
            })
            .collect())
    }

    async fn scan_pk_page(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT pk, sk, data FROM {} WHERE family_app = $family_app AND pk = $pk AND sk > $start_after ORDER BY sk ASC LIMIT $limit",
                self.table
            ))
            .bind(("family_app", family_app(family, app_id)))
            .bind(("pk", pk.to_owned()))
            .bind(("start_after", start_after.unwrap_or("").to_owned()))
            .bind(("limit", limit.max(1)))
            .await
            .map_err(surreal_error)?;
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(rows
            .into_iter()
            .map(|row| StoredDocument {
                pk: row.pk,
                sk: row.sk,
                data: row.data,
            })
            .collect())
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
        let mut query = String::with_capacity(keys.len() * 64);
        for index in 0..keys.len() {
            query.push_str(&format!(
                "DELETE type::record($table, $id{index}) RETURN BEFORE;"
            ));
        }
        let mut request = self.db.query(query).bind(("table", self.table.clone()));
        for (index, (pk, sk)) in keys.iter().enumerate() {
            request = request.bind((
                format!("id{index}"),
                Self::record_id(family, app_id, pk, sk),
            ));
        }
        let mut response = request.await.map_err(surreal_error)?;
        let mut deleted = 0_u64;
        for index in 0..keys.len() {
            let rows: Vec<SurrealDocumentRow> = response.take(index).map_err(surreal_error)?;
            deleted += rows.len() as u64;
        }
        Ok(deleted)
    }
}
