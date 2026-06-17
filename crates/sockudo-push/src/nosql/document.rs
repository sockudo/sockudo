use super::helpers::*;
use crate::domain::DeleteDeviceOutcome;
use crate::storage::PushStorageResult;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

#[derive(Clone, Debug)]
pub struct StoredDocument {
    pub(super) pk: String,
    pub(super) sk: String,
    pub(super) data: String,
}

#[async_trait]
pub trait DocumentBackend: Clone + Send + Sync + 'static {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()>;

    async fn put_if_absent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<bool>;

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>>;

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool>;

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>>;

    async fn scan_pk(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        Ok(self
            .scan_app(family, app_id)
            .await?
            .into_iter()
            .filter(|document| document.pk == pk)
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
        Ok(self
            .scan_pk(family, app_id, pk)
            .await?
            .into_iter()
            .filter(|document| start_after.is_none_or(|start| document.sk.as_str() > start))
            .take(limit.max(1))
            .collect())
    }

    async fn scan_app_page_by_pk(
        &self,
        family: &'static str,
        app_id: &str,
        start_after_pk: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        Ok(self
            .scan_app(family, app_id)
            .await?
            .into_iter()
            .filter(|document| start_after_pk.is_none_or(|start| document.pk.as_str() > start))
            .take(limit.max(1))
            .collect())
    }

    async fn delete_many(
        &self,
        family: &'static str,
        app_id: &str,
        keys: &[(String, String)],
    ) -> PushStorageResult<u64> {
        let mut deleted = 0_u64;
        for (pk, sk) in keys {
            if self.delete(family, app_id, pk, sk).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

#[derive(Clone)]
pub struct DocumentPushStore<B> {
    pub(super) backend: B,
}

impl<B> DocumentPushStore<B> {
    pub(super) fn with_backend(backend: B) -> Self {
        Self { backend }
    }
}

impl<B> DocumentPushStore<B>
where
    B: DocumentBackend,
{
    pub(super) async fn put_json<T: Serialize>(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        value: &T,
    ) -> PushStorageResult<()> {
        self.backend
            .put(family, app_id, pk, sk, to_json_string(value)?)
            .await
    }

    pub(super) async fn get_json<T: DeserializeOwned>(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<T>> {
        self.backend
            .get(family, app_id, pk, sk)
            .await?
            .map(|data| from_json_str(&data))
            .transpose()
    }

    pub(super) async fn delete_json(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        delete_outcome(self.backend.delete(family, app_id, pk, sk).await?)
    }

    pub(super) async fn scan_json<T: DeserializeOwned>(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<(String, String, T)>> {
        self.backend
            .scan_app(family, app_id)
            .await?
            .into_iter()
            .map(|doc| Ok((doc.pk, doc.sk, from_json_str(&doc.data)?)))
            .collect()
    }

    pub(super) async fn scan_pk_json<T: DeserializeOwned>(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
    ) -> PushStorageResult<Vec<(String, String, T)>> {
        self.backend
            .scan_pk(family, app_id, pk)
            .await?
            .into_iter()
            .map(|doc| Ok((doc.pk, doc.sk, from_json_str(&doc.data)?)))
            .collect()
    }

    pub(super) async fn scan_pk_page_json<T: DeserializeOwned>(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<(String, String, T)>> {
        self.backend
            .scan_pk_page(family, app_id, pk, start_after, limit)
            .await?
            .into_iter()
            .map(|doc| Ok((doc.pk, doc.sk, from_json_str(&doc.data)?)))
            .collect()
    }

    pub(super) async fn scan_app_page_by_pk_json<T: DeserializeOwned>(
        &self,
        family: &'static str,
        app_id: &str,
        start_after_pk: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<(String, String, T)>> {
        self.backend
            .scan_app_page_by_pk(family, app_id, start_after_pk, limit)
            .await?
            .into_iter()
            .map(|doc| Ok((doc.pk, doc.sk, from_json_str(&doc.data)?)))
            .collect()
    }
}
