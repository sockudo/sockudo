use super::helpers::*;
use super::*;
use crate::conformance::PushStoreConformance;
use crate::storage::PushStorageResult;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type TestDocumentKey = (String, String, String);
type TestDocumentMap = Arc<RwLock<BTreeMap<TestDocumentKey, String>>>;

#[derive(Clone, Default)]
struct TestDocumentBackend {
    inner: TestDocumentMap,
}

#[async_trait]
impl DocumentBackend for TestDocumentBackend {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()> {
        self.inner.write().await.insert(
            (family_app(family, app_id), pk.to_owned(), sk.to_owned()),
            data,
        );
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
        let key = (family_app(family, app_id), pk.to_owned(), sk.to_owned());
        let mut inner = self.inner.write().await;
        if inner.contains_key(&key) {
            return Ok(false);
        }
        inner.insert(key, data);
        Ok(true)
    }

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        Ok(self
            .inner
            .read()
            .await
            .get(&(family_app(family, app_id), pk.to_owned(), sk.to_owned()))
            .cloned())
    }

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool> {
        Ok(self
            .inner
            .write()
            .await
            .remove(&(family_app(family, app_id), pk.to_owned(), sk.to_owned()))
            .is_some())
    }

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        Ok(self
            .inner
            .read()
            .await
            .iter()
            .filter(|((family_app_key, _, _), _)| family_app_key == &family_app(family, app_id))
            .map(|((_, pk, sk), data)| StoredDocument {
                pk: pk.clone(),
                sk: sk.clone(),
                data: data.clone(),
            })
            .collect())
    }
}

fn test_store() -> DocumentPushStore<TestDocumentBackend> {
    DocumentPushStore::with_backend(TestDocumentBackend::default())
}

#[tokio::test]
async fn document_store_satisfies_device_registration_idempotency() {
    PushStoreConformance::assert_device_registration_idempotency(test_store())
        .await
        .unwrap();
}

#[tokio::test]
async fn document_store_satisfies_cursor_pagination_and_channel_fanout() {
    PushStoreConformance::assert_cursor_pagination_and_channel_fanout(test_store())
        .await
        .unwrap();
}

#[tokio::test]
async fn document_store_satisfies_stale_cleanup_scans() {
    PushStoreConformance::assert_stale_cleanup_scans(test_store())
        .await
        .unwrap();
}

#[tokio::test]
async fn document_store_satisfies_secondary_storage_contracts() {
    PushStoreConformance::assert_credentials_templates_schedule_events_and_idempotency(
            test_store(),
        )
        .await
        .unwrap();
}
