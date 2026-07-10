use super::helpers::*;
use super::*;
use crate::conformance::PushStoreConformance;
use crate::domain::{PublishCounters, PublishLifecycleState, PublishStatus};
use crate::storage::{PublishStatusCasOutcome, PushPublishStatusStore, PushStorageResult};
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

    async fn get_consistent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        self.get(family, app_id, pk, sk).await
    }

    async fn compare_and_swap(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        expected: &str,
        data: String,
    ) -> PushStorageResult<bool> {
        let key = (family_app(family, app_id), pk.to_owned(), sk.to_owned());
        let mut inner = self.inner.write().await;
        if inner.get(&key).map(String::as_str) != Some(expected) {
            return Ok(false);
        }
        inner.insert(key, data);
        Ok(true)
    }

    async fn compare_and_delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        expected: &str,
    ) -> PushStorageResult<bool> {
        let key = (family_app(family, app_id), pk.to_owned(), sk.to_owned());
        let mut inner = self.inner.write().await;
        if inner.get(&key).map(String::as_str) != Some(expected) {
            return Ok(false);
        }
        inner.remove(&key);
        Ok(true)
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

#[cfg(feature = "surrealdb")]
#[test]
fn surreal_duplicate_statement_errors_are_classified_as_conflicts() {
    assert!(surreal_conflict_error(&"record already exists"));
    assert!(surreal_conflict_error(&"duplicate key"));
    assert!(surreal_conflict_error(&"unique index violation"));
    assert!(!surreal_conflict_error(&"connection closed"));
}

fn test_publish_status(state: PublishLifecycleState) -> PublishStatus {
    PublishStatus {
        app_id: "app-1".to_owned(),
        publish_id: "publish-1".to_owned(),
        state,
        counters: PublishCounters {
            planned: 2,
            dispatched: 0,
            succeeded: 0,
            failed: 0,
            expired: 0,
            retry_scheduled: 0,
            retry_attempted: 0,
            dead_lettered: 0,
        },
        fanout_regime: None,
        retry_after_ms: None,
        error_reason: None,
    }
}

#[tokio::test]
async fn document_publish_status_create_is_atomic() {
    let store = test_store();
    let status = test_publish_status(PublishLifecycleState::Queued);

    assert_eq!(
        store
            .create_publish_status_if_absent(status.clone())
            .await
            .unwrap(),
        PublishStatusCasOutcome::Inserted { revision: 1 }
    );
    assert_eq!(
        store.create_publish_status_if_absent(status).await.unwrap(),
        PublishStatusCasOutcome::Conflict
    );

    let stored = store
        .get_versioned_publish_status("app-1", "publish-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.revision, 1);
    assert!(stored.updated_at_ms > 0);
}

#[tokio::test]
async fn document_publish_status_cas_allows_only_one_concurrent_writer() {
    let store = test_store();
    store
        .create_publish_status_if_absent(test_publish_status(PublishLifecycleState::Queued))
        .await
        .unwrap();
    let expected = store
        .get_versioned_publish_status("app-1", "publish-1")
        .await
        .unwrap()
        .unwrap();
    let mut first = expected.status.clone();
    first.counters.retry_scheduled = 1;
    let mut second = expected.status.clone();
    second.counters.retry_scheduled = 2;

    let first_store = store.clone();
    let first_expected = expected.clone();
    let (first_outcome, second_outcome) = tokio::join!(
        async move {
            first_store
                .compare_and_swap_publish_status(&first_expected, first)
                .await
                .unwrap()
        },
        async {
            store
                .compare_and_swap_publish_status(&expected, second)
                .await
                .unwrap()
        }
    );

    assert_eq!(
        usize::from(first_outcome.applied()) + usize::from(second_outcome.applied()),
        1
    );
    assert!(matches!(
        (first_outcome, second_outcome),
        (
            PublishStatusCasOutcome::Updated { revision: 2 },
            PublishStatusCasOutcome::Conflict
        ) | (
            PublishStatusCasOutcome::Conflict,
            PublishStatusCasOutcome::Updated { revision: 2 }
        )
    ));
    assert_eq!(
        store
            .get_versioned_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
    assert_eq!(
        store
            .backend
            .scan_pk(constants::FAMILY_STATUS_UPDATED_TIME, "app-1", "time",)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn document_publish_status_cas_upgrades_legacy_status_documents() {
    let store = test_store();
    let status = test_publish_status(PublishLifecycleState::Queued);
    store
        .backend
        .put(
            constants::FAMILY_STATUS,
            "app-1",
            "publish-1",
            constants::DEFAULT_SK,
            to_json_string(&status).unwrap(),
        )
        .await
        .unwrap();
    store
        .backend
        .put(
            constants::FAMILY_STATUS_UPDATED,
            "app-1",
            "publish-1",
            constants::DEFAULT_SK,
            to_json_string(&42_u64).unwrap(),
        )
        .await
        .unwrap();

    let expected = store
        .get_versioned_publish_status("app-1", "publish-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expected.revision, 1);
    assert_eq!(expected.updated_at_ms, 42);
    let mut next = expected.status.clone();
    next.state = PublishLifecycleState::Planning;
    assert_eq!(
        store
            .compare_and_swap_publish_status(&expected, next)
            .await
            .unwrap(),
        PublishStatusCasOutcome::Updated { revision: 2 }
    );

    let upgraded = store
        .get_versioned_publish_status("app-1", "publish-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(upgraded.revision, 2);
    assert!(upgraded.updated_at_ms > 42);
    assert_eq!(upgraded.status.state, PublishLifecycleState::Planning);
}

#[tokio::test]
async fn document_status_cleanup_ignores_a_stale_advisory_timestamp() {
    let store = test_store();
    let mut terminal = test_publish_status(PublishLifecycleState::Succeeded);
    terminal.counters.succeeded = 2;
    store
        .create_publish_status_if_absent(terminal)
        .await
        .unwrap();
    let first = store
        .get_versioned_publish_status("app-1", "publish-1")
        .await
        .unwrap()
        .unwrap();
    let mut next = first.status.clone();
    next.counters.retry_attempted = 1;
    store
        .compare_and_swap_publish_status(&first, next)
        .await
        .unwrap();
    let current = store
        .get_versioned_publish_status("app-1", "publish-1")
        .await
        .unwrap()
        .unwrap();
    let current_position =
        status_updated_position(current.updated_at_ms, &current.status.publish_id);
    store
        .backend
        .delete(
            constants::FAMILY_STATUS_UPDATED_TIME,
            "app-1",
            "time",
            &current_position,
        )
        .await
        .unwrap();
    let stale_position = status_updated_position(first.updated_at_ms, &first.status.publish_id);
    store
        .backend
        .put(
            constants::FAMILY_STATUS_UPDATED_TIME,
            "app-1",
            "time",
            &stale_position,
            r#"{ "data": "publish-1", "_v": 1 }"#.to_owned(),
        )
        .await
        .unwrap();

    let report = publishing::document_cleanup_publish_statuses(&store, "app-1", u64::MAX, 10)
        .await
        .unwrap();

    assert_eq!(report.deleted, 0);
    assert!(
        store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .backend
            .get(
                constants::FAMILY_STATUS_UPDATED_TIME,
                "app-1",
                "time",
                &stale_position,
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn document_status_cleanup_survives_missing_updated_pointer() {
    let store = test_store();
    store
        .create_publish_status_if_absent(test_publish_status(PublishLifecycleState::Succeeded))
        .await
        .unwrap();
    store
        .backend
        .delete(
            constants::FAMILY_STATUS_UPDATED,
            "app-1",
            "publish-1",
            constants::DEFAULT_SK,
        )
        .await
        .unwrap();

    let report = publishing::document_cleanup_publish_statuses(&store, "app-1", u64::MAX, 10)
        .await
        .unwrap();

    assert_eq!(report.deleted, 1);
    assert!(
        store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .is_none()
    );
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
