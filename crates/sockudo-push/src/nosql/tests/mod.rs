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
    full_scans: Arc<std::sync::atomic::AtomicUsize>,
    page_rows: Arc<std::sync::atomic::AtomicUsize>,
    child_write_gate: Option<Arc<ChildWriteGate>>,
    missing_parent_read_gate: Option<Arc<ChildWriteGate>>,
}

#[derive(Default)]
struct ChildWriteGate {
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    armed: std::sync::atomic::AtomicBool,
    fail_after_write: std::sync::atomic::AtomicBool,
    missing_reads_until_pause: std::sync::atomic::AtomicUsize,
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
        let gate = self.child_write_gate.as_ref().filter(|gate| {
            family == super::constants::FAMILY_FANOUT_SHARD
                && gate.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        });
        if let Some(gate) = gate {
            gate.started.notify_one();
            gate.resume.notified().await;
        }
        self.inner.write().await.insert(
            (family_app(family, app_id), pk.to_owned(), sk.to_owned()),
            data,
        );
        if gate.is_some_and(|gate| {
            gate.fail_after_write
                .load(std::sync::atomic::Ordering::SeqCst)
        }) {
            return Err(crate::storage::PushStorageError::Backend(
                "injected uncertain child write".to_owned(),
            ));
        }
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
        let value = self
            .inner
            .read()
            .await
            .get(&(family_app(family, app_id), pk.to_owned(), sk.to_owned()))
            .cloned();
        if value.is_none()
            && family == super::constants::FAMILY_STATUS
            && let Some(gate) = &self.missing_parent_read_gate
            && gate.armed.load(std::sync::atomic::Ordering::SeqCst)
            && gate
                .missing_reads_until_pause
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
                == 1
        {
            gate.armed.store(false, std::sync::atomic::Ordering::SeqCst);
            gate.started.notify_one();
            gate.resume.notified().await;
        }
        Ok(value)
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
        self.full_scans
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
    async fn scan_pk_page(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let family_key = family_app(family, app_id);
        let rows: Vec<_> = self
            .inner
            .read()
            .await
            .range((
                Excluded((
                    family_key.clone(),
                    pk.to_owned(),
                    start_after.unwrap_or("").to_owned(),
                )),
                Unbounded,
            ))
            .take_while(|((f, p, _), _)| f == &family_key && p == pk)
            .take(limit.max(1))
            .map(|((_, pk, sk), data)| StoredDocument {
                pk: pk.clone(),
                sk: sk.clone(),
                data: data.clone(),
            })
            .collect();
        self.page_rows
            .fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(rows)
    }

    async fn scan_app_page_by_pk(
        &self,
        family: &'static str,
        app_id: &str,
        start_after_pk: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let family_key = family_app(family, app_id);
        let rows: Vec<_> = self
            .inner
            .read()
            .await
            .range((
                Excluded((
                    family_key.clone(),
                    start_after_pk.unwrap_or("").to_owned(),
                    String::new(),
                )),
                Unbounded,
            ))
            .take_while(|((f, _, _), _)| f == &family_key)
            .filter(|((_, pk, _), _)| start_after_pk.is_none_or(|start| pk.as_str() > start))
            .take(limit.max(1))
            .map(|((_, pk, sk), data)| StoredDocument {
                pk: pk.clone(),
                sk: sk.clone(),
                data: data.clone(),
            })
            .collect();
        self.page_rows
            .fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(rows)
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

#[tokio::test]
async fn ordered_index_migration_resumes_legacy_wide_partitions_and_rolls_back() {
    use super::constants::*;
    use crate::domain::ChannelSubscription;
    use crate::storage::PushSubscriptionStore;
    use std::sync::atomic::Ordering;
    let store = test_store();
    // Legacy writers did not populate any ordered references. Include a wide pk.
    for channel in ["a", "a0", "b"] {
        for index in 0..9 {
            let subscription =
                ChannelSubscription::from_client("app-1", channel, format!("client-{index}"));
            store
                .put_json(
                    FAMILY_SUBSCRIPTION,
                    "app-1",
                    channel,
                    &subscription.device_id,
                    &subscription,
                )
                .await
                .unwrap();
        }
    }
    let legacy = store.list_subscriptions("app-1", 100, None).await.unwrap();
    assert_eq!(legacy.items.len(), 27);
    let mut checkpoint = store
        .migrate_ordered_indexes_page("app-1", None, 4)
        .await
        .unwrap();
    assert!(checkpoint.is_some());
    // A crash before the completion marker leaves legacy reading intact.
    let restarted = DocumentPushStore::with_backend(store.backend.clone());
    assert_eq!(
        restarted
            .list_subscriptions("app-1", 100, None)
            .await
            .unwrap(),
        legacy
    );
    while let Some(cursor) = checkpoint {
        let serialized = sonic_rs::to_string(&cursor).unwrap();
        checkpoint = restarted
            .migrate_ordered_indexes_page(
                "app-1",
                Some(sonic_rs::from_str(&serialized).unwrap()),
                4,
            )
            .await
            .unwrap();
    }
    store.backend.full_scans.store(0, Ordering::Relaxed);
    store.backend.page_rows.store(0, Ordering::Relaxed);
    let mut cursor = None;
    let mut found = Vec::new();
    loop {
        let page = restarted
            .list_subscriptions("app-1", 3, cursor)
            .await
            .unwrap();
        found.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(found, legacy.items);
    assert_eq!(store.backend.full_scans.load(Ordering::Relaxed), 0);
    assert!(store.backend.page_rows.load(Ordering::Relaxed) <= 36);
    restarted.disable_ordered_indexes("app-1").await.unwrap();
    assert_eq!(
        restarted
            .list_subscriptions("app-1", 100, None)
            .await
            .unwrap(),
        legacy
    );
    assert_eq!(store.backend.full_scans.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn ordered_subscription_reads_skip_crash_orphans_without_losing_progress() {
    use super::constants::*;
    use crate::domain::ChannelSubscription;
    use crate::storage::PushSubscriptionStore;
    let store = test_store();
    for index in 0..5 {
        store
            .upsert_subscription(ChannelSubscription::from_client(
                "app-1",
                "room",
                format!("client-{index}"),
            ))
            .await
            .unwrap();
    }
    let mut cursor = store
        .migrate_ordered_indexes_page("app-1", None, 2)
        .await
        .unwrap();
    while let Some(checkpoint) = cursor {
        cursor = store
            .migrate_ordered_indexes_page("app-1", Some(checkpoint), 2)
            .await
            .unwrap();
    }
    // Simulate a crash after canonical removal but before advisory index deletion.
    let deleted = ChannelSubscription::from_client("app-1", "room", "client-0");
    store
        .backend
        .delete(FAMILY_SUBSCRIPTION, "app-1", "room", &deleted.device_id)
        .await
        .unwrap();
    let first = store.list_subscriptions("app-1", 1, None).await.unwrap();
    assert!(first.items.is_empty());
    assert!(first.next_cursor.is_some());
    let remaining = store
        .list_subscriptions("app-1", 10, first.next_cursor)
        .await
        .unwrap();
    assert_eq!(remaining.items.len(), 4);
}

#[tokio::test]
async fn document_lifecycle_retention_survives_restart_and_drains_after_rollback() {
    let store = test_store();
    let restarted = DocumentPushStore::with_backend(store.backend.clone());
    crate::lifecycle::tests::exercise_retention(Arc::new(store), Arc::new(restarted))
        .await
        .unwrap();
}

mod lifecycle;
mod pagination;

#[tokio::test]
async fn document_cleanup_scans_are_bounded_and_progress_after_restart() {
    let store = test_store();
    let restarted = DocumentPushStore::with_backend(store.backend.clone());
    crate::lifecycle::tests::exercise_cleanup_progress(Arc::new(store), Arc::new(restarted)).await;
}
