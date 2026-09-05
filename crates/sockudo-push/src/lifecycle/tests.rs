use super::*;
use crate::cleanup::PushCleanupPolicy;
use crate::domain::{
    FanoutRegime, PublishCounters, PublishIntent, PublishLifecycleState, PublishStatus,
    PublishTarget, PushPayload,
};
use crate::storage::{DynPushStore, PushStorageResult};
use std::sync::Arc;

pub(crate) async fn exercise_retention(
    store: DynPushStore,
    restarted: DynPushStore,
) -> PushStorageResult<()> {
    crate::conformance::PushStoreConformance::assert_indexed_client_pagination(store.as_ref())
        .await?;
    let app = format!("lifecycle-{}", rand::random::<u64>());
    let now = crate::pipeline::now_ms();
    let original_idempotency = crate::storage::IdempotencyRecord {
        app_id: app.clone(),
        key: "insert-contract".to_owned(),
        publish_id: "original".to_owned(),
        expires_at_ms: now + 60_000,
    };
    assert!(
        store
            .put_idempotency_record_if_absent(original_idempotency.clone())
            .await?
    );
    let mut duplicate = original_idempotency.clone();
    duplicate.publish_id = "must-not-replace".to_owned();
    duplicate.expires_at_ms += 60_000;
    assert!(
        !restarted
            .put_idempotency_record_if_absent(duplicate)
            .await?,
        "duplicate conditional insert must return false"
    );
    assert_eq!(
        restarted
            .get_idempotency_record(&app, "insert-contract")
            .await?,
        Some(original_idempotency)
    );
    let event = PublishLogEvent {
        app_id: app.clone(),
        publish_id: "complete".to_owned(),
        event_id: "accepted".to_owned(),
        occurred_at_ms: now,
        intent: PublishIntent {
            app_id: app.clone(),
            publish_id: "complete".to_owned(),
            targets: vec![PublishTarget::Device {
                device_id: "device".to_owned(),
            }],
            payload: PushPayload {
                template_id: None,
                template_data: sonic_rs::json!({}),
                title: Some("kept until exact proof".to_owned()),
                body: None,
                icon: None,
                sound: None,
                collapse_key: None,
            },
            provider_overrides: vec![],
            not_before_ms: None,
            expires_at_ms: Some(now + crate::retry::MAX_RETRY_AGE_MS + 10_000),
        },
        fanout_regime: FanoutRegime::ShardPath,
        expected_recipients: 1,
        fast_threshold: 1,
        shard_size: 1,
    };
    let status = PublishStatus {
        app_id: app.clone(),
        publish_id: event.publish_id.clone(),
        state: PublishLifecycleState::Succeeded,
        counters: PublishCounters {
            planned: 1,
            succeeded: 7,
            dispatched: 7,
            ..Default::default()
        },
        fanout_regime: Some(FanoutRegime::ShardPath),
        retry_after_ms: None,
        error_reason: None,
    };
    store
        .create_publish_status_if_absent(status.clone())
        .await?;
    store.append_publish_log_event(event.clone()).await?;
    // More logs than one maintenance budget must progress just like shard receipts.
    for index in 1..8 {
        let mut extra = event.clone();
        extra.event_id = format!("accepted-{index}");
        extra.occurred_at_ms += index;
        store.append_publish_log_event(extra).await?;
    }
    let mut receipt = ShardJob {
        app_id: app.clone(),
        publish_id: event.publish_id.clone(),
        shard_id: PLANNER_RECEIPT_ID.to_owned(),
        target: event.intent.targets[0].clone(),
        payload: event.intent.payload.clone(),
        provider_overrides: vec![],
        not_before_ms: None,
        expires_at_ms: None,
        cursor: None,
        page_size: 1,
        shard_size: 1,
        emitted_recipients: 0,
        emitted_batches: 0,
        status: ShardJobStatus::Complete,
    };
    store.put_fanout_shard(receipt.clone()).await?;
    for index in 0..7 {
        receipt.shard_id = format!("shard-{index}");
        receipt.emitted_recipients = 1;
        receipt.emitted_batches = 1;
        store.put_fanout_shard(receipt.clone()).await?;
    }
    let policy = PushCleanupPolicy {
        publish_status_retention_ms: 1,
        delivery_event_retention_ms: 0,
        operator_event_retention_ms: 0,
        dead_letter_retention_ms: 0,
        batch_size: 2,
        max_deleted_per_tick: 2,
    };
    // An artificially short status TTL cannot shorten the maximum supported retry horizon.
    store
        .cleanup_expired_push_data(policy.request_at(now + 1_000))
        .await?;
    assert!(store.get_publish_status(&app, "complete").await?.is_some());
    assert_eq!(
        store
            .list_publish_log_events(&app, 10, None)
            .await?
            .items
            .len(),
        8
    );
    let old = now + crate::retry::MAX_RETRY_AGE_MS + 10_000;
    let expected = store
        .get_versioned_publish_status(&app, "complete")
        .await?
        .unwrap();
    exercise_provider_replay_guard(restarted.clone(), &app, "complete", false).await;
    exercise_provider_replay_guard(restarted.clone(), &app, "unknown", false).await;
    // Complete multiple bounded proof passes while the log's expiry is still in
    // the future. Advancing time must retry that proof without a status revision
    // change; a persisted unsafe result must not retain this publish forever.
    let warm_policy = PushCleanupPolicy {
        batch_size: 16,
        ..policy
    };
    for _ in 0..128 {
        let report = restarted
            .cleanup_expired_push_data(warm_policy.request_at(old - 1))
            .await?;
        assert!(report.total_deleted() <= 2);
    }
    assert_eq!(
        restarted
            .get_versioned_publish_status(&app, "complete")
            .await?
            .unwrap()
            .revision,
        expected.revision
    );
    let mut retired = false;
    // Shared document fixtures include feedback/conformance apps. A global maintenance tick
    // visits one app, and each app cursor needs an empty wrap pass between status visits.
    // Sixteen one-row proof steps therefore need up to 16 * 2 * 12 global app visits.
    // Keep the per-tick deletion assertion strict while allowing this fair bounded rotation.
    for _ in 0..512 {
        let report = restarted
            .cleanup_expired_push_data(policy.request_at(old))
            .await?;
        assert!(
            report.total_deleted() <= 2,
            "cleanup exceeded its durable deletion budget"
        );
        if restarted
            .get_publish_status(&app, "complete")
            .await?
            .is_none()
        {
            retired = true;
            break;
        }
    }
    assert!(retired, "bounded restartable receipt scan did not finish");
    assert!(restarted.is_publish_retired(&app, "complete").await?);
    exercise_provider_replay_guard(restarted.clone(), &app, "complete", true).await;
    assert!(
        !restarted
            .compare_and_swap_publish_status(&expected, status.clone())
            .await?
            .applied()
    );
    assert!(
        !restarted
            .create_publish_status_if_absent(status)
            .await?
            .applied()
    );
    assert!(
        restarted.put_fanout_shard(receipt).await.is_err(),
        "retirement must fence late child writes"
    );
    // Already elected cleanup remains resumable after worker restart.
    let drain = policy;
    for _ in 0..512 {
        restarted
            .cleanup_expired_push_data(drain.request_at(old))
            .await?;
    }
    assert!(
        restarted
            .list_publish_log_events(&app, 10, None)
            .await?
            .items
            .is_empty()
    );
    assert!(
        restarted
            .get_fanout_shard(&app, "complete", "shard-6")
            .await?
            .is_none()
    );
    assert!(
        restarted
            .get_fanout_shard(&app, "complete", PLANNER_RECEIPT_ID)
            .await?
            .is_none()
    );
    Ok(())
}

async fn exercise_provider_replay_guard(
    store: DynPushStore,
    app_id: &str,
    publish_id: &str,
    retired: bool,
) {
    use crate::dispatch::{AcceptAllDispatcher, ProviderDispatchWorker};
    use crate::domain::PushProviderKind;
    use crate::pipeline::{MemoryPushQueue, PushQueue, PushQueuePayload, PushQueueStage};
    let provider = PushProviderKind::Fcm;
    let queue = Arc::new(MemoryPushQueue::new());
    let mut batch = crate::dispatch::test_support::batch(provider);
    batch.app_id = app_id.to_owned();
    batch.publish_id = publish_id.to_owned();
    for job in &mut batch.jobs {
        job.app_id = app_id.to_owned();
        job.publish_id = publish_id.to_owned();
    }
    queue
        .produce(
            PushQueueStage::DeliveryJobs(provider),
            batch.queue_key(),
            PushQueuePayload::DeliveryBatch(Box::new(batch)),
        )
        .await
        .unwrap();
    let dispatcher = Arc::new(AcceptAllDispatcher::new(provider));
    let mut worker =
        ProviderDispatchWorker::new(provider, queue.clone(), dispatcher).with_store(store);
    assert_eq!(worker.run_once("lifecycle").await.unwrap(), 1);
    let feedback = queue
        .consume(PushQueueStage::DeliveryResults, "feedback", 10, 30_000)
        .await
        .unwrap();
    assert_eq!(
        feedback.len(),
        usize::from(!retired),
        "only canonical retirement proof suppresses provider replay"
    );
    assert_eq!(
        queue
            .lag(PushQueueStage::DeliveryJobs(provider))
            .await
            .unwrap()
            .ready_depth,
        0
    );
}

#[tokio::test]
async fn memory_lifecycle_retention_survives_interruption_and_keeps_deletion_bounds() {
    let store = Arc::new(crate::memory::MemoryPushStore::new());
    exercise_retention(store.clone(), store).await.unwrap();
}

pub(crate) async fn exercise_cleanup_progress(store: DynPushStore, restarted: DynPushStore) {
    let app = format!("cleanup-progress-{}", rand::random::<u64>());
    let now = crate::pipeline::now_ms();
    for index in 0..32 {
        store
            .put_idempotency_record_if_absent(crate::storage::IdempotencyRecord {
                app_id: app.clone(),
                key: format!("a-live-{index:03}"),
                publish_id: "live".to_owned(),
                expires_at_ms: now + 60_000,
            })
            .await
            .unwrap();
    }
    store
        .put_idempotency_record_if_absent(crate::storage::IdempotencyRecord {
            app_id: app.clone(),
            key: "z-expired".to_owned(),
            publish_id: "expired".to_owned(),
            expires_at_ms: now + 1,
        })
        .await
        .unwrap();
    let policy = PushCleanupPolicy {
        publish_status_retention_ms: 0,
        delivery_event_retention_ms: 0,
        operator_event_retention_ms: 0,
        dead_letter_retention_ms: 0,
        batch_size: 3,
        max_deleted_per_tick: 3,
    };
    let mut deleted = 0;
    for _ in 0..128 {
        let report = restarted
            .cleanup_expired_push_data(policy.request_at(now + 5))
            .await
            .unwrap();
        assert!(
            report.idempotency_records.scanned <= 3,
            "maintenance may not scan past its page budget"
        );
        assert!(report.total_deleted() <= 3);
        deleted += report.idempotency_records.deleted;
        if deleted > 0 {
            break;
        }
    }
    assert_eq!(
        deleted, 1,
        "an active prefix must not starve later expired records"
    );
    assert!(
        restarted
            .get_idempotency_record(&app, "a-live-031")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn memory_cleanup_scans_are_bounded_and_progress_past_active_prefixes() {
    let store = Arc::new(crate::memory::MemoryPushStore::new());
    exercise_cleanup_progress(store.clone(), store).await;
}

#[test]
fn lifecycle_requires_exact_outcomes_and_completed_planner_receipt() {
    let status = crate::storage::VersionedPublishStatus {
        status: PublishStatus {
            app_id: "app".to_owned(),
            publish_id: "publish".to_owned(),
            state: PublishLifecycleState::Succeeded,
            counters: PublishCounters {
                planned: 1,
                succeeded: 1,
                ..Default::default()
            },
            fanout_regime: None,
            retry_after_ms: None,
            error_reason: None,
        },
        revision: 1,
        updated_at_ms: 0,
        pending_feedback: Default::default(),
        pending_children: Default::default(),
    };
    let mut scan = LifecycleScan::new(1);
    scan.emitted_recipients = 2;
    scan.has_planner_receipt = true;
    assert!(
        !scan.proves_complete(&status),
        "an estimated terminal summary is insufficient"
    );
    scan.emitted_recipients = 1;
    assert!(scan.proves_complete(&status));
    scan.has_pending_shard = true;
    assert!(!scan.proves_complete(&status));
    scan.has_pending_shard = false;
    scan.has_planner_receipt = false;
    assert!(
        !scan.proves_complete(&status),
        "legacy missing exact evidence remains retained"
    );
}
