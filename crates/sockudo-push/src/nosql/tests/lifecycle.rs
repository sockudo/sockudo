use super::*;
use crate::cleanup::PushCleanupPolicy;
use crate::domain::{
    FanoutRegime, PublishIntent, PublishLogEvent, PublishTarget, PushPayload, ShardJob,
    ShardJobStatus,
};
use crate::storage::{PushCleanupStore, PushFanoutShardStore, PushPublishLogStore};
use std::sync::atomic::Ordering;

async fn fixture() -> (
    DocumentPushStore<TestDocumentBackend>,
    Arc<ChildWriteGate>,
    ShardJob,
    PushCleanupPolicy,
    u64,
) {
    let gate = Arc::new(ChildWriteGate::default());
    let backend = TestDocumentBackend {
        child_write_gate: Some(gate.clone()),
        ..Default::default()
    };
    let store = DocumentPushStore::with_backend(backend);
    let status = PublishStatus {
        app_id: "race".to_owned(),
        publish_id: "publish".to_owned(),
        state: PublishLifecycleState::Succeeded,
        counters: PublishCounters {
            succeeded: 1,
            planned: 1,
            ..Default::default()
        },
        fanout_regime: None,
        retry_after_ms: None,
        error_reason: None,
    };
    store.create_publish_status_if_absent(status).await.unwrap();
    let now = crate::pipeline::now_ms();
    let target = PublishTarget::Device {
        device_id: "device".to_owned(),
    };
    let payload: PushPayload =
        sonic_rs::from_str(r#"{"templateData":{},"title":"retained"}"#).unwrap();
    let event = PublishLogEvent {
        app_id: "race".to_owned(),
        publish_id: "publish".to_owned(),
        event_id: "accepted".to_owned(),
        occurred_at_ms: now,
        intent: PublishIntent {
            app_id: "race".to_owned(),
            publish_id: "publish".to_owned(),
            targets: vec![target.clone()],
            payload: payload.clone(),
            provider_overrides: vec![],
            not_before_ms: None,
            expires_at_ms: None,
        },
        fanout_regime: FanoutRegime::FastPath,
        expected_recipients: 1,
        fast_threshold: 1,
        shard_size: 1,
    };
    store.append_publish_log_event(event).await.unwrap();
    let shard = ShardJob {
        app_id: "race".to_owned(),
        publish_id: "publish".to_owned(),
        shard_id: crate::lifecycle::PLANNER_RECEIPT_ID.to_owned(),
        target,
        payload,
        provider_overrides: vec![],
        not_before_ms: None,
        expires_at_ms: None,
        cursor: None,
        page_size: 1,
        shard_size: 1,
        emitted_recipients: 1,
        emitted_batches: 1,
        status: ShardJobStatus::Complete,
    };
    store.put_fanout_shard(shard.clone()).await.unwrap();
    let policy = PushCleanupPolicy {
        publish_status_retention_ms: 1,
        delivery_event_retention_ms: 0,
        operator_event_retention_ms: 0,
        dead_letter_retention_ms: 0,
        batch_size: 16,
        max_deleted_per_tick: 16,
    };
    (
        store,
        gate,
        shard,
        policy,
        now + 2 * crate::retry::MAX_RETRY_AGE_MS,
    )
}

#[tokio::test]
async fn paused_child_write_fences_retirement_and_invalidates_the_prior_scan() {
    let (store, gate, mut shard, policy, old) = fixture().await;
    gate.armed.store(true, Ordering::SeqCst);
    shard.shard_id = "late-work".to_owned();
    shard.status = ShardJobStatus::Pending;
    let writer_store = store.clone();
    let writer = tokio::spawn(async move { writer_store.put_fanout_shard(shard).await });
    gate.started.notified().await;
    let expected = store
        .get_versioned_publish_status("race", "publish")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expected.pending_children.len(), 1);
    // Ordinary status writers must retain a paused operation's durable admission.
    assert!(
        store
            .compare_and_swap_publish_status(&expected, expected.status.clone())
            .await
            .unwrap()
            .applied()
    );
    let restarted = DocumentPushStore::with_backend(store.backend.clone());
    for _ in 0..5 {
        restarted
            .cleanup_expired_push_data(policy.request_at(old))
            .await
            .unwrap();
    }
    assert!(
        restarted
            .get_publish_status("race", "publish")
            .await
            .unwrap()
            .is_some()
    );
    gate.resume.notify_one();
    writer.await.unwrap().unwrap();
    for _ in 0..5 {
        restarted
            .cleanup_expired_push_data(policy.request_at(old))
            .await
            .unwrap();
    }
    let current = restarted
        .get_versioned_publish_status("race", "publish")
        .await
        .unwrap()
        .unwrap();
    assert!(current.pending_children.is_empty());
    assert!(
        restarted
            .get_fanout_shard("race", "publish", "late-work")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn cancelled_and_uncertain_child_writes_remain_pinned_after_restart() {
    for uncertain in [false, true] {
        let (store, gate, shard, policy, old) = fixture().await;
        gate.armed.store(true, Ordering::SeqCst);
        gate.fail_after_write.store(uncertain, Ordering::SeqCst);
        let writer_store = store.clone();
        let writer = tokio::spawn(async move { writer_store.put_fanout_shard(shard).await });
        gate.started.notified().await;
        if uncertain {
            gate.resume.notify_one();
            assert!(writer.await.unwrap().is_err());
        } else {
            writer.abort();
            assert!(writer.await.unwrap_err().is_cancelled());
        }
        let restarted = DocumentPushStore::with_backend(store.backend.clone());
        for _ in 0..5 {
            restarted
                .cleanup_expired_push_data(policy.request_at(old))
                .await
                .unwrap();
        }
        let retained = restarted
            .get_versioned_publish_status("race", "publish")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained.pending_children.len(), 1);
        assert_eq!(
            restarted
                .list_publish_log_events("race", 10, None)
                .await
                .unwrap()
                .items
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn child_admissions_are_bounded_and_retired_parents_reject_new_work() {
    let (store, _, shard, policy, old) = fixture().await;
    for token in 0..64 {
        assert!(
            store
                .set_child_admission("race", "publish", &format!("{token:064x}"), true)
                .await
                .unwrap()
        );
    }
    assert!(store.put_fanout_shard(shard).await.is_err());
    for token in 0..64 {
        store
            .set_child_admission("race", "publish", &format!("{token:064x}"), false)
            .await
            .unwrap();
    }
    for _ in 0..5 {
        store
            .cleanup_expired_push_data(policy.request_at(old))
            .await
            .unwrap();
    }
    assert!(
        store
            .get_publish_status("race", "publish")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .set_child_admission("race", "publish", &format!("{:064x}", 65), true)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn paused_orphan_child_is_rejected_after_parent_id_is_reused_and_retired() {
    let (base, _, mut shard, policy, old) = fixture().await;
    let status = base
        .get_publish_status("race", "publish")
        .await
        .unwrap()
        .unwrap();
    base.backend
        .delete(
            super::super::constants::FAMILY_STATUS,
            "race",
            "publish",
            super::super::constants::DEFAULT_SK,
        )
        .await
        .unwrap();
    let gate = Arc::new(ChildWriteGate::default());
    gate.armed.store(true, Ordering::SeqCst);
    gate.missing_reads_until_pause.store(2, Ordering::SeqCst);
    let mut backend = base.backend.clone();
    backend.missing_parent_read_gate = Some(gate.clone());
    let store = DocumentPushStore::with_backend(backend);
    shard.shard_id = "orphan-late-work".to_owned();
    shard.status = ShardJobStatus::Pending;
    let writer_store = store.clone();
    let writer = tokio::spawn(async move { writer_store.put_fanout_shard(shard).await });
    gate.started.notified().await;
    store.create_publish_status_if_absent(status).await.unwrap();
    for _ in 0..8 {
        store
            .cleanup_expired_push_data(policy.request_at(old))
            .await
            .unwrap();
    }
    assert!(store.is_publish_retired("race", "publish").await.unwrap());
    gate.resume.notify_one();
    assert!(writer.await.unwrap().is_err());
    assert!(
        store
            .get_fanout_shard("race", "publish", "orphan-late-work")
            .await
            .unwrap()
            .is_none()
    );
}
