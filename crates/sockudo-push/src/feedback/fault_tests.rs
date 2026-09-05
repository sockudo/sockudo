use super::*;
use crate::pipeline::{
    MemoryPushQueue, PushQueue, PushQueueBackendKind, PushQueueError, PushQueueResult,
    QueueAckToken, QueueHealth, QueueLagMetrics,
};
use crate::storage::{PushDeliveryEventStore, PushDeviceStore, PushPublishStatusStore};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct FaultQueue {
    inner: MemoryPushQueue,
    fail: AtomicBool,
}
#[async_trait::async_trait]
impl PushQueue for FaultQueue {
    fn backend(&self) -> PushQueueBackendKind {
        self.inner.backend()
    }
    async fn produce(
        &self,
        stage: PushQueueStage,
        key: String,
        payload: PushQueuePayload,
    ) -> PushQueueResult<String> {
        if stage == PushQueueStage::DeadLetters
            && key.ends_with('1')
            && self.fail.load(Ordering::SeqCst)
        {
            return Err(PushQueueError::Backend(
                "injected successor write failure".into(),
            ));
        }
        self.inner.produce(stage, key, payload).await
    }
    async fn retry_at(
        &self,
        stage: PushQueueStage,
        key: String,
        payload: PushQueuePayload,
        at: u64,
    ) -> PushQueueResult<String> {
        self.inner.retry_at(stage, key, payload, at).await
    }
    async fn consume(
        &self,
        stage: PushQueueStage,
        group: &str,
        max: usize,
        lease: u64,
    ) -> PushQueueResult<Vec<QueueMessage>> {
        self.inner.consume(stage, group, max, lease).await
    }
    async fn ack(&self, ack: QueueAckToken) -> PushQueueResult<()> {
        self.inner.ack(ack).await
    }
    async fn nack(&self, ack: QueueAckToken, at: Option<u64>) -> PushQueueResult<()> {
        self.inner.nack(ack, at).await
    }
    async fn dead_letter(&self, ack: QueueAckToken, reason: String) -> PushQueueResult<()> {
        self.inner.dead_letter(ack, reason).await
    }
    async fn health(&self) -> PushQueueResult<QueueHealth> {
        self.inner.health().await
    }
    async fn lag(&self, stage: PushQueueStage) -> PushQueueResult<QueueLagMetrics> {
        self.inner.lag(stage).await
    }
}

#[tokio::test]
async fn partial_successor_failure_nacks_source_and_replays_only_unfinished_effects() {
    let store = Arc::new(crate::memory::MemoryPushStore::new());
    let queue = Arc::new(FaultQueue {
        inner: MemoryPushQueue::new(),
        fail: AtomicBool::new(true),
    });
    store
        .put_publish_status(super::tests::status_with_planned("publish-1", 4))
        .await
        .unwrap();
    for number in 0..4 {
        let device_id = format!("device-{number}");
        store
            .upsert_device(super::tests::device(&device_id))
            .await
            .unwrap();
        let result = super::tests::rejected_result(
            "publish-1",
            &device_id,
            "transient",
            ProviderFailureClass::DeviceTransient,
        );
        queue
            .produce(
                PushQueueStage::DeliveryResults,
                format!("source-{number}"),
                PushQueuePayload::DeliveryResult(Box::new(result)),
            )
            .await
            .unwrap();
    }
    let processor = PushFeedbackProcessor::new(store.clone(), queue.clone());
    assert_eq!(processor.run_once("feedback").await.unwrap(), 0);
    let lag = queue.lag(PushQueueStage::DeliveryResults).await.unwrap();
    assert_eq!(lag.delayed_depth, 4);
    assert_eq!(lag.dead_letter_depth, 3);
    assert_eq!(
        store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap()
            .counters
            .failed,
        3
    );
    queue.fail.store(false, Ordering::SeqCst);
    tokio::time::sleep(std::time::Duration::from_millis(260)).await;
    let restarted = PushFeedbackProcessor::new(store.clone(), queue.clone());
    assert_eq!(restarted.run_once("feedback").await.unwrap(), 4);
    assert_eq!(
        store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap()
            .counters
            .failed,
        4
    );
    for number in 0..4 {
        assert_eq!(
            store
                .get_device("app-1", &format!("device-{number}"))
                .await
                .unwrap()
                .unwrap()
                .push
                .failure_count,
            1
        );
    }
    assert_eq!(
        store
            .list_delivery_events("app-1", "publish-1", 10, None)
            .await
            .unwrap()
            .items
            .len(),
        4
    );
    assert_eq!(
        queue
            .lag(PushQueueStage::DeadLetters)
            .await
            .unwrap()
            .ready_depth,
        4
    );
    assert_eq!(
        queue
            .lag(PushQueueStage::DeliveryResults)
            .await
            .unwrap()
            .inflight_depth,
        0
    );
}
