use sockudo_core::error::Error;
use sockudo_webhook::integration::QueueManager;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "push")]
const LOCAL_READY_CAP_PER_STAGE: usize = 4_096;
#[cfg(feature = "push")]
const LOCAL_PENDING_CAP_PER_STAGE: usize = 8_192;
#[cfg(feature = "push")]
const LOCAL_LEASE_TIMEOUT_MS: u64 = 30_000;

#[cfg(feature = "push")]
#[derive(Clone)]
pub(crate) struct QueueManagerPushQueue {
    backend: sockudo_push::PushQueueBackendKind,
    manager: Arc<QueueManager>,
    ownership: QueueManagerPushQueueStageOwnership,
    state: Arc<tokio::sync::Mutex<QueueManagerPushQueueState>>,
    notify: Arc<tokio::sync::Notify>,
    metrics: sockudo_push::PushMetrics,
}

#[cfg(feature = "push")]
#[derive(Clone, Debug)]
pub(crate) struct QueueManagerPushQueueStageOwnership {
    stages: Arc<BTreeSet<sockudo_push::PushQueueStage>>,
}

#[cfg(feature = "push")]
impl QueueManagerPushQueueStageOwnership {
    pub(crate) fn from_config(
        config: &sockudo_core::options::ServerOptions,
        admission: &super::capability::PushAdmissionSnapshot,
    ) -> Self {
        let mut stages = BTreeSet::new();
        if config.push.planner_worker_count > 0 {
            stages.insert(sockudo_push::PushQueueStage::PublishLog);
        }
        if config.push.shard_worker_count > 0 {
            stages.insert(sockudo_push::PushQueueStage::ShardJobs);
        }
        if config.push.feedback_worker_count > 0 {
            stages.insert(sockudo_push::PushQueueStage::DeliveryResults);
        }
        if config.push.retry_worker_count > 0 {
            stages.insert(sockudo_push::PushQueueStage::RetrySchedule);
        }
        stages.insert(sockudo_push::PushQueueStage::DeadLetters);
        for provider in admission.active_providers() {
            stages.insert(sockudo_push::PushQueueStage::DeliveryJobs(provider));
        }
        Self {
            stages: Arc::new(stages),
        }
    }

    #[cfg(test)]
    pub(crate) fn testing(stages: impl IntoIterator<Item = sockudo_push::PushQueueStage>) -> Self {
        Self {
            stages: Arc::new(stages.into_iter().collect()),
        }
    }

    fn can_process(&self, stage: sockudo_push::PushQueueStage) -> bool {
        self.stages.contains(&stage)
    }
}

#[cfg(feature = "push")]
struct QueueManagerPushQueueState {
    next_id: u64,
    started: std::collections::BTreeSet<sockudo_push::PushQueueStage>,
    ready: std::collections::BTreeMap<
        sockudo_push::PushQueueStage,
        std::collections::VecDeque<sockudo_push::QueueMessage>,
    >,
    pending: std::collections::BTreeMap<
        (sockudo_push::PushQueueStage, String),
        tokio::sync::oneshot::Sender<PushQueueAction>,
    >,
}

#[cfg(feature = "push")]
impl Default for QueueManagerPushQueueState {
    fn default() -> Self {
        Self {
            next_id: 1,
            started: std::collections::BTreeSet::new(),
            ready: std::collections::BTreeMap::new(),
            pending: std::collections::BTreeMap::new(),
        }
    }
}

#[cfg(feature = "push")]
#[derive(Debug)]
enum PushQueueAction {
    Ack,
    Nack(Option<u64>),
    DeadLetter(String),
}

#[cfg(feature = "push")]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PushQueueEnvelope {
    message_id: String,
    stage: sockudo_push::PushQueueStage,
    key: String,
    payload: sockudo_push::PushQueuePayload,
    attempt: u32,
    not_before_ms: Option<u64>,
}

#[cfg(feature = "push")]
impl QueueManagerPushQueue {
    pub(crate) fn new(
        backend: sockudo_push::PushQueueBackendKind,
        manager: Arc<QueueManager>,
        ownership: QueueManagerPushQueueStageOwnership,
    ) -> Self {
        Self {
            backend,
            manager,
            ownership,
            state: Arc::new(tokio::sync::Mutex::new(
                QueueManagerPushQueueState::default(),
            )),
            notify: Arc::new(tokio::sync::Notify::new()),
            metrics: sockudo_push::PushMetrics::default(),
        }
    }

    async fn next_message_id(&self) -> String {
        let mut state = self.state.lock().await;
        let id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        format!("push-{:020}", id)
    }

    async fn enqueue_envelope(
        &self,
        envelope: PushQueueEnvelope,
    ) -> sockudo_push::PushQueueResult<()> {
        let queue_name = envelope.stage.logical_topic();
        let job = push_queue_job_data(&envelope)?;
        self.manager
            .add_to_queue(&queue_name, job)
            .await
            .map_err(push_queue_manager_error)
    }

    async fn ensure_stage_processor(
        &self,
        stage: sockudo_push::PushQueueStage,
    ) -> sockudo_push::PushQueueResult<()> {
        if !self.ownership.can_process(stage) {
            return Err(sockudo_push::PushQueueError::UnsupportedBackend {
                backend: "queue-manager",
                reason: "this node is not configured to process the requested push queue stage",
            });
        }
        {
            let mut state = self.state.lock().await;
            if !state.started.insert(stage) {
                return Ok(());
            }
        }

        let queue_name = stage.logical_topic();
        let queue = self.clone();
        match self
            .manager
            .process_queue(
                &queue_name,
                Box::new(move |job| {
                    let queue = queue.clone();
                    Box::pin(async move {
                        queue
                            .handle_queue_job(job)
                            .await
                            .map_err(|error| Error::Internal(error.to_string()))
                    })
                }),
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.state.lock().await.started.remove(&stage);
                Err(push_queue_manager_error(error))
            }
        }
    }

    async fn handle_queue_job(
        &self,
        job: sockudo_core::webhook_types::JobData,
    ) -> sockudo_push::PushQueueResult<()> {
        let envelope = parse_push_queue_job(job)?;
        if let Some(not_before_ms) = envelope.not_before_ms {
            let now = push_queue_now_ms();
            if not_before_ms > now {
                tokio::time::sleep(Duration::from_millis(not_before_ms - now)).await;
            }
        }

        let route =
            sockudo_push::QueueRoute::for_message(envelope.stage, &envelope.key, &envelope.payload);
        let now = push_queue_now_ms();
        let should_requeue = {
            let state = self.state.lock().await;
            let ready_depth = state
                .ready
                .get(&envelope.stage)
                .map_or(0, std::collections::VecDeque::len);
            let pending_depth = state
                .pending
                .keys()
                .filter(|(pending_stage, _)| pending_stage == &envelope.stage)
                .count();
            ready_depth >= LOCAL_READY_CAP_PER_STAGE || pending_depth >= LOCAL_PENDING_CAP_PER_STAGE
        };
        if should_requeue {
            self.requeue_envelope(envelope, Some(now.saturating_add(1_000)), "local_capacity")
                .await?;
            return Ok(());
        }

        let token = sockudo_push::QueueAckToken {
            stage: envelope.stage,
            message_id: envelope.message_id.clone(),
        };
        let message = sockudo_push::QueueMessage {
            message_id: envelope.message_id.clone(),
            stage: envelope.stage,
            key: envelope.key.clone(),
            partition_key: route.partition_key,
            partition: route.partition,
            payload: envelope.payload.clone(),
            attempt: envelope.attempt,
            not_before_ms: envelope.not_before_ms,
            lease_deadline_ms: now.saturating_add(LOCAL_LEASE_TIMEOUT_MS),
            ack: token.clone(),
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut state = self.state.lock().await;
            state
                .pending
                .insert((envelope.stage, envelope.message_id.clone()), tx);
            state
                .ready
                .entry(envelope.stage)
                .or_default()
                .push_back(message);
        }
        self.notify.notify_waiters();

        let action = match tokio::time::timeout(Duration::from_millis(LOCAL_LEASE_TIMEOUT_MS), rx)
            .await
        {
            Ok(Ok(action)) => action,
            Ok(Err(_)) => PushQueueAction::Nack(Some(push_queue_now_ms().saturating_add(1_000))),
            Err(_) => PushQueueAction::Nack(Some(push_queue_now_ms().saturating_add(1_000))),
        };

        self.remove_pending_and_ready(envelope.stage, &envelope.message_id)
            .await;

        match action {
            PushQueueAction::Ack => Ok(()),
            PushQueueAction::Nack(retry_at_ms) => {
                self.requeue_envelope(envelope, retry_at_ms, "nack_or_lease_timeout")
                    .await
            }
            PushQueueAction::DeadLetter(reason) => {
                let dead_letter = sockudo_push::DeadLetter {
                    app_id: push_queue_payload_app_id(&envelope.payload),
                    publish_id: push_queue_payload_publish_id(&envelope.payload),
                    key: envelope.key,
                    stage: format!("{:?}", envelope.stage),
                    reason,
                    occurred_at_ms: push_queue_now_ms(),
                };
                let dlq = PushQueueEnvelope {
                    message_id: self.next_message_id().await,
                    stage: sockudo_push::PushQueueStage::DeadLetters,
                    key: dead_letter.key.clone(),
                    payload: sockudo_push::PushQueuePayload::DeadLetter(Box::new(dead_letter)),
                    attempt: 1,
                    not_before_ms: None,
                };
                self.enqueue_envelope(dlq).await
            }
        }
    }

    async fn requeue_envelope(
        &self,
        mut envelope: PushQueueEnvelope,
        retry_at_ms: Option<u64>,
        reason: &'static str,
    ) -> sockudo_push::PushQueueResult<()> {
        envelope.attempt = envelope.attempt.saturating_add(1);
        envelope.not_before_ms = retry_at_ms;
        self.metrics
            .queue_local_requeued(&push_queue_stage_label(envelope.stage), reason);
        self.enqueue_envelope(envelope).await
    }

    async fn remove_pending_and_ready(
        &self,
        stage: sockudo_push::PushQueueStage,
        message_id: &str,
    ) {
        let mut state = self.state.lock().await;
        state.pending.remove(&(stage, message_id.to_owned()));
        if let Some(ready) = state.ready.get_mut(&stage)
            && let Some(index) = ready
                .iter()
                .position(|message| message.message_id == message_id)
        {
            ready.remove(index);
        }
    }

    async fn complete(
        &self,
        token: sockudo_push::QueueAckToken,
        action: PushQueueAction,
    ) -> sockudo_push::PushQueueResult<()> {
        if let Some(tx) = self
            .state
            .lock()
            .await
            .pending
            .remove(&(token.stage, token.message_id))
        {
            let _ = tx.send(action);
        }
        Ok(())
    }
}

#[cfg(feature = "push")]
#[async_trait::async_trait]
impl sockudo_push::PushQueue for QueueManagerPushQueue {
    fn backend(&self) -> sockudo_push::PushQueueBackendKind {
        self.backend
    }

    async fn produce(
        &self,
        stage: sockudo_push::PushQueueStage,
        key: String,
        payload: sockudo_push::PushQueuePayload,
    ) -> sockudo_push::PushQueueResult<String> {
        let message_id = self.next_message_id().await;
        self.enqueue_envelope(PushQueueEnvelope {
            message_id: message_id.clone(),
            stage,
            key,
            payload,
            attempt: 1,
            not_before_ms: None,
        })
        .await?;
        Ok(message_id)
    }

    async fn retry_at(
        &self,
        stage: sockudo_push::PushQueueStage,
        key: String,
        payload: sockudo_push::PushQueuePayload,
        not_before_ms: u64,
    ) -> sockudo_push::PushQueueResult<String> {
        let message_id = self.next_message_id().await;
        self.enqueue_envelope(PushQueueEnvelope {
            message_id: message_id.clone(),
            stage,
            key,
            payload,
            attempt: 1,
            not_before_ms: Some(not_before_ms),
        })
        .await?;
        Ok(message_id)
    }

    async fn consume(
        &self,
        stage: sockudo_push::PushQueueStage,
        _consumer_group: &str,
        max_messages: usize,
        lease_timeout_ms: u64,
    ) -> sockudo_push::PushQueueResult<Vec<sockudo_push::QueueMessage>> {
        self.ensure_stage_processor(stage).await?;
        if self
            .state
            .lock()
            .await
            .ready
            .get(&stage)
            .is_none_or(|queue| queue.is_empty())
        {
            let _ = tokio::time::timeout(Duration::from_millis(50), self.notify.notified()).await;
        }

        let now = push_queue_now_ms();
        let mut state = self.state.lock().await;
        let mut messages = Vec::new();
        for _ in 0..max_messages.max(1) {
            let Some(mut message) = state.ready.entry(stage).or_default().pop_front() else {
                break;
            };
            if !state
                .pending
                .contains_key(&(stage, message.message_id.clone()))
            {
                continue;
            }
            message.lease_deadline_ms = now.saturating_add(lease_timeout_ms);
            messages.push(message);
        }
        Ok(messages)
    }

    async fn ack(&self, token: sockudo_push::QueueAckToken) -> sockudo_push::PushQueueResult<()> {
        self.complete(token, PushQueueAction::Ack).await
    }

    async fn nack(
        &self,
        token: sockudo_push::QueueAckToken,
        retry_at_ms: Option<u64>,
    ) -> sockudo_push::PushQueueResult<()> {
        self.complete(token, PushQueueAction::Nack(retry_at_ms))
            .await
    }

    async fn dead_letter(
        &self,
        token: sockudo_push::QueueAckToken,
        reason: String,
    ) -> sockudo_push::PushQueueResult<()> {
        self.complete(token, PushQueueAction::DeadLetter(reason))
            .await
    }

    async fn health(&self) -> sockudo_push::PushQueueResult<sockudo_push::QueueHealth> {
        self.manager
            .check_health()
            .await
            .map_err(push_queue_manager_error)?;
        Ok(sockudo_push::QueueHealth {
            backend: self.backend,
            healthy: true,
            details: "push queue is backed by the configured Sockudo queue manager; lag reports this node's local pull-ahead ready/pending depth, not broker-wide backlog".to_owned(),
        })
    }

    async fn lag(
        &self,
        stage: sockudo_push::PushQueueStage,
    ) -> sockudo_push::PushQueueResult<sockudo_push::QueueLagMetrics> {
        let state = self.state.lock().await;
        Ok(sockudo_push::QueueLagMetrics {
            ready_depth: state
                .ready
                .get(&stage)
                .map_or(0, |queue| queue.len() as u64),
            delayed_depth: 0,
            inflight_depth: state
                .pending
                .keys()
                .filter(|(pending_stage, _)| pending_stage == &stage)
                .count() as u64,
            dead_letter_depth: state
                .ready
                .get(&sockudo_push::PushQueueStage::DeadLetters)
                .map_or(0, |queue| queue.len() as u64),
        })
    }
}

#[cfg(feature = "push")]
fn push_queue_job_data(
    envelope: &PushQueueEnvelope,
) -> sockudo_push::PushQueueResult<sockudo_core::webhook_types::JobData> {
    Ok(sockudo_core::webhook_types::JobData {
        app_key: String::new(),
        app_id: push_queue_payload_app_id(&envelope.payload),
        app_secret: String::new(),
        payload: sockudo_core::webhook_types::JobPayload {
            time_ms: push_queue_now_ms().min(i64::MAX as u64) as i64,
            events: vec![
                push_queue_envelope_value(envelope)
                    .map_err(|error| sockudo_push::PushQueueError::Backend(error.to_string()))?,
            ],
        },
        original_signature: "push-queue".to_owned(),
    })
}

#[cfg(feature = "push")]
fn push_queue_envelope_value(
    envelope: &PushQueueEnvelope,
) -> Result<sonic_rs::Value, sonic_rs::Error> {
    let bytes = sonic_rs::to_vec(envelope)?;
    sonic_rs::from_slice(&bytes)
}

#[cfg(feature = "push")]
fn parse_push_queue_job(
    job: sockudo_core::webhook_types::JobData,
) -> sockudo_push::PushQueueResult<PushQueueEnvelope> {
    let event = job.payload.events.into_iter().next().ok_or_else(|| {
        sockudo_push::PushQueueError::Backend("push queue job missing envelope".to_owned())
    })?;
    push_queue_envelope_from_value(&event)
        .map_err(|error| sockudo_push::PushQueueError::Backend(error.to_string()))
}

#[cfg(feature = "push")]
fn push_queue_envelope_from_value(
    value: &sonic_rs::Value,
) -> Result<PushQueueEnvelope, sonic_rs::Error> {
    let bytes = sonic_rs::to_vec(value)?;
    sonic_rs::from_slice(&bytes)
}

#[cfg(feature = "push")]
fn push_queue_manager_error(error: Error) -> sockudo_push::PushQueueError {
    sockudo_push::PushQueueError::Backend(format!("push queue manager error: {error}"))
}

#[cfg(feature = "push")]
fn push_queue_payload_app_id(payload: &sockudo_push::PushQueuePayload) -> String {
    match payload {
        sockudo_push::PushQueuePayload::PublishLog(event) => event.app_id.clone(),
        sockudo_push::PushQueuePayload::ShardJob(job) => job.app_id.clone(),
        sockudo_push::PushQueuePayload::DeliveryBatch(batch) => batch.app_id.clone(),
        sockudo_push::PushQueuePayload::DeliveryResult(result) => result.app_id.clone(),
        sockudo_push::PushQueuePayload::DeliveryFeedback(feedback) => {
            feedback.result.app_id.clone()
        }
        sockudo_push::PushQueuePayload::DeadLetter(dead_letter) => dead_letter.app_id.clone(),
        sockudo_push::PushQueuePayload::RetrySchedule(entry) => entry.app_id.clone(),
    }
}

#[cfg(feature = "push")]
fn push_queue_payload_publish_id(payload: &sockudo_push::PushQueuePayload) -> String {
    match payload {
        sockudo_push::PushQueuePayload::PublishLog(event) => event.publish_id.clone(),
        sockudo_push::PushQueuePayload::ShardJob(job) => job.publish_id.clone(),
        sockudo_push::PushQueuePayload::DeliveryBatch(batch) => batch.publish_id.clone(),
        sockudo_push::PushQueuePayload::DeliveryResult(result) => result.publish_id.clone(),
        sockudo_push::PushQueuePayload::DeliveryFeedback(feedback) => {
            feedback.result.publish_id.clone()
        }
        sockudo_push::PushQueuePayload::DeadLetter(dead_letter) => dead_letter.publish_id.clone(),
        sockudo_push::PushQueuePayload::RetrySchedule(entry) => entry.publish_id.clone(),
    }
}

#[cfg(feature = "push")]
fn push_queue_stage_label(stage: sockudo_push::PushQueueStage) -> String {
    match stage {
        sockudo_push::PushQueueStage::PublishLog => "publish_log".to_owned(),
        sockudo_push::PushQueueStage::ShardJobs => "shard_jobs".to_owned(),
        sockudo_push::PushQueueStage::DeliveryJobs(provider) => {
            format!("delivery_jobs_{}", sockudo_push::provider_label(provider))
        }
        sockudo_push::PushQueueStage::DeliveryResults => "delivery_results".to_owned(),
        sockudo_push::PushQueueStage::DeadLetters => "dead_letters".to_owned(),
        sockudo_push::PushQueueStage::RetrySchedule => "retry_schedule".to_owned(),
    }
}

#[cfg(feature = "push")]
pub(crate) fn push_queue_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(all(test, feature = "push"))]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use sockudo_push::{
        DeliveryBatch, DeliveryJob, PushPayload, PushProviderKind, PushQueue, PushQueuePayload,
        PushQueueStage, PushRecipient, SecretString,
    };
    use sonic_rs::json;

    use super::*;

    #[tokio::test]
    async fn node_without_provider_worker_does_not_consume_provider_stage() {
        let queue = test_queue(QueueManagerPushQueueStageOwnership::testing([
            PushQueueStage::PublishLog,
        ]));
        let stage = PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
        queue
            .produce(
                stage,
                "delivery".to_owned(),
                PushQueuePayload::DeliveryBatch(Box::new(sample_batch())),
            )
            .await
            .unwrap();

        let error = queue.consume(stage, "node-without-fcm", 1, 30_000).await;
        assert!(matches!(
            error,
            Err(sockudo_push::PushQueueError::UnsupportedBackend { .. })
        ));
    }

    #[tokio::test]
    async fn node_with_provider_worker_consumes_provider_stage() {
        let stage = PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
        let queue = test_queue(QueueManagerPushQueueStageOwnership::testing([stage]));
        insert_local_message(&queue, stage).await;

        let message = next_message(&queue, stage).await;
        assert_eq!(message.stage, stage);
        let PushQueuePayload::DeliveryBatch(batch) = &message.payload else {
            panic!("expected delivery batch");
        };
        assert_eq!(batch.provider, PushProviderKind::Fcm);
        queue.ack(message.ack).await.unwrap();
    }

    #[tokio::test]
    async fn produce_does_not_start_provider_stage_processor() {
        let stage = PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
        let queue = test_queue(QueueManagerPushQueueStageOwnership::testing([stage]));
        queue
            .produce(
                stage,
                "delivery".to_owned(),
                PushQueuePayload::DeliveryBatch(Box::new(sample_batch())),
            )
            .await
            .unwrap();

        assert!(queue.state.lock().await.started.get(&stage).is_none());
    }

    #[tokio::test]
    async fn non_provider_stages_are_consumable_when_owned() {
        let stages = [
            PushQueueStage::PublishLog,
            PushQueueStage::ShardJobs,
            PushQueueStage::DeliveryResults,
            PushQueueStage::RetrySchedule,
            PushQueueStage::DeadLetters,
        ];
        let queue = test_queue(QueueManagerPushQueueStageOwnership::testing(stages));

        for stage in stages {
            assert!(
                queue
                    .consume(stage, "pipeline-worker", 1, 30_000)
                    .await
                    .unwrap()
                    .is_empty(),
                "{stage:?}"
            );
        }
    }

    fn test_queue(ownership: QueueManagerPushQueueStageOwnership) -> QueueManagerPushQueue {
        let driver = sockudo_queue::MemoryQueueManager::new();
        driver.start_processing();
        let manager = Arc::new(QueueManager::new(Box::new(driver)));
        QueueManagerPushQueue::new(
            sockudo_push::PushQueueBackendKind::Redis,
            manager,
            ownership,
        )
    }

    async fn next_message(
        queue: &QueueManagerPushQueue,
        stage: PushQueueStage,
    ) -> sockudo_push::QueueMessage {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let mut messages = queue.consume(stage, "worker", 1, 30_000).await.unwrap();
                if let Some(message) = messages.pop() {
                    return message;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap()
    }

    async fn insert_local_message(queue: &QueueManagerPushQueue, stage: PushQueueStage) {
        let payload = PushQueuePayload::DeliveryBatch(Box::new(sample_batch()));
        let route = sockudo_push::QueueRoute::for_message(stage, "delivery", &payload);
        let message_id = "external-1".to_owned();
        let token = sockudo_push::QueueAckToken {
            stage,
            message_id: message_id.clone(),
        };
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let mut state = queue.state.lock().await;
        state.pending.insert((stage, message_id.clone()), tx);
        state
            .ready
            .entry(stage)
            .or_default()
            .push_back(sockudo_push::QueueMessage {
                message_id,
                stage,
                key: "delivery".to_owned(),
                partition_key: route.partition_key,
                partition: route.partition,
                payload,
                attempt: 1,
                not_before_ms: None,
                lease_deadline_ms: 0,
                ack: token,
            });
    }

    fn sample_batch() -> DeliveryBatch {
        DeliveryBatch {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            provider: PushProviderKind::Fcm,
            batch_id: "batch-1".to_owned(),
            jobs: vec![DeliveryJob {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                provider: PushProviderKind::Fcm,
                batch_id: "batch-1".to_owned(),
                device_id: Some("device-1".to_owned()),
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new("fcm-token").unwrap(),
                },
                payload: Arc::new(PushPayload {
                    template_id: None,
                    template_data: json!({}),
                    title: Some("hello".to_owned()),
                    body: Some("body".to_owned()),
                    icon: None,
                    sound: None,
                    collapse_key: Some("collapse".to_owned()),
                }),
                rendered_payload: None,
                attempt: 1,
                first_attempt_at_ms: None,
                not_before_ms: None,
                expires_at_ms: None,
            }],
        }
    }
}
