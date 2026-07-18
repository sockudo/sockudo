use sockudo_core::error::Error;
use sockudo_webhook::integration::QueueManager;
use sonic_rs::JsonValueTrait;
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
    local_ready_cap_per_stage: usize,
    local_pending_cap_per_stage: usize,
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
        QueueManagerPendingMessage,
    >,
    dead_letters: Vec<QueueManagerDeadLetterRecord>,
}

#[cfg(feature = "push")]
impl Default for QueueManagerPushQueueState {
    fn default() -> Self {
        Self {
            next_id: 1,
            started: std::collections::BTreeSet::new(),
            ready: std::collections::BTreeMap::new(),
            pending: std::collections::BTreeMap::new(),
            dead_letters: Vec::new(),
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
enum LocalQueueAdmission {
    Admitted {
        action_rx: tokio::sync::oneshot::Receiver<PushQueueAction>,
        completion_tx: tokio::sync::watch::Sender<Option<bool>>,
    },
    AtCapacity,
    Duplicate(tokio::sync::watch::Receiver<Option<bool>>),
}

#[cfg(feature = "push")]
struct QueueManagerPendingMessage {
    enqueued_at_ms: u64,
    tx: Option<tokio::sync::oneshot::Sender<PushQueueAction>>,
    completion_tx: tokio::sync::watch::Sender<Option<bool>>,
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
    enqueued_at_ms: u64,
    not_before_ms: Option<u64>,
}

#[cfg(feature = "push")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PushQueueEnvelopeWire {
    message_id: String,
    stage: sockudo_push::PushQueueStage,
    key: String,
    payload_json: String,
    attempt: u32,
    enqueued_at_ms: Option<u64>,
    not_before_ms: Option<u64>,
}

#[cfg(feature = "push")]
impl TryFrom<&PushQueueEnvelope> for PushQueueEnvelopeWire {
    type Error = sockudo_push::PushQueueError;

    fn try_from(envelope: &PushQueueEnvelope) -> Result<Self, Self::Error> {
        Ok(Self {
            message_id: envelope.message_id.clone(),
            stage: envelope.stage,
            key: envelope.key.clone(),
            payload_json: sonic_rs::to_string(&envelope.payload)
                .map_err(|error| sockudo_push::PushQueueError::Backend(error.to_string()))?,
            attempt: envelope.attempt,
            enqueued_at_ms: Some(envelope.enqueued_at_ms),
            not_before_ms: envelope.not_before_ms,
        })
    }
}

#[cfg(feature = "push")]
impl TryFrom<PushQueueEnvelopeWire> for PushQueueEnvelope {
    type Error = sockudo_push::PushQueueError;

    fn try_from(wire: PushQueueEnvelopeWire) -> Result<Self, Self::Error> {
        Ok(Self {
            message_id: wire.message_id,
            stage: wire.stage,
            key: wire.key,
            payload: push_queue_payload_from_json(&wire.payload_json)?,
            attempt: wire.attempt,
            enqueued_at_ms: wire.enqueued_at_ms.unwrap_or(0),
            not_before_ms: wire.not_before_ms,
        })
    }
}

#[cfg(feature = "push")]
#[derive(Clone)]
struct QueueManagerDeadLetterRecord {
    dead_letter_id: String,
    dead_letter: sockudo_push::DeadLetter,
    provider: Option<sockudo_push::PushProviderKind>,
    original_stage: Option<sockudo_push::PushQueueStage>,
    original_key: Option<String>,
    original_payload: Option<sockudo_push::PushQueuePayload>,
}

#[cfg(feature = "push")]
impl QueueManagerDeadLetterRecord {
    fn entry(&self) -> sockudo_push::DeadLetterQueueEntry {
        sockudo_push::DeadLetterQueueEntry {
            dead_letter_id: self.dead_letter_id.clone(),
            dead_letter: self.dead_letter.clone(),
            provider: self.provider,
            replayable: self.original_stage.is_some()
                && self.original_key.is_some()
                && self.original_payload.is_some(),
        }
    }
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
            local_ready_cap_per_stage: LOCAL_READY_CAP_PER_STAGE,
            local_pending_cap_per_stage: LOCAL_PENDING_CAP_PER_STAGE,
        }
    }

    #[cfg(test)]
    fn with_local_caps(mut self, ready: usize, pending: usize) -> Self {
        assert!(ready > 0, "local ready capacity must be positive");
        assert!(pending > 0, "local pending capacity must be positive");
        self.local_ready_cap_per_stage = ready;
        self.local_pending_cap_per_stage = pending;
        self
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
        let admission = {
            let mut state = self.state.lock().await;
            let pending_key = (envelope.stage, envelope.message_id.clone());
            if let Some(pending) = state.pending.get(&pending_key) {
                LocalQueueAdmission::Duplicate(pending.completion_tx.subscribe())
            } else {
                let ready_depth = state
                    .ready
                    .get(&envelope.stage)
                    .map_or(0, std::collections::VecDeque::len);
                let pending_depth = state
                    .pending
                    .keys()
                    .filter(|(pending_stage, _)| pending_stage == &envelope.stage)
                    .count();
                if ready_depth >= self.local_ready_cap_per_stage
                    || pending_depth >= self.local_pending_cap_per_stage
                {
                    LocalQueueAdmission::AtCapacity
                } else {
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
                        enqueued_at_ms: envelope.enqueued_at_ms,
                        not_before_ms: envelope.not_before_ms,
                        lease_deadline_ms: now.saturating_add(LOCAL_LEASE_TIMEOUT_MS),
                        ack: token,
                    };
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let (completion_tx, _) = tokio::sync::watch::channel(None);
                    let previous = state.pending.insert(
                        pending_key,
                        QueueManagerPendingMessage {
                            enqueued_at_ms: envelope.enqueued_at_ms,
                            tx: Some(tx),
                            completion_tx: completion_tx.clone(),
                        },
                    );
                    debug_assert!(
                        previous.is_none(),
                        "duplicate pending push message passed the admission check"
                    );
                    let ready_depth = {
                        let ready = state.ready.entry(envelope.stage).or_default();
                        ready.push_back(message);
                        ready.len()
                    };
                    debug_assert!(ready_depth <= self.local_ready_cap_per_stage);
                    debug_assert!(
                        pending_depth.saturating_add(1) <= self.local_pending_cap_per_stage
                    );
                    LocalQueueAdmission::Admitted {
                        action_rx: rx,
                        completion_tx,
                    }
                }
            }
        };

        let (rx, completion_tx) = match admission {
            LocalQueueAdmission::Admitted {
                action_rx,
                completion_tx,
            } => (action_rx, completion_tx),
            LocalQueueAdmission::AtCapacity => {
                self.requeue_envelope(envelope, Some(now.saturating_add(1_000)), "local_capacity")
                    .await?;
                return Ok(());
            }
            LocalQueueAdmission::Duplicate(mut completion_rx) => {
                self.metrics.duplicate_suppressed();
                let completed = if let Some(completed) = *completion_rx.borrow() {
                    completed
                } else {
                    match tokio::time::timeout(
                        Duration::from_millis(LOCAL_LEASE_TIMEOUT_MS.saturating_add(5_000)),
                        completion_rx.wait_for(Option::is_some),
                    )
                    .await
                    {
                        Ok(Ok(completed)) => completed.unwrap_or(false),
                        Ok(Err(_)) | Err(_) => {
                            return Err(sockudo_push::PushQueueError::Backend(
                                "canonical duplicate push work did not complete".to_owned(),
                            ));
                        }
                    }
                };
                return if completed {
                    Ok(())
                } else {
                    Err(sockudo_push::PushQueueError::Backend(
                        "canonical duplicate push work failed".to_owned(),
                    ))
                };
            }
        };
        self.notify.notify_waiters();

        let action = match tokio::time::timeout(Duration::from_millis(LOCAL_LEASE_TIMEOUT_MS), rx)
            .await
        {
            Ok(Ok(action)) => action,
            Ok(Err(_)) => PushQueueAction::Nack(Some(push_queue_now_ms().saturating_add(1_000))),
            Err(_) => PushQueueAction::Nack(Some(push_queue_now_ms().saturating_add(1_000))),
        };

        let pending_stage = envelope.stage;
        let pending_message_id = envelope.message_id.clone();
        let result = match action {
            PushQueueAction::Ack => Ok(()),
            PushQueueAction::Nack(retry_at_ms) => {
                self.requeue_envelope(envelope, retry_at_ms, "nack_or_lease_timeout")
                    .await
            }
            PushQueueAction::DeadLetter(reason) => {
                let dead_letter = sockudo_push::DeadLetter {
                    app_id: push_queue_payload_app_id(&envelope.payload),
                    publish_id: push_queue_payload_publish_id(&envelope.payload),
                    key: envelope.key.clone(),
                    stage: format!("{:?}", envelope.stage),
                    reason,
                    occurred_at_ms: push_queue_now_ms(),
                };
                {
                    self.state
                        .lock()
                        .await
                        .dead_letters
                        .push(QueueManagerDeadLetterRecord {
                            dead_letter_id: sockudo_push::dead_letter_entry_id(
                                &dead_letter,
                                &envelope.message_id,
                            ),
                            dead_letter: dead_letter.clone(),
                            provider: push_queue_payload_provider(&envelope.payload),
                            original_stage: Some(envelope.stage),
                            original_key: Some(envelope.key.clone()),
                            original_payload: Some(envelope.payload.clone()),
                        });
                }
                let dlq = PushQueueEnvelope {
                    message_id: self.next_message_id().await,
                    stage: sockudo_push::PushQueueStage::DeadLetters,
                    key: dead_letter.key.clone(),
                    payload: sockudo_push::PushQueuePayload::DeadLetter(Box::new(dead_letter)),
                    attempt: 1,
                    enqueued_at_ms: push_queue_now_ms(),
                    not_before_ms: None,
                };
                self.enqueue_envelope(dlq).await
            }
        };
        let _ = completion_tx.send(Some(result.is_ok()));
        self.remove_pending_and_ready(pending_stage, &pending_message_id)
            .await;
        result
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
        let key = (token.stage, token.message_id);
        let mut state = self.state.lock().await;
        let send_failed = if let Some(pending) = state.pending.get_mut(&key)
            && let Some(tx) = pending.tx.take()
        {
            match tx.send(action) {
                Ok(()) => None,
                Err(_) => Some(pending.completion_tx.clone()),
            }
        } else {
            None
        };
        if let Some(completion_tx) = send_failed {
            let _ = completion_tx.send(Some(false));
            state.pending.remove(&key);
            if let Some(ready) = state.ready.get_mut(&key.0)
                && let Some(index) = ready.iter().position(|message| message.message_id == key.1)
            {
                ready.remove(index);
            }
            return Err(sockudo_push::PushQueueError::Backend(
                "canonical local push work is no longer running".to_owned(),
            ));
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
        let dead_letter_marker = push_queue_direct_dead_letter(stage, &payload);
        self.enqueue_envelope(PushQueueEnvelope {
            message_id: message_id.clone(),
            stage,
            key,
            payload,
            attempt: 1,
            enqueued_at_ms: push_queue_now_ms(),
            not_before_ms: None,
        })
        .await?;
        if let Some(dead_letter) = dead_letter_marker {
            self.state
                .lock()
                .await
                .dead_letters
                .push(QueueManagerDeadLetterRecord {
                    dead_letter_id: sockudo_push::dead_letter_entry_id(&dead_letter, &message_id),
                    dead_letter,
                    provider: None,
                    original_stage: None,
                    original_key: None,
                    original_payload: None,
                });
        }
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
        let dead_letter_marker = push_queue_direct_dead_letter(stage, &payload);
        self.enqueue_envelope(PushQueueEnvelope {
            message_id: message_id.clone(),
            stage,
            key,
            payload,
            attempt: 1,
            enqueued_at_ms: push_queue_now_ms(),
            not_before_ms: Some(not_before_ms),
        })
        .await?;
        if let Some(dead_letter) = dead_letter_marker {
            self.state
                .lock()
                .await
                .dead_letters
                .push(QueueManagerDeadLetterRecord {
                    dead_letter_id: sockudo_push::dead_letter_entry_id(&dead_letter, &message_id),
                    dead_letter,
                    provider: None,
                    original_stage: None,
                    original_key: None,
                    original_payload: None,
                });
        }
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

    async fn list_dead_letters(
        &self,
        app_id: &str,
        filter: sockudo_push::DeadLetterQueueFilter,
        limit: usize,
        cursor: Option<sockudo_push::PushCursor>,
    ) -> sockudo_push::PushQueueResult<sockudo_push::Page<sockudo_push::DeadLetterQueueEntry>> {
        let entries = self
            .state
            .lock()
            .await
            .dead_letters
            .iter()
            .map(QueueManagerDeadLetterRecord::entry)
            .collect::<Vec<_>>();
        sockudo_push::page_dead_letter_entries(app_id, filter, limit, cursor, entries)
    }

    async fn replay_dead_letter(
        &self,
        app_id: &str,
        dead_letter_id: &str,
    ) -> sockudo_push::PushQueueResult<Option<sockudo_push::DeadLetterReplayResult>> {
        let replay = {
            let state = self.state.lock().await;
            let Some(stored) = state.dead_letters.iter().find(|stored| {
                stored.dead_letter.app_id == app_id && stored.dead_letter_id == dead_letter_id
            }) else {
                return Ok(None);
            };
            let (Some(stage), Some(key), Some(payload)) = (
                stored.original_stage,
                stored.original_key.clone(),
                stored.original_payload.clone(),
            ) else {
                return Ok(Some(sockudo_push::DeadLetterReplayResult {
                    dead_letter_id: dead_letter_id.to_owned(),
                    replayed: false,
                    message_id: None,
                    reason: Some(
                        "dead-letter does not retain an original queue payload".to_owned(),
                    ),
                }));
            };
            (stage, key, payload)
        };
        let message_id = self.next_message_id().await;
        self.enqueue_envelope(PushQueueEnvelope {
            message_id: message_id.clone(),
            stage: replay.0,
            key: replay.1,
            payload: replay.2,
            attempt: 1,
            enqueued_at_ms: push_queue_now_ms(),
            not_before_ms: None,
        })
        .await?;
        Ok(Some(sockudo_push::DeadLetterReplayResult {
            dead_letter_id: dead_letter_id.to_owned(),
            replayed: true,
            message_id: Some(message_id),
            reason: None,
        }))
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
            ready_depth: ready_queue_depth(&state, stage),
            delayed_depth: 0,
            inflight_depth: pending_stage_depth(&state, stage),
            dead_letter_depth: state.dead_letters.len() as u64,
            oldest_ready_age_ms: oldest_ready_age_ms(&state, stage),
            oldest_delayed_age_ms: None,
            oldest_inflight_age_ms: oldest_pending_age_ms(&state, stage),
        })
    }
}

#[cfg(feature = "push")]
fn ready_queue_depth(
    state: &QueueManagerPushQueueState,
    stage: sockudo_push::PushQueueStage,
) -> u64 {
    state
        .ready
        .get(&stage)
        .map_or(0, |queue| queue.len() as u64)
}

#[cfg(feature = "push")]
fn pending_stage_depth(
    state: &QueueManagerPushQueueState,
    stage: sockudo_push::PushQueueStage,
) -> u64 {
    state
        .pending
        .keys()
        .filter(|(pending_stage, _)| pending_stage == &stage)
        .count() as u64
}

#[cfg(feature = "push")]
fn oldest_ready_age_ms(
    state: &QueueManagerPushQueueState,
    stage: sockudo_push::PushQueueStage,
) -> Option<u64> {
    let now = push_queue_now_ms();
    state.ready.get(&stage).and_then(|queue| {
        queue
            .iter()
            .map(|message| now.saturating_sub(message.enqueued_at_ms))
            .max()
    })
}

#[cfg(feature = "push")]
fn oldest_pending_age_ms(
    state: &QueueManagerPushQueueState,
    stage: sockudo_push::PushQueueStage,
) -> Option<u64> {
    let now = push_queue_now_ms();
    state
        .pending
        .iter()
        .filter_map(|((pending_stage, _), pending)| {
            (*pending_stage == stage).then_some(now.saturating_sub(pending.enqueued_at_ms))
        })
        .max()
}

#[cfg(feature = "push")]
fn push_queue_job_data(
    envelope: &PushQueueEnvelope,
) -> sockudo_push::PushQueueResult<sockudo_core::webhook_types::JobData> {
    Ok(sockudo_core::webhook_types::JobData {
        job_id: None,
        app_key: String::new(),
        app_id: push_queue_payload_app_id(&envelope.payload),
        app_secret: String::new(),
        payload: sockudo_core::webhook_types::JobPayload {
            time_ms: push_queue_now_ms().min(i64::MAX as u64) as i64,
            events: vec![push_queue_envelope_value(envelope)?],
        },
        original_signature: "push-queue".to_owned(),
    })
}

#[cfg(feature = "push")]
fn push_queue_envelope_value(
    envelope: &PushQueueEnvelope,
) -> sockudo_push::PushQueueResult<sonic_rs::Value> {
    let wire = PushQueueEnvelopeWire::try_from(envelope)?;
    let json = serde_json::to_string(&wire)
        .map_err(|error| sockudo_push::PushQueueError::Backend(error.to_string()))?;
    sonic_rs::to_value(&json)
        .map_err(|error| sockudo_push::PushQueueError::Backend(error.to_string()))
}

#[cfg(feature = "push")]
fn parse_push_queue_job(
    job: sockudo_core::webhook_types::JobData,
) -> sockudo_push::PushQueueResult<PushQueueEnvelope> {
    let fallback_enqueued_at_ms = u64::try_from(job.payload.time_ms)
        .ok()
        .filter(|time_ms| *time_ms > 0)
        .unwrap_or_else(push_queue_now_ms);
    let event = job.payload.events.into_iter().next().ok_or_else(|| {
        sockudo_push::PushQueueError::Backend("push queue job missing envelope".to_owned())
    })?;
    let mut envelope = push_queue_envelope_from_value(&event)?;
    if envelope.enqueued_at_ms == 0 {
        envelope.enqueued_at_ms = fallback_enqueued_at_ms;
    }
    Ok(envelope)
}

#[cfg(feature = "push")]
fn push_queue_envelope_from_value(
    value: &sonic_rs::Value,
) -> sockudo_push::PushQueueResult<PushQueueEnvelope> {
    let raw = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string());
    match serde_json::from_str::<PushQueueEnvelopeWire>(&raw) {
        Ok(wire) => PushQueueEnvelope::try_from(wire),
        Err(wire_error) => sonic_rs::from_str(&raw).map_err(|legacy_error| {
            sockudo_push::PushQueueError::Backend(format!(
                "invalid push queue envelope: {wire_error}; legacy decode failed: {legacy_error}"
            ))
        }),
    }
}

#[cfg(feature = "push")]
fn push_queue_payload_from_json(
    raw: &str,
) -> sockudo_push::PushQueueResult<sockudo_push::PushQueuePayload> {
    let value: sonic_rs::Value = sonic_rs::from_str(raw)
        .map_err(|error| sockudo_push::PushQueueError::Backend(error.to_string()))?;
    let kind = value
        .get("kind")
        .and_then(sonic_rs::Value::as_str)
        .ok_or_else(|| {
            sockudo_push::PushQueueError::Backend("push queue payload missing kind".to_owned())
        })?;
    match kind {
        "publishLog" => Ok(sockudo_push::PushQueuePayload::PublishLog(Box::new(
            push_queue_payload_decode(raw)?,
        ))),
        "shardJob" => Ok(sockudo_push::PushQueuePayload::ShardJob(Box::new(
            push_queue_payload_decode(raw)?,
        ))),
        "deliveryBatch" => Ok(sockudo_push::PushQueuePayload::DeliveryBatch(Box::new(
            push_queue_payload_decode(raw)?,
        ))),
        "deliveryResult" => Ok(sockudo_push::PushQueuePayload::DeliveryResult(Box::new(
            push_queue_payload_decode(raw)?,
        ))),
        "deliveryFeedback" => Ok(sockudo_push::PushQueuePayload::DeliveryFeedback(Box::new(
            push_queue_payload_decode(raw)?,
        ))),
        "deadLetter" => Ok(sockudo_push::PushQueuePayload::DeadLetter(Box::new(
            push_queue_payload_decode(raw)?,
        ))),
        "retrySchedule" => Ok(sockudo_push::PushQueuePayload::RetrySchedule(Box::new(
            push_queue_payload_decode(raw)?,
        ))),
        _ => Err(sockudo_push::PushQueueError::Backend(format!(
            "unsupported push queue payload kind {kind}"
        ))),
    }
}

#[cfg(feature = "push")]
fn push_queue_payload_decode<T>(raw: &str) -> sockudo_push::PushQueueResult<T>
where
    T: serde::de::DeserializeOwned,
{
    sonic_rs::from_str(raw)
        .map_err(|error| sockudo_push::PushQueueError::Backend(error.to_string()))
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
fn push_queue_payload_provider(
    payload: &sockudo_push::PushQueuePayload,
) -> Option<sockudo_push::PushProviderKind> {
    match payload {
        sockudo_push::PushQueuePayload::DeliveryBatch(batch) => Some(batch.provider),
        sockudo_push::PushQueuePayload::DeliveryResult(result) => Some(result.provider),
        sockudo_push::PushQueuePayload::DeliveryFeedback(feedback) => {
            Some(feedback.result.provider)
        }
        sockudo_push::PushQueuePayload::RetrySchedule(entry) => entry.provider,
        sockudo_push::PushQueuePayload::PublishLog(_)
        | sockudo_push::PushQueuePayload::ShardJob(_)
        | sockudo_push::PushQueuePayload::DeadLetter(_) => None,
    }
}

#[cfg(feature = "push")]
fn push_queue_direct_dead_letter(
    stage: sockudo_push::PushQueueStage,
    payload: &sockudo_push::PushQueuePayload,
) -> Option<sockudo_push::DeadLetter> {
    match (stage, payload) {
        (
            sockudo_push::PushQueueStage::DeadLetters,
            sockudo_push::PushQueuePayload::DeadLetter(dead_letter),
        ) => Some(dead_letter.as_ref().clone()),
        _ => None,
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
    use tokio::sync::Barrier;

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
    async fn lag_reports_local_ready_and_pending_age() {
        let stage = PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
        let queue = test_queue(QueueManagerPushQueueStageOwnership::testing([stage]));
        insert_local_message(&queue, stage).await;

        let lag = queue.lag(stage).await.unwrap();
        assert_eq!(lag.ready_depth, 1);
        assert_eq!(lag.inflight_depth, 1);
        assert!(lag.oldest_ready_age_ms.is_some());
        assert!(lag.oldest_inflight_age_ms.is_some());

        let message = next_message(&queue, stage).await;
        let lag = queue.lag(stage).await.unwrap();
        assert_eq!(lag.ready_depth, 0);
        assert_eq!(lag.inflight_depth, 1);
        assert!(lag.oldest_ready_age_ms.is_none());
        assert!(lag.oldest_inflight_age_ms.is_some());

        queue.ack(message.ack).await.unwrap();
        let lag = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let lag = queue.lag(stage).await.unwrap();
                if lag.inflight_depth == 0 {
                    break lag;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("acknowledged local fixture should leave inflight state");
        assert_eq!(lag.inflight_depth, 0);
        assert!(lag.oldest_inflight_age_ms.is_none());
    }

    #[tokio::test]
    async fn duplicate_broker_envelope_does_not_replace_pending_work() {
        let stage = PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
        let queue =
            test_queue(QueueManagerPushQueueStageOwnership::testing([stage])).with_local_caps(2, 2);
        let envelope = sample_queue_envelope("external-duplicate", stage);
        let original = spawn_queue_job(&queue, envelope.clone());
        wait_for_local_depths(&queue, stage, 1, 1).await;

        let duplicate_job = push_queue_job_data(&envelope).unwrap();
        let duplicate_queue = queue.clone();
        let duplicate =
            tokio::spawn(async move { duplicate_queue.handle_queue_job(duplicate_job).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while queue.metrics.get("sockudo_push_duplicate_suppressed_total") == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("duplicate should reach local suppression");

        assert!(
            !original.is_finished(),
            "original pending work was replaced"
        );
        assert!(
            !duplicate.is_finished(),
            "duplicate broker work acknowledged before the canonical job"
        );
        assert_eq!(local_depths(&queue, stage).await, (1, 1));
        assert_eq!(
            queue.metrics.get("sockudo_push_duplicate_suppressed_total"),
            1
        );

        queue
            .ack(first_ready_ack(&queue, stage).await)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), original)
            .await
            .expect("original pending work should finish after acknowledgement")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), duplicate)
            .await
            .expect("duplicate should finish with the canonical acknowledgement")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_canonical_job_releases_pending_capacity() {
        let stage = PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
        let queue =
            test_queue(QueueManagerPushQueueStageOwnership::testing([stage])).with_local_caps(1, 1);
        let canonical = spawn_queue_job(&queue, sample_queue_envelope("external-cancelled", stage));
        wait_for_local_depths(&queue, stage, 1, 1).await;
        let ack = first_ready_ack(&queue, stage).await;

        canonical.abort();
        assert!(canonical.await.unwrap_err().is_cancelled());
        let error = queue.ack(ack).await.unwrap_err();

        assert!(error.to_string().contains("no longer running"));
        assert_eq!(local_depths(&queue, stage).await, (0, 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_local_admission_respects_ready_and_pending_caps() {
        const JOBS: usize = 8;

        let stage = PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
        let queue =
            test_queue(QueueManagerPushQueueStageOwnership::testing([stage])).with_local_caps(1, 1);
        let barrier = Arc::new(Barrier::new(JOBS));
        let mut handles = Vec::with_capacity(JOBS);

        for index in 0..JOBS {
            let queue = queue.clone();
            let barrier = Arc::clone(&barrier);
            let envelope = sample_queue_envelope(&format!("external-{index}"), stage);
            handles.push(tokio::spawn(async move {
                let job = push_queue_job_data(&envelope).unwrap();
                barrier.wait().await;
                queue.handle_queue_job(job).await.unwrap();
            }));
        }

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if queue.metrics.get("sockudo_push_queue_local_requeued_total") == (JOBS - 1) as u64
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("excess local work should be requeued");

        assert_eq!(local_depths(&queue, stage).await, (1, 1));
        queue
            .ack(first_ready_ack(&queue, stage).await)
            .await
            .unwrap();

        for handle in handles {
            tokio::time::timeout(Duration::from_secs(1), handle)
                .await
                .expect("queue job should finish after capacity handling")
                .unwrap();
        }
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

        assert!(!queue.state.lock().await.started.contains(&stage));
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

    #[tokio::test]
    async fn dead_letters_are_listed_and_replayed() {
        let stage = PushQueueStage::PublishLog;
        let event = sample_publish_log_event();
        let queue = test_queue(QueueManagerPushQueueStageOwnership::testing([
            stage,
            PushQueueStage::DeadLetters,
        ]));
        let handle = spawn_queue_job(
            &queue,
            PushQueueEnvelope {
                message_id: "external-1".to_owned(),
                stage,
                key: event.queue_key(),
                payload: PushQueuePayload::PublishLog(Box::new(event)),
                attempt: 1,
                enqueued_at_ms: push_queue_now_ms(),
                not_before_ms: None,
            },
        );

        let message = next_message(&queue, stage).await;
        queue
            .dead_letter(message.ack, "provider worker failed".to_owned())
            .await
            .unwrap();

        let entry = next_dead_letter_entry(&queue).await;
        assert_eq!(entry.provider, None);
        assert!(entry.replayable);

        let replay = queue
            .replay_dead_letter("app-1", &entry.dead_letter_id)
            .await
            .unwrap()
            .unwrap();
        assert!(replay.replayed);
        assert!(replay.message_id.is_some());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn marker_dead_letters_are_listed_as_not_replayable() {
        let queue = test_queue(QueueManagerPushQueueStageOwnership::testing([
            PushQueueStage::DeadLetters,
        ]));
        let dead_letter = sockudo_push::DeadLetter {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            stage: "retry_schedule".to_owned(),
            key: "retry-key".to_owned(),
            reason: "retry attempts exhausted".to_owned(),
            occurred_at_ms: push_queue_now_ms(),
        };
        queue
            .produce(
                PushQueueStage::DeadLetters,
                dead_letter.key.clone(),
                PushQueuePayload::DeadLetter(Box::new(dead_letter)),
            )
            .await
            .unwrap();

        let entry = next_dead_letter_entry(&queue).await;
        assert!(!entry.replayable);
        let replay = queue
            .replay_dead_letter("app-1", &entry.dead_letter_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!replay.replayed);
        assert!(replay.reason.unwrap().contains("original queue payload"));
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

    fn spawn_queue_job(
        queue: &QueueManagerPushQueue,
        envelope: PushQueueEnvelope,
    ) -> tokio::task::JoinHandle<()> {
        let queue = queue.clone();
        tokio::spawn(async move {
            let job = push_queue_job_data(&envelope).unwrap();
            queue.handle_queue_job(job).await.unwrap();
        })
    }

    async fn wait_for_local_depths(
        queue: &QueueManagerPushQueue,
        stage: PushQueueStage,
        ready: usize,
        pending: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if local_depths(queue, stage).await == (ready, pending) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("local queue depths should reach the expected values");
    }

    async fn local_depths(queue: &QueueManagerPushQueue, stage: PushQueueStage) -> (usize, usize) {
        let state = queue.state.lock().await;
        let ready = state
            .ready
            .get(&stage)
            .map_or(0, std::collections::VecDeque::len);
        let pending = state
            .pending
            .keys()
            .filter(|(pending_stage, _)| *pending_stage == stage)
            .count();
        (ready, pending)
    }

    async fn first_ready_ack(
        queue: &QueueManagerPushQueue,
        stage: PushQueueStage,
    ) -> sockudo_push::QueueAckToken {
        queue
            .state
            .lock()
            .await
            .ready
            .get(&stage)
            .and_then(|ready| ready.front())
            .expect("one local ready message")
            .ack
            .clone()
    }

    async fn next_dead_letter_entry(
        queue: &QueueManagerPushQueue,
    ) -> sockudo_push::DeadLetterQueueEntry {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let page = queue
                    .list_dead_letters(
                        "app-1",
                        sockudo_push::DeadLetterQueueFilter::default(),
                        10,
                        None,
                    )
                    .await
                    .unwrap();
                if let Some(entry) = page.items.into_iter().next() {
                    return entry;
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
        let cleanup_message_id = message_id.clone();
        let token = sockudo_push::QueueAckToken {
            stage,
            message_id: message_id.clone(),
        };
        let enqueued_at_ms = push_queue_now_ms();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (completion_tx, _) = tokio::sync::watch::channel(None);
        let cleanup_completion_tx = completion_tx.clone();
        let mut state = queue.state.lock().await;
        state.pending.insert(
            (stage, message_id.clone()),
            QueueManagerPendingMessage {
                enqueued_at_ms,
                tx: Some(tx),
                completion_tx,
            },
        );
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
                enqueued_at_ms,
                not_before_ms: None,
                lease_deadline_ms: 0,
                ack: token,
            });
        drop(state);

        let cleanup_queue = queue.clone();
        tokio::spawn(async move {
            if rx.await.is_ok() {
                cleanup_queue
                    .remove_pending_and_ready(stage, &cleanup_message_id)
                    .await;
                let _ = cleanup_completion_tx.send(Some(true));
            }
        });
    }

    fn sample_queue_envelope(message_id: &str, stage: PushQueueStage) -> PushQueueEnvelope {
        PushQueueEnvelope {
            message_id: message_id.to_owned(),
            stage,
            key: format!("delivery-{message_id}"),
            payload: PushQueuePayload::DeliveryBatch(Box::new(sample_batch())),
            attempt: 1,
            enqueued_at_ms: push_queue_now_ms(),
            not_before_ms: None,
        }
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

    fn sample_publish_log_event() -> sockudo_push::PublishLogEvent {
        sockudo_push::PublishLogEvent {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            event_id: "event-1".to_owned(),
            occurred_at_ms: push_queue_now_ms(),
            intent: sockudo_push::PublishIntent {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                targets: vec![],
                payload: PushPayload {
                    template_id: None,
                    template_data: json!({}),
                    title: Some("hello".to_owned()),
                    body: Some("body".to_owned()),
                    icon: None,
                    sound: None,
                    collapse_key: None,
                },
                provider_overrides: vec![],
                not_before_ms: None,
                expires_at_ms: None,
            },
            fanout_regime: sockudo_push::FanoutRegime::FastPath,
            expected_recipients: 0,
            fast_threshold: 10_000,
            shard_size: 100_000,
        }
    }
}
