use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use crate::domain::{
    DeliveryBatch, DeliveryJob, DeviceDetails, FanoutConfig, FanoutRegime, ProviderOverridePayload,
    PublishLifecycleState, PublishLogEvent, PublishTarget, PushPayload, PushProviderKind,
    RenderedProviderPayload, ShardJob, ShardJobStatus, provider_key,
};
use crate::metrics::PushMetrics;
use crate::pipeline::{
    PushPipelineError, PushPipelineResult, PushQueuePayload, PushQueueStage, QueueMessage,
    dedupe_key, guard_publish_status_transition, mutate_publish_status_with_cas,
};
use crate::storage::{DynPushStore, SchedulerLock};
use crate::transform::render_all_provider_payloads;

type RenderedPayloadMap = BTreeMap<PushProviderKind, Arc<RenderedProviderPayload>>;
const PLANNER_LOCK_TTL_MS: u64 = 30_000;
const PLANNER_LOCK_RETRY_DELAY_MS: u64 = 1_000;

enum PlanningStart {
    Started { lock_id: String },
    AlreadyHandled,
    Locked { retry_at_ms: u64 },
}

#[derive(Clone)]
pub struct PushPlanner {
    store: DynPushStore,
    queue: crate::pipeline::DynPushQueue,
    config: FanoutConfig,
    metrics: PushMetrics,
}

impl PushPlanner {
    pub fn new(
        store: DynPushStore,
        queue: crate::pipeline::DynPushQueue,
        config: FanoutConfig,
    ) -> Self {
        Self {
            store,
            queue,
            config,
            metrics: PushMetrics::default(),
        }
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub async fn run_once(&self, consumer_group: &str) -> PushPipelineResult<usize> {
        let messages = self
            .queue
            .consume(PushQueueStage::PublishLog, consumer_group, 16, 30_000)
            .await?;
        let mut processed = 0;
        for message in messages {
            self.handle_publish_message(message, consumer_group).await?;
            processed += 1;
        }
        Ok(processed)
    }

    async fn handle_publish_message(
        &self,
        message: QueueMessage,
        consumer_group: &str,
    ) -> PushPipelineResult<()> {
        let started = Instant::now();
        let QueueMessage { payload, ack, .. } = message;
        let PushQueuePayload::PublishLog(event) = payload else {
            self.queue
                .dead_letter(ack, "unexpected payload for publish log".to_owned())
                .await?;
            return Ok(());
        };

        let lock_id = match self.begin_planning(&event, consumer_group).await? {
            PlanningStart::Started { lock_id } => lock_id,
            PlanningStart::AlreadyHandled => {
                self.queue.ack(ack).await?;
                return Ok(());
            }
            PlanningStart::Locked { retry_at_ms } => {
                self.queue.nack(ack, Some(retry_at_ms)).await?;
                return Ok(());
            }
        };
        self.persist_planner_receipt(&event, 0, 0, false).await?;
        let plan_result = match event.fanout_regime {
            FanoutRegime::FastPath => self.plan_fast_path(&event).await,
            FanoutRegime::ShardPath => self.plan_shard_path(&event).await,
        };
        if let Err(PushPipelineError::InvalidPayload(reason)) = plan_result {
            self.mark_failed(&event, reason.clone()).await?;
            self.release_planner_lock(&event, &lock_id, consumer_group)
                .await;
            self.queue.dead_letter(ack, reason).await?;
            return Ok(());
        }
        if let Err(error) = plan_result {
            self.release_planner_lock(&event, &lock_id, consumer_group)
                .await;
            return Err(error);
        }
        self.metrics.planner_duration(started.elapsed());
        self.mark_state(&event, PublishLifecycleState::Dispatching)
            .await?;
        self.release_planner_lock(&event, &lock_id, consumer_group)
            .await;
        self.queue.ack(ack).await?;
        Ok(())
    }

    async fn begin_planning(
        &self,
        event: &PublishLogEvent,
        owner_id: &str,
    ) -> PushPipelineResult<PlanningStart> {
        if !self.publish_can_be_planned(event).await? {
            return Ok(PlanningStart::AlreadyHandled);
        }

        let now_ms = crate::pipeline::now_ms();
        let lock_id = planner_lock_id(&event.publish_id);
        let lock = SchedulerLock {
            app_id: event.app_id.clone(),
            publish_id: lock_id.clone(),
            owner_id: owner_id.to_owned(),
            expires_at_ms: now_ms.saturating_add(PLANNER_LOCK_TTL_MS),
        };
        if !self.store.acquire_scheduler_lock(lock, now_ms).await? {
            return Ok(PlanningStart::Locked {
                retry_at_ms: now_ms.saturating_add(PLANNER_LOCK_RETRY_DELAY_MS),
            });
        }

        if !self.publish_can_be_planned(event).await? {
            self.release_planner_lock(event, &lock_id, owner_id).await;
            return Ok(PlanningStart::AlreadyHandled);
        }

        self.mark_state(event, PublishLifecycleState::Planning)
            .await?;
        Ok(PlanningStart::Started { lock_id })
    }

    async fn publish_can_be_planned(&self, event: &PublishLogEvent) -> PushPipelineResult<bool> {
        let Some(status) = self
            .store
            .get_publish_status(&event.app_id, &event.publish_id)
            .await?
        else {
            if self
                .store
                .is_publish_retired(&event.app_id, &event.publish_id)
                .await?
            {
                return Ok(false);
            }
            return Err(crate::storage::PushStorageError::Backend(
                "publish status is missing; publish log must retry after status repair".to_owned(),
            )
            .into());
        };
        Ok(matches!(
            status.state,
            PublishLifecycleState::Queued | PublishLifecycleState::Planning
        ))
    }

    async fn release_planner_lock(&self, event: &PublishLogEvent, lock_id: &str, owner_id: &str) {
        let _ = self
            .store
            .release_scheduler_lock(&event.app_id, lock_id, owner_id)
            .await;
    }

    async fn mark_state(
        &self,
        event: &PublishLogEvent,
        state: PublishLifecycleState,
    ) -> PushPipelineResult<()> {
        mutate_publish_status_with_cas(
            self.store.as_ref(),
            &self.metrics,
            "planner",
            &event.app_id,
            &event.publish_id,
            |current| {
                let next = state;
                if !guard_publish_status_transition(&self.metrics, "planner", current, next) {
                    return Ok(None);
                }
                let mut status = current.clone();
                status.state = next;
                if next == PublishLifecycleState::Dispatching {
                    status.retry_after_ms = event
                        .intent
                        .not_before_ms
                        .filter(|not_before_ms| *not_before_ms > crate::pipeline::now_ms());
                }
                Ok(Some(status))
            },
        )
        .await?;
        Ok(())
    }

    async fn mark_failed(&self, event: &PublishLogEvent, reason: String) -> PushPipelineResult<()> {
        mutate_publish_status_with_cas(
            self.store.as_ref(),
            &self.metrics,
            "planner",
            &event.app_id,
            &event.publish_id,
            |current| {
                let next = PublishLifecycleState::Failed;
                if !guard_publish_status_transition(&self.metrics, "planner", current, next) {
                    return Ok(None);
                }
                let mut status = current.clone();
                status.state = next;
                status.error_reason = Some(reason.clone());
                status.retry_after_ms = None;
                Ok(Some(status))
            },
        )
        .await?;
        Ok(())
    }

    async fn plan_fast_path(&self, event: &PublishLogEvent) -> PushPipelineResult<()> {
        let payload = Arc::new(event.intent.payload.clone());
        let rendered_payloads =
            rendered_payload_map(&event.intent.payload, &event.intent.provider_overrides)?;
        let mut batcher = ProviderBatcher::new(
            event.app_id.clone(),
            event.publish_id.clone(),
            "fast".to_owned(),
            self.config.provider_batch_size,
            ProviderBatcherPayloads {
                payload: Arc::clone(&payload),
                rendered_payloads: Arc::clone(&rendered_payloads),
            },
            ProviderBatcherTiming {
                not_before_ms: event.intent.not_before_ms,
                expires_at_ms: event.intent.expires_at_ms,
            },
            self.metrics.clone(),
        );
        for (index, target) in event.intent.targets.iter().enumerate() {
            if let Some(cursor) = self
                .stream_target(target, &mut batcher, Some(event.fast_threshold))
                .await?
            {
                let shard = ShardJob {
                    app_id: event.app_id.clone(),
                    publish_id: event.publish_id.clone(),
                    shard_id: format!("fast-continuation-{index}"),
                    target: target.clone(),
                    payload: event.intent.payload.clone(),
                    provider_overrides: event.intent.provider_overrides.clone(),
                    not_before_ms: event.intent.not_before_ms,
                    expires_at_ms: event.intent.expires_at_ms,
                    cursor: Some(cursor),
                    page_size: self.config.page_size,
                    shard_size: self.config.shard_size,
                    emitted_recipients: 0,
                    emitted_batches: 0,
                    status: ShardJobStatus::Pending,
                };
                self.store.put_fanout_shard(shard.clone()).await?;
                self.queue
                    .produce(
                        PushQueueStage::ShardJobs,
                        shard.queue_key(),
                        PushQueuePayload::ShardJob(Box::new(shard)),
                    )
                    .await?;
            }
        }
        batcher.flush(&self.queue).await?;
        self.persist_planner_receipt(
            event,
            batcher.emitted_recipients,
            batcher.emitted_batches,
            true,
        )
        .await
    }

    async fn persist_planner_receipt(
        &self,
        event: &PublishLogEvent,
        emitted_recipients: u64,
        emitted_batches: u64,
        complete: bool,
    ) -> PushPipelineResult<()> {
        let target = event.intent.targets.first().cloned().ok_or_else(|| {
            PushPipelineError::InvalidPayload("publish has no targets".to_owned())
        })?;
        self.store
            .put_fanout_shard(ShardJob {
                app_id: event.app_id.clone(),
                publish_id: event.publish_id.clone(),
                shard_id: crate::lifecycle::PLANNER_RECEIPT_ID.to_owned(),
                target,
                payload: event.intent.payload.clone(),
                provider_overrides: event.intent.provider_overrides.clone(),
                not_before_ms: event.intent.not_before_ms,
                expires_at_ms: event.intent.expires_at_ms,
                cursor: None,
                page_size: self.config.page_size,
                shard_size: self.config.shard_size,
                emitted_recipients,
                emitted_batches,
                status: if complete {
                    ShardJobStatus::Complete
                } else {
                    ShardJobStatus::Pending
                },
            })
            .await?;
        Ok(())
    }

    async fn plan_shard_path(&self, event: &PublishLogEvent) -> PushPipelineResult<()> {
        let mut direct_recipients = 0_u64;
        let mut direct_batches = 0_u64;
        let mut ids = BTreeSet::new();
        let payload = Arc::new(event.intent.payload.clone());
        let rendered_payloads =
            rendered_payload_map(&event.intent.payload, &event.intent.provider_overrides)?;
        for (index, target) in event.intent.targets.iter().enumerate() {
            match target {
                PublishTarget::Channel { .. } | PublishTarget::Client { .. } => {
                    let shard = ShardJob {
                        app_id: event.app_id.clone(),
                        publish_id: event.publish_id.clone(),
                        shard_id: dedupe_key(format!("shard-{index}"), &mut ids),
                        target: target.clone(),
                        payload: event.intent.payload.clone(),
                        provider_overrides: event.intent.provider_overrides.clone(),
                        not_before_ms: event.intent.not_before_ms,
                        expires_at_ms: event.intent.expires_at_ms,
                        cursor: None,
                        page_size: self.config.page_size,
                        shard_size: self.config.shard_size,
                        emitted_recipients: 0,
                        emitted_batches: 0,
                        status: ShardJobStatus::Pending,
                    };
                    self.store.put_fanout_shard(shard.clone()).await?;
                    self.queue
                        .produce(
                            PushQueueStage::ShardJobs,
                            shard.queue_key(),
                            PushQueuePayload::ShardJob(Box::new(shard)),
                        )
                        .await?;
                }
                _ => {
                    let mut batcher = ProviderBatcher::new(
                        event.app_id.clone(),
                        event.publish_id.clone(),
                        format!("direct-{index}"),
                        self.config.provider_batch_size,
                        ProviderBatcherPayloads {
                            payload: Arc::clone(&payload),
                            rendered_payloads: Arc::clone(&rendered_payloads),
                        },
                        ProviderBatcherTiming {
                            not_before_ms: event.intent.not_before_ms,
                            expires_at_ms: event.intent.expires_at_ms,
                        },
                        self.metrics.clone(),
                    );
                    self.stream_target(target, &mut batcher, None).await?;
                    batcher.flush(&self.queue).await?;
                    direct_recipients =
                        direct_recipients.saturating_add(batcher.emitted_recipients);
                    direct_batches = direct_batches.saturating_add(batcher.emitted_batches);
                }
            }
        }
        self.persist_planner_receipt(event, direct_recipients, direct_batches, true)
            .await
    }

    async fn stream_target(
        &self,
        target: &PublishTarget,
        batcher: &mut ProviderBatcher,
        max_recipients: Option<u64>,
    ) -> PushPipelineResult<Option<crate::domain::PushCursor>> {
        match target {
            PublishTarget::Device { device_id } => {
                if let Some(device) = self.store.get_device(&batcher.app_id, device_id).await? {
                    batcher.push_device(device, &self.queue).await?;
                }
                Ok(None)
            }
            PublishTarget::Client { .. } | PublishTarget::Channel { .. } => {
                stream_indexed_target(
                    &self.store,
                    &self.queue,
                    target,
                    None,
                    self.config.page_size,
                    max_recipients.unwrap_or(u64::MAX),
                    batcher,
                )
                .await
            }
            PublishTarget::Recipient { recipient } => {
                recipient.validate()?;
                batcher
                    .push_job(
                        recipient.provider(),
                        DeliveryJob {
                            app_id: batcher.app_id.clone(),
                            publish_id: batcher.publish_id.clone(),
                            provider: recipient.provider(),
                            batch_id: String::new(),
                            device_id: None,
                            recipient: recipient.clone(),
                            payload: Arc::clone(&batcher.payload),
                            rendered_payload: batcher.rendered_payload(recipient.provider()),
                            attempt: 1,
                            first_attempt_at_ms: None,
                            not_before_ms: batcher.not_before_ms,
                            expires_at_ms: batcher.expires_at_ms,
                        },
                        &self.queue,
                    )
                    .await?;
                Ok(None)
            }
            PublishTarget::ProviderTopic { .. }
            | PublishTarget::ProviderCondition { .. }
            | PublishTarget::RegisteredTopic { .. }
            | PublishTarget::UserTopic { .. }
            | PublishTarget::IndexedFilter { .. } => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct PushShardWorker {
    store: DynPushStore,
    queue: crate::pipeline::DynPushQueue,
    config: FanoutConfig,
    metrics: PushMetrics,
}

impl PushShardWorker {
    pub fn new(
        store: DynPushStore,
        queue: crate::pipeline::DynPushQueue,
        config: FanoutConfig,
    ) -> Self {
        Self {
            store,
            queue,
            config,
            metrics: PushMetrics::default(),
        }
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub async fn run_once(&self, consumer_group: &str) -> PushPipelineResult<usize> {
        let messages = self
            .queue
            .consume(PushQueueStage::ShardJobs, consumer_group, 8, 30_000)
            .await?;
        let mut processed = 0;
        for message in messages {
            self.handle_shard_message(message).await?;
            processed += 1;
        }
        Ok(processed)
    }

    async fn handle_shard_message(&self, message: QueueMessage) -> PushPipelineResult<()> {
        let QueueMessage { payload, ack, .. } = message;
        let PushQueuePayload::ShardJob(shard) = payload else {
            self.queue
                .dead_letter(ack, "unexpected payload for shard worker".to_owned())
                .await?;
            return Ok(());
        };
        let mut shard = *shard;
        if self
            .store
            .is_publish_retired(&shard.app_id, &shard.publish_id)
            .await?
        {
            self.queue.ack(ack).await?;
            return Ok(());
        }

        shard.status = ShardJobStatus::Running;
        self.store.put_fanout_shard(shard.clone()).await?;

        let rendered_payloads =
            match rendered_payload_map(&shard.payload, &shard.provider_overrides) {
                Ok(rendered_payloads) => rendered_payloads,
                Err(PushPipelineError::InvalidPayload(reason)) => {
                    shard.status = ShardJobStatus::Failed;
                    self.store.put_fanout_shard(shard).await?;
                    self.queue.dead_letter(ack, reason).await?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
        let mut batcher = ProviderBatcher::new(
            shard.app_id.clone(),
            shard.publish_id.clone(),
            shard.shard_id.clone(),
            self.config.provider_batch_size,
            ProviderBatcherPayloads {
                payload: Arc::new(shard.payload.clone()),
                rendered_payloads,
            },
            ProviderBatcherTiming {
                not_before_ms: shard.not_before_ms,
                expires_at_ms: shard.expires_at_ms,
            },
            self.metrics.clone(),
        );
        let next_cursor = self.stream_shard(&shard, &mut batcher).await?;
        batcher.flush(&self.queue).await?;
        shard.emitted_batches = batcher.emitted_batches;
        shard.emitted_recipients = batcher.emitted_recipients;
        shard.status = ShardJobStatus::Complete;
        self.store.put_fanout_shard(shard.clone()).await?;

        if let Some(cursor) = next_cursor {
            let next = ShardJob {
                shard_id: {
                    let identity = sonic_rs::to_string(&(&shard.shard_id, &cursor))
                        .map_err(|_| crate::domain::PushDomainError::CursorDecode)?;
                    format!("next-{}", crate::domain::stable_hash(identity.as_bytes()))
                },
                cursor: Some(cursor),
                status: ShardJobStatus::Pending,
                emitted_recipients: 0,
                emitted_batches: 0,
                ..shard
            };
            self.store.put_fanout_shard(next.clone()).await?;
            self.queue
                .produce(
                    PushQueueStage::ShardJobs,
                    next.queue_key(),
                    PushQueuePayload::ShardJob(Box::new(next)),
                )
                .await?;
        }

        self.queue.ack(ack).await?;
        Ok(())
    }

    async fn stream_shard(
        &self,
        shard: &ShardJob,
        batcher: &mut ProviderBatcher,
    ) -> PushPipelineResult<Option<crate::domain::PushCursor>> {
        stream_indexed_target(
            &self.store,
            &self.queue,
            &shard.target,
            shard.cursor.clone(),
            shard.page_size,
            shard.shard_size,
            batcher,
        )
        .await
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ChannelFanoutCursorV2 {
    version: u8,
    subscription_after: String,
    client_id: String,
    device_cursor: crate::domain::PushCursor,
}

#[allow(clippy::too_many_arguments)] // Shared initial and continuation traversal keeps one cursor contract.
async fn stream_indexed_target(
    store: &DynPushStore,
    queue: &crate::pipeline::DynPushQueue,
    target: &PublishTarget,
    cursor: Option<crate::domain::PushCursor>,
    page_size: usize,
    max_recipients: u64,
    batcher: &mut ProviderBatcher,
) -> PushPipelineResult<Option<crate::domain::PushCursor>> {
    use crate::domain::{PushCursor, PushCursorKind};
    let app_id = batcher.app_id.clone();
    let max_recipients = max_recipients.max(1);
    let page_size = page_size.max(1);
    let mut emitted = 0u64;
    let mut cursor = cursor;
    match target {
        PublishTarget::Client { client_id } => loop {
            let limit =
                page_size.min(usize::try_from(max_recipients - emitted).unwrap_or(usize::MAX));
            let page = store
                .list_devices_by_client(&app_id, client_id, limit, cursor)
                .await?;
            for device in page.items {
                batcher.push_device(device, queue).await?;
                emitted += 1;
            }
            cursor = page.next_cursor;
            if cursor.is_none() || emitted >= max_recipients {
                return Ok(cursor);
            }
        },
        PublishTarget::Channel { channel } => {
            let mut pending_client = None;
            if let Some(saved) = cursor
                .as_ref()
                .filter(|cursor| cursor.kind == PushCursorKind::ChannelFanout)
            {
                if saved.app_id != app_id {
                    return Err(crate::domain::PushDomainError::CursorDecode.into());
                }
                let state: ChannelFanoutCursorV2 = sonic_rs::from_str(&saved.position)
                    .map_err(|_| crate::domain::PushDomainError::CursorDecode)?;
                if state.version != 2 {
                    return Err(crate::domain::PushDomainError::CursorDecode.into());
                }
                cursor = Some(PushCursor {
                    app_id: app_id.clone(),
                    kind: PushCursorKind::ChannelSubscription,
                    position: state.subscription_after.clone(),
                    issued_at_ms: 0,
                });
                pending_client = Some((
                    state.subscription_after,
                    state.client_id,
                    Some(state.device_cursor),
                ));
            }
            loop {
                if let Some((subscription_after, client_id, mut device_cursor)) =
                    pending_client.take()
                {
                    loop {
                        let limit = page_size
                            .min(
                                usize::try_from(max_recipients.saturating_sub(emitted))
                                    .unwrap_or(usize::MAX),
                            )
                            .max(1);
                        let page = store
                            .list_devices_by_client(&app_id, &client_id, limit, device_cursor)
                            .await?;
                        for device in page.items {
                            batcher.push_device(device, queue).await?;
                            emitted += 1;
                        }
                        device_cursor = page.next_cursor;
                        if emitted >= max_recipients {
                            if let Some(device_cursor) = device_cursor {
                                let position = sonic_rs::to_string(&ChannelFanoutCursorV2 {
                                    version: 2,
                                    subscription_after,
                                    client_id,
                                    device_cursor,
                                })
                                .map_err(|_| crate::domain::PushDomainError::CursorDecode)?;
                                return Ok(Some(PushCursor {
                                    app_id,
                                    kind: PushCursorKind::ChannelFanout,
                                    position,
                                    issued_at_ms: 0,
                                }));
                            }
                            return Ok(cursor);
                        }
                        if device_cursor.is_none() {
                            break;
                        }
                    }
                }
                // A single subscription can expand to many devices; retain its exact
                // position before traversing the nested device pages.
                let page = store
                    .list_channel_subscribers(&app_id, channel, 1, cursor)
                    .await?;
                let Some(subscription) = page.items.into_iter().next() else {
                    return Ok(page.next_cursor);
                };
                let after = subscription.device_id.clone();
                cursor = Some(PushCursor {
                    app_id: app_id.clone(),
                    kind: PushCursorKind::ChannelSubscription,
                    position: after.clone(),
                    issued_at_ms: 0,
                });
                if let Some(client_id) = subscription.scoped_client_id() {
                    pending_client = Some((after, client_id.to_owned(), None));
                } else {
                    if let Some(device) = store.get_device(&app_id, &subscription.device_id).await?
                    {
                        batcher.push_device(device, queue).await?;
                        emitted += 1;
                    }
                    if emitted >= max_recipients || page.next_cursor.is_none() {
                        return Ok(page.next_cursor);
                    }
                }
            }
        }
        _ => Ok(None),
    }
}

struct ProviderBatcher {
    app_id: String,
    publish_id: String,
    batch_prefix: String,
    max_batch_size: usize,
    payload: Arc<PushPayload>,
    rendered_payloads: Arc<RenderedPayloadMap>,
    not_before_ms: Option<u64>,
    expires_at_ms: Option<u64>,
    batches: BTreeMap<PushProviderKind, Vec<DeliveryJob>>,
    batch_indexes: BTreeMap<PushProviderKind, u64>,
    emitted_recipients: u64,
    emitted_batches: u64,
    metrics: PushMetrics,
}

struct ProviderBatcherPayloads {
    payload: Arc<PushPayload>,
    rendered_payloads: Arc<RenderedPayloadMap>,
}

#[derive(Clone, Copy)]
struct ProviderBatcherTiming {
    not_before_ms: Option<u64>,
    expires_at_ms: Option<u64>,
}

impl ProviderBatcher {
    fn new(
        app_id: String,
        publish_id: String,
        batch_prefix: String,
        max_batch_size: usize,
        payloads: ProviderBatcherPayloads,
        timing: ProviderBatcherTiming,
        metrics: PushMetrics,
    ) -> Self {
        Self {
            app_id,
            publish_id,
            batch_prefix,
            max_batch_size,
            payload: payloads.payload,
            rendered_payloads: payloads.rendered_payloads,
            not_before_ms: timing.not_before_ms,
            expires_at_ms: timing.expires_at_ms,
            batches: BTreeMap::new(),
            batch_indexes: BTreeMap::new(),
            emitted_recipients: 0,
            emitted_batches: 0,
            metrics,
        }
    }

    async fn push_device(
        &mut self,
        device: DeviceDetails,
        queue: &crate::pipeline::DynPushQueue,
    ) -> PushPipelineResult<()> {
        let provider = device.push.recipient.provider();
        self.push_job(
            provider,
            DeliveryJob {
                app_id: self.app_id.clone(),
                publish_id: self.publish_id.clone(),
                provider,
                batch_id: String::new(),
                device_id: Some(device.id),
                recipient: device.push.recipient,
                payload: Arc::clone(&self.payload),
                rendered_payload: self.rendered_payload(provider),
                attempt: 1,
                first_attempt_at_ms: None,
                not_before_ms: self.not_before_ms,
                expires_at_ms: self.expires_at_ms,
            },
            queue,
        )
        .await
    }

    fn rendered_payload(&self, provider: PushProviderKind) -> Option<Arc<RenderedProviderPayload>> {
        self.rendered_payloads.get(&provider).cloned()
    }

    async fn push_job(
        &mut self,
        provider: PushProviderKind,
        job: DeliveryJob,
        queue: &crate::pipeline::DynPushQueue,
    ) -> PushPipelineResult<()> {
        let should_flush = {
            let jobs = self.batches.entry(provider).or_default();
            jobs.push(job);
            jobs.len() >= self.max_batch_size
        };
        self.emitted_recipients += 1;
        if should_flush {
            self.flush_provider(provider, queue).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, queue: &crate::pipeline::DynPushQueue) -> PushPipelineResult<()> {
        let providers = self.batches.keys().copied().collect::<Vec<_>>();
        for provider in providers {
            self.flush_provider(provider, queue).await?;
        }
        Ok(())
    }

    async fn flush_provider(
        &mut self,
        provider: PushProviderKind,
        queue: &crate::pipeline::DynPushQueue,
    ) -> PushPipelineResult<()> {
        let Some(mut jobs) = self.batches.remove(&provider) else {
            return Ok(());
        };
        if jobs.is_empty() {
            return Ok(());
        }
        let index = self.batch_indexes.entry(provider).or_default();
        *index += 1;
        let batch_id = format!(
            "{}-batch-{}-{}",
            self.batch_prefix,
            provider_key(provider),
            *index
        );
        for job in &mut jobs {
            job.batch_id.clone_from(&batch_id);
        }
        let jobs_len = jobs.len();
        let batch = DeliveryBatch {
            app_id: self.app_id.clone(),
            publish_id: self.publish_id.clone(),
            provider,
            batch_id,
            jobs,
        };
        let key = batch.queue_key();
        let payload = PushQueuePayload::DeliveryBatch(Box::new(batch));
        if let Some(not_before_ms) = self.not_before_ms {
            queue
                .retry_at(
                    PushQueueStage::DeliveryJobs(provider),
                    key,
                    payload,
                    not_before_ms,
                )
                .await?;
        } else {
            queue
                .produce(PushQueueStage::DeliveryJobs(provider), key, payload)
                .await?;
        }
        self.metrics
            .delivery_jobs_emitted(provider, &self.app_id, jobs_len as u64);
        self.emitted_batches += 1;
        Ok(())
    }
}

fn rendered_payload_map(
    payload: &PushPayload,
    overrides: &[ProviderOverridePayload],
) -> PushPipelineResult<Arc<RenderedPayloadMap>> {
    let rendered = render_all_provider_payloads(payload, overrides)
        .map_err(|error| PushPipelineError::InvalidPayload(error.to_string()))?;
    Ok(Arc::new(
        rendered
            .into_iter()
            .map(|payload| (payload.provider, Arc::new(payload)))
            .collect(),
    ))
}

fn planner_lock_id(publish_id: &str) -> String {
    format!("planner:publish-log:{publish_id}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::domain::{
        FanoutConfig, PublishIntent, PublishLifecycleState, PublishTarget, PushPayload,
        PushProviderKind, PushRecipient, SecretString,
    };
    use crate::memory::MemoryPushStore;
    use crate::metrics::PushMetrics;
    use crate::pipeline::{
        MemoryPushQueue, PushAcceptRequest, PushPipeline, PushQueue, PushQueuePayload,
        PushQueueStage, QueueLagMetrics,
    };
    use crate::storage::{PushPublishLogStore, PushPublishStatusStore};

    use super::*;

    #[tokio::test]
    async fn missing_parent_preserves_publish_log_until_status_repair() {
        let accepted_store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        accept_direct_publish(accepted_store.clone(), queue.clone(), 1_000).await;
        let accepted_status = accepted_store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        // Simulate a legacy partial restore: the accepted queue item survived,
        // but its canonical status was omitted from the restored store.
        let store = Arc::new(MemoryPushStore::new());
        let planner = PushPlanner::new(store.clone(), queue.clone(), FanoutConfig::default());
        let message = queue
            .consume(PushQueueStage::PublishLog, "planner-a", 1, 30_000)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let token = message.ack.clone();
        let error = planner
            .handle_publish_message(message, "planner-a")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("publish status is missing"));
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .inflight_depth,
            1
        );
        assert_eq!(
            delivery_lag(&queue, PushProviderKind::Fcm)
                .await
                .ready_depth,
            0
        );

        store.put_publish_status(accepted_status).await.unwrap();
        // Broker redelivery after the failed worker is fenced must still carry
        // the original item; completing the repair cannot require republishing.
        queue.nack(token, None).await.unwrap();
        let restarted = PushPlanner::new(store, queue.clone(), FanoutConfig::default());
        assert_eq!(restarted.run_once("planner-b").await.unwrap(), 1);
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .inflight_depth,
            0
        );
        assert_eq!(
            delivery_lag(&queue, PushProviderKind::Fcm)
                .await
                .ready_depth,
            1
        );
    }

    #[tokio::test]
    async fn duplicate_publish_log_after_dispatching_does_not_emit_more_delivery_jobs() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        accept_direct_publish(store.clone(), queue.clone(), 1_000).await;

        let planner = PushPlanner::new(store.clone(), queue.clone(), FanoutConfig::default());
        assert_eq!(planner.run_once("planner-a").await.unwrap(), 1);
        assert_eq!(
            delivery_lag(&queue, PushProviderKind::Fcm)
                .await
                .ready_depth,
            1
        );

        let event = store
            .list_publish_log_events("app-1", 10, None)
            .await
            .unwrap()
            .items
            .into_iter()
            .next()
            .unwrap();
        queue
            .produce(
                PushQueueStage::PublishLog,
                event.queue_key(),
                PushQueuePayload::PublishLog(Box::new(event)),
            )
            .await
            .unwrap();

        assert_eq!(planner.run_once("planner-b").await.unwrap(), 1);
        assert_eq!(
            delivery_lag(&queue, PushProviderKind::Fcm)
                .await
                .ready_depth,
            1
        );
        assert_eq!(
            store
                .get_publish_status("app-1", "publish-1")
                .await
                .unwrap()
                .unwrap()
                .state,
            PublishLifecycleState::Dispatching
        );
    }

    #[tokio::test]
    async fn planner_does_not_regress_a_terminal_publish_to_dispatching() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        accept_direct_publish(store.clone(), queue.clone(), 1_000).await;
        let event = store
            .list_publish_log_events("app-1", 1, None)
            .await
            .unwrap()
            .items
            .pop()
            .unwrap();
        let mut status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        status.state = PublishLifecycleState::Succeeded;
        status.counters.succeeded = 1;
        store.put_publish_status(status).await.unwrap();

        let metrics = PushMetrics::default();
        let planner = PushPlanner::new(store.clone(), queue, FanoutConfig::default())
            .with_metrics(metrics.clone());
        planner
            .mark_state(&event, PublishLifecycleState::Dispatching)
            .await
            .unwrap();

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.state, PublishLifecycleState::Succeeded);
        assert_eq!(metrics.get("sockudo_push_invariant_violations_total"), 1);
    }

    async fn accept_direct_publish(
        store: Arc<MemoryPushStore>,
        queue: Arc<MemoryPushQueue>,
        occurred_at_ms: u64,
    ) {
        PushPipeline::new(store, queue, FanoutConfig::default())
            .accept_publish(
                PushAcceptRequest {
                    intent: PublishIntent {
                        app_id: "app-1".to_owned(),
                        publish_id: "publish-1".to_owned(),
                        targets: vec![PublishTarget::Recipient {
                            recipient: PushRecipient::Fcm {
                                registration_token: SecretString::new("token-1").unwrap(),
                            },
                        }],
                        payload: PushPayload {
                            template_id: None,
                            template_data: sonic_rs::json!({}),
                            title: Some("hello".to_owned()),
                            body: Some("body".to_owned()),
                            icon: None,
                            sound: None,
                            collapse_key: None,
                        },
                        provider_overrides: Default::default(),
                        not_before_ms: None,
                        expires_at_ms: None,
                    },
                    expected_recipients: 1,
                },
                occurred_at_ms,
            )
            .await
            .unwrap();
    }

    async fn delivery_lag(queue: &MemoryPushQueue, provider: PushProviderKind) -> QueueLagMetrics {
        queue
            .lag(PushQueueStage::DeliveryJobs(provider))
            .await
            .unwrap()
    }
}
