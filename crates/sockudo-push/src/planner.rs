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
        let PushQueuePayload::PublishLog(event) = message.payload.clone() else {
            self.queue
                .dead_letter(message.ack, "unexpected payload for publish log".to_owned())
                .await?;
            return Ok(());
        };

        let lock_id = match self.begin_planning(&event, consumer_group).await? {
            PlanningStart::Started { lock_id } => lock_id,
            PlanningStart::AlreadyHandled => {
                self.queue.ack(message.ack).await?;
                return Ok(());
            }
            PlanningStart::Locked { retry_at_ms } => {
                self.queue.nack(message.ack, Some(retry_at_ms)).await?;
                return Ok(());
            }
        };
        let plan_result = match event.fanout_regime {
            FanoutRegime::FastPath => self.plan_fast_path(&event).await,
            FanoutRegime::ShardPath => self.plan_shard_path(&event).await,
        };
        if let Err(PushPipelineError::InvalidPayload(reason)) = plan_result {
            self.mark_failed(&event, reason.clone()).await?;
            self.release_planner_lock(&event, &lock_id, consumer_group)
                .await;
            self.queue.dead_letter(message.ack, reason).await?;
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
        self.queue.ack(message.ack).await?;
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
            return Ok(false);
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
        for target in &event.intent.targets {
            self.stream_target(target, &mut batcher, Some(event.fast_threshold))
                .await?;
        }
        batcher.flush(&self.queue).await
    }

    async fn plan_shard_path(&self, event: &PublishLogEvent) -> PushPipelineResult<()> {
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
                }
            }
        }
        Ok(())
    }

    async fn stream_target(
        &self,
        target: &PublishTarget,
        batcher: &mut ProviderBatcher,
        max_recipients: Option<u64>,
    ) -> PushPipelineResult<Option<crate::domain::PushCursor>> {
        let mut emitted = 0_u64;
        match target {
            PublishTarget::Device { device_id } => {
                if let Some(device) = self.store.get_device(&batcher.app_id, device_id).await? {
                    batcher.push_device(device, &self.queue).await?;
                }
                Ok(None)
            }
            PublishTarget::Client { client_id } => {
                let mut cursor = None;
                loop {
                    let page = self
                        .store
                        .list_devices(&batcher.app_id, self.config.page_size, cursor)
                        .await?;
                    let next_cursor = page.next_cursor.clone();
                    for device in page
                        .items
                        .into_iter()
                        .filter(|device| device.client_id.as_deref() == Some(client_id))
                    {
                        batcher.push_device(device, &self.queue).await?;
                        emitted += 1;
                        if max_recipients.is_some_and(|max| emitted >= max) {
                            return Ok(next_cursor);
                        }
                    }
                    cursor = next_cursor;
                    if cursor.is_none() {
                        return Ok(None);
                    }
                }
            }
            PublishTarget::Channel { channel } => {
                let mut cursor = None;
                loop {
                    let page = self
                        .store
                        .list_channel_subscribers(
                            &batcher.app_id,
                            channel,
                            self.config.page_size,
                            cursor,
                        )
                        .await?;
                    let next_cursor = page.next_cursor.clone();
                    for subscription in page.items {
                        if let Some(device) = self
                            .store
                            .get_device(&batcher.app_id, &subscription.device_id)
                            .await?
                        {
                            batcher.push_device(device, &self.queue).await?;
                            emitted += 1;
                            if max_recipients.is_some_and(|max| emitted >= max) {
                                return Ok(next_cursor);
                            }
                        }
                    }
                    cursor = next_cursor;
                    if cursor.is_none() {
                        return Ok(None);
                    }
                }
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
        let PushQueuePayload::ShardJob(shard) = message.payload.clone() else {
            self.queue
                .dead_letter(
                    message.ack,
                    "unexpected payload for shard worker".to_owned(),
                )
                .await?;
            return Ok(());
        };
        let mut shard = *shard;

        shard.status = ShardJobStatus::Running;
        self.store.put_fanout_shard(shard.clone()).await?;

        let rendered_payloads =
            match rendered_payload_map(&shard.payload, &shard.provider_overrides) {
                Ok(rendered_payloads) => rendered_payloads,
                Err(PushPipelineError::InvalidPayload(reason)) => {
                    shard.status = ShardJobStatus::Failed;
                    self.store.put_fanout_shard(shard).await?;
                    self.queue.dead_letter(message.ack, reason).await?;
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
                shard_id: format!("{}-next-{}", shard.shard_id, shard.emitted_recipients),
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

        self.queue.ack(message.ack).await?;
        Ok(())
    }

    async fn stream_shard(
        &self,
        shard: &ShardJob,
        batcher: &mut ProviderBatcher,
    ) -> PushPipelineResult<Option<crate::domain::PushCursor>> {
        let mut cursor = shard.cursor.clone();
        let mut emitted = 0_u64;
        match &shard.target {
            PublishTarget::Channel { channel } => loop {
                let remaining = shard.shard_size.saturating_sub(emitted).max(1);
                let limit = shard.page_size.min(remaining as usize);
                let page = self
                    .store
                    .list_channel_subscribers(&shard.app_id, channel, limit, cursor)
                    .await?;
                let next_cursor = page.next_cursor.clone();
                for subscription in page.items {
                    if let Some(device) = self
                        .store
                        .get_device(&shard.app_id, &subscription.device_id)
                        .await?
                    {
                        batcher.push_device(device, &self.queue).await?;
                        emitted += 1;
                        if emitted >= shard.shard_size {
                            return Ok(next_cursor);
                        }
                    }
                }
                cursor = next_cursor;
                if cursor.is_none() {
                    return Ok(None);
                }
            },
            PublishTarget::Client { client_id } => loop {
                let remaining = shard.shard_size.saturating_sub(emitted).max(1);
                let limit = shard.page_size.min(remaining as usize);
                let page = self
                    .store
                    .list_devices(&shard.app_id, limit, cursor)
                    .await?;
                let next_cursor = page.next_cursor.clone();
                for device in page
                    .items
                    .into_iter()
                    .filter(|device| device.client_id.as_deref() == Some(client_id))
                {
                    batcher.push_device(device, &self.queue).await?;
                    emitted += 1;
                    if emitted >= shard.shard_size {
                        return Ok(next_cursor);
                    }
                }
                cursor = next_cursor;
                if cursor.is_none() {
                    return Ok(None);
                }
            },
            _ => Ok(None),
        }
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
            job.batch_id = batch_id.clone();
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
