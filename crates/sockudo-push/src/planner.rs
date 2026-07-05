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
    dedupe_key,
};
use crate::storage::DynPushStore;
use crate::transform::render_all_provider_payloads;

type RenderedPayloadMap = BTreeMap<PushProviderKind, Arc<RenderedProviderPayload>>;

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
            self.handle_publish_message(message).await?;
            processed += 1;
        }
        Ok(processed)
    }

    async fn handle_publish_message(&self, message: QueueMessage) -> PushPipelineResult<()> {
        let started = Instant::now();
        let PushQueuePayload::PublishLog(event) = message.payload.clone() else {
            self.queue
                .dead_letter(message.ack, "unexpected payload for publish log".to_owned())
                .await?;
            return Ok(());
        };

        self.mark_state(&event, PublishLifecycleState::Planning)
            .await?;
        let plan_result = match event.fanout_regime {
            FanoutRegime::FastPath => self.plan_fast_path(&event).await,
            FanoutRegime::ShardPath => self.plan_shard_path(&event).await,
        };
        if let Err(PushPipelineError::InvalidPayload(reason)) = plan_result {
            self.mark_failed(&event, reason.clone()).await?;
            self.queue.dead_letter(message.ack, reason).await?;
            return Ok(());
        }
        plan_result?;
        self.metrics.planner_duration(started.elapsed());
        self.mark_state(&event, PublishLifecycleState::Dispatching)
            .await?;
        self.queue.ack(message.ack).await?;
        Ok(())
    }

    async fn mark_state(
        &self,
        event: &PublishLogEvent,
        state: PublishLifecycleState,
    ) -> PushPipelineResult<()> {
        if let Some(mut status) = self
            .store
            .get_publish_status(&event.app_id, &event.publish_id)
            .await?
        {
            status.state = state;
            if state == PublishLifecycleState::Dispatching {
                status.retry_after_ms = event
                    .intent
                    .not_before_ms
                    .filter(|not_before_ms| *not_before_ms > crate::pipeline::now_ms());
            }
            self.store.put_publish_status(status).await?;
        }
        Ok(())
    }

    async fn mark_failed(&self, event: &PublishLogEvent, reason: String) -> PushPipelineResult<()> {
        if let Some(mut status) = self
            .store
            .get_publish_status(&event.app_id, &event.publish_id)
            .await?
        {
            status.state = PublishLifecycleState::Failed;
            status.error_reason = Some(reason);
            status.retry_after_ms = None;
            self.store.put_publish_status(status).await?;
        }
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
