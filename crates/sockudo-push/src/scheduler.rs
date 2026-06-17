use crate::domain::{FanoutRegime, PublishIntent, PublishLogEvent, from_json_value};
use crate::pipeline::{DynPushQueue, PushPipelineResult, PushQueuePayload, PushQueueStage};
use crate::storage::{DynPushStore, SchedulerLock};

pub use crate::storage::{PushScheduleStore, ScheduledPushJob};

#[derive(Clone)]
pub struct PushScheduler {
    store: DynPushStore,
    queue: DynPushQueue,
    owner_id: String,
    page_size: usize,
    lock_ttl_ms: u64,
    fast_threshold: u64,
    shard_size: u64,
}

impl PushScheduler {
    pub fn new(
        store: DynPushStore,
        queue: DynPushQueue,
        owner_id: impl Into<String>,
        config: crate::domain::FanoutConfig,
    ) -> Self {
        Self {
            store,
            queue,
            owner_id: owner_id.into(),
            page_size: config.page_size,
            lock_ttl_ms: 30_000,
            fast_threshold: config.fast_threshold,
            shard_size: config.shard_size,
        }
    }

    pub fn with_lock_ttl_ms(mut self, lock_ttl_ms: u64) -> Self {
        self.lock_ttl_ms = lock_ttl_ms.max(1);
        self
    }

    pub async fn poll_due_bucket(
        &self,
        app_id: &str,
        due_minute_ms: u64,
        now_ms: u64,
    ) -> PushPipelineResult<usize> {
        let mut cursor = None;
        let mut emitted = 0_usize;
        loop {
            let page = self
                .store
                .list_due_scheduled_jobs(app_id, due_minute_ms, self.page_size, cursor)
                .await?;
            for job in page.items {
                if job.due_at_ms > now_ms {
                    continue;
                }
                if self.emit_due_job(job, now_ms).await? {
                    emitted += 1;
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(emitted);
            }
        }
    }

    async fn emit_due_job(&self, job: ScheduledPushJob, now_ms: u64) -> PushPipelineResult<bool> {
        let lock_expires_at = now_ms.saturating_add(self.lock_ttl_ms);
        let acquired = self
            .store
            .acquire_scheduler_lock(
                SchedulerLock {
                    app_id: job.app_id.clone(),
                    publish_id: job.publish_id.clone(),
                    owner_id: self.owner_id.clone(),
                    expires_at_ms: lock_expires_at,
                },
                now_ms,
            )
            .await?;
        if !acquired {
            return Ok(false);
        }

        let event = scheduled_publish_event(&job, now_ms, self.fast_threshold, self.shard_size)?;
        self.store.append_publish_log_event(event.clone()).await?;
        self.queue
            .produce(
                PushQueueStage::PublishLog,
                event.queue_key(),
                PushQueuePayload::PublishLog(Box::new(event)),
            )
            .await?;
        let finalized_at_ms = if lock_expires_at > 1_000_000_000_000 {
            crate::pipeline::now_ms()
        } else {
            now_ms
        };
        if finalized_at_ms > lock_expires_at {
            return Err(crate::pipeline::PushPipelineError::Backpressure(
                "scheduler lock expired before due job could be finalized".to_owned(),
            ));
        }
        self.store
            .delete_scheduled_job(&job.app_id, &job.publish_id)
            .await?;
        self.store
            .release_scheduler_lock(&job.app_id, &job.publish_id, &self.owner_id)
            .await?;
        Ok(true)
    }
}

fn scheduled_publish_event(
    job: &ScheduledPushJob,
    occurred_at_ms: u64,
    fast_threshold: u64,
    shard_size: u64,
) -> PushPipelineResult<PublishLogEvent> {
    if let Ok(event) = from_json_value::<PublishLogEvent>(&job.payload_json) {
        return Ok(PublishLogEvent {
            occurred_at_ms,
            ..event
        });
    }
    let intent = from_json_value::<PublishIntent>(&job.payload_json)
        .map_err(|error| crate::pipeline::PushPipelineError::InvalidPayload(error.to_string()))?;
    let fanout_regime = FanoutRegime::ShardPath;
    Ok(PublishLogEvent {
        app_id: job.app_id.clone(),
        publish_id: job.publish_id.clone(),
        event_id: format!("scheduled-{}-{occurred_at_ms}", job.publish_id),
        occurred_at_ms,
        intent,
        fanout_regime,
        expected_recipients: 0,
        fast_threshold,
        shard_size,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sonic_rs::json;

    use crate::domain::{
        DEFAULT_PUSH_FANOUT_SHARD_SIZE, FanoutConfig, PublishIntent, PublishTarget, PushPayload,
        to_json_value,
    };
    use crate::memory::MemoryPushStore;
    use crate::pipeline::{MemoryPushQueue, PushQueue, PushQueueStage};
    use crate::storage::PushSchedulerLockStore;

    use super::*;

    #[tokio::test]
    async fn scheduler_uses_distributed_lock_and_emits_publish_log_only() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let intent = PublishIntent {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            targets: vec![PublishTarget::Channel {
                channel: "room".to_owned(),
            }],
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
            not_before_ms: Some(1_000),
            expires_at_ms: None,
        };
        store
            .put_scheduled_job(ScheduledPushJob {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                due_at_ms: 1_000,
                due_minute_ms: 0,
                payload_json: to_json_value(&intent).unwrap(),
            })
            .await
            .unwrap();

        assert!(
            store
                .acquire_scheduler_lock(
                    SchedulerLock {
                        app_id: "app-1".to_owned(),
                        publish_id: "publish-1".to_owned(),
                        owner_id: "node-a".to_owned(),
                        expires_at_ms: 2_000,
                    },
                    1_000,
                )
                .await
                .unwrap()
        );

        let scheduler = PushScheduler::new(
            store.clone(),
            queue.clone(),
            "node-b",
            FanoutConfig::default(),
        );
        assert_eq!(
            scheduler.poll_due_bucket("app-1", 0, 1_000).await.unwrap(),
            0
        );
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .ready_depth,
            0
        );

        let scheduler = PushScheduler::new(
            store.clone(),
            queue.clone(),
            "node-c",
            FanoutConfig::default(),
        );
        assert_eq!(
            scheduler.poll_due_bucket("app-1", 0, 2_001).await.unwrap(),
            1
        );
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .ready_depth,
            1
        );
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryJobs(
                    crate::domain::PushProviderKind::Fcm
                ))
                .await
                .unwrap()
                .ready_depth,
            0
        );
    }

    #[test]
    fn scheduled_intent_falls_back_to_shard_path_until_recipient_count_is_known() {
        let job = ScheduledPushJob {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            due_at_ms: 1_000,
            due_minute_ms: 0,
            payload_json: json!({
                "appId": "app-1",
                "publishId": "publish-1",
                "targets": [{"type": "channel", "channel": "room"}],
                "payload": {"title": "hello", "body": "body"}
            }),
        };
        let event =
            scheduled_publish_event(&job, 1_000, 10_000, DEFAULT_PUSH_FANOUT_SHARD_SIZE).unwrap();
        assert_eq!(event.fanout_regime, FanoutRegime::ShardPath);
        assert_eq!(event.expected_recipients, 0);
    }
}
