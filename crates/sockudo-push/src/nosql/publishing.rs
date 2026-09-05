#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
use super::ordered::*;
use crate::cleanup::terminal_publish_state;
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, IdempotencyRecord,
    OperatorInvalidationEvent, Page, PublishStatusCasOutcome, PushCredentialStore,
    PushDeliveryEventStore, PushDeviceStore, PushFanoutShardStore, PushIdempotencyStore,
    PushOperatorEventStore, PushPublishLogStore, PushPublishStatusStore, PushScheduleStore,
    PushSchedulerLockStore, PushStorageError, PushStorageResult, PushSubscriptionStore,
    PushTemplateStore, ScheduledPushJob, SchedulerLock, VersionedPublishStatus,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

const INITIAL_PUBLISH_STATUS_REVISION: u64 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredPublishStatus {
    revision: u64,
    updated_at_ms: u64,
    status: PublishStatus,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pending_feedback: std::collections::BTreeMap<String, crate::storage::FeedbackReceipt>,
    #[serde(default)]
    pending_children: std::collections::BTreeSet<String>,
}

struct DecodedPublishStatus {
    versioned: VersionedPublishStatus,
    legacy: bool,
}

fn encode_publish_status(status: &VersionedPublishStatus) -> PushStorageResult<String> {
    to_json_string(&StoredPublishStatus {
        revision: status.revision,
        updated_at_ms: status.updated_at_ms,
        status: status.status.clone(),
        pending_feedback: status.pending_feedback.clone(),
        pending_children: status.pending_children.clone(),
    })
}

fn decode_publish_status(data: &str) -> PushStorageResult<DecodedPublishStatus> {
    if let Ok(stored) = from_json_str::<StoredPublishStatus>(data) {
        if stored.revision == 0 {
            return Err(PushStorageError::Backend(
                "push publish status revision must be greater than zero".to_owned(),
            ));
        }
        return Ok(DecodedPublishStatus {
            versioned: VersionedPublishStatus {
                status: stored.status,
                revision: stored.revision,
                updated_at_ms: stored.updated_at_ms,
                pending_feedback: stored.pending_feedback,
                pending_children: stored.pending_children,
            },
            legacy: false,
        });
    }

    Ok(DecodedPublishStatus {
        versioned: VersionedPublishStatus {
            status: from_json_str::<PublishStatus>(data)?,
            revision: INITIAL_PUBLISH_STATUS_REVISION,
            updated_at_ms: 0,
            pending_feedback: Default::default(),
            pending_children: Default::default(),
        },
        legacy: true,
    })
}

fn next_publish_status_updated_at(previous: u64) -> u64 {
    crate::pipeline::now_ms().max(previous.saturating_add(1))
}

impl<B> DocumentPushStore<B>
where
    B: DocumentBackend,
{
    pub(super) async fn read_versioned_publish_status(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<(String, VersionedPublishStatus)>> {
        let Some(data) = self
            .backend
            .get_consistent(FAMILY_STATUS, app_id, publish_id, DEFAULT_SK)
            .await?
        else {
            return Ok(None);
        };
        if super::lifecycle::tombstone(&data).is_some() {
            return Ok(None);
        }
        let mut decoded = decode_publish_status(&data)?;
        if decoded.legacy {
            decoded.versioned.updated_at_ms = self
                .backend
                .get_consistent(FAMILY_STATUS_UPDATED, app_id, publish_id, DEFAULT_SK)
                .await?
                .map(|updated_at| from_json_str::<u64>(&updated_at))
                .transpose()?
                .unwrap_or(0);
        }
        Ok(Some((data, decoded.versioned)))
    }

    // Each call owns a unique durable admission. It has no expiring lease: cleanup cannot
    // overtake a paused backend write. An uncertain/crashed write leaves visible bounded
    // metadata instead of silently turning absence of work into retirement proof.
    pub(super) async fn set_child_admission(
        &self,
        app_id: &str,
        publish_id: &str,
        token: &str,
        reserve: bool,
    ) -> PushStorageResult<bool> {
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(PushStorageError::Backend(
                "invalid child admission token".to_owned(),
            ));
        }
        for _ in 0..32 {
            let Some((raw, current)) = self
                .read_versioned_publish_status(app_id, publish_id)
                .await?
            else {
                if self
                    .backend
                    .get_consistent(FAMILY_STATUS, app_id, publish_id, DEFAULT_SK)
                    .await?
                    .is_some()
                {
                    return Err(PushStorageError::Backend(
                        "publish has been retired".to_owned(),
                    ));
                }
                return Err(PushStorageError::Backend(
                    "publish status is missing; child write must retry after status repair"
                        .to_owned(),
                ));
            };
            let mut updated = current.clone();
            if reserve {
                if updated.pending_children.len() >= 64 && !updated.pending_children.contains(token)
                {
                    return Err(PushStorageError::Backend(
                        "publish child admission capacity reached".to_owned(),
                    ));
                }
                updated.pending_children.insert(token.to_owned());
            } else if !updated.pending_children.remove(token) {
                return Err(PushStorageError::Backend(
                    "publish child admission was lost".to_owned(),
                ));
            }
            updated.revision = current.revision.checked_add(1).ok_or_else(|| {
                PushStorageError::Backend("publish status revision exhausted".to_owned())
            })?;
            updated.updated_at_ms = next_publish_status_updated_at(current.updated_at_ms);
            let index = self.prepare_publish_status_index(&updated).await?;
            if self
                .backend
                .compare_and_swap(
                    FAMILY_STATUS,
                    app_id,
                    publish_id,
                    DEFAULT_SK,
                    &raw,
                    encode_publish_status(&updated)?,
                )
                .await?
            {
                self.refresh_publish_status_indexes(&updated, Some(current.updated_at_ms))
                    .await;
                return Ok(true);
            }
            self.discard_publish_status_index_if_stale(&updated, &index)
                .await;
        }
        Err(PushStorageError::Backend(
            "publish child admission contention".to_owned(),
        ))
    }

    async fn prepare_publish_status_index(
        &self,
        status: &VersionedPublishStatus,
    ) -> PushStorageResult<String> {
        let app_id = &status.status.app_id;
        let publish_id = &status.status.publish_id;
        self.remember_cleanup_app(app_id).await?;
        let indexed_data = to_json_string(publish_id)?;
        self.backend
            .put(
                FAMILY_STATUS_UPDATED_TIME,
                app_id,
                "time",
                &status_updated_position(status.updated_at_ms, publish_id),
                indexed_data.clone(),
            )
            .await?;
        Ok(indexed_data)
    }

    async fn discard_publish_status_index_if_stale(
        &self,
        candidate: &VersionedPublishStatus,
        indexed_data: &str,
    ) {
        let app_id = &candidate.status.app_id;
        let publish_id = &candidate.status.publish_id;
        let canonical_updated_at_ms = match self
            .backend
            .get_consistent(FAMILY_STATUS, app_id, publish_id, DEFAULT_SK)
            .await
        {
            Ok(Some(data)) => match decode_publish_status(&data) {
                Ok(decoded) => Some(decoded.versioned.updated_at_ms),
                Err(error) => {
                    tracing::warn!(
                        app_id = %app_id,
                        publish_id = %publish_id,
                        operation = "decode-after-status-conflict",
                        error = %error,
                        "push publish status advisory index cleanup failed"
                    );
                    return;
                }
            },
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(
                    app_id = %app_id,
                    publish_id = %publish_id,
                    operation = "read-after-status-conflict",
                    error = %error,
                    "push publish status advisory index cleanup failed"
                );
                return;
            }
        };
        if canonical_updated_at_ms == Some(candidate.updated_at_ms) {
            return;
        }
        if let Err(error) = self
            .backend
            .compare_and_delete(
                FAMILY_STATUS_UPDATED_TIME,
                app_id,
                "time",
                &status_updated_position(candidate.updated_at_ms, publish_id),
                indexed_data,
            )
            .await
        {
            tracing::warn!(
                app_id = %app_id,
                publish_id = %publish_id,
                operation = "delete-conflicting-time-index",
                error = %error,
                "push publish status advisory index cleanup failed"
            );
        }
    }

    async fn refresh_publish_status_indexes(
        &self,
        status: &VersionedPublishStatus,
        previous_updated_at_ms: Option<u64>,
    ) {
        let app_id = &status.status.app_id;
        let publish_id = &status.status.publish_id;
        let updated_at_ms = status.updated_at_ms;

        // The discoverability index was durably staged before the canonical conditional write.
        // These remaining records are advisory, so interruption can only delay pointer cleanup.
        if let Err(error) = self
            .put_json(
                FAMILY_STATUS_UPDATED,
                app_id,
                publish_id,
                DEFAULT_SK,
                &updated_at_ms,
            )
            .await
        {
            tracing::warn!(
                app_id = %app_id,
                publish_id = %publish_id,
                operation = "write-updated-pointer",
                error = %error,
                "push publish status advisory index update failed"
            );
        }
        if let Some(previous_updated_at_ms) = previous_updated_at_ms
            && previous_updated_at_ms != updated_at_ms
            && let Err(error) = self
                .backend
                .delete(
                    FAMILY_STATUS_UPDATED_TIME,
                    app_id,
                    "time",
                    &status_updated_position(previous_updated_at_ms, publish_id),
                )
                .await
        {
            tracing::warn!(
                app_id = %app_id,
                publish_id = %publish_id,
                operation = "delete-old-time-index",
                error = %error,
                "push publish status advisory index update failed"
            );
        }
    }
}

#[async_trait]
impl<B> PushPublishStatusStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn is_publish_retired(&self, app_id: &str, publish_id: &str) -> PushStorageResult<bool> {
        Ok(self
            .backend
            .get_consistent(FAMILY_STATUS, app_id, publish_id, DEFAULT_SK)
            .await?
            .is_some_and(|raw| super::lifecycle::tombstone(&raw).is_some()))
    }

    async fn create_publish_status_if_absent(
        &self,
        status: PublishStatus,
    ) -> PushStorageResult<PublishStatusCasOutcome> {
        if self
            .backend
            .get_consistent(
                FAMILY_STATUS,
                &status.app_id,
                &status.publish_id,
                DEFAULT_SK,
            )
            .await?
            .is_some()
        {
            return Ok(PublishStatusCasOutcome::Conflict);
        }
        let versioned = VersionedPublishStatus {
            status,
            revision: INITIAL_PUBLISH_STATUS_REVISION,
            updated_at_ms: next_publish_status_updated_at(0),
            pending_feedback: Default::default(),
            pending_children: Default::default(),
        };
        let indexed_data = self.prepare_publish_status_index(&versioned).await?;
        if !self
            .backend
            .put_if_absent(
                FAMILY_STATUS,
                &versioned.status.app_id,
                &versioned.status.publish_id,
                DEFAULT_SK,
                encode_publish_status(&versioned)?,
            )
            .await?
        {
            self.discard_publish_status_index_if_stale(&versioned, &indexed_data)
                .await;
            return Ok(PublishStatusCasOutcome::Conflict);
        }

        self.refresh_publish_status_indexes(&versioned, None).await;
        Ok(PublishStatusCasOutcome::Inserted {
            revision: versioned.revision,
        })
    }

    async fn get_versioned_publish_status(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<VersionedPublishStatus>> {
        Ok(self
            .read_versioned_publish_status(app_id, publish_id)
            .await?
            .map(|(_, status)| status))
    }

    async fn compare_and_swap_publish_status(
        &self,
        expected: &VersionedPublishStatus,
        next: PublishStatus,
    ) -> PushStorageResult<PublishStatusCasOutcome> {
        self.compare_and_swap_feedback_status(expected, next, expected.pending_feedback.clone())
            .await
    }

    async fn compare_and_swap_feedback_status(
        &self,
        expected: &VersionedPublishStatus,
        next: PublishStatus,
        pending: std::collections::BTreeMap<String, crate::storage::FeedbackReceipt>,
    ) -> PushStorageResult<PublishStatusCasOutcome> {
        if pending.len() > crate::storage::MAX_PENDING_FEEDBACK {
            return Err(crate::storage::PushStorageError::Backend(
                "feedback receipt capacity reached".into(),
            ));
        }
        if expected.revision == 0
            || expected.status.app_id != next.app_id
            || expected.status.publish_id != next.publish_id
        {
            return Err(PushStorageError::Backend(
                "invalid push publish status CAS identity or revision".to_owned(),
            ));
        }

        let Some((expected_data, current)) = self
            .read_versioned_publish_status(&next.app_id, &next.publish_id)
            .await?
        else {
            return Ok(PublishStatusCasOutcome::Missing);
        };
        if current != *expected {
            return Ok(PublishStatusCasOutcome::Conflict);
        }

        let revision = current.revision.checked_add(1).ok_or_else(|| {
            PushStorageError::Backend("push publish status revision exhausted".to_owned())
        })?;
        let updated = VersionedPublishStatus {
            status: next,
            revision,
            updated_at_ms: next_publish_status_updated_at(current.updated_at_ms),
            pending_feedback: pending,
            pending_children: current.pending_children.clone(),
        };
        let indexed_data = self.prepare_publish_status_index(&updated).await?;
        if !self
            .backend
            .compare_and_swap(
                FAMILY_STATUS,
                &updated.status.app_id,
                &updated.status.publish_id,
                DEFAULT_SK,
                &expected_data,
                encode_publish_status(&updated)?,
            )
            .await?
        {
            self.discard_publish_status_index_if_stale(&updated, &indexed_data)
                .await;
            return Ok(PublishStatusCasOutcome::Conflict);
        }

        self.refresh_publish_status_indexes(&updated, Some(current.updated_at_ms))
            .await;
        Ok(PublishStatusCasOutcome::Updated { revision })
    }
}

#[async_trait]
impl<B> PushPublishLogStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn append_publish_log_event(&self, event: PublishLogEvent) -> PushStorageResult<()> {
        let token = hex::encode(rand::random::<[u8; 32]>());
        let admitted = self
            .set_child_admission(&event.app_id, &event.publish_id, &token, true)
            .await?;

        let position = format!("{:020}:{}", event.occurred_at_ms, event.event_id);
        self.write_ordered_reference(
            FAMILY_PUBLISH_LOG_ORDERED,
            &event.app_id,
            &position,
            &event.publish_id,
            &position,
        )
        .await?;
        self.put_json(
            FAMILY_PUBLISH_LOG,
            &event.app_id,
            &event.publish_id,
            &format!("{:020}:{}", event.occurred_at_ms, event.event_id),
            &event,
        )
        .await?;
        if admitted {
            self.set_child_admission(&event.app_id, &event.publish_id, &token, false)
                .await?;
        }
        Ok(())
    }

    async fn list_publish_log_events(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<PublishLogEvent>> {
        self.ordered_page(
            FAMILY_PUBLISH_LOG,
            FAMILY_PUBLISH_LOG_ORDERED,
            app_id,
            PushCursorKind::PublishLog,
            limit,
            cursor,
        )
        .await
    }
}

#[async_trait]
impl<B> PushFanoutShardStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_fanout_shard(&self, shard: ShardJob) -> PushStorageResult<()> {
        let token = hex::encode(rand::random::<[u8; 32]>());
        let admitted = self
            .set_child_admission(&shard.app_id, &shard.publish_id, &token, true)
            .await?;

        self.put_json(
            FAMILY_FANOUT_SHARD,
            &shard.app_id,
            &shard.publish_id,
            &shard.shard_id,
            &shard,
        )
        .await?;
        if admitted {
            self.set_child_admission(&shard.app_id, &shard.publish_id, &token, false)
                .await?;
        }
        Ok(())
    }

    async fn get_fanout_shard(
        &self,
        app_id: &str,
        publish_id: &str,
        shard_id: &str,
    ) -> PushStorageResult<Option<ShardJob>> {
        self.get_json(FAMILY_FANOUT_SHARD, app_id, publish_id, shard_id)
            .await
    }
}

#[async_trait]
impl<B> PushScheduleStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_scheduled_job(&self, job: ScheduledPushJob) -> PushStorageResult<()> {
        if let Some(existing) = self
            .get_json::<ScheduledPushJob>(
                FAMILY_SCHEDULED_JOB,
                &job.app_id,
                &job.publish_id,
                DEFAULT_SK,
            )
            .await?
        {
            self.delete_json(
                FAMILY_SCHEDULED_JOB_DUE,
                &existing.app_id,
                "due",
                &scheduled_due_position(&existing),
            )
            .await?;
        }
        self.put_json(
            FAMILY_SCHEDULED_JOB,
            &job.app_id,
            &job.publish_id,
            DEFAULT_SK,
            &job,
        )
        .await?;
        self.put_json(
            FAMILY_SCHEDULED_JOB_DUE,
            &job.app_id,
            "due",
            &scheduled_due_position(&job),
            &job,
        )
        .await?;
        self.put_json(
            FAMILY_SCHEDULED_APP,
            GLOBAL_APP_ID,
            "apps",
            &job.app_id,
            &job.app_id,
        )
        .await
    }

    async fn get_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<ScheduledPushJob>> {
        self.get_json(FAMILY_SCHEDULED_JOB, app_id, publish_id, DEFAULT_SK)
            .await
    }

    async fn delete_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        if let Some(existing) = self
            .get_json::<ScheduledPushJob>(FAMILY_SCHEDULED_JOB, app_id, publish_id, DEFAULT_SK)
            .await?
        {
            self.delete_json(
                FAMILY_SCHEDULED_JOB_DUE,
                app_id,
                "due",
                &scheduled_due_position(&existing),
            )
            .await?;
        }
        self.delete_json(FAMILY_SCHEDULED_JOB, app_id, publish_id, DEFAULT_SK)
            .await
    }

    async fn list_scheduled_apps(&self) -> PushStorageResult<Vec<String>> {
        Ok(self
            .scan_pk_json::<String>(FAMILY_SCHEDULED_APP, GLOBAL_APP_ID, "apps")
            .await?
            .into_iter()
            .map(|(_, _, app_id)| app_id)
            .collect())
    }

    async fn list_due_scheduled_jobs(
        &self,
        app_id: &str,
        due_minute_ms: u64,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ScheduledPushJob>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_pk_page_json::<ScheduledPushJob>(
                FAMILY_SCHEDULED_JOB_DUE,
                app_id,
                "due",
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .filter_map(|(_, position, job)| {
                (job.due_minute_ms <= due_minute_ms).then_some((position, job))
            })
            .collect::<Vec<_>>();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::ScheduledJob,
            rows,
            limit,
            start,
        ))
    }
}

#[async_trait]
impl<B> PushDeliveryEventStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn append_delivery_event(&self, event: DeliveryEvent) -> PushStorageResult<()> {
        self.remember_cleanup_app(&event.app_id).await?;
        let position = delivery_event_position(&event);
        self.put_json(
            FAMILY_DELIVERY_EVENT,
            &event.app_id,
            &event.publish_id,
            &position,
            &event,
        )
        .await?;
        self.put_json(
            FAMILY_DELIVERY_EVENT_TIME,
            &event.app_id,
            "time",
            &format!("{position}:{}", event.publish_id),
            &event,
        )
        .await
    }

    async fn list_delivery_events(
        &self,
        app_id: &str,
        publish_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeliveryEvent>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_pk_page_json::<DeliveryEvent>(
                FAMILY_DELIVERY_EVENT,
                app_id,
                publish_id,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .map(|(_, position, event)| (position, event))
            .collect::<Vec<_>>();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::DeliveryEvent,
            rows,
            limit,
            start,
        ))
    }

    async fn purge_delivery_events_before(
        &self,
        app_id: &str,
        before_ms: u64,
    ) -> PushStorageResult<u64> {
        let events = self
            .scan_pk_json::<DeliveryEvent>(FAMILY_DELIVERY_EVENT_TIME, app_id, "time")
            .await?;
        let mut primary_keys = Vec::new();
        let mut time_keys = Vec::new();
        for (_, time_position, event) in events
            .into_iter()
            .filter(|(_, _, event)| event.occurred_at_ms < before_ms)
        {
            primary_keys.push((event.publish_id.clone(), delivery_event_position(&event)));
            time_keys.push(("time".to_owned(), time_position));
        }
        let deleted = self
            .backend
            .delete_many(FAMILY_DELIVERY_EVENT, app_id, &primary_keys)
            .await?;
        self.backend
            .delete_many(FAMILY_DELIVERY_EVENT_TIME, app_id, &time_keys)
            .await?;
        Ok(deleted)
    }
}

pub(super) async fn document_cleanup_publish_statuses<B>(
    store: &DocumentPushStore<B>,
    app_id: &str,
    cutoff_ms: u64,
    limit: usize,
) -> PushStorageResult<crate::cleanup::PushCleanupCounters>
where
    B: DocumentBackend,
{
    let rows =
        super::coordination::cleanup_page(store, FAMILY_STATUS_UPDATED_TIME, app_id, limit).await?;
    let mut counters = crate::cleanup::PushCleanupCounters::default();
    for document in rows {
        counters.scanned = counters.scanned.saturating_add(1);
        let position = document.sk;
        let indexed_data = document.data;
        let publish_id = from_json_str::<String>(&indexed_data)?;
        let Some((indexed_updated_at_ms, indexed_publish_id)) =
            parse_status_updated_position(&position)
        else {
            continue;
        };
        if indexed_updated_at_ms >= cutoff_ms {
            continue;
        }
        if indexed_publish_id != publish_id {
            store
                .backend
                .compare_and_delete(
                    FAMILY_STATUS_UPDATED_TIME,
                    app_id,
                    "time",
                    &position,
                    &indexed_data,
                )
                .await?;
            continue;
        }

        let Some(status_data) = store
            .backend
            .get_consistent(FAMILY_STATUS, app_id, &publish_id, DEFAULT_SK)
            .await?
        else {
            if let Some(updated_data) = store
                .backend
                .get_consistent(FAMILY_STATUS_UPDATED, app_id, &publish_id, DEFAULT_SK)
                .await?
                && from_json_str::<u64>(&updated_data)? == indexed_updated_at_ms
            {
                store
                    .backend
                    .compare_and_delete(
                        FAMILY_STATUS_UPDATED,
                        app_id,
                        &publish_id,
                        DEFAULT_SK,
                        &updated_data,
                    )
                    .await?;
            }
            store
                .backend
                .compare_and_delete(
                    FAMILY_STATUS_UPDATED_TIME,
                    app_id,
                    "time",
                    &position,
                    &indexed_data,
                )
                .await?;
            continue;
        };

        if super::lifecycle::tombstone(&status_data).is_some() {
            continue;
        }
        if store
            .backend
            .get(
                FAMILY_FANOUT_SHARD,
                app_id,
                &publish_id,
                crate::lifecycle::PLANNER_RECEIPT_ID,
            )
            .await?
            .is_some()
        {
            continue;
        }
        let mut decoded = decode_publish_status(&status_data)?;
        if decoded.legacy {
            let Some(legacy_updated_at_ms) = store
                .backend
                .get_consistent(FAMILY_STATUS_UPDATED, app_id, &publish_id, DEFAULT_SK)
                .await?
                .map(|data| from_json_str::<u64>(&data))
                .transpose()?
            else {
                // The old format did not carry an authoritative timestamp. Without its matching
                // pointer, fail closed and retain the status rather than guessing from an index.
                continue;
            };
            decoded.versioned.updated_at_ms = legacy_updated_at_ms;
        }

        if decoded.versioned.updated_at_ms != indexed_updated_at_ms {
            store
                .backend
                .compare_and_delete(
                    FAMILY_STATUS_UPDATED_TIME,
                    app_id,
                    "time",
                    &position,
                    &indexed_data,
                )
                .await?;
            continue;
        }
        if !terminal_publish_state(decoded.versioned.status.state)
            || !decoded.versioned.pending_feedback.is_empty()
            || !decoded.versioned.pending_children.is_empty()
        {
            continue;
        }
        if store
            .backend
            .compare_and_delete(FAMILY_STATUS, app_id, &publish_id, DEFAULT_SK, &status_data)
            .await?
        {
            counters.deleted = counters.deleted.saturating_add(1);
            if let Some(updated_data) = store
                .backend
                .get_consistent(FAMILY_STATUS_UPDATED, app_id, &publish_id, DEFAULT_SK)
                .await?
                && from_json_str::<u64>(&updated_data)? == indexed_updated_at_ms
            {
                store
                    .backend
                    .compare_and_delete(
                        FAMILY_STATUS_UPDATED,
                        app_id,
                        &publish_id,
                        DEFAULT_SK,
                        &updated_data,
                    )
                    .await?;
            }
        }
        store
            .backend
            .compare_and_delete(
                FAMILY_STATUS_UPDATED_TIME,
                app_id,
                "time",
                &position,
                &indexed_data,
            )
            .await?;
    }
    Ok(counters)
}
