#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
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
            },
            legacy: false,
        });
    }

    Ok(DecodedPublishStatus {
        versioned: VersionedPublishStatus {
            status: from_json_str::<PublishStatus>(data)?,
            revision: INITIAL_PUBLISH_STATUS_REVISION,
            updated_at_ms: 0,
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
    async fn read_versioned_publish_status(
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
        self.put_json(
            FAMILY_PUBLISH_LOG,
            &event.app_id,
            &event.publish_id,
            &format!("{:020}:{}", event.occurred_at_ms, event.event_id),
            &event,
        )
        .await
    }

    async fn list_publish_log_events(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<PublishLogEvent>> {
        let start = cursor_position(cursor, app_id)?;
        let mut rows = self
            .scan_json::<PublishLogEvent>(FAMILY_PUBLISH_LOG, app_id)
            .await?
            .into_iter()
            .map(|(_, position, event)| (position, event))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(page_from_rows(
            app_id,
            PushCursorKind::PublishLog,
            rows,
            limit,
            start,
        ))
    }
}

#[async_trait]
impl<B> PushFanoutShardStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_fanout_shard(&self, shard: ShardJob) -> PushStorageResult<()> {
        self.put_json(
            FAMILY_FANOUT_SHARD,
            &shard.app_id,
            &shard.publish_id,
            &shard.shard_id,
            &shard,
        )
        .await
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
    let rows = store
        .backend
        .scan_pk_page(
            FAMILY_STATUS_UPDATED_TIME,
            app_id,
            "time",
            None,
            limit.max(1),
        )
        .await?;
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
        if !terminal_publish_state(decoded.versioned.status.state) {
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
