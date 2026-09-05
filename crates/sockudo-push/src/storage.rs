use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;

use crate::cleanup::{PushCleanupReport, PushCleanupRequest};
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushDomainError, ShardJob,
};

pub type PushStorageResult<T> = Result<T, PushStorageError>;
pub type DynPushStore = Arc<dyn PushStore + Send + Sync>;
pub const EXPECTED_PUSH_SCHEMA_VERSION: u32 = 2;
const MAX_UNCONDITIONAL_STATUS_WRITE_ATTEMPTS: usize = 8;

#[derive(Debug, Error)]
pub enum PushStorageError {
    #[error("push storage validation failed: {0}")]
    Validation(#[from] PushDomainError),
    #[error("push storage backend {backend} is not compiled in; enable feature `{feature}`")]
    FeatureDisabled {
        backend: &'static str,
        feature: &'static str,
    },
    #[error("push storage backend {backend} has schema support but no runtime implementation yet")]
    BackendDeferred { backend: &'static str },
    #[error("push storage backend error: {0}")]
    Backend(String),
}

/// A publish status together with the storage metadata used for optimistic concurrency.
///
/// The revision and update timestamp are storage-only fields. They are intentionally absent from
/// the public publish-status wire representation.
#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeedbackReceipt {
    pub occurred_at_ms: u64,
    pub retry_at_ms: Option<u64>,
    pub status_applied: bool,
}

pub const MAX_PENDING_FEEDBACK: usize = 64;

/// Storage payload includes bounded receipts, never exposed in the public status.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StoredStatusPayload {
    #[serde(flatten)]
    pub status: PublishStatus,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pending_feedback: BTreeMap<String, FeedbackReceipt>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub pending_children: std::collections::BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedPublishStatus {
    pub status: PublishStatus,
    pub revision: u64,
    pub updated_at_ms: u64,
    pub pending_feedback: BTreeMap<String, FeedbackReceipt>,
    pub pending_children: std::collections::BTreeSet<String>,
}

/// Result of an atomic publish-status create or compare-and-swap operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishStatusCasOutcome {
    Inserted { revision: u64 },
    Updated { revision: u64 },
    Conflict,
    Missing,
}

impl PublishStatusCasOutcome {
    pub const fn applied(self) -> bool {
        matches!(self, Self::Inserted { .. } | Self::Updated { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PushStorageBackendKind {
    Memory,
    Postgres,
    Mysql,
    DynamoDb,
    SurrealDb,
    ScyllaDb,
}

impl PushStorageBackendKind {
    pub fn startup_check(self) -> PushStorageResult<()> {
        match self {
            Self::Memory => Ok(()),
            #[cfg(feature = "postgres")]
            Self::Postgres => Ok(()),
            #[cfg(feature = "mysql")]
            Self::Mysql => Ok(()),
            #[cfg(not(feature = "postgres"))]
            Self::Postgres => Err(PushStorageError::FeatureDisabled {
                backend: "postgres",
                feature: "postgres",
            }),
            #[cfg(not(feature = "mysql"))]
            Self::Mysql => Err(PushStorageError::FeatureDisabled {
                backend: "mysql",
                feature: "mysql",
            }),
            #[cfg(feature = "dynamodb")]
            Self::DynamoDb => Ok(()),
            #[cfg(feature = "surrealdb")]
            Self::SurrealDb => Ok(()),
            #[cfg(feature = "scylladb")]
            Self::ScyllaDb => Ok(()),
            #[cfg(not(feature = "dynamodb"))]
            Self::DynamoDb => Err(PushStorageError::FeatureDisabled {
                backend: "dynamodb",
                feature: "dynamodb",
            }),
            #[cfg(not(feature = "surrealdb"))]
            Self::SurrealDb => Err(PushStorageError::FeatureDisabled {
                backend: "surrealdb",
                feature: "surrealdb",
            }),
            #[cfg(not(feature = "scylladb"))]
            Self::ScyllaDb => Err(PushStorageError::FeatureDisabled {
                backend: "scylladb",
                feature: "scylladb",
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeviceRegistrationChange {
    Inserted,
    Unchanged,
    Updated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceRegistrationOutcome {
    pub change: DeviceRegistrationChange,
    pub token_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<PushCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledPushJob {
    pub app_id: String,
    pub publish_id: String,
    pub due_at_ms: u64,
    pub due_minute_ms: u64,
    pub payload_json: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdempotencyRecord {
    pub app_id: String,
    pub key: String,
    pub publish_id: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerLock {
    pub app_id: String,
    pub publish_id: String,
    pub owner_id: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorInvalidationEvent {
    pub app_id: String,
    pub event_id: String,
    pub subject: String,
    pub occurred_at_ms: u64,
}

#[derive(Clone, Debug)]
pub enum DeviceFeedbackEffect {
    Success,
    Failure { threshold: u32, reason: String },
    Delete,
}

#[derive(Clone, Debug)]
pub struct DeviceFeedbackRequest {
    pub app_id: String,
    pub device_id: String,
    pub publish_id: String,
    pub receipt_id: String,
    pub occurred_at_ms: u64,
    pub expires_at_ms: u64,
    pub effect: DeviceFeedbackEffect,
}

#[derive(Default, Clone, Debug)]
pub struct DeviceFeedbackApplied {
    pub applied: bool,
    pub previous: Option<crate::domain::DevicePushState>,
    pub next: Option<crate::domain::DevicePushState>,
}

pub(crate) fn apply_device_feedback_effect(
    device: &mut DeviceDetails,
    request: &DeviceFeedbackRequest,
) {
    match &request.effect {
        DeviceFeedbackEffect::Success => {
            device.record_delivery_success();
            device.last_active_at_ms = device.last_active_at_ms.max(request.occurred_at_ms);
        }
        DeviceFeedbackEffect::Failure { threshold, reason } => {
            device.record_delivery_failure(*threshold, reason.clone());
        }
        DeviceFeedbackEffect::Delete => {}
    }
}

#[async_trait]
pub trait PushDeviceStore: Send + Sync {
    /// Release a canonical device fence only after the complete outcome record is durable.
    async fn complete_device_feedback_receipt(
        &self,
        _app_id: &str,
        _device_id: &str,
        _receipt_id: &str,
    ) -> PushStorageResult<()> {
        Ok(())
    }

    /// Atomically fence one device effect with its durable outcome receipt.
    async fn apply_device_feedback_once(
        &self,
        _request: DeviceFeedbackRequest,
    ) -> PushStorageResult<DeviceFeedbackApplied> {
        Err(PushStorageError::Backend(
            "atomic device feedback receipts are unsupported".into(),
        ))
    }

    async fn upsert_device(
        &self,
        device: DeviceDetails,
    ) -> PushStorageResult<DeviceRegistrationOutcome>;

    async fn get_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<Option<DeviceDetails>>;

    async fn delete_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome>;

    async fn list_devices(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>>;

    /// Page the devices assigned to one client. Implementations should use their
    /// client index; this fallback preserves compatibility with external stores.
    async fn list_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        let page = self.list_devices(app_id, limit, cursor).await?;
        Ok(Page {
            items: page
                .items
                .into_iter()
                .filter(|device| device.client_id.as_deref() == Some(client_id))
                .collect(),
            next_cursor: page.next_cursor,
        })
    }

    async fn delete_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
    ) -> PushStorageResult<u64>;

    async fn list_stale_devices(
        &self,
        app_id: &str,
        day_bucket: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>>;
}

#[async_trait]
pub trait PushSubscriptionStore: Send + Sync {
    async fn upsert_subscription(&self, subscription: ChannelSubscription)
    -> PushStorageResult<()>;

    async fn delete_subscription(
        &self,
        app_id: &str,
        channel: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome>;

    async fn list_channel_subscribers(
        &self,
        app_id: &str,
        channel: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>>;

    async fn list_device_channels(
        &self,
        app_id: &str,
        device_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>>;

    async fn list_subscriptions(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>>;

    async fn list_subscription_channels(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<String>>;

    async fn delete_subscriptions_by_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<u64>;

    async fn delete_subscriptions_by_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> PushStorageResult<u64>;
}

#[async_trait]
pub trait PushCredentialStore: Send + Sync {
    async fn put_credential(&self, credential: ProviderCredential) -> PushStorageResult<()>;

    async fn get_credential(
        &self,
        app_id: &str,
        credential_id: &str,
    ) -> PushStorageResult<Option<ProviderCredential>>;

    async fn list_credentials(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ProviderCredential>>;
}

#[async_trait]
pub trait PushTemplateStore: Send + Sync {
    async fn put_template(&self, template: NotificationTemplate) -> PushStorageResult<()>;

    async fn get_template(
        &self,
        app_id: &str,
        template_id: &str,
    ) -> PushStorageResult<Option<NotificationTemplate>>;

    async fn list_templates(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<NotificationTemplate>>;

    async fn delete_template(
        &self,
        app_id: &str,
        template_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome>;
}

#[async_trait]
pub trait PushPublishStatusStore: Send + Sync {
    /// True only while canonical, versioned retirement evidence is retained.
    /// Missing status is never proof that accepted delivery work has completed.
    async fn is_publish_retired(
        &self,
        _app_id: &str,
        _publish_id: &str,
    ) -> PushStorageResult<bool> {
        Ok(false)
    }

    /// Create the initial status without replacing an existing publish.
    async fn create_publish_status_if_absent(
        &self,
        status: PublishStatus,
    ) -> PushStorageResult<PublishStatusCasOutcome>;

    /// Load the status and the storage revision required for a later compare-and-swap.
    async fn get_versioned_publish_status(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<VersionedPublishStatus>>;

    /// Replace `expected` only if its storage revision is still current.
    async fn compare_and_swap_publish_status(
        &self,
        expected: &VersionedPublishStatus,
        next: PublishStatus,
    ) -> PushStorageResult<PublishStatusCasOutcome>;

    /// Atomically replace status and its bounded feedback receipts. Ordinary
    /// writers must preserve the receipts through compare_and_swap_publish_status.
    async fn compare_and_swap_feedback_status(
        &self,
        _expected: &VersionedPublishStatus,
        _next: PublishStatus,
        _pending: BTreeMap<String, FeedbackReceipt>,
    ) -> PushStorageResult<PublishStatusCasOutcome> {
        Err(PushStorageError::Backend(
            "atomic feedback status receipts are unsupported".into(),
        ))
    }

    /// Unconditionally seed or replace a status using bounded conditional writes.
    ///
    /// Production read/modify/write paths should call `compare_and_swap_publish_status` directly.
    /// This compatibility helper remains useful for fixtures and administrative repairs without
    /// reintroducing a blind write in any backend.
    async fn put_publish_status(&self, status: PublishStatus) -> PushStorageResult<()> {
        for _ in 0..MAX_UNCONDITIONAL_STATUS_WRITE_ATTEMPTS {
            let outcome = match self
                .get_versioned_publish_status(&status.app_id, &status.publish_id)
                .await?
            {
                Some(expected) => {
                    self.compare_and_swap_publish_status(&expected, status.clone())
                        .await?
                }
                None => self.create_publish_status_if_absent(status.clone()).await?,
            };
            if outcome.applied() {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }

        Err(PushStorageError::Backend(
            "publish status conditional write retries exhausted".to_owned(),
        ))
    }

    async fn get_publish_status(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<PublishStatus>> {
        Ok(self
            .get_versioned_publish_status(app_id, publish_id)
            .await?
            .map(|versioned| versioned.status))
    }
}

#[async_trait]
pub trait PushPublishLogStore: Send + Sync {
    async fn append_publish_log_event(&self, event: PublishLogEvent) -> PushStorageResult<()>;

    async fn list_publish_log_events(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<PublishLogEvent>>;
}

#[async_trait]
pub trait PushFanoutShardStore: Send + Sync {
    async fn put_fanout_shard(&self, shard: ShardJob) -> PushStorageResult<()>;

    async fn get_fanout_shard(
        &self,
        app_id: &str,
        publish_id: &str,
        shard_id: &str,
    ) -> PushStorageResult<Option<ShardJob>>;
}

#[async_trait]
pub trait PushScheduleStore: Send + Sync {
    async fn put_scheduled_job(&self, job: ScheduledPushJob) -> PushStorageResult<()>;

    async fn get_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<ScheduledPushJob>>;

    async fn delete_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome>;

    async fn list_scheduled_apps(&self) -> PushStorageResult<Vec<String>>;

    async fn list_due_scheduled_jobs(
        &self,
        app_id: &str,
        due_minute_ms: u64,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ScheduledPushJob>>;
}

#[async_trait]
pub trait PushDeliveryEventStore: Send + Sync {
    async fn append_delivery_event(&self, event: DeliveryEvent) -> PushStorageResult<()>;

    async fn list_delivery_events(
        &self,
        app_id: &str,
        publish_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeliveryEvent>>;

    async fn purge_delivery_events_before(
        &self,
        app_id: &str,
        before_ms: u64,
    ) -> PushStorageResult<u64>;
}

#[async_trait]
pub trait PushIdempotencyStore: Send + Sync {
    async fn put_idempotency_record_if_absent(
        &self,
        record: IdempotencyRecord,
    ) -> PushStorageResult<bool>;

    async fn get_idempotency_record(
        &self,
        app_id: &str,
        key: &str,
    ) -> PushStorageResult<Option<IdempotencyRecord>>;
}

#[async_trait]
pub trait PushSchedulerLockStore: Send + Sync {
    async fn acquire_scheduler_lock(
        &self,
        lock: SchedulerLock,
        now_ms: u64,
    ) -> PushStorageResult<bool>;

    async fn release_scheduler_lock(
        &self,
        app_id: &str,
        publish_id: &str,
        owner_id: &str,
    ) -> PushStorageResult<()>;
}

#[async_trait]
pub trait PushOperatorEventStore: Send + Sync {
    async fn append_operator_invalidation(
        &self,
        event: OperatorInvalidationEvent,
    ) -> PushStorageResult<()>;

    async fn list_operator_invalidations(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<OperatorInvalidationEvent>>;
}

#[async_trait]
pub trait PushCleanupStore: Send + Sync {
    async fn cleanup_expired_push_data(
        &self,
        request: PushCleanupRequest,
    ) -> PushStorageResult<PushCleanupReport>;
}

pub trait PushStore:
    PushDeviceStore
    + PushSubscriptionStore
    + PushCredentialStore
    + PushTemplateStore
    + PushPublishStatusStore
    + PushPublishLogStore
    + PushFanoutShardStore
    + PushScheduleStore
    + PushDeliveryEventStore
    + PushIdempotencyStore
    + PushSchedulerLockStore
    + PushOperatorEventStore
    + PushCleanupStore
{
}

impl<T> PushStore for T where
    T: PushDeviceStore
        + PushSubscriptionStore
        + PushCredentialStore
        + PushTemplateStore
        + PushPublishStatusStore
        + PushPublishLogStore
        + PushFanoutShardStore
        + PushScheduleStore
        + PushDeliveryEventStore
        + PushIdempotencyStore
        + PushSchedulerLockStore
        + PushOperatorEventStore
        + PushCleanupStore
{
}

#[cfg(test)]
mod migration_smoke_tests {
    const POSTGRES_PUSH_SCHEMA: &str =
        include_str!("../../../ops/migrations/postgres/001_push_schema.sql");
    const MYSQL_PUSH_SCHEMA: &str =
        include_str!("../../../ops/migrations/mysql/003_push_schema.sql");
    const DYNAMODB_PUSH_SCHEMA: &str =
        include_str!("../../../ops/migrations/dynamodb/001_push_schema.md");
    const SURREALDB_PUSH_SCHEMA: &str =
        include_str!("../../../ops/migrations/surrealdb/001_push_schema.surql");
    const SCYLLADB_PUSH_SCHEMA: &str =
        include_str!("../../../ops/migrations/scylladb/001_push_schema.cql");

    #[test]
    fn push_migrations_cover_required_storage_families() {
        for required in [
            "push_devices",
            "push_channel_subscribers",
            "push_provider_credentials",
            "push_notification_templates",
            "push_scheduled_jobs",
            "push_publish_status",
            "push_delivery_events",
            "push_dead_letters",
            "push_idempotency",
            "push_schema_version",
            "VALUES (1, 0)",
            "SELECT 2, 0",
            "revision BIGINT NOT NULL DEFAULT 1",
            "Online rollout",
            "Credential security",
            "Rollback",
        ] {
            assert!(
                POSTGRES_PUSH_SCHEMA.contains(required),
                "postgres: {required}"
            );
        }

        for required in [
            "push_devices",
            "push_channel_subscribers",
            "push_provider_credentials",
            "push_notification_templates",
            "push_scheduled_jobs",
            "push_publish_status",
            "push_delivery_events",
            "push_dead_letters",
            "push_idempotency",
            "push_schema_version",
            "VALUES (1, 0)",
            "SELECT 2, 0",
            "revision BIGINT UNSIGNED NOT NULL DEFAULT 1",
            "Online rollout",
            "Credential security",
            "Rollback",
        ] {
            assert!(MYSQL_PUSH_SCHEMA.contains(required), "mysql: {required}");
        }
    }

    #[test]
    fn mysql_push_schema_limits_indexed_identifiers_to_ascii() {
        for required in [
            "app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL",
            "device_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL",
            "client_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NULL",
            "channel VARCHAR(512) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL",
        ] {
            assert!(MYSQL_PUSH_SCHEMA.contains(required), "mysql: {required}");
        }
    }

    #[test]
    fn hyperscale_backend_schema_contracts_cover_denormalized_indexes() {
        for required in [
            "devices_by_id",
            "devices_by_client",
            "devices_by_transport",
            "devices_by_token",
            "devices_by_last_active",
            "channel_subscribers",
            "channels_by_device",
            "channels_by_client",
            "provider_credentials",
            "notification_templates",
            "scheduled_jobs_by_due_minute",
            "scheduled_jobs_by_id",
            "publish_status",
            "push_delivery_events",
            "push_dead_letters",
            "push_idempotency",
            "Credential Security",
            "Online Migration",
            "Rollback",
        ] {
            assert!(
                DYNAMODB_PUSH_SCHEMA.contains(required),
                "dynamodb: {required}"
            );
        }

        for required in [
            "devices_by_id",
            "devices_by_client",
            "devices_by_transport",
            "devices_by_token",
            "devices_by_last_active",
            "channel_subscribers",
            "channels_by_device",
            "channels_by_client",
            "provider_credentials",
            "notification_templates",
            "scheduled_jobs_by_due_minute",
            "scheduled_jobs_by_id",
            "publish_status",
            "push_delivery_events",
            "push_dead_letters",
            "push_idempotency",
        ] {
            assert!(
                SCYLLADB_PUSH_SCHEMA.contains(required),
                "scylladb: {required}"
            );
        }
    }

    #[test]
    fn surrealdb_schema_declares_small_tier_push_indexes() {
        for required in [
            "Supported small deployment tier only",
            "push_devices",
            "devices_by_id",
            "devices_by_client",
            "devices_by_transport",
            "devices_by_token",
            "devices_by_last_active",
            "push_channel_subscribers",
            "channel_subscribers",
            "channels_by_device",
            "channels_by_client",
            "push_idempotency",
        ] {
            assert!(
                SURREALDB_PUSH_SCHEMA.contains(required),
                "surrealdb: {required}"
            );
        }
    }

    #[test]
    fn hyperscale_status_contracts_include_cas_revisions() {
        assert!(DYNAMODB_PUSH_SCHEMA.contains("strictly positive `revision`"));
        assert!(SURREALDB_PUSH_SCHEMA.contains("revision ON push_publish_status"));
        assert!(SCYLLADB_PUSH_SCHEMA.contains("revision bigint"));
    }
}
