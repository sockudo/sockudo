//! Versioned, conservative retirement evidence for original publish payloads.
use crate::domain::{PublishLogEvent, ShardJob, ShardJobStatus};
use crate::storage::VersionedPublishStatus;
use serde::{Deserialize, Serialize};

pub(crate) const PLANNER_RECEIPT_ID: &str = "lifecycle-plan-v1";
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(crate) const RETIRED_SQL_STATE: &str = "retiredv1";

/// Kept in the canonical status slot while child cleanup runs. An old status decoder rejects
/// this shape. The archived record retains all storage metadata for an interrupted rollout.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublishTombstone {
    pub lifecycle_version: u8,
    pub app_id: String,
    pub publish_id: String,
    pub terminal_status: String,
    pub retired_at_ms: u64,
    pub keep_until_ms: u64,
}

impl PublishTombstone {
    pub fn new(status: &VersionedPublishStatus, encoded: String, now_ms: u64) -> Self {
        Self {
            lifecycle_version: 1,
            app_id: status.status.app_id.clone(),
            publish_id: status.status.publish_id.clone(),
            terminal_status: encoded,
            retired_at_ms: now_ms,
            keep_until_ms: now_ms.saturating_add(crate::retry::MAX_RETRY_AGE_MS),
        }
    }
}

/// A restartable, bounded scan of completed fanout receipts, fenced by status revision.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleScan {
    pub version: u8,
    pub revision: u64,
    pub after_shard: Option<String>,
    pub emitted_recipients: u128,
    pub has_planner_receipt: bool,
    pub has_pending_shard: bool,
    #[serde(default)]
    pub after_log: Option<String>,
    #[serde(default)]
    pub has_log: bool,
    #[serde(default)]
    pub has_unsafe_log: bool,
}
impl LifecycleScan {
    /// Expiry and age can become eligible without a status revision change.
    /// A completed unsafe pass must restart its bounded log scan on the next visit.
    pub fn finish_log_page(&mut self, more_logs: bool) -> bool {
        if more_logs {
            return false;
        }
        if self.has_unsafe_log {
            self.after_log = None;
            self.has_log = false;
            self.has_unsafe_log = false;
            return false;
        }
        self.has_log
    }

    pub fn log_position(&self) -> crate::storage::PushStorageResult<(u64, String)> {
        let Some(cursor) = self.after_log.as_deref() else {
            return Ok((0, String::new()));
        };
        let invalid =
            || crate::storage::PushStorageError::Backend("invalid lifecycle log cursor".to_owned());
        let (at, id) = cursor.split_once(':').ok_or_else(invalid)?;
        Ok((at.parse().map_err(|_| invalid())?, id.to_owned()))
    }

    pub fn new(revision: u64) -> Self {
        Self {
            version: 1,
            revision,
            ..Self::default()
        }
    }
    pub fn observe(&mut self, shard: &ShardJob) {
        self.after_shard = Some(shard.shard_id.clone());
        self.has_pending_shard |= shard.status != ShardJobStatus::Complete;
        self.has_planner_receipt |= shard.shard_id == PLANNER_RECEIPT_ID;
        self.emitted_recipients = self
            .emitted_recipients
            .saturating_add(u128::from(shard.emitted_recipients));
    }
    pub fn proves_complete(&self, status: &VersionedPublishStatus) -> bool {
        self.version == 1
            && self.revision == status.revision
            && self.has_planner_receipt
            && !self.has_pending_shard
            && status.pending_feedback.is_empty()
            && status.pending_children.is_empty()
            && status.status.state.is_terminal()
            && status.status.counters.terminal_outcomes() >= self.emitted_recipients
    }
}

pub(crate) fn status_is_old_enough(
    status: &VersionedPublishStatus,
    now_ms: u64,
    retention_ms: u64,
) -> bool {
    retention_ms > 0
        && status.status.state.is_terminal()
        && status.pending_feedback.is_empty()
        && status.pending_children.is_empty()
        && status.updated_at_ms
            < now_ms.saturating_sub(retention_ms.max(crate::retry::MAX_RETRY_AGE_MS))
        && status.status.counters.retry_scheduled <= status.status.counters.retry_attempted
}

pub(crate) fn log_is_old_enough(event: &PublishLogEvent, now_ms: u64) -> bool {
    event
        .intent
        .not_before_ms
        .unwrap_or(event.occurred_at_ms)
        .max(event.occurred_at_ms)
        < now_ms.saturating_sub(crate::retry::MAX_RETRY_AGE_MS)
        && event
            .intent
            .expires_at_ms
            .is_none_or(|expires| expires <= now_ms)
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(all(test, feature = "scylladb"))]
mod live_tests;
