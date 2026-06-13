use super::types::{
    PresenceHistoryDirection, PresenceHistoryDurableState, PresenceHistoryEventKind,
    PresenceHistoryPage, PresenceHistoryQueryBounds, PresenceHistoryReadRequest,
    PresenceHistoryResetResult, PresenceHistoryRetentionStats, PresenceHistoryRuntimeStatus,
    PresenceHistoryStreamInspection, PresenceHistoryStreamRuntimeState,
    PresenceHistoryTransitionRecord, PresenceSnapshot, PresenceSnapshotMember,
    PresenceSnapshotRequest,
};
use crate::error::{Error, Result};
use async_trait::async_trait;
use std::collections::BTreeMap;

#[async_trait]
pub trait PresenceHistoryStore: Send + Sync {
    async fn record_transition(&self, record: PresenceHistoryTransitionRecord) -> Result<()>;

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage>;

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        Ok(PresenceHistoryStreamRuntimeState::healthy(
            app_id, channel, None, "disabled",
        ))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamInspection> {
        Ok(PresenceHistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: None,
            next_serial: None,
            retained: PresenceHistoryRetentionStats::default(),
            state: self.stream_runtime_state(app_id, channel).await?,
        })
    }

    async fn reset_stream(
        &self,
        _app_id: &str,
        _channel: &str,
        _reason: &str,
        _requested_by: Option<&str>,
    ) -> Result<PresenceHistoryResetResult> {
        Err(Error::Configuration(
            "Presence history reset is not supported by this store".to_string(),
        ))
    }

    async fn snapshot_at(&self, request: PresenceSnapshotRequest) -> Result<PresenceSnapshot> {
        let mut members: BTreeMap<String, PresenceSnapshotMember> = BTreeMap::new();
        let mut events_replayed = 0_u64;
        let mut snapshot_serial = None;
        let mut snapshot_time_ms = None;
        let mut cursor = None;
        let bounds = PresenceHistoryQueryBounds {
            start_serial: None,
            end_serial: request.at_serial,
            start_time_ms: None,
            end_time_ms: request.at_time_ms,
        };
        let (retained, complete, truncated_by_retention) = loop {
            let page = self
                .read_page(PresenceHistoryReadRequest {
                    app_id: request.app_id.clone(),
                    channel: request.channel.clone(),
                    direction: PresenceHistoryDirection::OldestFirst,
                    limit: 1000,
                    cursor: cursor.clone(),
                    bounds: bounds.clone(),
                })
                .await?;

            for item in &page.items {
                events_replayed = events_replayed.saturating_add(1);
                snapshot_serial = Some(item.serial);
                snapshot_time_ms = Some(item.published_at_ms);
                match item.event {
                    PresenceHistoryEventKind::MemberAdded => {
                        members.insert(
                            item.user_id.clone(),
                            PresenceSnapshotMember {
                                user_id: item.user_id.clone(),
                                last_event: item.event,
                                last_event_serial: item.serial,
                                last_event_at_ms: item.published_at_ms,
                            },
                        );
                    }
                    PresenceHistoryEventKind::MemberUpdated => {
                        if let Some(member) = members.get_mut(&item.user_id) {
                            member.last_event = item.event;
                            member.last_event_serial = item.serial;
                            member.last_event_at_ms = item.published_at_ms;
                        }
                    }
                    PresenceHistoryEventKind::MemberRemoved => {
                        members.remove(&item.user_id);
                    }
                }
            }

            if !page.has_more {
                break (page.retained, page.complete, page.truncated_by_retention);
            }
            cursor = page.next_cursor;
        };

        Ok(PresenceSnapshot {
            channel: request.channel,
            members: members.into_values().collect(),
            events_replayed,
            snapshot_serial,
            snapshot_time_ms,
            retained,
            complete,
            truncated_by_retention,
        })
    }

    async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
        Ok(PresenceHistoryRuntimeStatus::default())
    }
}

#[derive(Default)]
pub struct NoopPresenceHistoryStore;

#[async_trait]
impl PresenceHistoryStore for NoopPresenceHistoryStore {
    async fn record_transition(&self, _record: PresenceHistoryTransitionRecord) -> Result<()> {
        Ok(())
    }

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage> {
        request.validate()?;
        Ok(PresenceHistoryPage {
            items: Vec::new(),
            next_cursor: None,
            retained: PresenceHistoryRetentionStats::default(),
            has_more: false,
            complete: true,
            truncated_by_retention: false,
            degraded: false,
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        Ok(PresenceHistoryStreamRuntimeState {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: None,
            durable_state: PresenceHistoryDurableState::ResetRequired,
            continuity_proven: false,
            reset_required: true,
            reason: Some("presence_history_disabled".to_string()),
            node_id: None,
            last_transition_at_ms: None,
            authoritative_source: "disabled".to_string(),
            observed_source: "disabled".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::now_ms;
    use crate::presence_history::MemoryPresenceHistoryStore;
    use crate::presence_history::test_support::transition;

    #[tokio::test]
    async fn snapshot_at_reconstructs_membership_from_events() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();

        // u1 joins, u2 joins, u1 leaves
        store
            .record_transition(transition(
                base,
                "join-u1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-u2",
                PresenceHistoryEventKind::MemberAdded,
                "u2",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "leave-u1",
                PresenceHistoryEventKind::MemberRemoved,
                "u1",
            ))
            .await
            .unwrap();

        // Snapshot at latest: only u2 should remain
        let snapshot = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: None,
                at_serial: None,
            })
            .await
            .unwrap();

        assert_eq!(snapshot.members.len(), 1);
        assert_eq!(snapshot.members[0].user_id, "u2");
        assert_eq!(snapshot.events_replayed, 3);
        assert!(snapshot.complete);

        // Snapshot at serial 2: both u1 and u2 should be present
        let snapshot_at_2 = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: None,
                at_serial: Some(2),
            })
            .await
            .unwrap();

        assert_eq!(snapshot_at_2.members.len(), 2);
        assert_eq!(snapshot_at_2.events_replayed, 2);
        assert_eq!(snapshot_at_2.snapshot_serial, Some(2));

        // Snapshot at time base+1: only u1 and u2 joined, u1 not yet left
        let snapshot_at_time = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: Some(base + 1),
                at_serial: None,
            })
            .await
            .unwrap();

        assert_eq!(snapshot_at_time.members.len(), 2);
        assert_eq!(snapshot_at_time.events_replayed, 2);
    }
}
