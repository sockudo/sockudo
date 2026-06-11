use super::store::PresenceHistoryStore;
use super::types::{
    PresenceHistoryDurableState, PresenceHistoryPage, PresenceHistoryReadRequest,
    PresenceHistoryResetResult, PresenceHistoryRuntimeStatus, PresenceHistoryStreamInspection,
    PresenceHistoryStreamRuntimeState, PresenceHistoryTransitionRecord,
};
use crate::error::Result;
use crate::history::now_ms;
use crate::metrics::MetricsInterface;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct TrackingPresenceHistoryStore {
    inner: Arc<dyn PresenceHistoryStore + Send + Sync>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    state_authority: String,
    runtime_states: Arc<RwLock<BTreeMap<String, PresenceHistoryStreamRuntimeState>>>,
}

impl TrackingPresenceHistoryStore {
    pub fn new(
        inner: Arc<dyn PresenceHistoryStore + Send + Sync>,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        state_authority: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            metrics,
            state_authority: state_authority.into(),
            runtime_states: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn channel_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }

    async fn current_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Option<PresenceHistoryStreamRuntimeState> {
        let key = Self::channel_key(app_id, channel);
        self.runtime_states.read().await.get(&key).cloned()
    }

    async fn publish_metrics(&self, app_id: &str) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };

        let states = self.runtime_states.read().await;
        let prefix = format!("{app_id}\0");
        let (degraded, reset_required) = states
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .fold(
                (0usize, 0usize),
                |(degraded, reset_required), (_, state)| match state.durable_state {
                    PresenceHistoryDurableState::Healthy => (degraded, reset_required),
                    PresenceHistoryDurableState::Degraded => (degraded + 1, reset_required),
                    PresenceHistoryDurableState::ResetRequired => {
                        (degraded + 1, reset_required + 1)
                    }
                },
            );
        metrics.update_presence_history_degraded_channels(app_id, degraded);
        metrics.update_presence_history_reset_required_channels(app_id, reset_required);
    }
}

#[async_trait]
impl PresenceHistoryStore for TrackingPresenceHistoryStore {
    async fn record_transition(&self, record: PresenceHistoryTransitionRecord) -> Result<()> {
        let key = Self::channel_key(&record.app_id, &record.channel);
        let inspection_before = self
            .inner
            .stream_inspection(&record.app_id, &record.channel)
            .await?;
        match self.inner.record_transition(record).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let durable_state = if inspection_before.retained.retained_events > 0 {
                    PresenceHistoryDurableState::ResetRequired
                } else {
                    PresenceHistoryDurableState::Degraded
                };
                self.runtime_states.write().await.insert(
                    key,
                    PresenceHistoryStreamRuntimeState {
                        app_id: inspection_before.app_id.clone(),
                        channel: inspection_before.channel.clone(),
                        stream_id: inspection_before.stream_id.clone(),
                        durable_state,
                        continuity_proven: durable_state.continuity_proven(),
                        reset_required: durable_state.reset_required(),
                        reason: Some(
                            match durable_state {
                                PresenceHistoryDurableState::Healthy => "presence_history_healthy",
                                PresenceHistoryDurableState::Degraded => {
                                    "presence_history_write_failed"
                                }
                                PresenceHistoryDurableState::ResetRequired => {
                                    "presence_history_reset_required_after_write_failure"
                                }
                            }
                            .to_string(),
                        ),
                        node_id: None,
                        last_transition_at_ms: Some(now_ms()),
                        authoritative_source: self.state_authority.clone(),
                        observed_source: self.state_authority.clone(),
                    },
                );
                self.publish_metrics(&inspection_before.app_id).await;
                Err(error)
            }
        }
    }

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage> {
        let mut page = self.inner.read_page(request.clone()).await?;
        if self
            .current_state(&request.app_id, &request.channel)
            .await
            .is_some_and(|state| state.durable_state != PresenceHistoryDurableState::Healthy)
        {
            page.complete = false;
            page.degraded = true;
        }
        Ok(page)
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        let inner_state = self.inner.stream_runtime_state(app_id, channel).await?;
        Ok(self
            .current_state(app_id, channel)
            .await
            .unwrap_or(inner_state))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamInspection> {
        let mut inspection = self.inner.stream_inspection(app_id, channel).await?;
        if let Some(state) = self.current_state(app_id, channel).await {
            inspection.state = state;
        }
        Ok(inspection)
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<PresenceHistoryResetResult> {
        let mut result = self
            .inner
            .reset_stream(app_id, channel, reason, requested_by)
            .await?;
        let key = Self::channel_key(app_id, channel);
        self.runtime_states.write().await.remove(&key);
        self.publish_metrics(app_id).await;
        result.inspection.state = self.inner.stream_runtime_state(app_id, channel).await?;
        Ok(result)
    }

    async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
        let mut status = self.inner.runtime_status().await?;
        status.state_authority = self.state_authority.clone();
        let states = self.runtime_states.read().await;
        let (degraded, reset_required) = states.values().fold(
            (0usize, 0usize),
            |(degraded, reset_required), state| match state.durable_state {
                PresenceHistoryDurableState::Healthy => (degraded, reset_required),
                PresenceHistoryDurableState::Degraded => (degraded + 1, reset_required),
                PresenceHistoryDurableState::ResetRequired => (degraded + 1, reset_required + 1),
            },
        );
        status.degraded_channels = degraded;
        status.reset_required_channels = reset_required;
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::history::now_ms;
    use crate::presence_history::{
        MemoryPresenceHistoryStore, PresenceHistoryDirection, PresenceHistoryEventKind,
        PresenceHistoryQueryBounds, PresenceHistoryRetentionStats, test_support::transition,
    };

    struct FailingPresenceHistoryStore;

    #[async_trait]
    impl PresenceHistoryStore for FailingPresenceHistoryStore {
        async fn record_transition(&self, _record: PresenceHistoryTransitionRecord) -> Result<()> {
            Err(Error::Internal(
                "forced presence history failure".to_string(),
            ))
        }

        async fn read_page(
            &self,
            request: PresenceHistoryReadRequest,
        ) -> Result<PresenceHistoryPage> {
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

        async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
            Ok(PresenceHistoryRuntimeStatus {
                enabled: true,
                backend: "failing".to_string(),
                state_authority: "failing".to_string(),
                degraded_channels: 0,
                reset_required_channels: 0,
                queue_depth: 0,
            })
        }
    }

    #[tokio::test]
    async fn tracking_presence_history_store_marks_failed_channels_degraded() {
        let store = TrackingPresenceHistoryStore::new(
            Arc::new(FailingPresenceHistoryStore),
            None,
            "tracking_wrapper",
        );
        let error = store
            .record_transition(transition(
                now_ms(),
                "join-1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Internal(_)));

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();
        assert!(page.degraded);
        assert!(!page.complete);

        let status = store.runtime_status().await.unwrap();
        assert_eq!(status.degraded_channels, 1);
        assert_eq!(status.reset_required_channels, 0);
    }

    #[tokio::test]
    async fn tracking_presence_history_store_escalates_existing_stream_failures_to_reset_required()
    {
        let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        inner
            .record_transition(transition(
                now_ms(),
                "join-1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap();

        struct ExistingStreamFailingStore {
            inner: Arc<MemoryPresenceHistoryStore>,
        }

        #[async_trait]
        impl PresenceHistoryStore for ExistingStreamFailingStore {
            async fn record_transition(
                &self,
                _record: PresenceHistoryTransitionRecord,
            ) -> Result<()> {
                Err(Error::Internal(
                    "forced failure after existing history".to_string(),
                ))
            }

            async fn read_page(
                &self,
                request: PresenceHistoryReadRequest,
            ) -> Result<PresenceHistoryPage> {
                self.inner.read_page(request).await
            }

            async fn stream_runtime_state(
                &self,
                app_id: &str,
                channel: &str,
            ) -> Result<PresenceHistoryStreamRuntimeState> {
                self.inner.stream_runtime_state(app_id, channel).await
            }

            async fn stream_inspection(
                &self,
                app_id: &str,
                channel: &str,
            ) -> Result<PresenceHistoryStreamInspection> {
                self.inner.stream_inspection(app_id, channel).await
            }

            async fn reset_stream(
                &self,
                app_id: &str,
                channel: &str,
                reason: &str,
                requested_by: Option<&str>,
            ) -> Result<PresenceHistoryResetResult> {
                self.inner
                    .reset_stream(app_id, channel, reason, requested_by)
                    .await
            }

            async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
                self.inner.runtime_status().await
            }
        }

        let store = TrackingPresenceHistoryStore::new(
            Arc::new(ExistingStreamFailingStore {
                inner: inner.clone(),
            }),
            None,
            "tracking_wrapper",
        );
        store
            .record_transition(transition(
                now_ms() + 1,
                "leave-1",
                PresenceHistoryEventKind::MemberRemoved,
                "u1",
            ))
            .await
            .unwrap_err();

        let state = store
            .stream_runtime_state("app", "presence-room")
            .await
            .unwrap();
        assert_eq!(
            state.durable_state,
            PresenceHistoryDurableState::ResetRequired
        );
        assert!(!state.continuity_proven);
        assert!(state.reset_required);

        let reset = store
            .reset_stream("app", "presence-room", "operator reset", Some("ops"))
            .await
            .unwrap();
        assert_eq!(reset.purged_events, 1);
        assert_eq!(
            reset.inspection.state.durable_state,
            PresenceHistoryDurableState::Healthy
        );
    }

    // --- Prompt 13: Chaos, load, and failover tests ---

    #[tokio::test]
    async fn tracking_store_fail_closed_degrades_on_write_failure() {
        let failing_store = Arc::new(FailingPresenceHistoryStore);
        let tracker = TrackingPresenceHistoryStore::new(failing_store, None, "test-node");
        let base = now_ms();

        // Write should fail but tracker should mark degraded, not panic
        let result = tracker
            .record_transition(transition(
                base,
                "k1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await;
        assert!(result.is_err());

        // Stream should be degraded
        let state = tracker
            .stream_runtime_state("app", "presence-room")
            .await
            .unwrap();
        assert!(
            !state.continuity_proven,
            "continuity should not be proven after write failure"
        );
    }
}
