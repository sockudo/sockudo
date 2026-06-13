use super::types::*;
use crate::error::Result;
use crate::history::now_ms;
use crate::versioned_messages::MessageSerial;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct MemoryAnnotationStore {
    pub(super) state: Arc<RwLock<MemoryAnnotationState>>,
}

#[derive(Default)]
pub(super) struct MemoryAnnotationState {
    pub(super) events_by_projection:
        BTreeMap<String, BTreeMap<AnnotationSerial, StoredAnnotationEvent>>,
    pub(super) raw_by_channel: BTreeMap<String, BTreeMap<AnnotationSerial, StoredAnnotationEvent>>,
    pub(super) projections: BTreeMap<String, StoredAnnotationProjection>,
}

impl MemoryAnnotationStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn channel_key(app_id: &str, channel_id: &str) -> String {
        format!("{app_id}\0{channel_id}")
    }

    fn projection_key(
        app_id: &str,
        channel_id: &str,
        message_serial: &MessageSerial,
        annotation_type: &AnnotationType,
    ) -> String {
        format!(
            "{}\0{}\0{}",
            Self::channel_key(app_id, channel_id),
            message_serial.as_str(),
            annotation_type.as_str()
        )
    }

    fn request_projection_key(request: &AnnotationProjectionRequest) -> String {
        Self::projection_key(
            &request.app_id,
            &request.channel_id,
            &request.message_serial,
            &request.annotation_type,
        )
    }

    pub(super) fn event_projection_key(record: &StoredAnnotationEvent) -> String {
        Self::projection_key(
            &record.app_id,
            &record.channel_id,
            record.message_serial(),
            record.annotation_type(),
        )
    }

    fn projection_max_serial(
        state: &MemoryAnnotationState,
        projection_key: &str,
    ) -> Option<AnnotationSerial> {
        state
            .events_by_projection
            .get(projection_key)
            .and_then(|events| events.keys().next_back().cloned())
    }

    fn projection_events(
        state: &MemoryAnnotationState,
        projection_key: &str,
    ) -> Vec<StoredAnnotationEvent> {
        state
            .events_by_projection
            .get(projection_key)
            .map(|events| events.values().cloned().collect())
            .unwrap_or_default()
    }

    fn build_projection(
        request: &AnnotationProjectionRequest,
        events: Vec<StoredAnnotationEvent>,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let projection = AnnotationProjection::rebuild_with_options(
            request.channel_id.clone(),
            request.message_serial.clone(),
            request.annotation_type.clone(),
            events.into_iter().map(|record| record.annotation),
            options,
        )?;
        Ok(StoredAnnotationProjection::from_projection(
            request.app_id.clone(),
            projection,
        ))
    }

    fn rebuild_projection_from_state(
        state: &mut MemoryAnnotationState,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let projection_key = Self::request_projection_key(&request);
        let events = Self::projection_events(state, &projection_key);
        let stored = Self::build_projection(&request, events, options)?;
        state.projections.insert(projection_key, stored.clone());
        Ok(stored)
    }

    async fn rebuild_projection_optimistic(
        &self,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        request.validate()?;
        let projection_key = Self::request_projection_key(&request);

        loop {
            let (events, expected_last_serial) = {
                let state = self.state.read().await;
                (
                    Self::projection_events(&state, &projection_key),
                    Self::projection_max_serial(&state, &projection_key),
                )
            };

            let projection = Self::build_projection(&request, events, options)?;
            let mut state = self.state.write().await;
            let current_last_serial = Self::projection_max_serial(&state, &projection_key);
            if current_last_serial == expected_last_serial {
                state
                    .projections
                    .insert(projection_key.clone(), projection.clone());
                return Ok(projection);
            }
        }
    }
}

#[async_trait]
impl AnnotationStore for MemoryAnnotationStore {
    async fn append_event(
        &self,
        mut record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection> {
        record.validate()?;
        if record.stored_at_ms == 0 {
            record.stored_at_ms = now_ms();
        }

        let projection_request = AnnotationProjectionRequest {
            app_id: record.app_id.clone(),
            channel_id: record.channel_id.clone(),
            message_serial: record.message_serial().clone(),
            annotation_type: record.annotation_type().clone(),
        };
        let projection_key = Self::event_projection_key(&record);
        let channel_key = Self::channel_key(&record.app_id, &record.channel_id);

        {
            let mut state = self.state.write().await;
            let events = state
                .events_by_projection
                .entry(projection_key)
                .or_default();
            events
                .entry(record.annotation_serial().clone())
                .or_insert_with(|| record.clone());
            state
                .raw_by_channel
                .entry(channel_key)
                .or_default()
                .entry(record.annotation_serial().clone())
                .or_insert(record);
        }

        self.rebuild_projection_optimistic(
            projection_request,
            AnnotationProjectionOptions::default(),
        )
        .await
    }

    async fn get_events(
        &self,
        request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        let key = Self::projection_key(
            &request.app_id,
            &request.channel_id,
            &request.message_serial,
            &request.annotation_type,
        );
        let state = self.state.read().await;
        Ok(state
            .events_by_projection
            .get(&key)
            .map(|events| events.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn replay_raw(
        &self,
        request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel_id);
        let state = self.state.read().await;
        let Some(events) = state.raw_by_channel.get(&key) else {
            return Ok(Vec::new());
        };

        let items = events
            .iter()
            .filter(|(serial, _)| {
                request
                    .after_annotation_serial
                    .as_ref()
                    .is_none_or(|after| *serial > after)
            })
            .map(|(_, record)| record.clone())
            .take(request.limit)
            .collect();
        Ok(items)
    }

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel_id);
        let state = self.state.read().await;
        Ok(state
            .raw_by_channel
            .get(&key)
            .and_then(|events| events.get(&request.annotation_serial).cloned()))
    }

    async fn get_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>> {
        request.validate()?;
        let key = Self::request_projection_key(&request);
        let state = self.state.read().await;
        let projection = state.projections.get(&key).cloned();
        let max_serial = Self::projection_max_serial(&state, &key);
        if projection
            .as_ref()
            .is_some_and(|projection| projection.last_annotation_serial == max_serial)
        {
            return Ok(projection);
        }
        if projection.is_none() && max_serial.is_none() {
            return Ok(None);
        }
        drop(state);

        self.rebuild_projection_optimistic(request, AnnotationProjectionOptions::default())
            .await
            .map(Some)
    }

    async fn list_projections_for_channel(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<Vec<StoredAnnotationProjection>> {
        let (projections, _) = self
            .list_projections_for_channel_with_rebuild_count(request)
            .await?;
        Ok(projections)
    }

    async fn list_projections_for_channel_with_rebuild_count(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<(Vec<StoredAnnotationProjection>, usize)> {
        request.validate()?;
        let requests = {
            let state = self.state.read().await;
            let mut requests = BTreeMap::new();
            for events in state.events_by_projection.values() {
                let Some(record) = events.values().next() else {
                    continue;
                };
                if record.app_id == request.app_id && record.channel_id == request.channel_id {
                    let projection_request = AnnotationProjectionRequest {
                        app_id: record.app_id.clone(),
                        channel_id: record.channel_id.clone(),
                        message_serial: record.message_serial().clone(),
                        annotation_type: record.annotation_type().clone(),
                    };
                    requests.insert(
                        Self::request_projection_key(&projection_request),
                        projection_request,
                    );
                }
            }
            for projection in state.projections.values() {
                if projection.app_id == request.app_id
                    && projection.channel_id == request.channel_id
                {
                    let projection_request = projection.projection_key();
                    requests.insert(
                        Self::request_projection_key(&projection_request),
                        projection_request,
                    );
                }
            }
            requests.into_values().collect::<Vec<_>>()
        };

        let mut projections = Vec::new();
        let mut rebuild_count = 0;
        for projection_request in requests {
            let should_rebuild = {
                let state = self.state.read().await;
                let key = Self::request_projection_key(&projection_request);
                let projection = state.projections.get(&key);
                let max_serial = Self::projection_max_serial(&state, &key);
                match (projection, max_serial) {
                    (None, Some(_)) => true,
                    (Some(projection), max_serial) => {
                        projection.last_annotation_serial != max_serial
                    }
                    _ => false,
                }
            };
            if let Some(projection) = self.get_projection(projection_request).await? {
                if should_rebuild {
                    rebuild_count += 1;
                }
                projections.push(projection);
            }
        }
        projections.sort_by(|left, right| {
            left.message_serial
                .cmp(&right.message_serial)
                .then_with(|| left.annotation_type.cmp(&right.annotation_type))
        });
        Ok((projections, rebuild_count))
    }

    async fn rebuild_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<StoredAnnotationProjection> {
        self.rebuild_projection_optimistic(request, AnnotationProjectionOptions::default())
            .await
    }

    async fn rebuild_projection_with_options(
        &self,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        self.rebuild_projection_optimistic(request, options).await
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }

        let mut state = self.state.write().await;
        let mut deleted = 0_u64;
        let mut has_more = false;
        let mut affected_projection_keys = BTreeSet::new();
        let mut raw_removals = Vec::new();

        for (projection_key, events) in state.events_by_projection.iter_mut() {
            let remaining = batch_size.saturating_sub(deleted as usize);
            if remaining == 0 {
                has_more = true;
                break;
            }

            let to_remove = events
                .iter()
                .filter(|(_, record)| record.stored_at_ms < before_ms)
                .map(|(serial, _)| serial.clone())
                .take(remaining)
                .collect::<Vec<_>>();

            for serial in to_remove {
                if let Some(record) = events.remove(&serial) {
                    deleted += 1;
                    affected_projection_keys.insert(projection_key.clone());
                    raw_removals.push((
                        Self::channel_key(&record.app_id, &record.channel_id),
                        serial,
                    ));
                }
            }
        }

        for (channel_key, serial) in raw_removals {
            if let Some(raw) = state.raw_by_channel.get_mut(&channel_key) {
                raw.remove(&serial);
            }
        }

        state
            .events_by_projection
            .retain(|_, events| !events.is_empty());
        state.raw_by_channel.retain(|_, events| !events.is_empty());

        let affected_projection_keys = affected_projection_keys.into_iter().collect::<Vec<_>>();
        let requests = affected_projection_keys
            .iter()
            .filter_map(|key| {
                state
                    .events_by_projection
                    .get(key)
                    .and_then(|events| events.values().next())
                    .map(|record| AnnotationProjectionRequest {
                        app_id: record.app_id.clone(),
                        channel_id: record.channel_id.clone(),
                        message_serial: record.message_serial().clone(),
                        annotation_type: record.annotation_type().clone(),
                    })
            })
            .collect::<Vec<_>>();

        for request in requests {
            Self::rebuild_projection_from_state(
                &mut state,
                request,
                AnnotationProjectionOptions::default(),
            )?;
        }
        for key in affected_projection_keys {
            if !state.events_by_projection.contains_key(&key) {
                state.projections.remove(&key);
            }
        }

        if !has_more
            && state.events_by_projection.values().any(|events| {
                events
                    .values()
                    .any(|record| record.stored_at_ms < before_ms)
            })
        {
            has_more = true;
        }

        Ok((deleted, has_more))
    }
}
