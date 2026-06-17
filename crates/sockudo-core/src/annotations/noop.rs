use super::types::*;
use crate::error::{Error, Result};
use async_trait::async_trait;

#[derive(Default)]
pub struct NoopAnnotationStore;

#[async_trait]
impl AnnotationStore for NoopAnnotationStore {
    async fn append_event(
        &self,
        _record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn get_events(
        &self,
        _request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn replay_raw(
        &self,
        _request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn get_event_by_serial(
        &self,
        _request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn get_projection(
        &self,
        _request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn rebuild_projection(
        &self,
        _request: AnnotationProjectionRequest,
    ) -> Result<StoredAnnotationProjection> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn rebuild_projection_with_options(
        &self,
        _request: AnnotationProjectionRequest,
        _options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }
}
