//! Bounded, disposable acceleration for durable annotation authorities.
//! A cache entry is reusable only under the backend's authoritative revision.
use sockudo_core::annotations::{
    AnnotationProjectionRequest, IncrementalProjection, StoredAnnotationEvent,
    StoredAnnotationProjection,
};
use sockudo_core::error::{Error, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};

const MAX_ENTRIES: usize = 64;
static REBUILDS: LazyLock<tokio::sync::Semaphore> =
    LazyLock::new(|| tokio::sync::Semaphore::new(4));
type ProjectionKey = (String, String, String, String);
const MAX_ACCOUNTED_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct AnnotationProjectionCache {
    active: Arc<tokio::sync::Semaphore>,
    entries: Mutex<BTreeMap<ProjectionKey, (u64, CachedProjection)>>,
}

impl Default for AnnotationProjectionCache {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
            active: Arc::new(tokio::sync::Semaphore::new(64)),
        }
    }
}

pub(super) struct CachedProjection {
    pub accumulator: IncrementalProjection,
    pub event_count: usize,
    pub valid_until: Option<i64>,
    pub expires_at: Option<i64>,
    accounted_bytes: usize,
    active: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl CachedProjection {
    pub fn rebuild(
        request: &AnnotationProjectionRequest,
        events: &[(StoredAnnotationEvent, Option<i64>)],
    ) -> Result<Self> {
        let mut ordered = events.iter().map(|(event, _)| event).collect::<Vec<_>>();
        ordered.sort_by(|left, right| left.annotation.serial.cmp(&right.annotation.serial));
        let accumulator = IncrementalProjection::rebuild(request, ordered.iter().copied())?;
        Ok(Self {
            accumulator,
            active: None,
            event_count: events.len(),
            valid_until: events.iter().filter_map(|(_, expiry)| *expiry).min(),
            expires_at: events.iter().filter_map(|(_, expiry)| *expiry).max(),
            accounted_bytes: events
                .iter()
                .map(|(event, _)| Self::event_bytes(event))
                .sum(),
        })
    }

    pub fn append(&mut self, record: &StoredAnnotationEvent, expiry: Option<i64>) -> Result<bool> {
        if !self.accumulator.append(&record.annotation)? {
            return Ok(false);
        }
        self.event_count += 1;
        self.accounted_bytes = self
            .accounted_bytes
            .saturating_add(Self::event_bytes(record));
        self.valid_until = self.valid_until.into_iter().chain(expiry).min();
        self.expires_at = self.expires_at.into_iter().chain(expiry).max();
        Ok(true)
    }

    fn event_bytes(record: &StoredAnnotationEvent) -> usize {
        // Conservative accounting includes IDs, summarizer maps, and allocator
        // overhead. Raw event bodies are not retained in the cache.
        [
            record.app_id.len(),
            record.channel_id.len(),
            record.annotation.id.as_str().len(),
            record.annotation.serial.as_str().len(),
            record.annotation.message_serial.as_str().len(),
            record.annotation.annotation_type.as_str().len(),
            record.annotation.name.as_ref().map_or(0, String::len),
            record.annotation.client_id.as_ref().map_or(0, String::len),
        ]
        .into_iter()
        .sum::<usize>()
        .saturating_mul(4)
        .saturating_add(1024)
    }
}

impl AnnotationProjectionCache {
    pub fn admit_rebuild(&self) -> Result<tokio::sync::SemaphorePermit<'static>> {
        REBUILDS.try_acquire().map_err(|_| {
            Error::BufferFull("annotation projection rebuild capacity is exhausted".into())
        })
    }

    fn key(request: &AnnotationProjectionRequest) -> ProjectionKey {
        (
            request.app_id.clone(),
            request.channel_id.clone(),
            request.message_serial.as_str().to_string(),
            request.annotation_type.as_str().to_string(),
        )
    }

    pub fn take(
        &self,
        request: &AnnotationProjectionRequest,
        revision: u64,
        current: Option<&StoredAnnotationProjection>,
    ) -> Option<CachedProjection> {
        let permit = self.active.clone().try_acquire_owned().ok()?;
        let (cached_revision, mut entry) = self
            .entries
            .lock()
            .expect("annotation cache poisoned")
            .remove(&Self::key(request))?;
        entry.active = Some(permit);
        (cached_revision == revision
            && current == Some(&entry.accumulator.projection)
            && entry
                .valid_until
                .is_none_or(|expiry| expiry > sockudo_core::history::now_ms().div_euclid(1000)))
        .then_some(entry)
    }

    pub fn put(
        &self,
        request: &AnnotationProjectionRequest,
        revision: u64,
        mut entry: CachedProjection,
    ) {
        entry.active.take();
        if entry.accounted_bytes > MAX_ACCOUNTED_BYTES {
            return;
        }
        let mut entries = self.entries.lock().expect("annotation cache poisoned");
        let key = Self::key(request);
        entries.remove(&key);
        let mut bytes: usize = entries
            .values()
            .map(|(_, entry)| entry.accounted_bytes)
            .sum();
        while entries.len() >= MAX_ENTRIES
            || bytes.saturating_add(entry.accounted_bytes) > MAX_ACCOUNTED_BYTES
        {
            let Some((_, (_, evicted))) = entries.pop_first() else {
                break;
            };
            bytes = bytes.saturating_sub(evicted.accounted_bytes);
        }
        entries.insert(key, (revision, entry));
    }
}

/// Limits retained decode input and per-event allocation overhead while a cold
/// projection is read. Driver pages are bounded separately by each backend.
#[derive(Default)]
pub(super) struct ProjectionReadBudget(usize);
impl ProjectionReadBudget {
    pub fn add(&mut self, encoded_bytes: usize) -> Result<()> {
        self.0 = self
            .0
            .saturating_add(encoded_bytes.saturating_mul(4).saturating_add(1024));
        if self.0 > 128 * 1024 * 1024 {
            return Err(Error::BufferFull(
                "annotation projection read exceeds its 128 MiB accounting budget".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stalled_cache_hits_cannot_be_replenished_without_bound() {
        let cache = AnnotationProjectionCache::default();
        let request = AnnotationProjectionRequest {
            app_id: "app".into(),
            channel_id: "room".into(),
            message_serial: sockudo_core::versioned_messages::MessageSerial::new("message")
                .unwrap(),
            annotation_type: sockudo_core::annotations::AnnotationType::new("reaction:distinct.v1")
                .unwrap(),
        };
        let mut stalled = Vec::new();
        for _ in 0..64 {
            let entry = CachedProjection::rebuild(&request, &[]).unwrap();
            let projection = entry.accumulator.projection.clone();
            cache.put(&request, 1, entry);
            stalled.push(cache.take(&request, 1, Some(&projection)).unwrap());
        }
        let entry = CachedProjection::rebuild(&request, &[]).unwrap();
        let projection = entry.accumulator.projection.clone();
        cache.put(&request, 1, entry);
        assert!(cache.take(&request, 1, Some(&projection)).is_none());
        drop(stalled.pop());
        assert!(cache.take(&request, 1, Some(&projection)).is_some());
    }
    #[test]
    fn read_budget_rejects_before_retaining_an_oversized_projection() {
        let mut budget = ProjectionReadBudget::default();
        for _ in 0..128 {
            budget.add(128 * 1024).unwrap();
        }
        assert!(matches!(
            budget.add(32 * 1024 * 1024),
            Err(Error::BufferFull(_))
        ));
    }
}
