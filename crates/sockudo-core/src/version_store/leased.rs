use super::store::VersionStore;
use super::types::*;
use crate::error::{Error, Result};
use crate::versioned_messages::MessageSerial;
use ahash::AHashMap;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LeaseKey {
    app_id: String,
    channel: String,
}

impl LeaseKey {
    fn new(app_id: &str, channel: &str) -> Self {
        Self {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct LeaseCursor {
    stream_id: String,
    next_delivery_serial: u64,
    end_exclusive: u64,
}

impl LeaseCursor {
    fn from_block(block: VersionWriteReservationBlock) -> Self {
        Self {
            stream_id: block.stream_id,
            next_delivery_serial: block.start_delivery_serial,
            end_exclusive: block.start_delivery_serial.saturating_add(block.len),
        }
    }

    fn take_next(&mut self) -> Option<VersionWriteReservation> {
        if self.next_delivery_serial >= self.end_exclusive {
            return None;
        }
        let reservation = VersionWriteReservation {
            stream_id: self.stream_id.clone(),
            delivery_serial: self.next_delivery_serial,
        };
        self.next_delivery_serial = self.next_delivery_serial.saturating_add(1);
        Some(reservation)
    }
}

#[derive(Default)]
struct LeaseState {
    leases: AHashMap<LeaseKey, LeaseCursor>,
    in_flight: AHashMap<LeaseKey, Vec<oneshot::Sender<()>>>,
}

/// Caches contiguous delivery-position blocks from an underlying [`VersionStore`].
///
/// The wrapper keeps the publish hot path at one store round-trip per lease
/// instead of one store round-trip per append. A small single-flight table
/// prevents concurrent lease misses from over-reserving unused serial ranges.
pub struct LeasedVersionStore {
    inner: Arc<dyn VersionStore + Send + Sync>,
    block_size: u64,
    state: Mutex<LeaseState>,
}

impl LeasedVersionStore {
    #[must_use]
    pub fn new(inner: Arc<dyn VersionStore + Send + Sync>, block_size: u64) -> Self {
        Self {
            inner,
            block_size: block_size.max(1),
            state: Mutex::new(LeaseState::default()),
        }
    }

    fn take_cached(&self, key: &LeaseKey) -> Option<VersionWriteReservation> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let cursor = state.leases.get_mut(key)?;
        let reservation = cursor.take_next();
        if cursor.next_delivery_serial >= cursor.end_exclusive {
            state.leases.remove(key);
        }
        reservation
    }

    fn take_cached_after(
        &self,
        key: &LeaseKey,
        after_delivery_serial: u64,
    ) -> Option<VersionWriteReservation> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let cursor = state.leases.get_mut(key)?;
        if cursor.next_delivery_serial <= after_delivery_serial {
            let next_after = after_delivery_serial.saturating_add(1);
            if next_after >= cursor.end_exclusive {
                state.leases.remove(key);
                return None;
            }
            cursor.next_delivery_serial = next_after;
        }

        let reservation = cursor.take_next();
        if cursor.next_delivery_serial >= cursor.end_exclusive {
            state.leases.remove(key);
        }
        reservation
    }

    fn start_or_join_reservation(&self, key: LeaseKey) -> Option<oneshot::Receiver<()>> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(waiters) = state.in_flight.get_mut(&key) {
            let (tx, rx) = oneshot::channel();
            waiters.push(tx);
            Some(rx)
        } else {
            state.in_flight.insert(key, Vec::new());
            None
        }
    }

    fn finish_reservation(&self, key: LeaseKey, block: VersionWriteReservationBlock) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        state
            .leases
            .insert(key.clone(), LeaseCursor::from_block(block));
        if let Some(waiters) = state.in_flight.remove(&key) {
            for waiter in waiters {
                let _ = waiter.send(());
            }
        }
    }

    fn fail_reservation(&self, key: &LeaseKey) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(waiters) = state.in_flight.remove(key) {
            for waiter in waiters {
                let _ = waiter.send(());
            }
        }
    }
}

#[async_trait]
impl VersionStore for LeasedVersionStore {
    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation> {
        if self.block_size == 1 {
            return self.inner.reserve_delivery_position(app_id, channel).await;
        }

        let key = LeaseKey::new(app_id, channel);
        loop {
            if let Some(reservation) = self.take_cached(&key) {
                return Ok(reservation);
            }

            if let Some(waiter) = self.start_or_join_reservation(key.clone()) {
                let _ = waiter.await;
                continue;
            }

            match self
                .inner
                .reserve_delivery_positions(app_id, channel, self.block_size)
                .await
            {
                Ok(block) => self.finish_reservation(key.clone(), block),
                Err(err) => {
                    self.fail_reservation(&key);
                    return Err(err);
                }
            }
        }
    }

    async fn reserve_delivery_positions(
        &self,
        app_id: &str,
        channel: &str,
        block_size: u64,
    ) -> Result<VersionWriteReservationBlock> {
        VersionWriteReservationBlock::validate(block_size)?;
        let first = self.reserve_delivery_position(app_id, channel).await?;
        let mut expected_next = first.delivery_serial.saturating_add(1);
        for _ in 1..block_size {
            let next = self.reserve_delivery_position(app_id, channel).await?;
            if next.stream_id != first.stream_id || next.delivery_serial != expected_next {
                return Err(Error::Internal(
                    "leased version store returned a non-contiguous reservation block".to_string(),
                ));
            }
            expected_next = expected_next.saturating_add(1);
        }
        Ok(VersionWriteReservationBlock {
            stream_id: first.stream_id,
            start_delivery_serial: first.delivery_serial,
            len: block_size,
        })
    }

    async fn reserve_delivery_position_after(
        &self,
        app_id: &str,
        channel: &str,
        after_delivery_serial: u64,
    ) -> Result<VersionWriteReservation> {
        let max_attempts = self.block_size.saturating_mul(2).max(64);
        for _ in 0..max_attempts {
            let key = LeaseKey::new(app_id, channel);
            if let Some(reservation) = self.take_cached_after(&key, after_delivery_serial) {
                return Ok(reservation);
            }

            if let Some(waiter) = self.start_or_join_reservation(key.clone()) {
                let _ = waiter.await;
                continue;
            }

            match self
                .inner
                .reserve_delivery_positions(app_id, channel, self.block_size)
                .await
            {
                Ok(block) => self.finish_reservation(key.clone(), block),
                Err(err) => {
                    self.fail_reservation(&key);
                    return Err(err);
                }
            }
        }

        Err(Error::Internal(format!(
            "leased version store could not reserve delivery_serial greater than {after_delivery_serial}"
        )))
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        self.inner.append_version(record).await
    }

    async fn commit_create(&self, request: VersionCreateRequest) -> Result<VersionCreateResult> {
        // Atomic commits own their delivery position in the durable store and
        // therefore deliberately bypass process-local reservation leases.
        self.inner.commit_create(request).await
    }

    async fn compare_and_apply(
        &self,
        request: VersionMutationRequest,
    ) -> Result<VersionMutationResult> {
        // Mutations deliberately bypass delivery leases: the underlying store
        // owns predecessor validation and delivery allocation in one commit.
        self.inner.compare_and_apply(request).await
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        self.inner.get_latest(app_id, channel, message_serial).await
    }

    async fn get_latest_batch(
        &self,
        app_id: &str,
        channel: &str,
        message_serials: &[MessageSerial],
    ) -> Result<BTreeMap<MessageSerial, StoredVersionRecord>> {
        self.inner
            .get_latest_batch(app_id, channel, message_serials)
            .await
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        self.inner.get_versions(request).await
    }

    async fn replay_after(
        &self,
        request: VersionReplayRequest,
    ) -> Result<Vec<StoredVersionRecord>> {
        self.inner.replay_after(request).await
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        self.inner.latest_by_history(app_id, channel).await
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        self.inner.stream_state(app_id, channel).await
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        self.inner.purge_before(before_ms, batch_size).await
    }
}
