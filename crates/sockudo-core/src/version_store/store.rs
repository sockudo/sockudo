use super::types::*;
use crate::error::{Error, Result};
use crate::versioned_messages::MessageSerial;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};

#[async_trait]
pub trait VersionStore: Send + Sync {
    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation>;

    async fn reserve_delivery_positions(
        &self,
        app_id: &str,
        channel: &str,
        block_size: u64,
    ) -> Result<VersionWriteReservationBlock> {
        VersionWriteReservationBlock::validate(block_size)?;
        if block_size == 1 {
            let reservation = self.reserve_delivery_position(app_id, channel).await?;
            return Ok(VersionWriteReservationBlock {
                stream_id: reservation.stream_id,
                start_delivery_serial: reservation.delivery_serial,
                len: 1,
            });
        }

        Err(Error::Configuration(
            "version store does not support block delivery reservations".to_string(),
        ))
    }

    async fn reserve_delivery_position_after(
        &self,
        app_id: &str,
        channel: &str,
        after_delivery_serial: u64,
    ) -> Result<VersionWriteReservation> {
        for _ in 0..1024 {
            let reservation = self.reserve_delivery_position(app_id, channel).await?;
            if reservation.delivery_serial > after_delivery_serial {
                return Ok(reservation);
            }
        }

        Err(Error::Internal(format!(
            "version store could not reserve delivery_serial greater than {after_delivery_serial}"
        )))
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()>;

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>>;

    /// Fetch the latest visible versions for a bounded set of logical messages.
    ///
    /// Missing serials are omitted from the returned map. Backends should
    /// override this when they can perform a more selective batch read. The
    /// fallback deliberately uses one channel-wide read instead of issuing one
    /// storage request per history row.
    async fn get_latest_batch(
        &self,
        app_id: &str,
        channel: &str,
        message_serials: &[MessageSerial],
    ) -> Result<BTreeMap<MessageSerial, StoredVersionRecord>> {
        if message_serials.is_empty() {
            return Ok(BTreeMap::new());
        }

        let requested = message_serials.iter().collect::<BTreeSet<_>>();
        let latest = self.latest_by_history(app_id, channel).await?;
        Ok(latest
            .into_iter()
            .filter_map(|record| {
                requested
                    .contains(record.message_serial())
                    .then(|| (record.message_serial().clone(), record))
            })
            .collect())
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage>;

    async fn replay_after(&self, request: VersionReplayRequest)
    -> Result<Vec<StoredVersionRecord>>;

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>>;

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState>;

    /// Purge version entries whose server-side `created_at_ms` is strictly
    /// older than `before_ms`. Backends with native TTL (ScyllaDB, DynamoDB)
    /// return `(0, false)` — the storage engine handles expiry asynchronously.
    ///
    /// `batch_size` caps the rows deleted per call so transaction/lock sizes
    /// stay bounded. Returns `(rows_deleted, has_more)`; callers loop while
    /// `has_more` is true, subject to a caller-supplied per-tick budget.
    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        let _ = (before_ms, batch_size);
        Ok((0, false))
    }
}

#[derive(Default)]
pub struct NoopVersionStore;

#[async_trait]
impl VersionStore for NoopVersionStore {
    async fn reserve_delivery_position(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<VersionWriteReservation> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn reserve_delivery_positions(
        &self,
        _app_id: &str,
        _channel: &str,
        _block_size: u64,
    ) -> Result<VersionWriteReservationBlock> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn append_version(&self, _record: StoredVersionRecord) -> Result<()> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn get_latest(
        &self,
        _app_id: &str,
        _channel: &str,
        _message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn get_versions(&self, _request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn replay_after(
        &self,
        _request: VersionReplayRequest,
    ) -> Result<Vec<StoredVersionRecord>> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn latest_by_history(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn stream_state(&self, _app_id: &str, _channel: &str) -> Result<VersionStreamState> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }
}
