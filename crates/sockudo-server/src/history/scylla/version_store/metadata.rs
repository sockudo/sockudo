use super::*;
use futures_util::TryStreamExt;

impl ScyllaVersionStore {
    /// Merge two ordered metadata streams without retaining channel payloads or
    /// a channel-sized identity set. Atomic records win over legacy projections.
    pub(super) async fn metadata_counts(
        &self,
        app_id: &str,
        channel: &str,
        count_active: bool,
    ) -> Result<(u64, usize)> {
        let mut atomic_query = Statement::new(format!(
            "SELECT commit_key, is_open_stream FROM {} WHERE app_id=? AND channel=? AND commit_key >= 'm:' AND commit_key < 'n:'",
            self.tables.version_commits_fq()
        ));
        atomic_query.set_page_size(100);
        let mut legacy_query = Statement::new(format!(
            "SELECT message_serial FROM {} WHERE app_id=? AND channel=?",
            self.tables.version_messages_fq()
        ));
        legacy_query.set_page_size(100);
        let atomic = self
            .session
            .query_iter(atomic_query, (app_id, channel))
            .await
            .map_err(|e| {
                Error::Internal(format!("failed to query atomic version metadata: {e}"))
            })?;
        let legacy = self
            .session
            .query_iter(legacy_query, (app_id, channel))
            .await
            .map_err(|e| {
                Error::Internal(format!("failed to query legacy version metadata: {e}"))
            })?;
        let mut atomic = atomic
            .rows_stream::<(String, Option<bool>)>()
            .map_err(|e| {
                Error::Internal(format!("failed to decode atomic version metadata: {e}"))
            })?;
        let mut legacy = legacy.rows_stream::<(String,)>().map_err(|e| {
            Error::Internal(format!("failed to decode legacy version metadata: {e}"))
        })?;
        let mut next_atomic = atomic
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("failed to read atomic version metadata: {e}")))?;
        let mut next_legacy = legacy
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("failed to read legacy version metadata: {e}")))?;
        let mut messages = 0u64;
        let mut active = 0usize;
        while next_atomic.is_some() || next_legacy.is_some() {
            let atomic_serial = next_atomic
                .as_ref()
                .map(|(key, _)| {
                    key.strip_prefix("m:").ok_or_else(|| {
                        Error::Internal("invalid atomic version metadata key".into())
                    })
                })
                .transpose()?;
            let legacy_serial = next_legacy.as_ref().map(|(serial,)| serial.as_str());
            let take_atomic = atomic_serial
                .is_some_and(|serial| legacy_serial.is_none_or(|legacy| serial <= legacy));
            let take_legacy = legacy_serial
                .is_some_and(|serial| atomic_serial.is_none_or(|atomic| serial <= atomic));
            let (serial, open) = if take_atomic {
                let (key, open) = next_atomic.as_ref().expect("atomic row selected");
                (
                    key.strip_prefix("m:")
                        .expect("validated metadata prefix")
                        .to_owned(),
                    *open,
                )
            } else {
                (
                    next_legacy.as_ref().expect("legacy row selected").0.clone(),
                    None,
                )
            };
            // TTL can retire the legacy payload before its later-written pointer.
            // Count only retained legacy messages, without loading their bodies.
            let retained = if take_atomic {
                true
            } else {
                self.session.query_unpaged(
                    format!("SELECT version_serial FROM {} WHERE app_id=? AND channel=? AND message_serial=? LIMIT 1", self.tables.version_entries_by_message_fq()),
                    (app_id, channel, serial.as_str()),
                ).await.map_err(|e| Error::Internal(format!("failed to read retained legacy version metadata: {e}")))?
                    .into_rows_result().map_err(|e| Error::Internal(format!("failed to decode retained legacy version metadata: {e}")))?
                    .rows_num() > 0
            };
            messages += u64::from(retained);
            if count_active && retained {
                let open = match open {
                    Some(open) => open,
                    None => self
                        .get_latest(
                            app_id,
                            channel,
                            &sockudo_core::versioned_messages::MessageSerial::new(serial)?,
                        )
                        .await?
                        .is_some_and(|record| record.is_open_ai_stream()),
                };
                active += usize::from(open);
            }
            if take_atomic {
                next_atomic = atomic.try_next().await.map_err(|e| {
                    Error::Internal(format!("failed to read atomic version metadata: {e}"))
                })?;
            }
            if take_legacy {
                next_legacy = legacy.try_next().await.map_err(|e| {
                    Error::Internal(format!("failed to read legacy version metadata: {e}"))
                })?;
            }
        }
        Ok((messages, active))
    }
}
