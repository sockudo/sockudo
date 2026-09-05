use sockudo_core::error::{Error, Result};
use sockudo_core::history::HistoryAppendRecord;
use sockudo_core::metrics::MetricsInterface;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, warn};

use sqlx::{PgPool, Row};

use super::HistoryTables;
use super::history_store::PostgresHistoryStore;
use super::stream_state::{DegradeRequest, decrement_app_queue_depth, mark_channel_degraded};

#[derive(Clone)]
pub(super) struct WriterHandle {
    pub(super) tx: mpsc::Sender<HistoryAppendRecord>,
}

impl PostgresHistoryStore {
    pub(super) fn start_writers(&mut self) {
        for shard in 0..self.config.writer_shards {
            let (tx, mut rx) =
                mpsc::channel::<HistoryAppendRecord>(self.config.writer_queue_capacity);
            let pool = self.pool.clone();
            let tables = self.tables.clone();
            let metrics = self.metrics.clone();
            let cache_manager = self.cache_manager.clone();
            let degraded_channels = self.degraded_channels.clone();
            let queue_depth_total = self.queue_depth_total.clone();
            let queue_depth_by_app = self.queue_depth_by_app.clone();
            tokio::spawn(async move {
                let mut pending = None;
                loop {
                    let record = match pending.take() {
                        Some(record) => record,
                        None => match rx.recv().await {
                            Some(record) => record,
                            None => break,
                        },
                    };
                    let started = Instant::now();
                    let mut bytes = record.payload_bytes.len();
                    let mut records = vec![record];
                    // Consume only already queued compatible work. There is no batching
                    // sleep, and collection itself has a record, byte and time bound.
                    while records.len() < 64
                        && started.elapsed() < std::time::Duration::from_millis(1)
                    {
                        let Ok(next) = rx.try_recv() else { break };
                        if bytes.saturating_add(next.payload_bytes.len()) > 1024 * 1024
                            || !compatible_append(
                                records.last().expect("nonempty history batch"),
                                &next,
                            )
                        {
                            pending = Some(next);
                            break;
                        }
                        bytes += next.payload_bytes.len();
                        records.push(next);
                    }
                    for record in &records {
                        queue_depth_total.fetch_sub(1, Ordering::Relaxed);
                        decrement_app_queue_depth(
                            &queue_depth_by_app,
                            &record.app_id,
                            metrics.as_deref(),
                        );
                    }
                    let batched = if records.len() > 1 {
                        match Self::persist_records(&pool, &tables, &records, metrics.clone()).await
                        {
                            Ok(committed) => committed,
                            Err(error) => {
                                warn!(error = %error, shard, record_count = records.len(), "history batch failed retrying individual records");
                                false
                            }
                        }
                    } else {
                        false
                    };
                    for record in &records {
                        let result = if batched {
                            Ok(())
                        } else {
                            Self::persist_record(&pool, &tables, record, metrics.clone()).await
                        };
                        if let Err(err) = result {
                            error!(shard, app_id = %record.app_id, channel = %record.channel,
                                serial = record.serial, error = %err, "history write failed");
                            if let Some(metrics) = metrics.as_ref() {
                                metrics.mark_history_write_failure(&record.app_id);
                            }
                            mark_channel_degraded(
                                &pool,
                                &tables,
                                &degraded_channels,
                                cache_manager.as_ref(),
                                metrics.as_deref(),
                                DegradeRequest {
                                    app_id: &record.app_id,
                                    channel: &record.channel,
                                    reason: "durable_history_write_failed",
                                    node_id: None,
                                },
                            )
                            .await;
                        } else if let Some(metrics) = metrics.as_ref() {
                            metrics.mark_history_write(&record.app_id);
                            metrics.track_history_write_latency(
                                &record.app_id,
                                started.elapsed().as_secs_f64() * 1000.0,
                            );
                        }
                    }
                }
            });
            self.writers.push(WriterHandle { tx });
        }
    }

    async fn persist_record(
        pool: &PgPool,
        tables: &HistoryTables,
        record: &HistoryAppendRecord,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        Self::persist_records(pool, tables, std::slice::from_ref(record), metrics)
            .await
            .map(|_| ())
    }

    // False means an imported/duplicate serial overlaps the retained range.
    // Process those records individually so intermediate retention cannot change
    // which canonical duplicate survives.
    async fn persist_records(
        pool: &PgPool,
        tables: &HistoryTables,
        records: &[HistoryAppendRecord],
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<bool> {
        let first = &records[0];
        let record = records.last().expect("nonempty history batch");
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("Failed to begin history transaction: {e}")))?;

        // Lock the channel metadata before mutating entries. Counts and byte
        // totals then remain exact across writers and across server nodes.
        let state_sql = format!(
            "SELECT stream_id, retained_messages, retained_bytes, retention_initialized, newest_available_serial FROM {} WHERE app_id = $1 AND channel = $2 FOR UPDATE",
            tables.streams
        );
        let state = sqlx::query(sqlx::AssertSqlSafe(state_sql.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to lock history stream: {e}")))?;
        if state.get::<String, _>("stream_id") != record.stream_id {
            return Err(Error::InvalidMessageFormat(
                "History append stream changed".into(),
            ));
        }
        if records.len() > 1
            && (!state.get::<bool, _>("retention_initialized")
                || state
                    .get::<Option<i64>, _>("newest_available_serial")
                    .is_some_and(|newest| first.serial <= newest.max(0) as u64))
        {
            return Ok(false);
        }
        let mut retained_messages = state.get::<i64, _>("retained_messages").max(0) as u64;
        let mut retained_bytes = state.get::<i64, _>("retained_bytes").max(0) as u64;
        if !state.get::<bool, _>("retention_initialized") {
            let initialize = format!(
                "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes),0) AS BIGINT) AS bytes FROM {} WHERE app_id=$1 AND channel=$2",
                tables.entries
            );
            let actual = sqlx::query(sqlx::AssertSqlSafe(initialize.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to reconcile legacy history accounting: {e}"
                    ))
                })?;
            retained_messages = actual.get::<i64, _>("count") as u64;
            retained_bytes = actual.get::<i64, _>("bytes") as u64;
        }
        let insert_sql = format!(
            "INSERT INTO {} (app_id, channel, stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING",
            tables.entries
        );
        for record in records {
            let inserted = sqlx::query(sqlx::AssertSqlSafe(insert_sql.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .bind(&record.stream_id)
                .bind(record.serial as i64)
                .bind(record.published_at_ms)
                .bind(&record.message_id)
                .bind(&record.event_name)
                .bind(&record.operation_kind)
                .bind(record.payload_bytes.as_ref())
                .bind(record.payload_bytes.len() as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to insert history row: {e}")))?
                .rows_affected();
            retained_messages += inserted;
            retained_bytes += inserted * record.payload_bytes.len() as u64;
        }

        let cutoff_ms = record.published_at_ms.saturating_sub(
            record
                .retention
                .retention_window_seconds
                .saturating_mul(1000)
                .min(i64::MAX as u64) as i64,
        );
        let age_sql = format!(
            "WITH removed AS (DELETE FROM {0} WHERE app_id=$1 AND channel=$2 AND (stream_id, serial) IN (SELECT stream_id, serial FROM {0} WHERE app_id=$1 AND channel=$2 AND published_at_ms<$3 ORDER BY published_at_ms, serial LIMIT 256) RETURNING payload_size_bytes) SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes),0) AS BIGINT) AS bytes FROM removed",
            tables.entries
        );
        let mut evicted_messages = 0;
        let mut evicted_bytes = 0;
        loop {
            let age = sqlx::query(sqlx::AssertSqlSafe(age_sql.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .bind(cutoff_ms)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to evict aged history rows: {e}")))?;
            let count = age.get::<i64, _>("count") as u64;
            let bytes = age.get::<i64, _>("bytes") as u64;
            evicted_messages += count;
            evicted_bytes += bytes;
            retained_messages = retained_messages.saturating_sub(count);
            retained_bytes = retained_bytes.saturating_sub(bytes);
            if count < 256 {
                break;
            }
        }

        // Read only an evictable prefix, at most 256 metadata rows at a time.
        // No payloads or full retained-channel aggregates enter this loop.
        loop {
            let excess_count = record
                .retention
                .max_messages_per_channel
                .map_or(0, |limit| retained_messages.saturating_sub(limit as u64));
            let excess_bytes = record
                .retention
                .max_bytes_per_channel
                .map_or(0, |limit| retained_bytes.saturating_sub(limit));
            if excess_count == 0 && excess_bytes == 0 {
                break;
            }
            let select = format!(
                "SELECT stream_id, serial, payload_size_bytes FROM {} WHERE app_id=$1 AND channel=$2 ORDER BY serial ASC LIMIT 256",
                tables.entries
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(select.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to read history eviction prefix: {e}"))
                })?;
            if rows.is_empty() {
                break;
            }
            let mut count = 0u64;
            let mut bytes = 0u64;
            let mut streams = Vec::<String>::new();
            let mut serials = Vec::<i64>::new();
            for row in rows {
                if count >= excess_count && bytes >= excess_bytes {
                    break;
                }
                count += 1;
                bytes += row.get::<i64, _>("payload_size_bytes").max(0) as u64;
                streams.push(row.get("stream_id"));
                serials.push(row.get("serial"));
            }
            let delete = format!(
                "DELETE FROM {} WHERE app_id=$1 AND channel=$2 AND (stream_id, serial) IN (SELECT * FROM UNNEST($3::text[], $4::bigint[]))",
                tables.entries
            );
            sqlx::query(sqlx::AssertSqlSafe(delete.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .bind(&streams)
                .bind(&serials)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to evict history prefix: {e}")))?;
            retained_messages = retained_messages.saturating_sub(count);
            retained_bytes = retained_bytes.saturating_sub(bytes);
            evicted_messages += count;
            evicted_bytes += bytes;
        }
        // Each extremum is one index seek, including non-monotonic timestamps.
        let bounds = format!(
            "SELECT (SELECT serial FROM {0} WHERE app_id=$1 AND channel=$2 ORDER BY serial ASC LIMIT 1) AS oldest_serial, (SELECT serial FROM {0} WHERE app_id=$1 AND channel=$2 ORDER BY serial DESC LIMIT 1) AS newest_serial, (SELECT published_at_ms FROM {0} WHERE app_id=$1 AND channel=$2 ORDER BY published_at_ms ASC LIMIT 1) AS oldest_time, (SELECT published_at_ms FROM {0} WHERE app_id=$1 AND channel=$2 ORDER BY published_at_ms DESC LIMIT 1) AS newest_time",
            tables.entries
        );
        let bounds = sqlx::query(sqlx::AssertSqlSafe(bounds.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to seek history bounds: {e}")))?;
        let update = format!(
            "UPDATE {} SET retention_initialized=TRUE, retained_messages=$3, retained_bytes=$4, oldest_available_serial=$5, newest_available_serial=$6, oldest_available_published_at_ms=$7, newest_available_published_at_ms=$8, updated_at_ms=$9, next_serial=GREATEST(next_serial,$10) WHERE app_id=$1 AND channel=$2",
            tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(retained_messages as i64)
            .bind(retained_bytes as i64)
            .bind(bounds.get::<Option<i64>, _>("oldest_serial"))
            .bind(bounds.get::<Option<i64>, _>("newest_serial"))
            .bind(bounds.get::<Option<i64>, _>("oldest_time"))
            .bind(bounds.get::<Option<i64>, _>("newest_time"))
            .bind(record.published_at_ms)
            .bind(record.serial.saturating_add(1) as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to update history retention: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("Failed to commit history transaction: {e}")))?;

        if let Some(metrics) = metrics.as_ref() {
            metrics.update_history_retained(&record.app_id, retained_messages, retained_bytes);
            if evicted_messages > 0 || evicted_bytes > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
        }

        Ok(true)
    }

    pub(super) fn select_writer(&self, app_id: &str, channel: &str) -> &WriterHandle {
        let shard = if self.writers.len() == 1 {
            0
        } else {
            let next = self.next_writer.fetch_add(1, Ordering::Relaxed);
            ((ahash::random_state::RandomState::with_seeds(1, 2, 3, 4)
                .hash_one(format!("{app_id}\0{channel}")) as usize)
                .wrapping_add(next))
                % self.writers.len()
        };
        &self.writers[shard]
    }
}

fn compatible_append(previous: &HistoryAppendRecord, next: &HistoryAppendRecord) -> bool {
    previous.app_id == next.app_id
        && previous.channel == next.channel
        && previous.stream_id == next.stream_id
        && previous.serial < next.serial
        && previous.published_at_ms <= next.published_at_ms
        && previous.retention.retention_window_seconds == next.retention.retention_window_seconds
        && previous.retention.max_messages_per_channel == next.retention.max_messages_per_channel
        && previous.retention.max_bytes_per_channel == next.retention.max_bytes_per_channel
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::history::{HistoryRetentionPolicy, HistoryStore};
    use sockudo_core::options::{DatabaseConnection, DatabasePooling, HistoryConfig};
    use std::time::Duration;

    async fn fixture() -> PostgresHistoryStore {
        let mut config = HistoryConfig {
            enabled: true,
            writer_shards: 1,
            ..Default::default()
        };
        config.postgres.table_prefix = format!(
            "c6batch_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..10]
        );
        PostgresHistoryStore::new(
            &DatabaseConnection {
                host: "127.0.0.1".into(),
                port: 15432,
                username: "postgres".into(),
                password: "postgres123".into(),
                database: "sockudo_test".into(),
                ..Default::default()
            },
            &DatabasePooling::default(),
            config,
            None,
            None,
        )
        .await
        .unwrap()
    }

    fn record(stream: &str, channel: &str, serial: u64, now: i64) -> HistoryAppendRecord {
        HistoryAppendRecord {
            app_id: "batch-app".into(),
            channel: channel.into(),
            stream_id: stream.into(),
            serial,
            published_at_ms: now,
            message_id: None,
            event_name: None,
            operation_kind: "create".into(),
            payload_bytes: tokio_util::bytes::Bytes::from(vec![b'x'; 16]),
            retention: HistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_messages_per_channel: Some(3),
                max_bytes_per_channel: Some(64),
            },
        }
    }

    async fn rows(store: &PostgresHistoryStore, channel: &str) -> Vec<(i64, Vec<u8>)> {
        let sql = format!(
            "SELECT serial, payload_bytes FROM {} WHERE app_id='batch-app' AND channel=$1 ORDER BY serial",
            store.tables.entries
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(channel)
            .fetch_all(&store.pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get("serial"), row.get("payload_bytes")))
            .collect()
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL at 15432; verifies actual queue batching and overlap fallback"]
    async fn c6_postgres_batch_preserves_retention_and_import_canonical_outcome() {
        let store = fixture().await;
        let stream = store
            .reserve_publish_position("batch-app", "batch")
            .await
            .unwrap()
            .stream_id;
        let now = sockudo_core::history::now_ms();
        PostgresHistoryStore::persist_record(
            &store.pool,
            &store.tables,
            &record(&stream, "batch", 0, now),
            None,
        )
        .await
        .unwrap();
        for serial in 1..=96 {
            store
                .append(record(&stream, "batch", serial, now + serial as i64))
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if rows(&store, "batch")
                .await
                .iter()
                .map(|row| row.0)
                .collect::<Vec<_>>()
                == vec![94, 95, 96]
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let head = store.channel_head("batch-app", "batch").await.unwrap();
        assert_eq!((head.retained_messages, head.retained_bytes), (3, 48));

        let stream = store
            .reserve_publish_position("batch-app", "imports")
            .await
            .unwrap()
            .stream_id;
        PostgresHistoryStore::persist_record(
            &store.pool,
            &store.tables,
            &record(&stream, "imports", 150, now - 2000),
            None,
        )
        .await
        .unwrap();
        let mut first = record(&stream, "imports", 100, now);
        first.retention.retention_window_seconds = 1;
        first.retention.max_messages_per_channel = Some(1);
        let mut second = first.clone();
        second.serial = 150;
        second.payload_bytes = tokio_util::bytes::Bytes::from_static(b"replacement");
        store.append(first).await.unwrap();
        store.append(second).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if rows(&store, "imports").await == vec![(150, b"replacement".to_vec())] {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "import overlap changed the canonical survivor"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL at 15432; injects one row failure in a private fixture table"]
    async fn c6_postgres_failed_batch_retries_individual_records_and_marks_degraded() {
        let store = fixture().await;
        let stream = store
            .reserve_publish_position("batch-app", "failure")
            .await
            .unwrap()
            .stream_id;
        let now = sockudo_core::history::now_ms();
        let mut seed = record(&stream, "failure", 0, now);
        seed.retention.max_messages_per_channel = None;
        seed.retention.max_bytes_per_channel = None;
        PostgresHistoryStore::persist_record(&store.pool, &store.tables, &seed, None)
            .await
            .unwrap();
        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT fixture_reject_three CHECK(serial <> 3)",
            store.tables.entries
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&store.pool)
            .await
            .unwrap();
        for serial in 1..=5 {
            let mut next = seed.clone();
            next.serial = serial;
            store.append(next).await.unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if rows(&store, "failure")
                .await
                .iter()
                .map(|row| row.0)
                .collect::<Vec<_>>()
                == vec![0, 1, 2, 4, 5]
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "good records were lost with the failed batch"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            store
                .channel_head("batch-app", "failure")
                .await
                .unwrap()
                .retained_messages,
            5
        );
        assert!(
            !store
                .resolved_stream_runtime_state("batch-app", "failure")
                .await
                .unwrap()
                .recovery_allowed
        );
    }
}
