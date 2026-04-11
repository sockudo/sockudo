use crate::error::Result;
use crate::history::{
    HistoryAppendRecord, HistoryDirection, HistoryPurgeMode, HistoryPurgeRequest,
    HistoryQueryBounds, HistoryReadRequest, HistoryRetentionPolicy, HistoryStore,
};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

fn make_record(
    app_id: &str,
    channel: &str,
    stream_id: &str,
    serial: u64,
    published_at_ms: i64,
    payload: &str,
) -> HistoryAppendRecord {
    HistoryAppendRecord {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        stream_id: stream_id.to_string(),
        serial,
        published_at_ms,
        message_id: Some(format!("msg-{serial}")),
        event_name: Some("event".to_string()),
        operation_kind: "append".to_string(),
        payload_bytes: Bytes::from(payload.to_string()),
        retention: HistoryRetentionPolicy {
            retention_window_seconds: 3600,
            max_messages_per_channel: None,
            max_bytes_per_channel: None,
        },
    }
}

/// Reusable conformance checks for future durable HistoryStore backends.
///
/// This is intentionally backend-agnostic and should be used by every non-memory
/// durable backend implementation before being considered production-ready.
pub struct HistoryStoreConformance;

impl HistoryStoreConformance {
    pub async fn assert_serial_monotonicity(
        store: Arc<dyn HistoryStore + Send + Sync>,
    ) -> Result<()> {
        let first = store.reserve_publish_position("app", "chat").await?;
        let second = store.reserve_publish_position("app", "chat").await?;
        assert_eq!(first.serial + 1, second.serial);
        assert_eq!(first.stream_id, second.stream_id);
        Ok(())
    }

    pub async fn assert_stream_id_continuity(
        store: Arc<dyn HistoryStore + Send + Sync>,
    ) -> Result<()> {
        let reservation = store.reserve_publish_position("app", "chat").await?;
        store
            .append(make_record(
                "app",
                "chat",
                &reservation.stream_id,
                reservation.serial,
                crate::history::now_ms(),
                "payload-1",
            ))
            .await?;
        let inspection = store.stream_inspection("app", "chat").await?;
        assert_eq!(
            inspection.stream_id.as_deref(),
            Some(reservation.stream_id.as_str())
        );
        assert_eq!(inspection.next_serial, Some(reservation.serial + 1));
        Ok(())
    }

    pub async fn assert_cursor_pagination(
        store: Arc<dyn HistoryStore + Send + Sync>,
    ) -> Result<()> {
        let reservation = store.reserve_publish_position("app", "chat").await?;
        for serial in reservation.serial..reservation.serial + 3 {
            store
                .append(make_record(
                    "app",
                    "chat",
                    &reservation.stream_id,
                    serial,
                    crate::history::now_ms() + serial as i64,
                    &format!("payload-{serial}"),
                ))
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let inspection = store.stream_inspection("app", "chat").await?;
        assert_eq!(inspection.next_serial, Some(reservation.serial + 3));

        let first = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 2,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            })
            .await?;
        assert_eq!(first.items.len(), 2);
        assert!(first.has_more);

        let second = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 2,
                cursor: first.next_cursor,
                bounds: HistoryQueryBounds::default(),
            })
            .await?;
        assert_eq!(second.items.len(), 1);
        assert!(!second.has_more);
        Ok(())
    }

    pub async fn assert_purge_and_reset_semantics(
        store: Arc<dyn HistoryStore + Send + Sync>,
    ) -> Result<()> {
        let reservation = store.reserve_publish_position("app", "chat").await?;
        for serial in reservation.serial..reservation.serial + 3 {
            store
                .append(make_record(
                    "app",
                    "chat",
                    &reservation.stream_id,
                    serial,
                    crate::history::now_ms() + serial as i64,
                    &format!("payload-{serial}"),
                ))
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let purge = store
            .purge_stream(
                "app",
                "chat",
                HistoryPurgeRequest {
                    mode: HistoryPurgeMode::BeforeSerial,
                    before_serial: Some(reservation.serial + 2),
                    before_time_ms: None,
                    reason: "conformance".to_string(),
                    requested_by: Some("test".to_string()),
                },
            )
            .await?;
        assert_eq!(
            purge.inspection.stream_id.as_deref(),
            Some(reservation.stream_id.as_str())
        );
        assert_eq!(
            purge.inspection.retained.oldest_serial,
            Some(reservation.serial + 2)
        );

        let reset = store
            .reset_stream("app", "chat", "conformance", Some("test"))
            .await?;
        assert_ne!(
            reset.previous_stream_id.as_deref(),
            Some(reset.new_stream_id.as_str())
        );
        assert_eq!(reset.inspection.retained.retained_messages, 0);
        assert_eq!(reset.inspection.next_serial, Some(1));
        Ok(())
    }
}
