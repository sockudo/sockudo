use super::*;
use bytes::Bytes;
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

#[test]
fn history_cursor_round_trip() {
    let cursor = HistoryCursor {
        version: 1,
        app_id: "app".to_string(),
        channel: "chat".to_string(),
        stream_id: "stream-1".to_string(),
        serial: 42,
        direction: HistoryDirection::NewestFirst,
        bounds: HistoryQueryBounds::default(),
    };
    let encoded = cursor.encode().unwrap();
    let decoded = HistoryCursor::decode(&encoded).unwrap();
    assert_eq!(decoded, cursor);
}

#[tokio::test]
async fn memory_history_store_orders_newest_first_with_cursor() {
    let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());
    let reservation = store.reserve_publish_position("app", "chat").await.unwrap();
    assert_eq!(reservation.serial, 1);
    let stream_id = reservation.stream_id;
    let base_ts = now_ms();

    for serial in 1..=3 {
        store
            .append(make_record(
                "app",
                "chat",
                &stream_id,
                serial,
                base_ts + serial as i64,
                &format!("payload-{serial}"),
            ))
            .await
            .unwrap();
    }

    let first_page = store
        .read_page(HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: HistoryDirection::NewestFirst,
            limit: 2,
            cursor: None,
            bounds: HistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(
        first_page
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );

    let second_page = store
        .read_page(HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: HistoryDirection::NewestFirst,
            limit: 2,
            cursor: first_page.next_cursor.clone(),
            bounds: HistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(
        second_page
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[tokio::test]
async fn memory_history_store_orders_oldest_first_with_cursor() {
    let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());
    let stream_id = store
        .reserve_publish_position("app", "chat")
        .await
        .unwrap()
        .stream_id;
    let base_ts = now_ms();

    for serial in 1..=3 {
        store
            .append(make_record(
                "app",
                "chat",
                &stream_id,
                serial,
                base_ts + serial as i64,
                &format!("payload-{serial}"),
            ))
            .await
            .unwrap();
    }

    let first_page = store
        .read_page(HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: HistoryDirection::OldestFirst,
            limit: 2,
            cursor: None,
            bounds: HistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(
        first_page
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let second_page = store
        .read_page(HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: HistoryDirection::OldestFirst,
            limit: 2,
            cursor: first_page.next_cursor.clone(),
            bounds: HistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(
        second_page
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![3]
    );
}

#[tokio::test]
async fn memory_history_store_evicts_by_retention_and_count() {
    let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig {
        retention_window: Duration::from_secs(1),
        max_messages_per_channel: Some(2),
        max_bytes_per_channel: None,
    });
    let stream_id = store
        .reserve_publish_position("app", "chat")
        .await
        .unwrap()
        .stream_id;

    let old_ts = now_ms() - 5_000;
    store
        .append(HistoryAppendRecord {
            retention: HistoryRetentionPolicy {
                retention_window_seconds: 1,
                max_messages_per_channel: Some(2),
                max_bytes_per_channel: None,
            },
            ..make_record("app", "chat", &stream_id, 1, old_ts, "old")
        })
        .await
        .unwrap();
    store
        .append(HistoryAppendRecord {
            retention: HistoryRetentionPolicy {
                retention_window_seconds: 1,
                max_messages_per_channel: Some(2),
                max_bytes_per_channel: None,
            },
            ..make_record("app", "chat", &stream_id, 2, now_ms(), "newer")
        })
        .await
        .unwrap();
    store
        .append(HistoryAppendRecord {
            retention: HistoryRetentionPolicy {
                retention_window_seconds: 1,
                max_messages_per_channel: Some(2),
                max_bytes_per_channel: None,
            },
            ..make_record("app", "chat", &stream_id, 3, now_ms(), "newest")
        })
        .await
        .unwrap();

    let page = store
        .read_page(HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: HistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: HistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(
        page.items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(page.retained.retained_messages, 2);
}

#[tokio::test]
async fn memory_history_store_filters_by_serial_and_time() {
    let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());
    let stream_id = store
        .reserve_publish_position("app", "chat")
        .await
        .unwrap()
        .stream_id;
    let base_ts = now_ms();

    for serial in 1..=5 {
        store
            .append(make_record(
                "app",
                "chat",
                &stream_id,
                serial,
                base_ts + (serial as i64 * 10),
                &format!("payload-{serial}"),
            ))
            .await
            .unwrap();
    }

    let page = store
        .read_page(HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: HistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: HistoryQueryBounds {
                start_serial: Some(2),
                end_serial: Some(4),
                start_time_ms: Some(base_ts + 20),
                end_time_ms: Some(base_ts + 40),
            },
        })
        .await
        .unwrap();

    assert_eq!(
        page.items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
}

#[tokio::test]
async fn memory_history_read_page_does_not_materialize_absent_channels() {
    let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());

    let page = store
        .read_page(HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "missing".to_string(),
            direction: HistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: HistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert!(page.items.is_empty());
    assert_eq!(store.channels.read().await.len(), 0);
}

#[tokio::test]
async fn memory_history_channel_head_does_not_materialize_absent_channels() {
    let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());

    let head = store.channel_head("app", "missing").await.unwrap();

    assert_eq!(head.retained_messages, 0);
    assert_eq!(head.newest_serial, None);
    assert_eq!(store.channels.read().await.len(), 0);
}
