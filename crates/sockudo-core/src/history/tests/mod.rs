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

#[test]
fn history_cursor_rejects_oversized_input_before_decoding() {
    let error = HistoryCursor::decode(&"A".repeat(16 * 1024 + 1)).unwrap_err();
    assert!(error.to_string().contains("exceeds 16 KiB"));
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

#[tokio::test]
async fn history_pages_preserve_scan_semantics_for_imports_and_clock_skew() {
    // Cover ordered gaps, duplicates, and imports whose arrival order differs
    // from canonical serial order. Time bounds cannot assume sorted clocks.
    for serials in [
        vec![1, 3, 8, 12, 20],
        vec![1, 8, 3, 20, 12],
        vec![1, 3, 3, 8, 20],
    ] {
        let store = MemoryHistoryStore::default();
        let stream = store
            .reserve_publish_position("app", "chat")
            .await
            .unwrap()
            .stream_id;
        let now = now_ms();
        let times = [now + 5, now + 1, now + 4, now + 2, now + 3];
        for (&serial, &time) in serials.iter().zip(&times) {
            store
                .append(make_record("app", "chat", &stream, serial, time, "payload"))
                .await
                .unwrap();
        }
        for direction in [HistoryDirection::OldestFirst, HistoryDirection::NewestFirst] {
            for cursor_serial in [None, Some(1), Some(3), Some(10), Some(20), Some(u64::MAX)] {
                for time_bound in [None, Some(now + 3)] {
                    let bounds = HistoryQueryBounds {
                        start_serial: Some(3),
                        end_serial: Some(20),
                        start_time_ms: time_bound,
                        end_time_ms: None,
                    };
                    let cursor = cursor_serial.map(|serial| HistoryCursor {
                        version: 1,
                        app_id: "app".into(),
                        channel: "chat".into(),
                        stream_id: stream.clone(),
                        serial,
                        direction,
                        bounds: bounds.clone(),
                    });
                    let page = store
                        .read_page(HistoryReadRequest {
                            app_id: "app".into(),
                            channel: "chat".into(),
                            direction,
                            limit: 2,
                            cursor,
                            bounds,
                        })
                        .await
                        .unwrap();
                    let mut expected: Vec<_> = serials
                        .iter()
                        .copied()
                        .zip(times)
                        .filter(|(serial, time)| {
                            *serial >= 3
                                && *serial <= 20
                                && time_bound.is_none_or(|start| *time >= start)
                                && cursor_serial.is_none_or(|cursor| match direction {
                                    HistoryDirection::OldestFirst => *serial > cursor,
                                    HistoryDirection::NewestFirst => *serial < cursor,
                                })
                        })
                        .collect();
                    if direction == HistoryDirection::NewestFirst {
                        expected.reverse();
                    }
                    assert_eq!(page.has_more, expected.len() > 2);
                    expected.truncate(2);
                    assert_eq!(
                        page.items
                            .iter()
                            .map(|item| (item.serial, item.published_at_ms))
                            .collect::<Vec<_>>(),
                        expected
                    );
                    for item in &page.items {
                        assert_eq!(item.payload_bytes.as_ref(), b"payload");
                    }
                    assert_eq!(
                        page.next_cursor.as_ref().map(|cursor| cursor.serial),
                        page.has_more.then(|| page.items.last().unwrap().serial)
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn history_blocked_channel_does_not_block_other_channel_reservations() {
    let store = MemoryHistoryStore::default();
    store.reserve_publish_position("app", "busy").await.unwrap();
    store
        .reserve_publish_position("app", "healthy")
        .await
        .unwrap();
    let busy = store
        .channels
        .read()
        .await
        .get("app\0busy")
        .unwrap()
        .clone();
    let _guard = busy.write().await;
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        store.reserve_publish_position("app", "healthy"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.serial, 2);
}

#[tokio::test]
async fn idle_history_maintenance_is_bounded_and_preserves_stream_identity() {
    let store = MemoryHistoryStore::default();
    let at = now_ms();
    let mut streams = Vec::new();
    for index in 0..12 {
        let channel = format!("idle-{index}");
        let stream = store
            .reserve_publish_position("app", &channel)
            .await
            .unwrap()
            .stream_id;
        let mut record = make_record("app", &channel, &stream, 1, at, "retained");
        record.retention.retention_window_seconds = 0;
        store.append(record).await.unwrap();
        streams.push((channel, stream));
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mut deleted = 0;
    loop {
        let (batch, more) = store.purge_before(now_ms(), 3).await.unwrap();
        assert!(batch <= 3);
        deleted += batch;
        if !more {
            break;
        }
    }
    // A record may expire during setup, but no expired payload remains even
    // though no channel read was used to trigger reclamation.
    assert!(deleted <= 12);
    assert!(!store.purge_before(now_ms(), 3).await.unwrap().1);
    for (channel, stream) in streams {
        let next = store
            .reserve_publish_position("app", &channel)
            .await
            .unwrap();
        assert_eq!(next.stream_id, stream);
        assert_eq!(next.serial, 2);
        assert_eq!(
            store
                .channel_head("app", &channel)
                .await
                .unwrap()
                .retained_messages,
            0
        );
    }
}

#[tokio::test]
async fn idle_maintenance_does_not_shorten_channel_retention_override() {
    let store = MemoryHistoryStore::default();
    let stream = store
        .reserve_publish_position("app", "long")
        .await
        .unwrap()
        .stream_id;
    let record = make_record("app", "long", &stream, 1, now_ms() - 100, "retained");
    store.append(record).await.unwrap();
    assert_eq!(store.purge_before(i64::MAX, 100).await.unwrap(), (0, false));
    assert_eq!(
        store
            .channel_head("app", "long")
            .await
            .unwrap()
            .retained_messages,
        1
    );
}

#[tokio::test]
async fn maintenance_releases_idle_payload_owners_before_any_channel_read() {
    let store = MemoryHistoryStore::default();
    let mut owners = Vec::new();
    for index in 0..32 {
        let channel = format!("payload-owner-{index}");
        let position = store
            .reserve_publish_position("app", &channel)
            .await
            .unwrap();
        let payload: std::sync::Arc<[u8]> = vec![b'x'; 8192].into();
        owners.push(std::sync::Arc::downgrade(&payload));
        let mut record = make_record(
            "app",
            &channel,
            &position.stream_id,
            position.serial,
            now_ms(),
            "",
        );
        record.payload_bytes = bytes::Bytes::from_owner(payload);
        record.retention.retention_window_seconds = 1;
        store.append(record).await.unwrap();
    }
    assert!(owners.iter().all(|owner| owner.upgrade().is_some()));
    tokio::time::sleep(Duration::from_millis(1010)).await;
    let mut deleted = 0;
    for _ in 0..16 {
        let (count, more) = store.purge_before(now_ms(), 4).await.unwrap();
        assert!(count <= 4);
        deleted += count;
        if !more {
            break;
        }
    }
    assert_eq!(deleted, 32);
    assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
}
