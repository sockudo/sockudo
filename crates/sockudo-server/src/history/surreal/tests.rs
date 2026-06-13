use super::*;
use sockudo_core::history_conformance::HistoryStoreConformance;

async fn is_surreal_available() -> bool {
    let db = connect("ws://127.0.0.1:18001").await;
    let Ok(db) = db else {
        return false;
    };
    if db
        .signin(Root {
            username: "root".to_string(),
            password: "root".to_string(),
        })
        .await
        .is_err()
    {
        return false;
    }
    db.health().await.is_ok()
}

async fn build_store() -> Arc<dyn HistoryStore + Send + Sync> {
    let settings = SurrealDbSettings {
        url: "ws://127.0.0.1:18001".to_string(),
        namespace: format!("sockudo_history_test_{}", uuid::Uuid::new_v4().simple()),
        database: "sockudo".to_string(),
        username: "root".to_string(),
        password: "root".to_string(),
        table_name: "applications".to_string(),
        cache_ttl: 300,
        cache_max_capacity: 100,
    };
    let config = HistoryConfig {
        enabled: true,
        backend: sockudo_core::options::HistoryBackend::SurrealDb,
        surrealdb: sockudo_core::options::SurrealDbHistoryConfig {
            table_prefix: format!("sockudo_history_{}", uuid::Uuid::new_v4().simple()),
            ..sockudo_core::options::SurrealDbHistoryConfig::default()
        },
        ..HistoryConfig::default()
    };
    create_surreal_history_store(&settings, config, None, None)
        .await
        .unwrap()
}

async fn seed_time_series(
    store: &Arc<dyn HistoryStore + Send + Sync>,
) -> sockudo_core::history::HistoryWriteReservation {
    let reservation = store.reserve_publish_position("app", "chat").await.unwrap();
    for offset in 0..5u64 {
        store
            .append(sockudo_core::history::HistoryAppendRecord {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                stream_id: reservation.stream_id.clone(),
                serial: reservation.serial + offset,
                published_at_ms: 1_000 + offset as i64,
                message_id: Some(format!("msg-{}", reservation.serial + offset)),
                event_name: Some("event".to_string()),
                operation_kind: "append".to_string(),
                payload_bytes: tokio_util::bytes::Bytes::from(format!(
                    "payload-{}",
                    reservation.serial + offset
                )),
                retention: sockudo_core::history::HistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_messages_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }
    reservation
}

#[tokio::test]
async fn surreal_history_store_conformance_serial_and_stream_continuity() {
    if !is_surreal_available().await {
        eprintln!("Skipping test: SurrealDB not available");
        return;
    }
    let store = build_store().await;
    HistoryStoreConformance::assert_serial_monotonicity(store.clone())
        .await
        .unwrap();
    HistoryStoreConformance::assert_stream_id_continuity(store)
        .await
        .unwrap();
}

#[tokio::test]
async fn surreal_history_store_conformance_pagination_and_reset_semantics() {
    if !is_surreal_available().await {
        eprintln!("Skipping test: SurrealDB not available");
        return;
    }
    let store = build_store().await;
    HistoryStoreConformance::assert_cursor_pagination(store.clone())
        .await
        .unwrap();
    HistoryStoreConformance::assert_purge_and_reset_semantics(store)
        .await
        .unwrap();
}

#[tokio::test]
async fn surreal_history_store_time_bounded_pagination_uses_time_ordering() {
    if !is_surreal_available().await {
        eprintln!("Skipping test: SurrealDB not available");
        return;
    }
    let store = build_store().await;
    let reservation = seed_time_series(&store).await;

    let first = store
        .read_page(sockudo_core::history::HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: sockudo_core::history::HistoryDirection::OldestFirst,
            limit: 2,
            cursor: None,
            bounds: sockudo_core::history::HistoryQueryBounds {
                start_serial: None,
                end_serial: None,
                start_time_ms: Some(1_001),
                end_time_ms: Some(1_004),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![reservation.serial + 1, reservation.serial + 2]
    );
    assert!(first.has_more);

    let second = store
        .read_page(sockudo_core::history::HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            direction: sockudo_core::history::HistoryDirection::OldestFirst,
            limit: 2,
            cursor: first.next_cursor,
            bounds: sockudo_core::history::HistoryQueryBounds {
                start_serial: None,
                end_serial: None,
                start_time_ms: Some(1_001),
                end_time_ms: Some(1_004),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![reservation.serial + 3, reservation.serial + 4]
    );
    assert!(!second.has_more);
}
