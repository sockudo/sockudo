use super::*;
use sockudo_core::message_envelope::PublishIdempotencyMetadata;
use sockudo_core::version_store::{
    VersionCreateLimits, VersionMutation, VersionMutationLimits, VersionPrecondition,
};
use sockudo_core::versioned_messages::{
    MessageAppend, MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
};
use sockudo_protocol::messages::MessageData;

fn version(number: u64) -> VersionMetadata {
    VersionMetadata {
        serial: VersionSerial::new(format!("ver:{number:020}")).unwrap(),
        client_id: Some("alice".into()),
        timestamp_ms: sockudo_core::history::now_ms(),
        description: None,
        metadata: None,
    }
}

#[tokio::test]
#[ignore = "requires the local audit PostgreSQL fixture on port 15432"]
async fn postgres_compact_versions_counters_imports_and_retention() {
    let config = DatabaseConnection {
        host: "127.0.0.1".into(),
        port: 15432,
        username: "postgres".into(),
        password: "postgres123".into(),
        database: "sockudo_test".into(),
        connection_pool_size: 1,
        ..Default::default()
    };
    let prefix = format!("perf_v{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let store = PostgresVersionStore::new(
        &config,
        &DatabasePooling {
            enabled: false,
            ..Default::default()
        },
        &prefix,
    )
    .await
    .unwrap();
    let record = StoredVersionRecord {
        app_id: "audit".into(),
        channel: "room".into(),
        original_client_id: Some("alice".into()),
        envelope: None,
        message: VersionedMessage::new_create(
            MessageSerial::new("msg:one").unwrap(),
            version(10),
            1,
            1,
            Some("text".into()),
            Some(MessageData::String("start".into())),
            None,
        ),
    };
    let VersionCreateResult::Applied { mut record, .. } = store
        .commit_create(VersionCreateRequest {
            record,
            limits: VersionCreateLimits::default(),
        })
        .await
        .unwrap()
    else {
        panic!("create was not applied")
    };
    let mut expected = vec![record.clone()];
    let mut last_request = None;
    for number in 2..=129 {
        let request = VersionMutationRequest {
            app_id: "audit".into(),
            channel: "room".into(),
            message_serial: record.message_serial().clone(),
            expected: VersionPrecondition::from_record(&record),
            version: version(number * 10),
            mutation: VersionMutation::Append(MessageAppend {
                data_fragment: "世界abcdef".repeat(8),
                extras: None,
            }),
            idempotency: Some(PublishIdempotencyMetadata {
                cache_key: format!("operation-{number}"),
                payload_fingerprint: format!("fingerprint-{number}"),
            }),
            limits: VersionMutationLimits {
                max_appends_per_message: Some(200),
                ..Default::default()
            },
        };
        let VersionMutationResult::Applied { record: next, .. } =
            store.compare_and_apply(request.clone()).await.unwrap()
        else {
            panic!("append was not applied")
        };
        record = next;
        expected.push(record.clone());
        last_request = Some(request);
    }
    let VersionMutationResult::Duplicate {
        record: duplicate, ..
    } = store
        .compare_and_apply(last_request.unwrap())
        .await
        .unwrap()
    else {
        panic!("receipt was not replayed")
    };
    assert_eq!(
        sonic_rs::to_vec(&duplicate).unwrap(),
        sonic_rs::to_vec(&record).unwrap()
    );
    // Each bounded page reconstructs its exact historical prefix and envelope,
    // including UTF-8, original fragment, metadata and all four identities.
    for index in [1, 13, 97, 128] {
        let page = store
            .get_versions(VersionStoreReadRequest {
                app_id: "audit".into(),
                channel: "room".into(),
                message_serial: record.message_serial().clone(),
                direction: VersionStoreDirection::OldestFirst,
                limit: 1,
                cursor: Some(VersionStoreCursor {
                    version: 1,
                    version_serial: expected[index - 1].version_serial().clone(),
                    direction: VersionStoreDirection::OldestFirst,
                }),
            })
            .await
            .unwrap();
        assert_eq!(
            sonic_rs::to_vec(&page.items[0]).unwrap(),
            sonic_rs::to_vec(&expected[index]).unwrap()
        );
    }
    assert_eq!(store.message_count("audit", "room").await.unwrap(), 1);
    let batch = store
        .get_latest_batch(
            "audit",
            "room",
            &[
                record.message_serial().clone(),
                record.message_serial().clone(),
                MessageSerial::new("missing").unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(
        sonic_rs::to_vec(batch.values().next().unwrap()).unwrap(),
        sonic_rs::to_vec(&record).unwrap()
    );
    let count_sql = format!("SELECT append_count FROM {}", store.tables.version_messages);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql.as_str()))
            .fetch_one(&store.pool)
            .await
            .unwrap(),
        128
    );
    let size_sql = format!(
        "SELECT SUM(payload_size_bytes)::bigint FROM {}",
        store.tables.version_entries
    );
    let stored: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(size_sql.as_str()))
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let full: usize = expected
        .iter()
        .map(|value| sonic_rs::to_vec(value).unwrap().len())
        .sum();
    assert!(stored as usize * 2 < full, "compact={stored}, full={full}");
    let snapshots_sql = format!("SELECT COUNT(*) FROM {}", store.text_table());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(snapshots_sql.as_str()))
            .fetch_one(&store.pool)
            .await
            .unwrap(),
        1
    );

    // Imports and replay from an old writer still maintain the counter; a
    // smaller imported version must not become the latest visible state.
    let mut imported = record.clone();
    imported.message.version.serial = VersionSerial::new("ver:00000000000000000015").unwrap();
    imported.message.replay_position.delivery_serial = 9000;
    store.append_version(imported.clone()).await.unwrap();
    store.append_version(imported).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql.as_str()))
            .fetch_one(&store.pool)
            .await
            .unwrap(),
        129
    );
    assert_eq!(
        store
            .get_latest("audit", "room", record.message_serial())
            .await
            .unwrap()
            .unwrap()
            .version_serial(),
        record.version_serial()
    );
    let age_sql = format!(
        "UPDATE {} SET created_at_ms = 1 WHERE delivery_serial <= 64",
        store.tables.version_entries
    );
    sqlx::query(sqlx::AssertSqlSafe(age_sql.as_str()))
        .execute(&store.pool)
        .await
        .unwrap();
    for _ in 0..4 {
        store.purge_before(2, 16).await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql.as_str()))
            .fetch_one(&store.pool)
            .await
            .unwrap(),
        66
    );
    assert_eq!(
        sonic_rs::to_vec(
            &store
                .get_latest("audit", "room", record.message_serial())
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        sonic_rs::to_vec(&record).unwrap()
    );
    let cutoff = sockudo_core::history::now_ms() + 1;
    while store.purge_before(cutoff, 32).await.unwrap().1 {}
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(snapshots_sql.as_str()))
            .fetch_one(&store.pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
#[ignore = "requires isolated local audit database fixture"]
async fn postgres_metadata_counts_preserve_legacy_and_latest_states() {
    let config = DatabaseConnection {
        host: "127.0.0.1".into(),
        port: 15432,
        username: "postgres".into(),
        password: "postgres123".into(),
        database: "sockudo_test".into(),
        connection_pool_size: 1,
        ..Default::default()
    };
    let prefix = format!("perf_v{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let store = PostgresVersionStore::new(
        &config,
        &DatabasePooling {
            enabled: false,
            ..Default::default()
        },
        &prefix,
    )
    .await
    .unwrap();
    crate::history::version_storage_tests::metadata_counts(&store).await;
    // Simulate legacy imported rows whose boolean was not updated with latest.
    let invalidate = format!(
        "UPDATE {} SET state_version_serial = NULL, is_open_stream = FALSE WHERE app_id = 'audit' AND channel = 'metadata'",
        store.tables.version_messages
    );
    sqlx::query(sqlx::AssertSqlSafe(invalidate.as_str()))
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .active_stream_count("audit", "metadata")
            .await
            .unwrap(),
        68
    );
    assert_eq!(
        store
            .active_stream_count("audit", "metadata")
            .await
            .unwrap(),
        68
    );
}
