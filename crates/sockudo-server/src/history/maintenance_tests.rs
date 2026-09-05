use super::*;
use sockudo_core::history::*;
use sockudo_core::options::*;
use std::time::Duration;

async fn wait_retained(
    store: &Arc<dyn HistoryStore + Send + Sync>,
    channel: &str,
    count: u64,
    bytes: u64,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let head = store.channel_head("c6-app", channel).await.unwrap();
        if head.retained_messages == count && head.retained_bytes == bytes {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected count={count} bytes={bytes}, got {head:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn record(
    stream: &str,
    channel: &str,
    serial: u64,
    now: i64,
    retention: HistoryRetentionPolicy,
) -> HistoryAppendRecord {
    HistoryAppendRecord {
        app_id: "c6-app".into(),
        channel: channel.into(),
        stream_id: stream.into(),
        serial,
        published_at_ms: now + serial as i64,
        message_id: Some(format!("message-{serial}")),
        event_name: Some("event".into()),
        operation_kind: "create".into(),
        payload_bytes: tokio_util::bytes::Bytes::from(vec![b'x'; 16]),
        retention,
    }
}
async fn assert_accounting(
    first: Arc<dyn HistoryStore + Send + Sync>,
    second: Arc<dyn HistoryStore + Send + Sync>,
) {
    let now = sockudo_core::history::now_ms();
    let policy = HistoryRetentionPolicy {
        retention_window_seconds: 3600,
        max_messages_per_channel: None,
        max_bytes_per_channel: None,
    };
    let stream = first
        .reserve_publish_position("c6-app", "retention")
        .await
        .unwrap()
        .stream_id;
    for serial in 1..=40 {
        let store = if serial % 2 == 0 { &first } else { &second };
        store
            .append(record(&stream, "retention", serial, now, policy.clone()))
            .await
            .unwrap();
    }
    wait_retained(&first, "retention", 40, 640).await;
    second
        .append(record(&stream, "retention", 40, now, policy.clone()))
        .await
        .unwrap();
    wait_retained(&second, "retention", 40, 640).await;
    let capped = HistoryRetentionPolicy {
        max_messages_per_channel: Some(10),
        max_bytes_per_channel: Some(100),
        ..policy.clone()
    };
    first
        .append(record(&stream, "retention", 41, now, capped.clone()))
        .await
        .unwrap();
    wait_retained(&first, "retention", 6, 96).await;
    let head = second.channel_head("c6-app", "retention").await.unwrap();
    assert_eq!(head.oldest_serial, Some(36));
    assert_eq!(head.newest_serial, Some(41));
    second
        .append(record(&stream, "retention", 41, now, capped))
        .await
        .unwrap();
    wait_retained(&first, "retention", 6, 96).await;
    let purged = first
        .purge_stream(
            "c6-app",
            "retention",
            HistoryPurgeRequest {
                mode: HistoryPurgeMode::BeforeSerial,
                before_serial: Some(40),
                before_time_ms: None,
                reason: "test".into(),
                requested_by: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(purged.purged_messages, 4);
    wait_retained(&first, "retention", 2, 32).await;
    let reset = first
        .reset_stream("c6-app", "retention", "test", None)
        .await
        .unwrap();
    assert_ne!(reset.new_stream_id, stream);
    first
        .append(record(
            &reset.new_stream_id,
            "retention",
            1,
            now,
            policy.clone(),
        ))
        .await
        .unwrap();
    wait_retained(&first, "retention", 1, 16).await;
    let parallel = first
        .reserve_publish_position("c6-app", "parallel")
        .await
        .unwrap()
        .stream_id;
    use futures_util::StreamExt;
    futures_util::stream::iter(1..=64)
        .map(|serial| {
            let store = if serial % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            let record = record(&parallel, "parallel", serial, now, policy.clone());
            async move {
                store.append(record).await.unwrap();
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
    wait_retained(&first, "parallel", 64, 1024).await;
    let floor = second
        .reserve_publish_position("c6-app", "parallel")
        .await
        .unwrap();
    assert!(floor.serial >= 65);
    // The expiry janitor shares accounting with append. Dynamo/Scylla may use
    // the trait's unsupported no-op; a supported janitor must subtract exactly.
    let before = first.channel_head("c6-app", "parallel").await.unwrap();
    let (removed, _) = first.purge_before(now + 1000, 7).await.unwrap();
    if removed > 0 {
        assert!(removed <= 7);
        let after = second.channel_head("c6-app", "parallel").await.unwrap();
        let remaining_retention = second
            .channel_head("c6-app", "retention")
            .await
            .unwrap()
            .retained_messages;
        assert_eq!(
            after.retained_messages + remaining_retention,
            before.retained_messages + 1 - removed
        );
        assert_eq!(after.retained_bytes, after.retained_messages * 16);
    }
}
async fn assert_large_retention_batches(store: Arc<dyn HistoryStore + Send + Sync>) {
    let now = sockudo_core::history::now_ms();
    let uncapped = HistoryRetentionPolicy {
        retention_window_seconds: 3600,
        max_messages_per_channel: None,
        max_bytes_per_channel: None,
    };
    for expiry in [false, true] {
        let channel = if expiry {
            "large-expiry"
        } else {
            "large-count"
        };
        let stream = store
            .reserve_publish_position("c6-app", channel)
            .await
            .unwrap()
            .stream_id;
        for serial in 1..=300 {
            store
                .append(record(&stream, channel, serial, now, uncapped.clone()))
                .await
                .unwrap();
        }
        wait_retained(&store, channel, 300, 4800).await;
        let policy = HistoryRetentionPolicy {
            retention_window_seconds: if expiry { 1 } else { 3600 },
            max_messages_per_channel: if expiry { None } else { Some(3) },
            max_bytes_per_channel: None,
        };
        store
            .append(record(&stream, channel, 301, now + 10000, policy))
            .await
            .unwrap();
        let expected = if expiry {
            vec![301]
        } else {
            vec![299, 300, 301]
        };
        wait_retained(
            &store,
            channel,
            expected.len() as u64,
            expected.len() as u64 * 16,
        )
        .await;
        let page = store
            .read_page(HistoryReadRequest {
                app_id: "c6-app".into(),
                channel: channel.into(),
                direction: HistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

async fn legacy_seed(store: &Arc<dyn HistoryStore + Send + Sync>) -> String {
    let stream = store
        .reserve_publish_position("c6-app", "legacy")
        .await
        .unwrap()
        .stream_id;
    let now = sockudo_core::history::now_ms();
    for serial in 1..=12 {
        store
            .append(record(
                &stream,
                "legacy",
                serial,
                now,
                HistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_messages_per_channel: None,
                    max_bytes_per_channel: None,
                },
            ))
            .await
            .unwrap();
    }
    wait_retained(store, "legacy", 12, 192).await;
    stream
}
async fn assert_legacy_migration(
    first: Arc<dyn HistoryStore + Send + Sync>,
    second: Arc<dyn HistoryStore + Send + Sync>,
    stream: &str,
) {
    let policy = HistoryRetentionPolicy {
        retention_window_seconds: 3600,
        max_messages_per_channel: None,
        max_bytes_per_channel: None,
    };
    let now = sockudo_core::history::now_ms();
    let (a, b) = tokio::join!(
        first.append(record(stream, "legacy", 13, now, policy.clone())),
        second.append(record(stream, "legacy", 14, now, policy.clone()))
    );
    a.unwrap();
    b.unwrap();
    wait_retained(&first, "legacy", 14, 224).await;
    let capped = HistoryRetentionPolicy {
        max_messages_per_channel: Some(7),
        ..policy
    };
    second
        .append(record(stream, "legacy", 15, now, capped))
        .await
        .unwrap();
    wait_retained(&first, "legacy", 7, 112).await;
    let page = first
        .read_page(HistoryReadRequest {
            app_id: "c6-app".into(),
            channel: "legacy".into(),
            direction: HistoryDirection::OldestFirst,
            limit: 20,
            cursor: None,
            bounds: Default::default(),
        })
        .await
        .unwrap();
    assert_eq!(
        page.items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        (9..=15).collect::<Vec<_>>()
    );
}
#[cfg(any(feature = "dynamodb", feature = "surrealdb", feature = "scylladb"))]
async fn assert_skewed_retention_preserves_backend_prefix_policy(
    store: Arc<dyn HistoryStore + Send + Sync>,
) {
    let stream = store
        .reserve_publish_position("c6-app", "skewed")
        .await
        .unwrap()
        .stream_id;
    let now = sockudo_core::history::now_ms();
    let uncapped = HistoryRetentionPolicy {
        retention_window_seconds: 3600,
        max_messages_per_channel: None,
        max_bytes_per_channel: None,
    };
    store
        .append(record(&stream, "skewed", 1, now, uncapped.clone()))
        .await
        .unwrap();
    store
        .append(record(&stream, "skewed", 2, now - 2000, uncapped))
        .await
        .unwrap();
    let capped = HistoryRetentionPolicy {
        retention_window_seconds: 1,
        max_messages_per_channel: Some(2),
        max_bytes_per_channel: None,
    };
    store
        .append(record(&stream, "skewed", 3, now, capped))
        .await
        .unwrap();
    let page = store
        .read_page(HistoryReadRequest {
            app_id: "c6-app".into(),
            channel: "skewed".into(),
            direction: HistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: Default::default(),
        })
        .await
        .unwrap();
    // Document backends first remove an expired serial prefix, then enforce caps.
    // Count eviction must not restart expiry past its first nonexpired serial.
    assert_eq!(
        page.items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        store
            .channel_head("c6-app", "skewed")
            .await
            .unwrap()
            .retained_messages,
        2
    );
}

fn config() -> HistoryConfig {
    let mut config = HistoryConfig {
        enabled: true,
        ..Default::default()
    };
    let prefix = format!("c6_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    config.postgres.table_prefix = prefix.clone();
    config.mysql.table_prefix = prefix.clone();
    config.dynamodb.table_prefix = prefix.clone();
    config.surrealdb.table_prefix = prefix.clone();
    config.scylladb.table_prefix = prefix;
    config
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires isolated history database fixtures"]
async fn c6_postgres_accounting() {
    let db = DatabaseConnection {
        host: "127.0.0.1".into(),
        port: 15432,
        username: "postgres".into(),
        password: "postgres123".into(),
        database: "sockudo_test".into(),
        ..Default::default()
    };
    let config = config();
    let first = Arc::new(
        super::postgres::PostgresHistoryStore::new(
            &db,
            &DatabasePooling::default(),
            config.clone(),
            None,
            None,
        )
        .await
        .unwrap(),
    );
    let second = Arc::new(
        super::postgres::PostgresHistoryStore::new(
            &db,
            &DatabasePooling::default(),
            config.clone(),
            None,
            None,
        )
        .await
        .unwrap(),
    );
    let first: Arc<dyn HistoryStore + Send + Sync> = first;
    let second: Arc<dyn HistoryStore + Send + Sync> = second;
    assert_accounting(first.clone(), second.clone()).await;
    assert_large_retention_batches(first.clone()).await;
    let stream = legacy_seed(&first).await;
    super::postgres::simulate_legacy_retention(&db, config.clone()).await;
    assert_legacy_migration(first, second, &stream).await;
}
#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires isolated history database fixtures"]
async fn c6_mysql_accounting() {
    let db = DatabaseConnection {
        host: "127.0.0.1".into(),
        port: 13306,
        username: "root".into(),
        password: "root123".into(),
        database: "sockudo".into(),
        ..Default::default()
    };
    let config = config();
    let first = super::mysql::create_mysql_history_store(
        &db,
        &DatabasePooling::default(),
        config.clone(),
        None,
        None,
    )
    .await
    .unwrap();
    let second = super::mysql::create_mysql_history_store(
        &db,
        &DatabasePooling::default(),
        config.clone(),
        None,
        None,
    )
    .await
    .unwrap();
    let first: Arc<dyn HistoryStore + Send + Sync> = first;
    let second: Arc<dyn HistoryStore + Send + Sync> = second;
    assert_accounting(first.clone(), second.clone()).await;
    assert_large_retention_batches(first.clone()).await;
    let stream = legacy_seed(&first).await;
    super::mysql::simulate_legacy_retention(&db, config.clone()).await;
    assert_legacy_migration(first, second, &stream).await;
}
#[cfg(feature = "dynamodb")]
#[tokio::test]
#[ignore = "requires isolated history database fixtures"]
async fn c6_dynamodb_accounting() {
    let db = DynamoDbSettings {
        region: "us-east-1".into(),
        table_name: format!("c6_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        endpoint_url: Some("http://127.0.0.1:18000".into()),
        aws_access_key_id: Some("dummy".into()),
        aws_secret_access_key: Some("dummy".into()),
        aws_profile_name: None,
    };
    let config = config();
    let first = super::dynamodb::create_dynamodb_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    let second = super::dynamodb::create_dynamodb_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    let first: Arc<dyn HistoryStore + Send + Sync> = first;
    let second: Arc<dyn HistoryStore + Send + Sync> = second;
    assert_accounting(first.clone(), second.clone()).await;
    assert_large_retention_batches(first.clone()).await;
    let stream = legacy_seed(&first).await;
    super::dynamodb::simulate_legacy_retention(&db, config.clone()).await;
    assert_legacy_migration(first.clone(), second, &stream).await;
    assert_skewed_retention_preserves_backend_prefix_policy(first).await;
}
#[cfg(feature = "surrealdb")]
#[tokio::test]
#[ignore = "requires isolated history database fixtures"]
async fn c6_surreal_accounting() {
    let db = SurrealDbSettings {
        url: "ws://127.0.0.1:18001".into(),
        namespace: format!("c6_{}", uuid::Uuid::new_v4().simple()),
        database: "sockudo".into(),
        username: "root".into(),
        password: "root".into(),
        ..Default::default()
    };
    let config = config();
    let first = super::surreal::create_surreal_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    let second = super::surreal::create_surreal_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    let first: Arc<dyn HistoryStore + Send + Sync> = first;
    let second: Arc<dyn HistoryStore + Send + Sync> = second;
    assert_accounting(first.clone(), second.clone()).await;
    assert_large_retention_batches(first.clone()).await;
    let stream = legacy_seed(&first).await;
    super::surreal::simulate_legacy_retention(&db, config.clone()).await;
    assert_legacy_migration(first.clone(), second, &stream).await;
    assert_skewed_retention_preserves_backend_prefix_policy(first).await;
}
#[cfg(feature = "scylladb")]
#[tokio::test]
#[ignore = "requires isolated history database fixtures"]
async fn c6_scylla_accounting() {
    let db = ScyllaDbSettings {
        nodes: vec!["127.0.0.1:19042".into()],
        keyspace: format!("c6_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        replication_factor: 1,
        ..Default::default()
    };
    let session = || async { super::scylla::tests::fixture_session().await.unwrap() };
    let config = config();
    let first = Arc::new(
        super::scylla::ScyllaHistoryStore::from_session(
            &db,
            config.clone(),
            None,
            None,
            session().await,
        )
        .await
        .unwrap(),
    );
    let second = Arc::new(
        super::scylla::ScyllaHistoryStore::from_session(
            &db,
            config.clone(),
            None,
            None,
            session().await,
        )
        .await
        .unwrap(),
    );
    let first: Arc<dyn HistoryStore + Send + Sync> = first;
    let second: Arc<dyn HistoryStore + Send + Sync> = second;
    assert_accounting(first.clone(), second.clone()).await;
    assert_large_retention_batches(first.clone()).await;
    let stream = legacy_seed(&first).await;
    super::scylla::simulate_legacy_retention(&db, config.clone(), &stream).await;
    assert_legacy_migration(first.clone(), second, &stream).await;
    assert_skewed_retention_preserves_backend_prefix_policy(first).await;
}

async fn benchmark_history(store: Arc<dyn HistoryStore + Send + Sync>) {
    for cap in [64, 1024] {
        for payload in [128, 16384] {
            let channel = format!("bench-{cap}-{payload}");
            let stream = store
                .reserve_publish_position("c6-app", &channel)
                .await
                .unwrap()
                .stream_id;
            let now = sockudo_core::history::now_ms();
            let policy = HistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_messages_per_channel: Some(cap),
                max_bytes_per_channel: Some((cap * payload) as u64),
            };
            let make = |serial| {
                let mut record = record(&stream, &channel, serial, now, policy.clone());
                record.payload_bytes = tokio_util::bytes::Bytes::from(vec![b'x'; payload]);
                record
            };
            let settled = |target| {
                let store = store.clone();
                let channel = channel.clone();
                async move {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
                    loop {
                        let head = store.channel_head("c6-app", &channel).await.unwrap();
                        if head.newest_serial == Some(target) {
                            assert_eq!(head.retained_messages, cap as u64);
                            assert_eq!(head.retained_bytes, (cap * payload) as u64);
                            assert_eq!(head.oldest_serial, Some(target - cap as u64 + 1));
                            break;
                        }
                        assert!(
                            tokio::time::Instant::now() < deadline,
                            "durable writer did not finish {head:?}"
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            };
            for serial in 1..=cap as u64 {
                store.append(make(serial)).await.unwrap();
            }
            settled(cap as u64).await;
            let mut next = cap as u64 + 1;
            for sample in 0..12 {
                let start = std::time::Instant::now();
                for serial in next..next + 8 {
                    store.append(make(serial)).await.unwrap();
                }
                settled(next + 7).await;
                let elapsed = start.elapsed().as_nanos();
                let page = store
                    .read_page(HistoryReadRequest {
                        app_id: "c6-app".into(),
                        channel: channel.clone(),
                        direction: HistoryDirection::NewestFirst,
                        limit: 8,
                        cursor: None,
                        bounds: Default::default(),
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    page.items
                        .iter()
                        .map(|item| item.serial)
                        .collect::<Vec<_>>(),
                    (next..next + 8).rev().collect::<Vec<_>>()
                );
                assert!(
                    page.items
                        .iter()
                        .all(|item| item.payload_bytes.len() == payload
                            && item.payload_bytes.iter().all(|byte| *byte == b'x'))
                );
                if sample >= 3 {
                    println!(
                        "C6,cap={cap},payload={payload},sample={},ns={elapsed},verified=8",
                        sample - 3
                    );
                }
                next += 8;
            }
        }
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires isolated local history fixtures; repeated release before/after benchmark"]
async fn benchmark_history_postgres() {
    let db = DatabaseConnection {
        host: "127.0.0.1".into(),
        port: 15432,
        username: "postgres".into(),
        password: "postgres123".into(),
        database: "sockudo_test".into(),
        ..Default::default()
    };
    let mut config = config();
    config.writer_shards = 1;
    let first = Arc::new(
        super::postgres::PostgresHistoryStore::new(
            &db,
            &DatabasePooling::default(),
            config.clone(),
            None,
            None,
        )
        .await
        .unwrap(),
    );
    let _second = Arc::new(
        super::postgres::PostgresHistoryStore::new(
            &db,
            &DatabasePooling::default(),
            config.clone(),
            None,
            None,
        )
        .await
        .unwrap(),
    );
    benchmark_history(first).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires isolated local history fixtures; repeated release before/after benchmark"]
async fn benchmark_history_mysql() {
    let db = DatabaseConnection {
        host: "127.0.0.1".into(),
        port: 13306,
        username: "root".into(),
        password: "root123".into(),
        database: "sockudo".into(),
        ..Default::default()
    };
    let mut config = config();
    config.writer_shards = 1;
    let first = super::mysql::create_mysql_history_store(
        &db,
        &DatabasePooling::default(),
        config.clone(),
        None,
        None,
    )
    .await
    .unwrap();
    let _second = super::mysql::create_mysql_history_store(
        &db,
        &DatabasePooling::default(),
        config.clone(),
        None,
        None,
    )
    .await
    .unwrap();
    benchmark_history(first).await;
}

#[cfg(feature = "dynamodb")]
#[tokio::test]
#[ignore = "requires isolated local history fixtures; repeated release before/after benchmark"]
async fn benchmark_history_dynamodb() {
    let db = DynamoDbSettings {
        region: "us-east-1".into(),
        table_name: format!("c6_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        endpoint_url: Some("http://127.0.0.1:18000".into()),
        aws_access_key_id: Some("dummy".into()),
        aws_secret_access_key: Some("dummy".into()),
        aws_profile_name: None,
    };
    let mut config = config();
    config.writer_shards = 1;
    let first = super::dynamodb::create_dynamodb_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    let _second = super::dynamodb::create_dynamodb_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    benchmark_history(first).await;
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
#[ignore = "requires isolated local history fixtures; repeated release before/after benchmark"]
async fn benchmark_history_surreal() {
    let db = SurrealDbSettings {
        url: "ws://127.0.0.1:18001".into(),
        namespace: format!("c6_{}", uuid::Uuid::new_v4().simple()),
        database: "sockudo".into(),
        username: "root".into(),
        password: "root".into(),
        ..Default::default()
    };
    let mut config = config();
    config.writer_shards = 1;
    let first = super::surreal::create_surreal_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    let _second = super::surreal::create_surreal_history_store(&db, config.clone(), None, None)
        .await
        .unwrap();
    benchmark_history(first).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
#[ignore = "requires isolated local history fixtures; repeated release before/after benchmark"]
async fn benchmark_history_scylla() {
    let db = ScyllaDbSettings {
        nodes: vec!["127.0.0.1:19042".into()],
        keyspace: format!("c6_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        replication_factor: 1,
        ..Default::default()
    };
    let session = || async { super::scylla::tests::fixture_session().await.unwrap() };
    let mut config = config();
    config.writer_shards = 1;
    let first = Arc::new(
        super::scylla::ScyllaHistoryStore::from_session(
            &db,
            config.clone(),
            None,
            None,
            session().await,
        )
        .await
        .unwrap(),
    );
    let _second = Arc::new(
        super::scylla::ScyllaHistoryStore::from_session(
            &db,
            config.clone(),
            None,
            None,
            session().await,
        )
        .await
        .unwrap(),
    );
    benchmark_history(first).await;
}
