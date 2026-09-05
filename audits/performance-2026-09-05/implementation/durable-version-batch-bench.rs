use super::create_version_store;
use sockudo_core::version_store::*;
use sockudo_core::options::*;
use sockudo_core::versioned_messages::*;
use sockudo_protocol::messages::MessageData;
use futures_util::{StreamExt, TryStreamExt};
use std::time::Instant;
fn record(index: usize, channel: &str, revision: u64) -> StoredVersionRecord {
    StoredVersionRecord { app_id: "audit".into(), channel: channel.into(), original_client_id: Some("actor".into()), envelope: None,
        message: VersionedMessage::new_create(MessageSerial::new(format!("m:{index:020}")).unwrap(),
            VersionMetadata { serial: VersionSerial::new(format!("v:{revision:020}")).unwrap(), client_id: Some("actor".into()),
                timestamp_ms: 1, description: None, metadata: None }, index as u64 + 1, index as u64 * 10 + revision,
                Some("event".into()), Some(MessageData::String(format!("{index}:{revision}:{}", "x".repeat(256)))), None) }
}
#[tokio::test]
#[ignore = "actual isolated audit database fixtures"]
async fn benchmark_durable_latest_batch() {
    let backend = std::env::var("AUDIT_BACKEND").unwrap();
    let driver = match backend.as_str() {
        "postgres" => VersionStoreDriver::Postgres,
        "mysql" => VersionStoreDriver::Mysql,
        "dynamodb" => VersionStoreDriver::DynamoDb,
        "scylladb" => VersionStoreDriver::ScyllaDb,
        "surrealdb" => VersionStoreDriver::SurrealDb,
        _ => panic!("unknown backend"),
    };
    let prefix = format!("c7_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let mut history = HistoryConfig::default();
    history.postgres.table_prefix = prefix.clone();
    history.mysql.table_prefix = prefix.clone();
    history.dynamodb.table_prefix = prefix.clone();
    history.scylladb.table_prefix = prefix.clone();
    history.surrealdb.table_prefix = prefix.clone();
    let mut db = DatabaseConfig::default();
    db.postgres = DatabaseConnection { host: "127.0.0.1".into(), port: 15432, username: "postgres".into(),
        password: "postgres123".into(), database: "sockudo_test".into(), connection_pool_size: 1, ..Default::default() };
    db.mysql = DatabaseConnection { host: "127.0.0.1".into(), port: 13306, username: "root".into(),
        password: "root123".into(), database: "sockudo".into(), connection_pool_size: 1, ..Default::default() };
    db.dynamodb = DynamoDbSettings { endpoint_url: Some("http://127.0.0.1:18000".into()), region: "us-east-1".into(),
        aws_access_key_id: Some("audit".into()), aws_secret_access_key: Some("audit".into()), ..Default::default() };
    db.scylladb = ScyllaDbSettings { nodes: vec!["127.0.0.1:19044".into()], keyspace: prefix.clone(), replication_factor: 1, ..Default::default() };
    db.surrealdb = SurrealDbSettings { url: "ws://127.0.0.1:18001".into(), namespace: "audit".into(), database: "version_batch_bench".into(), ..Default::default() };
    let options = VersionedMessagesConfig { enabled: true, driver, retention_window_seconds: 3600, ..Default::default() };
    let store = create_version_store(&options, &history, &db, &DatabasePooling { enabled: false, ..Default::default() }).await.unwrap();
    let count: usize = std::env::var("AUDIT_MESSAGES").unwrap_or_else(|_| "1000".into()).parse().unwrap();
    let channel = format!("room-{count}");
    futures_util::stream::iter(0..count)
        .map(|index| { let store = store.clone(); let channel = channel.clone(); async move {
            store.append_version(record(index, &channel, 1)).await
        } }).buffer_unordered(8).try_collect::<Vec<_>>().await.unwrap();
    // Initial revisions isolate batch retrieval; import ordering is verified by separate regressions.
    let mut ids = (0..100).rev().map(|index| record(index, &channel, 1).message_serial().clone()).collect::<Vec<_>>();
    ids.push(ids[0].clone());
    ids.push(MessageSerial::new("missing").unwrap());
    // Empty requests must produce no unrelated records.
    assert!(store.get_latest_batch("audit", &channel, &[]).await.unwrap().is_empty());
    println!("BATCH_CSV,backend,messages,sample,requested,returned,elapsed_us,returned_bytes");
    for sample in 0..9 {
        let start = Instant::now();
        let rows = store.get_latest_batch("audit", &channel, &ids).await.unwrap();
        let elapsed = start.elapsed().as_micros();
        assert_eq!(rows.len(), 100);
        let mut returned_bytes = 0;
        for index in 0..100 {
            let expected = record(index, &channel, 1);
            let actual = &rows[expected.message_serial()];
            let bytes = sonic_rs::to_vec(actual).unwrap();
            assert_eq!(bytes, sonic_rs::to_vec(&expected).unwrap());
            returned_bytes += bytes.len();
        }
        println!("BATCH_CSV,{backend},{count},{sample},102,100,{elapsed},{returned_bytes}");
    }
}
