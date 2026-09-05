use super::create_annotation_store;
use sockudo_core::annotations::*;
use sockudo_core::history::now_ms;
use sockudo_core::options::*;
use sockudo_core::versioned_messages::MessageSerial;
use std::time::Instant;

fn event(n: usize, channel: &str) -> StoredAnnotationEvent {
    StoredAnnotationEvent { app_id: "audit".into(), channel_id: channel.into(), stored_at_ms: now_ms(),
        annotation: Annotation { id: AnnotationId::new(format!("id:{n:020}")).unwrap(), action: AnnotationAction::Create,
            serial: AnnotationSerial::new(format!("ann:{n:020}")).unwrap(), message_serial: MessageSerial::new("message").unwrap(),
            annotation_type: AnnotationType::new("reaction:total.v1").unwrap(), name: None, client_id: None, count: None,
            data: None, encoding: None, timestamp: n as i64 } }
}

#[tokio::test]
#[ignore = "actual isolated audit database fixtures"]
async fn benchmark_durable_annotation_reuse() {
    let backend = std::env::var("AUDIT_BACKEND").unwrap();
    let driver = match backend.as_str() {
        "postgres" => VersionStoreDriver::Postgres,
        "mysql" => VersionStoreDriver::Mysql,
        "dynamodb" => VersionStoreDriver::DynamoDb,
        "scylladb" => VersionStoreDriver::ScyllaDb,
        "surrealdb" => VersionStoreDriver::SurrealDb,
        _ => panic!("unknown backend"),
    };
    let prefix = format!("c3_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
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
    db.surrealdb = SurrealDbSettings { url: "ws://127.0.0.1:18001".into(), namespace: "audit".into(), database: "annotation_bench".into(), ..Default::default() };
    let options = VersionedMessagesConfig { enabled: true, driver, retention_window_seconds: 3600, ..Default::default() };
    let store = create_annotation_store(&options, &history, &db, &DatabasePooling { enabled: false, ..Default::default() }).await.unwrap();
    println!("ANNOTATION_CSV,backend,retained,sample,duplicate_us,total");
    for count in [100, 1000] {
        let channel = format!("room-{count}");
        for n in 0..count {
            let result = store.append_event(event(n, &channel)).await.unwrap();
            assert_eq!(result.summary, AnnotationSummary::Total(TotalAnnotationSummary { total: n as u64 + 1 }));
        }
        for sample in 0..15 {
            let start = Instant::now();
            let projection = store.append_event(event(count - 1, &channel)).await.unwrap();
            let elapsed = start.elapsed().as_micros();
            assert_eq!(projection.summary, AnnotationSummary::Total(TotalAnnotationSummary { total: count as u64 }));
            assert_eq!(projection.last_annotation_serial, Some(event(count - 1, &channel).annotation.serial));
            println!("ANNOTATION_CSV,{backend},{count},{sample},{elapsed},{count}");
        }
    }
}
