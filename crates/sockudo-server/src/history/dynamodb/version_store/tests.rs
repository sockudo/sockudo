use super::*;
#[tokio::test]
#[ignore = "requires local audit DynamoDB Local fixture on port 18000"]
async fn dynamodb_compact_versions_restart_and_fencing() {
    let config = DynamoDbSettings {
        endpoint_url: Some("http://127.0.0.1:18000".into()),
        region: "us-east-1".into(),
        aws_access_key_id: Some("audit".into()),
        aws_secret_access_key: Some("audit".into()),
        ..Default::default()
    };
    let prefix = format!("c2_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let store = DynamoDbVersionStore::new(&config, &prefix, 3600)
        .await
        .unwrap();
    let (latest, expected, receipt_request) =
        crate::history::version_storage_tests::compact_versions(&store).await;
    let encoding = store
        .client
        .get_item()
        .table_name(&store.tables.version_messages)
        .key(
            "app_channel",
            SelfStore::attr_s(&SelfStore::app_channel_key("audit", "room")),
        )
        .key(
            "message_serial",
            SelfStore::attr_s(latest.message_serial().as_str()),
        )
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    let payload = encoding
        .get("latest_payload_bytes")
        .unwrap()
        .as_b()
        .unwrap();
    let reference = sockudo_core::version_store::EncodedVersionRecord::decode(payload.as_ref())
        .unwrap()
        .text
        .unwrap();
    let snapshot = store
        .client
        .get_item()
        .table_name(&store.tables.version_entries)
        .key(
            "app_channel",
            SelfStore::attr_s(&SelfStore::app_channel_key("audit", "room")),
        )
        .key(
            "message_version_key",
            SelfStore::attr_s(&SelfStore::text_key(&reference.snapshot_key)),
        )
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert!(
        snapshot.get(SelfStore::EXPIRES_AT_ATTR).is_none(),
        "durable receipt must retain its shared snapshot"
    );
    // A fresh process reads existing full and compact records automatically.
    drop(store);
    let reader = DynamoDbVersionStore::new(&config, &prefix, 3600)
        .await
        .unwrap();
    let record = reader
        .get_latest("audit", "room", latest.message_serial())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sonic_rs::to_vec(&record).unwrap(),
        sonic_rs::to_vec(&latest).unwrap()
    );
    let VersionMutationResult::Duplicate { record, .. } =
        reader.compare_and_apply(receipt_request).await.unwrap()
    else {
        panic!("receipt missing after restart")
    };
    assert_eq!(
        sonic_rs::to_vec(&record).unwrap(),
        sonic_rs::to_vec(&expected[1]).unwrap()
    );
    crate::history::version_storage_tests::raw_imports(&reader, &latest).await;
}
type SelfStore = DynamoDbVersionStore;

#[tokio::test]
#[ignore = "requires isolated local audit database fixture"]
async fn dynamodb_metadata_counts_preserve_legacy_and_latest_states() {
    let config = DynamoDbSettings {
        endpoint_url: Some("http://127.0.0.1:18000".into()),
        region: "us-east-1".into(),
        aws_access_key_id: Some("audit".into()),
        aws_secret_access_key: Some("audit".into()),
        ..Default::default()
    };
    let prefix = format!("c2_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let store = DynamoDbVersionStore::new(&config, &prefix, 3600)
        .await
        .unwrap();
    crate::history::version_storage_tests::metadata_counts(&store).await;
    store
        .client
        .update_item()
        .table_name(&store.tables.version_messages)
        .key(
            "app_channel",
            DynamoDbVersionStore::attr_s(&DynamoDbVersionStore::app_channel_key(
                "audit", "metadata",
            )),
        )
        .key(
            "message_serial",
            DynamoDbVersionStore::attr_s("msg:00000000000000000003"),
        )
        .update_expression("REMOVE is_open_stream")
        .send()
        .await
        .unwrap();
    assert_eq!(
        store
            .active_stream_count("audit", "metadata")
            .await
            .unwrap(),
        68
    );
}

#[cfg(feature = "push")]
#[tokio::test]
#[ignore = "requires local audit DynamoDB fixture and loopback fault endpoint"]
async fn dynamodb_latest_batch_retries_partial_results_and_fails_closed() {
    use axum::{Router, body::Bytes, extract::State, routing::post};
    use base64::Engine;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    #[derive(Clone)]
    struct Fault {
        calls: Arc<AtomicUsize>,
        exhaust: Arc<AtomicBool>,
        records: Arc<std::collections::BTreeMap<String, StoredVersionRecord>>,
    }
    async fn respond(State(fault): State<Fault>, bytes: Bytes) -> axum::Json<serde_json::Value> {
        let request: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let (table, batch) = request["RequestItems"]
            .as_object()
            .unwrap()
            .iter()
            .next()
            .unwrap();
        assert_eq!(batch["ConsistentRead"], true);
        let keys = batch["Keys"].as_array().unwrap();
        assert!(keys.len() <= 100);
        let call = fault.calls.fetch_add(1, Ordering::SeqCst);
        let processed = if fault.exhaust.load(Ordering::SeqCst) {
            0
        } else if call == 0 {
            1
        } else {
            keys.len()
        };
        let records = keys
            .iter()
            .take(processed)
            .map(|key| {
                let serial = key["message_serial"]["S"].as_str().unwrap();
                let data = base64::engine::general_purpose::STANDARD
                    .encode(sonic_rs::to_vec(&fault.records[serial]).unwrap());
                serde_json::json!({"latest_payload_bytes":{"B":data}})
            })
            .collect::<Vec<_>>();
        let mut response = serde_json::json!({"Responses":{table:records},"UnprocessedKeys":{}});
        if processed < keys.len() {
            response["UnprocessedKeys"][table] =
                serde_json::json!({"Keys":&keys[processed..],"ConsistentRead":true});
        }
        axum::Json(response)
    }
    let config = DynamoDbSettings {
        endpoint_url: Some("http://127.0.0.1:18000".into()),
        region: "us-east-1".into(),
        aws_access_key_id: Some("audit".into()),
        aws_secret_access_key: Some("audit".into()),
        ..Default::default()
    };
    let prefix = format!("c7_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let mut store = DynamoDbVersionStore::new(&config, &prefix, 3600)
        .await
        .unwrap();
    let mut records = std::collections::BTreeMap::new();
    for index in 0..2u64 {
        let record = StoredVersionRecord {
            app_id: "audit".into(),
            channel: "room".into(),
            original_client_id: Some("actor".into()),
            envelope: None,
            message: sockudo_core::versioned_messages::VersionedMessage::new_create(
                sockudo_core::versioned_messages::MessageSerial::new(format!("msg:{index}"))
                    .unwrap(),
                sockudo_core::versioned_messages::VersionMetadata {
                    serial: sockudo_core::versioned_messages::VersionSerial::new("version:1")
                        .unwrap(),
                    client_id: Some("actor".into()),
                    timestamp_ms: 1,
                    description: None,
                    metadata: None,
                },
                index + 1,
                index + 1,
                Some("event".into()),
                Some(sockudo_protocol::messages::MessageData::String(
                    "世界".repeat(100),
                )),
                None,
            ),
        };
        records.insert(record.message_serial().as_str().to_owned(), record);
    }
    let fault = Fault {
        calls: Arc::new(AtomicUsize::new(0)),
        exhaust: Arc::new(AtomicBool::new(false)),
        records: Arc::new(records),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/", post(respond))
        .with_state(fault.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let sdk = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
            "audit", "audit", None, None, "test",
        ))
        .endpoint_url(format!("http://{address}"))
        .build();
    store.client = Client::from_conf(sdk);
    let ids = fault
        .records
        .values()
        .map(|record| record.message_serial().clone())
        .collect::<Vec<_>>();
    let rows = store.get_latest_batch("audit", "room", &ids).await.unwrap();
    assert_eq!(fault.calls.load(Ordering::SeqCst), 2);
    assert_eq!(rows.len(), 2);
    for expected in fault.records.values() {
        assert_eq!(
            sonic_rs::to_vec(&rows[expected.message_serial()]).unwrap(),
            sonic_rs::to_vec(expected).unwrap()
        );
    }
    fault.calls.store(0, Ordering::SeqCst);
    fault.exhaust.store(true, Ordering::SeqCst);
    assert!(
        store.get_latest_batch("audit", "room", &ids).await.is_err(),
        "partial results must never escape after retry exhaustion"
    );
    assert_eq!(fault.calls.load(Ordering::SeqCst), 8);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
