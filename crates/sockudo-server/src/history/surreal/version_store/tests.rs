use super::*;
use sockudo_core::version_store::VersionMutationResult;
#[tokio::test]
#[ignore = "requires local audit SurrealDB fixture on port 18001"]
async fn surreal_compact_versions_restart_and_fencing() {
    let config = SurrealDbSettings {
        url: "ws://127.0.0.1:18001".into(),
        namespace: "audit".into(),
        database: "compact_versions".into(),
        ..Default::default()
    };
    let prefix = format!("c2_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let store = create_surreal_version_store(&config, &prefix)
        .await
        .unwrap();
    let (latest, expected, receipt_request) =
        crate::history::version_storage_tests::compact_versions(store.as_ref()).await;
    drop(store);
    let reader = create_surreal_version_store(&config, &prefix)
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
    crate::history::version_storage_tests::raw_imports(reader.as_ref(), &latest).await;

    // Legacy raw imports could leave a nonempty but stale optional cache.
    // Batch reads and the mutation predecessor must follow the current pointer.
    let authoritative = reader
        .get_latest("audit", "room", latest.message_serial())
        .await
        .unwrap()
        .unwrap();
    let db = connect(config.url.as_str()).await.unwrap();
    db.signin(Root {
        username: config.username.clone(),
        password: config.password.clone(),
    })
    .await
    .unwrap();
    db.use_ns(&config.namespace)
        .use_db(&config.database)
        .await
        .unwrap();
    db.query(format!(
        "UPDATE {prefix}_version_messages SET latest_payload_bytes = $stale WHERE app_id = 'audit' AND channel = 'room'"
    ))
    .bind(("stale", sonic_rs::to_vec(&expected[0]).unwrap()))
    .await
    .unwrap()
    .check()
    .unwrap();
    let batch = reader
        .get_latest_batch("audit", "room", &[latest.message_serial().clone()])
        .await
        .unwrap();
    assert_eq!(
        sonic_rs::to_vec(batch.values().next().unwrap()).unwrap(),
        sonic_rs::to_vec(&authoritative).unwrap()
    );
    use sockudo_core::version_store::{
        VersionMutation, VersionMutationLimits, VersionMutationRequest, VersionPrecondition,
    };
    use sockudo_core::versioned_messages::{MessageAppend, VersionSerial};
    let mut next_version = authoritative.message.version.clone();
    next_version.serial = VersionSerial::new("zzzzz:after-stale-cache").unwrap();
    let result = reader
        .compare_and_apply(VersionMutationRequest {
            app_id: "audit".into(),
            channel: "room".into(),
            message_serial: latest.message_serial().clone(),
            expected: VersionPrecondition::from_record(&authoritative),
            version: next_version,
            mutation: VersionMutation::Append(MessageAppend {
                data_fragment: " final".into(),
                extras: None,
            }),
            idempotency: None,
            limits: VersionMutationLimits::default(),
        })
        .await
        .unwrap();
    let VersionMutationResult::Applied { record, .. } = result else {
        panic!("stale cache replaced the authoritative predecessor")
    };
    assert_eq!(
        record.message.data,
        Some(sockudo_protocol::messages::MessageData::String(
            "newer import after import final".into()
        ))
    );
}

#[tokio::test]
#[ignore = "requires local audit SurrealDB fixture on port 18001"]
async fn surreal_compact_versions_retention_preserves_receipts() {
    let config = SurrealDbSettings {
        url: "ws://127.0.0.1:18001".into(),
        namespace: "audit".into(),
        database: "compact_versions".into(),
        ..Default::default()
    };
    let prefix = format!("c2_gc_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let store = create_surreal_version_store(&config, &prefix)
        .await
        .unwrap();
    let (_, expected, receipt_request) =
        crate::history::version_storage_tests::compact_versions(store.as_ref()).await;
    let cutoff = sockudo_core::history::now_ms() + 60_000;
    let mut purged = 0;
    for _ in 0..32 {
        let (count, more) = store.purge_before(cutoff, 17).await.unwrap();
        purged += count;
        let VersionMutationResult::Duplicate { record, .. } = store
            .compare_and_apply(receipt_request.clone())
            .await
            .unwrap()
        else {
            panic!("receipt lost during partial purge")
        };
        assert_eq!(
            sonic_rs::to_vec(&record).unwrap(),
            sonic_rs::to_vec(&expected[1]).unwrap()
        );
        if !more {
            break;
        }
    }
    assert!(purged >= 130);
    assert_eq!(store.message_count("audit", "room").await.unwrap(), 0);
    drop(store);
    let reader = create_surreal_version_store(&config, &prefix)
        .await
        .unwrap();
    let VersionMutationResult::Duplicate { record, .. } =
        reader.compare_and_apply(receipt_request).await.unwrap()
    else {
        panic!("receipt lost after purge and restart")
    };
    assert_eq!(
        sonic_rs::to_vec(&record).unwrap(),
        sonic_rs::to_vec(&expected[1]).unwrap()
    );
}

#[tokio::test]
#[ignore = "requires isolated local audit database fixture"]
async fn surreal_metadata_counts_preserve_legacy_and_latest_states() {
    let config = SurrealDbSettings {
        url: "ws://127.0.0.1:18001".into(),
        namespace: "audit".into(),
        database: "compact_versions".into(),
        ..Default::default()
    };
    let prefix = format!("c2_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let store = create_surreal_version_store(&config, &prefix)
        .await
        .unwrap();
    crate::history::version_storage_tests::metadata_counts(store.as_ref()).await;
    let db = connect(config.url.as_str()).await.unwrap();
    db.signin(Root {
        username: config.username.clone(),
        password: config.password.clone(),
    })
    .await
    .unwrap();
    db.use_ns(&config.namespace)
        .use_db(&config.database)
        .await
        .unwrap();
    db.query(format!("UPDATE {prefix}_version_messages SET state_version_serial=NONE, is_open_stream=false WHERE app_id='audit' AND channel='metadata' RETURN NONE"))
        .await.unwrap().check().unwrap();
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
