use super::*;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
struct FixtureAddress;
#[async_trait::async_trait]
impl AddressTranslator for FixtureAddress {
    async fn translate_address(
        &self,
        _: &UntranslatedPeer,
    ) -> std::result::Result<std::net::SocketAddr, TranslationError> {
        Ok("127.0.0.1:19044".parse().unwrap())
    }
}
async fn session() -> Arc<Session> {
    Arc::new(
        SessionBuilder::new()
            .known_node("127.0.0.1:19044")
            .address_translator(Arc::new(FixtureAddress))
            .disallow_shard_aware_port(true)
            .build()
            .await
            .unwrap(),
    )
}
#[tokio::test]
#[ignore = "requires local audit ScyllaDB fixture on port 19044"]
async fn scylla_compact_versions_restart_and_fencing() {
    let config = ScyllaDbSettings {
        keyspace: format!("c2_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        replication_factor: 1,
        ..Default::default()
    };
    let store = ScyllaVersionStore::from_session(&config, "test", 3600, session().await)
        .await
        .unwrap();
    let (latest, expected, receipt_request) =
        crate::history::version_storage_tests::compact_versions(&store).await;
    let result = store.session.query_unpaged(format!("SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND commit_key >= 'v:' AND commit_key < 'w:'", store.tables.version_commits_fq()), ("audit", "room")).await.unwrap().into_rows_result().unwrap();
    let stored: usize = result
        .rows::<(Vec<u8>,)>()
        .unwrap()
        .map(|row| row.unwrap().0.len())
        .sum();
    let full: usize = expected
        .iter()
        .map(|record| sonic_rs::to_vec(record).unwrap().len())
        .sum();
    assert!(stored * 2 < full, "stored={stored} full={full}");
    let result = store.session.query_unpaged(format!("SELECT commit_key FROM {} WHERE app_id = ? AND channel = ? AND commit_key >= 't:' AND commit_key < 'u:'", store.tables.version_commits_fq()), ("audit", "room")).await.unwrap().into_rows_result().unwrap();
    assert_eq!(result.rows::<(String,)>().unwrap().count(), 1);
    drop(store);
    let reader = ScyllaVersionStore::from_session(&config, "test", 3600, session().await)
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
}

#[tokio::test]
#[ignore = "requires local audit ScyllaDB fixture on port 19044"]
async fn scylla_raw_import_updates_atomic_latest_counts_and_preserves_compact_history() {
    use sockudo_core::version_store::{VersionMutationLimits, VersionPrecondition};
    use sockudo_core::versioned_messages::VersionSerial;
    use sockudo_protocol::messages::MessageData;
    let config = ScyllaDbSettings {
        keyspace: format!(
            "c2_import_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ),
        replication_factor: 1,
        ..Default::default()
    };
    let store = ScyllaVersionStore::from_session(&config, "test", 3600, session().await)
        .await
        .unwrap();
    let second = ScyllaVersionStore::from_session(&config, "test", 3600, session().await)
        .await
        .unwrap();
    let (latest, expected, mut append) =
        crate::history::version_storage_tests::compact_versions(&store).await;
    let mut older = latest.clone();
    older.message.version.serial = VersionSerial::new("ver:00000000000000000015").unwrap();
    older.message.replay_position.delivery_serial = 1000;
    older.message.data = Some(MessageData::String("older import".into()));
    older.message.append_fragment = Some("import".into());
    older.envelope = None;
    let mut newer = older.clone();
    newer.message.version.serial = VersionSerial::new("zzz:import").unwrap();
    newer.message.replay_position.delivery_serial = 1001;
    newer.message.data = Some(MessageData::String("newer import".into()));
    store.append_version(older.clone()).await.unwrap();
    assert_eq!(
        store
            .get_latest("audit", "room", latest.message_serial())
            .await
            .unwrap()
            .unwrap()
            .version_serial(),
        latest.version_serial()
    );
    second.append_version(newer.clone()).await.unwrap();
    store.append_version(newer.clone()).await.unwrap();
    let fetched = store
        .get_latest("audit", "room", latest.message_serial())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sonic_rs::to_value(&fetched).unwrap(),
        sonic_rs::to_value(&newer).unwrap()
    );
    let mut conflict = newer.clone();
    conflict.message.data = Some(MessageData::String("different".into()));
    store.append_version(conflict).await.unwrap();
    let first_winner = store
        .get_latest("audit", "room", newer.message_serial())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sonic_rs::to_value(&first_winner).unwrap(),
        sonic_rs::to_value(&newer).unwrap()
    );
    let replay = store
        .replay_after(VersionReplayRequest {
            app_id: "audit".into(),
            channel: "room".into(),
            after_delivery_serial: 999,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(replay.len(), 2);
    assert_eq!(
        sonic_rs::to_value(&replay[0]).unwrap(),
        sonic_rs::to_value(&older).unwrap()
    );
    assert_eq!(
        sonic_rs::to_value(&replay[1]).unwrap(),
        sonic_rs::to_value(&newer).unwrap()
    );
    let historical = store
        .get_versions(VersionStoreReadRequest {
            app_id: "audit".into(),
            channel: "room".into(),
            message_serial: latest.message_serial().clone(),
            direction: VersionStoreDirection::OldestFirst,
            limit: 256,
            cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(historical.items.len(), expected.len() + 3); // expected excludes final concurrent winner
    let original = historical
        .items
        .iter()
        .find(|record| record.version_serial() == expected[97].version_serial())
        .unwrap();
    assert_eq!(
        sonic_rs::to_value(original).unwrap(),
        sonic_rs::to_value(&expected[97]).unwrap()
    );
    let row = store
        .session
        .query_unpaged(
            format!(
                "SELECT append_count FROM {} WHERE app_id=? AND channel=? AND commit_key=?",
                store.tables.version_commits_fq()
            ),
            (
                "audit",
                "room",
                message_commit_key(latest.message_serial().as_str()),
            ),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(i64,)>()
        .unwrap();
    assert_eq!(row.0, 131);
    append.expected = VersionPrecondition::from_record(&newer);
    append.version.serial = VersionSerial::new("zzzz:next").unwrap();
    append.idempotency = None;
    append.limits = VersionMutationLimits {
        max_appends_per_message: Some(131),
        ..Default::default()
    };
    assert!(matches!(
        store.compare_and_apply(append.clone()).await.unwrap(),
        VersionMutationResult::Rejected(_)
    ));
    append.limits.max_appends_per_message = Some(132);
    let VersionMutationResult::Applied { record, .. } =
        second.compare_and_apply(append.clone()).await.unwrap()
    else {
        panic!("imported latest was not mutable");
    };
    assert!(record.delivery_serial() >= 1002);

    // Older imports change the append count without changing the latest version.
    // A mutation racing those imports must fence the count as well as the latest
    // identity, preserving all imported revisions and the following serial floor.
    use futures_util::StreamExt;
    let imports: Vec<_> = (0..8)
        .map(|index| {
            let mut imported = older.clone();
            imported.message.version.serial =
                VersionSerial::new(format!("ver:import:{index}")).unwrap();
            imported.message.replay_position.delivery_serial = 2000 + index;
            imported
        })
        .collect();
    append.expected = VersionPrecondition::from_record(&record);
    append.version.serial = VersionSerial::new("zzzzz:concurrent").unwrap();
    append.limits.max_appends_per_message = None;
    let import_work = futures_util::stream::iter(imports.iter())
        .map(|imported| {
            let store = &store;
            async move {
                store.append_version(imported.clone()).await.unwrap();
            }
        })
        .buffer_unordered(4)
        .collect::<Vec<_>>();
    let (_, outcome) = tokio::join!(import_work, second.compare_and_apply(append));
    let VersionMutationResult::Applied { record: winner, .. } = outcome.unwrap() else {
        panic!("concurrent mutation was not applied");
    };
    assert_eq!(winner.version_serial().as_str(), "zzzzz:concurrent");
    let rows = store
        .session
        .query_unpaged(
            format!(
                "SELECT append_count FROM {} WHERE app_id=? AND channel=? AND commit_key=?",
                store.tables.version_commits_fq()
            ),
            (
                "audit",
                "room",
                message_commit_key(latest.message_serial().as_str()),
            ),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    assert_eq!(rows.single_row::<(i64,)>().unwrap().0, 141);
    for imported in imports {
        let found = second
            .get_versions(VersionStoreReadRequest {
                app_id: "audit".into(),
                channel: "room".into(),
                message_serial: latest.message_serial().clone(),
                direction: VersionStoreDirection::OldestFirst,
                limit: 256,
                cursor: None,
            })
            .await
            .unwrap()
            .items
            .into_iter()
            .find(|item| item.version_serial() == imported.version_serial())
            .unwrap();
        assert_eq!(
            sonic_rs::to_value(&found).unwrap(),
            sonic_rs::to_value(&imported).unwrap()
        );
    }
}

#[tokio::test]
#[ignore = "requires isolated local audit database fixture"]
async fn scylla_metadata_counts_preserve_legacy_and_latest_states() {
    let config = ScyllaDbSettings {
        keyspace: format!("c2_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        replication_factor: 1,
        ..Default::default()
    };
    let store = ScyllaVersionStore::from_session(&config, "test", 3600, session().await)
        .await
        .unwrap();
    crate::history::version_storage_tests::metadata_counts(&store).await;
    store
        .session
        .query_unpaged(
            format!(
                "DELETE FROM {} WHERE app_id=? AND channel=? AND commit_key=?",
                store.tables.version_commits_fq()
            ),
            ("audit", "metadata", "m:msg:00000000000000000003"),
        )
        .await
        .unwrap();
    assert_eq!(store.message_count("audit", "metadata").await.unwrap(), 205);
    assert_eq!(
        store
            .active_stream_count("audit", "metadata")
            .await
            .unwrap(),
        68
    );
    // Native TTL can leave a later-written pointer after all its payload rows expire.
    store
        .session
        .query_unpaged(
            format!(
                "DELETE FROM {} WHERE app_id=? AND channel=? AND message_serial=?",
                store.tables.version_entries_by_message_fq()
            ),
            ("audit", "metadata", "msg:00000000000000000003"),
        )
        .await
        .unwrap();
    assert_eq!(store.message_count("audit", "metadata").await.unwrap(), 204);
    assert_eq!(
        store
            .active_stream_count("audit", "metadata")
            .await
            .unwrap(),
        67
    );
}
