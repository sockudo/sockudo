use sockudo_core::message_envelope::PublishIdempotencyMetadata;
use sockudo_core::version_store::*;
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

pub(super) async fn compact_versions(
    store: &dyn VersionStore,
) -> (
    StoredVersionRecord,
    Vec<StoredVersionRecord>,
    VersionMutationRequest,
) {
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
    let mut first_request = None;
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
        if first_request.is_none() {
            first_request = Some(request.clone());
        }
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
    // A different writer with the same expected version must not corrupt the
    // snapshot committed by the winner. Repeat the first receipt afterwards.
    let request = VersionMutationRequest {
        app_id: "audit".into(),
        channel: "room".into(),
        message_serial: record.message_serial().clone(),
        expected: VersionPrecondition::from_record(&record),
        version: version(2000),
        mutation: VersionMutation::Append(MessageAppend {
            data_fragment: " winner λ".into(),
            extras: None,
        }),
        idempotency: None,
        limits: VersionMutationLimits::default(),
    };
    let mut rival = request.clone();
    rival.version = version(2001);
    rival.mutation = VersionMutation::Append(MessageAppend {
        data_fragment: " rival 世界".into(),
        extras: None,
    });
    let (first, second) = tokio::join!(
        store.compare_and_apply(request),
        store.compare_and_apply(rival)
    );
    let mut applied = Vec::new();
    for outcome in [first, second] {
        match outcome.unwrap() {
            VersionMutationResult::Applied { record, .. } => applied.push(record),
            VersionMutationResult::Conflict { .. } => {}
            outcome => panic!("unexpected concurrent outcome: {outcome:?}"),
        }
    }
    assert_eq!(applied.len(), 1);
    record = applied.pop().unwrap();
    let latest = store
        .get_latest("audit", "room", record.message_serial())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sonic_rs::to_vec(&latest).unwrap(),
        sonic_rs::to_vec(&record).unwrap()
    );
    let replay = store
        .replay_after(VersionReplayRequest {
            app_id: "audit".into(),
            channel: "room".into(),
            after_delivery_serial: 1,
            limit: 256,
        })
        .await
        .unwrap();
    assert_eq!(replay.len(), 129);
    for (actual, expected) in replay
        .iter()
        .zip(expected.iter().skip(1).chain(std::iter::once(&record)))
    {
        assert_eq!(
            sonic_rs::to_vec(actual).unwrap(),
            sonic_rs::to_vec(expected).unwrap()
        );
    }
    let first_request = first_request.unwrap();
    let VersionMutationResult::Duplicate {
        record: receipt, ..
    } = store
        .compare_and_apply(first_request.clone())
        .await
        .unwrap()
    else {
        panic!("first receipt missing");
    };
    assert_eq!(
        sonic_rs::to_vec(&receipt).unwrap(),
        sonic_rs::to_vec(&expected[1]).unwrap()
    );
    (record, expected, first_request)
}

/// Imported revisions may arrive outside version and delivery order; a restart
/// must preserve their public payloads, latest selection, counter and serial floor.
pub(super) async fn raw_imports(store: &dyn VersionStore, latest: &StoredVersionRecord) {
    let mut older = latest.clone();
    older.message.version = version(15);
    older.message.data = Some(MessageData::String("older import".into()));
    older.message.replay_position.delivery_serial = 1000;
    older.envelope = None;
    let mut newer = older.clone();
    newer.message.version.serial = VersionSerial::new("zzz:import").unwrap();
    newer.message.data = Some(MessageData::String("newer import".into()));
    newer.message.replay_position.delivery_serial = 1001;
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
    store.append_version(newer.clone()).await.unwrap();
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
    let replay = store
        .replay_after(VersionReplayRequest {
            app_id: "audit".into(),
            channel: "room".into(),
            after_delivery_serial: 999,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        sonic_rs::to_value(&replay).unwrap(),
        sonic_rs::to_value(&vec![older, newer.clone()]).unwrap()
    );
    let mut append = VersionMutationRequest {
        app_id: "audit".into(),
        channel: "room".into(),
        message_serial: newer.message_serial().clone(),
        expected: VersionPrecondition::from_record(&newer),
        version: VersionMetadata {
            serial: VersionSerial::new("zzzz:next").unwrap(),
            ..version(1)
        },
        mutation: VersionMutation::Append(MessageAppend {
            data_fragment: " after import".into(),
            extras: None,
        }),
        idempotency: None,
        limits: VersionMutationLimits {
            max_appends_per_message: Some(131),
            ..Default::default()
        },
    };
    assert!(matches!(
        store.compare_and_apply(append.clone()).await.unwrap(),
        VersionMutationResult::Rejected(_)
    ));
    append.limits.max_appends_per_message = Some(132);
    let VersionMutationResult::Applied { record, .. } =
        store.compare_and_apply(append).await.unwrap()
    else {
        panic!("imported latest was not mutable")
    };
    assert!(record.delivery_serial() >= 1002);
    assert_eq!(
        record.message.data,
        Some(MessageData::String("newer import after import".into()))
    );
}

/// More than two metadata pages, sizeable bodies, older imports and terminal
/// latest revisions must produce the same authoritative counts as point reads.
pub(super) async fn metadata_counts(store: &dyn VersionStore) {
    use sockudo_protocol::messages::{AiExtras, MessageExtras};
    use std::collections::HashMap;
    for index in 0..205u64 {
        let record = StoredVersionRecord {
            app_id: "audit".into(),
            channel: "metadata".into(),
            original_client_id: None,
            envelope: None,
            message: VersionedMessage::new_create(
                MessageSerial::new(format!("msg:{index:020}")).unwrap(),
                version(10),
                index + 1,
                index * 10 + 1,
                Some("text".into()),
                Some(MessageData::String("x".repeat(16 * 1024))),
                Some(MessageExtras {
                    ai: Some(AiExtras {
                        transport: Some(HashMap::from([(
                            "status".into(),
                            if index % 3 == 0 {
                                "streaming"
                            } else {
                                "complete"
                            }
                            .into(),
                        )])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            ),
        };
        store.append_version(record.clone()).await.unwrap();
        if index % 3 == 0 {
            let mut older = record;
            older.message.version = version(5);
            older.message.replay_position.delivery_serial = index * 10 + 2;
            older.message.extras = None;
            store.append_version(older).await.unwrap();
        }
    }
    assert_eq!(store.message_count("audit", "metadata").await.unwrap(), 205);
    assert_eq!(
        store
            .active_stream_count("audit", "metadata")
            .await
            .unwrap(),
        69
    );
    assert_eq!(
        store.active_stream_count("audit", "missing").await.unwrap(),
        0
    );
    // Advance one streaming logical message to a terminal latest version.
    let serial = MessageSerial::new("msg:00000000000000000000").unwrap();
    let mut terminal = store
        .get_latest("audit", "metadata", &serial)
        .await
        .unwrap()
        .unwrap();
    terminal.message.version = version(20);
    terminal.message.replay_position.delivery_serial = 3000;
    terminal.message.extras = None;
    store.append_version(terminal).await.unwrap();
    assert_eq!(store.message_count("audit", "metadata").await.unwrap(), 205);
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
