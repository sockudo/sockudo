use super::*;
use crate::versioned_messages::MessageSerial;
use sonic_rs::json;

fn annotation_type(value: &str) -> AnnotationType {
    AnnotationType::new(value).unwrap()
}

fn serial(value: &str) -> AnnotationSerial {
    AnnotationSerial::new(value).unwrap()
}

fn event(
    serial_value: &str,
    annotation_type: &str,
    action: AnnotationAction,
    name: Option<&str>,
    client_id: Option<&str>,
    count: Option<u64>,
) -> Annotation {
    Annotation {
        id: AnnotationId::new(format!("ann:{serial_value}")).unwrap(),
        action,
        serial: serial(serial_value),
        message_serial: MessageSerial::new("msg:1").unwrap(),
        annotation_type: annotation_type.into(),
        name: name.map(str::to_string),
        client_id: client_id.map(str::to_string),
        count,
        data: Some(json!({"raw": serial_value})),
        encoding: None,
        timestamp: 1,
    }
}

impl From<&str> for AnnotationType {
    fn from(value: &str) -> Self {
        AnnotationType::new(value).unwrap()
    }
}

fn projection(annotation_type_value: &str, events: Vec<Annotation>) -> AnnotationProjection {
    AnnotationProjection::rebuild(
        "channel:1",
        MessageSerial::new("msg:1").unwrap(),
        annotation_type(annotation_type_value),
        events,
    )
    .unwrap()
}

fn projection_with_options(
    annotation_type_value: &str,
    events: Vec<Annotation>,
    options: AnnotationProjectionOptions,
) -> AnnotationProjection {
    AnnotationProjection::rebuild_with_options(
        "channel:1",
        MessageSerial::new("msg:1").unwrap(),
        annotation_type(annotation_type_value),
        events,
        options,
    )
    .unwrap()
}

fn stored_event(
    serial_value: &str,
    annotation_type: &str,
    action: AnnotationAction,
    name: Option<&str>,
    client_id: Option<&str>,
    count: Option<u64>,
    stored_at_ms: i64,
) -> StoredAnnotationEvent {
    StoredAnnotationEvent {
        app_id: "app".to_string(),
        channel_id: "chat".to_string(),
        annotation: event(
            serial_value,
            annotation_type,
            action,
            name,
            client_id,
            count,
        ),
        stored_at_ms,
    }
}

fn projection_request(annotation_type: &str) -> AnnotationProjectionRequest {
    AnnotationProjectionRequest {
        app_id: "app".to_string(),
        channel_id: "chat".to_string(),
        message_serial: MessageSerial::new("msg:1").unwrap(),
        annotation_type: annotation_type.into(),
    }
}

fn events_request(annotation_type: &str) -> AnnotationEventsRequest {
    AnnotationEventsRequest {
        app_id: "app".to_string(),
        channel_id: "chat".to_string(),
        message_serial: MessageSerial::new("msg:1").unwrap(),
        annotation_type: annotation_type.into(),
    }
}

#[test]
fn annotation_type_parses_namespace_summarizer_and_version() {
    let ty = annotation_type("reaction:distinct.v1");

    assert_eq!(ty.namespace().unwrap(), "reaction");
    assert_eq!(ty.summarizer().unwrap(), AnnotationSummarizer::Distinct);
    assert_eq!(ty.version().unwrap(), 1);
}

#[test]
fn annotation_type_rejects_unknown_summarizer() {
    let error = AnnotationType::new("reaction:unknown.v1").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported annotation summarizer"),
        "unexpected error: {error}"
    );
}

#[test]
fn validation_requires_name_for_named_summarizers() {
    let error = event(
        "ann:1",
        "reaction:distinct.v1",
        AnnotationAction::Create,
        None,
        Some("client-1"),
        None,
    )
    .validate()
    .unwrap_err();

    assert!(error.to_string().contains("annotation name is required"));
}

#[test]
fn validation_requires_client_for_ownership_summarizers() {
    for (serial, annotation_type, name) in [
        ("ann:1", "reaction:flag.v1", None),
        ("ann:2", "reaction:distinct.v1", Some("like")),
        ("ann:3", "reaction:unique.v1", Some("like")),
    ] {
        let error = event(
            serial,
            annotation_type,
            AnnotationAction::Create,
            name,
            None,
            None,
        )
        .validate()
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("annotation client_id is required"),
            "unexpected error for {annotation_type}: {error}"
        );
    }
}

#[test]
fn total_counts_unidentified_and_repeated_client_events() {
    let projection = projection(
        "reaction:total.v1",
        vec![
            event(
                "ann:1",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                None,
                None,
            ),
            event(
                "ann:2",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:3",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:4",
                "reaction:total.v1",
                AnnotationAction::Delete,
                None,
                None,
                None,
            ),
        ],
    );

    assert_eq!(
        projection.summary,
        AnnotationSummary::Total(TotalAnnotationSummary { total: 2 })
    );
}

#[test]
fn flag_counts_each_identified_client_once() {
    let projection = projection(
        "reaction:flag.v1",
        vec![
            event(
                "ann:1",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:3",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-2"),
                None,
            ),
            event(
                "ann:4",
                "reaction:flag.v1",
                AnnotationAction::Delete,
                None,
                Some("client-1"),
                None,
            ),
        ],
    );

    assert_eq!(
        projection.summary,
        AnnotationSummary::Flag(IdentifiedAnnotationSummary {
            total: 1,
            client_ids: vec!["client-2".to_string()],
            clipped: false,
        })
    );
}

#[test]
fn distinct_allows_one_client_in_multiple_names() {
    let projection = projection(
        "reaction:distinct.v1",
        vec![
            event(
                "ann:1",
                "reaction:distinct.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reaction:distinct.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:3",
                "reaction:distinct.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
        ],
    );

    let AnnotationSummary::Distinct(names) = projection.summary else {
        panic!("expected distinct summary");
    };

    assert_eq!(names["like"].total, 1);
    assert_eq!(names["laugh"].total, 1);
}

#[test]
fn unique_moves_client_between_names() {
    let projection = projection(
        "reaction:unique.v1",
        vec![
            event(
                "ann:1",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
            ),
        ],
    );

    let AnnotationSummary::Unique(names) = projection.summary else {
        panic!("expected unique summary");
    };

    assert!(!names.contains_key("like"));
    assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
}

#[test]
fn multiple_tracks_counts_and_removes_identified_client_contribution() {
    let projection = projection(
        "reaction:multiple.v1",
        vec![
            event(
                "ann:1",
                "reaction:multiple.v1",
                AnnotationAction::Create,
                Some("vote"),
                Some("client-1"),
                Some(2),
            ),
            event(
                "ann:2",
                "reaction:multiple.v1",
                AnnotationAction::Create,
                Some("vote"),
                Some("client-1"),
                Some(3),
            ),
            event(
                "ann:3",
                "reaction:multiple.v1",
                AnnotationAction::Create,
                Some("vote"),
                None,
                Some(4),
            ),
            event(
                "ann:4",
                "reaction:multiple.v1",
                AnnotationAction::Delete,
                Some("vote"),
                Some("client-1"),
                None,
            ),
        ],
    );

    let AnnotationSummary::Multiple(names) = projection.summary else {
        panic!("expected multiple summary");
    };

    let vote = &names["vote"];
    assert_eq!(vote.total, 4);
    assert_eq!(vote.total_unidentified, 4);
    assert_eq!(vote.total_client_ids, 0);
    assert!(vote.client_counts.is_empty());
}

#[test]
fn rebuild_sorts_by_serial_and_ignores_duplicate_serials() {
    let mut duplicate = event(
        "ann:2",
        "reaction:unique.v1",
        AnnotationAction::Create,
        Some("sad"),
        Some("client-1"),
        None,
    );
    duplicate.id = AnnotationId::new("ann:duplicate").unwrap();

    let projection = projection(
        "reaction:unique.v1",
        vec![
            event(
                "ann:2",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
            ),
            duplicate,
            event(
                "ann:1",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
        ],
    );

    let AnnotationSummary::Unique(names) = projection.summary else {
        panic!("expected unique summary");
    };

    assert_eq!(projection.applied_events, 2);
    assert_eq!(projection.last_serial.as_ref().unwrap().as_str(), "ann:2");
    assert!(!names.contains_key("like"));
    assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
}

#[test]
fn rebuild_honors_delete_tombstone_when_node_order_sorts_before_create() {
    let create_a = event(
        "00000000000000000001:node-z:00000000000000000001",
        "reaction:distinct.v1",
        AnnotationAction::Create,
        Some("like"),
        Some("client-a"),
        None,
    );
    let create_b = event(
        "00000000000000000001:node-a:00000000000000000003",
        "reaction:distinct.v1",
        AnnotationAction::Create,
        Some("like"),
        Some("client-b"),
        None,
    );
    let mut delete_a = event(
        "00000000000000000001:node-a:00000000000000000005",
        "reaction:distinct.v1",
        AnnotationAction::Delete,
        Some("like"),
        Some("client-a"),
        None,
    );
    delete_a.id = create_a.id.clone();

    let projection = projection("reaction:distinct.v1", vec![create_a, create_b, delete_a]);

    let AnnotationSummary::Distinct(names) = projection.summary else {
        panic!("expected distinct summary");
    };
    assert_eq!(projection.applied_events, 1);
    assert_eq!(names["like"].client_ids, vec!["client-b".to_string()]);
}

#[test]
fn summary_serialization_excludes_raw_annotation_payloads() {
    let projection = projection(
        "reaction:total.v1",
        vec![event(
            "ann:1",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
        )],
    );

    let serialized = sonic_rs::to_string(&projection.summary).unwrap();

    assert_eq!(serialized, r#"{"total":1}"#);
    assert!(!serialized.contains("raw"));
}

#[test]
fn client_id_clipping_reports_only_when_over_limit() {
    let exact = projection_with_options(
        "reaction:flag.v1",
        vec![
            event(
                "ann:1",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-2"),
                None,
            ),
        ],
        AnnotationProjectionOptions {
            client_id_limit: Some(2),
        },
    );
    let AnnotationSummary::Flag(exact) = exact.summary else {
        panic!("expected flag summary");
    };
    assert!(!exact.clipped);
    assert_eq!(exact.client_ids.len(), 2);

    let clipped = projection_with_options(
        "reaction:flag.v1",
        vec![
            event(
                "ann:1",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-2"),
                None,
            ),
            event(
                "ann:3",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-3"),
                None,
            ),
        ],
        AnnotationProjectionOptions {
            client_id_limit: Some(2),
        },
    );
    let AnnotationSummary::Flag(clipped) = clipped.summary else {
        panic!("expected flag summary");
    };
    assert!(clipped.clipped);
    assert_eq!(clipped.total, 3);
    assert_eq!(clipped.client_ids.len(), 2);
}

#[tokio::test]
async fn memory_store_appends_events_and_materializes_projection() {
    let store = MemoryAnnotationStore::new();

    let projection = store
        .append_event(stored_event(
            "ann:1",
            "reaction:flag.v1",
            AnnotationAction::Create,
            None,
            Some("client-1"),
            None,
            1,
        ))
        .await
        .unwrap();
    store
        .append_event(stored_event(
            "ann:2",
            "reaction:flag.v1",
            AnnotationAction::Create,
            None,
            Some("client-2"),
            None,
            2,
        ))
        .await
        .unwrap();

    assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:1");

    let projection = store
        .get_projection(projection_request("reaction:flag.v1"))
        .await
        .unwrap()
        .unwrap();
    let AnnotationSummary::Flag(summary) = projection.summary else {
        panic!("expected flag summary");
    };
    assert_eq!(summary.total, 2);
    assert_eq!(
        summary.client_ids,
        vec!["client-1".to_string(), "client-2".to_string()]
    );
}

#[tokio::test]
async fn memory_store_deduplicates_by_annotation_serial() {
    let store = MemoryAnnotationStore::new();

    store
        .append_event(stored_event(
            "ann:1",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            1,
        ))
        .await
        .unwrap();
    store
        .append_event(stored_event(
            "ann:1",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            2,
        ))
        .await
        .unwrap();

    let events = store
        .get_events(events_request("reaction:total.v1"))
        .await
        .unwrap();
    let projection = store
        .rebuild_projection(projection_request("reaction:total.v1"))
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(
        projection.summary,
        AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
    );
}

#[tokio::test]
async fn memory_store_replays_raw_channel_events_by_annotation_serial() {
    let store = MemoryAnnotationStore::new();

    for (serial, annotation_type, name) in [
        ("ann:1", "reaction:total.v1", None),
        ("ann:2", "reaction:distinct.v1", Some("like")),
        ("ann:3", "reaction:total.v1", None),
    ] {
        store
            .append_event(stored_event(
                serial,
                annotation_type,
                AnnotationAction::Create,
                name,
                Some("client-1"),
                None,
                1,
            ))
            .await
            .unwrap();
    }

    let replay = store
        .replay_raw(RawAnnotationReplayRequest {
            app_id: "app".to_string(),
            channel_id: "chat".to_string(),
            message_serial: None,
            after_annotation_serial: Some(AnnotationSerial::new("ann:1").unwrap()),
            limit: 10,
        })
        .await
        .unwrap();

    assert_eq!(
        replay
            .iter()
            .map(|record| record.annotation_serial().as_str())
            .collect::<Vec<_>>(),
        vec!["ann:2", "ann:3"]
    );
}

#[tokio::test]
async fn memory_store_filters_message_before_applying_replay_limit() {
    let store = MemoryAnnotationStore::new();
    let target_serial = MessageSerial::new("msg:target").unwrap();

    for index in 1..=4 {
        let mut record = stored_event(
            &format!("ann:{index}"),
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            index,
        );
        record.annotation.message_serial = if index == 4 {
            target_serial.clone()
        } else {
            MessageSerial::new(format!("msg:noise-{index}")).unwrap()
        };
        store.append_event(record).await.unwrap();
    }

    let replay = store
        .replay_raw(RawAnnotationReplayRequest {
            app_id: "app".to_string(),
            channel_id: "chat".to_string(),
            message_serial: Some(target_serial),
            after_annotation_serial: None,
            limit: 1,
        })
        .await
        .unwrap();

    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].annotation_serial().as_str(), "ann:4");
}

#[tokio::test]
async fn memory_store_get_projection_repairs_stale_watermark() {
    let store = MemoryAnnotationStore::new();

    store
        .append_event(stored_event(
            "ann:1",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            1,
        ))
        .await
        .unwrap();

    let late_event = stored_event(
        "ann:2",
        "reaction:total.v1",
        AnnotationAction::Create,
        None,
        None,
        None,
        2,
    );
    let projection_key = MemoryAnnotationStore::event_projection_key(&late_event);
    let channel_key =
        MemoryAnnotationStore::channel_key(&late_event.app_id, &late_event.channel_id);
    {
        let mut state = store.state.write().await;
        state
            .events_by_projection
            .entry(projection_key)
            .or_default()
            .insert(late_event.annotation_serial().clone(), late_event.clone());
        state
            .raw_by_channel
            .entry(channel_key)
            .or_default()
            .insert(late_event.annotation_serial().clone(), late_event);
    }

    let projection = store
        .get_projection(projection_request("reaction:total.v1"))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:2");
    assert_eq!(
        projection.summary,
        AnnotationSummary::Total(TotalAnnotationSummary { total: 2 })
    );
}

#[tokio::test]
async fn memory_store_list_projections_rebuilds_cold_projection_cache() {
    let store = MemoryAnnotationStore::new();
    let event = stored_event(
        "ann:1",
        "reaction:total.v1",
        AnnotationAction::Create,
        None,
        None,
        None,
        1,
    );
    let projection_key = MemoryAnnotationStore::event_projection_key(&event);
    let channel_key = MemoryAnnotationStore::channel_key(&event.app_id, &event.channel_id);
    {
        let mut state = store.state.write().await;
        state
            .events_by_projection
            .entry(projection_key)
            .or_default()
            .insert(event.annotation_serial().clone(), event.clone());
        state
            .raw_by_channel
            .entry(channel_key)
            .or_default()
            .insert(event.annotation_serial().clone(), event);
        state.projections.clear();
    }

    let projections = store
        .list_projections_for_channel(AnnotationProjectionsForChannelRequest {
            app_id: "app".to_string(),
            channel_id: "chat".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(projections.len(), 1);
    assert_eq!(
        projections[0].summary,
        AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
    );
}

#[tokio::test]
async fn memory_store_reports_cold_projection_cache_rebuild_count() {
    let store = MemoryAnnotationStore::new();
    let event = stored_event(
        "ann:1",
        "reaction:total.v1",
        AnnotationAction::Create,
        None,
        None,
        None,
        1,
    );
    let projection_key = MemoryAnnotationStore::event_projection_key(&event);
    let channel_key = MemoryAnnotationStore::channel_key(&event.app_id, &event.channel_id);
    {
        let mut state = store.state.write().await;
        state
            .events_by_projection
            .entry(projection_key)
            .or_default()
            .insert(event.annotation_serial().clone(), event.clone());
        state
            .raw_by_channel
            .entry(channel_key)
            .or_default()
            .insert(event.annotation_serial().clone(), event);
        state.projections.clear();
    }

    let (projections, rebuild_count) = store
        .list_projections_for_channel_with_rebuild_count(AnnotationProjectionsForChannelRequest {
            app_id: "app".to_string(),
            channel_id: "chat".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(projections.len(), 1);
    assert_eq!(rebuild_count, 1);
}

#[tokio::test]
async fn memory_store_converges_after_out_of_order_unique_events() {
    let store = MemoryAnnotationStore::new();

    store
        .append_event(stored_event(
            "ann:2",
            "reaction:unique.v1",
            AnnotationAction::Create,
            Some("laugh"),
            Some("client-1"),
            None,
            2,
        ))
        .await
        .unwrap();
    let projection = store
        .append_event(stored_event(
            "ann:1",
            "reaction:unique.v1",
            AnnotationAction::Create,
            Some("like"),
            Some("client-1"),
            None,
            1,
        ))
        .await
        .unwrap();

    let AnnotationSummary::Unique(names) = projection.summary else {
        panic!("expected unique summary");
    };
    assert!(!names.contains_key("like"));
    assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
    assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:2");
}

#[tokio::test]
async fn memory_store_converges_under_concurrent_unique_churn() {
    let store = MemoryAnnotationStore::new();

    let earlier = store.append_event(stored_event(
        "ann:1",
        "reaction:unique.v1",
        AnnotationAction::Create,
        Some("like"),
        Some("client-1"),
        None,
        1,
    ));
    let later = store.append_event(stored_event(
        "ann:2",
        "reaction:unique.v1",
        AnnotationAction::Create,
        Some("laugh"),
        Some("client-1"),
        None,
        2,
    ));

    let (earlier_result, later_result) = tokio::join!(earlier, later);
    earlier_result.unwrap();
    later_result.unwrap();

    let projection = store
        .get_projection(projection_request("reaction:unique.v1"))
        .await
        .unwrap()
        .unwrap();
    let AnnotationSummary::Unique(names) = projection.summary else {
        panic!("expected unique summary");
    };
    assert!(!names.contains_key("like"));
    assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
    assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:2");
}

#[tokio::test]
async fn memory_store_purge_removes_events_and_stale_projection() {
    let store = MemoryAnnotationStore::new();

    store
        .append_event(stored_event(
            "ann:1",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            1,
        ))
        .await
        .unwrap();

    let (deleted, has_more) = store.purge_before(2, 10).await.unwrap();

    assert_eq!(deleted, 1);
    assert!(!has_more);
    assert!(
        store
            .get_projection(projection_request("reaction:total.v1"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .replay_raw(RawAnnotationReplayRequest {
                app_id: "app".to_string(),
                channel_id: "chat".to_string(),
                message_serial: None,
                after_annotation_serial: None,
                limit: 10,
            })
            .await
            .unwrap()
            .is_empty()
    );
}
