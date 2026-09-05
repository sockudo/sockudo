use super::*;
use scylla::statement::batch::{Batch, BatchType};
use sockudo_core::history::HistoryDirection;

async fn page_session() -> Arc<Session> {
    use scylla::errors::TranslationError;
    use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
    struct FixtureAddress(std::net::SocketAddr);
    #[async_trait::async_trait]
    impl AddressTranslator for FixtureAddress {
        async fn translate_address(
            &self,
            _: &UntranslatedPeer,
        ) -> std::result::Result<std::net::SocketAddr, TranslationError> {
            Ok(self.0)
        }
    }
    let address =
        std::env::var("SOCKUDO_C8_SCYLLA_ADDR").unwrap_or_else(|_| "127.0.0.1:19042".into());
    Arc::new(
        SessionBuilder::new()
            .known_node(address.clone())
            .address_translator(Arc::new(FixtureAddress(address.parse().unwrap())))
            .disallow_shard_aware_port(true)
            .build()
            .await
            .unwrap(),
    )
}
async fn seeded_store(count: usize, payload_size: usize) -> ScyllaHistoryStore {
    let db = ScyllaDbSettings {
        nodes: vec!["127.0.0.1:19042".into()],
        keyspace: format!("c8_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        replication_factor: 1,
        ..Default::default()
    };
    let store = ScyllaHistoryStore::from_session(
        &db,
        HistoryConfig::default(),
        None,
        None,
        page_session().await,
    )
    .await
    .unwrap();
    let reservation = store
        .reserve_publish_position("audit", "page")
        .await
        .unwrap();
    let query = format!(
        "INSERT INTO {} (app_id,channel,stream_id,serial,published_at_ms,message_id,event_name,operation_kind,payload_bytes,payload_size_bytes) VALUES (?,?,?,?,?,?,?,?,?,?)",
        store.tables.entries_fq()
    );
    let payload = vec![b'x'; payload_size];
    for chunk in (1..=count)
        .collect::<Vec<_>>()
        .chunks((256 * 1024 / payload_size).clamp(1, 128))
    {
        let mut batch = Batch::new(BatchType::Logged);
        for _ in chunk {
            batch.append_statement(Statement::new(query.clone()));
        }
        let values: Vec<_> = chunk
            .iter()
            .map(|serial| {
                (
                    "audit",
                    "page",
                    &reservation.stream_id,
                    *serial as i64,
                    (*serial % 997) as i64,
                    Option::<String>::None,
                    Some("event"),
                    "create",
                    payload.as_slice(),
                    payload_size as i64,
                )
            })
            .collect();
        store.session.batch(&batch, values).await.unwrap();
    }
    store.session.query_unpaged(format!("UPDATE {} SET next_serial=?,retained_messages=?,retained_bytes=?,oldest_available_serial=1,newest_available_serial=?,oldest_available_published_at_ms=1,newest_available_published_at_ms=? WHERE app_id='audit' AND channel='page'",store.tables.streams_fq()),(count as i64+1,count as i64,(count*payload_size) as i64,count as i64,(count%997) as i64)).await.unwrap();
    store
}
fn request(direction: HistoryDirection, limit: usize) -> HistoryReadRequest {
    HistoryReadRequest {
        app_id: "audit".into(),
        channel: "page".into(),
        direction,
        limit,
        cursor: None,
        bounds: Default::default(),
    }
}

#[tokio::test]
#[ignore = "requires local audit ScyllaDB fixture on port 19042"]
async fn scylla_bounded_sparse_pages_preserve_every_serial_and_cursor() {
    let store = seeded_store(2500, 1024).await;
    for direction in [HistoryDirection::OldestFirst, HistoryDirection::NewestFirst] {
        let mut query = request(direction, 7);
        query.bounds.start_serial = Some(100);
        query.bounds.end_serial = Some(2400);
        query.bounds.start_time_ms = Some(990);
        query.bounds.end_time_ms = Some(993);
        let mut expected: Vec<_> = (100..=2400)
            .filter(|serial| (990..=993).contains(&(serial % 997)))
            .collect();
        if direction == HistoryDirection::NewestFirst {
            expected.reverse();
        }
        let mut actual = Vec::new();
        let mut empty_continuations = 0;
        let mut pages = 0;
        loop {
            let page = store.read_page(query.clone()).await.unwrap();
            pages += 1;
            assert!(pages < 20, "cursor must progress even across empty pages");
            if page.items.is_empty() && page.has_more {
                empty_continuations += 1;
            }
            for item in page.items {
                assert_eq!(item.payload_bytes.as_ref(), &vec![b'x'; 1024]);
                actual.push(item.serial);
            }
            if !page.has_more {
                assert!(page.next_cursor.is_none());
                break;
            }
            let cursor = page.next_cursor.unwrap();
            if let Some(prior) = query.cursor.as_ref() {
                match direction {
                    HistoryDirection::OldestFirst => assert!(cursor.serial > prior.serial),
                    HistoryDirection::NewestFirst => assert!(cursor.serial < prior.serial),
                }
            }
            query.cursor = Some(cursor);
        }
        assert_eq!(actual, expected);
        assert!(
            empty_continuations > 0,
            "fixture must exercise sparse bounded scans"
        );
    }
    let mut query = request(HistoryDirection::OldestFirst, 10);
    query.bounds.start_serial = Some(2487);
    query.bounds.end_serial = Some(2498);
    let first = store.read_page(query.clone()).await.unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        (2487..=2496).collect::<Vec<_>>()
    );
    query.cursor = first.next_cursor;
    let last = store.read_page(query).await.unwrap();
    assert_eq!(
        last.items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>(),
        vec![2497, 2498]
    );
    assert!(!last.has_more);
}

#[tokio::test]
#[ignore = "requires local audit ScyllaDB fixture; repeated release before/after measurement"]
async fn benchmark_scylla_bounded_history_pages() {
    for count in [1000, 10000] {
        for payload_size in [1024, 16384] {
            let store = seeded_store(count, payload_size).await;
            for direction in [HistoryDirection::OldestFirst, HistoryDirection::NewestFirst] {
                for limit in [1, 100] {
                    let mut query = request(direction, limit);
                    if direction == HistoryDirection::OldestFirst {
                        query.bounds.start_serial = Some((count / 2) as u64);
                    }
                    let expected: Vec<u64> = if direction == HistoryDirection::OldestFirst {
                        (count / 2..count / 2 + limit)
                            .map(|value| value as u64)
                            .collect()
                    } else {
                        (count - limit + 1..=count)
                            .rev()
                            .map(|value| value as u64)
                            .collect()
                    };
                    // Warm the service cache and route before timed repetitions.
                    for _ in 0..3 {
                        store.read_page(query.clone()).await.unwrap();
                    }
                    for sample in 0..9 {
                        let started = std::time::Instant::now();
                        let page = store.read_page(query.clone()).await.unwrap();
                        let nanos = started.elapsed().as_nanos();
                        assert_eq!(
                            page.items
                                .iter()
                                .map(|item| item.serial)
                                .collect::<Vec<_>>(),
                            expected
                        );
                        assert!(
                            page.items
                                .iter()
                                .all(|item| item.payload_bytes.len() == payload_size
                                    && item.payload_bytes.iter().all(|byte| *byte == b'x'))
                        );
                        assert!(page.has_more);
                        println!(
                            "C8,count={count},payload={payload_size},direction={direction:?},limit={limit},sample={sample},ns={nanos},verified={}",
                            page.items.len()
                        );
                    }
                }
            }
        }
    }
}
