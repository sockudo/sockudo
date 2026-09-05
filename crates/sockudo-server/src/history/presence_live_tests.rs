//! Actual backend coverage for bounded durable presence lookup, using fresh tables.
use super::*;
use async_trait::async_trait;
use sockudo_core::history::*;
use sockudo_core::options::*;
use sockudo_core::presence_history::*;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

struct ObservedHistory {
    inner: Arc<dyn HistoryStore + Send + Sync>,
    reads: AtomicUsize,
    fault: AtomicU8,
}

#[async_trait]
impl HistoryStore for ObservedHistory {
    async fn reserve_publish_position(
        &self,
        app: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        self.inner.reserve_publish_position(app, channel).await
    }
    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        self.inner.append(record).await
    }
    async fn stream_inspection(&self, app: &str, channel: &str) -> Result<HistoryStreamInspection> {
        self.inner.stream_inspection(app, channel).await
    }
    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut page = self.inner.read_page(request.clone()).await?;
        match self.fault.swap(0, Ordering::SeqCst) {
            1 => {
                page.complete = false;
                page.truncated_by_retention = true;
            }
            2 => {
                self.inner
                    .reset_stream(
                        &request.app_id,
                        &request.channel,
                        "synthetic lookup reset",
                        None,
                    )
                    .await?;
            }
            _ => {}
        }
        Ok(page)
    }
}

const APP: &str = "presence-audit";
const CHANNEL: &str = "presence-audit";
const DURABLE_CHANNEL: &str = "[presence-history]presence-audit";

fn transition(
    user: &str,
    key: &str,
    event: PresenceHistoryEventKind,
) -> PresenceHistoryTransitionRecord {
    PresenceHistoryTransitionRecord {
        app_id: APP.into(),
        channel: CHANNEL.into(),
        user_id: user.into(),
        dedupe_key: key.into(),
        event_kind: event,
        cause: PresenceHistoryEventCause::Join,
        published_at_ms: now_ms(),
        connection_id: None,
        user_info: None,
        dead_node_id: None,
        retention: PresenceHistoryRetentionPolicy {
            retention_window_seconds: 3600,
            max_events_per_channel: Some(2000),
            max_bytes_per_channel: None,
        },
    }
}

async fn settled(store: &dyn HistoryStore, count: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let inspection = store.stream_inspection(APP, DURABLE_CHANNEL).await.unwrap();
        if inspection.retained.retained_messages == count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "presence append did not settle: {inspection:?}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn exercise(backend: HistoryBackend) {
    let prefix = format!("c4_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let mut config = HistoryConfig {
        enabled: true,
        backend,
        writer_shards: 1,
        ..Default::default()
    };
    config.postgres.table_prefix = prefix.clone();
    config.mysql.table_prefix = prefix.clone();
    config.dynamodb.table_prefix = prefix.clone();
    config.surrealdb.table_prefix = prefix.clone();
    config.scylladb.table_prefix = prefix.clone();
    let database = DatabaseConfig {
        postgres: DatabaseConnection {
            host: "127.0.0.1".into(),
            port: 15432,
            username: "postgres".into(),
            password: "postgres123".into(),
            database: "sockudo_test".into(),
            ..Default::default()
        },
        mysql: DatabaseConnection {
            host: "127.0.0.1".into(),
            port: 13306,
            username: "root".into(),
            password: "root123".into(),
            database: "sockudo".into(),
            ..Default::default()
        },
        dynamodb: DynamoDbSettings {
            region: "us-east-1".into(),
            table_name: prefix.clone(),
            endpoint_url: Some("http://127.0.0.1:18000".into()),
            aws_access_key_id: Some("synthetic".into()),
            aws_secret_access_key: Some("synthetic".into()),
            aws_profile_name: None,
        },
        surrealdb: SurrealDbSettings {
            url: "ws://127.0.0.1:18001".into(),
            namespace: prefix.clone(),
            database: "sockudo".into(),
            username: "root".into(),
            password: "root".into(),
            ..Default::default()
        },
        scylladb: ScyllaDbSettings {
            nodes: vec!["127.0.0.1:19044".into()],
            keyspace: prefix,
            replication_factor: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let first = create_history_store(&config, &database, &DatabasePooling::default(), None, None)
        .await
        .unwrap();
    let second = create_history_store(&config, &database, &DatabasePooling::default(), None, None)
        .await
        .unwrap();
    let stream = first
        .reserve_publish_position(APP, DURABLE_CHANNEL)
        .await
        .unwrap()
        .stream_id;
    for serial in 1..=600 {
        let record = transition(
            &format!("user-{serial}"),
            &format!("seed-{serial}"),
            PresenceHistoryEventKind::MemberAdded,
        );
        let payload = sonic_rs::json!({"published_at_ms": record.published_at_ms, "event": record.event_kind, "cause": record.cause, "user_id": record.user_id, "connection_id": null, "user_info": null, "dead_node_id": null, "dedupe_key": record.dedupe_key});
        first
            .append(HistoryAppendRecord {
                app_id: APP.into(),
                channel: DURABLE_CHANNEL.into(),
                stream_id: stream.clone(),
                serial,
                published_at_ms: record.published_at_ms,
                message_id: None,
                event_name: None,
                operation_kind: "append".into(),
                payload_bytes: sonic_rs::to_vec(&payload).unwrap().into(),
                retention: HistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_messages_per_channel: Some(2000),
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }
    settled(first.as_ref(), 600).await;
    let observed = Arc::new(ObservedHistory {
        inner: first.clone(),
        reads: AtomicUsize::new(0),
        fault: AtomicU8::new(0),
    });
    let presence = DurablePresenceHistoryStore::new(observed.clone(), None);
    presence
        .record_transition(transition(
            "warm",
            "warm",
            PresenceHistoryEventKind::MemberAdded,
        ))
        .await
        .unwrap();
    settled(first.as_ref(), 601).await;
    let before = observed.reads.load(Ordering::SeqCst);
    presence
        .record_transition(transition(
            "user-1",
            "cold-query",
            PresenceHistoryEventKind::MemberAdded,
        ))
        .await
        .unwrap();
    assert!(
        observed.reads.load(Ordering::SeqCst) - before <= 2,
        "cold lookup must read only candidate ranges"
    );
    settled(first.as_ref(), 601).await;
    let remote = DurablePresenceHistoryStore::new(second, None);
    remote
        .record_transition(transition(
            "user-1",
            "remote-leave",
            PresenceHistoryEventKind::MemberRemoved,
        ))
        .await
        .unwrap();
    settled(first.as_ref(), 602).await;
    presence
        .record_transition(transition(
            "user-1",
            "local-rejoin",
            PresenceHistoryEventKind::MemberAdded,
        ))
        .await
        .unwrap();
    settled(first.as_ref(), 603).await;
    presence
        .record_transition(transition(
            "unseen-user",
            "seed-1",
            PresenceHistoryEventKind::MemberAdded,
        ))
        .await
        .unwrap();
    settled(first.as_ref(), 603).await;
    observed.fault.store(1, Ordering::SeqCst);
    assert!(
        presence
            .record_transition(transition(
                "user-100",
                "incomplete",
                PresenceHistoryEventKind::MemberAdded
            ))
            .await
            .is_err()
    );
    settled(first.as_ref(), 603).await;
    presence
        .record_transition(transition(
            "user-100",
            "recovered",
            PresenceHistoryEventKind::MemberAdded,
        ))
        .await
        .unwrap();
    observed.fault.store(2, Ordering::SeqCst);
    assert!(
        presence
            .record_transition(transition(
                "user-200",
                "reset-race",
                PresenceHistoryEventKind::MemberAdded
            ))
            .await
            .is_err()
    );
    settled(first.as_ref(), 0).await;
    assert_ne!(
        first
            .stream_inspection(APP, DURABLE_CHANNEL)
            .await
            .unwrap()
            .stream_id
            .as_deref(),
        Some(stream.as_str())
    );
    presence
        .record_transition(transition(
            "user-1",
            "seed-1",
            PresenceHistoryEventKind::MemberAdded,
        ))
        .await
        .unwrap();
    settled(first.as_ref(), 1).await;
}

macro_rules! live_case {
    ($feature:literal, $test:ident, $backend:ident) => {
        #[cfg(feature = $feature)]
        #[tokio::test]
        #[ignore = "requires authorized localhost synthetic history fixtures"]
        async fn $test() {
            exercise(HistoryBackend::$backend).await;
        }
    };
}
live_case!("postgres", postgres_presence_ranges_live, Postgres);
live_case!("mysql", mysql_presence_ranges_live, Mysql);
live_case!("dynamodb", dynamodb_presence_ranges_live, DynamoDb);
live_case!("surrealdb", surreal_presence_ranges_live, SurrealDb);
live_case!("scylladb", scylla_presence_ranges_live, ScyllaDb);
